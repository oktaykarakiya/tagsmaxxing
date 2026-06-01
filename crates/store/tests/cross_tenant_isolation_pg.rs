//! Cross-tenant isolation integration tests — the definitive proof that Postgres RLS
//! (§13) prevents tenant A from ever reading, writing, or searching tenant B's data
//! (plan §13, §31.5; acceptance of P5-T2, reworked in P6-T14).
//!
//! # Run under the non-privileged role
//!
//! These assertions only mean something when run as a role that **does not** bypass RLS. The
//! P6-T14 rework makes that real: every tenant-scoped read/write here goes through the
//! `kb_app` role (`NOSUPERUSER NOBYPASSRLS`, migration `0006_app_role.sql`) — either via a
//! `PgStore` method (which routes to its `app_pool`) or via a raw query inside
//! [`kb_testsupport::tenant_tx`] on the app pool. Seeding of tenants/users uses the privileged
//! pool (RLS bypassed, as a real provisioning path would). The earlier version connected as the
//! `kb` superuser and set the tenant GUC on a *separate* round-trip from the query, so RLS was
//! a no-op and these tests could not pass honestly (docs/analysis/07-rls-enforcement-blocker.md).
//!
//! These tests need **Podman** (this project targets Podman exclusively) and network access to
//! pull `pgvector/pgvector`. They are gated `#[ignore]` so `just ci` stays green when no
//! container runtime is available. Run them via the lane:
//!
//! ```text
//! systemctl --user start podman.socket
//! DOCKER_HOST=unix://$XDG_RUNTIME_DIR/podman/podman.sock \
//!   cargo test -p kb-store --test cross_tenant_isolation_pg -- --ignored
//! ```
//!
//! §31.5 checkpoint — this is the mandatory human-review gate for RLS/tenant isolation.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use chrono::Utc;
use kb_core::chunk::Chunk;
use kb_core::file::FileRecord;
use kb_core::hash::Sha256;
use kb_core::query::{Query, QueryFilters};
use kb_core::role::Role;
use kb_core::status::ProcessingStatus;
use kb_core::store::Store;
use kb_core::usage::UsageEvent;
use kb_store::PgStore;
use sqlx::PgPool;

// ── Fixture: two-tenant Postgres ───────────────────────────────────────────────

/// Fully initialised test setup with two tenants (A, B), each with one user, and a connected
/// [`PgStore`] backed by a fresh database in the per-binary shared pgvector container.
struct TwoTenantSetup {
    /// The store, connected (two-role) and with migrations applied.
    store: Arc<PgStore>,
    /// The **privileged** pool — provisioning + cross-tenant seeding (RLS bypassed).
    admin_pool: PgPool,
    /// The **kb_app** pool — RLS-enforced; all tenant-data assertions run through it.
    app_pool: PgPool,
    /// Tenant A id.
    tenant_a: i64,
    /// Tenant B id.
    tenant_b: i64,
    /// User id in tenant A.
    user_a: i64,
    /// User id in tenant B.
    user_b: i64,
}

/// Spin up a fresh DB in the shared pgvector container, run migrations, create two tenants and
/// two users (seeded via the privileged pool).
async fn setup_two_tenants() -> anyhow::Result<TwoTenantSetup> {
    let db = kb_testsupport::fresh_db().await?;
    let store = Arc::new(PgStore::with_roles(&db.admin_url, &db.app_url));
    store.connect().await?;
    let admin_pool = store.pool()?;
    let app_pool = store.app_pool()?;

    // Two tenants (the `tenants` table has no RLS; provisioned by the privileged pool).
    let tenant_a: i64 = sqlx::query_scalar(
        "INSERT INTO tenants (slug, name) VALUES ('tenant-a', 'Tenant A') RETURNING id",
    )
    .fetch_one(&admin_pool)
    .await?;
    let tenant_b: i64 = sqlx::query_scalar(
        "INSERT INTO tenants (slug, name) VALUES ('tenant-b', 'Tenant B') RETURNING id",
    )
    .fetch_one(&admin_pool)
    .await?;

    // One user per tenant. `users` is RLS-protected, but seeding runs on the privileged pool
    // (BYPASSRLS) — a realistic admin provisioning path.
    let user_a: i64 = sqlx::query_scalar(
        "INSERT INTO users (tenant_id, email, password_hash, role) \
         VALUES ($1, 'alice@a.example', 'hash-a', 'admin') RETURNING id",
    )
    .bind(tenant_a)
    .fetch_one(&admin_pool)
    .await?;
    let user_b: i64 = sqlx::query_scalar(
        "INSERT INTO users (tenant_id, email, password_hash, role) \
         VALUES ($1, 'bob@b.example', 'hash-b', 'admin') RETURNING id",
    )
    .bind(tenant_b)
    .fetch_one(&admin_pool)
    .await?;

    Ok(TwoTenantSetup {
        store,
        admin_pool,
        app_pool,
        tenant_a,
        tenant_b,
        user_a,
        user_b,
    })
}

// ── Raw-SQL helpers that run as kb_app inside the tenant's RLS transaction ──────────

/// `SELECT COUNT(*) …` evaluated as `tenant` on the kb_app pool — the GUC and the query share
/// one transaction, so RLS actually applies. This is the assertion primitive for isolation.
async fn count_as(pool: &PgPool, tenant: i64, sql: &str) -> i64 {
    kb_testsupport::count_as_tenant(pool, tenant, sql)
        .await
        .unwrap()
}

/// Run an `INSERT … RETURNING id` as `tenant` on the kb_app pool (proving kb_app can write its
/// *own* tenant's rows under the RLS `WITH CHECK`). `sql` must inline its literals.
async fn insert_id_as(pool: &PgPool, tenant: i64, sql: &str) -> i64 {
    let mut tx = kb_testsupport::tenant_tx(pool, tenant).await.unwrap();
    let id: i64 = sqlx::query_scalar(sql).fetch_one(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    id
}

// ── Helpers for test data creation ─────────────────────────────────────────────

fn make_file_rec(tenant_id: i64, document_id: i64, page_no: i32, label: &str) -> FileRecord {
    let mut hash = [0u8; 32];
    hash[..4].copy_from_slice(label.as_bytes().get(0..4).unwrap_or(b"0000"));
    FileRecord {
        id: 0,
        tenant_id,
        document_id,
        page_no,
        page_label: Some(label.into()),
        sha256: Sha256::from_bytes(hash),
        blob_key: format!("t{tenant_id}/{label}"),
        path: Some(format!("/tmp/{label}")),
        mime: Some("text/plain".into()),
        size_bytes: Some(100),
        meta: serde_json::json!({"page": label}),
        status: ProcessingStatus::Ready,
        ingested_at: Utc::now(),
    }
}

fn make_chunk(tenant_id: i64, document_id: i64, file_id: i64, idx: i32, content: &str) -> Chunk {
    Chunk {
        id: 0,
        tenant_id,
        document_id,
        file_id,
        page_no: Some(1),
        idx,
        content: content.into(),
        ts_offset: None,
        embedding: vec![0.1f32; 1024],
    }
}

fn spike_vector(tenant_specific_pos: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; 1024];
    if tenant_specific_pos < 1024 {
        v[tenant_specific_pos] = 1.0;
    }
    v
}

// ── Test 1: Document isolation (doc-07 Probe 1) ─────────────────────────────────

/// Tenant A inserts a document → tenant B's queries don't find it, even by direct id lookup.
/// This is exactly doc-07 "Probe 1": with tenant 2 active, a count of documents after tenant 1
/// inserted one must be 0. Under `kb_app` (not the superuser) it now is.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn tenant_b_cannot_read_tenant_a_document() -> anyhow::Result<()> {
    let s = setup_two_tenants().await?;

    // Insert a document as tenant A (as kb_app, in tenant A's RLS transaction).
    let doc_a = insert_id_as(
        &s.app_pool,
        s.tenant_a,
        &format!(
            "INSERT INTO documents (tenant_id, title, kind, status) \
             VALUES ({}, 'Secret Doc', 'document', 'ready') RETURNING id",
            s.tenant_a
        ),
    )
    .await;

    // Tenant A sees its own document.
    assert_eq!(
        count_as(
            &s.app_pool,
            s.tenant_a,
            &format!("SELECT COUNT(*) FROM documents WHERE id = {doc_a}")
        )
        .await,
        1,
        "tenant A must see its own document"
    );

    // Tenant B: direct id lookup must return zero rows (RLS filters out tenant A's row).
    assert_eq!(
        count_as(
            &s.app_pool,
            s.tenant_b,
            &format!("SELECT COUNT(*) FROM documents WHERE id = {doc_a}")
        )
        .await,
        0,
        "RLS violated: tenant B read tenant A's document via direct id"
    );

    // Tenant B's whole-table scan is empty (doc-07 Probe 1 returns 0).
    assert_eq!(
        count_as(&s.app_pool, s.tenant_b, "SELECT COUNT(*) FROM documents").await,
        0,
        "tenant B should see zero documents (RLS scoped to tenant B only)"
    );

    Ok(())
}

/// Tenant A inserts a document + file → tenant B sees neither. Exercises the `files` table and
/// the store's `upsert_file` (which runs as kb_app internally).
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn tenant_b_cannot_read_tenant_a_files() -> anyhow::Result<()> {
    let s = setup_two_tenants().await?;

    let doc_a = insert_id_as(
        &s.app_pool,
        s.tenant_a,
        &format!(
            "INSERT INTO documents (tenant_id, title, kind, status) \
             VALUES ({}, 'Doc A', 'document', 'ready') RETURNING id",
            s.tenant_a
        ),
    )
    .await;

    let rec = make_file_rec(s.tenant_a, doc_a, 1, "page-a1");
    let file_id = s.store.upsert_file(&rec).await?;
    assert!(file_id > 0);

    // Tenant A sees the file; tenant B sees nothing.
    assert_eq!(
        count_as(
            &s.app_pool,
            s.tenant_a,
            &format!("SELECT COUNT(*) FROM files WHERE id = {file_id}")
        )
        .await,
        1
    );
    assert_eq!(
        count_as(
            &s.app_pool,
            s.tenant_b,
            &format!("SELECT COUNT(*) FROM files WHERE id = {file_id}")
        )
        .await,
        0,
        "RLS violated: tenant B read tenant A's file via direct id"
    );
    assert_eq!(
        count_as(&s.app_pool, s.tenant_b, "SELECT COUNT(*) FROM files").await,
        0
    );

    Ok(())
}

// ── Test 2: Tag isolation ─────────────────────────────────────────────────────

/// Tags and tag aliases inserted by tenant A are invisible to tenant B.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn tags_are_tenant_scoped() -> anyhow::Result<()> {
    let s = setup_two_tenants().await?;

    let emb_a = vec![0.1f32; 1024];
    let tag_a1 = s.store.upsert_tag(s.tenant_a, "alpha", &emb_a).await?;
    let tag_a2 = s.store.upsert_tag(s.tenant_a, "beta", &emb_a).await?;
    s.store
        .insert_tag_alias(s.tenant_a, "first", tag_a1)
        .await?;
    s.store
        .insert_tag_alias(s.tenant_a, "second", tag_a2)
        .await?;

    // Tenant A sees both tags + aliases.
    assert_eq!(
        count_as(&s.app_pool, s.tenant_a, "SELECT COUNT(*) FROM tags").await,
        2,
        "tenant A should see 2 tags"
    );
    assert_eq!(
        count_as(&s.app_pool, s.tenant_a, "SELECT COUNT(*) FROM tag_aliases").await,
        2,
        "tenant A should see 2 aliases"
    );

    // Tenant B sees none.
    assert_eq!(
        count_as(&s.app_pool, s.tenant_b, "SELECT COUNT(*) FROM tags").await,
        0,
        "RLS violated: tenant B sees tenant A's tags"
    );
    assert_eq!(
        count_as(&s.app_pool, s.tenant_b, "SELECT COUNT(*) FROM tag_aliases").await,
        0,
        "RLS violated: tenant B sees tenant A's tag aliases"
    );

    // Tenant B's alias lookup + similar-tags (store methods, run as kb_app) return nothing.
    assert!(
        s.store.lookup_alias(s.tenant_b, "first").await?.is_none(),
        "RLS violated: tenant B resolved tenant A's tag alias"
    );
    assert!(
        s.store.find_similar_tags(s.tenant_b).await?.is_empty(),
        "RLS violated: tenant B sees tenant A's similar tags"
    );

    Ok(())
}

// ── Test 3: Usage events isolation ────────────────────────────────────────────

/// Usage events written by tenant A are invisible to tenant B.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn usage_events_are_tenant_scoped() -> anyhow::Result<()> {
    let s = setup_two_tenants().await?;

    let event_a = UsageEvent {
        id: 0,
        tenant_id: s.tenant_a,
        user_id: Some(s.user_a),
        model: "bge-m3".into(),
        role: Role::Embed,
        backend_id: Some("backend-1".into()),
        prompt_tokens: Some(512),
        completion_tokens: Some(0),
        latency_ms: Some(42),
        cost_micros: None,
        created_at: Utc::now(),
    };
    let ev_id = s.store.insert_usage_event(&event_a).await?;
    assert!(ev_id > 0);

    assert_eq!(
        count_as(
            &s.app_pool,
            s.tenant_a,
            &format!("SELECT COUNT(*) FROM usage_events WHERE id = {ev_id}")
        )
        .await,
        1
    );
    assert_eq!(
        count_as(
            &s.app_pool,
            s.tenant_b,
            &format!("SELECT COUNT(*) FROM usage_events WHERE id = {ev_id}")
        )
        .await,
        0,
        "RLS violated: tenant B read tenant A's usage event"
    );
    assert_eq!(
        count_as(&s.app_pool, s.tenant_b, "SELECT COUNT(*) FROM usage_events").await,
        0
    );

    Ok(())
}

// ── Test 4: Hybrid search isolation ───────────────────────────────────────────

/// Tenant A inserts a document with chunks → tenant B's hybrid search returns nothing.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn hybrid_search_is_tenant_scoped() -> anyhow::Result<()> {
    let s = setup_two_tenants().await?;

    let doc_a = insert_id_as(
        &s.app_pool,
        s.tenant_a,
        &format!(
            "INSERT INTO documents (tenant_id, title, kind, status) \
             VALUES ({}, 'Searchable Doc', 'document', 'ready') RETURNING id",
            s.tenant_a
        ),
    )
    .await;

    let rec = make_file_rec(s.tenant_a, doc_a, 1, "search-page");
    let file_id = s.store.upsert_file(&rec).await?;
    let chunks: Vec<Chunk> = vec![
        make_chunk(s.tenant_a, doc_a, file_id, 0, "unique pangolin content"),
        make_chunk(s.tenant_a, doc_a, file_id, 1, "more secret data here"),
    ];
    s.store.upsert_chunks(file_id, &chunks).await?;

    // Tenant A finds its own document.
    let q = Query {
        text: "pangolin".to_string(),
        filters: QueryFilters::default(),
        top_k: 10,
    };
    let hits_a = s
        .store
        .hybrid_search(s.tenant_a, &q, &spike_vector(0))
        .await?;
    assert!(
        !hits_a.is_empty(),
        "tenant A must find its own document via hybrid search"
    );
    assert_eq!(hits_a[0].document_id, doc_a);

    // Tenant B finds nothing (keyword + vector).
    let hits_b = s
        .store
        .hybrid_search(s.tenant_b, &q, &spike_vector(0))
        .await?;
    assert!(
        hits_b.is_empty(),
        "RLS violated: tenant B found tenant A's document via hybrid search"
    );
    let q_vector = Query {
        text: String::new(),
        filters: QueryFilters::default(),
        top_k: 10,
    };
    let hits_vec_b = s
        .store
        .hybrid_search(s.tenant_b, &q_vector, &spike_vector(0))
        .await?;
    assert!(
        hits_vec_b.is_empty(),
        "RLS violated: tenant B found tenant A's chunks via vector-only search"
    );

    Ok(())
}

// ── Test 5: Search with both tenants' data ────────────────────────────────────

/// When both tenants have documents, each tenant's search returns only their own data.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn each_tenant_only_sees_own_data_in_search() -> anyhow::Result<()> {
    let s = setup_two_tenants().await?;

    // Tenant A: distinctive "finances" document.
    let doc_a = insert_id_as(
        &s.app_pool,
        s.tenant_a,
        &format!(
            "INSERT INTO documents (tenant_id, title, kind, status) \
             VALUES ({}, 'Alpha Report', 'document', 'ready') RETURNING id",
            s.tenant_a
        ),
    )
    .await;
    let rec_a = make_file_rec(s.tenant_a, doc_a, 1, "alpha-page");
    let file_a = s.store.upsert_file(&rec_a).await?;
    s.store
        .upsert_chunks(
            file_a,
            &[make_chunk(
                s.tenant_a,
                doc_a,
                file_a,
                0,
                "alpha secret report about finances",
            )],
        )
        .await?;

    // Tenant B: a different document.
    let doc_b = insert_id_as(
        &s.app_pool,
        s.tenant_b,
        &format!(
            "INSERT INTO documents (tenant_id, title, kind, status) \
             VALUES ({}, 'Beta Notes', 'document', 'ready') RETURNING id",
            s.tenant_b
        ),
    )
    .await;
    let rec_b = make_file_rec(s.tenant_b, doc_b, 1, "beta-page");
    let file_b = s.store.upsert_file(&rec_b).await?;
    s.store
        .upsert_chunks(
            file_b,
            &[make_chunk(
                s.tenant_b,
                doc_b,
                file_b,
                0,
                "beta public meeting notes",
            )],
        )
        .await?;

    // Tenant A finds only alpha.
    let hits_a = s
        .store
        .hybrid_search(
            s.tenant_a,
            &Query {
                text: "finances".to_string(),
                filters: QueryFilters::default(),
                top_k: 10,
            },
            &spike_vector(0),
        )
        .await?;
    let doc_ids_a: Vec<i64> = hits_a.iter().map(|h| h.document_id).collect();
    assert!(
        doc_ids_a.contains(&doc_a),
        "tenant A should find its own doc"
    );
    assert!(
        !doc_ids_a.contains(&doc_b),
        "RLS violated: tenant A sees tenant B's document in search"
    );

    // Tenant B finds only beta.
    let hits_b = s
        .store
        .hybrid_search(
            s.tenant_b,
            &Query {
                text: "meeting".to_string(),
                filters: QueryFilters::default(),
                top_k: 10,
            },
            &spike_vector(0),
        )
        .await?;
    let doc_ids_b: Vec<i64> = hits_b.iter().map(|h| h.document_id).collect();
    assert!(
        doc_ids_b.contains(&doc_b),
        "tenant B should find its own doc"
    );
    assert!(
        !doc_ids_b.contains(&doc_a),
        "RLS violated: tenant B sees tenant A's document in search"
    );

    // Tenant A cannot find tenant B's content via keyword.
    let hits_a2 = s
        .store
        .hybrid_search(
            s.tenant_a,
            &Query {
                text: "meeting".to_string(),
                filters: QueryFilters::default(),
                top_k: 10,
            },
            &spike_vector(0),
        )
        .await?;
    assert!(
        hits_a2.iter().all(|h| h.document_id != doc_b),
        "RLS violated: tenant A found tenant B's 'meeting' document"
    );

    Ok(())
}

// ── Test 6: Transactional ingest isolation ────────────────────────────────────

/// `transactional_ingest` upserts a document as tenant A → tenant B cannot see any of the
/// ingested rows (document, files, tags, chunks).
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn transactional_ingest_respects_tenant_boundary() -> anyhow::Result<()> {
    use kb_core::document::Document;
    let s = setup_two_tenants().await?;

    let doc = Document {
        id: 0,
        tenant_id: s.tenant_a,
        title: Some("Ingested Doc".into()),
        summary: Some("Test summary".into()),
        user_note: None,
        kind: kb_core::kind::DocKind::Document,
        meta: serde_json::json!({}),
        page_count: 1,
        status: ProcessingStatus::Pending,
        created_at: Utc::now(),
        local_only: false,
    };
    let rec = make_file_rec(s.tenant_a, 0, 1, "isolated-page");
    let tag_id = s
        .store
        .upsert_tag(s.tenant_a, "isolated-tag", &vec![0.5f32; 1024])
        .await?;
    let chunk = make_chunk(s.tenant_a, 0, 0, 0, "isolated chunk text");

    let doc_id = s
        .store
        .transactional_ingest(&doc, &[rec], &[tag_id], &[vec![chunk]])
        .await?;
    assert!(doc_id > 0);

    // Tenant A sees everything it ingested.
    assert_eq!(
        count_as(&s.app_pool, s.tenant_a, "SELECT COUNT(*) FROM documents").await,
        1
    );
    assert_eq!(
        count_as(&s.app_pool, s.tenant_a, "SELECT COUNT(*) FROM files").await,
        1
    );
    assert_eq!(
        count_as(&s.app_pool, s.tenant_a, "SELECT COUNT(*) FROM chunks").await,
        1
    );
    assert_eq!(
        count_as(&s.app_pool, s.tenant_a, "SELECT COUNT(*) FROM tags").await,
        1
    );
    assert_eq!(
        count_as(
            &s.app_pool,
            s.tenant_a,
            "SELECT COUNT(*) FROM document_tags"
        )
        .await,
        1
    );

    // Tenant B sees nothing across all tables.
    for (table, label) in [
        ("documents", "documents"),
        ("files", "files"),
        ("chunks", "chunks"),
        ("tags", "tags"),
        ("document_tags", "document_tags"),
    ] {
        assert_eq!(
            count_as(
                &s.app_pool,
                s.tenant_b,
                &format!("SELECT COUNT(*) FROM {table}")
            )
            .await,
            0,
            "RLS violated: tenant B sees {label}"
        );
    }

    Ok(())
}

// ── Test 7: Users are tenant-scoped ───────────────────────────────────────────

/// Users belonging to tenant A are invisible to tenant B.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn users_are_tenant_scoped() -> anyhow::Result<()> {
    let s = setup_two_tenants().await?;

    // Tenant A sees its own user.
    assert_eq!(
        count_as(&s.app_pool, s.tenant_a, "SELECT COUNT(*) FROM users").await,
        1,
        "tenant A should see its own user"
    );
    assert_eq!(
        count_as(
            &s.app_pool,
            s.tenant_a,
            &format!("SELECT COUNT(*) FROM users WHERE id = {}", s.user_a)
        )
        .await,
        1,
    );

    // Tenant B cannot see tenant A's user, and sees exactly its own.
    assert_eq!(
        count_as(
            &s.app_pool,
            s.tenant_b,
            &format!("SELECT COUNT(*) FROM users WHERE id = {}", s.user_a)
        )
        .await,
        0,
        "RLS violated: tenant B read tenant A's user via direct id"
    );
    assert_eq!(
        count_as(&s.app_pool, s.tenant_b, "SELECT COUNT(*) FROM users").await,
        1,
        "tenant B should see its own user"
    );
    assert_eq!(
        count_as(
            &s.app_pool,
            s.tenant_b,
            &format!("SELECT COUNT(*) FROM users WHERE id = {}", s.user_b)
        )
        .await,
        1,
    );

    Ok(())
}

// ── Test 8: Jobs are tenant-scoped ────────────────────────────────────────────

/// A job enqueued by tenant A is invisible to tenant B (RLS on the `jobs` table, as seen by the
/// kb_app role — the cross-tenant queue itself runs on the privileged pool, which is correct).
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn jobs_are_tenant_scoped() -> anyhow::Result<()> {
    let s = setup_two_tenants().await?;

    let job_a = insert_id_as(
        &s.app_pool,
        s.tenant_a,
        &format!(
            "INSERT INTO jobs (tenant_id, kind, status) \
             VALUES ({}, 'ingest', 'queued') RETURNING id",
            s.tenant_a
        ),
    )
    .await;

    assert_eq!(
        count_as(
            &s.app_pool,
            s.tenant_a,
            &format!("SELECT COUNT(*) FROM jobs WHERE id = {job_a}")
        )
        .await,
        1
    );
    assert_eq!(
        count_as(
            &s.app_pool,
            s.tenant_b,
            &format!("SELECT COUNT(*) FROM jobs WHERE id = {job_a}")
        )
        .await,
        0,
        "RLS violated: tenant B read tenant A's job via direct id"
    );
    assert_eq!(
        count_as(&s.app_pool, s.tenant_b, "SELECT COUNT(*) FROM jobs").await,
        0
    );

    // The privileged pool, by contrast, is intentionally cross-tenant (the worker queue scans
    // all tenants): it sees tenant A's job. This asymmetry is the two-role design.
    let admin_sees: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE id = $1")
        .bind(job_a)
        .fetch_one(&s.admin_pool)
        .await?;
    assert_eq!(
        admin_sees, 1,
        "the privileged queue role must see jobs cross-tenant"
    );

    Ok(())
}
