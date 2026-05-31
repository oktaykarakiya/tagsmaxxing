//! Cross-tenant isolation integration tests — the definitive proof that Postgres RLS
//! (§13) prevents tenant A from ever reading, writing, or searching tenant B's data
//! (plan §13, §31.5; acceptance of P5-T2).
//!
//! These tests need **Podman** (this project targets Podman exclusively) and network
//! access to pull `pgvector/pgvector`. They are gated `#[ignore]` so `just ci` stays
//! green when no container runtime is available. Run them explicitly against the Podman
//! socket:
//!
//! ```text
//! systemctl --user start podman.socket
//! DOCKER_HOST=unix://$XDG_RUNTIME_DIR/podman/podman.sock \
//!   cargo test -p kb-store --test cross_tenant_isolation_pg -- --ignored
//! ```
//!
//! ## What this suite proves
//!
//! 1. **Document isolation** — tenant B cannot find tenant A's documents, even by id.
//! 2. **File isolation** — tenant B cannot access tenant A's file records.
//! 3. **Tag isolation** — tenant A's tags and aliases are invisible to tenant B.
//! 4. **Usage isolation** — usage events are scoped per tenant.
//! 5. **Search isolation** — hybrid search for tenant B never returns tenant A's chunks.
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
use testcontainers::core::ports::IntoContainerPort;
use testcontainers::core::wait::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

// ── Fixture: two-tenant Postgres ───────────────────────────────────────────────

/// Fully initialised test setup with two tenants (A, B), each with one user, and a
/// connected [`PgStore`] backed by a fresh pgvector container.
struct TwoTenantSetup {
    /// The testcontainers handle — dropped at test end to kill the container.
    _container: testcontainers::ContainerAsync<GenericImage>,
    /// The store, connected and with migrations applied.
    store: Arc<PgStore>,
    /// The underlying pool for direct SQL queries.
    pool: PgPool,
    /// Tenant A id.
    tenant_a: i64,
    /// Tenant B id.
    tenant_b: i64,
    /// User id in tenant A.
    user_a: i64,
    /// User id in tenant B.
    user_b: i64,
}

/// Spin up a pgvector container, run migrations, create two tenants and two users.
async fn setup_two_tenants() -> anyhow::Result<TwoTenantSetup> {
    let container = GenericImage::new("pgvector/pgvector", "pg17")
        .with_exposed_port(5432u16.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "kb")
        .with_env_var("POSTGRES_PASSWORD", "kb")
        .with_env_var("POSTGRES_DB", "kb")
        .start()
        .await?;

    let host_port = container.get_host_port_ipv4(5432u16.tcp()).await?;
    let url = format!("postgres://kb:kb@127.0.0.1:{host_port}/kb?sslmode=disable");

    let store = Arc::new(PgStore::new(&url));
    store.connect().await?;
    let pool = store.pool()?;

    // Create two tenants.
    let tenant_a: i64 = sqlx::query_scalar(
        "INSERT INTO tenants (slug, name) VALUES ('tenant-a', 'Tenant A') RETURNING id",
    )
    .fetch_one(&pool)
    .await?;

    let tenant_b: i64 = sqlx::query_scalar(
        "INSERT INTO tenants (slug, name) VALUES ('tenant-b', 'Tenant B') RETURNING id",
    )
    .fetch_one(&pool)
    .await?;

    // Create one user per tenant.
    let user_a: i64 = sqlx::query_scalar(
        "INSERT INTO users (tenant_id, email, password_hash, role) \
         VALUES ($1, 'alice@a.example', 'hash-a', 'admin') RETURNING id",
    )
    .bind(tenant_a)
    .fetch_one(&pool)
    .await?;

    let user_b: i64 = sqlx::query_scalar(
        "INSERT INTO users (tenant_id, email, password_hash, role) \
         VALUES ($1, 'bob@b.example', 'hash-b', 'admin') RETURNING id",
    )
    .bind(tenant_b)
    .fetch_one(&pool)
    .await?;

    Ok(TwoTenantSetup {
        _container: container,
        store,
        pool,
        tenant_a,
        tenant_b,
        user_a,
        user_b,
    })
}

/// Set the active tenant for RLS on the shared pool connection.
async fn switch_tenant(pool: &PgPool, tenant_id: i64) {
    sqlx::query("SELECT app_set_current_tenant($1)")
        .bind(tenant_id)
        .execute(pool)
        .await
        .unwrap();
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

// ── Test 1: Document isolation ────────────────────────────────────────────────

/// Tenant A inserts a document → tenant B's queries don't find it, even by direct id
/// lookup. RLS must block cross-tenant document reads.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn tenant_b_cannot_read_tenant_a_document() -> anyhow::Result<()> {
    let s = setup_two_tenants().await?;

    // ── Insert a document as tenant A ──
    switch_tenant(&s.pool, s.tenant_a).await;
    let doc_a: i64 = sqlx::query_scalar(
        "INSERT INTO documents (tenant_id, title, kind, status) \
         VALUES ($1, 'Secret Doc', 'document', 'ready') RETURNING id",
    )
    .bind(s.tenant_a)
    .fetch_one(&s.pool)
    .await?;

    // Confirm tenant A can see their own document.
    let count_a: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM documents WHERE id = $1")
        .bind(doc_a)
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(count_a, 1, "tenant A must see its own document");

    // ── Switch to tenant B ──
    switch_tenant(&s.pool, s.tenant_b).await;

    // Direct id lookup must return zero rows (RLS filters out tenant A's row).
    let count_b: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM documents WHERE id = $1")
        .bind(doc_a)
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(
        count_b, 0,
        "RLS violated: tenant B read tenant A's document via direct id"
    );

    // Verify tenant B can still see its own (empty) document set.
    let total_b: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM documents")
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(
        total_b, 0,
        "tenant B should see zero documents (RLS scoped to tenant B only)"
    );

    Ok(())
}

/// Tenant A inserts a document → tenant B attempts a direct `SELECT *` and gets
/// nothing. Also exercises the `files` table under the same RLS policies.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn tenant_b_cannot_read_tenant_a_files() -> anyhow::Result<()> {
    let s = setup_two_tenants().await?;

    // ── Insert a document + file as tenant A ──
    switch_tenant(&s.pool, s.tenant_a).await;
    let doc_a: i64 = sqlx::query_scalar(
        "INSERT INTO documents (tenant_id, title, kind, status) \
         VALUES ($1, 'Doc A', 'document', 'ready') RETURNING id",
    )
    .bind(s.tenant_a)
    .fetch_one(&s.pool)
    .await?;

    let rec = make_file_rec(s.tenant_a, doc_a, 1, "page-a1");
    let file_id = s.store.upsert_file(&rec).await?;
    assert!(file_id > 0);

    // Confirm the file is visible as tenant A.
    let visible_a: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM files WHERE id = $1")
        .bind(file_id)
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(visible_a, 1);

    // ── Switch to tenant B ──
    switch_tenant(&s.pool, s.tenant_b).await;

    // Direct id lookup → RLS blocks it.
    let visible_b: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM files WHERE id = $1")
        .bind(file_id)
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(
        visible_b, 0,
        "RLS violated: tenant B read tenant A's file via direct id"
    );

    // Broad scan → empty for tenant B.
    let total_files: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM files")
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(total_files, 0);

    Ok(())
}

// ── Test 2: Tag isolation ─────────────────────────────────────────────────────

/// Tags and tag aliases inserted by tenant A are invisible to tenant B.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn tags_are_tenant_scoped() -> anyhow::Result<()> {
    let s = setup_two_tenants().await?;

    // ── Create tags as tenant A ──
    switch_tenant(&s.pool, s.tenant_a).await;
    let emb_a = vec![0.1f32; 1024];
    let tag_a1 = s.store.upsert_tag(s.tenant_a, "alpha", &emb_a).await?;
    let tag_a2 = s.store.upsert_tag(s.tenant_a, "beta", &emb_a).await?;
    s.store
        .insert_tag_alias(s.tenant_a, "first", tag_a1)
        .await?;
    s.store
        .insert_tag_alias(s.tenant_a, "second", tag_a2)
        .await?;

    // Confirm tenant A sees both tags and aliases.
    let tags_a: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tags")
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(tags_a, 2, "tenant A should see 2 tags");

    let aliases_a: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tag_aliases")
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(aliases_a, 2, "tenant A should see 2 aliases");

    // ── Switch to tenant B ──
    switch_tenant(&s.pool, s.tenant_b).await;

    // Tenant B sees no tags.
    let tags_b: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tags")
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(tags_b, 0, "RLS violated: tenant B sees tenant A's tags");

    // Tenant B sees no aliases.
    let aliases_b: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tag_aliases")
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(
        aliases_b, 0,
        "RLS violated: tenant B sees tenant A's tag aliases"
    );

    // Tenant B's alias lookup returns None for tenant A's alias.
    let lookup = s.store.lookup_alias(s.tenant_b, "first").await?;
    assert!(
        lookup.is_none(),
        "RLS violated: tenant B resolved tenant A's tag alias"
    );

    // Tenant B's find_similar_tags returns empty.
    let similar = s.store.find_similar_tags(s.tenant_b).await?;
    assert!(
        similar.is_empty(),
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

    // ── Insert a usage event as tenant A ──
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
        created_at: Utc::now(),
    };
    switch_tenant(&s.pool, s.tenant_a).await;
    let ev_id = s.store.insert_usage_event(&event_a).await?;
    assert!(ev_id > 0);

    // Confirm tenant A sees it.
    let count_a: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM usage_events WHERE id = $1")
        .bind(ev_id)
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(count_a, 1);

    // ── Switch to tenant B ──
    switch_tenant(&s.pool, s.tenant_b).await;

    // Direct id lookup → RLS blocks it.
    let count_b: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM usage_events WHERE id = $1")
        .bind(ev_id)
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(
        count_b, 0,
        "RLS violated: tenant B read tenant A's usage event"
    );

    // Tenant B sees zero usage events total.
    let total_b: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM usage_events")
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(total_b, 0);

    Ok(())
}

// ── Test 4: Hybrid search isolation ───────────────────────────────────────────

/// Tenant A inserts a document with chunks → tenant B's hybrid search returns
/// nothing (the chunks are invisible).
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn hybrid_search_is_tenant_scoped() -> anyhow::Result<()> {
    let s = setup_two_tenants().await?;

    // ── Insert document + chunks as tenant A ──
    let doc_a: i64 = sqlx::query_scalar(
        "INSERT INTO documents (tenant_id, title, kind, status) \
         VALUES ($1, 'Searchable Doc', 'document', 'ready') RETURNING id",
    )
    .bind(s.tenant_a)
    .fetch_one(&s.pool)
    .await?;

    let rec = make_file_rec(s.tenant_a, doc_a, 1, "search-page");
    let file_id = s.store.upsert_file(&rec).await?;

    let chunks: Vec<Chunk> = vec![
        make_chunk(s.tenant_a, doc_a, file_id, 0, "unique pangolin content"),
        make_chunk(s.tenant_a, doc_a, file_id, 1, "more secret data here"),
    ];
    s.store.upsert_chunks(file_id, &chunks).await?;

    // ── Tenant A can search and find the document ──
    switch_tenant(&s.pool, s.tenant_a).await;
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
    assert_eq!(
        hits_a[0].document_id, doc_a,
        "tenant A's search should return the correct document"
    );

    // ── Tenant B cannot find the same content ──
    switch_tenant(&s.pool, s.tenant_b).await;
    let hits_b = s
        .store
        .hybrid_search(s.tenant_b, &q, &spike_vector(0))
        .await?;
    assert!(
        hits_b.is_empty(),
        "RLS violated: tenant B found tenant A's document via hybrid search"
    );

    // Vector-only search should also return nothing for tenant B.
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

/// When both tenants have documents, each tenant's search returns only their own
/// data — no cross-contamination.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn each_tenant_only_sees_own_data_in_search() -> anyhow::Result<()> {
    let s = setup_two_tenants().await?;

    // ── Tenant A: insert document with distinctive text ──
    switch_tenant(&s.pool, s.tenant_a).await;
    let doc_a: i64 = sqlx::query_scalar(
        "INSERT INTO documents (tenant_id, title, kind, status) \
         VALUES ($1, 'Alpha Report', 'document', 'ready') RETURNING id",
    )
    .bind(s.tenant_a)
    .fetch_one(&s.pool)
    .await?;

    let rec_a = make_file_rec(s.tenant_a, doc_a, 1, "alpha-page");
    let file_a = s.store.upsert_file(&rec_a).await?;
    let chunks_a = vec![make_chunk(
        s.tenant_a,
        doc_a,
        file_a,
        0,
        "alpha secret report about finances",
    )];
    s.store.upsert_chunks(file_a, &chunks_a).await?;

    // ── Tenant B: insert a different document ──
    switch_tenant(&s.pool, s.tenant_b).await;
    let doc_b: i64 = sqlx::query_scalar(
        "INSERT INTO documents (tenant_id, title, kind, status) \
         VALUES ($1, 'Beta Notes', 'document', 'ready') RETURNING id",
    )
    .bind(s.tenant_b)
    .fetch_one(&s.pool)
    .await?;

    let rec_b = make_file_rec(s.tenant_b, doc_b, 1, "beta-page");
    let file_b = s.store.upsert_file(&rec_b).await?;
    let chunks_b = vec![make_chunk(
        s.tenant_b,
        doc_b,
        file_b,
        0,
        "beta public meeting notes",
    )];
    s.store.upsert_chunks(file_b, &chunks_b).await?;

    // ── Tenant A search: finds only alpha ──
    switch_tenant(&s.pool, s.tenant_a).await;
    let q_alpha = Query {
        text: "finances".to_string(),
        filters: QueryFilters::default(),
        top_k: 10,
    };
    let hits_a = s
        .store
        .hybrid_search(s.tenant_a, &q_alpha, &spike_vector(0))
        .await?;
    let doc_ids_a: Vec<i64> = hits_a.iter().map(|h| h.document_id).collect();
    assert!(
        doc_ids_a.contains(&doc_a),
        "tenant A should find its own 'finances' document"
    );
    assert!(
        !doc_ids_a.contains(&doc_b),
        "RLS violated: tenant A sees tenant B's document in search"
    );

    // ── Tenant B search: finds only beta ──
    switch_tenant(&s.pool, s.tenant_b).await;
    let q_beta = Query {
        text: "meeting".to_string(),
        filters: QueryFilters::default(),
        top_k: 10,
    };
    let hits_b = s
        .store
        .hybrid_search(s.tenant_b, &q_beta, &spike_vector(0))
        .await?;
    let doc_ids_b: Vec<i64> = hits_b.iter().map(|h| h.document_id).collect();
    assert!(
        doc_ids_b.contains(&doc_b),
        "tenant B should find its own 'meeting' document"
    );
    assert!(
        !doc_ids_b.contains(&doc_a),
        "RLS violated: tenant B sees tenant A's document in search"
    );

    // ── Tenant A cannot find tenant B's content via keyword ──
    switch_tenant(&s.pool, s.tenant_a).await;
    let q_beta_from_a = Query {
        text: "meeting".to_string(),
        filters: QueryFilters::default(),
        top_k: 10,
    };
    let hits_a2 = s
        .store
        .hybrid_search(s.tenant_a, &q_beta_from_a, &spike_vector(0))
        .await?;
    assert!(
        hits_a2.iter().all(|h| h.document_id != doc_b),
        "RLS violated: tenant A found tenant B's 'meeting' document"
    );

    Ok(())
}

// ── Test 6: Transactional ingest isolation ────────────────────────────────────

/// `transactional_ingest` upserts a document as tenant A → tenant B cannot see any
/// of the ingested rows (document, files, tags, chunks).
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn transactional_ingest_respects_tenant_boundary() -> anyhow::Result<()> {
    let s = setup_two_tenants().await?;

    // ── Ingest as tenant A ──
    use kb_core::document::Document;

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
    };

    let rec = make_file_rec(s.tenant_a, 0, 1, "isolated-page");
    let tag_id = s
        .store
        .upsert_tag(s.tenant_a, "isolated-tag", &vec![0.5f32; 1024])
        .await?;

    let chunk = make_chunk(s.tenant_a, 0, 0, 0, "isolated chunk text");

    // Must be in tenant A context for the tag upsert above; transactional_ingest
    // sets its own tenant internally, but we still need a valid context.
    switch_tenant(&s.pool, s.tenant_a).await;
    let doc_id = s
        .store
        .transactional_ingest(&doc, &[rec], &[tag_id], &[vec![chunk]])
        .await?;
    assert!(doc_id > 0);

    // Confirm tenant A sees everything.
    let docs_a: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM documents")
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(docs_a, 1);
    let files_a: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM files")
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(files_a, 1);
    let chunks_a: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM chunks")
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(chunks_a, 1);
    let tags_a: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tags")
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(tags_a, 1);
    let dtags_a: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM document_tags")
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(dtags_a, 1);

    // ── Switch to tenant B ──
    switch_tenant(&s.pool, s.tenant_b).await;

    // Tenant B sees nothing across all tables.
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM documents")
            .fetch_one(&s.pool)
            .await?,
        0,
        "RLS: tenant B sees documents"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM files")
            .fetch_one(&s.pool)
            .await?,
        0,
        "RLS: tenant B sees files"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM chunks")
            .fetch_one(&s.pool)
            .await?,
        0,
        "RLS: tenant B sees chunks"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM tags")
            .fetch_one(&s.pool)
            .await?,
        0,
        "RLS: tenant B sees tags"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM document_tags")
            .fetch_one(&s.pool)
            .await?,
        0,
        "RLS: tenant B sees document_tags"
    );

    Ok(())
}

// ── Test 7: Users are tenant-scoped ───────────────────────────────────────────

/// Users belonging to tenant A are invisible to tenant B.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn users_are_tenant_scoped() -> anyhow::Result<()> {
    let s = setup_two_tenants().await?;

    // ── Tenant A context: user_a exists ──
    switch_tenant(&s.pool, s.tenant_a).await;
    let users_a: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(users_a, 1, "tenant A should see its own user");

    // Direct lookup works for own user.
    let email_a: Option<String> = sqlx::query_scalar("SELECT email FROM users WHERE id = $1")
        .bind(s.user_a)
        .fetch_optional(&s.pool)
        .await?;
    assert_eq!(email_a.as_deref(), Some("alice@a.example"));

    // ── Switch to tenant B ──
    switch_tenant(&s.pool, s.tenant_b).await;

    // Tenant B cannot see tenant A's user via direct id.
    let user_a_from_b: Option<String> = sqlx::query_scalar("SELECT email FROM users WHERE id = $1")
        .bind(s.user_a)
        .fetch_optional(&s.pool)
        .await?;
    assert!(
        user_a_from_b.is_none(),
        "RLS violated: tenant B read tenant A's user via direct id"
    );

    // Tenant B sees only its own user.
    let users_b: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(users_b, 1, "tenant B should see its own user");

    let email_b: Option<String> = sqlx::query_scalar("SELECT email FROM users WHERE id = $1")
        .bind(s.user_b)
        .fetch_optional(&s.pool)
        .await?;
    assert_eq!(email_b.as_deref(), Some("bob@b.example"));

    Ok(())
}

// ── Test 8: Jobs are tenant-scoped ────────────────────────────────────────────

/// A job enqueued by tenant A is invisible to tenant B.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn jobs_are_tenant_scoped() -> anyhow::Result<()> {
    let s = setup_two_tenants().await?;

    // ── Enqueue a job as tenant A ──
    switch_tenant(&s.pool, s.tenant_a).await;
    let job_a: i64 = sqlx::query_scalar(
        "INSERT INTO jobs (tenant_id, kind, status) \
         VALUES ($1, 'ingest', 'queued') RETURNING id",
    )
    .bind(s.tenant_a)
    .fetch_one(&s.pool)
    .await?;

    // Confirm tenant A sees it.
    let count_a: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE id = $1")
        .bind(job_a)
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(count_a, 1);

    // ── Switch to tenant B ──
    switch_tenant(&s.pool, s.tenant_b).await;

    // Direct id lookup returns nothing for tenant B.
    let count_b: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE id = $1")
        .bind(job_a)
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(
        count_b, 0,
        "RLS violated: tenant B read tenant A's job via direct id"
    );

    // Tenant B sees zero jobs.
    let total_b: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs")
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(total_b, 0);

    Ok(())
}
