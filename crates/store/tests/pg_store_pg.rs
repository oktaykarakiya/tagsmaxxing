// SPDX-License-Identifier: AGPL-3.0-or-later

//! Integration tests for `PgStore` against a real pgvector Postgres container.
//!
//! These tests need **Podman** (this project targets Podman exclusively) and network access
//! to pull `pgvector/pgvector`. They are gated `#[ignore]` so `just ci` stays green when
//! no container runtime is available. Run them explicitly against the Podman socket:
//!
//! ```text
//! systemctl --user start podman.socket
//! DOCKER_HOST=unix://$XDG_RUNTIME_DIR/podman/podman.sock \
//!   cargo test -p kb-store --test pg_store_pg -- --ignored
//! ```
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use kb_core::chunk::Chunk;
use kb_core::file::FileRecord;
use kb_core::hash::Sha256;
use kb_core::role::Role;
use kb_core::status::ProcessingStatus;
use kb_core::store::Store;
use kb_core::usage::UsageEvent;
use kb_store::PgStore;
use sqlx::PgPool;
use sqlx::Row;

/// Concrete setup type so each test gets a clean database without fighting the
/// `TestResult` alias (which maps to `Result<(), …>`).
struct Setup {
    store: Arc<PgStore>,
    /// The **privileged** pool for direct seeding + verification reads (BYPASSRLS). Tenant-data
    /// writes go through the store, which uses its kb_app pool under RLS.
    pool: PgPool,
    tenant_id: i64,
    document_id: i64,
}

// ── Fixture ──────────────────────────────────────────────────────────────────

/// Provision a fresh DB in the shared container, run migrations (two-role), insert a tenant +
/// document via the privileged pool, and return everything the tests need.
async fn setup_store() -> anyhow::Result<Setup> {
    let db = kb_testsupport::fresh_db().await?;
    let store = Arc::new(PgStore::with_roles(&db.admin_url, &db.app_url));
    store.connect().await?;

    let pool = store.pool()?;

    let tenant_id: i64 =
        sqlx::query_scalar("INSERT INTO tenants (slug, name) VALUES ('t', 'Test') RETURNING id")
            .fetch_one(&pool)
            .await?;

    let document_id: i64 = sqlx::query_scalar(
        "INSERT INTO documents (tenant_id, kind) VALUES ($1, 'document') RETURNING id",
    )
    .bind(tenant_id)
    .fetch_one(&pool)
    .await?;

    Ok(Setup {
        store,
        pool,
        tenant_id,
        document_id,
    })
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn make_file_record(
    tenant_id: i64,
    document_id: i64,
    sha256: Sha256,
    blob_key: &str,
) -> FileRecord {
    FileRecord {
        id: 0,
        tenant_id,
        document_id,
        page_no: 1,
        page_label: Some("test-page".into()),
        sha256,
        blob_key: blob_key.into(),
        path: Some("/tmp/test.txt".into()),
        mime: Some("text/plain".into()),
        size_bytes: Some(100),
        meta: serde_json::json!({"source": "test"}),
        status: ProcessingStatus::Pending,
        ingested_at: Utc::now(),
    }
}

fn make_chunk(
    tenant_id: i64,
    document_id: i64,
    file_id: i64,
    idx: i32,
    content: &str,
    embedding: Vec<f32>,
) -> Chunk {
    Chunk {
        id: 0,
        tenant_id,
        document_id,
        file_id,
        page_no: Some(1),
        idx,
        content: content.into(),
        ts_offset: None,
        embedding,
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

/// Establish a pool and run migrations via `PgStore::connect`.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn connect_establishes_pool_and_runs_migrations() -> anyhow::Result<()> {
    let s = setup_store().await?;

    // The pool is usable: query pgvector extension presence.
    let has_vector: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector')")
            .fetch_one(&s.pool)
            .await?;
    assert!(has_vector);

    Ok(())
}

/// `upsert_file` inserts a new row and returns its id.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn upsert_file_inserts_new_row() -> anyhow::Result<()> {
    let s = setup_store().await?;

    let sha = Sha256::from_bytes([1u8; 32]);
    let rec = make_file_record(s.tenant_id, s.document_id, sha, "key-insert");
    let id = s.store.upsert_file(&rec).await?;
    assert!(id > 0, "returned id should be positive");

    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM files WHERE id = $1 AND blob_key = 'key-insert'")
            .bind(id)
            .fetch_one(&s.pool)
            .await?;
    assert_eq!(count, 1);

    Ok(())
}

/// `upsert_file` is idempotent on `(tenant_id, sha256)` conflict.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn upsert_file_is_idempotent_on_sha256_conflict() -> anyhow::Result<()> {
    let s = setup_store().await?;

    let sha = Sha256::from_bytes([2u8; 32]);
    let rec = make_file_record(s.tenant_id, s.document_id, sha, "key-idempotent");

    let id1 = s.store.upsert_file(&rec).await?;
    let id2 = s.store.upsert_file(&rec).await?;
    assert_eq!(id1, id2, "idempotent re-insert must return the same id");

    // Only one row exists.
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM files WHERE tenant_id = $1 AND sha256 = $2")
            .bind(s.tenant_id)
            .bind(sha.as_bytes().as_slice())
            .fetch_one(&s.pool)
            .await?;
    assert_eq!(count, 1);

    Ok(())
}

/// `upsert_file` on conflict updates the non-key columns.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn upsert_file_updates_on_conflict() -> anyhow::Result<()> {
    let s = setup_store().await?;

    let sha = Sha256::from_bytes([3u8; 32]);
    let mut rec = make_file_record(s.tenant_id, s.document_id, sha, "key-update");

    let id1 = s.store.upsert_file(&rec).await?;

    // Re-upsert the SAME file (same tenant + sha256 + document_id) with changed non-key
    // fields. Under the BUG-INGEST-02 key (tenant_id, sha256, document_id), document_id is
    // part of the key, so it stays the same to hit the UPDATE branch — the same bytes under
    // a *different* document_id is intentionally a new file row (see transactional_ingest).
    rec.page_label = Some("updated-label".into());
    rec.meta = serde_json::json!({"updated": true});
    rec.status = ProcessingStatus::Ready;

    let id2 = s.store.upsert_file(&rec).await?;
    assert_eq!(id1, id2);

    let row =
        sqlx::query("SELECT document_id, page_label, meta::text, status FROM files WHERE id = $1")
            .bind(id1)
            .fetch_one(&s.pool)
            .await?;

    let db_doc_id: i64 = row.try_get("document_id")?;
    let db_label: String = row.try_get("page_label")?;
    let db_meta: String = row.try_get("meta")?;
    let db_status: String = row.try_get("status")?;

    assert_eq!(db_doc_id, s.document_id);
    assert_eq!(db_label, "updated-label");
    assert!(db_meta.contains("updated"));
    assert_eq!(db_status, "ready");

    Ok(())
}

/// `upsert_chunks` replaces chunks atomically.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn upsert_chunks_replaces_atomically() -> anyhow::Result<()> {
    let s = setup_store().await?;

    let sha = Sha256::from_bytes([4u8; 32]);
    let rec = make_file_record(s.tenant_id, s.document_id, sha, "key-chunks");
    let file_id = s.store.upsert_file(&rec).await?;

    let emb = vec![0.1f32; kb_store::EMBED_DIM];
    let chunks: Vec<Chunk> = (0..3)
        .map(|i| {
            make_chunk(
                s.tenant_id,
                s.document_id,
                file_id,
                i,
                &format!("chunk-{i}"),
                emb.clone(),
            )
        })
        .collect();

    s.store.upsert_chunks(file_id, &chunks).await?;

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM chunks WHERE file_id = $1")
        .bind(file_id)
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(count, 3);

    // Second upsert with different content — old rows must be gone.
    let new_chunks = vec![
        make_chunk(
            s.tenant_id,
            s.document_id,
            file_id,
            0,
            "new-chunk",
            vec![0.2f32; kb_store::EMBED_DIM],
        ),
        make_chunk(
            s.tenant_id,
            s.document_id,
            file_id,
            1,
            "new-chunk-2",
            vec![0.3f32; kb_store::EMBED_DIM],
        ),
    ];
    s.store.upsert_chunks(file_id, &new_chunks).await?;

    let count2: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM chunks WHERE file_id = $1")
        .bind(file_id)
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(count2, 2);

    let rows: Vec<String> =
        sqlx::query_scalar("SELECT content FROM chunks WHERE file_id = $1 ORDER BY idx")
            .bind(file_id)
            .fetch_all(&s.pool)
            .await?;
    assert_eq!(rows, vec!["new-chunk", "new-chunk-2"]);

    Ok(())
}

/// `upsert_chunks` with an empty slice deletes all chunks for the file.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn upsert_chunks_empty_slice_deletes_all() -> anyhow::Result<()> {
    let s = setup_store().await?;

    let sha = Sha256::from_bytes([5u8; 32]);
    let rec = make_file_record(s.tenant_id, s.document_id, sha, "key-empty");
    let file_id = s.store.upsert_file(&rec).await?;

    // Insert some chunks first.
    let chunks: Vec<Chunk> = (0..2)
        .map(|i| {
            make_chunk(
                s.tenant_id,
                s.document_id,
                file_id,
                i,
                &format!("temp-{i}"),
                vec![0.0f32; kb_store::EMBED_DIM],
            )
        })
        .collect();
    s.store.upsert_chunks(file_id, &chunks).await?;

    let before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM chunks WHERE file_id = $1")
        .bind(file_id)
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(before, 2);

    // Empty upsert clears all chunks.
    s.store.upsert_chunks(file_id, &[]).await?;

    let after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM chunks WHERE file_id = $1")
        .bind(file_id)
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(after, 0);

    Ok(())
}

/// `upsert_chunks` preserves the embedding dimension (pgvector 2560).
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn upsert_chunks_preserves_embedding_dimension() -> anyhow::Result<()> {
    let s = setup_store().await?;

    let sha = Sha256::from_bytes([6u8; 32]);
    let rec = make_file_record(s.tenant_id, s.document_id, sha, "key-emb");
    let file_id = s.store.upsert_file(&rec).await?;

    let emb = (0..kb_store::EMBED_DIM)
        .map(|i| i as f32 * 0.001)
        .collect::<Vec<f32>>();
    let chunks = vec![make_chunk(
        s.tenant_id,
        s.document_id,
        file_id,
        0,
        "emb-test",
        emb,
    )];
    s.store.upsert_chunks(file_id, &chunks).await?;

    // Query the vector dimension with pgvector's `vector_dims` function.
    let dim: i32 =
        sqlx::query_scalar("SELECT vector_dims(embedding) FROM chunks WHERE file_id = $1")
            .bind(file_id)
            .fetch_one(&s.pool)
            .await?;
    assert_eq!(dim, 2560);

    Ok(())
}

// ── Monthly token rollup (P14-T1) ───────────────────────────────────────────

/// Build a usage event for `tenant_id` with the given token counts and timestamp.
fn make_usage_event(
    tenant_id: i64,
    user_id: Option<i64>,
    prompt: i32,
    completion: i32,
    created_at: DateTime<Utc>,
) -> UsageEvent {
    UsageEvent {
        id: 0,
        tenant_id,
        user_id,
        model: "test-model".into(),
        role: Role::Embed,
        backend_id: None,
        prompt_tokens: Some(prompt),
        completion_tokens: Some(completion),
        latency_ms: None,
        cost_micros: None,
        created_at,
    }
}

/// `insert_usage_event` upserts the monthly rollup in the same transaction:
/// two events in the same UTC month sum into one `(tenant, period_month)` row.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn insert_usage_event_rolls_up_same_month() -> anyhow::Result<()> {
    let s = setup_store().await?;
    let when = Utc
        .with_ymd_and_hms(2026, 6, 10, 12, 0, 0)
        .single()
        .unwrap();

    // prompt 10 + completion 5 = 15, then 20 + 0 = 20  ⇒  35 in June 2026.
    s.store
        .insert_usage_event(&make_usage_event(s.tenant_id, Some(1), 10, 5, when))
        .await?;
    s.store
        .insert_usage_event(&make_usage_event(s.tenant_id, Some(1), 20, 0, when))
        .await?;

    // Exactly one rollup row, summing both events. Read via the privileged pool.
    let (period, tokens): (chrono::NaiveDate, i64) = sqlx::query_as(
        "SELECT period_month, tokens_used FROM tenant_monthly_usage WHERE tenant_id = $1",
    )
    .bind(s.tenant_id)
    .fetch_one(&s.pool)
    .await?;
    assert_eq!(period, chrono::NaiveDate::from_ymd_opt(2026, 6, 1).unwrap());
    assert_eq!(tokens, 35);

    let row_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM tenant_monthly_usage WHERE tenant_id = $1")
            .bind(s.tenant_id)
            .fetch_one(&s.pool)
            .await?;
    assert_eq!(row_count, 1, "same-month events share one rollup row");

    Ok(())
}

/// Events in different UTC months land in separate rollup rows.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn insert_usage_event_separates_distinct_months() -> anyhow::Result<()> {
    let s = setup_store().await?;
    let june = Utc.with_ymd_and_hms(2026, 6, 15, 0, 0, 0).single().unwrap();
    let july = Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).single().unwrap();

    s.store
        .insert_usage_event(&make_usage_event(s.tenant_id, None, 100, 0, june))
        .await?;
    s.store
        .insert_usage_event(&make_usage_event(s.tenant_id, None, 7, 0, july))
        .await?;

    let rows: Vec<(chrono::NaiveDate, i64)> = sqlx::query_as(
        "SELECT period_month, tokens_used FROM tenant_monthly_usage \
         WHERE tenant_id = $1 ORDER BY period_month",
    )
    .bind(s.tenant_id)
    .fetch_all(&s.pool)
    .await?;
    assert_eq!(rows.len(), 2, "two months ⇒ two rollup rows");
    assert_eq!(
        rows[0].0,
        chrono::NaiveDate::from_ymd_opt(2026, 6, 1).unwrap()
    );
    assert_eq!(rows[0].1, 100);
    assert_eq!(
        rows[1].0,
        chrono::NaiveDate::from_ymd_opt(2026, 7, 1).unwrap()
    );
    assert_eq!(rows[1].1, 7);

    Ok(())
}

/// A zero-token event records the usage event but creates no rollup row.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn insert_usage_event_zero_tokens_skips_rollup() -> anyhow::Result<()> {
    let s = setup_store().await?;
    let when = Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).single().unwrap();

    s.store
        .insert_usage_event(&make_usage_event(s.tenant_id, None, 0, 0, when))
        .await?;

    let usage_rows: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM usage_events WHERE tenant_id = $1")
            .bind(s.tenant_id)
            .fetch_one(&s.pool)
            .await?;
    assert_eq!(usage_rows, 1, "the usage event is still recorded");

    let rollup_rows: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM tenant_monthly_usage WHERE tenant_id = $1")
            .bind(s.tenant_id)
            .fetch_one(&s.pool)
            .await?;
    assert_eq!(rollup_rows, 0, "zero-token calls create no rollup row");

    Ok(())
}

/// `get_current_month_token_rollup` returns the running total for the current
/// UTC month, and 0 before any usage is recorded.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn get_current_month_token_rollup_reads_running_total() -> anyhow::Result<()> {
    let s = setup_store().await?;

    // No usage yet ⇒ 0.
    let before = s.store.get_current_month_token_rollup(s.tenant_id).await?;
    assert_eq!(before, 0);

    // Two events in the CURRENT month sum into the O(1) read.
    let now = Utc::now();
    s.store
        .insert_usage_event(&make_usage_event(s.tenant_id, Some(2), 30, 0, now))
        .await?;
    s.store
        .insert_usage_event(&make_usage_event(s.tenant_id, Some(2), 12, 0, now))
        .await?;

    let after = s.store.get_current_month_token_rollup(s.tenant_id).await?;
    assert_eq!(after, 42);

    Ok(())
}

/// P14-T8: `reconcile_month_token_rollup` heals drift between the O(1) rollup and
/// the authoritative `usage_events` SUM for the current UTC month.
///
/// Drift is injected by inserting `usage_events` rows **directly** via the
/// privileged pool (BYPASSRLS), bypassing `insert_usage_event` so the rollup is
/// never updated. After reconcile, the rollup row must equal the SUM, the call
/// must return that SUM, and a second reconcile must be a no-op.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn reconcile_month_token_rollup_heals_injected_drift() -> anyhow::Result<()> {
    let s = setup_store().await?;
    let now = Utc::now();

    // One legitimate event through the store → rollup = 15 (10 + 5).
    s.store
        .insert_usage_event(&make_usage_event(s.tenant_id, Some(1), 10, 5, now))
        .await?;
    assert_eq!(
        s.store.get_current_month_token_rollup(s.tenant_id).await?,
        15
    );

    // Inject drift: two raw usage_events (100 and 40 tokens) inserted straight
    // into the table, so the rollup does NOT see them. Authoritative SUM is now
    // 15 + 100 + 40 = 155, but the rollup still reads 15.
    for prompt in [100_i32, 40_i32] {
        sqlx::query(
            "INSERT INTO usage_events \
             (tenant_id, model, role, prompt_tokens, completion_tokens, created_at) \
             VALUES ($1, 'raw-seed', 'embed', $2, 0, $3)",
        )
        .bind(s.tenant_id)
        .bind(prompt)
        .bind(now)
        .execute(&s.pool)
        .await?;
    }
    assert_eq!(
        s.store.get_token_usage(s.tenant_id).await?,
        155,
        "authoritative SUM should include the raw-seeded events"
    );
    assert_eq!(
        s.store.get_current_month_token_rollup(s.tenant_id).await?,
        15,
        "rollup should still be stale before reconcile"
    );

    // Reconcile: returns the authoritative SUM and corrects the rollup row.
    let reconciled = s.store.reconcile_month_token_rollup(s.tenant_id).await?;
    assert_eq!(reconciled, 155, "reconcile returns the authoritative SUM");
    assert_eq!(
        s.store.get_current_month_token_rollup(s.tenant_id).await?,
        155,
        "rollup must equal the SUM after reconcile"
    );

    // Exactly one rollup row for the current month (corrected in place, not
    // duplicated). Read via the privileged pool.
    let row_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM tenant_monthly_usage WHERE tenant_id = $1")
            .bind(s.tenant_id)
            .fetch_one(&s.pool)
            .await?;
    assert_eq!(
        row_count, 1,
        "reconcile corrects in place, no duplicate row"
    );

    // Idempotent: a second reconcile with no new drift returns the same value
    // and leaves the rollup unchanged.
    let again = s.store.reconcile_month_token_rollup(s.tenant_id).await?;
    assert_eq!(again, 155);
    assert_eq!(
        s.store.get_current_month_token_rollup(s.tenant_id).await?,
        155
    );

    Ok(())
}
