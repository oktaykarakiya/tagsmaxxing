//! Integration tests for the pending-ingest staging layer (P15-T1) against a
//! real pgvector Postgres container.
//!
//! These tests need **Podman** (this project targets Podman exclusively) and
//! network access to pull `pgvector/pgvector`. They are gated `#[ignore]` so
//! `just ci` stays green when no container runtime is available. Run them
//! explicitly against the Podman socket:
//!
//! ```text
//! systemctl --user start podman.socket
//! DOCKER_HOST=unix://$XDG_RUNTIME_DIR/podman/podman.sock \
//!   cargo test -p kb-store --test ingest_staging_pg -- --ignored
//! ```
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use kb_core::document::Document;
use kb_core::file::FileRecord;
use kb_core::hash::Sha256;
use kb_core::kind::DocKind;
use kb_core::status::ProcessingStatus;
use kb_store::PgStore;
use sqlx::PgPool;

struct Setup {
    store: Arc<PgStore>,
    pool: PgPool,
    tenant_id: i64,
}

/// Provision a fresh DB, run migrations (two-role), insert a tenant via the
/// privileged pool.
async fn setup() -> anyhow::Result<Setup> {
    let db = kb_testsupport::fresh_db().await?;
    let store = Arc::new(PgStore::with_roles(&db.admin_url, &db.app_url));
    store.connect().await?;
    let pool = store.pool()?;

    let tenant_id: i64 =
        sqlx::query_scalar("INSERT INTO tenants (slug, name) VALUES ('t', 'Test') RETURNING id")
            .fetch_one(&pool)
            .await?;

    Ok(Setup {
        store,
        pool,
        tenant_id,
    })
}

fn doc(tenant_id: i64, note: &str) -> Document {
    Document {
        id: 0,
        tenant_id,
        title: None,
        summary: None,
        user_note: Some(note.to_string()),
        kind: DocKind::Document,
        meta: serde_json::json!({"src": "staging-test"}),
        page_count: 1,
        status: ProcessingStatus::Pending,
        created_at: chrono::Utc::now(),
        local_only: true,
    }
}

/// A file record whose sha256 is derived from `seed` (deterministic, distinct
/// per seed) with a matching blob key and path.
fn file(tenant_id: i64, seed: u8, page_no: i32) -> FileRecord {
    let hex: String = format!("{:02x}", seed).repeat(32);
    FileRecord {
        id: 0,
        tenant_id,
        document_id: 0,
        page_no,
        page_label: Some(format!("p{page_no}")),
        sha256: Sha256::from_hex(&hex).unwrap(),
        blob_key: hex.clone(),
        path: Some(format!("file-{seed}.txt")),
        mime: Some("text/plain".into()),
        size_bytes: Some(42),
        meta: serde_json::json!({}),
        status: ProcessingStatus::Pending,
        ingested_at: chrono::Utc::now(),
    }
}

/// Staging inserts a pending document + pending files and persists the
/// user_note/local_only the worker later recovers.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn staging_creates_pending_document_and_files() -> anyhow::Result<()> {
    let s = setup().await?;

    let pending = s
        .store
        .create_pending_ingest(&doc(s.tenant_id, "my note"), &[file(s.tenant_id, 0x11, 1)])
        .await?;
    assert!(!pending.reused);
    assert_eq!(pending.file_ids.len(), 1);

    // Document row is pending with the note + local_only persisted.
    let d = s
        .store
        .get_document(s.tenant_id, pending.document_id)
        .await?
        .expect("staged document must exist");
    assert_eq!(d.status, ProcessingStatus::Pending);
    assert_eq!(d.user_note.as_deref(), Some("my note"));
    assert!(d.local_only, "local_only must be persisted at staging");
    assert!(d.title.is_none(), "title unknown before processing");

    // File rows are pending and carry the blob key.
    let files = s
        .store
        .get_files_for_document(s.tenant_id, pending.document_id)
        .await?;
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].status, ProcessingStatus::Pending);
    assert_eq!(files[0].blob_key, "11".repeat(32));

    Ok(())
}

/// Re-staging an identical upload (same (sha256, path) set) reuses the existing
/// document and refreshes the note, instead of creating a twin.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn staging_identical_upload_reuses_document() -> anyhow::Result<()> {
    let s = setup().await?;
    let files = [file(s.tenant_id, 0x22, 1), file(s.tenant_id, 0x33, 2)];

    let first = s
        .store
        .create_pending_ingest(&doc(s.tenant_id, "first note"), &files)
        .await?;
    let second = s
        .store
        .create_pending_ingest(&doc(s.tenant_id, "second note"), &files)
        .await?;

    assert!(second.reused, "identical upload must reuse the document");
    assert_eq!(second.document_id, first.document_id);

    // Only one document exists; the note was refreshed.
    let d = s
        .store
        .get_document(s.tenant_id, first.document_id)
        .await?
        .unwrap();
    assert_eq!(d.user_note.as_deref(), Some("second note"));
    let n_docs: i64 = sqlx::query_scalar("SELECT count(*) FROM documents WHERE tenant_id=$1")
        .bind(s.tenant_id)
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(n_docs, 1);

    // A DIFFERENT file set makes a new document.
    let third = s
        .store
        .create_pending_ingest(&doc(s.tenant_id, "other"), &[file(s.tenant_id, 0x44, 1)])
        .await?;
    assert!(!third.reused);
    assert_ne!(third.document_id, first.document_id);

    Ok(())
}

/// `set_document_status` aligns the document and its non-ready files.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn set_document_status_transitions() -> anyhow::Result<()> {
    let s = setup().await?;
    let pending = s
        .store
        .create_pending_ingest(&doc(s.tenant_id, "n"), &[file(s.tenant_id, 0x55, 1)])
        .await?;

    s.store
        .set_document_status(s.tenant_id, pending.document_id, ProcessingStatus::Failed)
        .await?;
    let d = s
        .store
        .get_document(s.tenant_id, pending.document_id)
        .await?
        .unwrap();
    assert_eq!(d.status, ProcessingStatus::Failed);
    let files = s
        .store
        .get_files_for_document(s.tenant_id, pending.document_id)
        .await?;
    assert!(files.iter().all(|f| f.status == ProcessingStatus::Failed));

    // Retry path: back to pending.
    s.store
        .set_document_status(s.tenant_id, pending.document_id, ProcessingStatus::Pending)
        .await?;
    let d = s
        .store
        .get_document(s.tenant_id, pending.document_id)
        .await?
        .unwrap();
    assert_eq!(d.status, ProcessingStatus::Pending);

    Ok(())
}

/// Admission counts: per-tenant and global pending ingest jobs, counting only
/// `queued|running|failed` ingest jobs (done/dead and other kinds excluded).
/// Also proves the 0026 lease columns exist.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn count_pending_ingest_jobs_counts_only_open_ingest() -> anyhow::Result<()> {
    let s = setup().await?;
    let other_tenant: i64 =
        sqlx::query_scalar("INSERT INTO tenants (slug, name) VALUES ('t2','T2') RETURNING id")
            .fetch_one(&s.pool)
            .await?;

    let insert = |tenant: i64, kind: &'static str, status: &'static str| {
        let pool = s.pool.clone();
        async move {
            sqlx::query(
                "INSERT INTO jobs (tenant_id, kind, status, lease_expires_at, locked_by) \
                 VALUES ($1, $2, $3, now(), 'test:1')",
            )
            .bind(tenant)
            .bind(kind)
            .bind(status)
            .execute(&pool)
            .await
        }
    };

    // Tenant A: 3 open ingest (queued/running/failed) + 1 done + 1 dead + 1 other-kind.
    insert(s.tenant_id, "ingest", "queued").await?;
    insert(s.tenant_id, "ingest", "running").await?;
    insert(s.tenant_id, "ingest", "failed").await?;
    insert(s.tenant_id, "ingest", "done").await?;
    insert(s.tenant_id, "ingest", "dead").await?;
    insert(s.tenant_id, "retag", "queued").await?;
    // Tenant B: 1 open ingest.
    insert(other_tenant, "ingest", "queued").await?;

    let (tenant_pending, global_pending) = s.store.count_pending_ingest_jobs(s.tenant_id).await?;
    assert_eq!(tenant_pending, 3, "queued+running+failed ingest for tenant");
    assert_eq!(global_pending, 4, "tenant A's 3 + tenant B's 1");

    Ok(())
}
