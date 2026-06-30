// SPDX-License-Identifier: AGPL-3.0-or-later

//! Integration tests for the pending-ingest staging layer (P15-T1) against a
//! real pgvector Postgres container.
//!
//! These tests need **Podman** (this project targets Podman exclusively) and
//! network access to pull `paradedb/paradedb`. They are gated `#[ignore]` so
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
/// Also proves the lease columns exist.
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

/// `stage_pending_files` (P15-T5, the add-page flow) appends pending file rows
/// to an existing document and flips it back to pending for re-processing.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn stage_pending_files_appends_to_existing_document() -> anyhow::Result<()> {
    let s = setup().await?;
    let pending = s
        .store
        .create_pending_ingest(&doc(s.tenant_id, "base"), &[file(s.tenant_id, 0x66, 1)])
        .await?;
    // Simulate a finished first pass.
    s.store
        .set_document_status(s.tenant_id, pending.document_id, ProcessingStatus::Ready)
        .await?;

    // Add a second page.
    let mut added = file(s.tenant_id, 0x77, 2);
    added.document_id = pending.document_id;
    let ids = s
        .store
        .stage_pending_files(s.tenant_id, pending.document_id, &[added])
        .await?;
    assert_eq!(ids.len(), 1);

    // Document is pending again; both files present; the new one pending.
    let d = s
        .store
        .get_document(s.tenant_id, pending.document_id)
        .await?
        .unwrap();
    assert_eq!(d.status, ProcessingStatus::Pending);
    let files = s
        .store
        .get_files_for_document(s.tenant_id, pending.document_id)
        .await?;
    assert_eq!(files.len(), 2, "added page joins the existing one");
    assert_eq!(files[1].page_no, 2);
    assert_eq!(files[1].status, ProcessingStatus::Pending);

    // A nonexistent document errors.
    let mut orphan = file(s.tenant_id, 0x88, 1);
    orphan.document_id = 999_999;
    let err = s
        .store
        .stage_pending_files(s.tenant_id, 999_999, &[orphan])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not found"), "got: {err}");

    Ok(())
}

// ── P15-T9: tenant retry + failure visibility (§31.5 checkpoint) ───────────────

/// Stage a doc + enqueue a dead ingest job for it; returns (doc_id, job_id).
async fn stage_dead_ingest(s: &Setup, tag: &str) -> anyhow::Result<(i64, i64)> {
    let pending = s
        .store
        .create_pending_ingest(
            &doc(s.tenant_id, tag),
            &[file(s.tenant_id, tag.len() as u8, 1)],
        )
        .await?;
    let job_id: i64 = sqlx::query_scalar(
        "INSERT INTO jobs (tenant_id, document_id, kind, status, attempts, last_error) \
         VALUES ($1, $2, 'ingest', 'dead', 5, 'extraction failed: invalid bytes') RETURNING id",
    )
    .bind(s.tenant_id)
    .bind(pending.document_id)
    .fetch_one(&s.pool)
    .await?;
    // Mirror the dead-letter hook's document transition.
    sqlx::query("UPDATE documents SET status = 'failed' WHERE id = $1")
        .bind(pending.document_id)
        .execute(&s.pool)
        .await?;
    sqlx::query("UPDATE files SET status = 'failed' WHERE document_id = $1")
        .bind(pending.document_id)
        .execute(&s.pool)
        .await?;
    Ok((pending.document_id, job_id))
}

/// Full tenant-retry reset: dead job → queued (attempts 0, lease/error
/// cleared) and document + files → pending.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn retry_failed_ingest_resets_job_document_and_files() -> anyhow::Result<()> {
    let s = setup().await?;
    let (doc_id, job_id) = stage_dead_ingest(&s, "retry-cycle").await?;

    let outcome = s.store.retry_failed_ingest(s.tenant_id, doc_id).await?;
    assert_eq!(
        outcome,
        kb_store::RetryIngestOutcome::Retried { job_id },
        "the latest dead ingest job must be requeued"
    );

    let (status, attempts, last_error): (String, i32, Option<String>) =
        sqlx::query_as("SELECT status, attempts, last_error FROM jobs WHERE id = $1")
            .bind(job_id)
            .fetch_one(&s.pool)
            .await?;
    assert_eq!(status, "queued");
    assert_eq!(attempts, 0);
    assert!(last_error.is_none(), "error cleared on retry");

    let doc_status: String = sqlx::query_scalar("SELECT status FROM documents WHERE id = $1")
        .bind(doc_id)
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(doc_status, "pending");
    let failed_files: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM files WHERE document_id = $1 AND status = 'failed'",
    )
    .bind(doc_id)
    .fetch_one(&s.pool)
    .await?;
    assert_eq!(failed_files, 0, "failed files reset to pending");
    Ok(())
}

/// A queued/running/done job is not retryable; absence of any job reports NoJob.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn retry_refuses_non_terminal_jobs_and_reports_no_job() -> anyhow::Result<()> {
    let s = setup().await?;
    let pending = s
        .store
        .create_pending_ingest(&doc(s.tenant_id, "retry-live"), &[file(s.tenant_id, 77, 1)])
        .await?;

    // No job at all yet.
    assert_eq!(
        s.store
            .retry_failed_ingest(s.tenant_id, pending.document_id)
            .await?,
        kb_store::RetryIngestOutcome::NoJob
    );

    // A live (queued) job is not retryable.
    sqlx::query(
        "INSERT INTO jobs (tenant_id, document_id, kind, status) \
         VALUES ($1, $2, 'ingest', 'queued')",
    )
    .bind(s.tenant_id)
    .bind(pending.document_id)
    .execute(&s.pool)
    .await?;
    match s
        .store
        .retry_failed_ingest(s.tenant_id, pending.document_id)
        .await?
    {
        kb_store::RetryIngestOutcome::NotRetryable { status } => assert_eq!(status, "queued"),
        other => panic!("expected NotRetryable, got {other:?}"),
    }
    Ok(())
}

/// §31.5 NEGATIVE: another tenant cannot retry, reset, or read the error of a
/// foreign document — the explicit tenant filters find nothing and nothing
/// changes.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn cross_tenant_retry_and_error_read_are_inert() -> anyhow::Result<()> {
    let s = setup().await?;
    let (doc_id, job_id) = stage_dead_ingest(&s, "xtenant").await?;

    let intruder: i64 = sqlx::query_scalar(
        "INSERT INTO tenants (slug, name) VALUES ('intruder', 'I') RETURNING id",
    )
    .fetch_one(&s.pool)
    .await?;

    // Retry under the WRONG tenant: reports NoJob (no information about the
    // victim's job) and mutates nothing.
    assert_eq!(
        s.store.retry_failed_ingest(intruder, doc_id).await?,
        kb_store::RetryIngestOutcome::NoJob,
        "a foreign document id must look like it has no job at all"
    );
    let (status, attempts): (String, i32) =
        sqlx::query_as("SELECT status, attempts FROM jobs WHERE id = $1")
            .bind(job_id)
            .fetch_one(&s.pool)
            .await?;
    assert_eq!(status, "dead", "victim job untouched");
    assert_eq!(attempts, 5, "victim attempts untouched");
    let doc_status: String = sqlx::query_scalar("SELECT status FROM documents WHERE id = $1")
        .bind(doc_id)
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(doc_status, "failed", "victim document untouched");

    // Error read under the WRONG tenant: nothing leaks.
    assert_eq!(
        s.store.latest_ingest_job_error(intruder, doc_id).await?,
        None,
        "a foreign tenant must not read the victim's ingest error"
    );
    // …while the owner sees it.
    assert_eq!(
        s.store
            .latest_ingest_job_error(s.tenant_id, doc_id)
            .await?
            .as_deref(),
        Some("extraction failed: invalid bytes")
    );
    Ok(())
}

/// Retry never resurrects a document that already converged to ready (a stale
/// dead job from a lease race must not flip a ready document back to pending).
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn retry_does_not_downgrade_ready_documents() -> anyhow::Result<()> {
    let s = setup().await?;
    let (doc_id, job_id) = stage_dead_ingest(&s, "retry-ready").await?;
    sqlx::query("UPDATE documents SET status = 'ready' WHERE id = $1")
        .bind(doc_id)
        .execute(&s.pool)
        .await?;

    let outcome = s.store.retry_failed_ingest(s.tenant_id, doc_id).await?;
    assert_eq!(outcome, kb_store::RetryIngestOutcome::Retried { job_id });

    let doc_status: String = sqlx::query_scalar("SELECT status FROM documents WHERE id = $1")
        .bind(doc_id)
        .fetch_one(&s.pool)
        .await?;
    assert_eq!(
        doc_status, "ready",
        "ready documents stay ready; the requeued job no-ops as an idempotent replay"
    );
    Ok(())
}
