// SPDX-License-Identifier: AGPL-3.0-or-later

//! Integration test for the embedded queued-ingest workers (P15-T6) against a
//! real pgvector Postgres container: the **runtime-built** stack (real
//! JsonSchemaTagger / embedder / DocumentExtractor over a mock LLM backend)
//! drains a staged upload to `ready` via the spawned worker pool — the same
//! wiring `kb serve` starts.
//!
//! Needs **Podman** + network to pull `paradedb/paradedb`; gated `#[ignore]`:
//!
//! ```text
//! systemctl --user start podman.socket
//! DOCKER_HOST=unix://$XDG_RUNTIME_DIR/podman/podman.sock \
//!   cargo test -p kb-api --test queued_worker_pg -- --ignored
//! ```
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use kb_core::document::Document;
use kb_core::file::FileRecord;
use kb_core::hash::Sha256;
use kb_core::job::JobKind;
use kb_core::kind::DocKind;
use kb_core::role::Role;
use kb_core::status::ProcessingStatus;
use kb_mock_backend::MockBackend;
use sha2::Digest;

fn sha_of(bytes: &[u8]) -> Sha256 {
    let mut h = sha2::Sha256::new();
    h.update(bytes);
    Sha256::from_bytes(h.finalize().into())
}

/// Stage an upload → the spawned embedded worker pool processes it → the
/// document is `ready` with the mock tagger's metadata. Then a graceful
/// shutdown drains the workers + reaper.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn embedded_workers_drain_staged_upload_to_ready() -> anyhow::Result<()> {
    // ── mock LLM backend (text + embed) ─────────────────────────────────────
    let mock = MockBackend::start().await;
    mock.scenario().lock().await.chat_content = Some(
        serde_json::json!({
            "title": "Worker Doc",
            "summary": "Drained by the embedded pool.",
            "tags": ["worker", "queued"]
        })
        .to_string(),
    );
    mock.scenario().lock().await.embed_dim = Some(kb_store::EMBED_DIM);

    // ── runtime config: fresh DB + temp blob root + the mock backend ────────
    let db = kb_testsupport::fresh_db().await?;
    let blob_dir = tempfile::tempdir()?;
    let mut cfg = kb_config::Config::default();
    cfg.storage.postgres_url = db.admin_url.clone();
    cfg.storage.app_postgres_url = db.app_url.clone();
    cfg.blob.local_root = blob_dir.path().to_string_lossy().into_owned();
    cfg.backends.push(kb_config::Backend {
        id: "mock".into(),
        base_url: mock.url("/v1"),
        roles: vec![Role::Text, Role::Embed],
        slots: 8,
        priority: 0,
    });
    let app_config = kb_config::AppConfig::from_config(cfg);

    // Per-machine mode (no DB routing) — exactly what a worker process uses.
    let rt = kb_api::runtime::build_runtime(&app_config, false).await?;

    // ── tenant + staged upload (what process_upload_queued persists) ────────
    let admin = rt.pg_store.pool()?;
    let tenant_id: i64 =
        sqlx::query_scalar("INSERT INTO tenants (slug, name) VALUES ('t', 'Test') RETURNING id")
            .fetch_one(&admin)
            .await?;

    let body = b"Queued ingestion drains uploads in the background.".to_vec();
    let sha = sha_of(&body);
    let blob_key = sha.to_hex();
    rt.blob
        .put(&blob_key, bytes::Bytes::from(body.clone()))
        .await?;

    let doc = Document {
        id: 0,
        tenant_id,
        title: None,
        summary: None,
        user_note: Some("integration note".into()),
        kind: DocKind::Document,
        meta: serde_json::json!({}),
        page_count: 1,
        status: ProcessingStatus::Pending,
        created_at: chrono::Utc::now(),
        local_only: false,
        source_url: None,
        fetch_interval_secs: None,
        next_fetch_at: None,
        last_fetched_at: None,
        last_fetch_sha256: None,
        current_version: 1,
        fetch_failure_count: 0,
    };
    let file = FileRecord {
        id: 0,
        tenant_id,
        document_id: 0,
        page_no: 1,
        page_label: Some("doc.txt".into()),
        sha256: sha,
        blob_key,
        path: Some("doc.txt".into()),
        mime: Some("text/plain".into()),
        size_bytes: Some(body.len() as i64),
        meta: serde_json::json!({}),
        status: ProcessingStatus::Pending,
        ingested_at: chrono::Utc::now(),
    };
    let pending = rt.pg_store.create_pending_ingest(&doc, &[file]).await?;
    rt.job_queue
        .enqueue(
            tenant_id,
            None,
            None,
            Some(pending.document_id),
            JobKind::Ingest,
            0,
        )
        .await?;

    // ── spawn the embedded pool + reaper (the serve wiring) ─────────────────
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let workers = kb_api::worker_pool::spawn_ingest_workers(
        rt.job_queue.clone(),
        rt.ingest_pipeline.clone(),
        rt.pg_store.clone(),
        rt.blob.clone(),
        2,
        shutdown_rx.clone(),
    );
    let reaper = kb_api::worker_pool::spawn_lease_reaper(rt.job_queue.clone(), shutdown_rx);

    // ── poll until the worker finalizes the document ─────────────────────────
    let mut ready = false;
    for _ in 0..240 {
        let d = rt
            .pg_store
            .get_document(tenant_id, pending.document_id)
            .await?
            .expect("staged document exists");
        if d.status == ProcessingStatus::Ready {
            assert_eq!(d.title.as_deref(), Some("Worker Doc"));
            assert_eq!(
                d.user_note.as_deref(),
                Some("integration note"),
                "staged note survives the worker finalize"
            );
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    if !ready {
        // Surface the queue's own diagnosis before failing.
        let (job_status, last_error): (String, Option<String>) = sqlx::query_as(
            "SELECT status, last_error FROM jobs WHERE document_id = $1 AND kind = 'ingest'",
        )
        .bind(pending.document_id)
        .fetch_one(&admin)
        .await?;
        panic!(
            "embedded worker did not finalize within 60s: job status={job_status}, \
             last_error={last_error:?}"
        );
    }

    // Files ready + chunks embedded, all on the staged document id.
    let files = rt
        .pg_store
        .get_files_for_document(tenant_id, pending.document_id)
        .await?;
    assert!(files.iter().all(|f| f.status == ProcessingStatus::Ready));
    let chunks: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM chunks WHERE document_id = $1")
        .bind(pending.document_id)
        .fetch_one(&admin)
        .await?;
    assert!(chunks > 0, "content chunked + embedded by the worker");

    // The job completed (not stuck running/failed).
    let job_status: String =
        sqlx::query_scalar("SELECT status FROM jobs WHERE document_id = $1 AND kind = 'ingest'")
            .bind(pending.document_id)
            .fetch_one(&admin)
            .await?;
    assert_eq!(job_status, "done");

    // ── graceful drain ───────────────────────────────────────────────────────
    shutdown_tx.send(true)?;
    tokio::time::timeout(Duration::from_secs(10), workers)
        .await
        .expect("worker pool must drain on shutdown")?;
    tokio::time::timeout(Duration::from_secs(10), reaper)
        .await
        .expect("reaper must drain on shutdown")?;

    // Stop the runtime's background tasks too.
    let _ = rt.health_shutdown.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), rt.health_handle).await;

    Ok(())
}

/// P15-T8: a **standalone** `kb worker` process (no `kb serve`, no embedded
/// pool anywhere) drains a staged upload to `ready` through the full command
/// path — `worker_main` builds its own runtime from the per-machine config
/// (`use_db_routing = false`) and runs the pool + reaper until shutdown.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn standalone_worker_main_drains_staged_upload_to_ready() -> anyhow::Result<()> {
    // ── mock LLM backend (text + embed) ─────────────────────────────────────
    let mock = MockBackend::start().await;
    mock.scenario().lock().await.chat_content = Some(
        serde_json::json!({
            "title": "Standalone Worker Doc",
            "summary": "Drained by kb worker.",
            "tags": ["worker", "standalone"]
        })
        .to_string(),
    );
    mock.scenario().lock().await.embed_dim = Some(kb_store::EMBED_DIM);

    // ── per-machine worker config: fresh DB + temp blob root + mock backend ──
    let db = kb_testsupport::fresh_db().await?;
    let blob_dir = tempfile::tempdir()?;
    let mut cfg = kb_config::Config::default();
    cfg.storage.postgres_url = db.admin_url.clone();
    cfg.storage.app_postgres_url = db.app_url.clone();
    cfg.blob.local_root = blob_dir.path().to_string_lossy().into_owned();
    cfg.worker.concurrency = 1; // overridden below via the CLI-style argument
    cfg.backends.push(kb_config::Backend {
        id: "mock".into(),
        base_url: mock.url("/v1"),
        roles: vec![Role::Text, Role::Embed],
        slots: 4,
        priority: 0,
    });
    let app_config = kb_config::AppConfig::from_config(cfg);

    // ── stage an upload first (the API node's side of the hand-off) ─────────
    // A separate runtime handle plays the API node: it stages the pending
    // document + blob and enqueues the job. No worker pool is spawned on it.
    let api_side = kb_api::runtime::build_runtime(&app_config, false).await?;
    let admin = api_side.pg_store.pool()?;
    let tenant_id: i64 =
        sqlx::query_scalar("INSERT INTO tenants (slug, name) VALUES ('w', 'W') RETURNING id")
            .fetch_one(&admin)
            .await?;

    let body = b"Standalone workers drain the shared queue.".to_vec();
    let sha = sha_of(&body);
    let blob_key = sha.to_hex();
    api_side
        .blob
        .put(&blob_key, bytes::Bytes::from(body.clone()))
        .await?;

    let doc = Document {
        id: 0,
        tenant_id,
        title: None,
        summary: None,
        user_note: None,
        kind: DocKind::Document,
        meta: serde_json::json!({}),
        page_count: 1,
        status: ProcessingStatus::Pending,
        created_at: chrono::Utc::now(),
        local_only: false,
        source_url: None,
        fetch_interval_secs: None,
        next_fetch_at: None,
        last_fetched_at: None,
        last_fetch_sha256: None,
        current_version: 1,
        fetch_failure_count: 0,
    };
    let file = FileRecord {
        id: 0,
        tenant_id,
        document_id: 0,
        page_no: 1,
        page_label: Some("standalone.txt".into()),
        sha256: sha,
        blob_key,
        path: Some("standalone.txt".into()),
        mime: Some("text/plain".into()),
        size_bytes: Some(body.len() as i64),
        meta: serde_json::json!({}),
        status: ProcessingStatus::Pending,
        ingested_at: chrono::Utc::now(),
    };
    let pending = api_side
        .pg_store
        .create_pending_ingest(&doc, &[file])
        .await?;
    api_side
        .job_queue
        .enqueue(
            tenant_id,
            None,
            None,
            Some(pending.document_id),
            JobKind::Ingest,
            0,
        )
        .await?;

    // ── run the standalone worker (the full `kb worker` core) ───────────────
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let worker_cfg = app_config.clone();
    let worker = tokio::spawn(async move {
        kb_api::worker_pool::worker_main(&worker_cfg, Some(2), shutdown_rx).await
    });

    // ── poll until the standalone worker finalizes the document ─────────────
    let mut ready = false;
    for _ in 0..240 {
        let d = api_side
            .pg_store
            .get_document(tenant_id, pending.document_id)
            .await?
            .expect("staged document exists");
        if d.status == ProcessingStatus::Ready {
            assert_eq!(d.title.as_deref(), Some("Standalone Worker Doc"));
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    if !ready {
        let (job_status, last_error): (String, Option<String>) = sqlx::query_as(
            "SELECT status, last_error FROM jobs WHERE document_id = $1 AND kind = 'ingest'",
        )
        .bind(pending.document_id)
        .fetch_one(&admin)
        .await?;
        panic!(
            "standalone worker did not finalize within 60s: job status={job_status}, \
             last_error={last_error:?}"
        );
    }

    let chunks: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM chunks WHERE document_id = $1")
        .bind(pending.document_id)
        .fetch_one(&admin)
        .await?;
    assert!(chunks > 0, "content chunked + embedded by the worker");

    // ── graceful shutdown: worker_main drains and returns Ok ────────────────
    shutdown_tx.send(true)?;
    tokio::time::timeout(Duration::from_secs(90), worker)
        .await
        .expect("worker_main must drain on shutdown")?
        .expect("worker_main returns Ok");

    // Stop the API-side runtime's background tasks.
    let _ = api_side.health_shutdown.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), api_side.health_handle).await;

    Ok(())
}
