//! Integration test for the queued-ingest worker path (P15-T3) against a real
//! pgvector Postgres container: staged pending document → `process_queued_ingest`
//! → document/files `ready` with chunks; replay is idempotent.
//!
//! Needs **Podman** + network to pull `pgvector/pgvector`; gated `#[ignore]`:
//!
//! ```text
//! systemctl --user start podman.socket
//! DOCKER_HOST=unix://$XDG_RUNTIME_DIR/podman/podman.sock \
//!   cargo test -p kb-pipeline --test queued_ingest_pg -- --ignored
//! ```
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kb_core::blob::Blob;
use kb_core::document::Document;
use kb_core::extractor::{Extracted, Extractor, RawFile};
use kb_core::file::FileRecord;
use kb_core::hash::Sha256;
use kb_core::job::JobKind;
use kb_core::kind::DocKind;
use kb_core::role::Role;
use kb_core::status::ProcessingStatus;
use kb_core::tagger::{TagInput, TagOutput, Tagger};
use kb_llm::LlamaClient;
use kb_mock_backend::MockBackend;
use kb_pipeline::embedder::ChunkEmbedder;
use kb_pipeline::ingest::{IngestPipeline, IngestStore};
use kb_pipeline::tag_canonicalizer::TagCanonicalizer;
use kb_pipeline::tag_store::TagStore;
use kb_pipeline::{JobQueue, process_queued_ingest};
use kb_scheduler::{Pool, test_backend};
use kb_store::PgStore;
use reqwest::Client;
use sha2::Digest;

struct TestTagger;

#[async_trait]
impl Tagger for TestTagger {
    async fn tag(&self, _input: &TagInput, _local_only: bool) -> anyhow::Result<TagOutput> {
        Ok(TagOutput {
            title: "Queued Doc".into(),
            summary: "Finalized by the worker.".into(),
            tags: vec!["queued".into()],
        })
    }
}

struct TextMockExtractor;

#[async_trait]
impl Extractor for TextMockExtractor {
    async fn extract(&self, file: &RawFile) -> anyhow::Result<Extracted> {
        Ok(Extracted {
            text: String::from_utf8_lossy(&file.bytes).to_string(),
            meta: serde_json::json!({}),
            page_images: vec![],
        })
    }
}

fn sha_of(bytes: &[u8]) -> Sha256 {
    let mut h = sha2::Sha256::new();
    h.update(bytes);
    Sha256::from_bytes(h.finalize().into())
}

/// Staged pending doc + files → worker processes → ready with chunks; replay
/// short-circuits; the staged file rows are UPDATED in place (no duplicates).
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn queued_ingest_finalizes_staged_document() -> anyhow::Result<()> {
    // ── infra: pg store + tenant ────────────────────────────────────────────
    let db = kb_testsupport::fresh_db().await?;
    let store = Arc::new(PgStore::with_roles(&db.admin_url, &db.app_url));
    store.connect().await?;
    let admin = store.pool()?;
    let tenant_id: i64 =
        sqlx::query_scalar("INSERT INTO tenants (slug, name) VALUES ('t', 'Test') RETURNING id")
            .fetch_one(&admin)
            .await?;

    // ── blob store with the file bytes under the content-addressed key ──────
    let tmp = tempfile::tempdir()?;
    let blob: Arc<dyn Blob> = Arc::new(kb_store::LocalBlob::new(
        tmp.path().join("blobs"),
        "test".into(),
    ));
    let body = b"Queued ingestion turns uploads into background work.".to_vec();
    let sha = sha_of(&body);
    let blob_key = sha.to_hex();
    blob.put(&blob_key, bytes::Bytes::from(body.clone()))
        .await?;

    // ── stage the pending document + file (what the T5 handler will do) ─────
    let doc = Document {
        id: 0,
        tenant_id,
        title: None,
        summary: None,
        user_note: Some("from the upload form".into()),
        kind: DocKind::Document,
        meta: serde_json::json!({}),
        page_count: 1,
        status: ProcessingStatus::Pending,
        created_at: chrono::Utc::now(),
        local_only: false,
    };
    let file = FileRecord {
        id: 0,
        tenant_id,
        document_id: 0,
        page_no: 1,
        page_label: Some("doc.txt".into()),
        sha256: sha,
        blob_key: blob_key.clone(),
        path: Some("doc.txt".into()),
        mime: Some("text/plain".into()),
        size_bytes: Some(body.len() as i64),
        meta: serde_json::json!({}),
        status: ProcessingStatus::Pending,
        ingested_at: chrono::Utc::now(),
    };
    let pending = store.create_pending_ingest(&doc, &[file]).await?;

    // ── enqueue + claim the job ──────────────────────────────────────────────
    let queue = JobQueue::new(admin.clone(), 1_000, 3);
    queue
        .enqueue(
            tenant_id,
            None,
            None,
            Some(pending.document_id),
            JobKind::Ingest,
            0,
        )
        .await?;
    let job = queue
        .claim_kinds(&[JobKind::Ingest])
        .await?
        .expect("staged job claimable");
    assert_eq!(job.document_id, Some(pending.document_id));

    // ── pipeline against a mock LLM backend (p3 pattern) ────────────────────
    let mock = MockBackend::start().await;
    mock.scenario().lock().await.embed_dim = Some(kb_store::EMBED_DIM);
    let backend = Arc::new(test_backend(
        "mock-queued",
        mock.url("/v1"),
        vec![Role::Text, Role::Embed],
        0,
        8,
    ));
    let pool = Pool::new(vec![backend], Duration::from_secs(10));
    let llm = Arc::new(LlamaClient::new(
        pool,
        Client::new(),
        0,
        0,
        Duration::from_millis(200),
    ));
    let tagger: Arc<dyn Tagger> = Arc::new(TestTagger);
    let canon = Arc::new(TagCanonicalizer::new(
        Arc::clone(&store) as Arc<dyn TagStore>,
        Arc::clone(&llm),
        "embed".into(),
        0.85,
    ));
    let embedder = Arc::new(ChunkEmbedder::new(
        Arc::clone(&llm),
        "embed".into(),
        kb_store::EMBED_DIM,
    ));
    let mut extractors: HashMap<DocKind, Arc<dyn Extractor>> = HashMap::new();
    extractors.insert(DocKind::Document, Arc::new(TextMockExtractor));
    let pipeline = IngestPipeline::new(
        Arc::clone(&blob),
        extractors,
        tagger,
        canon,
        embedder,
        Arc::clone(&store) as Arc<dyn IngestStore>,
    );

    // ── process the claimed job ──────────────────────────────────────────────
    process_queued_ingest(&pipeline, store.as_ref(), blob.as_ref(), &job, false)
        .await
        .expect("queued ingest must succeed");

    // Document + files flipped to ready, chunks exist — all on the SAME id.
    let d = store
        .get_document(tenant_id, pending.document_id)
        .await?
        .expect("document exists");
    assert_eq!(d.status, ProcessingStatus::Ready);
    assert_eq!(d.title.as_deref(), Some("Queued Doc"));
    assert_eq!(
        d.user_note.as_deref(),
        Some("from the upload form"),
        "staged note survives finalize"
    );
    let files = store
        .get_files_for_document(tenant_id, pending.document_id)
        .await?;
    assert_eq!(
        files.len(),
        1,
        "staged file row updated in place, not duplicated"
    );
    assert_eq!(files[0].status, ProcessingStatus::Ready);
    let chunks: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM chunks WHERE document_id = $1")
        .bind(pending.document_id)
        .fetch_one(&admin)
        .await?;
    assert!(chunks > 0, "content chunked + embedded");

    // ── replay (lease-race duplicate) is an idempotent no-op success ────────
    process_queued_ingest(&pipeline, store.as_ref(), blob.as_ref(), &job, false)
        .await
        .expect("replay must short-circuit on ready");
    let docs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM documents WHERE tenant_id = $1")
        .bind(tenant_id)
        .fetch_one(&admin)
        .await?;
    assert_eq!(docs, 1, "no duplicate document from the replay");

    Ok(())
}
