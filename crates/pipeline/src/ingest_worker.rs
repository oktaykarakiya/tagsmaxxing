//! Queued-ingest job processing — the worker half of async ingestion
//! (P15-T3, plan §16).
//!
//! The upload handler stages a `pending` document + file rows (with blob keys)
//! and enqueues a [`JobKind::Ingest`](kb_core::job::JobKind::Ingest) job whose
//! `document_id` points at them. A worker claims the job and calls
//! [`process_queued_ingest`]: it loads the staged rows, fetches each file's
//! bytes from the blob store, and runs the full pipeline via
//! [`IngestPipeline::ingest_into`] — which finalizes the *same* document id to
//! `ready`. Re-processing an already-`ready` document is a no-op success, so
//! lease-expiry replays and duplicate completions converge.

use async_trait::async_trait;
use kb_core::blob::Blob;
use kb_core::document::Document;
use kb_core::file::FileRecord;
use kb_core::job::Job;
use kb_core::status::ProcessingStatus;

use crate::ingest::{IngestFile, IngestPipeline};
use crate::job_queue::PERMANENT_ERROR_PREFIX;

// ── Store trait ──────────────────────────────────────────────────────────────

/// Database reads a queued-ingest worker needs (mock-backend pattern, §31.3).
///
/// Implemented by `PgStore`; tests inject an in-memory mock.
#[async_trait]
pub trait QueuedIngestStore: Send + Sync {
    /// Fetch a document by id within the tenant (RLS-scoped).
    async fn get_document(&self, tenant_id: i64, doc_id: i64) -> anyhow::Result<Option<Document>>;

    /// Fetch a document's files ordered by `page_no` (RLS-scoped).
    async fn get_files_for_document(
        &self,
        tenant_id: i64,
        doc_id: i64,
    ) -> anyhow::Result<Vec<FileRecord>>;
}

/// Production implementation: delegates to the real Postgres store.
#[async_trait]
impl QueuedIngestStore for kb_store::PgStore {
    async fn get_document(&self, tenant_id: i64, doc_id: i64) -> anyhow::Result<Option<Document>> {
        <kb_store::PgStore>::get_document(self, tenant_id, doc_id).await
    }

    async fn get_files_for_document(
        &self,
        tenant_id: i64,
        doc_id: i64,
    ) -> anyhow::Result<Vec<FileRecord>> {
        <kb_store::PgStore>::get_files_for_document(self, tenant_id, doc_id).await
    }
}

// ── Input loading ────────────────────────────────────────────────────────────

/// Everything a worker needs to finalize one staged ingest job.
#[derive(Debug)]
pub struct QueuedIngestInputs {
    /// The staged document id the pipeline must finalize.
    pub document_id: i64,
    /// File bytes + metadata, in `page_no` order.
    pub files: Vec<IngestFile>,
    /// The user note persisted at staging (drives tagging).
    pub user_note: Option<String>,
    /// Effective local-only flag: the document's staged flag OR'd with the
    /// tenant's plan gate resolved at processing time (hot-swap rule).
    pub local_only: bool,
    /// `true` when the document is already `ready` — an idempotent replay
    /// (lease-race duplicate); the caller succeeds without re-processing.
    pub already_ready: bool,
}

/// Load and validate a queued ingest job's inputs (P15-T3).
///
/// `plan_local_only` is the tenant's plan gate resolved by the caller **at
/// processing time** (free plan → local models only); it is OR'd with the
/// document's own staged `local_only` flag.
///
/// # Errors
/// Returns a `jobs.last_error`-ready string. **Deterministic** failures — a
/// job with no `document_id`, a staged document/file set that does not exist
/// — are tagged with [`PERMANENT_ERROR_PREFIX`] so the worker loop
/// dead-letters them immediately instead of burning the retry budget
/// (P15-T9); transient ones (database or blob read errors) keep the queue's
/// normal backoff retry.
pub async fn load_queued_ingest_inputs(
    store: &dyn QueuedIngestStore,
    blob: &dyn Blob,
    job: &Job,
    plan_local_only: bool,
) -> Result<QueuedIngestInputs, String> {
    let document_id = job.document_id.ok_or_else(|| {
        format!(
            "{PERMANENT_ERROR_PREFIX}ingest job {} carries no document_id (legacy job?)",
            job.id
        )
    })?;

    let doc = store
        .get_document(job.tenant_id, document_id)
        .await
        .map_err(|e| format!("failed to load staged document {document_id}: {e}"))?
        .ok_or_else(|| {
            format!("{PERMANENT_ERROR_PREFIX}staged document {document_id} not found for tenant")
        })?;

    if doc.status == ProcessingStatus::Ready {
        // Idempotent replay: already finalized (e.g. a lease-expiry duplicate
        // whose first run committed). Nothing to do.
        return Ok(QueuedIngestInputs {
            document_id,
            files: Vec::new(),
            user_note: None,
            local_only: doc.local_only,
            already_ready: true,
        });
    }

    let file_rows = store
        .get_files_for_document(job.tenant_id, document_id)
        .await
        .map_err(|e| format!("failed to load files for document {document_id}: {e}"))?;
    if file_rows.is_empty() {
        return Err(format!(
            "{PERMANENT_ERROR_PREFIX}staged document {document_id} has no file rows"
        ));
    }

    let mut files = Vec::with_capacity(file_rows.len());
    for row in &file_rows {
        let bytes = blob
            .get(&row.blob_key)
            .await
            .map_err(|e| format!("failed to read blob '{}': {e}", row.blob_key))?;
        files.push(IngestFile {
            bytes: bytes.to_vec(),
            page_label: row.page_label.clone(),
            path: row.path.clone(),
        });
    }

    Ok(QueuedIngestInputs {
        document_id,
        files,
        user_note: doc.user_note,
        local_only: doc.local_only || plan_local_only,
        already_ready: false,
    })
}

// ── Job handler ──────────────────────────────────────────────────────────────

/// Process one queued ingest job end-to-end: load the staged inputs and
/// finalize via [`IngestPipeline::ingest_into`] (P15-T3, plan §16).
///
/// `plan_local_only` must be resolved by the caller at processing time (the
/// hot-swap rule — plans can change between enqueue and processing). The
/// model-call usage is attributed to `job.created_by` (P14-T1).
///
/// Designed as the [`run_worker_pool`](crate::run_worker_pool) handler body
/// for [`JobKind::Ingest`](kb_core::job::JobKind::Ingest) jobs; the thin
/// orchestration here is covered by the worker integration tests, with the
/// load/validate logic unit-tested via [`load_queued_ingest_inputs`].
///
/// # Errors
/// Returns a `jobs.last_error`-ready string on any failure; the worker loop
/// routes it into [`JobQueue::fail`](crate::JobQueue::fail) (backoff retry,
/// then dead-letter) — except **deterministic** failures, which carry
/// [`PERMANENT_ERROR_PREFIX`] and dead-letter immediately via
/// [`JobQueue::fail_permanent`](crate::JobQueue::fail_permanent) (P15-T9).
pub async fn process_queued_ingest(
    pipeline: &IngestPipeline,
    store: &dyn QueuedIngestStore,
    blob: &dyn Blob,
    job: &Job,
    plan_local_only: bool,
) -> Result<(), String> {
    let inputs = load_queued_ingest_inputs(store, blob, job, plan_local_only).await?;
    if inputs.already_ready {
        return Ok(());
    }
    pipeline
        .ingest_into(
            job.tenant_id,
            job.created_by,
            inputs.document_id,
            inputs.files,
            inputs.user_note,
            inputs.local_only,
        )
        .await
        .map(|_| ())
        .map_err(|e| classify_pipeline_error(&format!("{e:#}")))
}

/// Tag deterministic pipeline failures as permanent (P15-T9).
///
/// Extraction errors are a pure function of the stored bytes — every retry of
/// `"extraction failed for page …"` (the pipeline's own context string,
/// pinned by a unit test) must fail identically, so they dead-letter
/// immediately. Everything else (model backends, store writes) stays
/// transient and keeps the backoff retry.
fn classify_pipeline_error(formatted: &str) -> String {
    if formatted.starts_with("extraction failed") {
        format!("{PERMANENT_ERROR_PREFIX}{formatted}")
    } else {
        formatted.to_string()
    }
}

// ── Unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use kb_core::hash::Sha256;
    use kb_core::job::{JobKind, JobStatus};
    use kb_core::kind::DocKind;

    use super::*;

    /// In-memory [`QueuedIngestStore`].
    #[derive(Default)]
    struct MockStore {
        docs: HashMap<i64, Document>,
        files: HashMap<i64, Vec<FileRecord>>,
    }

    #[async_trait]
    impl QueuedIngestStore for MockStore {
        async fn get_document(
            &self,
            _tenant_id: i64,
            doc_id: i64,
        ) -> anyhow::Result<Option<Document>> {
            Ok(self.docs.get(&doc_id).cloned())
        }

        async fn get_files_for_document(
            &self,
            _tenant_id: i64,
            doc_id: i64,
        ) -> anyhow::Result<Vec<FileRecord>> {
            Ok(self.files.get(&doc_id).cloned().unwrap_or_default())
        }
    }

    fn doc(id: i64, status: ProcessingStatus, local_only: bool) -> Document {
        Document {
            id,
            tenant_id: 1,
            title: None,
            summary: None,
            user_note: Some("staged note".into()),
            kind: DocKind::Document,
            meta: serde_json::json!({}),
            page_count: 1,
            status,
            created_at: chrono::Utc::now(),
            local_only,
        }
    }

    fn file_row(doc_id: i64, blob_key: &str) -> FileRecord {
        FileRecord {
            id: 10,
            tenant_id: 1,
            document_id: doc_id,
            page_no: 1,
            page_label: Some("p1".into()),
            sha256: Sha256::from_hex(&"ab".repeat(32)).unwrap(),
            blob_key: blob_key.to_string(),
            path: Some("a.txt".into()),
            mime: Some("text/plain".into()),
            size_bytes: Some(4),
            meta: serde_json::json!({}),
            status: ProcessingStatus::Pending,
            ingested_at: chrono::Utc::now(),
        }
    }

    fn job(document_id: Option<i64>) -> Job {
        Job {
            id: 5,
            tenant_id: 1,
            file_id: None,
            document_id,
            kind: JobKind::Ingest,
            priority: 0,
            status: JobStatus::Running,
            attempts: 0,
            last_error: None,
            run_after: chrono::Utc::now(),
            created_at: chrono::Utc::now(),
            created_by: Some(7),
        }
    }

    /// A temp-dir LocalBlob with one stored object.
    async fn blob_with(key: &str, bytes: &[u8]) -> (tempfile::TempDir, Arc<dyn Blob>) {
        let tmp = tempfile::tempdir().unwrap();
        let blob: Arc<dyn Blob> = Arc::new(kb_store::LocalBlob::new(
            tmp.path().to_path_buf(),
            "t".into(),
        ));
        blob.put(key, bytes::Bytes::copy_from_slice(bytes))
            .await
            .unwrap();
        (tmp, blob)
    }

    #[tokio::test]
    async fn loads_staged_inputs_happy_path() {
        let mut store = MockStore::default();
        store
            .docs
            .insert(42, doc(42, ProcessingStatus::Pending, false));
        store.files.insert(42, vec![file_row(42, "k1")]);
        let (_tmp, blob) = blob_with("k1", b"file body").await;

        let inputs = load_queued_ingest_inputs(&store, blob.as_ref(), &job(Some(42)), false)
            .await
            .unwrap();
        assert_eq!(inputs.document_id, 42);
        assert!(!inputs.already_ready);
        assert_eq!(inputs.files.len(), 1);
        assert_eq!(inputs.files[0].bytes, b"file body");
        assert_eq!(inputs.files[0].path.as_deref(), Some("a.txt"));
        assert_eq!(inputs.user_note.as_deref(), Some("staged note"));
        assert!(!inputs.local_only);
    }

    #[tokio::test]
    async fn missing_document_id_errors() {
        let store = MockStore::default();
        let (_tmp, blob) = blob_with("k", b"x").await;
        let err = load_queued_ingest_inputs(&store, blob.as_ref(), &job(None), false)
            .await
            .unwrap_err();
        assert!(err.contains("no document_id"), "got: {err}");
        // Deterministic: retries cannot grow a document_id (P15-T9).
        assert!(err.starts_with(PERMANENT_ERROR_PREFIX), "got: {err}");
    }

    #[tokio::test]
    async fn unknown_document_errors() {
        let store = MockStore::default();
        let (_tmp, blob) = blob_with("k", b"x").await;
        let err = load_queued_ingest_inputs(&store, blob.as_ref(), &job(Some(99)), false)
            .await
            .unwrap_err();
        assert!(err.contains("not found"), "got: {err}");
        // Deterministic: staging commits before enqueue, so absence is final.
        assert!(err.starts_with(PERMANENT_ERROR_PREFIX), "got: {err}");
    }

    #[tokio::test]
    async fn ready_document_short_circuits() {
        let mut store = MockStore::default();
        store
            .docs
            .insert(42, doc(42, ProcessingStatus::Ready, false));
        // Deliberately no files / no blob: a replay must not touch them.
        let (_tmp, blob) = blob_with("unused", b"x").await;

        let inputs = load_queued_ingest_inputs(&store, blob.as_ref(), &job(Some(42)), false)
            .await
            .unwrap();
        assert!(inputs.already_ready);
        assert!(inputs.files.is_empty());
    }

    #[tokio::test]
    async fn missing_file_rows_error() {
        let mut store = MockStore::default();
        store
            .docs
            .insert(42, doc(42, ProcessingStatus::Pending, false));
        let (_tmp, blob) = blob_with("k", b"x").await;
        let err = load_queued_ingest_inputs(&store, blob.as_ref(), &job(Some(42)), false)
            .await
            .unwrap_err();
        assert!(err.contains("no file rows"), "got: {err}");
        assert!(err.starts_with(PERMANENT_ERROR_PREFIX), "got: {err}");
    }

    #[tokio::test]
    async fn missing_blob_errors_with_key() {
        let mut store = MockStore::default();
        store
            .docs
            .insert(42, doc(42, ProcessingStatus::Pending, false));
        store.files.insert(42, vec![file_row(42, "absent-key")]);
        let (_tmp, blob) = blob_with("other", b"x").await;
        let err = load_queued_ingest_inputs(&store, blob.as_ref(), &job(Some(42)), false)
            .await
            .unwrap_err();
        assert!(err.contains("absent-key"), "got: {err}");
        // Blob reads can fail transiently (shared S3): keep the retry budget.
        assert!(!err.starts_with(PERMANENT_ERROR_PREFIX), "got: {err}");
    }

    // ── classify_pipeline_error (P15-T9) ──────────────────────────────────

    #[test]
    fn extraction_failures_classify_as_permanent() {
        // Pins the pipeline's own context string ("extraction failed for page
        // …", ingest.rs) — extraction is a pure function of the stored bytes.
        let e = classify_pipeline_error(
            "extraction failed for page 1 (x.bin): TextExtractor: invalid utf-8",
        );
        assert!(e.starts_with(PERMANENT_ERROR_PREFIX), "got: {e}");
        assert!(e.contains("extraction failed for page 1"), "got: {e}");
    }

    #[test]
    fn model_and_store_failures_stay_transient() {
        for msg in [
            "model backend temporarily unavailable: tagger model call failed",
            "embedding failed: connection reset",
            "transactional ingest failed: deadlock detected",
        ] {
            let e = classify_pipeline_error(msg);
            assert_eq!(e, msg, "transient errors must pass through unchanged");
        }
    }

    /// The effective local_only is the staged flag OR the plan gate.
    #[tokio::test]
    async fn local_only_is_or_of_doc_flag_and_plan_gate() {
        let mut store = MockStore::default();
        store
            .docs
            .insert(1, doc(1, ProcessingStatus::Pending, false));
        store.files.insert(1, vec![file_row(1, "k1")]);
        store
            .docs
            .insert(2, doc(2, ProcessingStatus::Pending, true));
        store.files.insert(2, vec![file_row(2, "k1")]);
        let (_tmp, blob) = blob_with("k1", b"x").await;

        // doc=false, plan=true → true.
        let a = load_queued_ingest_inputs(&store, blob.as_ref(), &job(Some(1)), true)
            .await
            .unwrap();
        assert!(a.local_only);
        // doc=true, plan=false → true.
        let b = load_queued_ingest_inputs(&store, blob.as_ref(), &job(Some(2)), false)
            .await
            .unwrap();
        assert!(b.local_only);
        // doc=false, plan=false → false.
        let c = load_queued_ingest_inputs(&store, blob.as_ref(), &job(Some(1)), false)
            .await
            .unwrap();
        assert!(!c.local_only);
    }
}
