//! `kb-pipeline`: orchestrates ingestion (extract → tag → chunk → embed → upsert) and
//! retrieval (embed → hybrid search → RRF → rerank → optional RAG).
//!
//! This crate currently ships:
//! - The durable, Postgres-backed **job queue** (plan §16): enqueue, atomic claim with
//!   `SELECT … FOR UPDATE SKIP LOCKED`, complete, exponential backoff + dead-letter on
//!   failure, and a concurrent worker pool with graceful shutdown.
//! - The **document builder** (plan §5, §7, §27): constructs a [`Document`] and ordered
//!   [`FileRecord`]s from 1..N raw file bytes, computing SHA-256, MIME type, and blob
//!   keys before any DB I/O.
//! - The **tag canonicalizer** (plan §6.5, P3-T3): deduplicates raw LLM-proposed tags
//!   against a tenant's existing tag set via alias lookup and cosine similarity.
//! - The **token-aware chunker** (plan §7, §8, P3-T4): splits extracted text into
//!   overlapping chunks with page/file provenance and transcript `ts_offset` support.
//! - The **batch embedder** (plan §7, P3-T4): batches chunk content (and tag names)
//!   through [`LlamaClient`], verifying output dimensions against the schema.

pub mod chunker;
pub mod document_builder;
pub mod embedder;
pub mod eval;
pub mod export;
pub mod folder_watcher;
pub mod ingest;
pub mod integrity_scan;
pub mod job_queue;
pub mod metadata_merge;
pub mod orphan_gc;
pub mod restore_test;
pub mod retrieval;
pub mod rrf;
pub mod tag_canonicalizer;
pub mod tag_store;

pub use eval::{
    EvalBaseline, EvalMetrics, LabeledQuery, check_regression, compute_metrics, eval_queries,
    fixture_corpus, format_vector, recall_at_k, reciprocal_rank, spike_1024,
};
pub use export::{ExportPipeline, ExportStore, process_export_job};
pub use folder_watcher::{
    FolderWatchConfig, FolderWatchHandler, FolderWatcher, matches_allowed_extension,
    matches_ignore_pattern, should_ingest,
};
pub use ingest::{
    ExtractorRouter, IngestFile, IngestOutput, IngestPipeline, IngestStore, RetagStore,
    process_ingest_job, process_retag_job,
};
pub use integrity_scan::{
    FileShaRecord, IntegrityScanRunner, process_integrity_scan_job, run_integrity_scan,
    select_sample, should_run_now as integrity_scan_should_run_now, verify_hash,
};
pub use job_queue::{JobQueue, run_worker_pool};
pub use orphan_gc::{
    DbBlobRecord, OrphanGcRunner, filter_by_grace_period, find_missing_db_blobs, find_orphan_blobs,
    process_orphan_gc_job, run_orphan_gc, should_run_now as orphan_gc_should_run_now,
};
pub use restore_test::{
    BackupStatus, IntegrityCheck, IntegrityReport, RestoreTestRunner, ShellRestoreTestRunner,
    build_check_constraint_query, build_row_count_query, check_backup_staleness, parse_row_counts,
    process_restore_test_job, run_restore_test, should_run_now,
};
pub use retrieval::RetrievalPipeline;
pub use tag_store::TagStore;
