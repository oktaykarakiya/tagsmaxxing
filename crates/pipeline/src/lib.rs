// SPDX-License-Identifier: AGPL-3.0-or-later

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

pub mod cdn;
pub mod chunker;
pub mod crypto_shred;
pub mod document_builder;
pub mod embedder;
pub mod eval;
pub mod export;
pub mod folder_watcher;
pub mod ingest;
pub mod ingest_worker;
pub mod integrity_scan;
pub mod job_aging;
pub mod job_lease;
pub mod job_queue;
pub mod maintenance;
pub mod metadata_merge;
pub mod orphan_gc;
pub mod refetch_worker;
pub mod restore_test;
pub mod retrieval;
pub mod rrf;
pub mod tag_canonicalizer;
pub mod tag_store;
pub mod thumbnail;

pub use cdn::rewrite_blob_url;
pub use crypto_shred::{
    CryptoShredPipeline, CryptoShredSessionStore, CryptoShredStore, SessionStoreAdapter,
    process_delete_tenant_job,
};
pub use eval::{
    EvalBaseline, EvalMetrics, LabeledQuery, check_regression, compute_metrics, eval_queries,
    fixture_corpus, format_vector, recall_at_k, reciprocal_rank, spike,
};
pub use export::{ExportPipeline, ExportStore, process_export_job};
pub use folder_watcher::{
    FolderWatchConfig, FolderWatchHandler, FolderWatcher, matches_allowed_extension,
    matches_ignore_pattern, should_ingest,
};
pub use ingest::{
    ExtractorRouter, IngestError, IngestFile, IngestOutput, IngestPipeline, IngestStore,
    RetagStore, process_retag_job,
};
pub use ingest_worker::{
    QueuedIngestInputs, QueuedIngestStore, load_queued_ingest_inputs, process_queued_ingest,
};
pub use integrity_scan::{
    FileShaRecord, IntegrityScanRunner, process_integrity_scan_job, run_integrity_scan,
    select_sample, should_run_now as integrity_scan_should_run_now, verify_hash,
};
pub use job_lease::{LEASE_EXPIRED_ERROR, run_lease_reaper};
pub use job_queue::{JobQueue, PERMANENT_ERROR_PREFIX, run_worker_pool};
pub use maintenance::{
    B2LifecycleHandler, BlobCacheEvictHandler, FakeClock, LogPruneHandler, MaintenanceHandler,
    MaintenanceJobKind, MaintenanceScheduler, RealClock, ReembedCheckHandler, Schedule,
    VacuumHandler, is_due,
};
pub use orphan_gc::{
    DbBlobRecord, OrphanGcRunner, filter_by_grace_period, find_missing_db_blobs, find_orphan_blobs,
    process_orphan_gc_job, run_orphan_gc, should_run_now as orphan_gc_should_run_now,
};
pub use restore_test::{
    BackupStatus, IntegrityCheck, IntegrityReport, RestoreTestRunner, ShellRestoreTestRunner,
    build_check_constraint_query, build_row_count_query, check_backup_staleness, parse_row_counts,
    process_restore_test_job, run_restore_test, should_run_now,
};
pub use retrieval::{RetrievalPipeline, SearchMode};
pub use tag_store::TagStore;
pub use thumbnail::{
    FfmpegThumbnailer, ThumbnailConfig, ThumbnailGenerator, ThumbnailOutput, VideoThumbnailer,
};
