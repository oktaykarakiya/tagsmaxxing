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
pub mod ingest;
pub mod job_queue;
pub mod metadata_merge;
pub mod retrieval;
pub mod rrf;
pub mod tag_canonicalizer;
pub mod tag_store;

pub use ingest::{
    ExtractorRouter, IngestFile, IngestOutput, IngestPipeline, IngestStore, process_ingest_job,
};
pub use job_queue::{JobQueue, run_worker_pool};
pub use retrieval::RetrievalPipeline;
pub use tag_store::TagStore;
