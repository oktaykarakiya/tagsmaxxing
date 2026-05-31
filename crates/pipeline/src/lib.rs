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

pub mod document_builder;
pub mod job_queue;
pub mod tag_canonicalizer;

pub use job_queue::{JobQueue, run_worker_pool};
