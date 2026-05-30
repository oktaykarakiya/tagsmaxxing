//! `kb-pipeline`: orchestrates ingestion (extract → tag → chunk → embed → upsert) and
//! retrieval (embed → hybrid search → RRF → rerank → optional RAG).
//!
//! This crate currently ships the durable, Postgres-backed **job queue** (plan §16):
//! enqueue, atomic claim with `SELECT … FOR UPDATE SKIP LOCKED`, complete, exponential
//! backoff + dead-letter on failure, and a concurrent worker pool with graceful shutdown.
//! Extractors, chunking, and the full ingest pipeline land in subsequent P2/P3 tasks.

pub mod job_queue;

pub use job_queue::{JobQueue, run_worker_pool};
