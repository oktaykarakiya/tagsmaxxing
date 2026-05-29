//! `kb-pipeline`: orchestrates ingestion (extract -> tag -> chunk -> embed -> upsert) and
//! retrieval (embed -> hybrid search -> RRF -> rerank -> optional RAG).
//!
//! Skeleton crate. Implemented in phases P2-P4 per `BUILD_LEDGER.toml`; see plan §7
//! (ingestion) and §8 (retrieval).
