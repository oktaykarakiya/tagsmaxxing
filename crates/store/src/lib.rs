//! `kb-store`: the `Store` implementation over Postgres + pgvector (sqlx), including
//! hybrid (vector + keyword) search, RRF fusion, and document roll-up.
//!
//! Skeleton crate. Implemented in phase P4 per `BUILD_LEDGER.toml`; see plan §5
//! (schema) and §8 (retrieval pipeline).
