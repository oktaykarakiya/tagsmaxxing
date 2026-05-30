//! `kb-store`: the `Store` implementation over Postgres + pgvector (sqlx), including
//! hybrid (vector + keyword) search, RRF fusion, and document roll-up.
//!
//! This crate currently ships:
//! * the **schema migrations** (plan §5 schema, §13 RLS, §16 queue): forward-only SQL
//!   under `migrations/`, embedded and applied via [`MIGRATOR`];
//! * the **local-disk [`Blob`](kb_core::blob::Blob) implementation** (plan §20, P2-T1):
//!   content-addressed file storage under a hot-swappable root directory.
//!
//! The `Store` trait implementation, hybrid search, and RRF fusion land in phase P4
//! (see plan §8).

pub mod blob;
mod migrations;

pub use blob::LocalBlob;
pub use migrations::MIGRATOR;
