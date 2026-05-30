//! `kb-store`: the `Store` implementation over Postgres + pgvector (sqlx), including
//! hybrid (vector + keyword) search, RRF fusion, and document roll-up.
//!
//! This crate currently ships:
//! * the **schema migrations** (plan §5 schema, §13 RLS, §16 queue): forward-only SQL
//!   under `migrations/`, embedded and applied via [`MIGRATOR`];
//! * the **local-disk [`Blob`](kb_core::blob::Blob) implementation** (plan §20, P2-T1):
//!   content-addressed file storage under a hot-swappable root directory;
//! * **[`PgStore`]** — the Postgres-backed [`Store`](kb_core::store::Store) implementation
//!   (plan §5/§7/§13): `upsert_file`, `upsert_chunks`, and deferred `hybrid_search` (P4).

pub mod blob;
mod migrations;
pub mod pg_store;

pub use blob::LocalBlob;
pub use migrations::MIGRATOR;
pub use pg_store::PgStore;
