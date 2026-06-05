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
//! * **[`session_store`]** — [`SessionStore`](kb_core::session::SessionStore) implementations:
//!   [`InMemorySessionStore`](session_store::InMemorySessionStore) for dev/test and
//!   [`PgSessionStore`](session_store::PgSessionStore) for production (plan §13, P5-T4).

/// The embedding vector dimension the schema is built for — Qwen3-Embedding-4B = **2560**.
///
/// MUST equal the `VECTOR(N)` of `chunks.embedding` / `tags.embedding` (migration 0023) and the
/// `expected_dim` the embedder verifies. Single source of truth so callers that build
/// placeholder/zero embeddings (e.g. user-sourced tags) never drift from the schema and have
/// their INSERT rejected for a dimension mismatch.
pub const EMBED_DIM: usize = 2560;

pub mod admin_queries;
pub mod api_token_store;
pub mod b2_blob;
pub mod blob;
pub mod blob_cache;
pub mod budget;
pub mod crypto_shred_store;
pub mod dashboard;
pub mod db_routing;
pub mod decrypt_audit;
pub mod encrypted_blob;
pub(crate) mod hybrid_search;
mod migrations;
pub mod multipart_stream;
pub mod pg_store;
pub mod plan_enforcement;
pub mod plans;
pub mod session_store;
pub mod stripe_events;
pub mod subscription_lifecycle;
pub mod usage_recorder;
pub mod vault_kek;

pub use admin_queries::TenantInfo;
pub use admin_queries::UserView;
pub use b2_blob::{B2Blob, B2Config};
pub use blob::LocalBlob;
pub use blob_cache::BlobCache;
pub use dashboard::{ActivityEvent, DailyTokenUsage};
pub use db_routing::{PgRoutingNotifier, ROUTING_CHANNEL};
pub use encrypted_blob::EncryptedBlob;
pub use migrations::MIGRATOR;
pub use multipart_stream::{MultipartUploadConfig, PartProgress, StreamingMultipartUpload};
pub use pg_store::PgStore;
pub use plans::{Plan, TenantBilling};
pub use session_store::{InMemorySessionStore, PgSessionStore};
pub use vault_kek::VaultTransitKek;
