//! The `Store` capability — persistence + hybrid search (plan §4, §5, §8).

use async_trait::async_trait;

use crate::chunk::Chunk;
use crate::file::FileRecord;
use crate::query::{Hit, Query};

/// The durable store: metadata, tags, vectors, and the keyword index, with hybrid search in
/// SQL. The Postgres + pgvector implementation lives in `kb-store`.
#[async_trait]
pub trait Store: Send + Sync {
    /// Insert or update a file (page) record, returning its id. Idempotent on
    /// `(tenant_id, sha256)` for safe re-ingestion (plan §16).
    ///
    /// # Errors
    /// Returns an error if the database operation fails.
    async fn upsert_file(&self, rec: &FileRecord) -> anyhow::Result<i64>;

    /// Replace the chunks belonging to a file.
    ///
    /// # Errors
    /// Returns an error if the database operation fails.
    async fn upsert_chunks(&self, file_id: i64, chunks: &[Chunk]) -> anyhow::Result<()>;

    /// Run hybrid (vector + keyword) search, fuse with RRF, and roll results up to documents
    /// (deduplicated), each carrying its winning chunk's deep-link provenance (plan §8).
    ///
    /// `tenant_id` is required so the implementation can set the `app.current_tenant`
    /// Postgres GUC before querying tenant-scoped tables (RLS enforcement, plan §13).
    ///
    /// `query_embedding` is the embedded form of `query.text`, produced by the caller
    /// (usually via [`crate::embedder::Embedder::embed`] with [`crate::embedder::EmbedKind::Query`]).
    /// Its dimension must match the embedder configured in the schema.
    ///
    /// # Errors
    /// Returns an error if the database operation fails.
    async fn hybrid_search(
        &self,
        tenant_id: i64,
        query: &Query,
        query_embedding: &[f32],
    ) -> anyhow::Result<Vec<Hit>>;

    /// Run **keyword-only** (lexical) search — the RLS-safe degrade path for when a
    /// tenant is over its monthly token budget or the embed/rerank backend is
    /// unavailable (plan §8, §13, §29; P14-T5).
    ///
    /// Unlike [`Store::hybrid_search`], this takes **no query embedding**: it runs only
    /// the lexical (`ts_rank` on `tsv`) query, rolls the matching chunks up to documents,
    /// and truncates to `query.top_k`. There is no vector query, no RRF fusion, and no
    /// rerank — so it requires no AI backend and is never metered.
    ///
    /// `tenant_id` is required for the same reason as [`Store::hybrid_search`]: the
    /// implementation MUST set the `app.current_tenant` Postgres GUC (inside the same
    /// tenant transaction) before querying, so Row-Level Security scopes results to this
    /// tenant exactly like hybrid search (plan §13). It applies the identical
    /// kind/tag/date filters carried by `query`.
    ///
    /// # Errors
    /// Returns an error if the database operation fails.
    async fn keyword_search(&self, tenant_id: i64, query: &Query) -> anyhow::Result<Vec<Hit>>;
}
