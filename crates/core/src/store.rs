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
    /// # Errors
    /// Returns an error if the database operation fails.
    async fn hybrid_search(&self, query: &Query) -> anyhow::Result<Vec<Hit>>;
}
