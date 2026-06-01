//! The `Reranker` capability (plan §4, §8).

use async_trait::async_trait;

/// A cross-encoder that rescores retrieval candidates against the query for precision
/// (plan §8 step 5).
#[async_trait]
pub trait Reranker: Send + Sync {
    /// Score each document's relevance to `query`. The returned scores are order-aligned with
    /// `docs` (higher = more relevant).
    ///
    /// `local_only` constrains the backend selection to on-premises backends
    /// only (plan §26.6, P9-T9). When `true`, the call must not reach a remote
    /// provider. The scheduler enforces this at acquire time.
    ///
    /// # Errors
    /// Returns an error if the backend call fails. On success the output length equals
    /// `docs.len()`.
    async fn rerank(
        &self,
        query: &str,
        docs: &[String],
        local_only: bool,
    ) -> anyhow::Result<Vec<f32>>;
}
