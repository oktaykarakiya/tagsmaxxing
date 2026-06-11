// SPDX-License-Identifier: AGPL-3.0-or-later

//! The `Embedder` capability and its kind selector (plan §4).

use async_trait::async_trait;

use crate::macros::str_enum;

str_enum! {
    /// Which instruction the embedder should apply. Models like BGE-M3 / Qwen3-Embedding
    /// expect a different prefix for stored documents vs. search queries.
    pub enum EmbedKind {
        /// Embedding a stored document/chunk.
        Document = "document",
        /// Embedding a search query.
        Query = "query",
    }
}

/// Produces dense embeddings. The `embed` role pins to a single model+version for the life of
/// an index — mixing models silently corrupts the vector space (plan §11).
#[async_trait]
pub trait Embedder: Send + Sync {
    /// Output vector dimension; MUST equal the schema's `VECTOR(N)` (Qwen3-Embedding-4B = 2560).
    fn dim(&self) -> usize;

    /// Embed a batch of texts, applying the instruction selected by `kind`.
    ///
    /// # Errors
    /// Returns an error if the backend call fails. On success the output length equals
    /// `texts.len()` and every inner vector has length [`dim`](Embedder::dim).
    async fn embed(&self, texts: &[String], kind: EmbedKind) -> anyhow::Result<Vec<Vec<f32>>>;
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use std::str::FromStr;

    use super::*;

    #[test]
    fn embed_kind_round_trips() {
        for k in EmbedKind::all() {
            assert_eq!(EmbedKind::from_str(k.as_str()).unwrap(), *k);
        }
        assert_eq!(EmbedKind::Document.as_str(), "document");
        assert_eq!(EmbedKind::Query.as_str(), "query");
    }
}
