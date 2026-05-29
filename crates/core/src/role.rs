//! Model capability roles — what a backend can be asked to do (plan §6).

use crate::macros::str_enum;

str_enum! {
    /// The capability a model call requires. The scheduler routes each call to a backend
    /// that advertises the matching role; `embed` is special — it must pin to a single model
    /// and never fail over across different models (plan §11).
    pub enum Role {
        /// Text generation / summarization / RAG synthesis.
        Text = "text",
        /// Image understanding (VLM captioning, scanned-page OCR).
        Vision = "vision",
        /// Code understanding / summarization.
        Code = "code",
        /// Embedding generation (vectors). Must not fail over across models.
        Embed = "embed",
        /// Cross-encoder reranking of retrieval candidates.
        Rerank = "rerank",
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use std::str::FromStr;

    use super::*;

    #[test]
    fn wire_strings_are_locked() {
        // These appear in `config.toml` backend `roles` and in `usage_events.role`.
        assert_eq!(Role::Text.as_str(), "text");
        assert_eq!(Role::Embed.as_str(), "embed");
        assert_eq!(Role::Rerank.as_str(), "rerank");
        assert_eq!(Role::all().len(), 5);
    }

    #[test]
    fn parses_every_variant() {
        for r in Role::all() {
            assert_eq!(Role::from_str(r.as_str()).unwrap(), *r);
        }
    }
}
