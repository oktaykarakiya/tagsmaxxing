//! Retrieval query input and result types (plan §8).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::kind::DocKind;

/// Optional filters applied in the SQL `WHERE` before fusion (plan §8).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct QueryFilters {
    /// Restrict to these document kinds (empty = any).
    pub kinds: Vec<DocKind>,
    /// Restrict to documents carrying all of these canonical tag names (empty = any).
    pub tags: Vec<String>,
    /// Only documents created at/after this time.
    pub created_after: Option<DateTime<Utc>>,
    /// Only documents created at/before this time.
    pub created_before: Option<DateTime<Utc>>,
}

/// A retrieval request: free text plus filters and a result cap.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Query {
    /// The user's natural-language query.
    pub text: String,
    /// Structured filters.
    pub filters: QueryFilters,
    /// Maximum number of documents to return after roll-up/rerank.
    pub top_k: usize,
}

/// A single retrieval result: one document, with the winning chunk's provenance so the UI can
/// deep-link to the exact page or moment (plan §8).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Hit {
    /// The matched document.
    pub document_id: i64,
    /// Fused/rerank relevance score (higher = better).
    pub score: f32,
    /// Document title, if set.
    pub title: Option<String>,
    /// A snippet from the best-matching chunk.
    pub snippet: String,
    /// File/page that produced the winning chunk (deep-link target).
    pub file_id: i64,
    /// Page number of the winning chunk, if applicable.
    pub page_no: Option<i32>,
    /// Seconds offset of the winning chunk for audio/video, if applicable.
    pub ts_offset: Option<f64>,
}
