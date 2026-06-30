// SPDX-License-Identifier: AGPL-3.0-or-later

//! The chunk record — an embedded unit of retrievable content (plan §5).

use serde::{Deserialize, Serialize};

/// A chunk of content with its embedding and provenance. Retrieval matches chunks then rolls
/// up to documents (plan §8); `ts_offset` enables deep-linking to a moment in audio/video.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Chunk {
    /// Surrogate primary key (`chunks.id`).
    pub id: i64,
    /// Owning tenant.
    pub tenant_id: i64,
    /// Parent document (retrieval rolls up to here).
    pub document_id: i64,
    /// Source file/page (provenance for deep-links).
    pub file_id: i64,
    /// Page within the document this chunk came from.
    pub page_no: Option<i32>,
    /// 0-based index of the chunk within its source.
    pub idx: i32,
    /// The chunk text (also fed to the BM25 keyword index).
    pub content: String,
    /// Seconds into the audio/video for transcript chunks; `None` otherwise.
    pub ts_offset: Option<f64>,
    /// Dense embedding. Length MUST equal the embedder/schema dimension (Qwen3-Embedding-4B = 2560).
    pub embedding: Vec<f32>,
}
