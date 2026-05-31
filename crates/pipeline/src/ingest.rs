//! [`IngestPipeline`] — top-level orchestrator wiring the full §7 ingestion flow
//! from raw bytes to a ready document (plan §7, §10, P3-T7).

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context;
use bytes::Bytes;
use kb_core::blob::Blob;
use kb_core::document::Document;
use kb_core::extractor::{Extracted, Extractor, RawFile};
use kb_core::file::FileRecord;
use kb_core::kind::DocKind;
use kb_core::status::ProcessingStatus;
use kb_core::tagger::{TagInput, Tagger};
use kb_store::PgStore;

use crate::chunker::{DEFAULT_CHUNK_SIZE_CHARS, DEFAULT_OVERLAP_CHARS, chunk_text};
use crate::document_builder::{DocumentBuilder, PageInput};
use crate::embedder::ChunkEmbedder;
use crate::metadata_merge::MetadataMerger;
use crate::tag_canonicalizer::TagCanonicalizer;

/// Outcome of a single ingestion run.
#[derive(Debug)]
pub struct IngestOutput {
    /// The generated document id.
    pub document_id: i64,
    /// Canonical tag ids assigned to this document.
    pub tag_ids: Vec<i64>,
    /// Number of chunks embedded.
    pub chunk_count: usize,
}

/// Routes [`DocKind`] → [`Extractor`] implementation.
pub type ExtractorRouter = HashMap<DocKind, Arc<dyn Extractor>>;

/// A single file input to the ingestion pipeline.
#[derive(Debug, Clone)]
pub struct IngestFile {
    /// Raw file bytes.
    pub bytes: Vec<u8>,
    /// Optional page label (e.g. "front", "back").
    pub page_label: Option<String>,
    /// Optional filesystem path or filename.
    pub path: Option<String>,
}

/// The ingestion pipeline orchestrator (plan §7, §10).
pub struct IngestPipeline {
    blob: Arc<dyn Blob>,
    extractors: ExtractorRouter,
    tagger: Arc<dyn Tagger>,
    canonicalizer: Arc<TagCanonicalizer>,
    embedder: Arc<ChunkEmbedder>,
    store: Arc<PgStore>,
}

impl IngestPipeline {
    /// Create a new pipeline. All components must be fully initialised
    /// before ingestion begins.
    #[must_use]
    pub fn new(
        blob: Arc<dyn Blob>,
        extractors: ExtractorRouter,
        tagger: Arc<dyn Tagger>,
        canonicalizer: Arc<TagCanonicalizer>,
        embedder: Arc<ChunkEmbedder>,
        store: Arc<PgStore>,
    ) -> Self {
        Self {
            blob,
            extractors,
            tagger,
            canonicalizer,
            embedder,
            store,
        }
    }

    /// Run the full 8-step ingestion flow.
    ///
    /// # Errors
    /// Returns an error if any step fails. The final `transactional_ingest`
    /// is atomic.
    pub async fn ingest(
        &self,
        tenant_id: i64,
        files: Vec<IngestFile>,
        user_note: Option<String>,
    ) -> anyhow::Result<IngestOutput> {
        // 1. Build document + file records.
        let (document, file_records) = if files.len() == 1 {
            let f = &files[0];
            DocumentBuilder::build_single(
                tenant_id,
                &f.bytes,
                f.path.as_deref(),
                user_note.as_deref(),
            )
        } else {
            let pages: Vec<PageInput<'_>> = files
                .iter()
                .map(|f| PageInput {
                    bytes: &f.bytes,
                    page_label: f.page_label.as_deref(),
                    path: f.path.as_deref(),
                })
                .collect();
            DocumentBuilder::build_multi(tenant_id, &pages, user_note.as_deref())
        };

        // 2. Store blobs.
        for file in &file_records {
            let idx = (file.page_no - 1) as usize;
            if let Some(input) = files.get(idx) {
                self.blob
                    .put(&file.blob_key, Bytes::copy_from_slice(&input.bytes))
                    .await
                    .with_context(|| format!("failed to store blob for {}", file.blob_key))?;
            }
        }

        // 3. Per-file extraction.
        let mut extracted_pairs: Vec<(FileRecord, Extracted)> =
            Vec::with_capacity(file_records.len());
        for file in &file_records {
            let kind = infer_kind_from_mime(file.mime.as_deref());
            let extracted = if let Some(extractor) = self.extractors.get(&kind) {
                let raw = RawFile {
                    bytes: Bytes::new(),
                    mime: file.mime.clone(),
                    kind,
                    path: file.path.clone(),
                };
                extractor.extract(&raw).await.with_context(|| {
                    format!(
                        "extraction failed for page {} ({})",
                        file.page_no,
                        file.path.as_deref().unwrap_or("?")
                    )
                })?
            } else {
                Extracted::default()
            };
            extracted_pairs.push((file.clone(), extracted));
        }

        // 4. Merge metadata.
        let merged = MetadataMerger::merge(&extracted_pairs);

        // 5. Tag.
        let tag_input = TagInput {
            text: merged.merged_text.clone(),
            user_note: document.user_note.clone(),
            kind: merged.kind,
            meta: merged.merged_meta.clone(),
        };
        let tag_output = self.tagger.tag(&tag_input).await?;

        // 6. Canonicalize tags.
        let tag_ids = self
            .canonicalizer
            .canonicalize(tenant_id, &tag_output.tags)
            .await?;

        // 7. Chunk + embed.
        let mut all_text_chunks = Vec::new();
        for (file, extracted) in &extracted_pairs {
            let file_chunks = chunk_text(
                &extracted.text,
                file.id,
                Some(file.page_no),
                None,
                DEFAULT_CHUNK_SIZE_CHARS,
                DEFAULT_OVERLAP_CHARS,
            );
            all_text_chunks.extend(file_chunks);
        }
        let embedded_chunks = if all_text_chunks.is_empty() {
            Vec::new()
        } else {
            self.embedder
                .embed_chunks(all_text_chunks, tenant_id, document.id)
                .await?
        };
        let chunk_count = embedded_chunks.len();

        // 8. Transactional ingest.
        let final_doc = Document {
            id: document.id,
            tenant_id,
            title: Some(tag_output.title),
            summary: Some(tag_output.summary),
            user_note: document.user_note,
            kind: merged.kind,
            meta: merged.merged_meta,
            page_count: merged.page_count,
            status: ProcessingStatus::Ready,
            created_at: document.created_at,
        };

        let doc_id = self
            .store
            .transactional_ingest(&final_doc, &file_records, &tag_ids, &[embedded_chunks])
            .await?;

        Ok(IngestOutput {
            document_id: doc_id,
            tag_ids,
            chunk_count,
        })
    }
}

/// Map a MIME type to a [`DocKind`] for extractor routing.
fn infer_kind_from_mime(mime: Option<&str>) -> DocKind {
    match mime {
        Some(m) if m.starts_with("image/") => DocKind::Image,
        Some(m) if m.starts_with("audio/") => DocKind::Audio,
        Some(m) if m.starts_with("video/") => DocKind::Video,
        Some(m)
            if m == "text/plain"
                || m == "text/html"
                || m == "text/markdown"
                || m.starts_with("application/pdf")
                || m.starts_with("application/msword")
                || m.starts_with("application/vnd.openxmlformats-officedocument")
                || m.starts_with("application/vnd.oasis.opendocument") =>
        {
            DocKind::Document
        }
        Some(m)
            if m == "application/zip"
                || m == "application/x-tar"
                || m == "application/gzip"
                || m == "application/x-bzip2"
                || m == "application/x-xz"
                || m == "application/zstd" =>
        {
            DocKind::Archive
        }
        _ => DocKind::Binary,
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn infer_kind_mappings() {
        assert_eq!(infer_kind_from_mime(Some("text/plain")), DocKind::Document);
        assert_eq!(infer_kind_from_mime(Some("image/png")), DocKind::Image);
        assert_eq!(
            infer_kind_from_mime(Some("application/pdf")),
            DocKind::Document
        );
        assert_eq!(
            infer_kind_from_mime(Some("application/zip")),
            DocKind::Archive
        );
        assert_eq!(infer_kind_from_mime(None), DocKind::Binary);
        assert_eq!(
            infer_kind_from_mime(Some("application/octet-stream")),
            DocKind::Binary
        );
    }

    #[test]
    fn ingest_file_construction() {
        let f = IngestFile {
            bytes: b"hello".to_vec(),
            page_label: Some("front".into()),
            path: Some("/tmp/test.txt".into()),
        };
        assert_eq!(f.bytes, b"hello");
        assert_eq!(f.page_label.as_deref(), Some("front"));
    }

    #[test]
    fn empty_router() {
        let router: ExtractorRouter = HashMap::new();
        assert!(router.is_empty());
    }

    #[test]
    fn router_registration() {
        use async_trait::async_trait;
        use kb_core::extractor::{Extracted, Extractor, RawFile};
        use std::sync::Arc;

        struct DummyExtractor;
        #[async_trait]
        impl Extractor for DummyExtractor {
            async fn extract(&self, _: &RawFile) -> anyhow::Result<Extracted> {
                Ok(Extracted::default())
            }
        }

        let mut router: ExtractorRouter = HashMap::new();
        router.insert(DocKind::Document, Arc::new(DummyExtractor));
        assert_eq!(router.len(), 1);
        assert!(router.contains_key(&DocKind::Document));
    }
}
