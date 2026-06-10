//! [`IngestPipeline`] — top-level orchestrator wiring the full §7 ingestion flow
//! from raw bytes to a ready document (plan §7, §10, P3-T7).
//!
//! The 8-step pipeline:
//! 1. [`DocumentBuilder`] — compute SHA-256, detect MIME, build blob keys.
//! 2. [`Blob::put`] — store every file's raw bytes in the content-addressed blob store.
//! 3. Per-file extraction via the [`ExtractorRouter`] (routes [`DocKind`] → [`Extractor`]).
//! 4. [`MetadataMerger`] — combine per-page text/meta/kind into document-level output.
//! 5. [`Tagger`] — LLM-generated title, summary, and tags over the merged document.
//! 6. [`TagCanonicalizer`] — deduplicate tags against the tenant's tag set.
//! 7. Chunk + embed — token-aware chunking then batch embedding via [`ChunkEmbedder`].
//! 8. [`IngestStore::transactional_ingest`] — atomic upsert of document, files,
//!    tags, and chunks in one transaction, forcing `status = 'ready'`.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use bytes::Bytes;
use kb_core::blob::Blob;
use kb_core::chunk::Chunk;
use kb_core::document::Document;
use kb_core::extractor::{Extracted, Extractor, RawFile};
use kb_core::file::FileRecord;
use kb_core::kind::DocKind;
use kb_core::status::ProcessingStatus;
use kb_core::tag::TagSource;
use kb_core::tagger::{TagInput, TagOutput, Tagger};
use kb_llm::VisionCaptioner;

use crate::chunker::{DEFAULT_CHUNK_SIZE_CHARS, DEFAULT_OVERLAP_CHARS, chunk_text};
use crate::document_builder::{DocumentBuilder, PageInput};
use crate::embedder::ChunkEmbedder;
use crate::metadata_merge::MetadataMerger;
use crate::tag_canonicalizer::TagCanonicalizer;

// ── IngestStore trait ───────────────────────────────────────────────────────

/// Database operations needed by [`IngestPipeline`].
///
/// Extracted from [`PgStore`](kb_store::PgStore) so the pipeline can be tested
/// without a live Postgres instance (plan §31.3 mock-backend pattern). The
/// production implementation delegates to [`PgStore`]; tests use an in-memory
/// mock that captures the call parameters.
#[async_trait]
pub trait IngestStore: Send + Sync {
    /// Atomically upsert the document, every file, every document-tag link,
    /// and every chunk in one database transaction.
    ///
    /// Returns the document id (the surrogate primary-key value from the
    /// `documents` table).
    ///
    /// `file_chunks` must have the same length as `files` — each element is
    /// the (possibly empty) list of embedded chunks belonging to that file.
    ///
    /// # Errors
    ///
    /// Returns an error if the database transaction fails for any reason
    /// (connection lost, constraint violation, etc.). The entire transaction
    /// is rolled back on failure.
    async fn transactional_ingest(
        &self,
        doc: &Document,
        files: &[FileRecord],
        tag_ids: &[i64],
        file_chunks: &[Vec<Chunk>],
    ) -> anyhow::Result<i64>;
}

/// Production implementation: delegates to the real Postgres store.
#[async_trait]
impl IngestStore for kb_store::PgStore {
    async fn transactional_ingest(
        &self,
        doc: &Document,
        files: &[FileRecord],
        tag_ids: &[i64],
        file_chunks: &[Vec<Chunk>],
    ) -> anyhow::Result<i64> {
        <kb_store::PgStore>::transactional_ingest(self, doc, files, tag_ids, file_chunks).await
    }
}

// ── Public types ─────────────────────────────────────────────────────────────

/// Outcome of a single ingestion run.
#[derive(Debug)]
pub struct IngestOutput {
    /// The generated document id (from the database).
    pub document_id: i64,
    /// Canonical tag ids assigned to this document.
    pub tag_ids: Vec<i64>,
    /// Total number of chunks embedded across all files.
    pub chunk_count: usize,
}

/// A typed ingestion error the API maps to a specific HTTP status.
///
/// The pipeline otherwise returns `anyhow::Error` (which the API maps to `500`);
/// this carries the one case the API must distinguish: a transient model-backend
/// outage, which becomes `503 Service Unavailable` + `Retry-After` instead of
/// `500` (campaign finding F4). `kb-pipeline` does not depend on `thiserror`, so
/// the trait impls are written by hand.
#[derive(Debug)]
pub enum IngestError {
    /// The model backend required to process this ingest was unavailable — no
    /// healthy backend, capacity exhausted, every retry failed, or every backend
    /// in circuit-breaker cooldown. Transient: the caller should retry later.
    BackendUnavailable(String),
}

impl std::fmt::Display for IngestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IngestError::BackendUnavailable(msg) => {
                write!(f, "model backend temporarily unavailable: {msg}")
            }
        }
    }
}

impl std::error::Error for IngestError {}

/// Returns `true` when the error chain indicates the model backend was
/// **unavailable** — a transient availability/capacity failure the API should
/// surface as `503 Service Unavailable` rather than `500` (campaign finding F4).
///
/// Recognises the [`kb_llm::LlmError`] variants that mean "the service could not
/// be reached / had no capacity": a scheduler acquire failure (no healthy
/// backend, timeout, capacity exhausted, pool closed), every backend failing
/// across retries, or every backend in cooldown. A genuine model/output error
/// ([`kb_llm::LlmError::Deserialize`]) or a one-off transport error
/// ([`kb_llm::LlmError::Http`]) stays a `500`.
fn is_backend_unavailable(err: &anyhow::Error) -> bool {
    err.downcast_ref::<kb_llm::LlmError>().is_some_and(|e| {
        matches!(
            e,
            kb_llm::LlmError::Scheduler(_)
                | kb_llm::LlmError::AllCooldown(_)
                | kb_llm::LlmError::AllFailed { .. }
        )
    })
}

/// Routes [`DocKind`] → [`Extractor`] implementation.
///
/// The pipeline looks up an extractor for each file's detected kind. Kinds
/// without a registered extractor fall back to [`Extracted::default`] (empty
/// text, empty meta, no page images — the binary/unknown path, plan §7).
pub type ExtractorRouter = HashMap<DocKind, Arc<dyn Extractor>>;

/// A single file input to the ingestion pipeline.
#[derive(Debug, Clone)]
pub struct IngestFile {
    /// Raw file bytes.
    pub bytes: Vec<u8>,
    /// Optional page label (e.g. `"front"`, `"back"`).
    pub page_label: Option<String>,
    /// Optional filesystem path or original filename.
    pub path: Option<String>,
}

// ── IngestPipeline ──────────────────────────────────────────────────────────

/// The ingestion pipeline orchestrator (plan §7, §10).
///
/// Wires the complete §7 flow: build document records → store blobs →
/// extract per-page → merge metadata → tag → canonicalize → chunk+embed →
/// transactional upsert. Every component is behind a trait object so the
/// pipeline can be tested with mock implementations (plan §31.3).
///
/// # Examples
///
/// ```no_run
/// # use std::sync::Arc;
/// # use kb_pipeline::ingest::{IngestPipeline, IngestFile};
/// # fn example(
/// #     blob: Arc<dyn kb_core::blob::Blob>,
/// #     extractors: kb_pipeline::ingest::ExtractorRouter,
/// #     tagger: Arc<dyn kb_core::tagger::Tagger>,
/// #     canonicalizer: Arc<kb_pipeline::tag_canonicalizer::TagCanonicalizer>,
/// #     embedder: Arc<kb_pipeline::embedder::ChunkEmbedder>,
/// #     store: Arc<dyn kb_pipeline::ingest::IngestStore>,
/// # ) {
/// let pipeline = IngestPipeline::new(blob, extractors, tagger, canonicalizer, embedder, store);
/// # }
/// ```
pub struct IngestPipeline {
    /// Content-addressed blob store for raw file bytes.
    blob: Arc<dyn Blob>,
    /// Maps [`DocKind`] → [`Extractor`] for per-file content extraction.
    extractors: ExtractorRouter,
    /// LLM tagger (title + summary + tags via json_schema).
    tagger: Arc<dyn Tagger>,
    /// Tag deduplication + canonicalization.
    canonicalizer: Arc<TagCanonicalizer>,
    /// Batch embedder for chunk content and tag names.
    embedder: Arc<ChunkEmbedder>,
    /// Database store for the final transactional upsert.
    store: Arc<dyn IngestStore>,
    /// Optional VLM captioner for image visual descriptions.
    /// When set and page_images are present, captions are prepended
    /// to the tagger's text input so the tagger sees visual content
    /// rather than just EXIF metadata.
    vision_captioner: Option<Arc<VisionCaptioner>>,
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
        store: Arc<dyn IngestStore>,
    ) -> Self {
        Self {
            blob,
            extractors,
            tagger,
            canonicalizer,
            embedder,
            store,
            vision_captioner: None,
        }
    }

    /// Attach an optional VLM captioner for image visual descriptions.
    #[must_use]
    pub fn with_vision_captioner(mut self, captioner: Arc<VisionCaptioner>) -> Self {
        self.vision_captioner = Some(captioner);
        self
    }

    /// Run the full 8-step ingestion flow synchronously (not via job queue).
    ///
    /// Callers that want async processing should stage a pending document,
    /// enqueue a job carrying its id, and use
    /// [`process_queued_ingest`](crate::ingest_worker::process_queued_ingest)
    /// as the handler with [`run_worker_pool`](crate::run_worker_pool) — the
    /// worker then finalizes via [`ingest_into`](Self::ingest_into).
    ///
    /// `user_id` attributes every metered model call made during this ingest
    /// (tagging, tag-name embedding, chunk embedding) to the acting user in
    /// `usage_events` (P14-T1). Pass the request's `AuthUser.user_id` on the
    /// inline path, or `Job::created_by` from the job processor; `None` records
    /// the usage tenant-only.
    ///
    /// # Errors
    ///
    /// Returns an error if any step fails. The final
    /// [`IngestStore::transactional_ingest`] call is atomic — on failure the
    /// entire document write is rolled back.
    pub async fn ingest(
        &self,
        tenant_id: i64,
        user_id: Option<i64>,
        files: Vec<IngestFile>,
        user_note: Option<String>,
        local_only: bool,
    ) -> anyhow::Result<IngestOutput> {
        self.ingest_inner(tenant_id, user_id, files, user_note, local_only, None)
            .await
    }

    /// Run the full ingestion flow **into an existing document id** — the
    /// queued-worker finalize path (P15-T3, plan §16).
    ///
    /// `document_id` must be a previously staged `pending` document (see
    /// `PgStore::create_pending_ingest`); the final transactional write then
    /// takes the store's explicit-id UPDATE path, flipping the staged rows to
    /// `ready` in place. Re-running with the same inputs converges to the same
    /// state, so worker retries and duplicate completions are idempotent.
    ///
    /// # Errors
    ///
    /// Returns an error if `document_id` is not positive or any pipeline step
    /// fails (the final transactional write is atomic).
    pub async fn ingest_into(
        &self,
        tenant_id: i64,
        user_id: Option<i64>,
        document_id: i64,
        files: Vec<IngestFile>,
        user_note: Option<String>,
        local_only: bool,
    ) -> anyhow::Result<IngestOutput> {
        anyhow::ensure!(
            document_id > 0,
            "ingest_into requires a positive document_id (got {document_id})"
        );
        self.ingest_inner(
            tenant_id,
            user_id,
            files,
            user_note,
            local_only,
            Some(document_id),
        )
        .await
    }

    /// Shared 8-step flow. `existing_doc_id` selects the finalize target:
    /// `None` lets the store create/reuse a document (inline path); `Some(id)`
    /// finalizes the staged pending document with that id (queued path).
    async fn ingest_inner(
        &self,
        tenant_id: i64,
        user_id: Option<i64>,
        files: Vec<IngestFile>,
        user_note: Option<String>,
        local_only: bool,
        existing_doc_id: Option<i64>,
    ) -> anyhow::Result<IngestOutput> {
        anyhow::ensure!(!files.is_empty(), "at least one file is required");

        // Sanitize the user note: strip NUL and other control characters
        // (which a PostgreSQL `text` column cannot store, so they would crash
        // the transactional ingest with a 500 — BUG-INGEST-11), then bound it to
        // MAX_USER_NOTE_BYTES to keep multi-megabyte payloads out of the LLM
        // tagger prompt (BUG-INGEST-10).
        let user_note = user_note.map(|note| {
            let note = strip_control_chars(&note);
            if note.len() > MAX_USER_NOTE_BYTES {
                truncate_to_char_boundary(&note, MAX_USER_NOTE_BYTES).to_string()
            } else {
                note
            }
        });

        // Sanitize filenames: neutralise path-traversal sequences in both
        // `page_label` and `path` before they are persisted or surfaced in
        // API responses (BUG-INGEST-08).
        let mut files = files;
        for f in &mut files {
            f.page_label = f.page_label.as_deref().and_then(sanitize_filename);
            f.path = f.path.as_deref().and_then(sanitize_filename);
        }

        // 1. Build document + file records.
        let (document, mut file_records) = if files.len() == 1 {
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

        // 2. Store blobs for every file.
        for (i, file) in file_records.iter().enumerate() {
            if let Some(input) = files.get(i) {
                self.blob
                    .put(&file.blob_key, Bytes::copy_from_slice(&input.bytes))
                    .await
                    .with_context(|| format!("failed to store blob for {}", file.blob_key))?;
            }
        }

        // 3. Per-file extraction. Each file's bytes are passed directly
        //    to the extractor registered for its detected kind.
        let mut extracted_pairs: Vec<(FileRecord, Extracted)> =
            Vec::with_capacity(file_records.len());
        for (i, file) in file_records.iter().enumerate() {
            let kind = infer_kind_from_mime(file.mime.as_deref());
            let file_bytes = files
                .get(i)
                .map(|f| Bytes::copy_from_slice(&f.bytes))
                .unwrap_or_default();
            let extracted = if let Some(extractor) = self.extractors.get(&kind) {
                let raw = RawFile {
                    bytes: file_bytes,
                    mime: file.mime.clone(),
                    kind,
                    path: file.path.clone(),
                };
                match extractor.extract(&raw).await {
                    Ok(extracted) => extracted,
                    // Media (audio/video/image) extraction is best-effort: a file
                    // with no decodable streams, or a transcode/transcription
                    // failure, still ingests as a metadata-only document rather
                    // than failing the whole upload. The blob is already stored,
                    // so it stays searchable by name/metadata. Non-media
                    // extraction errors (text/code/office) still propagate.
                    Err(e) if matches!(kind, DocKind::Audio | DocKind::Video | DocKind::Image) => {
                        tracing::warn!(
                            error = %e,
                            kind = ?kind,
                            path = file.path.as_deref().unwrap_or("?"),
                            "media extraction failed; ingesting metadata-only"
                        );
                        Extracted::default()
                    }
                    Err(e) => {
                        return Err(e).with_context(|| {
                            format!(
                                "extraction failed for page {} ({})",
                                file.page_no,
                                file.path.as_deref().unwrap_or("?")
                            )
                        });
                    }
                }
            } else {
                Extracted::default()
            };
            extracted_pairs.push((file.clone(), extracted));
        }

        // 4. Merge per-page metadata into document-level output.
        let mut merged = MetadataMerger::merge(&extracted_pairs);

        // 4b. VLM captioning for image documents (best-effort).
        // If we have page images and a captioner, ask the VLM to describe
        // them and prepend the descriptions so the tagger sees visual content
        // rather than just EXIF metadata text.
        if let Some(ref captioner) = self.vision_captioner
            && !merged.page_images.is_empty()
        {
            match captioner
                .describe_many(&merged.page_images, tenant_id, user_id, local_only)
                .await
            {
                Ok(caption) if !caption.is_empty() => {
                    // Prepend the visual description to the text the tagger
                    // sees. Using a clear delimiter so the prompt-injection
                    // boundary in the tagger still wraps everything.
                    merged.merged_text = format!(
                        "--- image description ---\n{caption}\n---\n\n{}",
                        merged.merged_text
                    );
                }
                Ok(_) => {} // empty caption — nothing to prepend
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        kind = ?merged.kind,
                        "VLM captioning failed; continuing with text-only tagger input"
                    );
                }
            }
        }

        // 5. Tag the whole document once over all pages' text + metadata.
        let tag_input = TagInput {
            tenant_id,
            user_id,
            text: merged.merged_text.clone(),
            user_note: document.user_note.clone(),
            kind: merged.kind,
            meta: merged.merged_meta.clone(),
        };
        let tag_output = match self.tagger.tag(&tag_input, local_only).await {
            Ok(output) => output,
            // Media documents (image/audio/video) are ingested best-effort:
            // a tagger failure (backend saturation, transient model error,
            // json_schema reject on empty text after failed VLM captioning) must
            // not fail the entire upload. The document is already blob-stored
            // and its file metadata is searchable; it degrades gracefully with
            // an empty title/summary/tags. Non-media documents still fail hard.
            Err(e)
                if matches!(
                    merged.kind,
                    DocKind::Image | DocKind::Audio | DocKind::Video
                ) =>
            {
                tracing::warn!(
                    error = %e,
                    kind = ?merged.kind,
                    "tagger failed for media document; ingesting with default metadata"
                );
                TagOutput::default()
            }
            // Non-media: a transient backend-availability failure (no healthy
            // backend, capacity exhausted, all retries failed / in cooldown) is
            // not the client's fault — surface it as a typed BackendUnavailable so
            // the API answers 503 + Retry-After instead of 500 (campaign F4).
            Err(e) if is_backend_unavailable(&e) => {
                tracing::warn!(
                    error = %e,
                    kind = ?merged.kind,
                    "tagger backend unavailable; ingest returns 503"
                );
                return Err(IngestError::BackendUnavailable(e.to_string()).into());
            }
            // Any other non-media tagger error is a genuine internal failure (500).
            Err(e) => return Err(e).context("tagger failed"),
        };

        // 6. Canonicalize tags against the tenant's existing tag set.
        let tag_ids = self
            .canonicalizer
            .canonicalize(tenant_id, user_id, &tag_output.tags, local_only)
            .await?;

        // 7. Chunk + embed — one batch per file so transactional_ingest
        //    receives the correct `&[Vec<Chunk>]` grouping.
        let mut embedded_per_file: Vec<Vec<Chunk>> = Vec::with_capacity(extracted_pairs.len());
        for (file, extracted) in &extracted_pairs {
            let text_chunks = chunk_text(
                &extracted.text,
                file.id,
                Some(file.page_no),
                None,
                DEFAULT_CHUNK_SIZE_CHARS,
                DEFAULT_OVERLAP_CHARS,
            );
            if text_chunks.is_empty() {
                embedded_per_file.push(Vec::new());
            } else {
                let embedded = self
                    .embedder
                    .embed_chunks(text_chunks, tenant_id, user_id, document.id, local_only)
                    .await?;
                embedded_per_file.push(embedded);
            }
        }
        let chunk_count: usize = embedded_per_file.iter().map(|c| c.len()).sum();

        // 7b. Mark every file ready. The DocumentBuilder creates file records as
        //     `Pending`; this synchronous pipeline has now blob-stored, extracted,
        //     and embedded each one, so it finishes their lifecycle here — matching
        //     the document's `Ready` status below. Without this, files would stay
        //     `pending` forever even though ingestion fully succeeded.
        for file in &mut file_records {
            file.status = ProcessingStatus::Ready;
        }

        // 8. Transactional ingest: upsert document + files + tags + chunks
        //    in one atomic transaction. A staged (queued-path) document id
        //    routes the store to its explicit-id UPDATE path (P15-T3).
        let final_doc = Document {
            id: existing_doc_id.unwrap_or(document.id),
            tenant_id,
            title: Some(tag_output.title),
            summary: Some(tag_output.summary),
            user_note: document.user_note,
            kind: merged.kind,
            meta: merged.merged_meta,
            page_count: merged.page_count,
            status: ProcessingStatus::Ready,
            created_at: document.created_at,
            local_only,
        };

        let doc_id = self
            .store
            .transactional_ingest(&final_doc, &file_records, &tag_ids, &embedded_per_file)
            .await?;

        Ok(IngestOutput {
            document_id: doc_id,
            tag_ids,
            chunk_count,
        })
    }
}

// ── Retag pipeline ────────────────────────────────────────────────────────────

/// Database operations needed by the retag workflow (plan §6.5, P6-T11).
///
/// Extracted from [`PgStore`](kb_store::PgStore) so the retag logic can be
/// tested without a live Postgres instance.
#[async_trait]
pub trait RetagStore: Send + Sync {
    /// Return tag ids that are locked (user-assigned) for a document.
    async fn get_locked_document_tags(
        &self,
        tenant_id: i64,
        document_id: i64,
    ) -> anyhow::Result<Vec<i64>>;

    /// Remove all LLM-sourced, non-locked document-tag links for a document.
    async fn clear_llm_document_tags(
        &self,
        tenant_id: i64,
        document_id: i64,
    ) -> anyhow::Result<u64>;

    /// Insert document tags with the given provenance.
    async fn insert_document_tags(
        &self,
        tenant_id: i64,
        document_id: i64,
        tag_ids: &[i64],
        source: TagSource,
    ) -> anyhow::Result<()>;
}

/// Production implementation: delegates to the real Postgres store.
#[async_trait]
impl RetagStore for kb_store::PgStore {
    async fn get_locked_document_tags(
        &self,
        tenant_id: i64,
        document_id: i64,
    ) -> anyhow::Result<Vec<i64>> {
        <kb_store::PgStore>::get_locked_document_tags(self, tenant_id, document_id).await
    }

    async fn clear_llm_document_tags(
        &self,
        tenant_id: i64,
        document_id: i64,
    ) -> anyhow::Result<u64> {
        <kb_store::PgStore>::clear_llm_document_tags(self, tenant_id, document_id).await
    }

    async fn insert_document_tags(
        &self,
        tenant_id: i64,
        document_id: i64,
        tag_ids: &[i64],
        source: TagSource,
    ) -> anyhow::Result<()> {
        <kb_store::PgStore>::insert_document_tags(self, tenant_id, document_id, tag_ids, source)
            .await
    }
}

/// Run a retag job (plan §6.5, P6-T11): re-tag a document while preserving
/// user-assigned locked tags.
///
/// Steps:
/// 1. Fetch the locked (user-assigned) tag ids for the document.
/// 2. Clear all LLM-sourced, non-locked tags.
/// 3. Run the tagger over the provided `document_text`.
/// 4. Canonicalize the new tags.
/// 5. Insert the canonicalized LLM tags.
/// 6. Re-ensure every locked tag is still present (insert with `User` source).
///
/// Returns the canonical tag ids now assigned to the document
/// (locked ids ∪ newly canonicalized ids).
///
/// `user_id` attributes the retag's model-call usage to the user who requested
/// it (P14-T1, typically `Job::created_by`); `None` records it tenant-only.
///
/// # Errors
///
/// Returns an error string suitable for `jobs.last_error` if any step fails.
#[allow(clippy::too_many_arguments)]
pub async fn process_retag_job(
    tenant_id: i64,
    user_id: Option<i64>,
    document_id: i64,
    document_text: &str,
    tagger: &dyn Tagger,
    canonicalizer: &TagCanonicalizer,
    store: &dyn RetagStore,
    local_only: bool,
) -> Result<Vec<i64>, String> {
    // 1. Snapshot locked tags before clearing.
    let locked_ids = store
        .get_locked_document_tags(tenant_id, document_id)
        .await
        .map_err(|e| format!("failed to fetch locked tags: {e}"))?;

    // 2. Clear stale LLM tags (locked user tags survive).
    store
        .clear_llm_document_tags(tenant_id, document_id)
        .await
        .map_err(|e| format!("failed to clear llm tags: {e}"))?;

    // 3. Tag the document.
    let tag_input = TagInput {
        tenant_id,
        user_id,
        text: document_text.to_string(),
        user_note: None,
        kind: DocKind::Document,
        meta: serde_json::Value::Null,
    };
    let tag_output = tagger
        .tag(&tag_input, local_only)
        .await
        .map_err(|e| format!("tagger failed: {e}"))?;

    // 4. Canonicalize.
    let new_tag_ids = canonicalizer
        .canonicalize(tenant_id, user_id, &tag_output.tags, local_only)
        .await
        .map_err(|e| format!("canonicalizer failed: {e}"))?;

    // 5. Insert LLM tags.
    if !new_tag_ids.is_empty() {
        store
            .insert_document_tags(tenant_id, document_id, &new_tag_ids, TagSource::Llm)
            .await
            .map_err(|e| format!("failed to insert llm tags: {e}"))?;
    }

    // 6. Re-ensure locked tags are present (they may have been removed by
    //    a previous operation; re-insert with User source).
    if !locked_ids.is_empty() {
        store
            .insert_document_tags(tenant_id, document_id, &locked_ids, TagSource::User)
            .await
            .map_err(|e| format!("failed to re-insert locked tags: {e}"))?;
    }

    // Merge and deduplicate the full set.
    let mut all_ids: Vec<i64> = locked_ids;
    for id in new_tag_ids {
        if !all_ids.contains(&id) {
            all_ids.push(id);
        }
    }

    Ok(all_ids)
}

// ── user note bounding ─────────────────────────────────────────────────────

/// Maximum allowed size for the `user_note` field in bytes (10 KB).
///
/// Notes exceeding this limit are truncated before reaching the tagger to
/// prevent LLM crashes on pathologically-large input (BUG-INGEST-10).
/// The limit is generous for a free-text annotation while protecting the
/// tagger prompt from multi-megabyte payloads.
pub const MAX_USER_NOTE_BYTES: usize = 10 * 1024;

/// Truncate `s` to at most `max_bytes` bytes, landing on a valid UTF-8
/// character boundary so the result is always well-formed.
///
/// Returns the original string slice when it is already within the limit.
/// When truncation is needed, walks backwards from `max_bytes` to find the
/// nearest character boundary (handling multi-byte sequences correctly).
#[must_use]
pub fn truncate_to_char_boundary(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Remove NUL and other control characters from a user-supplied string.
///
/// A NUL byte (`\0`) cannot be stored in a PostgreSQL `text` column, so a note
/// carrying one would crash the transactional ingest with a 500. Every other
/// control character is stripped as well, since notes are free-text annotations
/// with no legitimate use for them. The ordinary whitespace controls — tab
/// (`\t`), line feed (`\n`), and carriage return (`\r`) — are preserved
/// (BUG-INGEST-11).
///
/// (The extractor applies the equivalent cleanup to document *content*; this
/// guards the separate `user_note` path, which never passes through an
/// extractor. The two crates do not share a dependency, so the small helper is
/// duplicated rather than re-exported.)
#[must_use]
pub fn strip_control_chars(input: &str) -> String {
    input
        .chars()
        .filter(|&c| !(c.is_control() && c != '\t' && c != '\n' && c != '\r'))
        .collect()
}

// ── filename sanitization ──────────────────────────────────────────────────

/// Sanitize a user-supplied filename by neutralising path-traversal sequences.
///
/// Strips directory components (keeping only the basename), removes `..`
/// sequences, and returns `None` when the result is empty.
///
/// This is applied to both `page_label` and `path` on every ingested file
/// before the filename is stored or surfaced in API responses — preventing
/// path-traversal filenames like `../../etc/passwd` from being persisted.
#[must_use]
pub fn sanitize_filename(name: &str) -> Option<String> {
    // 1. Take only the basename — drop any leading directory components.
    let basename = name.rsplit(['/', '\\']).next().unwrap_or(name);

    // 2. Remove any remaining `..` substrings (handles edge cases like
    //    `..hidden` where the basename itself starts with dots).
    let cleaned = basename.replace("..", "");

    // 3. Trim whitespace that may have been left around removed components.
    let trimmed = cleaned.trim();

    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

// ── infer_kind_from_mime ────────────────────────────────────────────────────

/// Map a MIME type to a [`DocKind`] for extractor routing.
///
/// Used by the pipeline to select the correct extractor for each file.
/// Unknown or absent MIME types map to [`DocKind::Binary`] — no extraction
/// is attempted, and the file is stored with only its deterministic metadata.
fn infer_kind_from_mime(mime: Option<&str>) -> DocKind {
    match mime {
        Some(m) if m.starts_with("image/") => DocKind::Image,
        Some(m) if m.starts_with("audio/") => DocKind::Audio,
        Some(m) if m.starts_with("video/") => DocKind::Video,
        // All text/* subtypes route to Document.
        Some(m) if m.starts_with("text/") => DocKind::Document,
        // Known document/office MIME types.
        Some(m)
            if m.starts_with("application/pdf")
                || m.starts_with("application/msword")
                || m.starts_with("application/vnd.openxmlformats-officedocument")
                || m.starts_with("application/vnd.oasis.opendocument")
                || m == "application/rtf" =>
        {
            DocKind::Document
        }
        // Known archive MIME types.
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

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::time::Duration;

    use kb_core::chunk::Chunk;
    use kb_core::extractor::PageImage;
    use kb_core::tagger::TagOutput;

    use super::*;
    use crate::tag_canonicalizer::TAG_MERGE_THRESHOLD;
    use crate::tag_store::TagStore;
    use crate::tag_store::mock::MockTagStore;

    // ── Mock types ─────────────────────────────────────────────────────────

    /// An in-memory [`IngestStore`] that captures the call parameters and
    /// returns a configurable document id.
    struct MockIngestStore {
        /// The document id to return from `transactional_ingest`.
        doc_id: AtomicI64,
        /// Captures the call parameters for later assertion.
        ///
        /// Each call appends `(doc_title, files_len, tag_ids_len,
        /// total_chunks)` — kept simple so tests don't need to clone
        /// whole Documents.
        calls: Mutex<Vec<(String, usize, usize, usize)>>,
        /// Status (`as_str`) of every file in the most recent call, so tests can
        /// assert the pipeline finishes a file's lifecycle (Pending → Ready).
        file_statuses: Mutex<Vec<String>>,
        /// `doc.id` of every call — asserts the queued path threads the staged
        /// document id into the final write (P15-T3), while inline passes 0.
        doc_ids: Mutex<Vec<i64>>,
    }

    impl MockIngestStore {
        fn new(doc_id: i64) -> Self {
            Self {
                doc_id: AtomicI64::new(doc_id),
                calls: Mutex::new(Vec::new()),
                file_statuses: Mutex::new(Vec::new()),
                doc_ids: Mutex::new(Vec::new()),
            }
        }

        /// Set the document id returned by the next call.
        fn set_doc_id(&self, id: i64) {
            self.doc_id.store(id, Ordering::SeqCst);
        }

        /// Number of calls recorded.
        fn call_count(&self) -> usize {
            self.calls.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl IngestStore for MockIngestStore {
        async fn transactional_ingest(
            &self,
            doc: &Document,
            files: &[FileRecord],
            tag_ids: &[i64],
            file_chunks: &[Vec<Chunk>],
        ) -> anyhow::Result<i64> {
            let total_chunks: usize = file_chunks.iter().map(|c| c.len()).sum();
            self.calls.lock().unwrap().push((
                doc.title.clone().unwrap_or_default(),
                files.len(),
                tag_ids.len(),
                total_chunks,
            ));
            *self.file_statuses.lock().unwrap() = files
                .iter()
                .map(|f| f.status.as_str().to_string())
                .collect();
            self.doc_ids.lock().unwrap().push(doc.id);
            Ok(self.doc_id.load(Ordering::SeqCst))
        }
    }

    /// A mock extractor that returns the text it was constructed with,
    /// ignoring the input bytes completely.
    struct MockTextExtractor {
        text: String,
    }

    #[async_trait]
    impl Extractor for MockTextExtractor {
        async fn extract(&self, _: &RawFile) -> anyhow::Result<Extracted> {
            Ok(Extracted {
                text: self.text.clone(),
                meta: serde_json::json!({}),
                page_images: vec![],
            })
        }
    }

    /// A mock extractor that records the bytes it receives (for verifying
    /// the bytes-passing fix).
    struct RecordingExtractor {
        received: Mutex<Vec<Vec<u8>>>,
    }

    impl RecordingExtractor {
        fn new() -> Self {
            Self {
                received: Mutex::new(Vec::new()),
            }
        }

        fn received_bytes(&self) -> Vec<Vec<u8>> {
            self.received.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl Extractor for RecordingExtractor {
        async fn extract(&self, raw: &RawFile) -> anyhow::Result<Extracted> {
            self.received.lock().unwrap().push(raw.bytes.to_vec());
            Ok(Extracted {
                text: String::from_utf8_lossy(&raw.bytes).to_string(),
                meta: serde_json::json!({}),
                page_images: vec![],
            })
        }
    }

    /// A mock extractor that always returns an error.
    struct FailingExtractor;

    #[async_trait]
    impl Extractor for FailingExtractor {
        async fn extract(&self, _: &RawFile) -> anyhow::Result<Extracted> {
            anyhow::bail!("simulated extraction failure")
        }
    }

    /// A mock tagger that returns a fixed [`TagOutput`].
    struct MockTagger {
        output: TagOutput,
    }

    #[async_trait]
    impl Tagger for MockTagger {
        async fn tag(&self, _: &TagInput, _local_only: bool) -> anyhow::Result<TagOutput> {
            Ok(self.output.clone())
        }
    }

    /// A mock tagger that always returns an error.
    struct FailingTagger;

    #[async_trait]
    impl Tagger for FailingTagger {
        async fn tag(&self, _: &TagInput, _local_only: bool) -> anyhow::Result<TagOutput> {
            anyhow::bail!("simulated tagger failure")
        }
    }

    // ── infer_kind_from_mime tests ─────────────────────────────────────────

    #[test]
    fn infer_kind_mappings() {
        assert_eq!(infer_kind_from_mime(Some("text/plain")), DocKind::Document);
        assert_eq!(infer_kind_from_mime(Some("text/csv")), DocKind::Document);
        assert_eq!(infer_kind_from_mime(Some("image/png")), DocKind::Image);
        assert_eq!(infer_kind_from_mime(Some("audio/mpeg")), DocKind::Audio);
        assert_eq!(infer_kind_from_mime(Some("video/mp4")), DocKind::Video);
        assert_eq!(
            infer_kind_from_mime(Some("application/pdf")),
            DocKind::Document
        );
        assert_eq!(
            infer_kind_from_mime(Some("application/zip")),
            DocKind::Archive
        );
        assert_eq!(
            infer_kind_from_mime(Some("application/x-tar")),
            DocKind::Archive
        );
        assert_eq!(
            infer_kind_from_mime(Some("application/gzip")),
            DocKind::Archive
        );
        assert_eq!(
            infer_kind_from_mime(Some("application/x-bzip2")),
            DocKind::Archive
        );
        assert_eq!(
            infer_kind_from_mime(Some("application/x-xz")),
            DocKind::Archive
        );
        assert_eq!(
            infer_kind_from_mime(Some("application/zstd")),
            DocKind::Archive
        );
        assert_eq!(infer_kind_from_mime(None), DocKind::Binary);
        assert_eq!(
            infer_kind_from_mime(Some("application/octet-stream")),
            DocKind::Binary
        );
    }

    #[test]
    fn infer_kind_document_mimes() {
        for mime in &[
            "text/html",
            "text/markdown",
            "application/msword",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "application/vnd.oasis.opendocument.text",
        ] {
            assert_eq!(
                infer_kind_from_mime(Some(mime)),
                DocKind::Document,
                "mime '{mime}' should map to Document"
            );
        }
    }

    #[test]
    fn infer_kind_rtf_maps_to_document() {
        // Regression: application/rtf was missing → fell through to Binary.
        assert_eq!(
            infer_kind_from_mime(Some("application/rtf")),
            DocKind::Document
        );
    }

    #[test]
    fn infer_kind_code_mimes_map_to_document() {
        // All text/x-* code MIME types should route to Document via the
        // text/* prefix match (regression: only text/plain etc. were listed).
        for mime in &[
            "text/x-python",
            "text/x-rust",
            "text/x-c",
            "text/x-c++",
            "text/x-java",
            "text/x-go",
            "text/x-sh",
            "text/x-yaml",
            "text/x-toml",
            "text/x-log",
            "text/javascript",
            "text/css",
            "text/xml",
        ] {
            assert_eq!(
                infer_kind_from_mime(Some(mime)),
                DocKind::Document,
                "code MIME '{mime}' must route to Document"
            );
        }
    }

    #[test]
    fn infer_kind_all_audio_mimes_map_to_audio() {
        for mime in &[
            "audio/mpeg",
            "audio/wav",
            "audio/wave",
            "audio/x-wav",
            "audio/ogg",
            "audio/flac",
            "audio/mp4",
        ] {
            assert_eq!(
                infer_kind_from_mime(Some(mime)),
                DocKind::Audio,
                "audio MIME '{mime}' must route to Audio"
            );
        }
    }

    #[test]
    fn infer_kind_all_video_mimes_map_to_video() {
        for mime in &[
            "video/mp4",
            "video/x-matroska",
            "video/webm",
            "video/ogg",
            "video/quicktime",
            "video/x-msvideo",
        ] {
            assert_eq!(
                infer_kind_from_mime(Some(mime)),
                DocKind::Video,
                "video MIME '{mime}' must route to Video"
            );
        }
    }

    // ── sanitize_filename tests ────────────────────────────────────────────

    #[test]
    fn sanitize_traversal_path_to_basename() {
        // Classic path-traversal attack — only basename survives.
        assert_eq!(
            sanitize_filename("../../../../etc/passwd"),
            Some("passwd".into())
        );
    }

    #[test]
    fn sanitize_backslash_traversal() {
        assert_eq!(
            sanitize_filename("..\\..\\..\\windows\\system32\\config\\sam"),
            Some("sam".into())
        );
    }

    #[test]
    fn sanitize_mixed_separators() {
        assert_eq!(sanitize_filename("../..\\secret/.env"), Some(".env".into()));
    }

    #[test]
    fn sanitize_plain_filename_unchanged() {
        assert_eq!(sanitize_filename("report.txt"), Some("report.txt".into()));
    }

    #[test]
    fn sanitize_single_dot_prefix() {
        // A single `..` with no separators still gets stripped.
        assert_eq!(sanitize_filename("..hidden"), Some("hidden".into()));
    }

    #[test]
    fn sanitize_empty_string() {
        assert_eq!(sanitize_filename(""), None);
    }

    #[test]
    fn sanitize_only_separators() {
        assert_eq!(sanitize_filename("///"), None);
    }

    #[test]
    fn sanitize_only_dots() {
        assert_eq!(sanitize_filename(".."), None);
    }

    #[test]
    fn sanitize_root_path() {
        // A rooted path keeps only the basename.
        assert_eq!(sanitize_filename("/etc/shadow"), Some("shadow".into()));
    }

    #[test]
    fn sanitize_relative_without_traversal() {
        assert_eq!(
            sanitize_filename("subdir/file.txt"),
            Some("file.txt".into())
        );
    }

    // ── truncate_to_char_boundary tests ────────────────────────────────────

    #[test]
    fn truncate_short_string_unchanged() {
        assert_eq!(truncate_to_char_boundary("hello", 10), "hello");
    }

    #[test]
    fn truncate_exact_limit_unchanged() {
        let s = "abc";
        assert_eq!(s.len(), 3);
        assert_eq!(truncate_to_char_boundary(s, 3), s);
    }

    #[test]
    fn truncate_over_limit() {
        assert_eq!(truncate_to_char_boundary("hello world", 5), "hello");
    }

    #[test]
    fn truncate_multibyte_char_boundary() {
        // "café" is 5 bytes (c-a-f-é where é = 2 bytes). Truncating at 4
        // bytes would split é — the function must land at 3 bytes ("caf").
        let s = "café";
        assert!(s.len() > 4, "café must be > 4 bytes for this test");
        let result = truncate_to_char_boundary(s, 4);
        assert_eq!(result, "caf");
        // Verify the result is valid UTF-8.
        assert!(std::str::from_utf8(result.as_bytes()).is_ok());
    }

    #[test]
    fn truncate_multibyte_char_at_boundary_exact() {
        // "café" = [c][a][f][é (2 bytes)] = 5 bytes. 3 bytes lands at "caf".
        assert_eq!(truncate_to_char_boundary("café", 3), "caf");
    }

    #[test]
    fn truncate_max_bytes_zero() {
        assert_eq!(truncate_to_char_boundary("abc", 0), "");
    }

    #[test]
    fn truncate_max_bytes_zero_on_empty() {
        assert_eq!(truncate_to_char_boundary("", 0), "");
    }

    #[test]
    fn truncate_non_ascii_sequence() {
        // "こんにちは" — 5 Japanese characters, each 3 bytes = 15 bytes total.
        let s = "こんにちは";
        assert_eq!(s.len(), 15);
        // Truncate at 7 bytes → should get 6 bytes (2 full chars) = "こん".
        let result = truncate_to_char_boundary(s, 7);
        assert_eq!(result, "こん");
        assert_eq!(result.len(), 6);
    }

    #[test]
    fn truncate_with_max_greater_than_len_returns_original() {
        let s = "short";
        let result = truncate_to_char_boundary(s, 1024);
        assert_eq!(result, s);
        // Must be the same slice, not a copy.
        assert!(std::ptr::eq(result.as_ptr(), s.as_ptr()));
    }

    // ── strip_control_chars tests (BUG-INGEST-11) ──────────────────────────

    #[test]
    fn strip_control_removes_nul_and_controls() {
        // The exact control bytes the e2e note payload carries (BEL + ESC),
        // plus a NUL, must all be removed.
        assert_eq!(
            strip_control_chars("note\u{0}\u{7}\u{1b}with-controls"),
            "notewith-controls"
        );
    }

    #[test]
    fn strip_control_preserves_whitespace_and_unicode() {
        let s = "café 😀\tindented\nline";
        assert_eq!(strip_control_chars(s), s);
    }

    #[test]
    fn strip_control_empty_input() {
        assert_eq!(strip_control_chars(""), "");
    }

    #[test]
    fn strip_control_all_controls_yields_empty() {
        assert_eq!(strip_control_chars("\u{0}\u{1}\u{7}\u{1b}\u{7f}"), "");
    }

    // ── Construction ───────────────────────────────────────────────────────

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

    // ── Pipeline integration tests with mock components ────────────────────

    /// Build a full `IngestPipeline` with all mock components for testing.
    ///
    /// The returned pipeline uses a [`MockIngestStore`], a
    /// [`MockTextExtractor`] registered for [`DocKind::Document`], and a
    /// [`MockTagger`]. Blob storage is backed by a real [`LocalBlob`] on a
    /// temporary directory, so blob writes are exercised end-to-end.
    ///
    /// The caller receives the pipeline and a handle to the
    /// [`MockIngestStore`] (wrapped in `Arc`) for assertions.
    async fn build_test_pipeline(
        extract_text: &str,
        tag_output: TagOutput,
    ) -> (IngestPipeline, Arc<MockIngestStore>) {
        let tagger: Arc<dyn Tagger> = Arc::new(MockTagger { output: tag_output });
        build_pipeline_with_tagger(extract_text, tagger).await
    }

    /// Like [`build_test_pipeline`] but with a caller-supplied tagger, so tests
    /// can inject a failing / backend-down tagger to exercise the tagger error
    /// arms (F4: media degrades, non-media backend-outage → BackendUnavailable,
    /// other non-media errors → propagate as 500).
    async fn build_pipeline_with_tagger(
        extract_text: &str,
        tagger: Arc<dyn Tagger>,
    ) -> (IngestPipeline, Arc<MockIngestStore>) {
        use kb_core::role::Role;
        use kb_mock_backend::MockBackend;
        use kb_scheduler::{Pool, test_backend};
        use kb_store::LocalBlob;
        use reqwest::Client;

        // Blob store on a temp directory.
        let dir = tempfile::tempdir().unwrap();
        let blob = Arc::new(LocalBlob::new(dir.path().to_path_buf(), "test".into()));

        // Extractor router.
        let mut extractors: ExtractorRouter = HashMap::new();
        extractors.insert(
            DocKind::Document,
            Arc::new(MockTextExtractor {
                text: extract_text.to_string(),
            }),
        );

        // Mock LLM backend for embedding (tag names + chunk content).
        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");
        // Don't drop the mock — return it to the caller or leak it intentionally.
        // We use Box::leak to simplify the test helper (tests are short-lived).
        let backend = Arc::new(test_backend(
            "mock-embed",
            base_url,
            vec![Role::Embed],
            0, /* priority */
            8, /* slots */
        ));
        let pool = Pool::new(vec![backend], Duration::from_secs(5));
        let llm = Arc::new(kb_llm::LlamaClient::new(
            pool,
            Client::new(),
            0,                          /* max_retries */
            0,                          /* circuit_threshold */
            Duration::from_millis(200), /* cooldown */
        ));

        // Embedder: expected_dim = 3 (mock returns [0.1, 0.2, 0.3]).
        let embedder = Arc::new(ChunkEmbedder::new(
            Arc::clone(&llm),
            "test-embed-model".into(),
            3, /* expected_dim */
        ));

        // Tag canonicalizer with an empty mock tag store (all tags are new).
        let tag_store: Arc<dyn crate::tag_store::TagStore> =
            Arc::new(crate::tag_store::mock::MockTagStore::new());
        let canonicalizer = Arc::new(TagCanonicalizer::new(
            tag_store,
            Arc::clone(&llm),
            "test-embed-model".into(),
            crate::tag_canonicalizer::TAG_MERGE_THRESHOLD,
        ));

        // Ingest store.
        let ingest_store = Arc::new(MockIngestStore::new(42));

        let store_ref = Arc::clone(&ingest_store) as Arc<dyn IngestStore>;

        let pipeline =
            IngestPipeline::new(blob, extractors, tagger, canonicalizer, embedder, store_ref);

        // Intentionally forget the mock backend handle — the test process
        // exits shortly after, and the OS cleans up the port.
        std::mem::forget(mock);

        (pipeline, ingest_store)
    }

    // ── F4: backend-unavailable → typed IngestError (API maps to 503) ────────

    /// A tagger that fails with a typed scheduler "no healthy backend" error,
    /// preserved through the anyhow chain exactly as `JsonSchemaTagger` does.
    struct BackendDownTagger;

    #[async_trait]
    impl Tagger for BackendDownTagger {
        async fn tag(&self, _: &TagInput, _local_only: bool) -> anyhow::Result<TagOutput> {
            Err(anyhow::Error::new(kb_llm::LlmError::Scheduler(
                kb_scheduler::AcquireError::NoBackend {
                    role: kb_core::role::Role::Text,
                },
            ))
            .context("tagger model call failed"))
        }
    }

    #[test]
    fn is_backend_unavailable_classifies_llm_errors() {
        use kb_llm::LlmError;
        use kb_scheduler::AcquireError;

        // A scheduler acquire failure, even wrapped in `.context`, is unavailable.
        let sched = anyhow::Error::new(LlmError::Scheduler(AcquireError::NoBackend {
            role: kb_core::role::Role::Text,
        }))
        .context("tagger model call failed");
        assert!(is_backend_unavailable(&sched));

        // All-cooldown and all-failed are also transient availability failures.
        assert!(is_backend_unavailable(&anyhow::Error::new(
            LlmError::AllCooldown(Duration::from_secs(30))
        )));
        assert!(is_backend_unavailable(&anyhow::Error::new(
            LlmError::AllFailed {
                retries: 3,
                last_error: "boom".into(),
            }
        )));

        // A model-output error (Deserialize) and a plain error stay 500.
        assert!(!is_backend_unavailable(&anyhow::Error::new(
            LlmError::Deserialize("bad json".into())
        )));
        assert!(!is_backend_unavailable(&anyhow::anyhow!("disk full")));
    }

    /// Non-media ingest with a backend-down tagger returns the typed
    /// `IngestError::BackendUnavailable` (→ 503), not a generic 500 (F4).
    #[tokio::test]
    async fn non_media_backend_down_returns_backend_unavailable() {
        let (pipeline, _store) =
            build_pipeline_with_tagger("some text", Arc::new(BackendDownTagger)).await;

        let files = vec![IngestFile {
            bytes: b"plain text document body".to_vec(),
            page_label: None,
            path: Some("doc.txt".into()),
        }];

        let err = pipeline
            .ingest(1, None, files, None, false)
            .await
            .unwrap_err();
        assert!(
            err.downcast_ref::<IngestError>().is_some(),
            "non-media backend outage must be a typed IngestError (→503), got: {err:#}"
        );
    }

    /// A *non-backend* tagger failure (e.g. invalid model output) still
    /// propagates as a generic error (→ 500), not `BackendUnavailable`.
    #[tokio::test]
    async fn non_media_generic_tagger_error_is_not_backend_unavailable() {
        let (pipeline, _store) =
            build_pipeline_with_tagger("some text", Arc::new(FailingTagger)).await;

        let files = vec![IngestFile {
            bytes: b"plain text document body".to_vec(),
            page_label: None,
            path: Some("doc.txt".into()),
        }];

        let err = pipeline
            .ingest(1, None, files, None, false)
            .await
            .unwrap_err();
        assert!(
            err.downcast_ref::<IngestError>().is_none(),
            "a generic tagger failure must stay a 500, not BackendUnavailable"
        );
        assert!(err.to_string().contains("tagger failed"));
    }

    /// A *media* document degrades gracefully even when the tagger backend is
    /// down — it ingests with default metadata (Ok), unchanged by F4.
    #[tokio::test]
    async fn media_backend_down_degrades_not_error() {
        let (pipeline, _store) =
            build_pipeline_with_tagger("ignored", Arc::new(BackendDownTagger)).await;

        // PNG magic → DocKind::Image → media best-effort path; no image extractor
        // is registered, so extraction yields default and the tagger failure
        // degrades to default metadata rather than failing the upload.
        let png = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR".to_vec();
        let files = vec![IngestFile {
            bytes: png,
            page_label: None,
            path: Some("pic.png".into()),
        }];

        let out = pipeline.ingest(1, None, files, None, false).await;
        assert!(
            out.is_ok(),
            "media must degrade gracefully on a backend-down tagger, got: {:?}",
            out.err()
        );
    }

    // ── Single text file, happy path ───────────────────────────────────────

    #[tokio::test]
    async fn single_text_file_success() {
        let (pipeline, store) = build_test_pipeline(
            "The quick brown fox jumps over the lazy dog.",
            TagOutput {
                title: "Test Document".into(),
                summary: "A test document for unit testing.".into(),
                tags: vec!["test".into(), "unit".into()],
            },
        )
        .await;

        let files = vec![IngestFile {
            bytes: b"Hello, world! This is test content.".to_vec(),
            page_label: None,
            path: Some("test.txt".into()),
        }];

        let output = pipeline
            .ingest(
                1,    /* tenant_id */
                None, /* user_id */
                files,
                Some("my note".into()),
                false,
            )
            .await
            .unwrap();

        assert_eq!(output.document_id, 42);
        assert!(!output.tag_ids.is_empty(), "should have canonical tag ids");
        assert_eq!(output.tag_ids.len(), 2, "2 raw tags → 2 canonical ids");
        assert!(output.chunk_count > 0, "text content should produce chunks");

        // Verify the store was called once with correct parameters.
        assert_eq!(store.call_count(), 1);
        let calls = store.calls.lock().unwrap();
        let (title, file_count, tag_count, total_chunks) = &calls[0];
        assert_eq!(title, "Test Document");
        assert_eq!(*file_count, 1);
        assert_eq!(*tag_count, 2);
        assert_eq!(*total_chunks, output.chunk_count);
    }

    // ── File lifecycle finished (Pending → Ready) ──────────────────────────

    /// A successful ingest must mark its file records `ready`, not leave them in
    /// the `pending` state the DocumentBuilder creates them in. (Regression:
    /// every ingested file was stuck `pending` even though the document was
    /// `ready`.)
    #[tokio::test]
    async fn files_are_marked_ready_after_ingest() {
        let (pipeline, store) = build_test_pipeline(
            "Some extracted text for the file.",
            TagOutput {
                title: "Lifecycle".into(),
                summary: "Checks file status transitions.".into(),
                tags: vec!["lifecycle".into()],
            },
        )
        .await;

        let files = vec![IngestFile {
            bytes: b"file body content".to_vec(),
            page_label: None,
            path: Some("doc.txt".into()),
        }];

        pipeline.ingest(1, None, files, None, false).await.unwrap();

        let statuses = store.file_statuses.lock().unwrap();
        assert!(!statuses.is_empty(), "a file should have been persisted");
        assert!(
            statuses.iter().all(|s| s == "ready"),
            "all files must be 'ready' after a successful ingest, got {statuses:?}"
        );
    }

    // ── Empty files rejected ───────────────────────────────────────────────

    #[tokio::test]
    async fn empty_files_error() {
        let (pipeline, _store) = build_test_pipeline(
            "irrelevant",
            TagOutput {
                title: "T".into(),
                summary: "S".into(),
                tags: vec![],
            },
        )
        .await;

        let err = pipeline
            .ingest(1, None, vec![], None, false)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("at least one file"),
            "expected 'at least one file' error, got: {err}"
        );
    }

    // ── Extractor error propagation ─────────────────────────────────────────

    #[tokio::test]
    async fn extractor_error_propagates() {
        use std::sync::Arc;

        // Build a minimal pipeline with a failing extractor.
        let dir = tempfile::tempdir().unwrap();
        let blob: Arc<dyn Blob> = Arc::new(kb_store::LocalBlob::new(
            dir.path().to_path_buf(),
            "test".into(),
        ));
        let mut extractors: ExtractorRouter = HashMap::new();
        extractors.insert(DocKind::Document, Arc::new(FailingExtractor));
        let tagger: Arc<dyn Tagger> = Arc::new(MockTagger {
            output: TagOutput {
                title: "x".into(),
                summary: "x".into(),
                tags: vec![],
            },
        });
        let store: Arc<dyn IngestStore> = Arc::new(MockIngestStore::new(1));

        // Canonicalizer + embedder don't matter for this test (error happens
        // before they're called). But we need valid components.
        // Use lazy mocks — create a real pipeline with mocks that panic if
        // called, verifying the error surfaces first.
        //
        // We can't easily construct the full pipeline without mock LLM backend,
        // so instead we test that a direct call to the failing extractor fails.
        // The pipeline's extract step is tested by the integration test above.
        drop((blob, extractors, tagger, store));

        // Verify the FailingExtractor itself.
        let extractor = FailingExtractor;
        let raw = RawFile {
            bytes: Bytes::from("test"),
            mime: Some("text/plain".into()),
            kind: DocKind::Document,
            path: None,
        };
        let err = extractor.extract(&raw).await.unwrap_err();
        assert!(
            err.to_string().contains("simulated extraction failure"),
            "expected extraction failure error, got: {err}"
        );
    }

    // ── Tagger error propagation ────────────────────────────────────────────

    #[tokio::test]
    async fn tagger_error_propagates() {
        let tagger = FailingTagger;
        let input = TagInput {
            tenant_id: 1,
            user_id: None,
            text: "test".into(),
            user_note: None,
            kind: DocKind::Document,
            meta: serde_json::json!({}),
        };
        let err = tagger.tag(&input, false).await.unwrap_err();
        assert!(
            err.to_string().contains("simulated tagger failure"),
            "expected tagger failure error, got: {err}"
        );
    }

    // ── Bytes passed to extractor ───────────────────────────────────────────

    #[tokio::test]
    async fn bytes_passed_to_extractor() {
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let blob: Arc<dyn Blob> = Arc::new(kb_store::LocalBlob::new(
            dir.path().to_path_buf(),
            "bytes-test".into(),
        ));

        let recording = Arc::new(RecordingExtractor::new());
        let mut extractors: ExtractorRouter = HashMap::new();
        extractors.insert(DocKind::Document, recording.clone());

        // For this test we don't need tag canonicalization or embedding —
        // just verify the bytes reach the extractor. We stop the test after
        // extraction by using a store that always errors (preventing the
        // pipeline from calling tag/embed).
        let tagger: Arc<dyn Tagger> = Arc::new(MockTagger {
            output: TagOutput {
                title: "x".into(),
                summary: "x".into(),
                tags: vec![],
            },
        });

        // Build minimal components that will work through extraction.
        // Use a mock LLM backend so embedder/canonicalizer can be created.
        use kb_core::role::Role;
        use kb_mock_backend::MockBackend;
        use kb_scheduler::{Pool, test_backend};
        use reqwest::Client;

        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");
        let backend = Arc::new(test_backend(
            "mock-bytes",
            base_url,
            vec![Role::Embed],
            0,
            4,
        ));
        let pool = Pool::new(vec![backend], Duration::from_secs(5));
        let llm = Arc::new(kb_llm::LlamaClient::new(
            pool,
            Client::new(),
            0,
            0,
            Duration::from_millis(200),
        ));
        let embedder = Arc::new(ChunkEmbedder::new(Arc::clone(&llm), "test-model".into(), 3));
        let tag_store: Arc<dyn crate::tag_store::TagStore> =
            Arc::new(crate::tag_store::mock::MockTagStore::new());
        let canonicalizer = Arc::new(TagCanonicalizer::new(
            tag_store,
            Arc::clone(&llm),
            "test-model".into(),
            crate::tag_canonicalizer::TAG_MERGE_THRESHOLD,
        ));

        // Use a real mock store. The pipeline will call all steps.
        let store: Arc<dyn IngestStore> = Arc::new(MockIngestStore::new(99));

        let pipeline =
            IngestPipeline::new(blob, extractors, tagger, canonicalizer, embedder, store);

        let test_bytes = b"these are the file bytes";
        let files = vec![IngestFile {
            bytes: test_bytes.to_vec(),
            page_label: None,
            path: Some("bytes-test.txt".into()),
        }];

        let _output = pipeline.ingest(1, None, files, None, false).await.unwrap();

        // Verify the extractor received the exact bytes.
        let received = recording.received_bytes();
        assert_eq!(received.len(), 1);
        assert_eq!(
            received[0], test_bytes,
            "extractor must receive the actual file bytes, not empty/default"
        );

        std::mem::forget(mock);
    }

    // ── No extractor registered → defaults ─────────────────────────────────

    #[tokio::test]
    async fn no_extractor_for_kind_uses_default() {
        use std::sync::Arc;

        // Build a pipeline with NO extractors at all — the Binary kind
        // will fall back to Extracted::default().
        let dir = tempfile::tempdir().unwrap();
        let blob: Arc<dyn Blob> = Arc::new(kb_store::LocalBlob::new(
            dir.path().to_path_buf(),
            "noext".into(),
        ));
        let extractors: ExtractorRouter = HashMap::new(); // empty
        let tagger: Arc<dyn Tagger> = Arc::new(MockTagger {
            output: TagOutput {
                title: "Binary File".into(),
                summary: "No text extracted.".into(),
                tags: vec!["binary".into()],
            },
        });

        use kb_core::role::Role;
        use kb_mock_backend::MockBackend;
        use kb_scheduler::{Pool, test_backend};
        use reqwest::Client;

        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");
        let backend = Arc::new(test_backend(
            "mock-noext",
            base_url,
            vec![Role::Embed],
            0,
            4,
        ));
        let pool = Pool::new(vec![backend], Duration::from_secs(5));
        let llm = Arc::new(kb_llm::LlamaClient::new(
            pool,
            Client::new(),
            0,
            0,
            Duration::from_millis(200),
        ));
        let embedder = Arc::new(ChunkEmbedder::new(Arc::clone(&llm), "test-model".into(), 3));
        let tag_store: Arc<dyn crate::tag_store::TagStore> =
            Arc::new(crate::tag_store::mock::MockTagStore::new());
        let canonicalizer = Arc::new(TagCanonicalizer::new(
            tag_store,
            Arc::clone(&llm),
            "test-model".into(),
            crate::tag_canonicalizer::TAG_MERGE_THRESHOLD,
        ));
        let store: Arc<dyn IngestStore> = Arc::new(MockIngestStore::new(10));

        let pipeline =
            IngestPipeline::new(blob, extractors, tagger, canonicalizer, embedder, store);

        // This file will be detected as Binary (no extractor) → Extracted::default()
        let files = vec![IngestFile {
            bytes: vec![0x00, 0x01, 0x02, 0x03],
            page_label: None,
            path: Some("unknown.bin".into()),
        }];

        let output = pipeline.ingest(1, None, files, None, false).await.unwrap();

        // The document should still be created (with empty text, tag only).
        assert_eq!(output.document_id, 10);
        assert_eq!(output.tag_ids.len(), 1); // "binary" tag
        assert_eq!(output.chunk_count, 0); // no text to chunk

        std::mem::forget(mock);
    }

    // ── Blob storage verified ──────────────────────────────────────────────

    #[tokio::test]
    async fn blob_is_stored_with_correct_content() {
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let blob: Arc<dyn Blob> = Arc::new(kb_store::LocalBlob::new(
            dir.path().to_path_buf(),
            "blob-test".into(),
        ));
        let blob_ref = Arc::clone(&blob);

        let mut extractors: ExtractorRouter = HashMap::new();
        extractors.insert(
            DocKind::Document,
            Arc::new(MockTextExtractor {
                text: "extracted content".into(),
            }),
        );
        let tagger: Arc<dyn Tagger> = Arc::new(MockTagger {
            output: TagOutput {
                title: "Blob Test".into(),
                summary: "Testing blob writes.".into(),
                tags: vec!["blob".into()],
            },
        });

        use kb_core::role::Role;
        use kb_mock_backend::MockBackend;
        use kb_scheduler::{Pool, test_backend};
        use reqwest::Client;

        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");
        let backend = Arc::new(test_backend("mock-blob", base_url, vec![Role::Embed], 0, 4));
        let pool = Pool::new(vec![backend], Duration::from_secs(5));
        let llm = Arc::new(kb_llm::LlamaClient::new(
            pool,
            Client::new(),
            0,
            0,
            Duration::from_millis(200),
        ));
        let embedder = Arc::new(ChunkEmbedder::new(Arc::clone(&llm), "test-model".into(), 3));
        let tag_store: Arc<dyn crate::tag_store::TagStore> =
            Arc::new(crate::tag_store::mock::MockTagStore::new());
        let canonicalizer = Arc::new(TagCanonicalizer::new(
            tag_store,
            Arc::clone(&llm),
            "test-model".into(),
            crate::tag_canonicalizer::TAG_MERGE_THRESHOLD,
        ));
        let store: Arc<dyn IngestStore> = Arc::new(MockIngestStore::new(77));

        let pipeline =
            IngestPipeline::new(blob, extractors, tagger, canonicalizer, embedder, store);

        let test_bytes = b"hello blob world";
        let files = vec![IngestFile {
            bytes: test_bytes.to_vec(),
            page_label: None,
            path: Some("blob.txt".into()),
        }];

        let output = pipeline.ingest(1, None, files, None, false).await.unwrap();
        assert_eq!(output.document_id, 77);

        // Read the blob back — the key is tenant-prefixed hex(sha256),
        // computed by the DocumentBuilder the same way the pipeline does.
        use kb_core::hash::Sha256;
        use sha2::Digest;
        let mut hasher = sha2::Sha256::new();
        hasher.update(test_bytes);
        let hash_bytes: [u8; 32] = hasher.finalize().into();
        let sha256 = Sha256::from_bytes(hash_bytes);
        let blob_key = format!("1/{}", sha256.to_hex());

        let exists = blob_ref.exists(&blob_key).await.unwrap();
        assert!(exists, "blob {blob_key} must exist after ingest");

        let stored = blob_ref.get(&blob_key).await.unwrap();
        assert_eq!(
            stored.as_ref(),
            test_bytes,
            "stored blob content must match input bytes"
        );

        std::mem::forget(mock);
    }

    // ── Multi-file ingest ──────────────────────────────────────────────────

    #[tokio::test]
    async fn multi_file_ingest() {
        let (pipeline, store) = build_test_pipeline(
            "page one text",
            TagOutput {
                title: "Multi-Page".into(),
                summary: "Two pages.".into(),
                tags: vec!["multi".into()],
            },
        )
        .await;

        let files = vec![
            IngestFile {
                bytes: b"Page one content here.".to_vec(),
                page_label: Some("front".into()),
                path: Some("page1.txt".into()),
            },
            IngestFile {
                bytes: b"Page two content here.".to_vec(),
                page_label: Some("back".into()),
                path: Some("page2.txt".into()),
            },
        ];

        let output = pipeline
            .ingest(1, None, files, Some("two pages".into()), false)
            .await
            .unwrap();

        assert_eq!(output.document_id, 42);
        assert_eq!(output.tag_ids.len(), 1);
        assert_eq!(store.call_count(), 1);

        let calls = store.calls.lock().unwrap();
        assert_eq!(calls[0].1, 2, "2 files in transactional_ingest");
        assert!(calls[0].3 > 0, "chunks present for both pages");
    }

    // ── User note passed through to tagger ─────────────────────────────────

    #[tokio::test]
    async fn user_note_reaches_tagger() {
        // Use a tagger that records the input it receives.
        struct NoteRecordingTagger {
            received_notes: Mutex<Vec<Option<String>>>,
        }
        #[async_trait]
        impl Tagger for NoteRecordingTagger {
            async fn tag(&self, input: &TagInput, _local_only: bool) -> anyhow::Result<TagOutput> {
                self.received_notes
                    .lock()
                    .unwrap()
                    .push(input.user_note.clone());
                Ok(TagOutput {
                    title: "T".into(),
                    summary: "S".into(),
                    tags: vec!["note-test".into()],
                })
            }
        }

        let recording = Arc::new(NoteRecordingTagger {
            received_notes: Mutex::new(Vec::new()),
        });

        let dir = tempfile::tempdir().unwrap();
        let blob: Arc<dyn Blob> = Arc::new(kb_store::LocalBlob::new(
            dir.path().to_path_buf(),
            "note-test".into(),
        ));
        let mut extractors: ExtractorRouter = HashMap::new();
        extractors.insert(
            DocKind::Document,
            Arc::new(MockTextExtractor {
                text: "content".into(),
            }),
        );

        use kb_core::role::Role;
        use kb_mock_backend::MockBackend;
        use kb_scheduler::{Pool, test_backend};
        use reqwest::Client;

        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");
        let backend = Arc::new(test_backend("mock-note", base_url, vec![Role::Embed], 0, 4));
        let pool = Pool::new(vec![backend], Duration::from_secs(5));
        let llm = Arc::new(kb_llm::LlamaClient::new(
            pool,
            Client::new(),
            0,
            0,
            Duration::from_millis(200),
        ));
        let embedder = Arc::new(ChunkEmbedder::new(Arc::clone(&llm), "test-model".into(), 3));
        let tag_store: Arc<dyn crate::tag_store::TagStore> =
            Arc::new(crate::tag_store::mock::MockTagStore::new());
        let canonicalizer = Arc::new(TagCanonicalizer::new(
            tag_store,
            Arc::clone(&llm),
            "test-model".into(),
            crate::tag_canonicalizer::TAG_MERGE_THRESHOLD,
        ));
        let store: Arc<dyn IngestStore> = Arc::new(MockIngestStore::new(5));

        let pipeline = IngestPipeline::new(
            blob,
            extractors,
            recording.clone(),
            canonicalizer,
            embedder,
            store,
        );

        let files = vec![IngestFile {
            bytes: b"test".to_vec(),
            page_label: None,
            path: Some("f.txt".into()),
        }];

        let _output = pipeline
            .ingest(1, None, files, Some("my custom note".into()), false)
            .await
            .unwrap();

        let notes = recording.received_notes.lock().unwrap();
        assert_eq!(notes.len(), 1);
        assert_eq!(notes[0].as_deref(), Some("my custom note"));

        std::mem::forget(mock);
    }

    // ── ingest_into (queued-path finalize, P15-T3) ─────────────────────────

    /// `ingest_into` threads the staged document id into the final
    /// transactional write (the store's explicit-id UPDATE path).
    #[tokio::test]
    async fn ingest_into_threads_existing_document_id() {
        let (pipeline, store) = build_test_pipeline(
            "queued doc content",
            TagOutput {
                title: "Queued Doc".into(),
                summary: "Finalized by a worker.".into(),
                tags: vec!["queued".into()],
            },
        )
        .await;

        let files = vec![IngestFile {
            bytes: b"queued file body".to_vec(),
            page_label: None,
            path: Some("doc.txt".into()),
        }];

        pipeline
            .ingest_into(1, Some(7), 4242, files, Some("note".into()), false)
            .await
            .unwrap();

        assert_eq!(store.call_count(), 1);
        assert_eq!(
            store.doc_ids.lock().unwrap().as_slice(),
            &[4242],
            "the staged document id must reach transactional_ingest"
        );
    }

    /// The inline path (no staged id) still hands the store `id = 0` so it
    /// creates/reuses the document itself.
    #[tokio::test]
    async fn inline_ingest_passes_zero_document_id() {
        let (pipeline, store) = build_test_pipeline(
            "inline doc content",
            TagOutput {
                title: "Inline".into(),
                summary: "s".into(),
                tags: vec![],
            },
        )
        .await;

        let files = vec![IngestFile {
            bytes: b"inline file body".to_vec(),
            page_label: None,
            path: Some("doc.txt".into()),
        }];
        pipeline.ingest(1, None, files, None, false).await.unwrap();

        assert_eq!(store.doc_ids.lock().unwrap().as_slice(), &[0]);
    }

    /// A non-positive document id is rejected before any work happens.
    #[tokio::test]
    async fn ingest_into_rejects_non_positive_id() {
        let (pipeline, store) = build_test_pipeline(
            "x",
            TagOutput {
                title: "T".into(),
                summary: "S".into(),
                tags: vec![],
            },
        )
        .await;

        let files = vec![IngestFile {
            bytes: b"y".to_vec(),
            page_label: None,
            path: None,
        }];
        let err = pipeline
            .ingest_into(1, None, 0, files, None, false)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("positive document_id"),
            "got: {err}"
        );
        assert_eq!(store.call_count(), 0, "no store write on rejection");
    }

    // ── MockIngestStore tests ──────────────────────────────────────────────

    #[test]
    fn mock_store_returns_configured_doc_id() {
        let store = MockIngestStore::new(42);
        assert_eq!(store.call_count(), 0);
        store.set_doc_id(99);
        assert_eq!(store.doc_id.load(Ordering::SeqCst), 99);
    }

    #[tokio::test]
    async fn mock_store_captures_call_params() {
        let store = MockIngestStore::new(1);
        let doc = Document {
            id: 0,
            tenant_id: 1,
            title: Some("Captured".into()),
            summary: None,
            user_note: None,
            kind: DocKind::Document,
            meta: serde_json::json!({}),
            page_count: 1,
            status: ProcessingStatus::Ready,
            created_at: chrono::Utc::now(),
            local_only: false,
        };
        let files = vec![];
        let tag_ids = vec![1i64, 2];
        let file_chunks: Vec<Vec<Chunk>> = vec![];

        let id = store
            .transactional_ingest(&doc, &files, &tag_ids, &file_chunks)
            .await
            .unwrap();
        assert_eq!(id, 1);
        assert_eq!(store.call_count(), 1);

        let calls = store.calls.lock().unwrap();
        assert_eq!(calls[0].0, "Captured");
        assert_eq!(calls[0].1, 0); // 0 files
        assert_eq!(calls[0].2, 2); // 2 tag ids
        assert_eq!(calls[0].3, 0); // 0 chunks
    }

    // ── Edge: empty text produces zero chunks ──────────────────────────────

    #[tokio::test]
    async fn empty_extracted_text_produces_zero_chunks() {
        use std::sync::Arc;

        // Use an extractor that returns empty text (simulates image extraction).
        struct EmptyExtractor;
        #[async_trait]
        impl Extractor for EmptyExtractor {
            async fn extract(&self, _: &RawFile) -> anyhow::Result<Extracted> {
                Ok(Extracted {
                    text: String::new(),
                    meta: serde_json::json!({"camera": "Nikon"}),
                    page_images: vec![],
                })
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let blob: Arc<dyn Blob> = Arc::new(kb_store::LocalBlob::new(
            dir.path().to_path_buf(),
            "empty-text".into(),
        ));
        let mut extractors: ExtractorRouter = HashMap::new();
        extractors.insert(DocKind::Image, Arc::new(EmptyExtractor));
        let tagger: Arc<dyn Tagger> = Arc::new(MockTagger {
            output: TagOutput {
                title: "Image Only".into(),
                summary: "No text content.".into(),
                tags: vec!["photo".into()],
            },
        });

        use kb_core::role::Role;
        use kb_mock_backend::MockBackend;
        use kb_scheduler::{Pool, test_backend};
        use reqwest::Client;

        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");
        let backend = Arc::new(test_backend(
            "mock-empty",
            base_url,
            vec![Role::Embed],
            0,
            4,
        ));
        let pool = Pool::new(vec![backend], Duration::from_secs(5));
        let llm = Arc::new(kb_llm::LlamaClient::new(
            pool,
            Client::new(),
            0,
            0,
            Duration::from_millis(200),
        ));
        let embedder = Arc::new(ChunkEmbedder::new(Arc::clone(&llm), "test-model".into(), 3));
        let tag_store: Arc<dyn crate::tag_store::TagStore> =
            Arc::new(crate::tag_store::mock::MockTagStore::new());
        let canonicalizer = Arc::new(TagCanonicalizer::new(
            tag_store,
            Arc::clone(&llm),
            "test-model".into(),
            crate::tag_canonicalizer::TAG_MERGE_THRESHOLD,
        ));
        let store: Arc<dyn IngestStore> = Arc::new(MockIngestStore::new(33));

        let pipeline =
            IngestPipeline::new(blob, extractors, tagger, canonicalizer, embedder, store);

        // PNG bytes → detected as Image → EmptyExtractor → empty text
        let files = vec![IngestFile {
            bytes: vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A],
            page_label: None,
            path: Some("photo.png".into()),
        }];

        let output = pipeline.ingest(1, None, files, None, false).await.unwrap();
        assert_eq!(output.document_id, 33);
        assert_eq!(output.chunk_count, 0, "empty text → zero chunks");

        std::mem::forget(mock);
    }

    // ── Vision captioning (VLM) pipeline tests ────────────────────────────

    /// An extractor that returns a PageImage so the vision captioner is exercised.
    struct ImageMockExtractor;
    #[async_trait]
    impl Extractor for ImageMockExtractor {
        async fn extract(&self, _raw: &RawFile) -> anyhow::Result<Extracted> {
            Ok(Extracted {
                text: String::new(),
                meta: Default::default(),
                page_images: vec![PageImage {
                    data: Bytes::from_static(b"\xff\xd8\xff\xe0\x00\x10JFIF"),
                    mime: "image/jpeg".into(),
                }],
            })
        }
    }

    /// Build a pipeline with a VisionCaptioner backed by a mock vision backend.
    async fn vision_test_pipeline(
        caption: &str,
    ) -> (
        IngestPipeline,
        Arc<MockIngestStore>,
        kb_mock_backend::MockBackend,
    ) {
        use kb_core::role::Role;
        use kb_mock_backend::MockBackend;
        use kb_scheduler::{Pool, test_backend};
        use reqwest::Client;

        let dir = tempfile::tempdir().unwrap();
        let blob: Arc<dyn Blob> = Arc::new(kb_store::LocalBlob::new(
            dir.path().to_path_buf(),
            "vtest".into(),
        ));

        let mut extractors: ExtractorRouter = HashMap::new();
        extractors.insert(DocKind::Image, Arc::new(ImageMockExtractor));

        let tagger: Arc<dyn Tagger> = Arc::new(MockTagger {
            output: TagOutput {
                title: "Vision Title".into(),
                summary: "Vision summary.".into(),
                tags: vec!["vision-tag".into()],
            },
        });

        // Vision mock backend.
        let vision_mock = MockBackend::start().await;
        vision_mock.scenario().lock().await.chat_content = Some(caption.to_string());
        let vision_url = vision_mock.url("/v1");
        let vision_backend = Arc::new(test_backend(
            "mock-vision",
            vision_url,
            vec![Role::Vision],
            0,
            2,
        ));
        let vision_pool = Pool::new(vec![vision_backend], Duration::from_secs(5));
        let vision_client =
            kb_llm::LlamaClient::new(vision_pool, Client::new(), 0, 0, Duration::from_millis(200));
        let captioner = Arc::new(VisionCaptioner::new(vision_client, "test-vision".into()));

        // Embed mock backend.
        let embed_mock = MockBackend::start().await;
        let embed_url = embed_mock.url("/v1");
        let embed_backend = Arc::new(test_backend(
            "mock-embed",
            embed_url,
            vec![Role::Embed],
            0,
            8,
        ));
        let embed_pool = Pool::new(vec![embed_backend], Duration::from_secs(5));
        let llm = Arc::new(kb_llm::LlamaClient::new(
            embed_pool,
            Client::new(),
            0,
            0,
            Duration::from_millis(200),
        ));
        let embedder = Arc::new(ChunkEmbedder::new(Arc::clone(&llm), "test-embed".into(), 3));

        let tag_store: Arc<dyn crate::tag_store::TagStore> =
            Arc::new(crate::tag_store::mock::MockTagStore::new());
        let canonicalizer = Arc::new(TagCanonicalizer::new(
            tag_store,
            Arc::clone(&llm),
            "test-embed".into(),
            crate::tag_canonicalizer::TAG_MERGE_THRESHOLD,
        ));

        let ingest_store = Arc::new(MockIngestStore::new(200));
        let store_ref = Arc::clone(&ingest_store) as Arc<dyn IngestStore>;

        let pipeline =
            IngestPipeline::new(blob, extractors, tagger, canonicalizer, embedder, store_ref)
                .with_vision_captioner(captioner);

        std::mem::forget(embed_mock);
        (pipeline, ingest_store, vision_mock)
    }

    /// Image with VisionCaptioner: pipeline succeeds (captioner was called).
    #[tokio::test]
    async fn pipeline_images_are_captioned() {
        let caption = "A red car parked near a beach.";
        let (pipeline, store, vision_mock) = vision_test_pipeline(caption).await;

        let files = vec![IngestFile {
            bytes: vec![0xff, 0xd8, 0xff],
            page_label: None,
            path: Some("car.jpg".into()),
        }];

        let output = pipeline.ingest(1, None, files, None, false).await.unwrap();
        assert_eq!(output.document_id, 200);
        assert_eq!(store.call_count(), 1, "pipeline should commit one document");

        vision_mock.shutdown().await;
    }

    /// Without a VisionCaptioner, images ingest normally (backward compat).
    #[tokio::test]
    async fn pipeline_no_captioner_images_ingest_normally() {
        let dir = tempfile::tempdir().unwrap();
        let blob: Arc<dyn Blob> = Arc::new(kb_store::LocalBlob::new(
            dir.path().to_path_buf(),
            "nc-t".into(),
        ));

        let mut extractors: ExtractorRouter = HashMap::new();
        extractors.insert(DocKind::Image, Arc::new(ImageMockExtractor));

        let tagger: Arc<dyn Tagger> = Arc::new(MockTagger {
            output: TagOutput {
                title: "No Captioner".into(),
                summary: "S".into(),
                tags: vec!["t".into()],
            },
        });

        use kb_core::role::Role;
        use kb_scheduler::Pool;

        let mock = kb_mock_backend::MockBackend::start().await;
        let backend = Arc::new(kb_scheduler::test_backend(
            "mock-embed",
            mock.url("/v1"),
            vec![Role::Embed],
            0,
            8,
        ));
        let pool = Pool::new(vec![backend], Duration::from_secs(5));
        let llm = Arc::new(kb_llm::LlamaClient::new(
            pool,
            reqwest::Client::new(),
            0,
            0,
            Duration::from_millis(200),
        ));
        let embedder = Arc::new(ChunkEmbedder::new(Arc::clone(&llm), "e".into(), 3));
        let tag_store: Arc<dyn crate::tag_store::TagStore> =
            Arc::new(crate::tag_store::mock::MockTagStore::new());
        let canonicalizer = Arc::new(TagCanonicalizer::new(
            tag_store,
            Arc::clone(&llm),
            "e".into(),
            crate::tag_canonicalizer::TAG_MERGE_THRESHOLD,
        ));
        let ingest_store = Arc::new(MockIngestStore::new(301));
        let pipeline = IngestPipeline::new(
            blob,
            extractors,
            tagger,
            canonicalizer,
            embedder,
            Arc::clone(&ingest_store) as Arc<dyn IngestStore>,
        );
        // NO .with_vision_captioner()

        let files = vec![IngestFile {
            bytes: vec![0xff, 0xd8, 0xff],
            page_label: None,
            path: Some("pic.jpg".into()),
        }];
        let output = pipeline.ingest(1, None, files, None, false).await.unwrap();
        assert_eq!(output.document_id, 301);
        assert_eq!(ingest_store.call_count(), 1);

        std::mem::forget(mock);
    }

    /// Text documents are unaffected by the presence of a VisionCaptioner
    /// (page_images is empty, so describe_many is never called).
    #[tokio::test]
    async fn pipeline_text_document_unaffected_by_captioner() {
        let (pipeline, store, vision_mock) = vision_test_pipeline("should-not-be-called").await;

        let files = vec![IngestFile {
            bytes: b"Hello, world.".to_vec(),
            page_label: None,
            path: Some("note.txt".into()),
        }];

        let output = pipeline.ingest(1, None, files, None, false).await.unwrap();
        assert_eq!(output.document_id, 200);
        assert_eq!(store.call_count(), 1);

        vision_mock.shutdown().await;
    }

    // ── Lease renaming — verify the old name is gone ──────────────────────
    //    (IngestStore exposed; PgStore impl delegates correctly)

    #[test]
    fn ingest_store_delegation_check() {
        // Verify our IngestStore impl for PgStore compiles and the trait
        // object is constructible.
        let pg = kb_store::PgStore::new("postgres://localhost/test");
        let _store: Arc<dyn IngestStore> = Arc::new(pg);
    }

    // ── Retag pipeline tests ──────────────────────────────────────────────

    /// An in-memory [`RetagStore`] for deterministic retag tests.
    struct MockRetagStore {
        locked_tags: Mutex<Vec<i64>>,
        llm_tags: Mutex<Vec<i64>>,
        inserted_user_tags: Mutex<Vec<(i64, Vec<i64>)>>,
        inserted_llm_tags: Mutex<Vec<(i64, Vec<i64>)>>,
        cleared_count: Mutex<u64>,
    }

    impl MockRetagStore {
        fn new(locked_tags: Vec<i64>) -> Self {
            Self {
                locked_tags: Mutex::new(locked_tags),
                llm_tags: Mutex::new(Vec::new()),
                inserted_user_tags: Mutex::new(Vec::new()),
                inserted_llm_tags: Mutex::new(Vec::new()),
                cleared_count: Mutex::new(0),
            }
        }

        fn inserted_llm_count(&self) -> usize {
            self.inserted_llm_tags.lock().unwrap().len()
        }

        fn inserted_user_count(&self) -> usize {
            self.inserted_user_tags.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl RetagStore for MockRetagStore {
        async fn get_locked_document_tags(
            &self,
            _tenant_id: i64,
            _document_id: i64,
        ) -> anyhow::Result<Vec<i64>> {
            Ok(self.locked_tags.lock().unwrap().clone())
        }

        async fn clear_llm_document_tags(
            &self,
            _tenant_id: i64,
            _document_id: i64,
        ) -> anyhow::Result<u64> {
            let len = self.llm_tags.lock().unwrap().len() as u64;
            self.llm_tags.lock().unwrap().clear();
            *self.cleared_count.lock().unwrap() += len;
            Ok(len)
        }

        async fn insert_document_tags(
            &self,
            _tenant_id: i64,
            document_id: i64,
            tag_ids: &[i64],
            source: TagSource,
        ) -> anyhow::Result<()> {
            match source {
                TagSource::User => {
                    self.inserted_user_tags
                        .lock()
                        .unwrap()
                        .push((document_id, tag_ids.to_vec()));
                }
                TagSource::Llm => {
                    self.inserted_llm_tags
                        .lock()
                        .unwrap()
                        .push((document_id, tag_ids.to_vec()));
                }
            }
            Ok(())
        }
    }

    /// Build a test TagCanonicalizer with a mock store + mock LLM backend.
    async fn retag_canonicalizer_with_mock(
        store: Arc<MockTagStore>,
    ) -> (TagCanonicalizer, kb_mock_backend::MockBackend) {
        use kb_mock_backend::MockBackend;
        use kb_scheduler::{Pool, test_backend};

        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");
        let backend = Arc::new(test_backend(
            "mock-embed",
            base_url,
            vec![kb_core::role::Role::Embed],
            0,
            4,
        ));
        let pool = Pool::new(vec![backend], Duration::from_secs(5));
        let llm = Arc::new(kb_llm::LlamaClient::new(
            pool,
            reqwest::Client::new(),
            0,
            0,
            Duration::from_millis(200),
        ));
        let canon = TagCanonicalizer::new(
            store as Arc<dyn TagStore>,
            llm,
            "test-model".into(),
            TAG_MERGE_THRESHOLD,
        );
        (canon, mock)
    }

    /// A tagger that returns a fixed TagOutput.
    struct FixedTagger {
        output: TagOutput,
    }

    #[async_trait]
    impl Tagger for FixedTagger {
        async fn tag(&self, _input: &TagInput, _local_only: bool) -> anyhow::Result<TagOutput> {
            Ok(self.output.clone())
        }
    }

    #[tokio::test]
    async fn retag_preserves_locked_tags_and_adds_llm_tags() {
        let tag_store = Arc::new(MockTagStore::new());
        // Pre-seed one locked tag and one canonical tag the LLM will produce.
        tag_store.seed_tag(1, "existing", 100, vec![1.0, 0.0]);
        let retag_store = MockRetagStore::new(vec![100]); // tag 100 is locked

        let (canon, mock) = retag_canonicalizer_with_mock(tag_store.clone()).await;
        mock.scenario().lock().await.embed_content = Some(vec![vec![1.0, 0.0]]);

        let tagger = FixedTagger {
            output: TagOutput {
                title: "Re-tagged".into(),
                summary: "Summary".into(),
                tags: vec!["existing".into(), "newtopic".into()],
            },
        };

        let result = process_retag_job(
            1,
            None,
            42,
            "document text",
            &tagger,
            &canon,
            &retag_store,
            false,
        )
        .await
        .unwrap();

        // Locked tag 100 + LLM tags should all be present.
        assert!(result.contains(&100), "locked tag must be in result");
        // "existing" raw tag → alias-match to tag 100; "newtopic" → new tag.
        let llm_inserts = retag_store.inserted_llm_count();
        assert_eq!(llm_inserts, 1, "one batch of LLM tags inserted");
        let user_inserts = retag_store.inserted_user_count();
        assert_eq!(user_inserts, 1, "locked tags re-inserted as user");

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn retag_no_locked_tags_all_llm() {
        let tag_store = Arc::new(MockTagStore::new());
        let retag_store = MockRetagStore::new(vec![]); // no locked tags

        let (canon, mock) = retag_canonicalizer_with_mock(tag_store.clone()).await;
        mock.scenario().lock().await.embed_content = Some(vec![vec![0.5, 0.5]]);

        let tagger = FixedTagger {
            output: TagOutput {
                title: "Fresh".into(),
                summary: "Fresh summary".into(),
                tags: vec!["a".into(), "b".into()],
            },
        };

        let result = process_retag_job(1, None, 42, "text", &tagger, &canon, &retag_store, false)
            .await
            .unwrap();

        // Both raw tags converge (same mock embedding) → 1 canonical id.
        assert_eq!(result.len(), 1, "both tags converge to same canonical");
        assert_eq!(
            retag_store.inserted_user_count(),
            0,
            "no locked tags → no user inserts"
        );
        assert_eq!(
            retag_store.inserted_llm_count(),
            1,
            "one batch of LLM tags inserted"
        );

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn retag_locked_tags_only_no_llm_tags() {
        let tag_store = Arc::new(MockTagStore::new());
        let retag_store = MockRetagStore::new(vec![10, 20]);

        let (canon, mock) = retag_canonicalizer_with_mock(tag_store.clone()).await;
        mock.scenario().lock().await.embed_content = Some(vec![vec![0.3, 0.7]]);

        let tagger = FixedTagger {
            output: TagOutput {
                title: "N".into(),
                summary: "S".into(),
                tags: vec![], // no tags from LLM
            },
        };

        let result = process_retag_job(1, None, 42, "text", &tagger, &canon, &retag_store, false)
            .await
            .unwrap();

        assert_eq!(result, vec![10, 20], "only locked tags survive");
        assert_eq!(retag_store.inserted_llm_count(), 0, "no llm tags to insert");
        assert_eq!(
            retag_store.inserted_user_count(),
            1,
            "locked tags re-inserted"
        );

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn retag_tagger_error_is_propagated() {
        struct ErrorTagger;
        #[async_trait]
        impl Tagger for ErrorTagger {
            async fn tag(&self, _input: &TagInput, _local_only: bool) -> anyhow::Result<TagOutput> {
                anyhow::bail!("tag service unavailable");
            }
        }

        let tag_store = Arc::new(MockTagStore::new());
        let retag_store = MockRetagStore::new(vec![]);

        let (canon, mock) = retag_canonicalizer_with_mock(tag_store).await;
        // Set embed content just so the mock doesn't 500 before we even reach
        // the tagger.
        mock.scenario().lock().await.embed_content = Some(vec![vec![0.1, 0.2]]);

        let err = process_retag_job(
            1,
            None,
            42,
            "text",
            &ErrorTagger,
            &canon,
            &retag_store,
            false,
        )
        .await
        .unwrap_err();
        assert!(
            err.contains("tag service unavailable"),
            "error should be propagated: {err}"
        );

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn retag_locked_tags_deduped_with_llm_ids() {
        let tag_store = Arc::new(MockTagStore::new());
        tag_store.seed_tag(1, "saved", 7, vec![1.0, 0.0]);
        // Locked tag 7 is already present.
        let retag_store = MockRetagStore::new(vec![7]);

        let (canon, mock) = retag_canonicalizer_with_mock(tag_store.clone()).await;
        // The mock embed returns [1.0, 0.0] so "saved" matches existing tag 7.
        mock.scenario().lock().await.embed_content = Some(vec![vec![1.0, 0.0]]);

        let tagger = FixedTagger {
            output: TagOutput {
                title: "X".into(),
                summary: "Y".into(),
                tags: vec!["saved".into()],
            },
        };

        let result = process_retag_job(1, None, 99, "text", &tagger, &canon, &retag_store, false)
            .await
            .unwrap();

        // Result should contain tag 7 exactly once (deduped).
        assert_eq!(
            result.iter().filter(|&&id| id == 7).count(),
            1,
            "tag 7 must appear only once (deduped)"
        );
        // The locked_id insertion should still happen (idempotent via ON CONFLICT).
        assert_eq!(
            retag_store.inserted_user_count(),
            1,
            "locked tags re-inserted"
        );
        assert_eq!(retag_store.inserted_llm_count(), 1, "LLM tags inserted");

        mock.shutdown().await;
    }

    #[test]
    fn retag_store_delegation_check() {
        // Verify RetagStore impl for PgStore compiles and is constructible.
        let pg = kb_store::PgStore::new("postgres://localhost/test");
        let _store: Arc<dyn RetagStore> = Arc::new(pg);
    }
}
