// SPDX-License-Identifier: AGPL-3.0-or-later

//! `POST /api/ingest` — multipart file upload handler.
//!
//! Accepts one or more files, an optional `user_note` text field, and an
//! optional `group_as_document` flag. Each file's bytes are stored in the
//! blob store and an ingest job is enqueued via the job queue.
//!
//! Two processing modes, selected per request by the hot-swappable
//! `[ingest].mode` (P15-T5): **queued** (default) stages a pending document +
//! enqueues a job for background workers and returns immediately; **inline**
//! runs the full pipeline synchronously (the rollback lever). The shared
//! helpers ([`process_upload_queued`], [`process_upload_inline`],
//! [`process_add_page_queued`]) serve both the JSON API and the Web UI.
//!
//! # Upload security (plan §17, §31.5)
//!
//! Every uploaded file passes through [`kb_extract::security::validate_upload`]
//! before blob storage or job enqueue. This enforces:
//! - Per-file size cap (500 MiB)
//! - Path-traversal detection on filenames
//! - MIME-type allow-list via magic-byte inspection

use std::sync::Arc;

use axum::Extension;
use axum::extract::{Multipart, State};
use axum::http::StatusCode;
use axum::response::Json;
use kb_core::blob::Blob;
use kb_core::job::JobKind;
use kb_extract::security;
use kb_pipeline::ingest::{IngestError, IngestFile, IngestPipeline};
use kb_pipeline::job_queue::JobQueue;
use serde::Serialize;

use crate::AppState;
use crate::AuthUser;

// ── Response types ─────────────────────────────────────────────────────────────

/// Successful ingest response (202 Accepted).
#[derive(Debug, Serialize)]
pub struct IngestResponse {
    /// The enqueued job id.
    job_id: i64,
    /// Always `null` until the job completes (then the worker sets it).
    document_id: Option<i64>,
    /// Human-readable message.
    message: String,
}

/// Error response body.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    /// Machine-readable error code.
    error: String,
    /// Human-readable description.
    message: String,
}

/// Payload size limit for ingest requests: 100 MiB.
pub const MAX_PAYLOAD_BYTES: u64 = 100 * 1024 * 1024;

/// Extra headroom on the HTTP body limit for multipart framing overhead
/// (boundaries, Content-Disposition headers, MIME metadata). The payload
/// limit is still enforced by [`parse_multipart`]; this headroom only ensures
/// axum's [`DefaultBodyLimit`] does not reject a request whose *file* bytes
/// are exactly at [`MAX_PAYLOAD_BYTES`] before the application can parse it.
pub const MULTIPART_FRAMING_HEADROOM: u64 = 64 * 1024; // 64 KiB

/// Build a user-facing error message from a [`QuotaError`], including an
/// upsell suggestion when plan context is available (P11-T5).
pub(crate) fn quota_error_response(err: &kb_core::quota::QuotaError) -> String {
    let mut msg = err.to_string();
    if let Some(upsell) = err.upsell_message() {
        msg.push_str(". ");
        msg.push_str(&upsell);
    }
    msg
}

// ── Handler ────────────────────────────────────────────────────────────────────

/// `POST /api/ingest` — multipart upload of 1..N files, enqueues an ingest job.
///
/// # Request
///
/// Multipart form data:
/// * `files` (required, repeated) — file parts.
/// * `user_note` (optional) — free-text note attached to the document.
/// * `group_as_document` (optional) — when `"true"`, all files become pages of
///   one document; otherwise each file creates its own one-page document.
///
/// # Response
///
/// * `202 Accepted` — [`IngestResponse`] with the enqueued job id.
/// * `400 Bad Request` — no files provided, unsupported MIME, or malformed body.
/// * `401 Unauthorized` — rejected by middleware before this handler runs.
/// * `413 Payload Too Large` — total upload exceeds [`MAX_PAYLOAD_BYTES`].
/// * `429 Too Many Requests` — server at ingest capacity, retry after backoff.
/// * `503 Service Unavailable` — the model backend is temporarily unavailable
///   (e.g. the tagger has no healthy backend); retry after `Retry-After` (F4).
/// * `500 Internal Server Error` — pipeline or store failure.
pub async fn ingest(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    multipart: Multipart,
) -> Result<(StatusCode, Json<IngestResponse>), (StatusCode, Json<ErrorResponse>)> {
    // ── Suspend check: suspended tenants are read-only (§29, P11-T4) ─────────
    if let Err(e) = state
        .pg_store
        .check_tenant_not_suspended(auth_user.tenant_id)
        .await
    {
        let msg = e.to_string();
        // If the error contains "suspended", it's a client-visible 403.
        if msg.contains("suspended") {
            return Err((
                StatusCode::FORBIDDEN,
                Json(ErrorResponse {
                    error: "account_suspended".into(),
                    message: msg,
                }),
            ));
        }
        // Otherwise it's a database error (the method already logs internally).
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "internal_error".into(),
                message: "an unexpected error occurred".into(),
            }),
        ));
    }

    // ── Resolve the ingest mode per request (hot-swappable, P15-T5) ──────────
    // "queued" (default): stage + enqueue, workers process in the background;
    // "inline": the previous synchronous path (rollback lever — edit
    // config.toml, no restart).
    let ingest_cfg = state
        .app_config
        .as_ref()
        .map(|c| c.current().ingest.clone())
        .unwrap_or_default();
    let queued_mode = ingest_cfg.mode != "inline";

    // ── Backpressure: acquire an in-flight slot or return 429 (P8-T9, P14-T7) ──
    // INLINE mode only: the limiter gated the pipeline work that ran inside
    // this request. In queued mode the request itself is cheap (validate +
    // blob put + two inserts) and the backlog is bounded by the queue caps
    // instead (P15-T5).
    let _permit = if queued_mode {
        None
    } else if let Some(limiter) = &state.inflight_limiter {
        match limiter.try_acquire(auth_user.tenant_id) {
            Some(permit) => Some(permit),
            None => {
                kb_metrics::record_ingest_throttled();
                return Err(throttled_error());
            }
        }
    } else {
        None
    };

    // ── Parse multipart ──────────────────────────────────────────────────────
    let parsed = parse_multipart(multipart, MAX_PAYLOAD_BYTES)
        .await
        .map_err(multipart_error_to_response)?;

    if parsed.files.is_empty() {
        return Err(bad_request("no_files", "at least one file is required"));
    }

    // ── Storage quota check (plan-driven, P11-T5) ───────────────────────────
    let total_bytes: i64 = parsed.files.iter().map(|f| f.bytes.len() as i64).sum();
    if let Err(e) = state
        .pg_store
        .check_plan_storage_quota(auth_user.tenant_id, total_bytes)
        .await
    {
        // Check if this is a QuotaError → 413 with upsell.
        if let Some(qe) = e.downcast_ref::<kb_core::quota::QuotaError>() {
            kb_metrics::record_quota_rejection("storage");
            let message = quota_error_response(qe);
            return Err((
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(ErrorResponse {
                    error: "storage_quota_exceeded".into(),
                    message,
                }),
            ));
        }
        // Otherwise it's a database error.
        tracing::error!(error = %e, "ingest: storage quota check failed");
        return Err(internal_error(e));
    }

    // ── Monthly token-budget check (plan-driven, P14-T4) ────────────────────
    // UX-first hard block: if the tenant has ALREADY met or exceeded its
    // monthly token budget (read from the O(1) rollup, not a SUM), reject the
    // *next* ingest with 429 + upsell. The job that crossed the budget already
    // ran to completion — only subsequent ingests are blocked (bounded
    // overshoot, see PgStore::check_plan_token_budget_rollup). No pre-extraction
    // token estimate is made here, by design: estimating would falsely reject
    // large media uploads whose transcribed token count is small. Per-job
    // overshoot is instead bounded by the storage quota checked just above.
    if let Err(e) = state
        .pg_store
        .check_plan_token_budget_rollup(auth_user.tenant_id)
        .await
    {
        if let Some(qe) = e.downcast_ref::<kb_core::quota::QuotaError>() {
            kb_metrics::record_quota_rejection("tokens");
            let message = quota_error_response(qe);
            return Err((
                StatusCode::TOO_MANY_REQUESTS,
                Json(ErrorResponse {
                    error: "token_budget_exceeded".into(),
                    message,
                }),
            ));
        }
        // Otherwise it's a database error.
        tracing::error!(error = %e, "ingest: token budget check failed");
        return Err(internal_error(e));
    }

    // ── Resolve the remote-models plan gate (P14-T6) ─────────────────────────
    // Per request (hot-swappable): a free-plan tenant is forced to local-only
    // models; pro/team (and grandfathered) tenants may use remote backends.
    // Fail-closed to local-only on a lookup error (never leaks remote access).
    let local_only =
        crate::handlers::resolve_local_only(&state.pg_store, auth_user.tenant_id).await;

    // ── Ensure required components are present ───────────────────────────────
    let blob = state
        .blob
        .as_ref()
        .ok_or_else(|| internal_error(anyhow::anyhow!("ingest: blob store not configured")))?;

    // ── Queued mode (default, P15-T5): stage + enqueue, return immediately ───
    if queued_mode {
        let job_queue = state
            .job_queue
            .as_ref()
            .ok_or_else(|| internal_error(anyhow::anyhow!("ingest: job queue not configured")))?;
        let result = process_upload_queued(
            blob.as_ref(),
            &state.pg_store,
            job_queue.as_ref(),
            &ingest_cfg,
            auth_user.tenant_id,
            Some(auth_user.user_id),
            &parsed,
            local_only,
        )
        .await
        .map_err(map_ingest_error)?;

        return Ok((
            StatusCode::ACCEPTED,
            Json(IngestResponse {
                job_id: result.job_id,
                document_id: Some(result.document_id),
                message: format!(
                    "upload accepted; document {} queued for processing ({} file(s))",
                    result.document_id,
                    parsed.files.len()
                ),
            }),
        ));
    }

    // ── Inline mode (rollback lever): the synchronous pipeline ───────────────
    let pipeline = state
        .ingest_pipeline
        .as_ref()
        .ok_or_else(|| internal_error(anyhow::anyhow!("ingest: pipeline not configured")))?;
    let result = process_upload_inline(
        blob.as_ref(),
        pipeline.as_ref(),
        auth_user.tenant_id,
        Some(auth_user.user_id),
        &parsed,
        local_only,
    )
    .await
    .map_err(map_ingest_error)?;

    let doc_id_str = result.document_id.map(|id| id.to_string());
    Ok((
        StatusCode::ACCEPTED,
        Json(IngestResponse {
            job_id: result.job_id,
            document_id: result.document_id,
            message: doc_id_str.map_or_else(
                || format!("ingest processed ({} file(s))", result.file_count),
                |id| {
                    format!(
                        "ingest processed, document {} created ({} file(s))",
                        id, result.file_count
                    )
                },
            ),
        }),
    ))
}

// ── Queued upload path (P15-T5, plan §16) ───────────────────────────────────

/// The bounded ingest queue is full — admission refused (P15-T5).
///
/// Carries the live counts so the 429 body tells the caller how saturated the
/// queue is. `Retry-After` is added by the `ensure_retry_after` middleware.
#[derive(Debug)]
pub(crate) struct QueueFull {
    /// This tenant's not-yet-done ingest jobs.
    pub tenant_pending: i64,
    /// The per-tenant cap that was hit (or would be exceeded).
    pub tenant_cap: u32,
    /// All tenants' not-yet-done ingest jobs.
    pub global_pending: i64,
    /// The global cap.
    pub global_cap: u32,
}

impl std::fmt::Display for QueueFull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ingest queue is full (tenant {}/{}, global {}/{}); retry shortly",
            self.tenant_pending, self.tenant_cap, self.global_pending, self.global_cap
        )
    }
}

impl std::error::Error for QueueFull {}

/// Best-effort cleanup of already-written blobs when staging or enqueue fails.
///
/// If `blob.put()` succeeds but `create_pending_ingest`/`enqueue` fails, the
/// stored bytes are orphaned (no DB row references them). Calling this on the
/// error path deletes them immediately rather than waiting for the orphan GC's
/// 24 h grace period. Each per-blob failure is logged at `warn` but not surfaced
/// (the staging/enqueue error is the primary one; delete failures don't change
/// that the upload was already rejected).
async fn cleanup_blobs(blob: &dyn Blob, file_records: &[kb_core::file::FileRecord]) {
    let mut seen = std::collections::HashSet::new();
    for r in file_records {
        if !seen.insert(&r.blob_key) {
            continue; // skip dedup keys within a single upload
        }
        if let Err(e) = blob.delete(&r.blob_key).await {
            tracing::warn!(
                error = %e,
                blob_key = %r.blob_key,
                "ingest: failed to clean up orphaned blob after staging failure"
            );
        }
    }
}

/// Result from [`process_upload_queued`].
pub(crate) struct QueuedIngestResult {
    /// The enqueued ingest job id (pollable at `/api/jobs/:id`).
    pub job_id: i64,
    /// The staged (pending) document id.
    pub document_id: i64,
}

/// Stage an upload for asynchronous processing (P15-T5, plan §16).
///
/// The queued-mode upload path: validate → bounded-queue admission → store
/// blobs → stage a `pending` document + file rows → enqueue an ingest job
/// carrying the document id. Returns immediately; a worker finalizes the
/// document to `ready` in the background.
///
/// Inputs are sanitized exactly like the inline pipeline (control characters
/// stripped + note bounded, filenames reduced to safe basenames) so the staged
/// rows are storable and safe; the pipeline re-applies the same sanitizers at
/// processing time (idempotent).
///
/// # Errors
/// Typed errors the caller maps via [`map_ingest_error`]:
/// [`security::UploadRejected`] → 400, [`QueueFull`] → 429 `queue_full`;
/// anything else (blob store, DB) is an internal 500.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_upload_queued(
    blob: &dyn Blob,
    pg_store: &kb_store::PgStore,
    job_queue: &JobQueue,
    caps: &kb_config::Ingest,
    tenant_id: i64,
    user_id: Option<i64>,
    parsed: &ParsedUpload,
    local_only: bool,
) -> anyhow::Result<QueuedIngestResult> {
    use anyhow::Context as _;
    use kb_pipeline::document_builder::{DocumentBuilder, PageInput};
    use kb_pipeline::ingest::{
        MAX_USER_NOTE_BYTES, sanitize_filename, strip_control_chars, truncate_to_char_boundary,
    };

    // ── Validate each file (the §17 upload-edge guards) ─────────────────────
    for f in &parsed.files {
        security::validate_upload(
            &f.bytes,
            f.path.as_deref(),
            security::MAX_INDIVIDUAL_FILE_BYTES,
        )?;
    }

    // ── Bounded-queue admission (caps hot-swapped per request) ──────────────
    let (tenant_pending, global_pending) = pg_store.count_pending_ingest_jobs(tenant_id).await?;
    if tenant_pending >= i64::from(caps.max_pending_per_tenant)
        || global_pending >= i64::from(caps.max_pending_global)
    {
        return Err(QueueFull {
            tenant_pending,
            tenant_cap: caps.max_pending_per_tenant,
            global_pending,
            global_cap: caps.max_pending_global,
        }
        .into());
    }

    // ── Sanitize note + filenames (mirrors the pipeline; staged rows must be
    //    Postgres-storable — a NUL in a note would otherwise fail the insert) ─
    let user_note = parsed.user_note.as_deref().map(|note| {
        let note = strip_control_chars(note);
        if note.len() > MAX_USER_NOTE_BYTES {
            truncate_to_char_boundary(&note, MAX_USER_NOTE_BYTES).to_string()
        } else {
            note
        }
    });
    let cleaned: Vec<(Vec<u8>, Option<String>, Option<String>)> = parsed
        .files
        .iter()
        .map(|f| {
            (
                f.bytes.clone(),
                f.page_label.as_deref().and_then(sanitize_filename),
                f.path.as_deref().and_then(sanitize_filename),
            )
        })
        .collect();

    // ── Build the document + file records (sha256, blob keys, MIME, kind) ───
    let (mut document, file_records) = if cleaned.len() == 1 {
        let (bytes, _, path) = &cleaned[0];
        DocumentBuilder::build_single(tenant_id, bytes, path.as_deref(), user_note.as_deref())
    } else {
        let pages: Vec<PageInput<'_>> = cleaned
            .iter()
            .map(|(bytes, page_label, path)| PageInput {
                bytes,
                page_label: page_label.as_deref(),
                path: path.as_deref(),
            })
            .collect();
        DocumentBuilder::build_multi(tenant_id, &pages, user_note.as_deref())
    };
    // Persist the plan gate resolved at upload; the worker re-resolves at
    // processing time and ORs the two (the hot-swap rule).
    document.local_only = local_only;

    // ── Store blobs at the records' content-addressed keys ──────────────────
    for (record, (bytes, _, _)) in file_records.iter().zip(&cleaned) {
        blob.put(&record.blob_key, bytes::Bytes::copy_from_slice(bytes))
            .await
            .with_context(|| format!("failed to store blob {}", record.blob_key))?;
    }

    // ── Stage pending rows + enqueue the job ────────────────────────────────
    let pending = match pg_store
        .create_pending_ingest(&document, &file_records)
        .await
    {
        Ok(p) => p,
        Err(e) => {
            cleanup_blobs(blob, &file_records).await;
            return Err(e);
        }
    };
    let job_id = match job_queue
        .enqueue(
            tenant_id,
            user_id,
            None,
            Some(pending.document_id),
            JobKind::Ingest,
            0,
        )
        .await
    {
        Ok(id) => id,
        Err(e) => {
            cleanup_blobs(blob, &file_records).await;
            return Err(e);
        }
    };

    tracing::info!(
        tenant_id,
        document_id = pending.document_id,
        job_id,
        files = parsed.files.len(),
        reused = pending.reused,
        "upload staged for queued ingestion"
    );

    Ok(QueuedIngestResult {
        job_id,
        document_id: pending.document_id,
    })
}

/// Stage additional pages onto an **existing** document and enqueue it for
/// re-processing (P15-T5; the web "add page" flow — previously this enqueued
/// payload-less jobs no worker could process).
///
/// New file rows are staged `pending` with page numbers continuing after the
/// document's current pages; the whole document is re-processed by the worker
/// so its title/summary/tags reflect the updated content.
///
/// # Errors
/// [`security::UploadRejected`] for invalid files (→ 400 via
/// [`map_ingest_error`]); an error if the document does not exist for the
/// tenant; blob/DB failures (→ 500).
pub(crate) async fn process_add_page_queued(
    blob: &dyn Blob,
    pg_store: &kb_store::PgStore,
    job_queue: &JobQueue,
    tenant_id: i64,
    user_id: Option<i64>,
    document_id: i64,
    parsed: &ParsedUpload,
) -> anyhow::Result<(i64, usize)> {
    use anyhow::Context as _;
    use kb_pipeline::document_builder::{DocumentBuilder, PageInput};
    use kb_pipeline::ingest::sanitize_filename;

    for f in &parsed.files {
        security::validate_upload(
            &f.bytes,
            f.path.as_deref(),
            security::MAX_INDIVIDUAL_FILE_BYTES,
        )?;
    }

    // The document must exist within the tenant (RLS-scoped read).
    let existing_files = pg_store
        .get_files_for_document(tenant_id, document_id)
        .await?;
    anyhow::ensure!(
        pg_store
            .get_document(tenant_id, document_id)
            .await?
            .is_some(),
        "document {document_id} not found"
    );

    // Reuse DocumentBuilder's detection (sha256, blob keys, MIME); the built
    // document itself is discarded — only the file records are staged, with
    // page numbers continuing after the existing pages.
    let cleaned: Vec<(Vec<u8>, Option<String>, Option<String>)> = parsed
        .files
        .iter()
        .map(|f| {
            (
                f.bytes.clone(),
                f.page_label.as_deref().and_then(sanitize_filename),
                f.path.as_deref().and_then(sanitize_filename),
            )
        })
        .collect();
    let pages: Vec<PageInput<'_>> = cleaned
        .iter()
        .map(|(bytes, page_label, path)| PageInput {
            bytes,
            page_label: page_label.as_deref(),
            path: path.as_deref(),
        })
        .collect();
    let (_discarded_doc, mut records) = DocumentBuilder::build_multi(tenant_id, &pages, None);
    let offset = existing_files.len() as i32;
    for r in &mut records {
        r.page_no += offset;
        r.document_id = document_id;
    }

    for (record, (bytes, _, _)) in records.iter().zip(&cleaned) {
        blob.put(&record.blob_key, bytes::Bytes::copy_from_slice(bytes))
            .await
            .with_context(|| format!("failed to store blob {}", record.blob_key))?;
    }

    if let Err(e) = pg_store
        .stage_pending_files(tenant_id, document_id, &records)
        .await
    {
        cleanup_blobs(blob, &records).await;
        return Err(e);
    }
    let job_id = match job_queue
        .enqueue(
            tenant_id,
            user_id,
            None,
            Some(document_id),
            JobKind::Ingest,
            0,
        )
        .await
    {
        Ok(id) => id,
        Err(e) => {
            cleanup_blobs(blob, &records).await;
            return Err(e);
        }
    };

    Ok((job_id, records.len()))
}

/// Result from [`process_upload_inline`].
pub(crate) struct InlineIngestResult {
    /// The job id (still enqueued for audit; completed immediately).
    pub job_id: i64,
    /// The document id assigned by the store.
    pub document_id: Option<i64>,
    /// Number of files processed.
    pub file_count: usize,
}

/// Store blobs and process files through the ingest pipeline inline.
///
/// This is the primary upload path: files are stored in the blob store, then
/// fed directly through the pipeline (extract → tag → embed → store). A job
/// is enqueued for audit/replay but is completed synchronously so the caller
/// gets the document id immediately.
///
/// `user_id` is the uploading user; it is threaded into the pipeline so every
/// metered model call is attributed to that user in `usage_events` (P14-T1).
///
/// `local_only` gates remote model backends per the tenant's plan (P14-T6):
/// `true` forces the pipeline to use ONLY local models (free plan), `false`
/// permits remote backends if configured (pro/team or grandfathered). The
/// caller resolves it per request via [`super::resolve_local_only`].
pub(crate) async fn process_upload_inline(
    blob: &dyn Blob,
    pipeline: &IngestPipeline,
    tenant_id: i64,
    user_id: Option<i64>,
    parsed: &ParsedUpload,
    local_only: bool,
) -> anyhow::Result<InlineIngestResult> {
    // ── Validate each file ─────────────────────────────────────────────────
    for f in &parsed.files {
        security::validate_upload(
            &f.bytes,
            f.path.as_deref(),
            security::MAX_INDIVIDUAL_FILE_BYTES,
        )?;
    }

    // ── Store each file in the blob store ──────────────────────────────────
    let mut blob_keys: Vec<String> = Vec::with_capacity(parsed.files.len());
    for f in &parsed.files {
        let sha256 = compute_blob_key(&f.bytes);
        blob.put(&sha256, bytes::Bytes::copy_from_slice(&f.bytes))
            .await?;
        blob_keys.push(sha256);
    }

    // ── Process through ingest pipeline ────────────────────────────────────
    let output = match pipeline
        .ingest(
            tenant_id,
            user_id, // attribute metered usage to the uploading user (P14-T1)
            parsed.files.clone(),
            parsed.user_note.clone(),
            local_only, // free plan → local-only models; pro/team → remote OK (P14-T6)
        )
        .await
    {
        Ok(o) => o,
        Err(e) => {
            // Pipeline failed after blobs were already stored — clean up.
            for key in &blob_keys {
                if let Err(del_err) = blob.delete(key).await {
                    tracing::warn!(
                        error = %del_err,
                        blob_key = %key,
                        "ingest: failed to clean up orphaned blob after inline pipeline failure"
                    );
                }
            }
            return Err(e);
        }
    };

    // Use a dummy job id for the response (no async queue needed).
    let job_id = 0;

    Ok(InlineIngestResult {
        job_id,
        document_id: Some(output.document_id),
        file_count: parsed.files.len(),
    })
}

// ── Multipart parsing ──────────────────────────────────────────────────────────

/// Why a multipart upload body could not be parsed.
///
/// Distinguishes an over-size body (→ `413 Payload Too Large`) from a malformed
/// or unreadable one (→ `400 Bad Request`) so [`parse_multipart`] callers can map
/// the status correctly (campaign finding F3). `kb-api` does not depend on
/// `thiserror`, so `Display` is implemented by hand.
#[derive(Debug)]
pub(crate) enum UploadParseError {
    /// The upload exceeded the size limit — either the soft payload cap in
    /// [`parse_multipart`] or axum's [`DefaultBodyLimit`] (which `MultipartError`
    /// reports as `413`). Maps to `413 Payload Too Large`.
    TooLarge(String),
    /// The multipart body was malformed or unreadable. Maps to `400 Bad Request`.
    Malformed(String),
}

impl std::fmt::Display for UploadParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UploadParseError::TooLarge(m) | UploadParseError::Malformed(m) => f.write_str(m),
        }
    }
}

/// Classify an axum [`MultipartError`](axum::extract::multipart::MultipartError)
/// into an [`UploadParseError`]: a body/length-limit overflow (which axum reports
/// as `413`) becomes [`UploadParseError::TooLarge`]; anything else is
/// [`UploadParseError::Malformed`].
fn classify_multipart_err(e: axum::extract::multipart::MultipartError) -> UploadParseError {
    if e.status() == StatusCode::PAYLOAD_TOO_LARGE {
        UploadParseError::TooLarge(e.to_string())
    } else {
        UploadParseError::Malformed(e.to_string())
    }
}

/// Map an [`UploadParseError`] to a JSON HTTP response: `TooLarge` → `413`
/// (`payload_too_large`), `Malformed` → `400` (`invalid_multipart`).
fn multipart_error_to_response(e: UploadParseError) -> (StatusCode, Json<ErrorResponse>) {
    match e {
        UploadParseError::TooLarge(msg) => {
            tracing::warn!(error = %msg, "ingest: upload too large (413)");
            (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(ErrorResponse {
                    error: "payload_too_large".into(),
                    message: msg,
                }),
            )
        }
        UploadParseError::Malformed(msg) => {
            tracing::warn!(error = %msg, "ingest: bad multipart (400)");
            bad_request("invalid_multipart", &msg)
        }
    }
}

/// Parsed multipart upload data.
pub(crate) struct ParsedUpload {
    /// File contents and metadata.
    pub files: Vec<IngestFile>,
    /// Optional user note.
    pub user_note: Option<String>,
    /// Whether files should be grouped into one document. Parsed for form
    /// compatibility; both processing paths currently treat every upload as
    /// one (multi-page) document, matching the long-standing inline behavior.
    #[allow(dead_code)]
    pub group_as_document: bool,
    /// CSRF token from the form (set by Web UI; empty for API clients).
    pub csrf_token: String,
}

/// Parse a multipart form body into [`ParsedUpload`].
///
/// Accumulates file parts into `IngestFile` entries and extracts text fields.
/// Enforces `max_total_bytes` as a soft payload limit (reading stops early when
/// the running total exceeds the limit) and surfaces over-size bodies — the soft
/// cap or axum's [`DefaultBodyLimit`] — as [`UploadParseError::TooLarge`] so the
/// caller can answer `413 Payload Too Large` rather than `400` (campaign finding
/// F3). Genuinely malformed/unreadable bodies are [`UploadParseError::Malformed`]
/// → `400`.
///
/// Extracts the optional `csrf_token` form field for Web UI double-submit
/// cookie validation. For pure API clients (no form), the field is absent
/// and the token will be an empty string.
pub(crate) async fn parse_multipart(
    mut multipart: Multipart,
    max_total_bytes: u64,
) -> Result<ParsedUpload, UploadParseError> {
    let mut files = Vec::new();
    let mut user_note: Option<String> = None;
    let mut group_as_document = false;
    let mut csrf_token = String::new();
    let mut total_bytes: u64 = 0;

    loop {
        let field = multipart
            .next_field()
            .await
            .map_err(classify_multipart_err)?;

        let Some(field) = field else {
            break;
        };

        let name = field.name().map(|s| s.to_string()).unwrap_or_default();

        match name.as_str() {
            "files" => {
                let file_name = field.file_name().map(|s| s.to_string());
                let bytes = field.bytes().await.map_err(classify_multipart_err)?;

                total_bytes = total_bytes.saturating_add(bytes.len() as u64);
                if total_bytes > max_total_bytes {
                    return Err(UploadParseError::TooLarge(format!(
                        "total upload size exceeds limit of {max_total_bytes} bytes"
                    )));
                }

                files.push(IngestFile {
                    bytes: bytes.to_vec(),
                    page_label: file_name.clone(),
                    path: file_name,
                });
            }
            "user_note" => {
                let text = field.text().await.map_err(classify_multipart_err)?;
                if !text.is_empty() {
                    user_note = Some(text);
                }
            }
            "group_as_document" => {
                let text = field.text().await.map_err(classify_multipart_err)?;
                group_as_document = text.trim().eq_ignore_ascii_case("true");
            }
            "csrf_token" => {
                let text = field.text().await.map_err(classify_multipart_err)?;
                csrf_token = text;
            }
            _ => {
                // Ignore unknown fields.
            }
        }
    }

    Ok(ParsedUpload {
        files,
        user_note,
        group_as_document,
        csrf_token,
    })
}

// ── Blob key helper ────────────────────────────────────────────────────────────

/// Compute a content-addressed blob key from raw bytes.
///
/// Uses the SHA-256 hex digest, tenant-prefixed so blobs are namespaced per
/// tenant. This mirrors the [`DocumentBuilder`](kb_pipeline::document_builder)
/// blob-key layout.
fn compute_blob_key(bytes: &[u8]) -> String {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    let hash: [u8; 32] = hasher.finalize().into();
    hex::encode(hash)
}

// ── Error helpers ──────────────────────────────────────────────────────────────

fn bad_request(code: &str, msg: &str) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorResponse {
            error: code.into(),
            message: msg.into(),
        }),
    )
}

fn internal_error(e: impl std::fmt::Display) -> (StatusCode, Json<ErrorResponse>) {
    tracing::error!(error = %e, "ingest handler error");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            error: "internal_error".into(),
            message: "an unexpected error occurred".into(),
        }),
    )
}

/// Build a `503 Service Unavailable` response for a transient backend outage.
///
/// The `detail` is logged but not exposed to the client (it may name internal
/// roles/backends). The `Retry-After` header is added by the `ensure_retry_after`
/// middleware (`commands::serve`), so every 503 carries retry guidance.
fn service_unavailable(detail: impl std::fmt::Display) -> (StatusCode, Json<ErrorResponse>) {
    tracing::warn!(error = %detail, "ingest: model backend unavailable (503)");
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(ErrorResponse {
            error: "service_unavailable".into(),
            message: "the service is temporarily unavailable, please retry shortly".into(),
        }),
    )
}

/// Map an ingest-pipeline error to an HTTP response (BUG-INGEST-06/07).
///
/// Upload-validation rejections — empty, oversized, disallowed-type, or
/// unsafe-named bytes — are **client** errors the caller can fix, so they become
/// `400 Bad Request`. A [`QuotaError`](kb_core::quota::QuotaError) is mapped by
/// kind: [`StorageExceeded`](kb_core::quota::QuotaError::StorageExceeded) →
/// `413 Payload Too Large`, [`TokensExceeded`](kb_core::quota::QuotaError::TokensExceeded)
/// → `429 Too Many Requests` (both with an upsell message, P14-T4). Everything
/// else (blob store, DB, pipeline faults) is an internal `500`. Without this
/// downcast, every validation rejection surfaced as a `500`.
pub(crate) fn map_ingest_error(e: anyhow::Error) -> (StatusCode, Json<ErrorResponse>) {
    if let Some(rejected) = e.downcast_ref::<security::UploadRejected>() {
        tracing::warn!(error = %rejected, "ingest: rejected upload (400)");
        return bad_request("invalid_upload", &rejected.to_string());
    }
    if let Some(qe) = e.downcast_ref::<kb_core::quota::QuotaError>() {
        return quota_error_to_response(qe);
    }
    // A transient model-backend outage (tagger had no healthy backend, etc.) is
    // a 503 + Retry-After, not a 500 (campaign finding F4): the request is fine,
    // the service is momentarily unable to process it.
    if let Some(unavailable) = e.downcast_ref::<IngestError>() {
        return service_unavailable(unavailable);
    }
    // The bounded ingest queue is full (P15-T5): 429 queue_full with the live
    // counts; Retry-After is added by the ensure_retry_after middleware.
    if let Some(full) = e.downcast_ref::<QueueFull>() {
        tracing::warn!(
            tenant_pending = full.tenant_pending,
            global_pending = full.global_pending,
            "ingest: queue full (429)"
        );
        kb_metrics::record_queue_full_rejection();
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(ErrorResponse {
                error: "queue_full".into(),
                message: full.to_string(),
            }),
        );
    }
    internal_error(e)
}

/// Map a [`QuotaError`](kb_core::quota::QuotaError) to its HTTP response,
/// dispatching on the exhausted limit (P14-T4):
/// - [`StorageExceeded`](kb_core::quota::QuotaError::StorageExceeded) →
///   `413 Payload Too Large` (`storage_quota_exceeded`),
/// - [`TokensExceeded`](kb_core::quota::QuotaError::TokensExceeded) →
///   `429 Too Many Requests` (`token_budget_exceeded`),
/// - [`UserLimitExceeded`](kb_core::quota::QuotaError::UserLimitExceeded) →
///   `403 Forbidden` (`user_limit_exceeded`).
///
/// Each response body carries the limit detail plus an upsell suggestion. The
/// matching `kb_quota_rejections_total{limit=…}` counter is incremented so the
/// rejection is observable regardless of which path produced the error.
pub(crate) fn quota_error_to_response(
    qe: &kb_core::quota::QuotaError,
) -> (StatusCode, Json<ErrorResponse>) {
    use kb_core::quota::QuotaError;
    let message = quota_error_response(qe);
    let (status, limit, error_code) = match qe {
        QuotaError::StorageExceeded { .. } => (
            StatusCode::PAYLOAD_TOO_LARGE,
            "storage",
            "storage_quota_exceeded",
        ),
        QuotaError::TokensExceeded { .. } => (
            StatusCode::TOO_MANY_REQUESTS,
            "tokens",
            "token_budget_exceeded",
        ),
        QuotaError::UserLimitExceeded { .. } => {
            (StatusCode::FORBIDDEN, "users", "user_limit_exceeded")
        }
    };
    tracing::warn!(error = %qe, status = %status, "ingest: quota exceeded");
    kb_metrics::record_quota_rejection(limit);
    (
        status,
        Json(ErrorResponse {
            error: error_code.into(),
            message,
        }),
    )
}

/// Build a `429 Too Many Requests` error tuple without headers.
///
/// The `Retry-After` header is added by the `ensure_retry_after`
/// middleware in [`serve`](crate::commands::serve) so that every 429/503 in
/// the app carries the header.
fn throttled_error() -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::TOO_MANY_REQUESTS,
        Json(ErrorResponse {
            error: "too_many_requests".into(),
            message: "server is at capacity, please retry after a short delay".into(),
        }),
    )
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::Arc;
    use std::time::Duration;

    use axum::Router;
    use axum::body::Body;
    use axum::extract::FromRequest;
    use axum::http::{Method, StatusCode};
    use axum::routing::post;
    use kb_core::session::DEFAULT_SESSION_TTL_SECS;
    use kb_core::user::UserRole;
    use kb_pipeline::job_queue::JobQueue;
    use kb_store::PgStore;
    use kb_store::session_store::InMemorySessionStore;
    use tower::ServiceExt;

    use super::*;
    use crate::AppState;
    use crate::middleware::auth_middleware;

    /// Build a test state with an InMemorySessionStore, a disconnected PgStore
    /// (not used in ingest), and a real JobQueue backed by a disconnected pool.
    fn test_state() -> Arc<AppState> {
        let session_store: Arc<dyn kb_core::session::SessionStore> =
            Arc::new(InMemorySessionStore::new());
        let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
        // JobQueue with a disconnected pool will fail on enqueue, but that's
        // fine for middleware/parse tests. Integration-style tests mock the
        // queue separately.
        let job_queue = Arc::new(JobQueue::new(
            sqlx::PgPool::connect_lazy("postgres://localhost/test?sslmode=disable")
                .expect("connect_lazy always succeeds"),
            10_000,
            3,
        ));
        Arc::new(
            AppState::new(
                session_store,
                pg_store,
                Some(Duration::from_secs(DEFAULT_SESSION_TTL_SECS)),
                false,
            )
            .with_job_queue(job_queue),
        )
    }

    /// Build a test router with the ingest endpoint behind auth middleware.
    fn ingest_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/api/ingest", post(ingest))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ))
            .with_state(state)
    }

    /// Create a valid session and return its cookie token.
    async fn login(state: &AppState) -> String {
        state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap()
    }

    // ── parse_multipart pure-logic tests ────────────────────────────────────

    /// Build a multipart body from raw bytes.
    fn multipart_body(
        boundary: &str,
        files: &[(&str, &[u8])],
        extra_fields: &[(&str, &str)],
    ) -> Vec<u8> {
        let mut body = Vec::new();
        for (name, data) in files {
            let disposition = format!(
                "Content-Disposition: form-data; name=\"files\"; filename=\"{name}\"\r\n\
                 Content-Type: application/octet-stream\r\n\r\n"
            );
            body.extend_from_slice(b"\r\n--");
            body.extend_from_slice(boundary.as_bytes());
            body.extend_from_slice(b"\r\n");
            body.extend_from_slice(disposition.as_bytes());
            body.extend_from_slice(data);
        }
        for (name, value) in extra_fields {
            let field = format!("Content-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}");
            body.extend_from_slice(b"\r\n--");
            body.extend_from_slice(boundary.as_bytes());
            body.extend_from_slice(b"\r\n");
            body.extend_from_slice(field.as_bytes());
        }
        body.extend_from_slice(b"\r\n--");
        body.extend_from_slice(boundary.as_bytes());
        body.extend_from_slice(b"--\r\n");
        body
    }

    #[tokio::test]
    async fn parse_single_file() {
        let boundary = "testboundary";
        let body_bytes = multipart_body(boundary, &[("test.txt", b"hello world")], &[]);
        let body = Body::from(body_bytes);
        let content_type = format!("multipart/form-data; boundary={boundary}");

        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/api/ingest")
            .header("Content-Type", content_type)
            .body(body)
            .unwrap();

        // Parse via axum's Multipart extractor.
        let multipart = <Multipart as FromRequest<()>>::from_request(request, &())
            .await
            .unwrap();
        let parsed = parse_multipart(multipart, MAX_PAYLOAD_BYTES).await.unwrap();

        assert_eq!(parsed.files.len(), 1);
        assert_eq!(parsed.files[0].bytes, b"hello world");
        assert_eq!(parsed.files[0].page_label.as_deref(), Some("test.txt"));
        assert!(parsed.user_note.is_none());
        assert!(!parsed.group_as_document);
    }

    #[tokio::test]
    async fn parse_multiple_files() {
        let boundary = "boundary2";
        let body_bytes = multipart_body(
            boundary,
            &[("a.txt", b"aaa"), ("b.txt", b"bbb"), ("c.txt", b"ccc")],
            &[],
        );
        let body = Body::from(body_bytes);
        let content_type = format!("multipart/form-data; boundary={boundary}");

        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/api/ingest")
            .header("Content-Type", content_type)
            .body(body)
            .unwrap();

        let multipart = <Multipart as FromRequest<()>>::from_request(request, &())
            .await
            .unwrap();
        let parsed = parse_multipart(multipart, MAX_PAYLOAD_BYTES).await.unwrap();

        assert_eq!(parsed.files.len(), 3);
        assert_eq!(parsed.files[0].bytes, b"aaa");
        assert_eq!(parsed.files[1].bytes, b"bbb");
        assert_eq!(parsed.files[2].bytes, b"ccc");
    }

    #[tokio::test]
    async fn parse_user_note_field() {
        let boundary = "boundary3";
        let body_bytes = multipart_body(
            boundary,
            &[("doc.pdf", b"pdf content")],
            &[("user_note", "my important document")],
        );
        let body = Body::from(body_bytes);
        let content_type = format!("multipart/form-data; boundary={boundary}");

        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/api/ingest")
            .header("Content-Type", content_type)
            .body(body)
            .unwrap();

        let multipart = <Multipart as FromRequest<()>>::from_request(request, &())
            .await
            .unwrap();
        let parsed = parse_multipart(multipart, MAX_PAYLOAD_BYTES).await.unwrap();

        assert_eq!(parsed.user_note.as_deref(), Some("my important document"));
    }

    #[tokio::test]
    async fn parse_group_as_document_true() {
        let boundary = "boundary4";
        let body_bytes = multipart_body(
            boundary,
            &[("front.jpg", b"front"), ("back.jpg", b"back")],
            &[("group_as_document", "true")],
        );
        let body = Body::from(body_bytes);
        let content_type = format!("multipart/form-data; boundary={boundary}");

        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/api/ingest")
            .header("Content-Type", content_type)
            .body(body)
            .unwrap();

        let multipart = <Multipart as FromRequest<()>>::from_request(request, &())
            .await
            .unwrap();
        let parsed = parse_multipart(multipart, MAX_PAYLOAD_BYTES).await.unwrap();

        assert!(parsed.group_as_document);
        assert_eq!(parsed.files.len(), 2);
    }

    #[tokio::test]
    async fn parse_group_as_document_false() {
        let boundary = "boundary5";
        let body_bytes = multipart_body(
            boundary,
            &[("a.txt", b"a")],
            &[("group_as_document", "false")],
        );
        let body = Body::from(body_bytes);
        let content_type = format!("multipart/form-data; boundary={boundary}");

        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/api/ingest")
            .header("Content-Type", content_type)
            .body(body)
            .unwrap();

        let multipart = <Multipart as FromRequest<()>>::from_request(request, &())
            .await
            .unwrap();
        let parsed = parse_multipart(multipart, MAX_PAYLOAD_BYTES).await.unwrap();

        assert!(!parsed.group_as_document);
    }

    #[tokio::test]
    async fn parse_empty_user_note_is_none() {
        let boundary = "boundary6";
        let body_bytes = multipart_body(boundary, &[("f.txt", b"data")], &[("user_note", "")]);
        let body = Body::from(body_bytes);
        let content_type = format!("multipart/form-data; boundary={boundary}");

        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/api/ingest")
            .header("Content-Type", content_type)
            .body(body)
            .unwrap();

        let multipart = <Multipart as FromRequest<()>>::from_request(request, &())
            .await
            .unwrap();
        let parsed = parse_multipart(multipart, MAX_PAYLOAD_BYTES).await.unwrap();

        assert_eq!(parsed.files.len(), 1);
        assert!(parsed.user_note.is_none());
    }

    // ── F3: over-size → 413, malformed → 400 ────────────────────────────────

    /// A body whose running file-byte total exceeds the soft cap is classified
    /// as `TooLarge` (→ 413), not a generic parse error (→ 400).
    #[tokio::test]
    async fn parse_multipart_over_soft_cap_is_too_large() {
        let boundary = "f3boundary";
        // An 11-byte file with a 4-byte cap → the running total trips the soft cap.
        let body_bytes = multipart_body(boundary, &[("big.txt", b"hello world")], &[]);
        let body = Body::from(body_bytes);
        let content_type = format!("multipart/form-data; boundary={boundary}");
        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/api/ingest")
            .header("Content-Type", content_type)
            .body(body)
            .unwrap();
        let multipart = <Multipart as FromRequest<()>>::from_request(request, &())
            .await
            .unwrap();

        let result = parse_multipart(multipart, 4).await;
        assert!(
            matches!(result, Err(UploadParseError::TooLarge(_))),
            "soft-cap overflow must be TooLarge"
        );
    }

    /// The over-size/malformed split maps to the correct status + error code.
    #[test]
    fn multipart_error_maps_too_large_to_413_and_malformed_to_400() {
        let (status, body) =
            multipart_error_to_response(UploadParseError::TooLarge("too big".into()));
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(body.error, "payload_too_large");

        let (status, body) =
            multipart_error_to_response(UploadParseError::Malformed("garbage".into()));
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body.error, "invalid_multipart");
    }

    /// `Display` surfaces the inner message (callers render it to users).
    #[test]
    fn upload_parse_error_display_is_inner_message() {
        assert_eq!(UploadParseError::TooLarge("m1".into()).to_string(), "m1");
        assert_eq!(UploadParseError::Malformed("m2".into()).to_string(), "m2");
    }

    // ── compute_blob_key ────────────────────────────────────────────────────

    #[test]
    fn blob_key_is_hex_sha256() {
        let key = compute_blob_key(b"hello");
        assert_eq!(key.len(), 64, "SHA-256 hex digest is 64 chars");
        // Known SHA-256 of "hello".
        let expected = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
        assert_eq!(key, expected);
    }

    #[test]
    fn blob_key_is_deterministic() {
        let a = compute_blob_key(b"same data");
        let b = compute_blob_key(b"same data");
        assert_eq!(a, b);
    }

    #[test]
    fn blob_key_differs_for_different_content() {
        let a = compute_blob_key(b"content a");
        let b = compute_blob_key(b"content b");
        assert_ne!(a, b);
    }

    // ── Middleware integration tests ────────────────────────────────────────

    #[tokio::test]
    async fn unauthenticated_returns_401() {
        let state = test_state();
        let router = ingest_router(state);

        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/api/ingest")
            .header("Content-Type", "multipart/form-data; boundary=x")
            .body(Body::from("--x--\r\n"))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn empty_files_returns_400() {
        let state = test_state();
        let token = login(&state).await;
        let router = ingest_router(state.clone());

        // Multipart with no file parts.
        let boundary = "nofiles";
        let body_str = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"user_note\"\r\n\r\njust a note\r\n--{boundary}--\r\n"
        );
        let content_type = format!("multipart/form-data; boundary={boundary}");

        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/api/ingest")
            .header("Content-Type", content_type)
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::from(body_str))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let err: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(err["error"], "no_files");
    }

    // ── Error helper tests ──────────────────────────────────────────────────

    #[test]
    fn bad_request_has_400_status() {
        let (status, body) = bad_request("missing_field", "field 'q' is required");
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body.error, "missing_field");
        assert_eq!(body.message, "field 'q' is required");
    }

    #[test]
    fn internal_error_has_500_status() {
        let (status, body) = internal_error(anyhow::anyhow!("disk full"));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body.error, "internal_error");
    }

    #[test]
    fn upload_rejection_maps_to_400_else_500() {
        // BUG-INGEST-06/07: a typed UploadRejected (zero-byte upload detects as
        // application/x-empty) maps to 400, not 500.
        let rejected = anyhow::Error::new(security::UploadRejected::DisallowedMime(
            "application/x-empty".into(),
        ));
        let (status, body) = map_ingest_error(rejected);
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body.error, "invalid_upload");

        // A typed storage QuotaError maps to 413 (so web + JSON callers agree).
        // The error code is `storage_quota_exceeded`, matching the inline
        // pre-enqueue storage check on both the JSON and web upload paths.
        let quota = anyhow::Error::new(kb_core::quota::QuotaError::StorageExceeded {
            current: 100,
            additional: 10,
            total: 110,
            limit: 50,
            plan_code: None,
            upsell_plan_code: None,
        });
        let (status, body) = map_ingest_error(quota);
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(body.error, "storage_quota_exceeded");

        // An untyped error (e.g. a DB fault) still maps to 500.
        let (status, _) = map_ingest_error(anyhow::anyhow!("db connection lost"));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn quota_error_to_response_maps_each_limit() {
        use kb_core::quota::QuotaError;

        // Storage → 413.
        let storage = QuotaError::StorageExceeded {
            current: 100,
            additional: 10,
            total: 110,
            limit: 50,
            plan_code: Some("free".into()),
            upsell_plan_code: Some("pro".into()),
        };
        let (status, body) = quota_error_to_response(&storage);
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(body.error, "storage_quota_exceeded");

        // Tokens → 429 (the P14-T4 mapping), with the upsell appended.
        let tokens = QuotaError::TokensExceeded {
            current: 10_000,
            additional: 1,
            total: 10_001,
            limit: 10_000,
            plan_code: Some("free".into()),
            upsell_plan_code: Some("pro".into()),
        };
        let (status, body) = quota_error_to_response(&tokens);
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(body.error, "token_budget_exceeded");
        assert!(
            body.message.contains("token quota exceeded")
                && body.message.contains("Upgrade from free to pro"),
            "429 body must carry the limit detail + upsell: {}",
            body.message
        );

        // User limit → 403.
        let users = QuotaError::UserLimitExceeded {
            current: 5,
            limit: 5,
            plan_code: Some("free".into()),
            upsell_plan_code: Some("team".into()),
        };
        let (status, body) = quota_error_to_response(&users);
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(body.error, "user_limit_exceeded");
    }

    #[test]
    fn map_ingest_error_backend_unavailable_is_503() {
        // F4: a typed BackendUnavailable from the pipeline → 503 (not 500), with
        // a generic client message (no internal role/backend leak).
        let e = anyhow::Error::new(IngestError::BackendUnavailable(
            "scheduler: no healthy backend serves role `text`".into(),
        ));
        let (status, body) = map_ingest_error(e);
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body.error, "service_unavailable");
        assert!(
            !body.message.contains("text") && !body.message.contains("scheduler"),
            "503 body must not leak internal backend detail: {}",
            body.message
        );
    }

    #[test]
    fn map_ingest_error_queue_full_is_429_with_counts() {
        // P15-T5: a typed QueueFull from the queued upload path → 429 queue_full
        // with the live counts in the body (Retry-After added by middleware).
        let e = anyhow::Error::new(QueueFull {
            tenant_pending: 200,
            tenant_cap: 200,
            global_pending: 950,
            global_cap: 2000,
        });
        let (status, body) = map_ingest_error(e);
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(body.error, "queue_full");
        assert!(
            body.message.contains("200/200") && body.message.contains("950/2000"),
            "queue_full body must carry the counts: {}",
            body.message
        );
    }

    #[test]
    fn queue_full_display_names_both_bounds() {
        let msg = QueueFull {
            tenant_pending: 5,
            tenant_cap: 10,
            global_pending: 70,
            global_cap: 100,
        }
        .to_string();
        assert!(msg.contains("tenant 5/10"), "{msg}");
        assert!(msg.contains("global 70/100"), "{msg}");
    }

    #[test]
    fn map_ingest_error_routes_tokens_to_429() {
        // A TokensExceeded surfacing through the generic mapper must become 429,
        // not 413 (P14-T4) — storage stays 413, asserted above.
        let tokens = anyhow::Error::new(kb_core::quota::QuotaError::TokensExceeded {
            current: 10_000,
            additional: 1,
            total: 10_001,
            limit: 10_000,
            plan_code: None,
            upsell_plan_code: None,
        });
        let (status, body) = map_ingest_error(tokens);
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(body.error, "token_budget_exceeded");
    }

    // ── throttled_error tests ────────────────────────────────────────────────

    #[test]
    fn throttled_error_has_429_status() {
        let (status, body) = throttled_error();
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(body.error, "too_many_requests");
    }

    #[test]
    fn throttled_error_body_contains_message() {
        let (_, body) = throttled_error();
        assert!(body.message.contains("capacity"));
    }
}
