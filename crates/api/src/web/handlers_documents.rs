// SPDX-License-Identifier: AGPL-3.0-or-later

//! Web UI document detail page handlers (P6-T7).
//!
//! These handlers render and manage the document detail page:
//! * `GET /documents/:id` — full document detail page with metadata, tags, and page strip.
//! * `POST /documents/:id/tags/:tag_id/remove` — remove a tag from the document.
//! * `POST /documents/:id/add-tag` — add a tag by name (user-sourced, locked).
//! * `POST /documents/:id/retag` — enqueue a retag job preserving locked tags.
//! * `POST /documents/:id/reorder` — update page order via form submission.
//! * `POST /documents/:id/add-page` — upload an additional file to the document.
//!
//! All routes are auth-gated and validate CSRF on mutating requests.

use std::sync::Arc;

use askama::Template;
use axum::Extension;
use axum::Form;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use kb_core::job::JobKind;
use kb_core::tag::TagSource;
use serde::Deserialize;

use crate::AppState;
use crate::AuthUser;

use super::csrf;
use super::templates::*;

// ── Form types ───────────────────────────────────────────────────────────────────

/// Form data for removing a tag from a document.
#[derive(Debug, Deserialize)]
pub struct RemoveTagForm {
    /// CSRF token.
    pub csrf_token: String,
}

/// Form data for adding a tag by name.
#[derive(Debug, Deserialize)]
pub struct AddTagForm {
    /// CSRF token.
    pub csrf_token: String,
    /// Tag name to add (may be new or existing).
    #[serde(default)]
    pub tag_name: String,
}

/// Form data for re-tag.
#[derive(Debug, Deserialize)]
pub struct RetagForm {
    /// CSRF token.
    pub csrf_token: String,
}

/// Form data for retrying a failed ingest (P15-T9).
#[derive(Debug, serde::Deserialize)]
pub struct RetryForm {
    /// CSRF token.
    pub csrf_token: String,
}

/// Form data for page reorder.
///
/// File ids and page numbers are sent as comma-separated strings
/// because `serde_urlencoded` does not support duplicate-key `Vec<T>`
/// (only the last value survives).
#[derive(Debug, Deserialize)]
pub struct ReorderForm {
    /// CSRF token.
    pub csrf_token: String,
    /// Comma-separated file ids in the desired order.
    #[serde(default)]
    pub file_ids: String,
    /// Comma-separated page numbers (1-based), aligned with file_ids.
    #[serde(default)]
    pub page_nos: String,
}

impl ReorderForm {
    /// Parse `file_ids` and `page_nos` into aligned vectors.
    fn parse_ids(&self) -> (Vec<i64>, Vec<i32>) {
        let file_ids: Vec<i64> = self
            .file_ids
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        let page_nos: Vec<i32> = self
            .page_nos
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        (file_ids, page_nos)
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────────

/// Render a template into an HTML response with the given status code.
fn render_template<T: Template>(template: &T, status: StatusCode) -> Response {
    match template.render() {
        Ok(html) => (status, Html(html)).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "Askama template render failure");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Html("Internal server error".to_owned()),
            )
                .into_response()
        }
    }
}

/// Render a template with `StatusCode::OK`.
fn render_ok<T: Template>(template: &T) -> Response {
    render_template(template, StatusCode::OK)
}

/// Return a simple HTML error for CSRF validation failures.
///
/// Does NOT hit the database — just renders an error message.
fn csrf_error_response(status: StatusCode, msg: &str) -> Response {
    let html = format!(
        "<!DOCTYPE html><html><body>\
         <h1>CSRF Validation Failed</h1>\
         <p>{msg}</p>\
         <p><a href=\"/search\">Back to search</a></p>\
         </body></html>"
    );
    (status, Html(html)).into_response()
}

/// Generate a fresh CSRF token for form re-renders.
fn generate_fresh_csrf() -> String {
    csrf::generate_csrf_token().unwrap_or_default()
}

/// Map a MIME type to an emoji icon character for the page strip.
pub(crate) fn mime_icon(mime: &str) -> &'static str {
    if mime.starts_with("image/") {
        "\u{1f5bc}" // 🖼
    } else if mime.starts_with("audio/") {
        "\u{1f3b5}" // 🎵
    } else if mime.starts_with("video/") {
        "\u{1f3ac}" // 🎬
    } else if mime.contains("pdf") {
        "\u{1f4c4}" // 📄
    } else {
        "\u{1f4c1}" // 📁
    }
}

/// Format file size in bytes to a human-readable string.
pub(crate) fn format_file_size(bytes: i64) -> String {
    if bytes < 0 {
        return "0 B".to_string();
    }
    let bytes = bytes as u64;
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_idx = 0;
    while size >= 1024.0 && unit_idx + 1 < UNITS.len() {
        size /= 1024.0;
        unit_idx += 1;
    }
    if unit_idx == 0 {
        format!("{bytes} {unit}", unit = UNITS[0])
    } else {
        format!("{size:.1} {unit}", unit = UNITS[unit_idx])
    }
}

/// Build a full `DocumentDetailPage` from the database for a tenant + document id.
///
/// Returns the rendered page on success, or an HTML error response.
async fn build_document_detail_page(
    state: &AppState,
    tenant_id: i64,
    doc_id: i64,
    csrf_token: &str,
    error: &str,
) -> Response {
    let doc = match state.pg_store.get_document(tenant_id, doc_id).await {
        Ok(Some(d)) => d,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Html("<h1>404 — Document not found</h1>".to_owned()),
            )
                .into_response();
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to fetch document {doc_id}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Html("Internal server error".to_owned()),
            )
                .into_response();
        }
    };

    let files = match state
        .pg_store
        .get_files_for_document(tenant_id, doc_id)
        .await
    {
        Ok(f) => f,
        Err(e) => {
            tracing::error!(error = %e, "failed to fetch files for doc {doc_id}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Html("Internal server error".to_owned()),
            )
                .into_response();
        }
    };

    let tags = match state
        .pg_store
        .get_tags_for_document(tenant_id, doc_id)
        .await
    {
        Ok(t) => t,
        Err(e) => {
            tracing::error!(error = %e, "failed to fetch tags for doc {doc_id}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Html("Internal server error".to_owned()),
            )
                .into_response();
        }
    };

    let all_tags = state
        .pg_store
        .list_tags(tenant_id)
        .await
        .unwrap_or_default();

    let tag_chips: Vec<DocumentTagChip> = tags
        .iter()
        .map(|t| DocumentTagChip {
            tag_id: t.id,
            tag_name: t.name.clone(),
        })
        .collect();

    let tenant_tag_names: Vec<String> = all_tags.iter().map(|t| t.name.clone()).collect();

    let file_entries: Vec<DocumentFileEntry> = files
        .iter()
        .map(|f| DocumentFileEntry {
            file_id: f.id,
            page_no: f.page_no,
            label: f
                .page_label
                .clone()
                .unwrap_or_else(|| format!("Page {}", f.page_no)),
            icon: mime_icon(f.mime.as_deref().unwrap_or("")).to_string(),
            size_display: f.size_bytes.map(format_file_size).unwrap_or_default(),
        })
        .collect();

    // Failure UX (P15-T9): surface the latest ingest error on failed docs.
    // Sanitized for display: bounded length, and Askama HTML-escapes it.
    let last_error = if doc.status == kb_core::status::ProcessingStatus::Failed {
        match state
            .pg_store
            .latest_ingest_job_error(tenant_id, doc_id)
            .await
        {
            Ok(Some(e)) => sanitize_job_error(&e),
            Ok(None) => String::new(),
            Err(e) => {
                tracing::warn!(error = %e, %doc_id, "failed to load ingest error for display");
                String::new()
            }
        }
    } else {
        String::new()
    };

    let page = DocumentDetailPage {
        csrf_token: csrf_token.to_string(),
        error: error.to_string(),
        doc_id,
        title: doc.title.unwrap_or_default(),
        summary: doc.summary.unwrap_or_default(),
        user_note: doc.user_note.unwrap_or_default(),
        kind: doc.kind.as_str().to_string(),
        meta_json: serde_json::to_string_pretty(&doc.meta).unwrap_or_default(),
        page_count: doc.page_count,
        // Presentation status (P15-T10): a queued/staged document reads as
        // "processing" to the user (the badge's yellow arm + the auto-refresh
        // script key off it); ready/failed pass through unchanged.
        status: match doc.status {
            kb_core::status::ProcessingStatus::Pending => "processing".to_string(),
            other => other.as_str().to_string(),
        },
        created_at: doc.created_at.format("%b %d, %Y %H:%M").to_string(),
        tags: tag_chips,
        tenant_tag_names,
        files: file_entries,
        last_error,
    };

    render_ok(&page)
}

// ── POST /documents/:id/retry (P15-T9) ──────────────────────────────────────────

/// Bound, display-safe rendering of a `jobs.last_error` string.
///
/// The raw error is an internal diagnostic (it may chain extractor/model
/// context). For the tenant-facing page it is truncated to a single bounded
/// line on a char boundary; Askama escapes HTML on output.
fn sanitize_job_error(raw: &str) -> String {
    const MAX_CHARS: usize = 240;
    let line = raw.lines().next().unwrap_or_default().trim();
    if line.chars().count() <= MAX_CHARS {
        line.to_string()
    } else {
        let cut: String = line.chars().take(MAX_CHARS).collect();
        format!("{cut}…")
    }
}

/// `POST /documents/:id/retry` — requeue a failed ingest from the detail page.
///
/// Validates CSRF, then delegates to the same tenant-scoped store operation
/// as the JSON API (`/api/documents/:id/retry`): ownership is proven by the
/// page rebuild's RLS-scoped document read; the job reset itself carries an
/// explicit `tenant_id` on every statement (§31.5). Re-renders the detail
/// page — on success the status badge shows `pending` again.
pub async fn retry_document_ingest_web(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(doc_id): Path<i64>,
    headers: HeaderMap,
    Form(form): Form<RetryForm>,
) -> Response {
    if let Err((status, msg)) = csrf::validate_csrf(&headers, &form.csrf_token) {
        return csrf_error_response(status, &msg);
    }

    // RLS-scoped ownership proof BEFORE touching the jobs table: a foreign or
    // unknown id renders the 404 page and the retry never runs.
    match state
        .pg_store
        .get_document(auth_user.tenant_id, doc_id)
        .await
    {
        Ok(Some(_)) => {}
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Html("<h1>404 — Document not found</h1>".to_owned()),
            )
                .into_response();
        }
        Err(e) => {
            tracing::error!(error = %e, %doc_id, "retry: failed to fetch document");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Html("Internal server error".to_owned()),
            )
                .into_response();
        }
    }

    let message = match state
        .pg_store
        .retry_failed_ingest(auth_user.tenant_id, doc_id)
        .await
    {
        Ok(kb_store::RetryIngestOutcome::Retried { .. }) => "",
        Ok(kb_store::RetryIngestOutcome::NotRetryable { status }) => {
            tracing::info!(%doc_id, %status, "retry refused: job not in a terminal failure state");
            "This document's ingest is not in a failed state — nothing to retry."
        }
        Ok(kb_store::RetryIngestOutcome::NoJob) => {
            "This document has no background ingest job to retry."
        }
        Err(e) => {
            tracing::error!(error = %e, %doc_id, "retry failed");
            "Retry failed — please try again."
        }
    };

    let csrf = generate_fresh_csrf();
    let mut resp =
        build_document_detail_page(&state, auth_user.tenant_id, doc_id, &csrf, message).await;
    resp.headers_mut().insert(
        axum::http::header::SET_COOKIE,
        csrf::csrf_cookie_value(&csrf, state.session_ttl, state.secure_cookies),
    );
    resp
}

// ── GET /documents/:id ──────────────────────────────────────────────────────────

/// `GET /documents/:id` — render the document detail page.
///
/// Loads the document, its files (ordered by page number), canonical tags,
/// and the tenant's full tag list (for autocomplete) from the database,
/// then renders the Askama template.
pub async fn document_detail_page(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(doc_id): Path<i64>,
) -> Response {
    let csrf = match csrf::generate_csrf_token() {
        Ok(t) => t,
        Err(e) => {
            tracing::error!(error = %e, "failed to generate CSRF token");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Html("Internal server error".to_owned()),
            )
                .into_response();
        }
    };

    let mut resp = build_document_detail_page(&state, auth_user.tenant_id, doc_id, &csrf, "").await;

    resp.headers_mut().insert(
        axum::http::header::SET_COOKIE,
        csrf::csrf_cookie_value(&csrf, state.session_ttl, state.secure_cookies),
    );

    resp
}

// ── POST /documents/:id/tags/:tag_id/remove ─────────────────────────────────────

/// `POST /documents/:id/tags/:tag_id/remove` — remove a tag from a document.
///
/// Validates CSRF and deletes the document-tag link regardless of source/locked
/// (intentional user action). Redirects back to the document detail page.
pub async fn remove_document_tag(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path((doc_id, tag_id)): Path<(i64, i64)>,
    headers: HeaderMap,
    Form(form): Form<RemoveTagForm>,
) -> Response {
    if let Err((status, msg)) = csrf::validate_csrf(&headers, &form.csrf_token) {
        return csrf_error_response(status, &msg);
    }

    if let Err(e) = state
        .pg_store
        .remove_document_tag(auth_user.tenant_id, doc_id, tag_id)
        .await
    {
        tracing::warn!(error = %e, %doc_id, %tag_id, "failed to remove document tag");
    }

    let csrf = generate_fresh_csrf();
    let mut resp = build_document_detail_page(&state, auth_user.tenant_id, doc_id, &csrf, "").await;
    resp.headers_mut().insert(
        axum::http::header::SET_COOKIE,
        csrf::csrf_cookie_value(&csrf, state.session_ttl, state.secure_cookies),
    );
    resp
}

// ── POST /documents/:id/add-tag ─────────────────────────────────────────────────

/// `POST /documents/:id/add-tag` — add a tag to a document by canonical name.
///
/// Looks up the tag by name; if it exists, links it as a user-sourced, locked
/// tag. If it doesn't exist, a new tag is created (no embedding) and linked.
/// The tag is locked so it survives re-tag operations (P6-T11 contract).
pub async fn add_document_tag(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(doc_id): Path<i64>,
    headers: HeaderMap,
    Form(form): Form<AddTagForm>,
) -> Response {
    if let Err((status, msg)) = csrf::validate_csrf(&headers, &form.csrf_token) {
        return csrf_error_response(status, &msg);
    }

    let tag_name = form.tag_name.trim().to_string();
    if tag_name.is_empty() {
        let csrf = generate_fresh_csrf();
        let mut resp = build_document_detail_page(
            &state,
            auth_user.tenant_id,
            doc_id,
            &csrf,
            "Tag name cannot be empty.",
        )
        .await;
        resp.headers_mut().insert(
            axum::http::header::SET_COOKIE,
            csrf::csrf_cookie_value(&csrf, state.session_ttl, state.secure_cookies),
        );
        return resp;
    }

    // Look up existing tag or create a new one (no embedding — user-sourced).
    let tag_id = match state
        .pg_store
        .find_tag_by_name(auth_user.tenant_id, &tag_name)
        .await
    {
        Ok(Some(id)) => id,
        Ok(None) => {
            // Create a new tag with a zero embedding at the schema dimension
            // ([`kb_store::EMBED_DIM`]; must match the tags.embedding VECTOR(N) or the DB
            // rejects the INSERT). A user-sourced tag's embedding is never used for search.
            let dummy_embed: Vec<f32> = vec![0.0; kb_store::EMBED_DIM];
            match state
                .pg_store
                .upsert_tag(auth_user.tenant_id, &tag_name, &dummy_embed)
                .await
            {
                Ok(id) => id,
                Err(e) => {
                    tracing::error!(error = %e, "failed to create tag");
                    let csrf = generate_fresh_csrf();
                    let mut resp = build_document_detail_page(
                        &state,
                        auth_user.tenant_id,
                        doc_id,
                        &csrf,
                        "Failed to create tag.",
                    )
                    .await;
                    resp.headers_mut().insert(
                        axum::http::header::SET_COOKIE,
                        csrf::csrf_cookie_value(&csrf, state.session_ttl, state.secure_cookies),
                    );
                    return resp;
                }
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to look up tag");
            let csrf = generate_fresh_csrf();
            let mut resp = build_document_detail_page(
                &state,
                auth_user.tenant_id,
                doc_id,
                &csrf,
                "Failed to look up tag.",
            )
            .await;
            resp.headers_mut().insert(
                axum::http::header::SET_COOKIE,
                csrf::csrf_cookie_value(&csrf, state.session_ttl, state.secure_cookies),
            );
            return resp;
        }
    };

    // Link the tag to the document as user-sourced and locked.
    if let Err(e) = state
        .pg_store
        .insert_document_tags(auth_user.tenant_id, doc_id, &[tag_id], TagSource::User)
        .await
    {
        tracing::warn!(error = %e, %doc_id, %tag_id, "failed to link tag to document");
    }

    let csrf = generate_fresh_csrf();
    let mut resp = build_document_detail_page(&state, auth_user.tenant_id, doc_id, &csrf, "").await;
    resp.headers_mut().insert(
        axum::http::header::SET_COOKIE,
        csrf::csrf_cookie_value(&csrf, state.session_ttl, state.secure_cookies),
    );
    resp
}

// ── POST /documents/:id/retag ───────────────────────────────────────────────────

/// `POST /documents/:id/retag` — enqueue a retag job for the document.
///
/// The retag job re-runs the tagger on the current document text while
/// preserving locked/user tags (P6-T11). The result is available after
/// the worker processes the job.
pub async fn retag_document(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(doc_id): Path<i64>,
    headers: HeaderMap,
    Form(form): Form<RetagForm>,
) -> Response {
    if let Err((status, msg)) = csrf::validate_csrf(&headers, &form.csrf_token) {
        return csrf_error_response(status, &msg);
    }

    let job_queue = match state.job_queue.as_ref() {
        Some(q) => q,
        None => {
            let csrf = generate_fresh_csrf();
            let mut resp = build_document_detail_page(
                &state,
                auth_user.tenant_id,
                doc_id,
                &csrf,
                "Job queue not configured — cannot retag.",
            )
            .await;
            resp.headers_mut().insert(
                axum::http::header::SET_COOKIE,
                csrf::csrf_cookie_value(&csrf, state.session_ttl, state.secure_cookies),
            );
            return resp;
        }
    };

    match job_queue
        .enqueue(
            auth_user.tenant_id,
            Some(auth_user.user_id),
            None,
            Some(doc_id),
            JobKind::Retag,
            100,
        )
        .await
    {
        Ok(_job_id) => {
            tracing::info!(%doc_id, "retag job enqueued");
            let csrf = generate_fresh_csrf();
            let mut resp =
                build_document_detail_page(&state, auth_user.tenant_id, doc_id, &csrf, "").await;
            resp.headers_mut().insert(
                axum::http::header::SET_COOKIE,
                csrf::csrf_cookie_value(&csrf, state.session_ttl, state.secure_cookies),
            );
            resp
        }
        Err(e) => {
            tracing::error!(error = %e, %doc_id, "failed to enqueue retag job");
            let csrf = generate_fresh_csrf();
            let mut resp = build_document_detail_page(
                &state,
                auth_user.tenant_id,
                doc_id,
                &csrf,
                "Failed to enqueue retag job.",
            )
            .await;
            resp.headers_mut().insert(
                axum::http::header::SET_COOKIE,
                csrf::csrf_cookie_value(&csrf, state.session_ttl, state.secure_cookies),
            );
            resp
        }
    }
}

// ── POST /documents/:id/reorder ─────────────────────────────────────────────────

/// `POST /documents/:id/reorder` — update file page numbers.
///
/// Accepts comma-separated `file_ids` and `page_nos` strings. Each file's
/// `page_no` is updated to the corresponding value.
pub async fn reorder_pages(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(doc_id): Path<i64>,
    headers: HeaderMap,
    Form(form): Form<ReorderForm>,
) -> Response {
    if let Err((status, msg)) = csrf::validate_csrf(&headers, &form.csrf_token) {
        return csrf_error_response(status, &msg);
    }

    let (file_ids, page_nos) = form.parse_ids();

    if file_ids.is_empty() {
        let csrf = generate_fresh_csrf();
        let mut resp = build_document_detail_page(
            &state,
            auth_user.tenant_id,
            doc_id,
            &csrf,
            "No files to reorder.",
        )
        .await;
        resp.headers_mut().insert(
            axum::http::header::SET_COOKIE,
            csrf::csrf_cookie_value(&csrf, state.session_ttl, state.secure_cookies),
        );
        return resp;
    }

    if file_ids.len() != page_nos.len() {
        let csrf = generate_fresh_csrf();
        let mut resp = build_document_detail_page(
            &state,
            auth_user.tenant_id,
            doc_id,
            &csrf,
            "Mismatched file_ids and page_nos counts.",
        )
        .await;
        resp.headers_mut().insert(
            axum::http::header::SET_COOKIE,
            csrf::csrf_cookie_value(&csrf, state.session_ttl, state.secure_cookies),
        );
        return resp;
    }

    for (i, (&file_id, page_no)) in file_ids.iter().zip(page_nos.iter()).enumerate() {
        if let Err(e) = state
            .pg_store
            .update_file_page_order(auth_user.tenant_id, file_id, *page_no)
            .await
        {
            tracing::warn!(error = %e, %file_id, %page_no, idx = i, "failed to update page order");
        }
    }

    let csrf = generate_fresh_csrf();
    let mut resp = build_document_detail_page(&state, auth_user.tenant_id, doc_id, &csrf, "").await;
    resp.headers_mut().insert(
        axum::http::header::SET_COOKIE,
        csrf::csrf_cookie_value(&csrf, state.session_ttl, state.secure_cookies),
    );
    resp
}

// ── POST /documents/:id/add-page ────────────────────────────────────────────────

/// `POST /documents/:id/add-page` — upload an additional file as a new page.
///
/// Parses a multipart form with one or more files, stores them in the blob
/// store, and enqueues an ingest job for the document.
pub async fn add_page(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(doc_id): Path<i64>,
    headers: HeaderMap,
    multipart: axum::extract::Multipart,
) -> Response {
    // ── Resolve the ingest config per request (hot-swappable) ───────────────
    let ingest_cfg = state
        .app_config
        .as_ref()
        .map(|c| c.current().ingest.clone())
        .unwrap_or_default();

    let parsed =
        match crate::handlers::ingest::parse_multipart(multipart, ingest_cfg.max_payload_bytes)
            .await
        {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, "add_page: bad multipart");
                let csrf = generate_fresh_csrf();
                let mut resp = build_document_detail_page(
                    &state,
                    auth_user.tenant_id,
                    doc_id,
                    &csrf,
                    &format!("Invalid request: {e}"),
                )
                .await;
                resp.headers_mut().insert(
                    axum::http::header::SET_COOKIE,
                    csrf::csrf_cookie_value(&csrf, state.session_ttl, state.secure_cookies),
                );
                return resp;
            }
        };

    // Validate CSRF.
    if let Err((_status, msg)) = csrf::validate_csrf(&headers, &parsed.csrf_token) {
        let csrf = generate_fresh_csrf();
        let mut resp =
            build_document_detail_page(&state, auth_user.tenant_id, doc_id, &csrf, &msg).await;
        resp.headers_mut().insert(
            axum::http::header::SET_COOKIE,
            csrf::csrf_cookie_value(&csrf, state.session_ttl, state.secure_cookies),
        );
        return resp;
    }

    if parsed.files.is_empty() {
        let csrf = generate_fresh_csrf();
        let mut resp = build_document_detail_page(
            &state,
            auth_user.tenant_id,
            doc_id,
            &csrf,
            "No files selected.",
        )
        .await;
        resp.headers_mut().insert(
            axum::http::header::SET_COOKIE,
            csrf::csrf_cookie_value(&csrf, state.session_ttl, state.secure_cookies),
        );
        return resp;
    }

    let blob = match state.blob.as_ref() {
        Some(b) => b.as_ref(),
        None => {
            let csrf = generate_fresh_csrf();
            let mut resp = build_document_detail_page(
                &state,
                auth_user.tenant_id,
                doc_id,
                &csrf,
                "Blob store not configured.",
            )
            .await;
            resp.headers_mut().insert(
                axum::http::header::SET_COOKIE,
                csrf::csrf_cookie_value(&csrf, state.session_ttl, state.secure_cookies),
            );
            return resp;
        }
    };

    let job_queue = match state.job_queue.as_ref() {
        Some(q) => q,
        None => {
            let csrf = generate_fresh_csrf();
            let mut resp = build_document_detail_page(
                &state,
                auth_user.tenant_id,
                doc_id,
                &csrf,
                "Job queue not configured.",
            )
            .await;
            resp.headers_mut().insert(
                axum::http::header::SET_COOKIE,
                csrf::csrf_cookie_value(&csrf, state.session_ttl, state.secure_cookies),
            );
            return resp;
        }
    };

    match crate::handlers::ingest::process_add_page_queued(
        blob,
        &state.pg_store,
        job_queue,
        auth_user.tenant_id,
        Some(auth_user.user_id),
        doc_id,
        &parsed,
    )
    .await
    {
        Ok((_job_id, _file_count)) => {
            let csrf = generate_fresh_csrf();
            let mut resp =
                build_document_detail_page(&state, auth_user.tenant_id, doc_id, &csrf, "").await;
            resp.headers_mut().insert(
                axum::http::header::SET_COOKIE,
                csrf::csrf_cookie_value(&csrf, state.session_ttl, state.secure_cookies),
            );
            resp
        }
        Err(e) => {
            tracing::error!(error = %e, %doc_id, "add-page failed");
            // Surface the specific reason for client-side rejections (zero-byte /
            // disallowed-MIME / oversized / unsafe-named) rather than a generic
            // failure message; keep the generic message for true server faults.
            let msg = match e.downcast_ref::<kb_extract::security::UploadRejected>() {
                Some(rejected) => rejected.to_string(),
                None => "Failed to process uploaded files.".to_string(),
            };
            let csrf = generate_fresh_csrf();
            let mut resp =
                build_document_detail_page(&state, auth_user.tenant_id, doc_id, &csrf, &msg).await;
            resp.headers_mut().insert(
                axum::http::header::SET_COOKIE,
                csrf::csrf_cookie_value(&csrf, state.session_ttl, state.secure_cookies),
            );
            resp
        }
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::Arc;
    use std::time::Duration;

    use axum::Router;
    use axum::body::Body;
    use axum::http::{Method, StatusCode, header};
    use axum::routing::{get, post};
    use kb_core::session::{DEFAULT_SESSION_TTL_SECS, SessionStore};
    use kb_core::user::UserRole;
    use kb_store::PgStore;
    use kb_store::session_store::InMemorySessionStore;
    use tower::ServiceExt;

    use super::*;
    use crate::AppState;
    use crate::middleware::auth_middleware;
    use crate::web::security::security_headers_middleware;

    fn test_state() -> Arc<AppState> {
        let session_store: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
        let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
        Arc::new(AppState::new(
            session_store,
            pg_store,
            Some(Duration::from_secs(DEFAULT_SESSION_TTL_SECS)),
            false,
        ))
    }

    fn doc_page_router(state: Arc<AppState>) -> Router {
        let public = Router::new();

        let protected = Router::new()
            .route("/documents/{id}", get(document_detail_page))
            .route(
                "/documents/{id}/tags/{tag_id}/remove",
                post(remove_document_tag),
            )
            .route("/documents/{id}/add-tag", post(add_document_tag))
            .route("/documents/{id}/retag", post(retag_document))
            .route("/documents/{id}/reorder", post(reorder_pages))
            .route("/documents/{id}/add-page", post(add_page))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ));

        public
            .merge(protected)
            .layer(axum::middleware::from_fn(security_headers_middleware))
            .with_state(state)
    }

    // ── mime_icon tests ────────────────────────────────────────────────────

    #[test]
    fn mime_icon_image() {
        assert_eq!(mime_icon("image/png"), "\u{1f5bc}");
        assert_eq!(mime_icon("image/jpeg"), "\u{1f5bc}");
    }

    #[test]
    fn mime_icon_audio() {
        assert_eq!(mime_icon("audio/mpeg"), "\u{1f3b5}");
        assert_eq!(mime_icon("audio/wav"), "\u{1f3b5}");
    }

    #[test]
    fn mime_icon_video() {
        assert_eq!(mime_icon("video/mp4"), "\u{1f3ac}");
    }

    #[test]
    fn mime_icon_pdf() {
        assert_eq!(mime_icon("application/pdf"), "\u{1f4c4}");
    }

    #[test]
    fn mime_icon_default() {
        assert_eq!(mime_icon("application/octet-stream"), "\u{1f4c1}");
        assert_eq!(mime_icon("text/plain"), "\u{1f4c1}");
        assert_eq!(mime_icon(""), "\u{1f4c1}");
    }

    // ── format_file_size tests ──────────────────────────────────────────────

    #[test]
    fn format_bytes() {
        assert_eq!(format_file_size(0), "0 B");
        assert_eq!(format_file_size(500), "500 B");
    }

    #[test]
    fn format_kilobytes() {
        assert_eq!(format_file_size(1024), "1.0 KB");
        assert_eq!(format_file_size(1536), "1.5 KB");
    }

    #[test]
    fn format_megabytes() {
        assert_eq!(format_file_size(1_048_576), "1.0 MB");
        assert_eq!(format_file_size(5_500_000), "5.2 MB");
    }

    #[test]
    fn format_gigabytes() {
        assert_eq!(format_file_size(1_073_741_824), "1.0 GB");
    }

    #[test]
    fn format_negative_returns_zero() {
        assert_eq!(format_file_size(-1), "0 B");
    }

    // ── ReorderForm parse_ids tests ─────────────────────────────────────────

    #[test]
    fn parse_ids_single_pair() {
        let form = ReorderForm {
            csrf_token: "x".into(),
            file_ids: "10".into(),
            page_nos: "1".into(),
        };
        let (ids, nos) = form.parse_ids();
        assert_eq!(ids, vec![10]);
        assert_eq!(nos, vec![1]);
    }

    #[test]
    fn parse_ids_multiple_pairs() {
        let form = ReorderForm {
            csrf_token: "x".into(),
            file_ids: "10,20,30".into(),
            page_nos: "3,1,2".into(),
        };
        let (ids, nos) = form.parse_ids();
        assert_eq!(ids, vec![10, 20, 30]);
        assert_eq!(nos, vec![3, 1, 2]);
    }

    // ── sanitize_job_error (P15-T9) ────────────────────────────────────────

    #[test]
    fn sanitize_short_error_passes_through_trimmed() {
        assert_eq!(
            sanitize_job_error("  tagger model call failed  "),
            "tagger model call failed"
        );
    }

    #[test]
    fn sanitize_keeps_only_the_first_line() {
        assert_eq!(
            sanitize_job_error("extraction failed\ninternal stack line\nmore"),
            "extraction failed"
        );
    }

    #[test]
    fn sanitize_truncates_long_errors_on_char_boundary() {
        // 300 multibyte chars → truncated to 240 chars + ellipsis, valid UTF-8.
        let long = "é".repeat(300);
        let out = sanitize_job_error(&long);
        assert_eq!(out.chars().count(), 241, "240 kept + ellipsis");
        assert!(out.ends_with('…'));
        assert!(std::str::from_utf8(out.as_bytes()).is_ok());
    }

    #[test]
    fn sanitize_empty_is_empty() {
        assert_eq!(sanitize_job_error(""), "");
    }

    #[test]
    fn parse_ids_empty() {
        let form = ReorderForm {
            csrf_token: "x".into(),
            file_ids: String::new(),
            page_nos: String::new(),
        };
        let (ids, nos) = form.parse_ids();
        assert!(ids.is_empty());
        assert!(nos.is_empty());
    }

    #[test]
    fn parse_ids_whitespace_tolerant() {
        let form = ReorderForm {
            csrf_token: "x".into(),
            file_ids: " 10 , 20 ".into(),
            page_nos: " 1, 2 ".into(),
        };
        let (ids, nos) = form.parse_ids();
        assert_eq!(ids, vec![10, 20]);
        assert_eq!(nos, vec![1, 2]);
    }

    #[test]
    fn parse_ids_mismatched_lengths() {
        let form = ReorderForm {
            csrf_token: "x".into(),
            file_ids: "1,2,3".into(),
            page_nos: "1".into(),
        };
        let (ids, nos) = form.parse_ids();
        assert_eq!(ids.len(), 3);
        assert_eq!(nos.len(), 1);
    }

    // ── GET /documents/:id tests ────────────────────────────────────────────

    #[tokio::test]
    async fn document_detail_unauthenticated_returns_401() {
        let state = test_state();
        let router = doc_page_router(state);

        let req = axum::http::Request::builder()
            .uri("/documents/1")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn document_detail_authenticated_db_fails_returns_500() {
        let state = test_state();
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = doc_page_router(state);

        let req = axum::http::Request::builder()
            .uri("/documents/1")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        // Without a connected DB, get_document returns an error → 500.
        assert!(resp.status().is_server_error());
    }

    #[tokio::test]
    async fn document_detail_has_security_headers() {
        let state = test_state();
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = doc_page_router(state);

        let req = axum::http::Request::builder()
            .uri("/documents/1")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert!(resp.headers().contains_key("content-security-policy"));
        assert!(resp.headers().contains_key("x-content-type-options"));
    }

    // ── POST /documents/:id/tags/:tag_id/remove ────────────────────────────

    #[tokio::test]
    async fn remove_tag_unauthenticated_returns_401() {
        let state = test_state();
        let router = doc_page_router(state);

        let body = "csrf_token=fake";
        let req = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/documents/1/tags/42/remove")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn remove_tag_no_csrf_cookie_returns_403() {
        let state = test_state();
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = doc_page_router(state);

        let body = "csrf_token=faketoken";
        let req = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/documents/1/tags/42/remove")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    // ── POST /documents/:id/add-tag ────────────────────────────────────────

    #[tokio::test]
    async fn add_tag_unauthenticated_returns_401() {
        let state = test_state();
        let router = doc_page_router(state);

        let body = "csrf_token=fake&tag_name=important";
        let req = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/documents/1/add-tag")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn add_tag_no_csrf_cookie_returns_403() {
        let state = test_state();
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = doc_page_router(state);

        let body = "csrf_token=fake&tag_name=test";
        let req = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/documents/1/add-tag")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    // ── POST /documents/:id/retag ──────────────────────────────────────────

    #[tokio::test]
    async fn retag_unauthenticated_returns_401() {
        let state = test_state();
        let router = doc_page_router(state);

        let body = "csrf_token=fake";
        let req = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/documents/1/retag")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn retag_no_csrf_cookie_returns_403() {
        let state = test_state();
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = doc_page_router(state);

        let body = "csrf_token=fake123";
        let req = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/documents/1/retag")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn retag_without_job_queue_returns_error() {
        let state = test_state();
        let csrf_token = csrf::generate_csrf_token().unwrap();
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = doc_page_router(state);

        let body = format!("csrf_token={csrf_token}");
        let req = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/documents/1/retag")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(
                "Cookie",
                format!("__Host-session={token}; __Host-csrf={csrf_token}"),
            )
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        // Renders the detail page with error (DB failure → 500).
        assert!(resp.status().is_server_error());
    }

    // ── POST /documents/:id/reorder ────────────────────────────────────────

    #[tokio::test]
    async fn reorder_unauthenticated_returns_401() {
        let state = test_state();
        let router = doc_page_router(state);

        let body = "csrf_token=fake&file_ids=1&page_nos=1";
        let req = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/documents/1/reorder")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn reorder_no_csrf_cookie_returns_403() {
        let state = test_state();
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = doc_page_router(state);

        let body = "csrf_token=fake&file_ids=1&page_nos=1";
        let req = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/documents/1/reorder")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    // ── POST /documents/:id/add-page ───────────────────────────────────────

    #[tokio::test]
    async fn add_page_unauthenticated_returns_401() {
        let state = test_state();
        let router = doc_page_router(state);

        let boundary = "addpg1";
        let body_str = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"csrf_token\"\r\n\r\nabc\r\n--{boundary}--\r\n"
        );
        let req = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/documents/1/add-page")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(Body::from(body_str))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── Form deserialization tests ──────────────────────────────────────────

    #[test]
    fn add_tag_form_deserialization() {
        let body = "csrf_token=abc&tag_name=important";
        let form: AddTagForm = serde_urlencoded::from_str(body).unwrap();
        assert_eq!(form.csrf_token, "abc");
        assert_eq!(form.tag_name, "important");
    }

    #[test]
    fn add_tag_form_empty_tag_name() {
        let body = "csrf_token=abc";
        let form: AddTagForm = serde_urlencoded::from_str(body).unwrap();
        assert_eq!(form.csrf_token, "abc");
        assert!(form.tag_name.is_empty());
    }

    #[test]
    fn remove_tag_form_deserialization() {
        let body = "csrf_token=xyz";
        let form: RemoveTagForm = serde_urlencoded::from_str(body).unwrap();
        assert_eq!(form.csrf_token, "xyz");
    }

    #[test]
    fn retag_form_deserialization() {
        let body = "csrf_token=retag123";
        let form: RetagForm = serde_urlencoded::from_str(body).unwrap();
        assert_eq!(form.csrf_token, "retag123");
    }

    #[test]
    fn reorder_form_deserialization() {
        let body = "csrf_token=csrf1&file_ids=10,20&page_nos=1,2";
        let form: ReorderForm = serde_urlencoded::from_str(body).unwrap();
        assert_eq!(form.csrf_token, "csrf1");
        assert_eq!(form.file_ids, "10,20");
        assert_eq!(form.page_nos, "1,2");
        let (ids, nos) = form.parse_ids();
        assert_eq!(ids, vec![10, 20]);
        assert_eq!(nos, vec![1, 2]);
    }

    #[test]
    fn reorder_form_empty_defaults() {
        let body = "csrf_token=x";
        let form: ReorderForm = serde_urlencoded::from_str(body).unwrap();
        assert_eq!(form.csrf_token, "x");
        assert!(form.file_ids.is_empty());
        assert!(form.page_nos.is_empty());
    }
}
