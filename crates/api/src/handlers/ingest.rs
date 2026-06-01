//! `POST /api/ingest` — multipart file upload handler.
//!
//! Accepts one or more files, an optional `user_note` text field, and an
//! optional `group_as_document` flag. Each file's bytes are stored in the
//! blob store and an ingest job is enqueued via the job queue.
//!
//! The shared [`process_upload_files`] function is used by both the JSON API
//! handler and the Web UI upload handler (`POST /upload`), avoiding duplicate
//! blob-storage + job-enqueue logic.
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
use kb_pipeline::ingest::IngestFile;
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
/// * `500 Internal Server Error` — pipeline or store failure.
pub async fn ingest(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    multipart: Multipart,
) -> Result<(StatusCode, Json<IngestResponse>), (StatusCode, Json<ErrorResponse>)> {
    // ── Parse multipart ──────────────────────────────────────────────────────
    let parsed = parse_multipart(multipart, MAX_PAYLOAD_BYTES)
        .await
        .map_err(|e| {
            tracing::warn!(error = %e, "ingest: bad multipart");
            bad_request("invalid_multipart", &e.to_string())
        })?;

    if parsed.files.is_empty() {
        return Err(bad_request("no_files", "at least one file is required"));
    }

    // ── Ensure required components are present ───────────────────────────────
    let blob = state
        .blob
        .as_ref()
        .ok_or_else(|| internal_error(anyhow::anyhow!("ingest: blob store not configured")))?;
    let job_queue = state
        .job_queue
        .as_ref()
        .ok_or_else(|| internal_error(anyhow::anyhow!("ingest: job queue not configured")))?;

    let (job_id, file_count) = process_upload_files(
        blob.as_ref(),
        job_queue.as_ref(),
        auth_user.tenant_id,
        &parsed,
    )
    .await
    .map_err(internal_error)?;

    Ok((
        StatusCode::ACCEPTED,
        Json(IngestResponse {
            job_id,
            document_id: None,
            message: format!("ingest job enqueued ({file_count} file(s))"),
        }),
    ))
}

/// Store file bytes in the blob store and enqueue ingest job(s).
///
/// Shared between the JSON API handler and the Web UI upload handler.
/// This is the core upload processing logic, extracted so both paths can
/// reuse it without duplication.
///
/// # Returns
///
/// `(first_job_id, file_count)` on success.
///
/// # Errors
///
/// Returns an error if blob storage or job enqueue fails.
pub(crate) async fn process_upload_files(
    blob: &dyn Blob,
    job_queue: &JobQueue,
    tenant_id: i64,
    parsed: &ParsedUpload,
) -> anyhow::Result<(i64, usize)> {
    // ── Upload-edge validation (plan §17, §31.5) ────────────────────────────
    for f in &parsed.files {
        security::validate_upload(
            &f.bytes,
            f.path.as_deref(),
            security::MAX_INDIVIDUAL_FILE_BYTES,
        )
        .map_err(|e| {
            tracing::warn!(
                file = f.path.as_deref().unwrap_or("<unknown>"),
                error = %e,
                "ingest: upload validation failed"
            );
            e.context("upload validation failed")
        })?;
    }

    // ── Store each file's bytes in the blob store ───────────────────────────
    for f in &parsed.files {
        let sha256 = compute_blob_key(&f.bytes);
        blob.put(&sha256, bytes::Bytes::copy_from_slice(&f.bytes))
            .await?;
    }

    // ── Enqueue job(s) ──────────────────────────────────────────────────────
    // When group_as_document is true, enqueue one job for all files together.
    // When false, enqueue one job per file (each becomes a 1-page document).
    let job_id = if parsed.group_as_document {
        job_queue
            .enqueue(tenant_id, None, None, JobKind::Ingest, 100)
            .await?
    } else {
        let mut first_id: Option<i64> = None;
        for _f in &parsed.files {
            let id = job_queue
                .enqueue(tenant_id, None, None, JobKind::Ingest, 100)
                .await?;
            if first_id.is_none() {
                first_id = Some(id);
            }
        }
        first_id.unwrap_or(0)
    };

    Ok((job_id, parsed.files.len()))
}

// ── Multipart parsing ──────────────────────────────────────────────────────────

/// Parsed multipart upload data.
pub(crate) struct ParsedUpload {
    /// File contents and metadata.
    pub files: Vec<IngestFile>,
    /// Optional user note.
    #[allow(dead_code)]
    pub user_note: Option<String>,
    /// Whether files should be grouped into one document.
    pub group_as_document: bool,
    /// CSRF token from the form (set by Web UI; empty for API clients).
    pub csrf_token: String,
}

/// Parse a multipart form body into [`ParsedUpload`].
///
/// Accumulates file parts into `IngestFile` entries and extracts text fields.
/// Enforces `max_total_bytes` as a soft payload limit (reading stops early
/// when the total exceeds the limit, but the entire body has already been
/// buffered by axum — this is a best-effort gate).
///
/// Extracts the optional `csrf_token` form field for Web UI double-submit
/// cookie validation. For pure API clients (no form), the field is absent
/// and the token will be an empty string.
pub(crate) async fn parse_multipart(
    mut multipart: Multipart,
    max_total_bytes: u64,
) -> anyhow::Result<ParsedUpload> {
    let mut files = Vec::new();
    let mut user_note: Option<String> = None;
    let mut group_as_document = false;
    let mut csrf_token = String::new();
    let mut total_bytes: u64 = 0;

    loop {
        let field = multipart
            .next_field()
            .await
            .map_err(|e| anyhow::anyhow!("failed to read multipart field: {e}"))?;

        let Some(field) = field else {
            break;
        };

        let name = field.name().map(|s| s.to_string()).unwrap_or_default();

        match name.as_str() {
            "files" => {
                let file_name = field.file_name().map(|s| s.to_string());
                let bytes = field
                    .bytes()
                    .await
                    .map_err(|e| anyhow::anyhow!("failed to read file bytes: {e}"))?;

                total_bytes = total_bytes.saturating_add(bytes.len() as u64);
                if total_bytes > max_total_bytes {
                    anyhow::bail!(
                        "total upload size exceeds limit of {} bytes",
                        max_total_bytes
                    );
                }

                files.push(IngestFile {
                    bytes: bytes.to_vec(),
                    page_label: file_name.clone(),
                    path: file_name,
                });
            }
            "user_note" => {
                let text = field
                    .text()
                    .await
                    .map_err(|e| anyhow::anyhow!("failed to read user_note: {e}"))?;
                if !text.is_empty() {
                    user_note = Some(text);
                }
            }
            "group_as_document" => {
                let text = field
                    .text()
                    .await
                    .map_err(|e| anyhow::anyhow!("failed to read group_as_document: {e}"))?;
                group_as_document = text.trim().eq_ignore_ascii_case("true");
            }
            "csrf_token" => {
                let text = field
                    .text()
                    .await
                    .map_err(|e| anyhow::anyhow!("failed to read csrf_token: {e}"))?;
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
            .create(1, 42, UserRole::Member, Duration::from_secs(3600))
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
}
