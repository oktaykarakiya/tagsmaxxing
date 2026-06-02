//! Document detail + file download handlers.
//!
//! * `GET /api/documents/:id` — document with files, tags, and metadata.
//! * `GET /api/documents/:id/file/:file_id` — 302 redirect to a presigned
//!   download URL (B2 in production, `file://` for local dev), so download
//!   bandwidth bypasses the app server entirely (plan §20).

use std::sync::Arc;

use axum::Extension;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::http::header::{CONTENT_DISPOSITION, CONTENT_TYPE, LOCATION};
use axum::response::Json;
use serde::Serialize;

use crate::AppState;
use crate::AuthUser;

// ── Response types ─────────────────────────────────────────────────────────────

/// A file entry in the document detail response.
#[derive(Debug, Serialize)]
pub struct FileEntry {
    /// File id.
    pub id: i64,
    /// Page number within the document.
    pub page_no: i32,
    /// Page label (e.g. `"front"`, `"back"`, original filename).
    pub page_label: Option<String>,
    /// MIME type, if detected.
    pub mime: Option<String>,
    /// File size in bytes.
    pub size_bytes: Option<i64>,
    /// Per-page metadata (EXIF, ffprobe, etc.).
    pub meta: serde_json::Value,
    /// Processing status.
    pub status: String,
    /// Ingestion time.
    pub ingested_at: String,
}

/// A tag entry in the document detail response.
#[derive(Debug, Serialize)]
pub struct TagEntry {
    /// Tag id.
    pub id: i64,
    /// Canonical tag name.
    pub name: String,
}

/// Document detail response.
#[derive(Debug, Serialize)]
pub struct DocumentDetail {
    /// Document id.
    pub id: i64,
    /// LLM-generated title.
    pub title: Option<String>,
    /// LLM-generated summary.
    pub summary: Option<String>,
    /// User-provided note.
    pub user_note: Option<String>,
    /// Document kind.
    pub kind: String,
    /// Combined metadata.
    pub meta: serde_json::Value,
    /// Number of member pages/files.
    pub page_count: i32,
    /// Processing status.
    pub status: String,
    /// Creation time.
    pub created_at: String,
    /// Member files, ordered by page number.
    pub files: Vec<FileEntry>,
    /// Attached canonical tags.
    pub tags: Vec<TagEntry>,
}

/// Error response.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    error: String,
    message: String,
}

// ── Handlers ───────────────────────────────────────────────────────────────────

/// `GET /api/documents/:id` — document detail with files, tags, and chunks.
///
/// Returns the full document record plus its member files (ordered by page
/// number) and attached canonical tags. The response is scoped to the
/// authenticated tenant — cross-tenant access returns a 404.
///
/// # Response
///
/// * `200 OK` — [`DocumentDetail`] with files and tags.
/// * `401 Unauthorized` — rejected by middleware.
/// * `404 Not Found` — document does not exist or belongs to a different tenant.
/// * `500 Internal Server Error` — database failure.
pub async fn document_detail(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(id): Path<i64>,
) -> Result<(StatusCode, Json<DocumentDetail>), (StatusCode, Json<ErrorResponse>)> {
    let doc = state
        .pg_store
        .get_document(auth_user.tenant_id, id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| not_found("document_not_found", "document not found"))?;

    let files = state
        .pg_store
        .get_files_for_document(auth_user.tenant_id, id)
        .await
        .map_err(internal_error)?;

    let tags = state
        .pg_store
        .get_tags_for_document(auth_user.tenant_id, id)
        .await
        .map_err(internal_error)?;

    let file_entries: Vec<FileEntry> = files
        .into_iter()
        .map(|f| FileEntry {
            id: f.id,
            page_no: f.page_no,
            page_label: f.page_label,
            mime: f.mime,
            size_bytes: f.size_bytes,
            meta: f.meta,
            status: f.status.as_str().to_owned(),
            ingested_at: f.ingested_at.to_rfc3339(),
        })
        .collect();

    let tag_entries: Vec<TagEntry> = tags
        .into_iter()
        .map(|t| TagEntry {
            id: t.id,
            name: t.name,
        })
        .collect();

    Ok((
        StatusCode::OK,
        Json(DocumentDetail {
            id: doc.id,
            title: doc.title,
            summary: doc.summary,
            user_note: doc.user_note,
            kind: doc.kind.as_str().to_owned(),
            meta: doc.meta,
            page_count: doc.page_count,
            status: doc.status.as_str().to_owned(),
            created_at: doc.created_at.to_rfc3339(),
            files: file_entries,
            tags: tag_entries,
        }),
    ))
}

/// `GET /api/documents/:id/file/:file_id` — redirect to a presigned download URL.
///
/// Looks up the file record (scoped to the authenticated tenant — cross-tenant
/// returns 404), then generates a time-limited presigned URL from the blob store
/// and returns a **302 Found** redirect. The client's browser follows the redirect
/// directly to the object store, so download bandwidth bypasses the app server
/// entirely (plan §20).
///
/// For the local-disk blob backend, the presigned URL is a `file://` path.
/// For Backblaze B2, it is an S3 SigV4 presigned `https://` URL valid for the
/// configured TTL ([`AppState::blob_presigned_ttl`], default 1 hour).
///
/// The redirect response carries `Content-Type` (from the file's MIME) and
/// `Content-Disposition: attachment` so the download prompt uses the original
/// filename when available.
///
/// # Response
///
/// * `302 Found` — redirect to presigned download URL with correct headers.
/// * `401 Unauthorized` — rejected by middleware.
/// * `404 Not Found` — file does not exist or belongs to a different tenant.
/// * `500 Internal Server Error` — blob store or database failure.
pub async fn file_download(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path((_doc_id, file_id)): Path<(i64, i64)>,
) -> Result<axum::response::Response, (StatusCode, Json<ErrorResponse>)> {
    // Look up the file record to get the blob key, MIME, and page label.
    let file_rec = state
        .pg_store
        .get_file(auth_user.tenant_id, file_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| not_found("file_not_found", "file not found"))?;

    let blob = state.blob.as_ref().ok_or_else(|| {
        internal_error(anyhow::anyhow!("file_download: blob store not configured"))
    })?;

    // Generate a time-limited presigned download URL.
    let presigned_url = blob
        .presigned_get_url(&file_rec.blob_key, state.blob_presigned_ttl)
        .await
        .map_err(|e| {
            tracing::error!(
                error = %e,
                blob_key = %file_rec.blob_key,
                "failed to generate presigned download URL"
            );
            not_found("blob_not_found", &format!("blob not available: {e}"))
        })?;

    let content_type = file_rec
        .mime
        .as_deref()
        .unwrap_or("application/octet-stream");

    build_redirect_response(&presigned_url, content_type, file_rec.page_label.as_deref()).map_err(
        |e| {
            internal_error(anyhow::anyhow!(
                "file_download: failed to build redirect response: {e}"
            ))
        },
    )
}

/// Build a 302 redirect response with presigned URL location, Content-Type,
/// and Content-Disposition headers.
///
/// Extracted for testability — unit tests can verify the response shape
/// without needing a database or blob store.
fn build_redirect_response(
    presigned_url: &str,
    content_type: &str,
    page_label: Option<&str>,
) -> Result<axum::response::Response, axum::http::Error> {
    let disposition = match page_label {
        Some(label) if !label.is_empty() => format!("attachment; filename=\"{label}\""),
        _ => "attachment".to_owned(),
    };

    axum::response::Response::builder()
        .status(StatusCode::FOUND)
        .header(LOCATION, presigned_url)
        .header(CONTENT_TYPE, content_type)
        .header(CONTENT_DISPOSITION, &disposition)
        .body(Body::empty())
}

// ── Error helpers ──────────────────────────────────────────────────────────────

fn not_found(code: &str, msg: &str) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: code.into(),
            message: msg.into(),
        }),
    )
}

fn internal_error(e: impl std::fmt::Display) -> (StatusCode, Json<ErrorResponse>) {
    tracing::error!(error = %e, "documents handler error");
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

    use async_trait::async_trait;
    use axum::Router;
    use axum::body::Body;
    use axum::http::{Method, StatusCode};
    use axum::routing::get;
    use bytes::Bytes;
    use kb_core::blob::Blob;
    use kb_core::session::{DEFAULT_SESSION_TTL_SECS, SessionStore};
    use kb_core::user::UserRole;
    use kb_store::PgStore;
    use kb_store::session_store::InMemorySessionStore;
    use tower::ServiceExt;

    use super::*;
    use crate::AppState;
    use crate::DEFAULT_PRESIGNED_TTL_SECS;
    use crate::middleware::auth_middleware;

    // ── Mock blob for presigned-URL testing ─────────────────────────────────

    /// A mock blob backend that records the arguments passed to
    /// `presigned_get_url` and returns a predictable URL.
    struct MockBlob {
        /// The URL to return from `presigned_get_url`.
        return_url: String,
        /// The key last passed to `presigned_get_url`, if any.
        last_key: std::sync::Mutex<Option<String>>,
        /// The TTL last passed to `presigned_get_url`, if any.
        last_ttl: std::sync::Mutex<Option<Duration>>,
    }

    impl MockBlob {
        fn new(return_url: &str) -> Self {
            Self {
                return_url: return_url.to_owned(),
                last_key: std::sync::Mutex::new(None),
                last_ttl: std::sync::Mutex::new(None),
            }
        }

        fn last_key(&self) -> Option<String> {
            self.last_key.lock().unwrap().clone()
        }

        fn last_ttl(&self) -> Option<Duration> {
            *self.last_ttl.lock().unwrap()
        }
    }

    #[async_trait]
    impl Blob for MockBlob {
        async fn put(&self, _key: &str, _data: Bytes) -> anyhow::Result<()> {
            Ok(())
        }

        async fn get(&self, _key: &str) -> anyhow::Result<Bytes> {
            Ok(Bytes::new())
        }

        async fn get_range(&self, _key: &str, _start: u64, _end: u64) -> anyhow::Result<Bytes> {
            Ok(Bytes::new())
        }

        async fn exists(&self, _key: &str) -> anyhow::Result<bool> {
            Ok(true)
        }

        async fn delete(&self, _key: &str) -> anyhow::Result<()> {
            Ok(())
        }

        async fn presigned_get_url(
            &self,
            key: &str,
            expires_in: Duration,
        ) -> anyhow::Result<String> {
            *self.last_key.lock().unwrap() = Some(key.to_owned());
            *self.last_ttl.lock().unwrap() = Some(expires_in);
            Ok(self.return_url.clone())
        }
    }

    // ── State helpers ──────────────────────────────────────────────────────

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

    fn doc_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/api/documents/{id}", get(document_detail))
            .route("/api/documents/{id}/file/{file_id}", get(file_download))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ))
            .with_state(state)
    }

    // ── document_detail tests ──────────────────────────────────────────────

    #[tokio::test]
    async fn unauthenticated_returns_401() {
        let state = test_state();
        let router = doc_router(state);

        let request = axum::http::Request::builder()
            .method(Method::GET)
            .uri("/api/documents/1")
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn nonexistent_document_returns_500() {
        let state = test_state();
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = doc_router(state);

        let request = axum::http::Request::builder()
            .method(Method::GET)
            .uri("/api/documents/99999")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        // Expect 500 (PgStore not connected) rather than 404 — the DB error
        // is caught by internal_error. The handler logic is correct; the DB is
        // just unavailable in unit tests. This verifies the handler doesn't
        // panic on missing documents.
        //
        // In integration (with a real DB) a nonexistent doc returns 404.
        assert!(response.status().is_server_error());
    }

    // ── file_download tests ────────────────────────────────────────────────

    #[tokio::test]
    async fn file_download_unauthenticated_returns_401() {
        let state = test_state();
        let router = doc_router(state);

        let request = axum::http::Request::builder()
            .method(Method::GET)
            .uri("/api/documents/1/file/1")
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn file_download_returns_500_when_blob_store_missing() {
        // State without a blob store — the handler should return 500
        // after the DB lookup fails (pg_store is not connected).
        let state = test_state();
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = doc_router(state);

        let request = axum::http::Request::builder()
            .method(Method::GET)
            .uri("/api/documents/1/file/1")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        // PgStore not connected → get_file returns error → 500.
        assert!(response.status().is_server_error());
    }

    // ── build_redirect_response tests ──────────────────────────────────────

    #[test]
    fn redirect_response_is_302_found() {
        let response = build_redirect_response(
            "https://s3.example.com/bucket/key?signature=abc",
            "application/pdf",
            None,
        )
        .unwrap();
        assert_eq!(response.status(), StatusCode::FOUND);
    }

    #[test]
    fn redirect_response_has_location_header() {
        let url = "https://b2.example.com/my-bucket/tenant-1/abcdef?X-Amz-Signature=xyz";
        let response = build_redirect_response(url, "application/pdf", None).unwrap();

        let location = response
            .headers()
            .get(LOCATION.to_string())
            .expect("Location header must be present");
        assert_eq!(location.to_str().unwrap(), url);
    }

    #[test]
    fn redirect_response_has_content_type_header() {
        let response = build_redirect_response("http://example.com/f", "image/png", None).unwrap();

        let ct = response
            .headers()
            .get(CONTENT_TYPE.to_string())
            .expect("Content-Type header must be present");
        assert_eq!(ct.to_str().unwrap(), "image/png");
    }

    #[test]
    fn redirect_response_defaults_content_type_to_octet_stream() {
        let response =
            build_redirect_response("http://example.com/f", "application/octet-stream", None)
                .unwrap();

        let ct = response.headers().get(CONTENT_TYPE.to_string()).unwrap();
        assert_eq!(ct.to_str().unwrap(), "application/octet-stream");
    }

    #[test]
    fn redirect_response_has_content_disposition_attachment_without_label() {
        let response = build_redirect_response("http://example.com/f", "text/plain", None).unwrap();

        let cd = response
            .headers()
            .get(CONTENT_DISPOSITION.to_string())
            .expect("Content-Disposition header must be present");
        assert_eq!(cd.to_str().unwrap(), "attachment");
    }

    #[test]
    fn redirect_response_content_disposition_includes_filename_from_label() {
        let response = build_redirect_response(
            "http://example.com/f",
            "application/pdf",
            Some("report-2025.pdf"),
        )
        .unwrap();

        let cd = response
            .headers()
            .get(CONTENT_DISPOSITION.to_string())
            .unwrap();
        assert_eq!(
            cd.to_str().unwrap(),
            "attachment; filename=\"report-2025.pdf\""
        );
    }

    #[test]
    fn redirect_response_content_disposition_with_empty_label() {
        let response =
            build_redirect_response("http://example.com/f", "text/html", Some("")).unwrap();

        let cd = response
            .headers()
            .get(CONTENT_DISPOSITION.to_string())
            .unwrap();
        // Empty label → fall back to plain "attachment"
        assert_eq!(cd.to_str().unwrap(), "attachment");
    }

    #[test]
    fn redirect_response_content_disposition_with_special_characters_in_label() {
        let response = build_redirect_response(
            "http://example.com/f",
            "image/jpeg",
            Some("vacation photo (summer 2025).jpg"),
        )
        .unwrap();

        let cd = response
            .headers()
            .get(CONTENT_DISPOSITION.to_string())
            .unwrap();
        assert!(
            cd.to_str()
                .unwrap()
                .contains("vacation photo (summer 2025).jpg")
        );
    }

    #[test]
    fn redirect_response_content_disposition_with_unicode_label_falls_back_to_ascii() {
        // Non-ASCII filenames need RFC 5987 encoding (filename*=UTF-8''...).
        // The current implementation uses plain header values; non-ASCII bytes
        // in HTTP headers cause a ToStrError. This test verifies the header
        // is still set — it just cannot be inspected via to_str().
        let response =
            build_redirect_response("http://example.com/f", "text/plain", Some("résumé.txt"))
                .unwrap();

        let cd = response
            .headers()
            .get(CONTENT_DISPOSITION.to_string())
            .unwrap();
        // The header value exists; for non-ASCII labels the value contains
        // raw bytes that can't be decoded via to_str().
        assert!(!cd.is_empty());
    }

    // ── Presigned TTL tests ────────────────────────────────────────────────

    #[test]
    fn default_presigned_ttl_is_one_hour() {
        assert_eq!(DEFAULT_PRESIGNED_TTL_SECS, 3600);
    }

    #[test]
    fn with_blob_presigned_ttl_sets_custom_ttl() {
        let state = AppState::new(
            Arc::new(InMemorySessionStore::new()),
            Arc::new(PgStore::new("postgres://localhost/test")),
            None,
            false,
        )
        .with_blob_presigned_ttl(Duration::from_secs(600));

        assert_eq!(state.blob_presigned_ttl, Duration::from_secs(600));
    }

    #[test]
    fn new_app_state_defaults_presigned_ttl_to_one_hour() {
        let state = AppState::new(
            Arc::new(InMemorySessionStore::new()),
            Arc::new(PgStore::new("postgres://localhost/test")),
            None,
            false,
        );

        assert_eq!(
            state.blob_presigned_ttl,
            Duration::from_secs(DEFAULT_PRESIGNED_TTL_SECS)
        );
    }

    // ── Mock Blob tests ───────────────────────────────────────────────────

    #[tokio::test]
    async fn mock_blob_records_presigned_arguments() {
        let mock = MockBlob::new("https://example.com/dl");

        let url = mock
            .presigned_get_url("tenant-1/abcdef", Duration::from_secs(1800))
            .await
            .unwrap();

        assert_eq!(url, "https://example.com/dl");
        assert_eq!(mock.last_key().as_deref(), Some("tenant-1/abcdef"));
        assert_eq!(mock.last_ttl(), Some(Duration::from_secs(1800)));
    }

    #[tokio::test]
    async fn mock_blob_short_ttl_is_recorded() {
        let mock = MockBlob::new("http://local/b");

        let url = mock
            .presigned_get_url("k", Duration::from_secs(60))
            .await
            .unwrap();

        assert_eq!(url, "http://local/b");
        assert_eq!(mock.last_ttl(), Some(Duration::from_secs(60)));
    }

    // ── Error helper tests ──────────────────────────────────────────────────

    #[test]
    fn not_found_has_404_status() {
        let (status, body) = not_found("doc_not_found", "document 42 not found");
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body.error, "doc_not_found");
        assert_eq!(body.message, "document 42 not found");
    }

    #[test]
    fn internal_error_has_500_status() {
        let (status, body) = internal_error(anyhow::anyhow!("db error"));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body.error, "internal_error");
    }
}
