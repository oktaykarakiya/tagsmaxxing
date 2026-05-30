//! Tika-sidecar extractor for rich documents (PDF, DOCX, PPTX, XLSX, ODT, RTF,
//! and any other format Apache Tika supports).
//!
//! Sends raw bytes via HTTP PUT to the Tika `/tika` endpoint, receives extracted
//! text and metadata, and maps them into [`Extracted`]. The Tika base URL is
//! hot-swappable via [`arc_swap::ArcSwap`] (plan §6, CLAUDE.md hot-swappable rule).

use std::sync::Arc;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use kb_core::extractor::{Extracted, Extractor, RawFile};
use reqwest::Client;

/// Hot-swappable configuration handle for the Tika server base URL.
///
/// Wraps the base URL in an [`ArcSwap`] so the running pipeline can switch to a
/// different Tika instance without a restart. Callers read the current URL per
/// request via the internal `load()`.
///
/// # Example
///
/// ```rust
/// use kb_extract::tika::TikaConfig;
///
/// let cfg = TikaConfig::new("http://localhost:9998".into());
/// assert_eq!(cfg.current_url(), "http://localhost:9998");
/// cfg.set_url("http://tika2:9998".into());
/// assert_eq!(cfg.current_url(), "http://tika2:9998");
/// ```
#[derive(Debug, Clone)]
pub struct TikaConfig {
    base_url: Arc<ArcSwap<String>>,
}

impl TikaConfig {
    /// Create a new config pointing at `base_url`.
    ///
    /// The URL should not include a trailing slash; the extractor appends `/tika`
    /// to form the full endpoint.
    pub fn new(base_url: String) -> Self {
        Self {
            base_url: Arc::new(ArcSwap::from_pointee(base_url)),
        }
    }

    /// Hot-swap the Tika base URL at runtime.
    ///
    /// Subsequent [`TikaExtractor::extract`] calls will use the new URL without
    /// any restart or reconfiguration.
    pub fn set_url(&self, url: String) {
        self.base_url.store(Arc::new(url));
    }

    /// Return a clone of the current base URL.
    pub fn current_url(&self) -> String {
        self.base_url.load().as_ref().clone()
    }
}

/// Extracts text and metadata from rich documents by delegating to an Apache Tika
/// server.
///
/// Sends the file bytes to `{base_url}/tika` via HTTP PUT with
/// `Accept: application/json`. Tika returns a JSON object whose
/// `"X-TIKA:content"` field becomes [`Extracted::text`] and whose remaining fields
/// become [`Extracted::meta`].
///
/// # Document formats
///
/// Tika handles PDF, DOCX, PPTX, XLSX, ODT, RTF, and many more — this extractor
/// is the universal fallback for all office/document formats that Rust native
/// crates don't cover directly (plan §11).
///
/// # Examples
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use arc_swap::ArcSwap;
/// use bytes::Bytes;
/// use kb_core::extractor::{Extracted, Extractor, RawFile};
/// use kb_core::kind::DocKind;
/// use kb_extract::tika::{TikaConfig, TikaExtractor};
///
/// # async fn example() -> anyhow::Result<()> {
/// let config = TikaConfig::new("http://localhost:9998".into());
/// let client = reqwest::Client::new();
/// let ex = TikaExtractor::new(client, config);
/// let raw = RawFile {
///     bytes: Bytes::from_static(b"%PDF-1.4 fake pdf"),
///     mime: Some("application/pdf".into()),
///     kind: DocKind::Document,
///     path: Some("report.pdf".into()),
/// };
/// let out = ex.extract(&raw).await?;
/// println!("text: {}", out.text);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct TikaExtractor {
    /// Shared HTTP client (connection pooling, timeouts configured externally).
    http: Client,
    /// Hot-swappable Tika base URL.
    config: TikaConfig,
}

impl TikaExtractor {
    /// Create a new extractor with a given HTTP client and Tika configuration.
    ///
    /// The `http` client should be configured with appropriate timeouts for the
    /// expected document sizes. The `config`'s base URL is read on every
    /// [`extract`](Extractor::extract) call, so the operator can hot-swap it at
    /// runtime via [`TikaConfig::set_url`].
    pub fn new(http: Client, config: TikaConfig) -> Self {
        Self { http, config }
    }
}

#[async_trait]
impl Extractor for TikaExtractor {
    async fn extract(&self, file: &RawFile) -> anyhow::Result<Extracted> {
        let base = self.config.base_url.load();
        let url = format!("{}/tika", base.as_str());
        // Drop the guard so we don't hold it across the await point.
        drop(base);

        let resp = self
            .http
            .put(&url)
            .header("Accept", "application/json")
            .body(file.bytes.clone())
            .send()
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "TikaExtractor: HTTP request failed for '{}': {e}",
                    file.path.as_deref().unwrap_or("<unknown>")
                )
            })?;

        let status = resp.status();
        if !status.is_success() {
            let body_text = resp.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "TikaExtractor: server returned HTTP {status} for '{}': {}",
                file.path.as_deref().unwrap_or("<unknown>"),
                body_text
            ));
        }

        let body_bytes = resp
            .bytes()
            .await
            .map_err(|e| anyhow::anyhow!("TikaExtractor: failed to read response body: {e}"))?;

        // Try to parse as JSON (the normal path — we requested application/json).
        // Fall back to treating the body as plain text (older Tika versions or
        // misconfigured Accept headers).
        if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&body_bytes) {
            let text = json
                .get("X-TIKA:content")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            // Remove the content key so metadata doesn't duplicate the text.
            let mut meta = json;
            if let Some(obj) = meta.as_object_mut() {
                obj.remove("X-TIKA:content");
            }

            Ok(Extracted {
                text,
                meta,
                page_images: Vec::new(),
            })
        } else {
            // Not JSON — treat the body as plain UTF-8 text.
            let text = String::from_utf8(body_bytes.to_vec()).map_err(|e| {
                anyhow::anyhow!(
                    "TikaExtractor: response for '{}' is neither valid JSON nor UTF-8 text: {e}",
                    file.path.as_deref().unwrap_or("<unknown>")
                )
            })?;

            Ok(Extracted {
                text,
                meta: serde_json::Value::Object(Default::default()),
                page_images: Vec::new(),
            })
        }
    }
}

// ── unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::net::SocketAddr;
    use std::sync::Arc;

    use axum::Router;
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::response::Response;
    use axum::routing::put;
    use bytes::Bytes;
    use kb_core::extractor::{Extractor, RawFile};
    use kb_core::kind::DocKind;
    use reqwest::Client;
    use serde_json::{Value, json};
    use tokio::net::TcpListener;
    use tokio::sync::Mutex;

    use super::*;

    // ── mock Tika server ─────────────────────────────────────────────────

    /// A lightweight mock Tika server for deterministic tests.
    ///
    /// Binds to `127.0.0.1:0` and serves `PUT /tika`. The response is fully
    /// controlled by a shared [`MockScenario`] so tests can inject well-formed
    /// JSON, plain text, HTTP errors, and binary garbage without a real Tika
    /// instance.
    struct MockTika {
        addr: SocketAddr,
        scenario: Arc<Mutex<MockScenario>>,
        shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    }

    /// What the mock Tika server should return on the next request.
    #[derive(Debug, Clone)]
    struct MockScenario {
        /// HTTP status code.
        status: StatusCode,
        /// Value of the `Content-Type` response header.
        content_type: String,
        /// Raw response body bytes.
        body: Vec<u8>,
    }

    impl Default for MockScenario {
        fn default() -> Self {
            Self {
                status: StatusCode::OK,
                content_type: "application/json".into(),
                body: Vec::new(),
            }
        }
    }

    /// Convenience: build a [`MockScenario`] that returns valid Tika JSON.
    fn tika_json_response(text: &str, extra_meta: Value) -> MockScenario {
        let mut body = json!({"X-TIKA:content": text});
        if let Some(obj) = body.as_object_mut()
            && let Some(meta_obj) = extra_meta.as_object()
        {
            for (k, v) in meta_obj {
                obj.insert(k.clone(), v.clone());
            }
        }
        MockScenario {
            status: StatusCode::OK,
            content_type: "application/json".into(),
            body: serde_json::to_vec(&body).unwrap(),
        }
    }

    /// Convenience: build a [`MockScenario`] that returns plain text.
    fn tika_text_response(text: &str) -> MockScenario {
        MockScenario {
            status: StatusCode::OK,
            content_type: "text/plain".into(),
            body: text.as_bytes().to_vec(),
        }
    }

    impl MockTika {
        /// Start the mock server on an ephemeral port.
        async fn start() -> Self {
            let scenario = Arc::new(Mutex::new(MockScenario::default()));
            let state = Arc::clone(&scenario);

            let app = Router::new()
                .route("/tika", put(handle_tika))
                .with_state(state);

            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

            tokio::spawn(async move {
                if let Err(e) = axum::serve(listener, app)
                    .with_graceful_shutdown(async {
                        shutdown_rx.await.ok();
                    })
                    .await
                {
                    eprintln!("mock tika server stopped with error: {e}");
                }
            });

            MockTika {
                addr,
                scenario,
                shutdown_tx: Some(shutdown_tx),
            }
        }

        /// Return the base URL (without `/tika` suffix) for use with [`TikaConfig`].
        fn base_url(&self) -> String {
            format!("http://{}", self.addr)
        }

        /// Lock the scenario for mutation between calls.
        async fn set_scenario(&self, s: MockScenario) {
            *self.scenario.lock().await = s;
        }

        /// Gracefully shut down.
        async fn shutdown(mut self) {
            if let Some(tx) = self.shutdown_tx.take() {
                let _ = tx.send(());
                tokio::task::yield_now().await;
            }
        }
    }

    /// Axum handler for `PUT /tika` — reads the shared scenario and responds.
    async fn handle_tika(State(scenario): State<Arc<Mutex<MockScenario>>>) -> Response {
        let s = scenario.lock().await.clone();
        let mut resp = Response::new(axum::body::Body::from(s.body));
        *resp.status_mut() = s.status;
        resp.headers_mut().insert(
            axum::http::header::CONTENT_TYPE,
            s.content_type.parse().unwrap(),
        );
        resp
    }

    // ── helpers ──────────────────────────────────────────────────────────

    /// Build a [`TikaExtractor`] backed by a running mock Tika server.
    async fn extractor_with_mock(mock: &MockTika) -> TikaExtractor {
        let config = TikaConfig::new(mock.base_url());
        TikaExtractor::new(Client::new(), config)
    }

    /// Build a sample [`RawFile`] for a PDF-like document.
    fn pdf_raw() -> RawFile {
        RawFile {
            bytes: Bytes::from_static(b"%PDF-1.4\nfake pdf content"),
            mime: Some("application/pdf".into()),
            kind: DocKind::Document,
            path: Some("report.pdf".into()),
        }
    }

    // ── TikaConfig tests ─────────────────────────────────────────────────

    #[test]
    fn config_stores_and_returns_url() {
        let cfg = TikaConfig::new("http://tika:9998".into());
        assert_eq!(cfg.current_url(), "http://tika:9998");
    }

    #[test]
    fn config_hot_swaps_url() {
        let cfg = TikaConfig::new("http://tika1:9998".into());
        assert_eq!(cfg.current_url(), "http://tika1:9998");
        cfg.set_url("http://tika2:9998".into());
        assert_eq!(cfg.current_url(), "http://tika2:9998");
    }

    #[test]
    fn config_clone_shares_state() {
        let cfg1 = TikaConfig::new("http://a:9998".into());
        let cfg2 = cfg1.clone();
        cfg2.set_url("http://b:9998".into());
        // Both see the new URL — they share the same ArcSwap.
        assert_eq!(cfg1.current_url(), "http://b:9998");
        assert_eq!(cfg2.current_url(), "http://b:9998");
    }

    #[test]
    fn config_handles_empty_url() {
        let cfg = TikaConfig::new(String::new());
        assert_eq!(cfg.current_url(), "");
    }

    // ── extraction: happy path ───────────────────────────────────────────

    /// Full JSON response with text content and extra metadata.
    #[tokio::test]
    async fn extracts_text_and_metadata_from_json() {
        let mock = MockTika::start().await;
        mock.set_scenario(tika_json_response(
            "Hello from PDF",
            json!({"dc:title": "My Doc", "Content-Type": "application/pdf"}),
        ))
        .await;

        let ex = extractor_with_mock(&mock).await;
        let out = ex.extract(&pdf_raw()).await.unwrap();

        assert_eq!(out.text, "Hello from PDF");
        assert_eq!(out.meta["dc:title"], "My Doc");
        assert_eq!(out.meta["Content-Type"], "application/pdf");
        // X-TIKA:content must be removed from meta.
        assert!(out.meta.get("X-TIKA:content").is_none());
        assert!(out.page_images.is_empty());

        mock.shutdown().await;
    }

    /// JSON response with no metadata beyond the content.
    #[tokio::test]
    async fn extracts_text_only_from_json() {
        let mock = MockTika::start().await;
        mock.set_scenario(tika_json_response("plain text content", json!({})))
            .await;

        let ex = extractor_with_mock(&mock).await;
        let out = ex.extract(&pdf_raw()).await.unwrap();

        assert_eq!(out.text, "plain text content");
        // Should be an empty object (X-TIKA:content was removed).
        assert_eq!(out.meta, json!({}));

        mock.shutdown().await;
    }

    /// JSON response with empty content string.
    #[tokio::test]
    async fn handles_empty_content_in_json() {
        let mock = MockTika::start().await;
        mock.set_scenario(tika_json_response(
            "",
            json!({"Content-Type": "application/pdf"}),
        ))
        .await;

        let ex = extractor_with_mock(&mock).await;
        let out = ex.extract(&pdf_raw()).await.unwrap();

        assert_eq!(out.text, "");
        assert_eq!(out.meta["Content-Type"], "application/pdf");

        mock.shutdown().await;
    }

    /// JSON response where X-TIKA:content field is completely absent.
    #[tokio::test]
    async fn handles_missing_content_field() {
        let mock = MockTika::start().await;
        let body = json!({"dc:title": "No Content Here"});
        mock.set_scenario(MockScenario {
            status: StatusCode::OK,
            content_type: "application/json".into(),
            body: serde_json::to_vec(&body).unwrap(),
        })
        .await;

        let ex = extractor_with_mock(&mock).await;
        let out = ex.extract(&pdf_raw()).await.unwrap();

        assert_eq!(out.text, "");
        assert_eq!(out.meta["dc:title"], "No Content Here");

        mock.shutdown().await;
    }

    // ── extraction: plain-text fallback ──────────────────────────────────

    /// When Tika returns text/plain (e.g. older version), the body becomes text
    /// and metadata is empty.
    #[tokio::test]
    async fn falls_back_to_plain_text() {
        let mock = MockTika::start().await;
        mock.set_scenario(tika_text_response("Extracted as plain text\n"))
            .await;

        let ex = extractor_with_mock(&mock).await;
        let out = ex.extract(&pdf_raw()).await.unwrap();

        assert_eq!(out.text, "Extracted as plain text\n");
        assert_eq!(out.meta, json!({}));
        assert!(out.page_images.is_empty());

        mock.shutdown().await;
    }

    /// Empty plain-text response.
    #[tokio::test]
    async fn handles_empty_plain_text() {
        let mock = MockTika::start().await;
        mock.set_scenario(tika_text_response("")).await;

        let ex = extractor_with_mock(&mock).await;
        let out = ex.extract(&pdf_raw()).await.unwrap();

        assert_eq!(out.text, "");
        assert_eq!(out.meta, json!({}));

        mock.shutdown().await;
    }

    // ── extraction: errors ───────────────────────────────────────────────

    /// Server returns HTTP 500.
    #[tokio::test]
    async fn error_on_500_response() {
        let mock = MockTika::start().await;
        mock.set_scenario(MockScenario {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            content_type: "text/plain".into(),
            body: b"Tika crashed".to_vec(),
        })
        .await;

        let ex = extractor_with_mock(&mock).await;
        let err = ex.extract(&pdf_raw()).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("500"),
            "error should mention HTTP 500, got: {msg}"
        );
        assert!(
            msg.contains("report.pdf"),
            "error should mention filename, got: {msg}"
        );
        assert!(
            msg.contains("Tika crashed"),
            "error should include body, got: {msg}"
        );

        mock.shutdown().await;
    }

    /// Server returns HTTP 422 (Tika's "unsupported format" code).
    #[tokio::test]
    async fn error_on_422_unsupported_format() {
        let mock = MockTika::start().await;
        mock.set_scenario(MockScenario {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            content_type: "text/plain".into(),
            body: b"Unsupported media type".to_vec(),
        })
        .await;

        let ex = extractor_with_mock(&mock).await;
        let err = ex.extract(&pdf_raw()).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("422"),
            "error should mention HTTP 422, got: {msg}"
        );
        assert!(
            msg.contains("Unsupported"),
            "error should include body, got: {msg}"
        );

        mock.shutdown().await;
    }

    /// Server is unreachable (mock shut down before call).
    #[tokio::test]
    async fn error_on_connection_refused() {
        let mock = MockTika::start().await;
        let addr = mock.addr;
        // Shut down the mock so the port is no longer accepting connections.
        mock.shutdown().await;

        // Build an extractor pointing at the now-dead port.
        let config = TikaConfig::new(format!("http://{addr}"));
        let ex = TikaExtractor::new(Client::new(), config);

        let err = ex.extract(&pdf_raw()).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("HTTP request failed"),
            "error should mention HTTP request failure, got: {msg}"
        );
        assert!(
            msg.contains("report.pdf"),
            "error should mention filename, got: {msg}"
        );
    }

    /// Response body is neither valid JSON nor valid UTF-8.
    #[tokio::test]
    async fn error_on_non_utf8_binary_response() {
        let mock = MockTika::start().await;
        // Binary garbage that isn't valid UTF-8.
        let binary_body: Vec<u8> = vec![0x80, 0xFF, 0x00, 0x01];
        mock.set_scenario(MockScenario {
            status: StatusCode::OK,
            content_type: "application/octet-stream".into(),
            body: binary_body,
        })
        .await;

        let ex = extractor_with_mock(&mock).await;
        let err = ex.extract(&pdf_raw()).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("neither valid JSON nor UTF-8"), "got: {msg}");
        assert!(
            msg.contains("report.pdf"),
            "error should mention filename, got: {msg}"
        );

        mock.shutdown().await;
    }

    // ── extraction: file-level metadata ──────────────────────────────────

    /// Tika's JSON response includes detected Content-Type and other metadata.
    #[tokio::test]
    async fn preserves_tika_content_type_in_meta() {
        let mock = MockTika::start().await;
        mock.set_scenario(
            tika_json_response("slide content", json!({"Content-Type": "application/vnd.openxmlformats-officedocument.presentationml.presentation"})),
        ).await;

        let raw = RawFile {
            bytes: Bytes::from_static(b"fake pptx bytes"),
            mime: Some(
                "application/vnd.openxmlformats-officedocument.presentationml.presentation".into(),
            ),
            kind: DocKind::Document,
            path: Some("slides.pptx".into()),
        };

        let ex = extractor_with_mock(&mock).await;
        let out = ex.extract(&raw).await.unwrap();

        assert_eq!(out.text, "slide content");
        assert!(
            out.meta["Content-Type"]
                .as_str()
                .is_some_and(|ct| ct.contains("presentationml")),
            "Content-Type should be preserved in meta"
        );

        mock.shutdown().await;
    }

    /// Large metadata payloads (many Tika fields).
    #[tokio::test]
    async fn handles_rich_metadata() {
        let mock = MockTika::start().await;
        mock.set_scenario(tika_json_response(
            "content",
            json!({
                "dc:title": "Q4 Report",
                "dc:creator": "Alice",
                "dc:subject": "Finance",
                "dcterms:created": "2026-01-15",
                "meta:page-count": "42",
                "Content-Type": "application/pdf",
                "pdf:PDFVersion": "1.7",
                "xmp:CreatorTool": "LibreOffice",
            }),
        ))
        .await;

        let ex = extractor_with_mock(&mock).await;
        let out = ex.extract(&pdf_raw()).await.unwrap();

        assert_eq!(out.text, "content");
        assert_eq!(out.meta["dc:title"], "Q4 Report");
        assert_eq!(out.meta["dc:creator"], "Alice");
        assert_eq!(out.meta["meta:page-count"], "42");
        assert_eq!(out.meta["pdf:PDFVersion"], "1.7");

        mock.shutdown().await;
    }

    /// Error message includes the filename when path is set.
    #[tokio::test]
    async fn error_includes_filename_on_failure() {
        let mock = MockTika::start().await;
        mock.set_scenario(MockScenario {
            status: StatusCode::BAD_GATEWAY,
            content_type: "text/plain".into(),
            body: b"upstream timeout".to_vec(),
        })
        .await;

        let raw = RawFile {
            bytes: Bytes::from_static(b"fake"),
            mime: Some("application/pdf".into()),
            kind: DocKind::Document,
            path: Some("important_report.pdf".into()),
        };

        let ex = extractor_with_mock(&mock).await;
        let err = ex.extract(&raw).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("important_report.pdf"), "got: {msg}");

        mock.shutdown().await;
    }

    /// Error message uses <unknown> when path is missing.
    #[tokio::test]
    async fn error_handles_missing_path() {
        let mock = MockTika::start().await;
        mock.set_scenario(MockScenario {
            status: StatusCode::SERVICE_UNAVAILABLE,
            content_type: "text/plain".into(),
            body: b"down".to_vec(),
        })
        .await;

        let raw = RawFile {
            bytes: Bytes::from_static(b"fake"),
            mime: None,
            kind: DocKind::Document,
            path: None,
        };

        let ex = extractor_with_mock(&mock).await;
        let err = ex.extract(&raw).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("<unknown>"), "got: {msg}");

        mock.shutdown().await;
    }

    // ── extraction: hot-swap URL ─────────────────────────────────────────

    /// After switching the base URL, the next extract call hits the new server.
    #[tokio::test]
    async fn hot_swap_url_takes_effect_on_next_call() {
        let mock1 = MockTika::start().await;
        let mock2 = MockTika::start().await;

        // mock1 returns "from server 1"
        mock1
            .set_scenario(tika_json_response("from server 1", json!({})))
            .await;
        // mock2 returns "from server 2"
        mock2
            .set_scenario(tika_json_response("from server 2", json!({})))
            .await;

        let config = TikaConfig::new(mock1.base_url());
        let ex = TikaExtractor::new(Client::new(), config.clone());

        // First call hits mock1.
        let out = ex.extract(&pdf_raw()).await.unwrap();
        assert_eq!(out.text, "from server 1");

        // Hot-swap to mock2.
        config.set_url(mock2.base_url());

        // Second call hits mock2.
        let out = ex.extract(&pdf_raw()).await.unwrap();
        assert_eq!(out.text, "from server 2");

        mock1.shutdown().await;
        mock2.shutdown().await;
    }

    // ── extraction: non-json but valid utf8 entity ───────────────────────

    /// Tika sometimes returns an XML entity or HTML error even on 200.
    /// The fallback path should treat that as plain text.
    #[tokio::test]
    async fn non_json_utf8_body_becomes_text() {
        let mock = MockTika::start().await;
        mock.set_scenario(MockScenario {
            status: StatusCode::OK,
            content_type: "text/html".into(),
            body: b"<html><body>Error: something went wrong</body></html>".to_vec(),
        })
        .await;

        let ex = extractor_with_mock(&mock).await;
        let out = ex.extract(&pdf_raw()).await.unwrap();

        assert_eq!(
            out.text,
            "<html><body>Error: something went wrong</body></html>"
        );
        assert_eq!(out.meta, json!({}));

        mock.shutdown().await;
    }

    // ── Docx extraction through Tika (integration-like) ──────────────────

    /// Simulate a DOCX file being processed by Tika.
    #[tokio::test]
    async fn simulates_docx_extraction() {
        let mock = MockTika::start().await;
        mock.set_scenario(
            tika_json_response("Annual Report 2026\n\nRevenue grew 15% year-over-year.", json!({
                "dc:title": "Annual Report",
                "Content-Type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "meta:word-count": "1500",
            })),
        ).await;

        let raw = RawFile {
            bytes: Bytes::from_static(b"PK\x03\x04 fake docx zip"),
            mime: Some(
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document".into(),
            ),
            kind: DocKind::Document,
            path: Some("report.docx".into()),
        };

        let ex = extractor_with_mock(&mock).await;
        let out = ex.extract(&raw).await.unwrap();

        assert!(out.text.contains("Revenue grew"));
        assert_eq!(out.meta["dc:title"], "Annual Report");
        assert_eq!(out.meta["meta:word-count"], "1500");

        mock.shutdown().await;
    }
}
