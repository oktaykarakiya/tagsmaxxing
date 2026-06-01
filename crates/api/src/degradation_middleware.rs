//! Degradation middleware — injects the `X-Degraded` response header when
//! any subsystem is unhealthy (plan §22, P8-T9).
//!
//! The middleware reads [`DegradationState`](kb_core::degradation::DegradationState)
//! from the shared [`AppState`](crate::AppState) on every request. When one or more
//! subsystems are degraded, the `X-Degraded` header is added to the response listing
//! the affected subsystems (e.g. `X-Degraded: blob-store, embed`).
//!
//! When everything is healthy, no header is injected.

use std::sync::Arc;

use axum::extract::State;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::Response;

use crate::AppState;

/// Axum middleware that adds the `X-Degraded` header to every response
/// when the system is in a degraded state.
///
/// Reads [`DegradationState`] from [`AppState`] — the state must include
/// a `degradation` field (set during server startup). When left as `None`
/// (e.g. in tests that construct a minimal `AppState`), the middleware is
/// a no-op (no header injected).
///
/// # Header format
///
/// - `X-Degraded: blob-store, embed` — multiple subsystems separated by `, `.
/// - Absent — all subsystems are healthy.
pub async fn degradation_middleware(
    State(state): State<Arc<AppState>>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let mut response = next.run(request).await;

    if let Some(ref ds) = state.degradation
        && let Some(header_val) = ds.degraded_header_value()
        && let Ok(val) = axum::http::HeaderValue::from_str(&header_val)
    {
        response.headers_mut().insert("x-degraded", val);
    }

    response
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::get;
    use kb_core::degradation::DegradationState;
    use kb_core::session::SessionStore;
    use tower::ServiceExt;

    use super::*;
    use crate::AppState;

    /// Build a minimal test state; uses a mock PgStore that will never be
    /// connected (the middleware doesn't touch the DB).
    fn test_state(degradation: Option<Arc<DegradationState>>) -> Arc<AppState> {
        let session_store: Arc<dyn SessionStore> =
            Arc::new(kb_store::session_store::InMemorySessionStore::new());
        let pg_store = Arc::new(kb_store::PgStore::new(
            "postgres://localhost/test?sslmode=disable",
        ));
        Arc::new(AppState {
            session_store,
            pg_store,
            session_ttl: Duration::from_secs(86400),
            secure_cookies: false,
            ingest_pipeline: None,
            retrieval_pipeline: None,
            blob: None,
            job_queue: None,
            backend_pool: None,
            blob_presigned_ttl: Duration::from_secs(3600),
            degradation,
            inflight_limiter: None,
        })
    }

    fn test_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/test", get(|| async { "ok" }))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                degradation_middleware,
            ))
            .with_state(state)
    }

    #[tokio::test]
    async fn no_header_when_healthy() {
        let ds = Arc::new(DegradationState::default());
        let state = test_state(Some(ds));
        let router = test_router(state);
        let req = Request::builder().uri("/test").body(Body::empty()).unwrap();

        let response = router.oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get("x-degraded").is_none());
    }

    #[tokio::test]
    async fn header_present_when_blob_store_degraded() {
        let ds = Arc::new(DegradationState::default());
        ds.blob_store_degraded.store(true, Ordering::Release);
        let state = test_state(Some(ds));
        let router = test_router(state);
        let req = Request::builder().uri("/test").body(Body::empty()).unwrap();

        let response = router.oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let header = response
            .headers()
            .get("x-degraded")
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(header, "blob-store");
    }

    #[tokio::test]
    async fn header_present_when_multiple_degraded() {
        let ds = Arc::new(DegradationState::default());
        ds.blob_store_degraded.store(true, Ordering::Release);
        ds.embed_degraded.store(true, Ordering::Release);
        let state = test_state(Some(ds));
        let router = test_router(state);
        let req = Request::builder().uri("/test").body(Body::empty()).unwrap();

        let response = router.oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let header = response
            .headers()
            .get("x-degraded")
            .unwrap()
            .to_str()
            .unwrap();
        // Order depends on Vec construction order (blob-store first, then embed).
        assert_eq!(header, "blob-store, embed");
    }

    #[tokio::test]
    async fn no_header_when_degradation_is_none() {
        // When AppState has no degradation field (None), the middleware is a no-op.
        let state = test_state(None);
        let router = test_router(state);
        let req = Request::builder().uri("/test").body(Body::empty()).unwrap();

        let response = router.oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get("x-degraded").is_none());
    }

    #[tokio::test]
    async fn response_body_passes_through() {
        let ds = Arc::new(DegradationState::default());
        ds.text_degraded.store(true, Ordering::Release);
        let state = test_state(Some(ds));
        let router = test_router(state);
        let req = Request::builder().uri("/test").body(Body::empty()).unwrap();

        let response = router.oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert_eq!(std::str::from_utf8(&body).unwrap(), "ok");
    }
}
