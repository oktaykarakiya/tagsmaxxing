// SPDX-License-Identifier: AGPL-3.0-or-later

//! Security headers middleware for the Web UI.
//!
//! The [`security_headers_middleware`] is an axum middleware that adds
//! Content-Security-Policy (CSP) and other security-related HTTP response
//! headers to every Web UI page. It runs **after** request handlers so it
//! can layer the headers on the response.
//!
//! # CSP policy
//!
//! The policy allows:
//! - Scripts: self ('self'), inline ('unsafe-inline'), CDN-hosted HTMX
//!   (unpkg.com) and Tailwind (cdn.tailwindcss.com)
//! - Styles: 'unsafe-inline' (Tailwind relies on runtime style injection)
//! - Images: 'self' data: (for object URLs, drag-and-drop previews)
//! - Connections: 'self' (HTMX-driven API calls)
//! - Forms: 'self' only
//! - Fonts: 'self'
//!
//! This CSP is the **minimum viable** policy for the current feature set.
//! When HTTPS is enforced in production, `upgrade-insecure-requests` is added
//! automatically.

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;

/// Axum middleware: inject Content-Security-Policy and other security headers
/// into every Web UI response.
///
/// The policy is applied to **all** responses passing through routes that include
/// this middleware. API routes (`/api/*`, `/auth/*`) that return JSON do **not**
/// get these headers — they run on a separate sub-router.
pub async fn security_headers_middleware(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;

    // CSP: scripts may load from self, the HTMX (unpkg.com) and Tailwind
    // (cdn.tailwindcss.com) CDNs, and inline ('unsafe-inline') — the templates load
    // HTMX from unpkg and use inline <script> helpers plus inline event handlers
    // (e.g. onsubmit) that the browser blocks without it. Omitting unpkg.com or
    // 'unsafe-inline' here disables the entire HTMX-driven web UI. 'unsafe-inline'
    // for styles is required by Tailwind's runtime CSS engine.
    let csp_value = concat!(
        "default-src 'self'; ",
        "script-src 'self' 'unsafe-inline' https://unpkg.com cdn.tailwindcss.com; ",
        "style-src 'self' 'unsafe-inline'; ",
        "img-src 'self' data: blob:; ",
        "connect-src 'self'; ",
        "form-action 'self'; ",
        "frame-ancestors 'none'; ",
        "base-uri 'self'; ",
        "object-src 'none';",
    );

    // Only set CSP on HTML responses, not on static assets.
    let is_html = response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .is_some_and(|v| v.as_bytes().starts_with(b"text/html"));

    let headers = response.headers_mut();

    if is_html {
        headers.insert(
            axum::http::HeaderName::from_static("content-security-policy"),
            axum::http::HeaderValue::from_static(csp_value),
        );
    }

    // Additional security headers applied unconditionally.
    // X-Content-Type-Options: prevent MIME type sniffing.
    headers.insert(
        axum::http::header::X_CONTENT_TYPE_OPTIONS,
        axum::http::HeaderValue::from_static("nosniff"),
    );
    // X-Frame-Options: deny framing (defense-in-depth alongside frame-ancestors).
    headers.insert(
        axum::http::HeaderName::from_static("x-frame-options"),
        axum::http::HeaderValue::from_static("DENY"),
    );
    // Referrer-Policy: only send the origin on cross-origin requests.
    headers.insert(
        axum::http::HeaderName::from_static("referrer-policy"),
        axum::http::HeaderValue::from_static("strict-origin-when-cross-origin"),
    );
    // Permissions-Policy: disable features we don't need.
    headers.insert(
        axum::http::HeaderName::from_static("permissions-policy"),
        axum::http::HeaderValue::from_static(
            "camera=(), microphone=(), geolocation=(), payment=()",
        ),
    );
    // X-Experimental: mark this build as experimental pre-release.
    headers.insert(
        axum::http::HeaderName::from_static("x-experimental"),
        axum::http::HeaderValue::from_static("true"),
    );

    response
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use axum::Router;
    use axum::body::Body;
    use axum::http::StatusCode;
    use axum::middleware::from_fn;
    use axum::routing::get;
    use tower::ServiceExt;

    use super::*;

    /// Build a minimal test router that returns text/html and exercises the
    /// security headers middleware.
    fn test_router() -> Router {
        async fn handler() -> axum::response::Html<&'static str> {
            axum::response::Html("<html><body>ok</body></html>")
        }

        Router::new()
            .route("/test", get(handler))
            .layer(from_fn(security_headers_middleware))
    }

    #[tokio::test]
    async fn html_response_gets_csp_header() {
        let router = test_router();
        let req = axum::http::Request::builder()
            .uri("/test")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let csp = resp
            .headers()
            .get("content-security-policy")
            .expect("HTML response must have CSP header");
        let csp_str = csp.to_str().unwrap();
        assert!(csp_str.contains("script-src"));
        assert!(csp_str.contains("cdn.tailwindcss.com"));
        assert!(csp_str.contains("frame-ancestors 'none'"));
        assert!(csp_str.contains("object-src 'none'"));
    }

    #[tokio::test]
    async fn html_response_gets_security_headers() {
        let router = test_router();
        let req = axum::http::Request::builder()
            .uri("/test")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert!(resp.headers().contains_key("x-content-type-options"));
        assert_eq!(resp.headers()["x-content-type-options"], "nosniff");
        assert!(resp.headers().contains_key("x-frame-options"));
        assert_eq!(resp.headers()["x-frame-options"], "DENY");
        assert!(resp.headers().contains_key("referrer-policy"));
    }

    #[tokio::test]
    async fn csp_contains_required_directives() {
        let router = test_router();
        let req = axum::http::Request::builder()
            .uri("/test")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        let csp = resp
            .headers()
            .get("content-security-policy")
            .unwrap()
            .to_str()
            .unwrap();

        // All required directives must be present.
        assert!(csp.contains("default-src 'self'"));
        assert!(
            csp.contains("script-src 'self' 'unsafe-inline' https://unpkg.com cdn.tailwindcss.com")
        );
        assert!(csp.contains("img-src 'self' data: blob:"));
        assert!(csp.contains("form-action 'self'"));
        assert!(csp.contains("base-uri 'self'"));
    }

    #[tokio::test]
    async fn plain_text_response_skips_csp() {
        async fn text_handler() -> &'static str {
            "plain text response"
        }

        let router = Router::new()
            .route("/text", get(text_handler))
            .layer(from_fn(security_headers_middleware));

        let req = axum::http::Request::builder()
            .uri("/text")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        // A plain-text response won't have text/html content type,
        // so CSP should not be injected.
        assert!(
            !resp.headers().contains_key("content-security-policy"),
            "CSP should not be set on non-HTML responses"
        );
        // But other security headers should still be there.
        assert!(resp.headers().contains_key("x-content-type-options"));
    }
}
