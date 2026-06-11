// SPDX-License-Identifier: AGPL-3.0-or-later

//! Legal page handlers (P12-T6): Terms of Service, Privacy Policy, DPA,
//! Acceptable Use Policy, Cookie Policy, and Data Retention & Erasure.
//!
//! All routes in this module are **public** (no auth middleware). The DPA
//! page dynamically lists remote AI model providers from the routing table
//! as sub-processors; the other five pages are static Askama templates.

use std::sync::Arc;

use axum::extract::State;
use axum::response::Response;

use crate::AppState;

use super::handlers::render_ok;
use super::templates::{
    AupPage, CookiesPage, DpaPage, PrivacyPage, RetentionPage, SubProcessorEntry, TermsPage,
};

// ── GET /terms ──────────────────────────────────────────────────────────────────

/// `GET /terms` — render the Terms of Service page.
///
/// Static content with no database dependency. Accessible without
/// authentication.
pub(crate) async fn terms_page() -> Response {
    render_ok(&TermsPage)
}

// ── GET /privacy ────────────────────────────────────────────────────────────────

/// `GET /privacy` — render the Privacy Policy page.
///
/// Static content describing data collection, usage, sub-processors, and
/// user rights. Accessible without authentication.
pub(crate) async fn privacy_page() -> Response {
    render_ok(&PrivacyPage)
}

// ── GET /dpa ────────────────────────────────────────────────────────────────────

/// `GET /dpa` — render the Data Processing Agreement page.
///
/// Dynamically lists remote AI model providers from the routing table as
/// sub-processors alongside the static sub-processors (B2, Stripe, email
/// provider). When the database is not connected or the provider query
/// fails, the sub-processor table still renders with the static entries
/// and a note that the dynamic list is unavailable.
pub(crate) async fn dpa_page(State(state): State<Arc<AppState>>) -> Response {
    let sub_processors = build_sub_processor_list(&state).await;

    let page = DpaPage { sub_processors };
    render_ok(&page)
}

// ── GET /aup ────────────────────────────────────────────────────────────────────

/// `GET /aup` — render the Acceptable Use Policy page.
///
/// Static content describing prohibited content, prohibited activities,
/// AI model usage rules, and enforcement procedures. Accessible without
/// authentication.
pub(crate) async fn aup_page() -> Response {
    render_ok(&AupPage)
}

// ── GET /cookies ────────────────────────────────────────────────────────────────

/// `GET /cookies` — render the Cookie Policy page.
///
/// Static content listing the strictly necessary cookies used by the
/// Service. Accessible without authentication.
pub(crate) async fn cookies_page() -> Response {
    render_ok(&CookiesPage)
}

// ── GET /retention ──────────────────────────────────────────────────────────────

/// `GET /retention` — render the Data Retention & Erasure statement.
///
/// Explains the full data lifecycle including crypto-shredding.
/// Accessible without authentication.
pub(crate) async fn retention_page() -> Response {
    render_ok(&RetentionPage)
}

// ── Helpers ─────────────────────────────────────────────────────────────────────

/// Build the dynamic sub-processor list for the DPA page from the current
/// routing table.
///
/// Queries [`PgStore::list_providers`] and includes only **enabled, remote**
/// providers (non-local kinds) — local providers do not transmit data
/// off-instance and are not sub-processors. Each entry is mapped to a
/// [`SubProcessorEntry`] for template rendering.
///
/// When the routing table query fails (e.g. database not connected), an
/// empty list is returned and a warning is logged. The DPA template
/// still renders with the static sub-processor entries.
async fn build_sub_processor_list(state: &AppState) -> Vec<SubProcessorEntry> {
    let providers = match state.pg_store.list_providers().await {
        Ok(providers) => providers,
        Err(e) => {
            tracing::warn!(
                error = %e,
                "dpa_page: failed to list providers for sub-processor table"
            );
            return Vec::new();
        }
    };

    providers
        .into_iter()
        .filter(|p| p.enabled)
        .filter(|p| !matches!(p.kind, kb_core::routing::ProviderKind::Local))
        .map(|p| {
            let kind_label = provider_kind_label(&p.kind);
            SubProcessorEntry {
                name: p.name,
                kind: kind_label,
                location: "Provider-defined".to_string(),
            }
        })
        .collect()
}

/// Map a [`ProviderKind`](kb_core::routing::ProviderKind) to a human-readable
/// label for the DPA sub-processor table.
fn provider_kind_label(kind: &kb_core::routing::ProviderKind) -> String {
    match kind {
        kb_core::routing::ProviderKind::OpenAiCompat => "OpenAI-compatible API".to_string(),
        kb_core::routing::ProviderKind::Anthropic => "Anthropic API".to_string(),
        kb_core::routing::ProviderKind::GeminiNative => "Google Gemini API".to_string(),
        kb_core::routing::ProviderKind::Local => "Local (no data transmission)".to_string(),
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use askama::Template;
    use axum::body::Body;
    use axum::http::StatusCode;
    use tower::ServiceExt;

    /// Build a minimal test `AppState` with a disconnected PgStore (no DB
    /// available for the DPA page's routing table query).
    fn test_state() -> Arc<AppState> {
        use kb_store::PgStore;

        let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
        Arc::new(AppState::new(
            Arc::new(kb_store::session_store::InMemorySessionStore::new()),
            pg_store,
            Some(std::time::Duration::from_secs(3600)),
            false,
        ))
    }

    /// Build the legal-page sub-router for integration tests.
    fn legal_router(state: Arc<AppState>) -> axum::Router {
        use axum::routing::get;

        axum::Router::new()
            .route("/terms", get(terms_page))
            .route("/privacy", get(privacy_page))
            .route("/dpa", get(dpa_page))
            .route("/aup", get(aup_page))
            .route("/cookies", get(cookies_page))
            .route("/retention", get(retention_page))
            .layer(axum::middleware::from_fn(
                crate::web::security::security_headers_middleware,
            ))
            .with_state(state)
    }

    // ── Template rendering tests ──────────────────────────────────────────

    #[test]
    fn terms_page_template_renders() {
        let html = TermsPage.render().expect("terms template must render");
        assert!(html.contains("Terms of Service"), "terms must have title");
        assert!(
            html.contains("Acceptance of Terms"),
            "terms must have acceptance section"
        );
        assert!(
            html.contains("Account Responsibilities"),
            "terms must have account responsibilities"
        );
        assert!(
            html.contains("Acceptable Use"),
            "terms must reference acceptable use"
        );
        assert!(
            html.contains("Limitation of Liability"),
            "terms must have liability section"
        );
        assert!(
            html.contains("<meta name=\"description\""),
            "must have meta description"
        );
    }

    #[test]
    fn privacy_page_template_renders() {
        let html = PrivacyPage.render().expect("privacy template must render");
        assert!(html.contains("Privacy Policy"), "privacy must have title");
        assert!(
            html.contains("Information We Collect"),
            "privacy must have collection section"
        );
        assert!(
            html.contains("Sub-Processors"),
            "privacy must mention sub-processors"
        );
        assert!(
            html.contains("Data Retention"),
            "privacy must mention retention"
        );
        assert!(
            html.contains("Your Rights"),
            "privacy must have rights section"
        );
        assert!(
            html.contains("<meta name=\"description\""),
            "must have meta description"
        );
    }

    #[test]
    fn dpa_page_template_renders_with_sub_processors() {
        let entries = vec![
            SubProcessorEntry {
                name: "openai-paid".to_string(),
                kind: "OpenAI-compatible API".to_string(),
                location: "Provider-defined".to_string(),
            },
            SubProcessorEntry {
                name: "claude-prod".to_string(),
                kind: "Anthropic API".to_string(),
                location: "Provider-defined".to_string(),
            },
        ];

        let page = DpaPage {
            sub_processors: entries,
        };
        let html = page.render().expect("dpa template must render");

        assert!(
            html.contains("Data Processing Agreement"),
            "dpa must have title"
        );
        assert!(
            html.contains("Sub-Processors"),
            "dpa must have sub-processor section"
        );
        assert!(
            html.contains("openai-paid"),
            "dpa must list openai provider"
        );
        assert!(
            html.contains("claude-prod"),
            "dpa must list claude provider"
        );
        assert!(html.contains("Anthropic API"), "dpa must show kind label");
        // Static sub-processors must still be present.
        assert!(
            html.contains("Backblaze B2"),
            "dpa must list B2 as static sub-processor"
        );
        assert!(html.contains("Stripe, Inc."), "dpa must list Stripe");
        assert!(
            html.contains("Email Provider"),
            "dpa must list email provider"
        );
        // Security measures table must be present.
        assert!(
            html.contains("Encryption at rest"),
            "dpa must mention encryption at rest"
        );
        assert!(html.contains("AES-256-GCM"), "dpa must mention cipher");
        assert!(
            html.contains("<meta name=\"description\""),
            "must have meta description"
        );
    }

    #[test]
    fn dpa_page_template_renders_without_sub_processors() {
        let page = DpaPage {
            sub_processors: Vec::new(),
        };
        let html = page.render().expect("dpa must render with empty providers");
        // The table should still have the static sub-processors.
        assert!(
            html.contains("Backblaze B2"),
            "static sub-processors must render"
        );
        assert!(html.contains("Stripe, Inc."), "static Stripe must render");
    }

    #[test]
    fn aup_page_template_renders() {
        let html = AupPage.render().expect("aup template must render");
        assert!(
            html.contains("Acceptable Use Policy"),
            "aup must have title"
        );
        assert!(
            html.contains("Prohibited Content"),
            "aup must have prohibited content section"
        );
        assert!(
            html.contains("Prohibited Activities"),
            "aup must have prohibited activities section"
        );
        assert!(
            html.contains("AI Model Usage"),
            "aup must have AI usage section"
        );
        assert!(
            html.contains("Enforcement"),
            "aup must have enforcement section"
        );
        assert!(
            html.contains("<meta name=\"description\""),
            "must have meta description"
        );
    }

    #[test]
    fn cookies_page_template_renders() {
        let html = CookiesPage.render().expect("cookies template must render");
        assert!(html.contains("Cookie Policy"), "cookies must have title");
        assert!(
            html.contains("strictly necessary"),
            "cookies must mention strictly necessary"
        );
        assert!(
            html.contains("__Host-session"),
            "cookies must list session cookie"
        );
        assert!(
            html.contains("__Host-csrf"),
            "cookies must list CSRF cookie"
        );
        assert!(html.contains("HttpOnly"), "cookies must mention HttpOnly");
        assert!(html.contains("SameSite"), "cookies must mention SameSite");
        assert!(
            html.contains("<meta name=\"description\""),
            "must have meta description"
        );
    }

    #[test]
    fn retention_page_template_renders() {
        let html = RetentionPage
            .render()
            .expect("retention template must render");
        assert!(html.contains("Data Retention"), "retention must have title");
        assert!(
            html.contains("Cryptographic Shredding"),
            "retention must have crypto-shredding section"
        );
        assert!(
            html.contains("Crypto-Shredding"),
            "retention must explain crypto-shredding"
        );
        assert!(
            html.contains("Key Encryption Key"),
            "retention must mention KEK"
        );
        assert!(
            html.contains("Data Encryption Key"),
            "retention must mention DEK"
        );
        assert!(
            html.contains("AES-256-GCM"),
            "retention must mention cipher"
        );
        assert!(
            html.contains("Backup Retention"),
            "retention must have backup section"
        );
        assert!(
            html.contains("Data Export"),
            "retention must mention export before deletion"
        );
        assert!(
            html.contains("<meta name=\"description\""),
            "must have meta description"
        );
    }

    // ── provider_kind_label tests ──────────────────────────────────────────

    #[test]
    fn provider_kind_label_openai() {
        let label = provider_kind_label(&kb_core::routing::ProviderKind::OpenAiCompat);
        assert!(label.contains("OpenAI"));
    }

    #[test]
    fn provider_kind_label_anthropic() {
        let label = provider_kind_label(&kb_core::routing::ProviderKind::Anthropic);
        assert!(label.contains("Anthropic"));
    }

    #[test]
    fn provider_kind_label_gemini() {
        let label = provider_kind_label(&kb_core::routing::ProviderKind::GeminiNative);
        assert!(label.contains("Gemini"));
    }

    #[test]
    fn provider_kind_label_local() {
        let label = provider_kind_label(&kb_core::routing::ProviderKind::Local);
        assert!(label.contains("Local"));
    }

    // ── Integration tests (axum::test) ────────────────────────────────────

    #[tokio::test]
    async fn get_terms_returns_html() {
        let state = test_state();
        let router = legal_router(state);

        let req = axum::http::Request::builder()
            .uri("/terms")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(ct.contains("text/html"));

        let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(body_str.contains("Terms of Service"));
    }

    #[tokio::test]
    async fn get_privacy_returns_html() {
        let state = test_state();
        let router = legal_router(state);

        let req = axum::http::Request::builder()
            .uri("/privacy")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(body_str.contains("Privacy Policy"));
    }

    #[tokio::test]
    async fn get_dpa_returns_html() {
        let state = test_state(); // disconnected DB → empty sub-processor list
        let router = legal_router(state);

        let req = axum::http::Request::builder()
            .uri("/dpa")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(body_str.contains("Data Processing Agreement"));
        // Static sub-processors must be present even without DB.
        assert!(
            body_str.contains("Backblaze B2"),
            "dpa must show static sub-processors even with disconnected DB"
        );
    }

    #[tokio::test]
    async fn get_aup_returns_html() {
        let state = test_state();
        let router = legal_router(state);

        let req = axum::http::Request::builder()
            .uri("/aup")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(body_str.contains("Acceptable Use Policy"));
    }

    #[tokio::test]
    async fn get_cookies_returns_html() {
        let state = test_state();
        let router = legal_router(state);

        let req = axum::http::Request::builder()
            .uri("/cookies")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(body_str.contains("Cookie Policy"));
        assert!(body_str.contains("__Host-session"));
    }

    #[tokio::test]
    async fn get_retention_returns_html() {
        let state = test_state();
        let router = legal_router(state);

        let req = axum::http::Request::builder()
            .uri("/retention")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(body_str.contains("Data Retention"));
        assert!(body_str.contains("Crypto-Shredding"));
    }

    #[tokio::test]
    async fn all_legal_routes_are_public() {
        for path in &[
            "/terms",
            "/privacy",
            "/dpa",
            "/aup",
            "/cookies",
            "/retention",
        ] {
            let state = test_state();
            let router = legal_router(state);

            let req = axum::http::Request::builder()
                .uri(*path)
                .body(Body::empty())
                .unwrap();
            let resp = router.oneshot(req).await.unwrap();
            assert_eq!(
                resp.status(),
                StatusCode::OK,
                "legal route {path} must return 200 without auth"
            );
        }
    }

    #[tokio::test]
    async fn legal_pages_have_security_headers() {
        for path in &[
            "/terms",
            "/privacy",
            "/dpa",
            "/aup",
            "/cookies",
            "/retention",
        ] {
            let state = test_state();
            let router = legal_router(state);

            let req = axum::http::Request::builder()
                .uri(*path)
                .body(Body::empty())
                .unwrap();
            let resp = router.oneshot(req).await.unwrap();

            assert_eq!(resp.status(), StatusCode::OK);
            assert!(
                resp.headers().contains_key("content-security-policy"),
                "legal route {path} must have CSP header"
            );
            assert!(
                resp.headers().contains_key("x-content-type-options"),
                "legal route {path} must have X-Content-Type-Options header"
            );
        }
    }
}
