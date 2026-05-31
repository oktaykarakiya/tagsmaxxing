//! Web UI module: Askama templates, CSRF protection, security headers, and
//! HTML page handlers for the Local File Knowledge Base.
//!
//! This module provides the server-rendered Web UI (plan §12) using:
//! - **Askama** for type-safe Jinja-like template rendering
//! - **Tailwind CSS** (CDN) for styling
//! - **CSRF tokens** on all mutating forms (double-submit cookie pattern)
//! - **Security headers** (CSP, X-Content-Type-Options, X-Frame-Options, etc.)
//!
//! # Architecture
//!
//! The web router lives alongside the JSON API router. When the application starts,
//! both routers are merged: web routes serve HTML pages at `/login`, `/register`,
//! `/logout`, `/search`, `/upload`, etc.; API routes serve JSON at `/api/*` and
//! `/auth/*`. The auth middleware gates the web's protected routes identically
//! to the API routes.
//!
//! # CSRF pattern
//!
//! Each page load generates a random 32-byte token, stores it in a `__Host-csrf`
//! cookie, and renders it in a hidden form field. On submission, the handler
//! compares the cookie and field values. Mismatches receive `403 Forbidden`.

pub(crate) mod csrf;
pub(crate) mod handlers;
pub(crate) mod security;
pub(crate) mod templates;

/// Build the web UI sub-router with all middleware and state.
///
/// The returned router carries its own [`Arc<AppState>`] and can be merged
/// into the main router via [`axum::Router::merge`].
///
/// Route table:
///
/// | Method | Path        | Auth? | Description                     |
/// |--------|-------------|-------|---------------------------------|
/// | GET    | `/`         | yes   | Redirect to /search             |
/// | GET    | `/login`    | no    | Login form (HTML)               |
/// | POST   | `/login`    | no    | Login form submission           |
/// | GET    | `/register` | no    | Registration form (HTML)        |
/// | POST   | `/register` | no    | Registration form submission    |
/// | POST   | `/logout`  | yes   | Revoke session, redirect        |
pub(crate) fn build_web_router(
    state: std::sync::Arc<crate::AppState>,
) -> axum::Router<std::sync::Arc<crate::AppState>> {
    use axum::middleware::from_fn_with_state;
    use axum::routing::{get, post};

    // Public web routes (no auth required).
    let public = axum::Router::new()
        .route(
            "/login",
            get(handlers::login_page).post(handlers::login_submit),
        )
        .route(
            "/register",
            get(handlers::register_page).post(handlers::register_submit),
        );

    // Protected web routes (auth required).
    let protected = axum::Router::new()
        .route("/", get(handlers::root_redirect))
        .route("/logout", post(handlers::logout_web))
        .layer(from_fn_with_state(
            std::sync::Arc::clone(&state),
            crate::middleware::auth_middleware,
        ));

    public
        .merge(protected)
        .layer(axum::middleware::from_fn(
            security::security_headers_middleware,
        ))
        .with_state(state)
}
