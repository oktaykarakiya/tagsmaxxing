//! Web UI route handlers: login page, register page, logout, and the root
//! redirect (`GET /` → `/login` or `/search`).
//!
//! These handlers render Askama templates and return HTML responses. CSRF
//! tokens are generated or validated on every request. Auth-related actions
//! (login, register, logout) delegate to the existing [`super::super::handlers::auth`]
//! JSON handlers and translate the responses into HTML pages with flash messages
//! and redirects.
//!
//! # HTML form submissions
//!
//! When the login/register forms are submitted, this handler parses
//! `application/x-www-form-urlencoded` bodies (the standard HTML form encoding),
//! validates the CSRF token via [`super::csrf::validate_csrf`], then calls the
//! corresponding JSON handler. Responses are 303 redirects:
//! - Success → `GET /search`
//! - Failure → back to the form with an error message.

use std::sync::Arc;

use askama::Template;
use axum::Extension;
use axum::Form;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Redirect, Response};
use serde::Deserialize;

use crate::AppState;
use crate::AuthUser;
use crate::handlers::auth::{LoginRequest, RegisterRequest};

use super::csrf;
use super::templates::{LoginPage, RegisterPage};

// ── Form data types (deserialized from application/x-www-form-urlencoded) ──

/// HTML form data for the login page.
#[derive(Debug, Deserialize)]
pub struct LoginForm {
    /// Hidden CSRF token.
    pub csrf_token: String,
    /// Tenant short slug.
    pub tenant_slug: String,
    /// User's email.
    pub email: String,
    /// Plaintext password.
    pub password: String,
}

/// HTML form data for the registration page.
#[derive(Debug, Deserialize)]
pub struct RegisterForm {
    /// Hidden CSRF token.
    pub csrf_token: String,
    /// Tenant short slug.
    pub tenant_slug: String,
    /// User's email.
    pub email: String,
    /// Plaintext password.
    pub password: String,
}

// ── Helpers ────────────────────────────────────────────────────────────────────

/// Render a template into an HTML response with the given status code.
///
/// If template rendering fails, returns a 500 error. This should be extremely
/// rare — Askama templates are compiled at build time, so the only runtime
/// failures are I/O or edge cases in format strings.
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

/// Generate a fresh CSRF token for form re-renders (after a failed submission).
///
/// Falls back to an empty string on error (which will fail CSRF validation on
/// the next submission, prompting a page reload — safe).
fn generate_fresh_csrf() -> String {
    csrf::generate_csrf_token().unwrap_or_default()
}

// ── GET / ──────────────────────────────────────────────────────────────────────

/// `GET /` — redirect to `/search` (the auth middleware runs before this handler,
/// so the user is authenticated by the time we get here).
///
/// Returns a `303 See Other` redirect.
pub async fn root_redirect(Extension(_auth_user): Extension<AuthUser>) -> impl IntoResponse {
    Redirect::to("/search")
}

// ── GET /login ─────────────────────────────────────────────────────────────────

/// `GET /login` — render the login page HTML form.
///
/// The form includes a hidden `csrf_token` field. A new CSRF token is generated
/// on each page load and set as the `__Host-csrf` cookie. The CSRF cookie is
/// deliberately NOT `HttpOnly` so HTMX and JavaScript can read it.
///
/// Query parameters can pre-fill the tenant slug and email fields (used after
/// a failed registration redirect).
pub async fn login_page(State(state): State<Arc<AppState>>) -> Result<Response, StatusCode> {
    let token = csrf::generate_csrf_token().map_err(|e| {
        tracing::error!(error = %e, "failed to generate CSRF token");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let page = LoginPage {
        csrf_token: token.clone(),
        tenant_slug: String::new(),
        email: String::new(),
        error: String::new(),
    };

    let mut resp = render_ok(&page);

    // Set the CSRF cookie.
    resp.headers_mut().insert(
        axum::http::header::SET_COOKIE,
        csrf::csrf_cookie_value(&token, state.session_ttl, state.secure_cookies),
    );

    Ok(resp)
}

// ── POST /login ────────────────────────────────────────────────────────────────

/// `POST /login` — process an HTML form login submission.
///
/// Validates CSRF, delegates to the shared [`crate::handlers::auth::login`]
/// logic, and on success redirects to `/search` with a `Set-Cookie` header.
/// On failure, re-renders the login form with an error message.
pub async fn login_submit(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Form(form): Form<LoginForm>,
) -> Response {
    // ── 1. Validate CSRF ────────────────────────────────────────────────────
    if let Err((status, msg)) = csrf::validate_csrf(&headers, &form.csrf_token) {
        let page = LoginPage {
            csrf_token: generate_fresh_csrf(),
            tenant_slug: form.tenant_slug.clone(),
            email: form.email.clone(),
            error: msg,
        };
        return render_template(&page, status);
    }

    // ── 2. Call the JSON login handler ──────────────────────────────────────
    let req = LoginRequest {
        tenant_slug: form.tenant_slug.clone(),
        email: form.email.clone(),
        password: form.password.clone(),
    };
    let json_body = axum::Json(req);

    match crate::handlers::auth::login(State(state.clone()), json_body).await {
        Ok((_status, auth_headers, _json)) => {
            // Build a 303 redirect with the Set-Cookie header from the auth handler
            // and a new CSRF cookie.
            let mut resp_headers = auth_headers;
            if let Ok(fresh_token) = csrf::generate_csrf_token() {
                resp_headers.append(
                    axum::http::header::SET_COOKIE,
                    csrf::csrf_cookie_value(&fresh_token, state.session_ttl, state.secure_cookies),
                );
            }
            let mut resp = Redirect::to("/search").into_response();
            resp.headers_mut().extend(resp_headers);
            resp
        }
        Err((status, err_json)) => {
            // Re-render the login form with the error message.
            let page = LoginPage {
                csrf_token: generate_fresh_csrf(),
                tenant_slug: form.tenant_slug,
                email: form.email,
                error: err_json.message.clone(),
            };
            render_template(&page, status)
        }
    }
}

// ── GET /register ──────────────────────────────────────────────────────────────

/// `GET /register` — render the registration page HTML form.
///
/// Identical pattern to [`login_page`]: CSRF token in cookie + hidden field.
pub async fn register_page(State(state): State<Arc<AppState>>) -> Result<Response, StatusCode> {
    let token = csrf::generate_csrf_token().map_err(|e| {
        tracing::error!(error = %e, "failed to generate CSRF token");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let page = RegisterPage {
        csrf_token: token.clone(),
        tenant_slug: String::new(),
        email: String::new(),
        error: String::new(),
    };

    let mut resp = render_ok(&page);
    resp.headers_mut().insert(
        axum::http::header::SET_COOKIE,
        csrf::csrf_cookie_value(&token, state.session_ttl, state.secure_cookies),
    );

    Ok(resp)
}

// ── POST /register ─────────────────────────────────────────────────────────────

/// `POST /register` — process an HTML form registration submission.
///
/// Validates CSRF, delegates to [`crate::handlers::auth::register`], and on
/// success redirects to `/search`. On failure, re-renders the registration
/// form with a generic error message (prevents user enumeration).
pub async fn register_submit(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Form(form): Form<RegisterForm>,
) -> Response {
    // ── 1. Validate CSRF ────────────────────────────────────────────────────
    if let Err((status, msg)) = csrf::validate_csrf(&headers, &form.csrf_token) {
        let page = RegisterPage {
            csrf_token: generate_fresh_csrf(),
            tenant_slug: form.tenant_slug.clone(),
            email: form.email.clone(),
            error: msg,
        };
        return render_template(&page, status);
    }

    // ── 2. Call the JSON register handler ───────────────────────────────────
    let req = RegisterRequest {
        tenant_slug: form.tenant_slug.clone(),
        email: form.email.clone(),
        password: form.password.clone(),
    };
    let json_body = axum::Json(req);

    match crate::handlers::auth::register(State(state.clone()), json_body).await {
        Ok((_status, auth_headers, _json)) => {
            let mut resp_headers = auth_headers;
            if let Ok(fresh_token) = csrf::generate_csrf_token() {
                resp_headers.append(
                    axum::http::header::SET_COOKIE,
                    csrf::csrf_cookie_value(&fresh_token, state.session_ttl, state.secure_cookies),
                );
            }
            let mut resp = Redirect::to("/search").into_response();
            resp.headers_mut().extend(resp_headers);
            resp
        }
        Err((status, err_json)) => {
            let page = RegisterPage {
                csrf_token: generate_fresh_csrf(),
                tenant_slug: form.tenant_slug,
                email: form.email,
                error: err_json.message.clone(),
            };
            render_template(&page, status)
        }
    }
}

// ── POST /logout (web) ─────────────────────────────────────────────────────────

/// `POST /logout` — revoke the session, clear cookies, and redirect to `/login`.
///
/// Requires the auth middleware (a valid session). Delegates to
/// [`crate::handlers::auth::logout`] and appends a CSRF cookie clearance.
pub async fn logout_web(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    headers: HeaderMap,
) -> Response {
    // Call the JSON logout handler.
    match crate::handlers::auth::logout(State(state.clone()), Extension(auth_user), headers).await {
        Ok((_status, mut resp_headers, _json)) => {
            // Also clear the CSRF cookie.
            resp_headers.append(
                axum::http::header::SET_COOKIE,
                csrf::cleared_csrf_cookie_value(state.secure_cookies),
            );
            let mut resp = Redirect::to("/login").into_response();
            resp.headers_mut().extend(resp_headers);
            resp
        }
        Err((status, _err_json)) => {
            // If logout fails (e.g. session store error), still redirect to /login
            // — the session cookie will expire on its own.
            let resp = Redirect::to("/login").into_response();
            (status, resp).into_response()
        }
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::Arc;
    use std::time::Duration;

    use axum::body::Body;
    use axum::http::{StatusCode, header};
    use axum::routing::{get, post};
    use kb_core::session::SessionStore;
    use kb_core::user::UserRole;
    use kb_store::PgStore;
    use kb_store::session_store::InMemorySessionStore;
    use tower::ServiceExt;

    use super::*;
    use crate::AppState;
    use crate::middleware::auth_middleware;
    use crate::web::security::security_headers_middleware;

    /// Build a test state with InMemorySessionStore + disconnected PgStore.
    fn test_state() -> Arc<AppState> {
        let session_store: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
        let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
        Arc::new(AppState::new(
            session_store,
            pg_store,
            Some(Duration::from_secs(3600)),
            false,
        ))
    }

    /// Full web router for integration tests.
    fn web_router(state: Arc<AppState>) -> axum::Router {
        // Public pages (no auth).
        let public = axum::Router::new()
            .route("/login", get(login_page).post(login_submit))
            .route("/register", get(register_page).post(register_submit));

        // Protected pages (auth required).
        let protected = axum::Router::new()
            .route("/", get(root_redirect))
            .route("/logout", post(logout_web))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ));

        public
            .merge(protected)
            .layer(axum::middleware::from_fn(security_headers_middleware))
            .with_state(state)
    }

    // ── GET /login tests ────────────────────────────────────────────────────

    #[tokio::test]
    async fn get_login_returns_html_with_csrf() {
        let state = test_state();
        let router = web_router(state);

        let req = axum::http::Request::builder()
            .uri("/login")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(ct.contains("text/html"), "login page must return HTML");

        let has_csrf = resp
            .headers()
            .get_all(header::SET_COOKIE)
            .iter()
            .any(|h| h.to_str().unwrap_or("").contains("__Host-csrf="));
        assert!(has_csrf, "login page must set CSRF cookie");

        let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(
            body_str.contains("csrf_token"),
            "HTML must contain CSRF hidden field"
        );
        assert!(body_str.contains("<form"), "HTML must contain a form");
    }

    #[tokio::test]
    async fn login_page_has_security_headers() {
        let state = test_state();
        let router = web_router(state);

        let req = axum::http::Request::builder()
            .uri("/login")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.headers().contains_key("content-security-policy"));
        assert!(resp.headers().contains_key("x-content-type-options"));
    }

    // ── GET /register tests ─────────────────────────────────────────────────

    #[tokio::test]
    async fn get_register_returns_html_with_csrf() {
        let state = test_state();
        let router = web_router(state);

        let req = axum::http::Request::builder()
            .uri("/register")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(
            body_str.contains("csrf_token"),
            "register must contain CSRF hidden field"
        );
        assert!(body_str.contains("<form"), "register must contain a form");
    }

    // ── POST /login tests (CSRF validation) ─────────────────────────────────

    #[tokio::test]
    async fn login_submit_no_csrf_cookie_fails_403() {
        let state = test_state();
        let router = web_router(state);

        let body = "csrf_token=fake&tenant_slug=t1&email=a@b.com&password=secret123";
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/login")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn login_submit_mismatched_csrf_fails_403() {
        let state = test_state();
        let router = web_router(state);

        let body = "csrf_token=wrong&tenant_slug=t1&email=a@b.com&password=secret123";
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/login")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(
                header::COOKIE,
                "__Host-csrf=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    // ── POST /register tests (CSRF validation) ──────────────────────────────

    #[tokio::test]
    async fn register_submit_no_csrf_cookie_fails_403() {
        let state = test_state();
        let router = web_router(state);

        let body = "csrf_token=fake&tenant_slug=t1&email=a@b.com&password=secret123456";
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/register")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    // ── GET / (root redirect) ───────────────────────────────────────────────

    #[tokio::test]
    async fn root_redirect_authenticated() {
        let state = test_state();

        // Create a valid session.
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, Duration::from_secs(3600))
            .await
            .unwrap();

        let router = web_router(state);

        let req = axum::http::Request::builder()
            .uri("/")
            .header(header::COOKIE, format!("__Host-session={token}"))
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::SEE_OTHER);
        let location = resp
            .headers()
            .get(header::LOCATION)
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(location, "/search");
    }

    #[tokio::test]
    async fn root_redirect_unauthenticated() {
        let state = test_state();
        let router = web_router(state);

        let req = axum::http::Request::builder()
            .uri("/")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        // Without a valid session, the auth middleware returns 401.
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── POST /logout (web) ──────────────────────────────────────────────────

    #[tokio::test]
    async fn logout_web_clears_cookies_and_redirects() {
        let state = test_state();

        let token = state
            .session_store
            .create(1, 42, UserRole::Member, Duration::from_secs(3600))
            .await
            .unwrap();

        let router = web_router(state.clone());

        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/logout")
            .header(header::COOKIE, format!("__Host-session={token}"))
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::SEE_OTHER);

        // Verify the session was revoked.
        let result = state.session_store.validate(&token).await.unwrap();
        assert!(result.is_none(), "session must be revoked after logout");

        // Verify redirect to /login.
        let location = resp
            .headers()
            .get(header::LOCATION)
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(location, "/login");

        // Verify Set-Cookie headers clear both session and CSRF cookies.
        let cookie_vals: Vec<_> = resp
            .headers()
            .get_all(header::SET_COOKIE)
            .iter()
            .map(|h| h.to_str().unwrap().to_string())
            .collect();
        let has_session_clear = cookie_vals
            .iter()
            .any(|c| c.contains("__Host-session=") && c.contains("Max-Age=0"));
        let has_csrf_clear = cookie_vals
            .iter()
            .any(|c| c.contains("__Host-csrf=") && c.contains("Max-Age=0"));
        assert!(has_session_clear, "logout must clear session cookie");
        assert!(has_csrf_clear, "logout must clear CSRF cookie");
    }
}
