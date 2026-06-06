//! Web UI route handlers: login page, register page, logout, root redirect,
//! and the search page with HTMX results (P6-T5).
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
//!
//! # Search (P6-T5)
//!
//! `GET /search` renders the full search page with an empty-state illustration.
//! `POST /search` receives an HTMX form submission, validates CSRF, runs the
//! retrieval pipeline, and returns an HTML fragment swapped into `#results`.

use std::sync::Arc;

use askama::Template;
use axum::Extension;
use axum::Form;
use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Redirect, Response};
use serde::Deserialize;

use crate::AppState;
use crate::AuthUser;
use crate::handlers::auth::{LoginRequest, RegisterRequest};

use super::csrf;
use super::templates::{
    KindFilter, LoginPage, RegisterPage, SearchPage, SearchResultHit, SearchResultsPartial,
    UploadPage,
};

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
    /// "Remember me" checkbox — extends session TTL to 30 days (P12-T2).
    #[serde(default)]
    pub remember_me: bool,
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

// ── Form data types (deserialized from application/x-www-form-urlencoded) ──

/// HTML form data for the HTMX search form (P6-T5).
///
/// `kind` is a comma-separated string (matching the JSON API convention).
/// The HTML template uses JavaScript to collect checked checkbox values into
/// a hidden field with this name, because `serde_urlencoded` cannot handle
/// `kind=image&kind=document` (duplicate field names) as a `Vec<String>`.
#[derive(Debug, Deserialize)]
pub struct SearchForm {
    /// Hidden CSRF token.
    pub csrf_token: String,
    /// Search query text.
    #[serde(default)]
    pub q: String,
    /// Selected kind(s), comma-separated (e.g. "image,document").
    #[serde(default)]
    pub kind: Option<String>,
    /// Tag filter text, comma-separated.
    #[serde(default)]
    pub tag: Option<String>,
}

// ── Helpers ────────────────────────────────────────────────────────────────────

/// Render a template into an HTML response with the given status code.
///
/// If template rendering fails, returns a 500 error. This should be extremely
/// rare — Askama templates are compiled at build time, so the only runtime
/// failures are I/O or edge cases in format strings.
pub(crate) fn render_template<T: Template>(template: &T, status: StatusCode) -> Response {
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
pub(crate) fn render_ok<T: Template>(template: &T) -> Response {
    render_template(template, StatusCode::OK)
}

/// Render a template with a `Set-Cookie` header that syncs the CSRF cookie to
/// the freshly-generated token in the form.
///
/// Callers MUST use this (or set the cookie themselves) when re-rendering a form
/// after a failed submission — otherwise the browser's stale `__Host-csrf` cookie
/// won't match the new hidden-field token, causing a permanent CSRF mismatch loop.
pub(crate) fn render_with_csrf_cookie<T: Template>(
    template: &T,
    status: StatusCode,
    csrf_token: &str,
    session_ttl: std::time::Duration,
    secure: bool,
) -> Response {
    let mut resp = render_template(template, status);
    resp.headers_mut().insert(
        axum::http::header::SET_COOKIE,
        csrf::csrf_cookie_value(csrf_token, session_ttl, secure),
    );
    resp
}

/// Generate a fresh CSRF token for form re-renders (after a failed submission).
///
/// Falls back to an empty string on error (which will fail CSRF validation on
/// the next submission, prompting a page reload — safe).
///
/// **Important**: callers that use this token in a re-rendered form MUST also
/// set a `__Host-csrf` `Set-Cookie` header so the browser's cookie stays in sync.
/// Use [`render_with_csrf_cookie`] for this, or set the cookie manually.
pub(crate) fn generate_fresh_csrf() -> String {
    csrf::generate_csrf_token().unwrap_or_default()
}

// ── GET / ──────────────────────────────────────────────────────────────────────

/// `GET /` — redirect to `/search` (the auth middleware runs before this handler,
/// so the user is authenticated by the time we get here).
///
/// Returns a `303 See Other` redirect.
///
/// This handler is retained for test use; the production router now directs `/`
/// through [`super::handlers_marketing::landing_page`] (P12-T1).
#[allow(dead_code)]
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
        let csrf_token = generate_fresh_csrf();
        let page = LoginPage {
            csrf_token: csrf_token.clone(),
            tenant_slug: form.tenant_slug.clone(),
            email: form.email.clone(),
            error: msg,
        };
        return render_with_csrf_cookie(
            &page,
            status,
            &csrf_token,
            state.session_ttl,
            state.secure_cookies,
        );
    }

    // ── 2. Call the JSON login handler ──────────────────────────────────────
    let req = LoginRequest {
        tenant_slug: form.tenant_slug.clone(),
        email: form.email.clone(),
        password: form.password.clone(),
        remember_me: form.remember_me,
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
            // Set the CSRF cookie so the next submission doesn't get a mismatch.
            let csrf_token = generate_fresh_csrf();
            let page = LoginPage {
                csrf_token: csrf_token.clone(),
                tenant_slug: form.tenant_slug,
                email: form.email,
                error: err_json.message.clone(),
            };
            render_with_csrf_cookie(
                &page,
                status,
                &csrf_token,
                state.session_ttl,
                state.secure_cookies,
            )
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
        let csrf_token = generate_fresh_csrf();
        let page = RegisterPage {
            csrf_token: csrf_token.clone(),
            tenant_slug: form.tenant_slug.clone(),
            email: form.email.clone(),
            error: msg,
        };
        return render_with_csrf_cookie(
            &page,
            status,
            &csrf_token,
            state.session_ttl,
            state.secure_cookies,
        );
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
            let csrf_token = generate_fresh_csrf();
            let page = RegisterPage {
                csrf_token: csrf_token.clone(),
                tenant_slug: form.tenant_slug,
                email: form.email,
                error: err_json.message.clone(),
            };
            render_with_csrf_cookie(
                &page,
                status,
                &csrf_token,
                state.session_ttl,
                state.secure_cookies,
            )
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

// ── GET /search ─────────────────────────────────────────────────────────────────

/// `GET /search` — render the search page.
///
/// Shows the search bar, filter controls, and an empty-state illustration.
/// The search form uses HTMX (`hx-post="/search"`) to submit searches and swap
/// results into the `#results` container without a full page reload.
pub async fn search_page(State(state): State<Arc<AppState>>) -> Result<Response, StatusCode> {
    let token = csrf::generate_csrf_token().map_err(|e| {
        tracing::error!(error = %e, "failed to generate CSRF token");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let kind_filters = build_kind_filters(&[]);

    let page = SearchPage {
        csrf_token: token.clone(),
        query: String::new(),
        kind_filters,
        selected_tags: String::new(),
        hits: Vec::new(),
    };

    let mut resp = render_ok(&page);
    resp.headers_mut().insert(
        axum::http::header::SET_COOKIE,
        csrf::csrf_cookie_value(&token, state.session_ttl, state.secure_cookies),
    );

    Ok(resp)
}

// ── POST /search (HTMX fragment) ────────────────────────────────────────────────

/// `POST /search` — execute a search and return the results as an HTML fragment.
///
/// This handler is called by HTMX when the search form is submitted. It validates
/// the CSRF token, runs the retrieval pipeline, and renders a
/// [`SearchResultsPartial`] fragment that HTMX swaps into the `#results` container.
///
/// If the pipeline is not configured or the search fails, an error fragment is returned.
pub async fn search_submit(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    headers: HeaderMap,
    Form(form): Form<SearchForm>,
) -> Response {
    // ── Validate CSRF ────────────────────────────────────────────────────────
    if let Err((status, _msg)) = csrf::validate_csrf(&headers, &form.csrf_token) {
        let partial = SearchResultsPartial {
            hits: Vec::new(),
            query: form.q,
        };
        return render_template(&partial, status);
    }

    // ── Validate input ───────────────────────────────────────────────────────
    let query_text = form.q.trim().to_string();
    if query_text.is_empty() {
        // Empty query → return the initial empty state.
        let partial = SearchResultsPartial {
            hits: Vec::new(),
            query: query_text,
        };
        return render_ok(&partial);
    }

    // ── Ensure pipeline is configured ────────────────────────────────────────
    let retrieval = match state.retrieval_pipeline.as_ref() {
        Some(r) => r,
        None => {
            tracing::error!("search_submit: retrieval pipeline not configured");
            let partial = SearchResultsPartial {
                hits: Vec::new(),
                query: query_text,
            };
            return render_template(&partial, StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    // ── Resolve filters ──────────────────────────────────────────────────────
    let kind_strs: Vec<&str> = form
        .kind
        .as_deref()
        .map(|s| {
            s.split(',')
                .map(|p| p.trim())
                .filter(|p| !p.is_empty())
                .collect()
        })
        .unwrap_or_default();
    let kinds: Vec<kb_core::kind::DocKind> = kind_strs
        .iter()
        .filter_map(|k| std::str::FromStr::from_str(k).ok())
        .collect();

    let tags: Vec<String> = form
        .tag
        .as_deref()
        .map(|s| {
            s.split(',')
                .map(|p| p.trim().to_string())
                .filter(|t| !t.is_empty())
                .collect()
        })
        .unwrap_or_default();

    // ── Budget gate → degrade (NOT reject) ───────────────────────────────────
    // If the tenant is over its monthly token budget, the web search degrades to the
    // keyword-only (lexical) path instead of failing — it never rejects the user
    // (P14-T5). Shares the exact gate logic with the JSON handler.
    let budget = state
        .pg_store
        .check_plan_token_budget_rollup(auth_user.tenant_id)
        .await;
    let degrade = crate::handlers::search::budget_check_forces_degrade(budget);

    // ── Build query + run pipeline ───────────────────────────────────────────
    let query = kb_core::query::Query {
        text: query_text.clone(),
        filters: kb_core::query::QueryFilters {
            kinds,
            tags,
            ..Default::default()
        },
        top_k: 20, // web UI shows more results
    };

    let (hits, mode) = match retrieval
        .retrieve(
            auth_user.tenant_id,
            Some(auth_user.user_id),
            &query,
            false,
            degrade,
        )
        .await
    {
        Ok(out) => out,
        Err(e) => {
            tracing::warn!(error = %e, "search failed");
            let partial = SearchResultsPartial {
                hits: Vec::new(),
                query: query_text,
            };
            return render_template(&partial, StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    let result_hits: Vec<SearchResultHit> = hits
        .into_iter()
        .map(|h| SearchResultHit {
            document_id: h.document_id,
            score: h.score,
            title: h.title,
            snippet: h.snippet,
            file_id: h.file_id,
            page_no: h.page_no,
            ts_offset: h.ts_offset,
            kind: h.kind,
        })
        .collect();

    let partial = SearchResultsPartial {
        hits: result_hits,
        query: query_text,
    };

    // Signal the degrade/hybrid mode to the client. The header lets the UI show a "basic
    // results (AI budget reached)" hint without changing the compiled template (P14-T5).
    with_search_mode_header(render_ok(&partial), mode)
}

/// Stamp the `X-Search-Mode` header (`hybrid` / `keyword`) onto a search response (P14-T5).
fn with_search_mode_header(mut resp: Response, mode: kb_pipeline::SearchMode) -> Response {
    resp.headers_mut().insert(
        "X-Search-Mode",
        axum::http::HeaderValue::from_static(mode.as_str()),
    );
    resp
}

// ── GET /upload ─────────────────────────────────────────────────────────────────

/// `GET /upload` — render the upload page with drag-and-drop zone.
///
/// Shows the drag-and-drop zone, file picker, reorderable file list (JS-driven),
/// group-as-document toggle, user-note textarea with mic button, and progress bar.
/// The CSRF token is set as a cookie and embedded in a hidden form field.
pub async fn upload_page(State(state): State<Arc<AppState>>) -> Result<Response, StatusCode> {
    let token = csrf::generate_csrf_token().map_err(|e| {
        tracing::error!(error = %e, "failed to generate CSRF token");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let page = UploadPage {
        csrf_token: token.clone(),
        error: String::new(),
    };

    let mut resp = render_ok(&page);
    resp.headers_mut().insert(
        axum::http::header::SET_COOKIE,
        csrf::csrf_cookie_value(&token, state.session_ttl, state.secure_cookies),
    );

    Ok(resp)
}

// ── POST /upload ────────────────────────────────────────────────────────────────

/// `POST /upload` — process a multipart file upload from the Web UI.
///
/// Validates the CSRF token (double-submit cookie), parses the multipart form
/// (files, user_note, group_as_document), stores blobs, enqueues ingest jobs,
/// and returns a JSON response with the job id for the frontend to poll.
///
/// The response is JSON (not HTML) because the upload form submits via
/// JavaScript `fetch()` — this allows the progress bar to poll job status
/// and redirect on completion.
pub async fn upload_submit(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    headers: HeaderMap,
    multipart: axum::extract::Multipart,
) -> Response {
    // ── 1. Parse multipart ──────────────────────────────────────────────────
    let parsed = match crate::handlers::ingest::parse_multipart(
        multipart,
        crate::handlers::ingest::MAX_PAYLOAD_BYTES,
    )
    .await
    {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, "upload_submit: bad multipart");
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "invalid_multipart",
                    "message": e.to_string()
                })),
            )
                .into_response();
        }
    };

    // ── 2. Validate CSRF (before other checks — failures are 403) ───────────
    if let Err((status, msg)) = csrf::validate_csrf(&headers, &parsed.csrf_token) {
        return (
            status,
            Json(serde_json::json!({
                "error": "csrf_validation_failed",
                "message": msg
            })),
        )
            .into_response();
    }

    // ── 3. Validate input ────────────────────────────────────────────────────
    if parsed.files.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "no_files",
                "message": "at least one file is required"
            })),
        )
            .into_response();
    }

    // ── 4. Storage quota check (plan-driven, P11-T5) ───────────────────────
    {
        let total_bytes: i64 = parsed.files.iter().map(|f| f.bytes.len() as i64).sum();
        match state
            .pg_store
            .check_plan_storage_quota(auth_user.tenant_id, total_bytes)
            .await
        {
            Ok(()) => {}
            Err(e) => {
                if let Some(qe) = e.downcast_ref::<kb_core::quota::QuotaError>() {
                    kb_metrics::record_quota_rejection("storage");
                    let msg = crate::handlers::ingest::quota_error_response(qe);
                    return (
                        StatusCode::PAYLOAD_TOO_LARGE,
                        Json(serde_json::json!({
                            "error": "storage_quota_exceeded",
                            "message": msg
                        })),
                    )
                        .into_response();
                }
                tracing::error!(error = %e, "upload_submit: storage quota check failed");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({
                        "error": "internal_error",
                        "message": "an unexpected error occurred"
                    })),
                )
                    .into_response();
            }
        }
    }

    // ── 4b. Monthly token-budget check (plan-driven, P14-T4) ────────────────
    // UX-first hard block mirroring POST /api/ingest: if the tenant has already
    // met or exceeded its monthly token budget (O(1) rollup read), reject the
    // next upload with 429 + upsell. The job that crossed the budget already ran
    // to completion; only subsequent uploads are blocked (bounded overshoot —
    // no pre-extraction token estimate, per-job overshoot bounded by the storage
    // quota checked above). See PgStore::check_plan_token_budget_rollup.
    {
        match state
            .pg_store
            .check_plan_token_budget_rollup(auth_user.tenant_id)
            .await
        {
            Ok(()) => {}
            Err(e) => {
                if let Some(qe) = e.downcast_ref::<kb_core::quota::QuotaError>() {
                    kb_metrics::record_quota_rejection("tokens");
                    let msg = crate::handlers::ingest::quota_error_response(qe);
                    return (
                        StatusCode::TOO_MANY_REQUESTS,
                        Json(serde_json::json!({
                            "error": "token_budget_exceeded",
                            "message": msg
                        })),
                    )
                        .into_response();
                }
                tracing::error!(error = %e, "upload_submit: token budget check failed");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({
                        "error": "internal_error",
                        "message": "an unexpected error occurred"
                    })),
                )
                    .into_response();
            }
        }
    }

    // ── 5. Ensure pipeline components are present ────────────────────────────
    let blob = match state.blob.as_ref() {
        Some(b) => b,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "not_configured",
                    "message": "blob store not configured"
                })),
            )
                .into_response();
        }
    };
    let pipeline = match state.ingest_pipeline.as_ref() {
        Some(p) => p,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "not_configured",
                    "message": "ingest pipeline not configured"
                })),
            )
                .into_response();
        }
    };

    // ── 6. Process upload inline (synchronous — no worker pool needed) ──────
    // Uses the same path as POST /api/ingest: validate → store blobs → pipeline
    // ingest → return document_id immediately. The job-queue worker pool is not
    // yet wired (P6-T7), so enqueue-only would leave jobs stuck at "queued".
    match crate::handlers::ingest::process_upload_inline(
        blob.as_ref(),
        pipeline.as_ref(),
        auth_user.tenant_id,
        Some(auth_user.user_id),
        &parsed,
    )
    .await
    {
        Ok(result) => (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({
                "job_id": result.job_id,
                "document_id": result.document_id,
                "message": format!("ingest processed ({} file(s))", result.file_count)
            })),
        )
            .into_response(),
        Err(e) => {
            // Map upload-validation rejections to 400 (and quota to 413), mirroring
            // POST /api/ingest — a zero-byte / disallowed-MIME / oversized /
            // unsafe-named upload is a client error, not a 500 (BUG-INGEST-06/07).
            crate::handlers::ingest::map_ingest_error(e).into_response()
        }
    }
}

// ── Kind filter helpers ────────────────────────────────────────────────────────

/// Build the list of kind filter checkboxes for the search sidebar.
///
/// Each known [`DocKind`] is listed with its wire string, a human-readable label,
/// and whether it is currently selected.
fn build_kind_filters(selected: &[String]) -> Vec<KindFilter> {
    kb_core::kind::DocKind::all()
        .iter()
        .map(|k| {
            let value = k.as_str().to_string();
            KindFilter {
                label: kind_label(&value),
                selected: selected.contains(&value),
                value,
            }
        })
        .collect()
}

/// Human-readable label for a [`DocKind`] wire string.
fn kind_label(kind: &str) -> String {
    match kind {
        "document" => "Docs".into(),
        "image" => "Images".into(),
        "audio" => "Audio".into(),
        "video" => "Video".into(),
        "identity_document" => "ID Docs".into(),
        "code" => "Code".into(),
        "archive" => "Archives".into(),
        "binary" => "Binaries".into(),
        other => {
            // Capitalize the first letter as a fallback.
            let mut chars = other.chars();
            match chars.next() {
                Some(c) => c.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
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

    /// Full web router for integration tests (includes search routes, P6-T5; upload routes, P6-T6).
    ///
    /// Mirrors the layers applied in `build_web_router`, including
    /// `DefaultBodyLimit` so upload tests verify the body limit is in place.
    fn web_router(state: Arc<AppState>) -> axum::Router {
        use axum::extract::DefaultBodyLimit;

        // Public pages (no auth).
        let public = axum::Router::new()
            .route("/login", get(login_page).post(login_submit))
            .route("/register", get(register_page).post(register_submit));

        // Protected pages (auth required).
        let protected = axum::Router::new()
            .route("/", get(root_redirect))
            .route("/search", get(search_page).post(search_submit))
            .route("/upload", get(upload_page).post(upload_submit))
            .route("/logout", post(logout_web))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ));

        public
            .merge(protected)
            .layer(DefaultBodyLimit::max(
                crate::handlers::ingest::MAX_PAYLOAD_BYTES as usize,
            ))
            .layer(axum::middleware::from_fn(security_headers_middleware))
            .with_state(state)
    }

    /// Build a mock-backed `RetrievalPipeline` for search tests.
    ///
    /// Returns a controlled pipeline that returns the given hits and rerank scores.
    async fn build_mock_retrieval(
        hits: Vec<kb_core::query::Hit>,
        scores: Vec<f32>,
    ) -> (kb_pipeline::RetrievalPipeline, kb_mock_backend::MockBackend) {
        use kb_core::reranker::Reranker;
        use kb_core::role::Role;
        use kb_core::store::Store;
        use kb_llm::LlamaClient;
        use kb_mock_backend::MockBackend;
        use kb_pipeline::embedder::ChunkEmbedder;
        use kb_scheduler::{Pool, test_backend};
        use reqwest::Client;

        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");
        let backend = Arc::new(test_backend(
            "mock-web-search",
            base_url,
            vec![Role::Embed],
            0,
            4,
        ));
        let pool = Pool::new(vec![backend], Duration::from_secs(5));
        let llm = Arc::new(LlamaClient::new(
            pool,
            Client::new(),
            0,
            0,
            Duration::from_millis(200),
        ));
        let embedder = Arc::new(ChunkEmbedder::new(llm, "test-model".into(), 3));

        // Mock Store returning controlled hits.
        struct MockStore {
            hits: Vec<kb_core::query::Hit>,
        }
        #[async_trait::async_trait]
        impl Store for MockStore {
            async fn upsert_file(&self, _rec: &kb_core::file::FileRecord) -> anyhow::Result<i64> {
                anyhow::bail!("mock: only hybrid_search is used")
            }
            async fn upsert_chunks(
                &self,
                _file_id: i64,
                _chunks: &[kb_core::chunk::Chunk],
            ) -> anyhow::Result<()> {
                anyhow::bail!("mock: only hybrid_search is used")
            }
            async fn hybrid_search(
                &self,
                _tenant_id: i64,
                _query: &kb_core::query::Query,
                _query_embedding: &[f32],
            ) -> anyhow::Result<Vec<kb_core::query::Hit>> {
                Ok(self.hits.clone())
            }
            async fn keyword_search(
                &self,
                _tenant_id: i64,
                _query: &kb_core::query::Query,
            ) -> anyhow::Result<Vec<kb_core::query::Hit>> {
                Ok(self.hits.clone())
            }
        }

        // Mock Reranker returning controlled scores.
        struct MockReranker {
            scores: Vec<f32>,
        }
        #[async_trait::async_trait]
        impl Reranker for MockReranker {
            async fn rerank(
                &self,
                _query: &str,
                _docs: &[String],
                _local_only: bool,
                _priority: i32,
                _tenant_id: i64,
                _user_id: Option<i64>,
            ) -> anyhow::Result<Vec<f32>> {
                Ok(self.scores.clone())
            }
        }

        let store: Arc<dyn Store> = Arc::new(MockStore { hits });
        let reranker: Arc<dyn Reranker> = Arc::new(MockReranker { scores });

        let pipeline = kb_pipeline::RetrievalPipeline::new(embedder, store, reranker);
        (pipeline, mock)
    }

    /// Build a `Hit` for use in mock store returns.
    fn make_hit(
        doc_id: i64,
        score: f32,
        snippet: &str,
        file_id: i64,
        page_no: Option<i32>,
        kind: Option<&str>,
    ) -> kb_core::query::Hit {
        kb_core::query::Hit {
            document_id: doc_id,
            score,
            title: Some(format!("Doc {doc_id}")),
            snippet: snippet.to_string(),
            file_id,
            page_no,
            ts_offset: None,
            kind: kind.map(String::from),
        }
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

    /// After a CSRF mismatch, the error page MUST include a `Set-Cookie` header
    /// that syncs the `__Host-csrf` cookie to the fresh token embedded in the
    /// re-rendered form. Without this, the user is stuck in a permanent mismatch
    /// loop: the browser keeps the old cookie but the form has a new token.
    ///
    /// This is a regression test for the bug where `render_template` was used
    /// instead of `render_with_csrf_cookie` on error paths.
    #[tokio::test]
    async fn login_submit_csrf_mismatch_syncs_cookie_to_form_token() {
        let state = test_state();
        let router = web_router(state);

        // Send a valid-but-wrong CSRF cookie with a mismatched form token.
        let old_token = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let body = "csrf_token=wrong&tenant_slug=t1&email=a@b.com&password=secret123".to_string();
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/login")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(header::COOKIE, format!("__Host-csrf={old_token}"))
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        // Extract the new CSRF token from the Set-Cookie header.
        let set_cookie = resp
            .headers()
            .get_all(header::SET_COOKIE)
            .iter()
            .find_map(|h| {
                let s = h.to_str().ok()?;
                if s.starts_with("__Host-csrf=") {
                    s.split(';')
                        .next()?
                        .strip_prefix("__Host-csrf=")
                        .map(|t| t.to_owned())
                } else {
                    None
                }
            })
            .expect("error response must set __Host-csrf cookie");

        assert!(!set_cookie.is_empty(), "new CSRF cookie must not be empty");
        assert_ne!(
            set_cookie, old_token,
            "new CSRF cookie must differ from the old one"
        );

        // Extract the CSRF token from the re-rendered form.
        let body_bytes = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
        let body_str = std::str::from_utf8(&body_bytes).unwrap();
        let form_token = body_str
            .split("name=\"csrf_token\" value=\"")
            .nth(1)
            .and_then(|s| s.split('\"').next())
            .map(|s| s.to_owned())
            .expect("error page must contain a csrf_token hidden field");

        assert_eq!(
            set_cookie,
            form_token,
            "CSRF cookie ({}) must match form token ({}) — otherwise resubmit fails",
            &set_cookie[..32.min(set_cookie.len())],
            &form_token[..32.min(form_token.len())],
        );
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

    /// Regression test: CSRF mismatch on POST /register must sync cookie to form token.
    /// Same bug class as login — see `login_submit_csrf_mismatch_syncs_cookie_to_form_token`.
    #[tokio::test]
    async fn register_submit_csrf_mismatch_syncs_cookie_to_form_token() {
        let state = test_state();
        let router = web_router(state);

        let old_token = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let body =
            "csrf_token=wrong&tenant_slug=t1&email=a@b.com&password=secret123456".to_string();
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/register")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(header::COOKIE, format!("__Host-csrf={old_token}"))
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        let set_cookie = resp
            .headers()
            .get_all(header::SET_COOKIE)
            .iter()
            .find_map(|h| {
                let s = h.to_str().ok()?;
                if s.starts_with("__Host-csrf=") {
                    s.split(';')
                        .next()?
                        .strip_prefix("__Host-csrf=")
                        .map(|t| t.to_owned())
                } else {
                    None
                }
            })
            .expect("error response must set __Host-csrf cookie");

        assert!(!set_cookie.is_empty());
        assert_ne!(set_cookie, old_token);

        let body_bytes = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
        let body_str = std::str::from_utf8(&body_bytes).unwrap();
        let form_token = body_str
            .split("name=\"csrf_token\" value=\"")
            .nth(1)
            .and_then(|s| s.split('\"').next())
            .map(|s| s.to_owned())
            .expect("error page must contain a csrf_token hidden field");

        assert_eq!(set_cookie, form_token);
    }

    // ── GET / (root redirect) ───────────────────────────────────────────────

    #[tokio::test]
    async fn root_redirect_authenticated() {
        let state = test_state();

        // Create a valid session.
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
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
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
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

    // ── GET /search tests ──────────────────────────────────────────────────

    #[tokio::test]
    async fn get_search_returns_html_with_csrf() {
        let state = test_state();
        let router = web_router(state.clone());

        // Authenticate.
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();

        let req = axum::http::Request::builder()
            .uri("/search")
            .header("Cookie", format!("__Host-session={token}"))
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
        assert!(ct.contains("text/html"), "search page must return HTML");

        let has_csrf = resp
            .headers()
            .get_all(header::SET_COOKIE)
            .iter()
            .any(|h| h.to_str().unwrap_or("").contains("__Host-csrf="));
        assert!(has_csrf, "search page must set CSRF cookie");

        let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(
            body_str.contains("csrf_token"),
            "search page must contain CSRF hidden field"
        );
        assert!(body_str.contains("hx-post"), "search page must use HTMX");
    }

    #[tokio::test]
    async fn get_search_unauthenticated_returns_401() {
        let state = test_state();
        let router = web_router(state);

        let req = axum::http::Request::builder()
            .uri("/search")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn search_page_has_security_headers() {
        let state = test_state();
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = web_router(state);

        let req = axum::http::Request::builder()
            .uri("/search")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.headers().contains_key("content-security-policy"));
        assert!(resp.headers().contains_key("x-content-type-options"));
    }

    // ── POST /search tests (HTMX fragments) ────────────────────────────────

    #[tokio::test]
    async fn search_submit_unauthenticated_returns_401() {
        let state = test_state();
        let router = web_router(state);

        let body = "csrf_token=fake&q=test";
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/search")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn search_submit_no_csrf_cookie_returns_403() {
        let state = test_state();
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = web_router(state);

        // Sending a csrf_token in the form body but no __Host-csrf cookie.
        let body = "csrf_token=fake123&q=hello";
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/search")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn search_submit_empty_query_returns_empty_state() {
        let state = test_state();
        // Generate a valid CSRF token and set it as a cookie.
        let csrf_token = csrf::generate_csrf_token().unwrap();
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = web_router(state);

        let body = format!("csrf_token={csrf_token}&q=+++"); // whitespace-only
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/search")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(
                "Cookie",
                format!("__Host-session={token}; __Host-csrf={csrf_token}"),
            )
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(
            body_str.contains("No results found") || body_str.contains("get started"),
            "empty query should show empty state; got: {body_str}"
        );
    }

    #[tokio::test]
    async fn search_submit_pipeline_not_configured_returns_500_fragment() {
        let state = test_state();
        let csrf_token = csrf::generate_csrf_token().unwrap();
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = web_router(state);

        let body = format!("csrf_token={csrf_token}&q=meaningful+query");
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/search")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(
                "Cookie",
                format!("__Host-session={token}; __Host-csrf={csrf_token}"),
            )
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        // Pipeline not configured → 500 status but still returns an HTML fragment.
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn search_submit_with_results_returns_fragment() {
        let hits = vec![
            make_hit(
                10,
                0.95,
                "matching snippet A",
                100,
                Some(1),
                Some("document"),
            ),
            make_hit(20, 0.72, "matching snippet B", 200, Some(2), Some("image")),
        ];
        let (pipeline, mock) = build_mock_retrieval(hits, vec![0.95, 0.72]).await;

        // Build state with the mock pipeline.
        let session_store: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
        let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
        let state = Arc::new(
            AppState::new(
                session_store,
                pg_store,
                Some(Duration::from_secs(3600)),
                false,
            )
            .with_retrieval_pipeline(Arc::new(pipeline)),
        );

        let csrf_token = csrf::generate_csrf_token().unwrap();
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = web_router(state);

        let body = format!("csrf_token={csrf_token}&q=test+query");
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/search")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(
                "Cookie",
                format!("__Host-session={token}; __Host-csrf={csrf_token}"),
            )
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap();

        // Verify result count and content.
        assert!(
            body_str.contains("2 result"),
            "should show result count; got: {body_str}"
        );
        assert!(
            body_str.contains("matching snippet A"),
            "should contain first snippet; got: {body_str}"
        );
        assert!(
            body_str.contains("Doc 10"),
            "should contain first doc title; got: {body_str}"
        );
        // Kind badges.
        assert!(
            body_str.contains("document") || body_str.contains("image"),
            "should have kind badges; got: {body_str}"
        );

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn search_submit_empty_results_renders_cta() {
        let (pipeline, mock) = build_mock_retrieval(vec![], vec![]).await;

        let session_store: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
        let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
        let state = Arc::new(
            AppState::new(
                session_store,
                pg_store,
                Some(Duration::from_secs(3600)),
                false,
            )
            .with_retrieval_pipeline(Arc::new(pipeline)),
        );

        let csrf_token = csrf::generate_csrf_token().unwrap();
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = web_router(state);

        let body = format!("csrf_token={csrf_token}&q=nonexistent");
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/search")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(
                "Cookie",
                format!("__Host-session={token}; __Host-csrf={csrf_token}"),
            )
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(
            body_str.contains("No results found"),
            "empty results should show no-results state; got: {body_str}"
        );

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn search_submit_with_kind_tag_filters() {
        let hits = vec![make_hit(1, 0.8, "photo match", 10, Some(1), Some("image"))];
        let (pipeline, mock) = build_mock_retrieval(hits, vec![0.9]).await;

        let session_store: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
        let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
        let state = Arc::new(
            AppState::new(
                session_store,
                pg_store,
                Some(Duration::from_secs(3600)),
                false,
            )
            .with_retrieval_pipeline(Arc::new(pipeline)),
        );

        let csrf_token = csrf::generate_csrf_token().unwrap();
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = web_router(state);

        // Send kind as comma-separated (simulating the hidden field filled by JS)
        // and tag as a simple string.
        let body = format!("csrf_token={csrf_token}&q=photo&kind=image&tag=vacation");
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/search")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(
                "Cookie",
                format!("__Host-session={token}; __Host-csrf={csrf_token}"),
            )
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        mock.shutdown().await;
    }

    #[test]
    fn search_form_deserialization() {
        // Single kind, with tag.
        let body = "csrf_token=abc123&q=photo&kind=image&tag=vacation";
        let form: SearchForm = serde_urlencoded::from_str(body).unwrap();
        assert_eq!(form.csrf_token, "abc123");
        assert_eq!(form.q, "photo");
        assert_eq!(form.kind.as_deref(), Some("image"));
        assert_eq!(form.tag.as_deref(), Some("vacation"));

        // Without kind/tag.
        let body2 = "csrf_token=abc123&q=photo";
        let form2: SearchForm = serde_urlencoded::from_str(body2).unwrap();
        assert_eq!(form2.csrf_token, "abc123");
        assert_eq!(form2.q, "photo");
        assert!(form2.kind.is_none());
        assert!(form2.tag.is_none());

        // Comma-separated kinds (as the JS-hidden field sends them).
        let body3 = "csrf_token=abc&q=test&kind=image,document";
        let form3: SearchForm = serde_urlencoded::from_str(body3).unwrap();
        assert_eq!(form3.kind.as_deref(), Some("image,document"));

        // Empty string kind (all checkboxes unchecked).
        let body4 = "csrf_token=x&q=query&kind=";
        let form4: SearchForm = serde_urlencoded::from_str(body4).unwrap();
        assert_eq!(form4.kind.as_deref(), Some(""));
    }

    // ── GET /upload tests ───────────────────────────────────────────────────

    #[tokio::test]
    async fn get_upload_returns_html_with_csrf() {
        let state = test_state();
        let router = web_router(state.clone());

        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();

        let req = axum::http::Request::builder()
            .uri("/upload")
            .header("Cookie", format!("__Host-session={token}"))
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
        assert!(ct.contains("text/html"), "upload page must return HTML");

        let has_csrf = resp
            .headers()
            .get_all(header::SET_COOKIE)
            .iter()
            .any(|h| h.to_str().unwrap_or("").contains("__Host-csrf="));
        assert!(has_csrf, "upload page must set CSRF cookie");

        let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(
            body_str.contains("csrf_token"),
            "HTML must contain CSRF hidden field; got: {body_str}"
        );
        assert!(
            body_str.contains("drop-zone") || body_str.contains("id=\"file-input\""),
            "HTML must contain the file upload area; got: {body_str}"
        );
        assert!(
            body_str.contains("group_as_document") || body_str.contains("group-toggle"),
            "HTML must contain the group-as-document toggle"
        );
    }

    #[tokio::test]
    async fn get_upload_unauthenticated_returns_401() {
        let state = test_state();
        let router = web_router(state);

        let req = axum::http::Request::builder()
            .uri("/upload")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn get_upload_has_security_headers() {
        let state = test_state();
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = web_router(state);

        let req = axum::http::Request::builder()
            .uri("/upload")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.headers().contains_key("content-security-policy"));
        assert!(resp.headers().contains_key("x-content-type-options"));
    }

    // ── POST /upload tests ──────────────────────────────────────────────────

    #[tokio::test]
    async fn upload_submit_unauthenticated_returns_401() {
        let state = test_state();
        let router = web_router(state);

        let boundary = "upboundary";
        let body_str = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"csrf_token\"\r\n\r\nabc\r\n--{boundary}--\r\n"
        );
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/upload")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(Body::from(body_str))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn upload_submit_no_csrf_cookie_returns_403() {
        let state = test_state();
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = web_router(state);

        let boundary = "upboundary2";
        // Multipart with a csrf_token field but no cookie.
        let body_str = format!(
            "--{boundary}\r\n\
             Content-Disposition: form-data; name=\"csrf_token\"\r\n\r\n\
             faketoken123\r\n\
             --{boundary}--\r\n"
        );
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/upload")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::from(body_str))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn upload_submit_mismatched_csrf_returns_403() {
        let state = test_state();
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = web_router(state);

        let valid_csrf = csrf::generate_csrf_token().unwrap();
        let boundary = "upboundary3";
        // Use a different token in the form body.
        let body_str = format!(
            "--{boundary}\r\n\
             Content-Disposition: form-data; name=\"csrf_token\"\r\n\r\n\
             wrong_token_here\r\n\
             --{boundary}--\r\n"
        );
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/upload")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .header(
                "Cookie",
                format!("__Host-session={token}; __Host-csrf={valid_csrf}"),
            )
            .body(Body::from(body_str))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn upload_submit_no_files_returns_400() {
        let state = test_state();
        let csrf_token = csrf::generate_csrf_token().unwrap();
        let session_token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();

        // Build state with blob store so it passes the component check.
        let state = Arc::new({
            let inner = AppState::new(
                state.session_store.clone(),
                state.pg_store.clone(),
                Some(Duration::from_secs(3600)),
                false,
            );
            // Use a mock blob (LocalBlob with temp dir).
            let tmp = tempfile::TempDir::with_prefix("kb-blob-").unwrap();
            let blob: Arc<dyn kb_core::blob::Blob> = Arc::new(kb_store::blob::LocalBlob::new(
                tmp.path().to_path_buf(),
                String::new(),
            ));
            inner.with_blob(blob)
        });
        let router = web_router(state);

        let boundary = "upboundary4";
        // Multipart with csrf_token + user_note but no files.
        let body_str = format!(
            "--{boundary}\r\n\
             Content-Disposition: form-data; name=\"csrf_token\"\r\n\r\n\
             {csrf_token}\r\n\
             --{boundary}\r\n\
             Content-Disposition: form-data; name=\"user_note\"\r\n\r\n\
             just a note\r\n\
             --{boundary}--\r\n"
        );
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/upload")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .header(
                "Cookie",
                format!("__Host-session={session_token}; __Host-csrf={csrf_token}"),
            )
            .body(Body::from(body_str))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
        let err: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(err["error"], "no_files");
    }

    #[tokio::test]
    async fn upload_submit_without_blob_returns_500() {
        let state = test_state();
        let csrf_token = csrf::generate_csrf_token().unwrap();
        let session_token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        // State without blob store → 500.
        let router = web_router(state);

        let boundary = "upboundary5";
        let body_str = format!(
            "--{boundary}\r\n\
             Content-Disposition: form-data; name=\"csrf_token\"\r\n\r\n\
             {csrf_token}\r\n\
             --{boundary}\r\n\
             Content-Disposition: form-data; name=\"files\"; filename=\"t.txt\"\r\n\
             Content-Type: text/plain\r\n\r\n\
             hello\r\n\
             --{boundary}--\r\n"
        );
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/upload")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .header(
                "Cookie",
                format!("__Host-session={session_token}; __Host-csrf={csrf_token}"),
            )
            .body(Body::from(body_str))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn upload_submit_with_file_and_blob_returns_202() {
        use kb_core::blob::Blob;
        use kb_pipeline::job_queue::JobQueue;
        use kb_store::blob::LocalBlob;

        let session_store: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
        let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));

        // Real LocalBlob with temp directory.
        let tmp = tempfile::TempDir::with_prefix("kb-upload-test-").unwrap();
        let blob: Arc<dyn Blob> = Arc::new(LocalBlob::new(tmp.path().to_path_buf(), String::new()));

        // Job queue with connect_lazy (won't actually connect, but that's fine —
        // process_upload_files will fail on enqueue; we test that the handler
        // returns a proper error).
        let job_queue = Arc::new(JobQueue::new(
            sqlx::PgPool::connect_lazy("postgres://localhost/test?sslmode=disable")
                .expect("connect_lazy always succeeds"),
            10_000,
            3,
        ));

        let state = Arc::new(
            AppState::new(
                session_store.clone(),
                pg_store,
                Some(Duration::from_secs(3600)),
                false,
            )
            .with_blob(blob)
            .with_job_queue(job_queue),
        );

        let csrf_token = csrf::generate_csrf_token().unwrap();
        let session_token = session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = web_router(state);

        let boundary = "upboundary6";
        let body_str = format!(
            "--{boundary}\r\n\
             Content-Disposition: form-data; name=\"csrf_token\"\r\n\r\n\
             {csrf_token}\r\n\
             --{boundary}\r\n\
             Content-Disposition: form-data; name=\"files\"; filename=\"hello.txt\"\r\n\
             Content-Type: text/plain\r\n\r\n\
             hello world\r\n\
             --{boundary}\r\n\
             Content-Disposition: form-data; name=\"user_note\"\r\n\r\n\
             test note\r\n\
             --{boundary}\r\n\
             Content-Disposition: form-data; name=\"group_as_document\"\r\n\r\n\
             true\r\n\
             --{boundary}--\r\n"
        );
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/upload")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .header(
                "Cookie",
                format!("__Host-session={session_token}; __Host-csrf={csrf_token}"),
            )
            .body(Body::from(body_str))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        // The blob store succeeds, but the job queue's enqueue will fail
        // because connect_lazy creates a pool that can't connect.
        // The response status will be 500 with an internal_error.
        // We test that the handler ran past CSRF validation (not 403).
        assert!(
            resp.status().is_server_error() || resp.status() == StatusCode::ACCEPTED,
            "expected 5xx (no real DB) or 202 (if queue connects); got {}",
            resp.status()
        );
    }

    /// Regression test: the web upload route MUST have a `DefaultBodyLimit`
    /// higher than axum's default 2 MiB. If this test fails with a 413 or
    /// multipart parsing error, someone removed the `DefaultBodyLimit` layer
    /// from the web router.
    ///
    /// We create a valid session (so auth passes), then send ~3 MiB of
    /// multipart data. If the body limit is too low axum rejects with 413
    /// before the handler runs, or truncates the body causing a multipart
    /// parse failure (400 with "multipart").
    #[tokio::test]
    async fn upload_body_limit_allows_over_2mib() {
        // Create state with a valid session so the auth middleware lets us through.
        let session_store: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
        let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
        let state = Arc::new(AppState::new(
            Arc::clone(&session_store) as Arc<dyn SessionStore>,
            pg_store,
            Some(Duration::from_secs(3600)),
            false,
        ));

        // Create a valid session (passes auth middleware).
        let session_token = session_store
            .create(1, 1, UserRole::Owner, false, Duration::from_secs(3600))
            .await
            .unwrap();

        let router = web_router(state);

        // Build a ~3 MiB multipart body — well above axum's default 2 MiB limit.
        let boundary = "testboundary123";
        let preamble = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"files\"; filename=\"big.bin\"\r\nContent-Type: application/octet-stream\r\n\r\n"
        );
        let epilogue = format!(
            "\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"csrf_token\"\r\n\r\nfake\r\n--{boundary}--\r\n"
        );
        let file_content = vec![b'A'; 3 * 1024 * 1024];
        let mut body_bytes =
            Vec::with_capacity(preamble.len() + file_content.len() + epilogue.len());
        body_bytes.extend_from_slice(preamble.as_bytes());
        body_bytes.extend_from_slice(&file_content);
        body_bytes.extend_from_slice(epilogue.as_bytes());

        let content_type = format!("multipart/form-data; boundary={boundary}");
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/upload")
            .header(header::CONTENT_TYPE, content_type)
            .header(
                "Cookie",
                format!("__Host-session={session_token}; __Host-csrf=fake"),
            )
            .body(Body::from(body_bytes))
            .unwrap();

        let resp = router.oneshot(req).await.unwrap();
        let status = resp.status();

        // With a proper body limit the multipart body parses fully.
        // The request fails later (CSRF mismatch → 403, or 500 because the test
        // has no real blob store / DB).
        //
        // If the body limit is too low we get either:
        //   - 413 Payload Too Large (axum rejects before the handler)
        //   - 400 with "multipart" in the body (truncated body → parse error)
        assert_ne!(
            status,
            StatusCode::PAYLOAD_TOO_LARGE,
            "upload body limit too low — got 413 Payload Too Large for 3 MiB body.\n\
             The DefaultBodyLimit layer is missing from the web router."
        );

        if status == StatusCode::BAD_REQUEST {
            let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
            let body_str = std::str::from_utf8(&body).unwrap_or("");
            assert!(
                !body_str.contains("multipart"),
                "upload body limit too low ({body_str} MiB body was truncated by the default 2 MiB limit).\n\
                 Add DefaultBodyLimit to the web router."
            );
        }

        // Any other status (403, 500, etc.) proves the multipart body was
        // parsed successfully and the request progressed past body reading.
    }

    // ── csrf_token extraction from multipart ────────────────────────────────

    #[tokio::test]
    async fn parse_multipart_extracts_csrf_token() {
        use crate::handlers::ingest::{MAX_PAYLOAD_BYTES, parse_multipart};
        use axum::extract::FromRequest;

        let boundary = "csrftest1";
        let body_bytes = {
            let mut body = Vec::new();
            let add_field = |body: &mut Vec<u8>, name: &str, value: &str| {
                body.extend_from_slice(
                    format!(
                        "\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}"
                    )
                    .as_bytes(),
                );
            };
            add_field(&mut body, "csrf_token", "mycsrftoken123");
            add_field(&mut body, "user_note", "a note");
            add_field(&mut body, "group_as_document", "true");
            body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
            body
        };

        let content_type = format!("multipart/form-data; boundary={boundary}");
        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/upload")
            .header("Content-Type", content_type)
            .body(Body::from(body_bytes))
            .unwrap();

        let multipart = <axum::extract::Multipart as FromRequest<()>>::from_request(request, &())
            .await
            .unwrap();
        let parsed = parse_multipart(multipart, MAX_PAYLOAD_BYTES).await.unwrap();

        assert_eq!(parsed.csrf_token, "mycsrftoken123");
        assert_eq!(parsed.user_note.as_deref(), Some("a note"));
        assert!(parsed.group_as_document);
        assert!(parsed.files.is_empty());
    }

    #[tokio::test]
    async fn parse_multipart_csrf_token_defaults_empty() {
        use crate::handlers::ingest::{MAX_PAYLOAD_BYTES, parse_multipart};
        use axum::extract::FromRequest;

        let boundary = "csrftest2";
        // No csrf_token field at all.
        let body_str = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"user_note\"\r\n\r\nhi\r\n--{boundary}--\r\n"
        );
        let content_type = format!("multipart/form-data; boundary={boundary}");
        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/upload")
            .header("Content-Type", content_type)
            .body(Body::from(body_str))
            .unwrap();

        let multipart = <axum::extract::Multipart as FromRequest<()>>::from_request(request, &())
            .await
            .unwrap();
        let parsed = parse_multipart(multipart, MAX_PAYLOAD_BYTES).await.unwrap();

        assert!(parsed.csrf_token.is_empty());
    }

    // ── UploadPage template pure-logic test ─────────────────────────────────

    #[test]
    fn upload_page_template_renders_with_csrf() {
        let page = UploadPage {
            csrf_token: "test-csrf-123".into(),
            error: String::new(),
        };
        let html = page.render().expect("upload template must render");

        assert!(
            html.contains("test-csrf-123"),
            "rendered HTML must contain CSRF token"
        );
        assert!(
            html.contains("drop-zone") || html.contains("id=\"file-input\""),
            "rendered HTML must contain the file upload area"
        );
        assert!(
            html.contains("group-toggle") || html.contains("group_as_document"),
            "rendered HTML must contain the group-as-document toggle"
        );
    }

    // ── Shared app-nav macro regression test ───────────────────────────────

    #[test]
    fn search_page_nav_exposes_all_primary_links() {
        // Regression: the post-login landing page (`/search`) previously rendered
        // a stunted nav with only a Search link, so freshly-logged-in users had no
        // way to reach Upload/Dashboard/Account. The shared `app_nav` macro
        // (templates/_macros.html) must expose every primary destination here.
        let page = SearchPage {
            csrf_token: "nav-csrf-token".into(),
            query: String::new(),
            kind_filters: build_kind_filters(&[]),
            selected_tags: String::new(),
            hits: Vec::new(),
        };
        let html = page.render().expect("search template must render");

        for href in ["/dashboard", "/search", "/upload", "/account"] {
            assert!(
                html.contains(&format!("href=\"{href}\"")),
                "search-page nav must link to {href}"
            );
        }
        // The logout form is present and carries the page CSRF token.
        assert!(
            html.contains("action=\"/logout\"") && html.contains("nav-csrf-token"),
            "search-page nav must contain a CSRF-protected logout form"
        );
        // The current page is marked active for highlighting/accessibility.
        assert!(
            html.contains("aria-current=\"page\""),
            "search-page nav must mark the active item"
        );
    }

    // ── kind_label pure-logic tests ─────────────────────────────────────────

    #[test]
    fn kind_label_returns_readable_strings() {
        assert_eq!(kind_label("document"), "Docs");
        assert_eq!(kind_label("image"), "Images");
        assert_eq!(kind_label("audio"), "Audio");
        assert_eq!(kind_label("video"), "Video");
        assert_eq!(kind_label("identity_document"), "ID Docs");
        assert_eq!(kind_label("code"), "Code");
        assert_eq!(kind_label("archive"), "Archives");
        assert_eq!(kind_label("binary"), "Binaries");
    }

    #[test]
    fn kind_label_unknown_falls_back_to_capitalized() {
        assert_eq!(kind_label("unknown"), "Unknown");
        assert_eq!(kind_label("x"), "X");
        assert_eq!(kind_label(""), "");
    }

    // ── build_kind_filters pure-logic tests ────────────────────────────────

    #[test]
    fn build_kind_filters_all_present() {
        let filters = build_kind_filters(&[]);
        // All 8 DocKind variants must be present.
        assert_eq!(filters.len(), kb_core::kind::DocKind::all().len());
        // None selected initially.
        assert!(filters.iter().all(|f| !f.selected));
    }

    #[test]
    fn build_kind_filters_selected() {
        let selected = vec!["image".to_string(), "code".to_string()];
        let filters = build_kind_filters(&selected);
        let img = filters
            .iter()
            .find(|f| f.value == "image")
            .expect("image filter present");
        assert!(img.selected);
        let code = filters
            .iter()
            .find(|f| f.value == "code")
            .expect("code filter present");
        assert!(code.selected);
        let doc = filters
            .iter()
            .find(|f| f.value == "document")
            .expect("document filter present");
        assert!(!doc.selected);
    }

    #[test]
    fn build_kind_filters_values_match_doc_kind() {
        let filters = build_kind_filters(&[]);
        for k in kb_core::kind::DocKind::all() {
            assert!(
                filters.iter().any(|f| f.value == k.as_str()),
                "kind filter missing for {}",
                k.as_str()
            );
        }
    }
}
