// SPDX-License-Identifier: AGPL-3.0-or-later

//! Authentication and rate-limiting middleware.
//!
//! # Auth middleware
//!
//! [`auth_middleware`] runs on every request to protected routes. It tries two
//! credential sources in order:
//!
//! 1. **Bearer token** (plan §30, P12-T5): extracts the `Authorization: Bearer <token>`
//!    header, hashes it, and validates against the [`ApiTokenStore`]. On success,
//!    injects [`AuthUser`](crate::AuthUser) and passes through. An *invalid* Bearer
//!    token is a hard 401 — there is no fallback to cookies.
//! 2. **Session cookie** (plan §13, P5-T5): extracts the `__Host-session` cookie,
//!    validates against the [`SessionStore`]. On success, injects [`AuthUser`]
//!    and slides the session expiry.
//!
//! If neither credential is present or valid, the middleware returns `401 Unauthorized`.
//!
//! # Email verification gate
//!
//! After authentication, [`auth_middleware`] checks whether the user's email has been
//! verified. Requests to API paths (`/api/*`) are blocked with `403 Forbidden` when
//! `email_verified` is `false`. Web UI routes (HTML pages) are **not** blocked here
//! so the user can still see the "verify your email" prompt; individual sensitive web
//! handlers can opt in by layering [`require_email_verified_middleware`].
//!
//! # Login rate limiter
//!
//! [`login_rate_limit_middleware`] protects `POST /auth/login` against brute-force
//! attacks. It tracks attempts per client IP in a fixed window (5 attempts per 60 s)
//! and returns `429 Too Many Requests` with a `Retry-After` header when exceeded.
//!
//! # Request correlation (plan §18, P14-T11)
//!
//! [`request_id_middleware`] runs **outermost** on every request. It adopts an
//! incoming `X-Request-Id` header (or mints a fresh UUID v4 when absent), runs the
//! inner stack inside a [`request_span`](kb_logging::request_span) so every
//! downstream tracing event carries the `request_id` field, and echoes the id back
//! on the response. Once [`auth_middleware`] resolves an [`AuthUser`], it nests a
//! [`tenant_span`](kb_logging::tenant_span) inside the request span so handler logs
//! additionally carry `tenant_id` + `user_id`. Both use
//! [`tracing::Instrument`]`::instrument` (not `Span::enter`) so the span follows the
//! request future across `.await` points.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use axum::extract::MatchedPath;
use axum::extract::State;
use axum::http::Method;
use axum::http::Request;
use axum::http::StatusCode;
use axum::http::{HeaderName, HeaderValue};
use axum::middleware::Next;
use axum::response::Response;

use tower_http::cors::{Any, CorsLayer};
use tracing::Instrument;

use crate::AppState;
use crate::AuthUser;

/// The HTTP header carrying the per-request correlation id (read on the way in,
/// echoed on the way out).
const REQUEST_ID_HEADER: &str = "x-request-id";

/// Maximum accepted length of an inbound `X-Request-Id`.
///
/// A client-supplied id is echoed into logs and the response header, so it is
/// bounded to keep log lines and headers small; anything longer (or not valid
/// header-safe ASCII) is discarded in favour of a freshly minted UUID.
const MAX_REQUEST_ID_LEN: usize = 200;

// ── Request correlation id (plan §18, P14-T11) ───────────────────────────────────

/// Resolve the correlation id for a request: adopt a sane inbound
/// `X-Request-Id`, otherwise mint a fresh UUID v4.
///
/// An inbound id is accepted only when it is non-empty, at most
/// [`MAX_REQUEST_ID_LEN`] bytes, and entirely visible ASCII (no spaces or
/// control characters) so it is safe to place verbatim into a response header
/// and structured log line. Any other input — including a missing or
/// non-ASCII header — yields a freshly generated id, so every request always
/// gets a usable, header-safe id.
fn resolve_request_id(headers: &axum::http::HeaderMap) -> String {
    if let Some(raw) = headers.get(REQUEST_ID_HEADER).and_then(|v| v.to_str().ok()) {
        let candidate = raw.trim();
        if !candidate.is_empty()
            && candidate.len() <= MAX_REQUEST_ID_LEN
            && candidate.bytes().all(|b| b.is_ascii_graphic() || b == b' ')
            && !candidate.contains(' ')
        {
            return candidate.to_owned();
        }
    }
    uuid::Uuid::new_v4().to_string()
}

/// Axum middleware: attach a correlation id to every request.
///
/// This layer is mounted **outermost** so it sees every request before any
/// other middleware. It [`resolve_request_id`]s a correlation id (adopting a
/// caller-supplied `X-Request-Id` or minting a UUID v4), then runs the inner
/// stack inside a [`request_span`](kb_logging::request_span) via
/// [`tracing::Instrument`] — *not* `Span::enter()`, which would not stay
/// attached across the inner `.await`. As a result, every tracing event emitted
/// while handling the request carries the `request_id` field. Finally the id is
/// echoed back on the response as `X-Request-Id` so clients (and proxies) can
/// correlate their call with server logs.
pub async fn request_id_middleware(request: Request<axum::body::Body>, next: Next) -> Response {
    let request_id = resolve_request_id(request.headers());
    let span = kb_logging::request_span(&request_id);

    // `.instrument(span)` keeps the span entered across every `.await` inside
    // the inner stack (handlers, auth, DB calls). A bare `span.enter()` guard
    // would be dropped at the first await and lose the correlation field.
    let mut response = next.run(request).instrument(span).await;

    // Echo the id back so callers can correlate. A resolved id is always
    // header-safe (graphic ASCII, no spaces), so this never fails in practice;
    // fall back silently rather than poison the response on the impossible path.
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response
            .headers_mut()
            .insert(HeaderName::from_static(REQUEST_ID_HEADER), value);
    }

    response
}

/// Run the inner request future inside a [`tenant_span`](kb_logging::tenant_span)
/// so downstream handler logs carry `tenant_id` + `user_id`.
///
/// Called from [`auth_middleware`] once an [`AuthUser`] has been resolved. The
/// span is entered via [`tracing::Instrument`] (not `Span::enter()`) so it stays
/// attached across the handler's `.await` points. Because the request-id layer
/// runs outermost, this tenant span nests **inside** the active request span, and
/// events emitted by handlers carry `request_id`, `tenant_id`, and `user_id`
/// together.
///
/// Additional context fields — `tenant_id`, `job_id`, `document_id` — are
/// recorded on the span when available from request extensions, so downstream
/// log correlation works without handlers having to manage their own spans.
async fn run_in_tenant_span(
    user: AuthUser,
    request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let span =
        kb_logging::tenant_span(&user.tenant_id.to_string(), Some(&user.user_id.to_string()));

    // Record additional correlation fields from request extensions when
    // available (set by handlers in the ingest/search/document paths).
    if let Some(tenant_id) = request.extensions().get::<AuthUser>().map(|u| u.tenant_id) {
        span.record("tenant_id", tenant_id);
    }
    if let Some(job_id) = request.extensions().get::<kb_logging::JobId>() {
        span.record("job_id", job_id.0);
    }
    if let Some(doc_id) = request.extensions().get::<kb_logging::DocumentId>() {
        span.record("document_id", doc_id.0);
    }

    run_maybe_idempotent(request, next).instrument(span).await
}

/// Axum middleware: extract and validate the session cookie, injecting
/// [`AuthUser`] into request extensions on success.
///
/// # Cookie extraction
///
/// Parses the `Cookie` header, looking for `__Host-session=<token>`.
/// If the cookie is missing, malformed, or the token is unknown/expired/revoked,
/// the middleware returns `401 Unauthorized` with a JSON `{error, message}` body.
///
/// # Sliding expiration
///
/// On every valid request the middleware calls
/// [`SessionStore::extend`](kb_core::session::SessionStore::extend) to slide the
/// session expiry forward. Failures to extend are logged but do **not** reject the
/// request — a session that can't be extended is still valid for the current request.
///
/// # Error envelope
///
/// All error responses from this middleware use the canonical `{error, message}`
/// JSON envelope, matching the rest of the `/api/*` surface.
pub async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    // ── 1. Try Bearer token (API token) ─────────────────────────────────────
    if let Some(bearer_token) = extract_bearer_token(request.headers()) {
        // If an Authorization header is present, it MUST be valid. We do NOT
        // fall through to the cookie check — a bad Bearer token is a hard 401.
        if let Some(store) = &state.api_token_store {
            match store.validate_token(&bearer_token).await {
                Ok(Some(info)) => {
                    let email_verified = info.email_verified;
                    let auth_user = AuthUser {
                        tenant_id: info.tenant_id,
                        user_id: info.user_id,
                        role: info.user_role,
                        email_verified,
                    };
                    request.extensions_mut().insert(auth_user);
                    // Gate API routes on email verification.
                    if !email_verified && is_api_path(request.uri().path()) {
                        return Ok(auth_error_response(
                            StatusCode::FORBIDDEN,
                            "email_not_verified",
                            "Email verification required to access this resource",
                        ));
                    }
                    // Per-token rate limiting for API routes.
                    if is_api_path(request.uri().path())
                        && let Err(retry_after) = TOKEN_RATE_LIMITER.check(&bearer_token)
                    {
                        return Ok(rate_limit_response(retry_after));
                    }
                    // Plan per-minute rate limiting for API routes.
                    {
                        let is_api = is_api_path(request.uri().path());
                        let tenant_id = request.extensions().get::<AuthUser>().map(|u| u.tenant_id);
                        if is_api && let Some(tid) = tenant_id {
                            match state.pg_store.resolve_rate_cap(tid).await {
                                Ok(Some(cap)) if cap > 0 => {
                                    if let Err(retry_after) =
                                        PLAN_RATE_LIMITER.check(tid, cap as usize)
                                    {
                                        kb_metrics::record_rate_limit_rejection("plan");
                                        return Ok(rate_limit_response(retry_after));
                                    }
                                }
                                Ok(_) => { /* no cap or zero — pass through */ }
                                Err(e) => {
                                    tracing::warn!(error = %e, tenant_id = tid, "failed to resolve plan rate cap (failing open)");
                                }
                            }
                        }
                    }
                    let response = run_in_tenant_span(auth_user, request, next).await?;
                    return Ok(add_private_cache_control_if_html(response));
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(error = %e, "API token store error during Bearer validation");
                }
            }
        }
        // Bearer token present but invalid → 401.
        return Ok(auth_error_response(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Invalid or expired API token",
        ));
    }

    // ── 2. Fall back to session cookie ──────────────────────────────────────
    let token = extract_session_cookie(request.headers());

    let token = match token {
        Some(t) => t,
        None => {
            return Ok(auth_error_response(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "Authentication required",
            ));
        }
    };

    let info = match state.session_store.validate(&token).await {
        Ok(Some(info)) => info,
        Ok(None) => {
            return Ok(auth_error_response(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "Invalid or expired session",
            ));
        }
        Err(e) => {
            tracing::warn!(error = %e, "session store error during validation");
            return Ok(auth_error_response(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "Authentication required",
            ));
        }
    };

    // Inject the authenticated user into request extensions.
    let email_verified = info.email_verified;
    let auth_user = AuthUser {
        tenant_id: info.tenant_id,
        user_id: info.user_id,
        role: info.user_role,
        email_verified,
    };
    request.extensions_mut().insert(auth_user);

    // Slide the session expiry (best-effort — failure does not reject the request).
    if let Err(e) = state.session_store.extend(&token, state.session_ttl).await {
        tracing::warn!(error = %e, "failed to extend session (sliding expiration)");
    }

    // Gate API routes on email verification.
    if !email_verified && is_api_path(request.uri().path()) {
        return Ok(auth_error_response(
            StatusCode::FORBIDDEN,
            "email_not_verified",
            "Email verification required to access this resource",
        ));
    }

    // Plan per-minute rate limiting for API routes.
    {
        let is_api = is_api_path(request.uri().path());
        let tenant_id = request.extensions().get::<AuthUser>().map(|u| u.tenant_id);
        if is_api && let Some(tid) = tenant_id {
            match state.pg_store.resolve_rate_cap(tid).await {
                Ok(Some(cap)) if cap > 0 => {
                    if let Err(retry_after) = PLAN_RATE_LIMITER.check(tid, cap as usize) {
                        kb_metrics::record_rate_limit_rejection("plan");
                        return Ok(rate_limit_response(retry_after));
                    }
                }
                Ok(_) => { /* no cap or zero — pass through */ }
                Err(e) => {
                    tracing::warn!(error = %e, tenant_id = tid, "failed to resolve plan rate cap (failing open)");
                }
            }
        }
    }

    let response = run_in_tenant_span(auth_user, request, next).await?;
    Ok(add_private_cache_control_if_html(response))
}

/// Build a JSON error response with the canonical `{error, message}` envelope
/// used by all `/api/*` routes.
///
/// Sets the response status code and `Content-Type: application/json`.
fn auth_error_response(status: StatusCode, error: &str, message: &str) -> Response {
    let body = axum::body::Body::from(
        serde_json::json!({
            "error": error,
            "message": message
        })
        .to_string(),
    );
    let mut response = Response::new(body);
    *response.status_mut() = status;
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/json"),
    );
    response
}

/// Mark an authenticated HTML response as non-shared-cacheable.
///
/// Authenticated pages (e.g. `/account`, `/dashboard`) render tenant-private
/// HTML. Without an explicit directive a shared/proxy cache could store one
/// tenant's rendered page and serve it to another user. This sets
/// `Cache-Control: private, no-store` on HTML responses (`Content-Type:
/// text/html`) so shared caches never retain them.
///
/// Only HTML responses are touched: JSON API payloads and static assets pass
/// through unchanged (they are not tenant-private HTML and may carry their own
/// caching policy). Any pre-existing `Cache-Control` on an HTML response is
/// replaced — private authenticated HTML must never be publicly cacheable.
///
/// The directive is a fixed security invariant, not an operator-tunable knob,
/// so it is a compile-time constant rather than hot-swappable configuration.
fn add_private_cache_control_if_html(mut response: Response) -> Response {
    let is_html = response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .is_some_and(|v| v.as_bytes().starts_with(b"text/html"));

    if is_html {
        response.headers_mut().insert(
            axum::http::header::CACHE_CONTROL,
            axum::http::HeaderValue::from_static("private, no-store"),
        );
    }

    response
}

// ── Bearer token parsing ───────────────────────────────────────────────────────

/// Extract the Bearer token from the `Authorization` request header.
///
/// Returns `Some(token)` if the header has the form `Bearer <token>` (case-insensitive
/// prefix, one or more spaces/tabs), or `None` if the header is missing, malformed,
/// or uses a different auth scheme.
pub(crate) fn extract_bearer_token(headers: &axum::http::HeaderMap) -> Option<String> {
    let auth_header = headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?;

    // Case-insensitive prefix match: "Bearer " or "bearer "
    let remainder = auth_header
        .strip_prefix("Bearer ")
        .or_else(|| auth_header.strip_prefix("bearer "))?;

    let token = remainder.trim();
    if token.is_empty() {
        None
    } else {
        Some(token.to_owned())
    }
}

// ── Cookie parsing ─────────────────────────────────────────────────────────────

/// Extract the session token from the `Cookie` request header.
///
/// Returns `Some(token)` if the `__Host-session` cookie is present and non-empty,
/// or `None` if the cookie is missing or empty.
pub(crate) fn extract_session_cookie(headers: &axum::http::HeaderMap) -> Option<String> {
    let cookie_header = headers.get(axum::http::header::COOKIE)?.to_str().ok()?;

    for pair in cookie_header.split(';') {
        let pair = pair.trim();
        if let Some((name, value)) = pair.split_once('=')
            && name.trim() == "__Host-session"
        {
            let token = value.trim();
            if !token.is_empty() {
                return Some(token.to_owned());
            }
        }
    }

    None
}

/// Returns `true` when `path` targets a JSON API endpoint that requires email
/// verification.
///
/// Paths starting with `/api/` are the programmatic (non-browser) surface —
/// ingest, search, tokens, etc. Web UI pages (HTML) are excluded so that
/// unverified users can still see verification prompts.
fn is_api_path(path: &str) -> bool {
    path.starts_with("/api/")
}

// ── Email verification middleware ────────────────────────────────────────────────

/// Axum middleware: require that the authenticated user's email has been verified.
///
/// Reads [`AuthUser`](crate::AuthUser) from request extensions (injected by
/// [`auth_middleware`]). Returns `403 Forbidden` when `email_verified` is `false`.
/// This middleware is intended to be layered **after** [`auth_middleware`] on
/// individual sensitive web routes (e.g. team invite, workspace deletion) that
/// need per-route email-verification gating.
///
/// # Extension requirement
///
/// This middleware **must** run after [`auth_middleware`] — if no `AuthUser` is
/// present in extensions, it returns `401 Unauthorized` (the caller is not
/// authenticated at all).
pub async fn require_email_verified_middleware(
    request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let verified = request
        .extensions()
        .get::<AuthUser>()
        .map(|u| u.email_verified)
        .unwrap_or(false);

    if !verified {
        return Err(StatusCode::FORBIDDEN);
    }

    Ok(next.run(request).await)
}

// ── Login brute-force rate limiter ───────────────────────────────────────────────

/// In-memory fixed-window rate limiter for login brute-force protection.
///
/// Tracks login attempts per client IP within a fixed time window. The attempt
/// cap is supplied at [`check`](Self::check) time (not stored) so it can be read
/// from configuration on every request — mirroring [`PlanRateLimiter`] and
/// honouring the hot-swappable-config rule.
struct LoginBruteForceLimiter {
    inner: Mutex<HashMap<IpAddr, Vec<Instant>>>,
    window: Duration,
}

impl LoginBruteForceLimiter {
    /// Create a new limiter with the given time window.
    fn new(window: Duration) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            window,
        }
    }

    /// Check whether `ip` is allowed to make another login attempt under
    /// `max_attempts` per window.
    ///
    /// Prunes expired entries for the IP, then checks whether the remaining
    /// count is below `max_attempts`. If allowed, records the attempt and
    /// returns `Ok(())`. If rate-limited, returns `Err(retry_after_secs)`
    /// where `retry_after_secs` is the suggested `Retry-After` header value.
    fn check(&self, ip: IpAddr, max_attempts: usize) -> Result<(), u64> {
        let mut map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        let cutoff = now - self.window;
        let attempts = map.entry(ip).or_default();
        attempts.retain(|t| *t > cutoff);
        if attempts.len() >= max_attempts {
            let oldest = attempts.iter().min().copied().unwrap_or(now);
            let elapsed = now.duration_since(oldest).as_secs();
            let retry = self.window.as_secs().saturating_sub(elapsed).max(1);
            Err(retry)
        } else {
            attempts.push(now);
            Ok(())
        }
    }
}

/// Resolve the per-IP login attempt cap **at call time** (hot-swappable, plan
/// CLAUDE.md): reads `KB_LOGIN_RATE_MAX_ATTEMPTS` fresh on every request,
/// defaulting to `5`. Trusted environments where many logins legitimately share
/// one source IP (CI/e2e behind a reverse proxy, an office NAT) raise it via env
/// without a rebuild; production keeps the strict default.
fn login_rate_max_attempts() -> usize {
    parse_login_rate_max_attempts(std::env::var("KB_LOGIN_RATE_MAX_ATTEMPTS").ok())
}

/// Pure parser for [`login_rate_max_attempts`]: a positive integer, else the
/// default of `5` (covers unset, non-numeric, and zero).
fn parse_login_rate_max_attempts(raw: Option<String>) -> usize {
    raw.and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(5)
}

/// Global login brute-force limiter (60-second window). The attempt cap is read
/// per request via [`login_rate_max_attempts`].
static LOGIN_BRUTE_FORCE_LIMITER: LazyLock<LoginBruteForceLimiter> =
    LazyLock::new(|| LoginBruteForceLimiter::new(Duration::from_secs(60)));

// ── Per-token rate limiter ─────────────────────────────────────────────────────

/// In-memory fixed-window rate limiter for per-API-token request throttling.
///
/// Tracks request counts per raw Bearer token within a configurable time window.
/// When a token exceeds `max_requests` within `window`, further requests receive
/// `429 Too Many Requests` with a `Retry-After` header.
struct TokenRateLimiter {
    inner: Mutex<HashMap<String, Vec<Instant>>>,
    max_requests: usize,
    window: Duration,
}

impl TokenRateLimiter {
    /// Create a new limiter with the given request cap and time window.
    fn new(max_requests: usize, window: Duration) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            max_requests,
            window,
        }
    }

    /// Check whether `token` is allowed to make another request.
    ///
    /// Prunes expired entries for the token, then checks whether the remaining
    /// count is below `max_requests`. If allowed, records the request and
    /// returns `Ok(())`. If rate-limited, returns `Err(retry_after_secs)`
    /// where `retry_after_secs` is the suggested `Retry-After` header value.
    fn check(&self, token: &str) -> Result<(), u64> {
        let mut map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        let cutoff = now - self.window;
        let attempts = map.entry(token.to_owned()).or_default();
        attempts.retain(|t| *t > cutoff);
        if attempts.len() >= self.max_requests {
            let oldest = attempts.iter().min().copied().unwrap_or(now);
            let elapsed = now.duration_since(oldest).as_secs();
            let retry = self.window.as_secs().saturating_sub(elapsed).max(1);
            Err(retry)
        } else {
            attempts.push(now);
            Ok(())
        }
    }

    /// Clear all tracked tokens (test-only).
    #[cfg(test)]
    fn reset(&self) {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }
}

/// Global per-token rate limiter: 100 requests per 60-second window per token.
static TOKEN_RATE_LIMITER: LazyLock<TokenRateLimiter> =
    LazyLock::new(|| TokenRateLimiter::new(100, Duration::from_secs(60)));

// ── Plan per-minute rate limiter ─────────────────────────────────────────────────

/// In-memory fixed-window rate limiter for per-plan per-minute request throttling.
///
/// Tracks request counts per tenant within a configurable time window. Different
/// tenants may have different caps (free = 5/min, pro = 30/min, team = 100/min),
/// so `max_requests` is passed at check time rather than fixed at construction.
struct PlanRateLimiter {
    inner: Mutex<HashMap<i64, Vec<Instant>>>,
    window: Duration,
}

impl PlanRateLimiter {
    /// Create a new limiter with the given window.
    fn new(window: Duration) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            window,
        }
    }

    /// Check whether `tenant_id` is allowed to make another request under the
    /// given per-minute cap.
    ///
    /// Prunes expired entries for that tenant, then checks whether the remaining
    /// count is below `max_requests`. If allowed, records the attempt and returns
    /// `Ok(())`. If rate-limited, returns `Err(retry_after_secs)`.
    fn check(&self, tenant_id: i64, max_requests: usize) -> Result<(), u64> {
        let mut map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        let cutoff = now - self.window;
        let attempts = map.entry(tenant_id).or_default();
        attempts.retain(|t| *t > cutoff);
        if attempts.len() >= max_requests {
            let oldest = attempts.iter().min().copied().unwrap_or(now);
            let elapsed = now.duration_since(oldest).as_secs();
            let retry = self.window.as_secs().saturating_sub(elapsed).max(1);
            Err(retry)
        } else {
            attempts.push(now);
            Ok(())
        }
    }

    /// Clear all tracked tenants (test-only).
    #[cfg(test)]
    fn reset(&self) {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }
}

/// Global plan rate limiter: enforces per-minute caps per tenant from plan
/// `features.rate_caps.per_minute`.
static PLAN_RATE_LIMITER: LazyLock<PlanRateLimiter> =
    LazyLock::new(|| PlanRateLimiter::new(Duration::from_secs(60)));

/// Build a 429 rate-limit response with a `Retry-After` header and the canonical
/// `{error, message}` JSON envelope.
///
/// Used by per-token, per-plan, and login rate limiters — the response format is
/// identical.
fn rate_limit_response(retry_after: u64) -> Response {
    let body = axum::body::Body::from(
        serde_json::json!({
            "error": "rate_limited",
            "message": format!("Too many requests. Please try again in {retry_after}s.")
        })
        .to_string(),
    );
    let mut response = Response::new(body);
    *response.status_mut() = StatusCode::TOO_MANY_REQUESTS;
    response.headers_mut().insert(
        "retry-after",
        axum::http::HeaderValue::from_str(&retry_after.to_string())
            .unwrap_or(axum::http::HeaderValue::from_static("60")),
    );
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/json"),
    );
    response
}

/// Axum middleware: rate-limit `POST /auth/login` requests to prevent brute-force.
///
/// Tracks attempts per client IP (read from `X-Forwarded-For`, `X-Real-IP`, or
/// defaulting to `127.0.0.1`). After [`login_rate_max_attempts`] attempts (env
/// `KB_LOGIN_RATE_MAX_ATTEMPTS`, default 5) within 60 seconds, returns
/// `429 Too Many Requests` with a `Retry-After` header and a JSON error body.
///
/// Requests to paths other than `POST /auth/login` pass through unchanged.
pub async fn login_rate_limit_middleware(
    request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    login_rate_limit_with(&LOGIN_BRUTE_FORCE_LIMITER, request, next).await
}

/// Core of [`login_rate_limit_middleware`], parameterized over the limiter
/// instance so it can be exercised against an isolated, per-caller limiter.
///
/// Production wires the process-global [`LOGIN_BRUTE_FORCE_LIMITER`]; tests pass
/// a freshly-constructed limiter so concurrent cases never share window state.
/// Behaviour is otherwise identical: non-`POST /auth/login` requests pass
/// through, and an over-cap client receives `429` with a `Retry-After` header.
async fn login_rate_limit_with(
    limiter: &LoginBruteForceLimiter,
    request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    if request.method() != axum::http::Method::POST || request.uri().path() != "/auth/login" {
        return Ok(next.run(request).await);
    }

    let ip = extract_client_ip(&request);
    match limiter.check(ip, login_rate_max_attempts()) {
        Ok(()) => Ok(next.run(request).await),
        Err(retry_after) => {
            kb_metrics::record_rate_limit_rejection("login");
            let body = axum::body::Body::from(format!(
                r#"{{"error":"rate_limited","message":"Too many login attempts. Please try again in {retry_after}s."}}"#
            ));
            let mut response = Response::new(body);
            *response.status_mut() = StatusCode::TOO_MANY_REQUESTS;
            response.headers_mut().insert(
                "retry-after",
                axum::http::HeaderValue::from_str(&retry_after.to_string())
                    .unwrap_or(axum::http::HeaderValue::from_static("60")),
            );
            Ok(response)
        }
    }
}

/// Extract the client's IP address from request headers.
///
/// Prefers `X-Forwarded-For` (first entry), then `X-Real-IP`, falling back to
/// `127.0.0.1` when no proxy headers are present (typical for local dev / tests).
///
/// # Security note
///
/// This function trusts the `X-Forwarded-For` and `X-Real-IP` headers as set by
/// the upstream reverse proxy. **The deployment MUST run behind a trusted reverse
/// proxy (Caddy, nginx) that strips externally-supplied `X-Forwarded-*` headers**
/// before forwarding requests to this application. Without this guarantee, an
/// attacker can spoof their source IP by injecting these headers, bypassing the
/// per-IP rate limiter. This is standard practice for any web application that
/// uses proxy-supplied addressing — the proxy is part of the trusted compute
/// boundary.
fn extract_client_ip(request: &Request<axum::body::Body>) -> IpAddr {
    if let Some(forwarded) = request
        .headers()
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        && let Some(first) = forwarded.split(',').next().map(str::trim)
        && let Ok(ip) = first.parse::<IpAddr>()
    {
        return ip;
    }
    if let Some(real_ip) = request
        .headers()
        .get("x-real-ip")
        .and_then(|v| v.to_str().ok())
        && let Ok(ip) = real_ip.parse::<IpAddr>()
    {
        return ip;
    }
    IpAddr::from([127, 0, 0, 1])
}

// ── Idempotency-key deduplication ────────────────────────────────────────────────

/// Idempotency-key response cache TTL: 24 hours.
const IDEMPOTENCY_TTL: Duration = Duration::from_secs(86400);

/// A cached response for an idempotency key.
#[derive(Clone)]
struct CachedIdempotentResponse {
    status: StatusCode,
    content_type: Option<String>,
    body: Vec<u8>,
    created_at: Instant,
}

/// In-memory idempotency-key store.
///
/// Maps `Idempotency-Key` header values to previously-seen responses so that
/// retrying the same ingest request returns the original result instead of
/// creating a duplicate document.
///
/// Entries expire after [`IDEMPOTENCY_TTL`] and are pruned on each access.
struct IdempotencyStore {
    inner: Mutex<HashMap<String, CachedIdempotentResponse>>,
    ttl: Duration,
}

impl IdempotencyStore {
    /// Create a new store with the given entry TTL.
    fn new(ttl: Duration) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            ttl,
        }
    }

    /// Look up a cached response by idempotency key.
    ///
    /// Prunes expired entries before the lookup. Returns `None` when the
    /// key is unknown or its entry has expired.
    fn get(&self, key: &str) -> Option<CachedIdempotentResponse> {
        let mut map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let cutoff = Instant::now() - self.ttl;
        map.retain(|_, v| v.created_at > cutoff);
        map.get(key).cloned()
    }

    /// Store a response for an idempotency key.
    fn store(&self, key: String, status: StatusCode, content_type: Option<String>, body: Vec<u8>) {
        let mut map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        map.insert(
            key,
            CachedIdempotentResponse {
                status,
                content_type,
                body,
                created_at: Instant::now(),
            },
        );
    }

    /// Number of entries currently in the store (test-only).
    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Clear all entries (test-only).
    #[cfg(test)]
    fn reset(&self) {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }
}

/// Global idempotency-key store with a [`IDEMPOTENCY_TTL`] entry lifetime.
static IDEMPOTENCY_STORE: LazyLock<IdempotencyStore> =
    LazyLock::new(|| IdempotencyStore::new(IDEMPOTENCY_TTL));

/// Extract the `Idempotency-Key` request header, returning `None` if absent
/// or empty.
///
/// Header name comparison is case-insensitive per HTTP/1.1.
fn extract_idempotency_key(headers: &axum::http::HeaderMap) -> Option<String> {
    headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(String::from)
}

/// Run the inner handler with idempotency-key deduplication for
/// `POST /api/ingest`.
///
/// On the first request with a given `Idempotency-Key`, the handler runs
/// normally and a successful (2xx) response is cached. On subsequent requests
/// with the same key, the cached response is returned without re-executing
/// the handler, preventing duplicate document creation.
///
/// Non-successful responses (4xx, 5xx) are **not** cached — the caller can
/// correct the request and retry with the same key.
///
/// Requests without an `Idempotency-Key` header or to paths other than
/// `POST /api/ingest` pass through unchanged.
async fn run_maybe_idempotent(
    request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    // Only apply idempotency to POST /api/ingest.
    if request.method() != Method::POST || request.uri().path() != "/api/ingest" {
        return Ok(next.run(request).await);
    }

    let key = match extract_idempotency_key(request.headers()) {
        Some(k) => k,
        None => return Ok(next.run(request).await),
    };

    // Scope the idempotency key to the authenticated user to prevent
    // cross-user response replay (IDOR). Without this, User A with
    // Idempotency-Key "abc" would receive User B's cached response for
    // the same key.
    let scoped_key = match request.extensions().get::<AuthUser>() {
        Some(u) => idempotency_key_for_user(u, &key),
        None => return Ok(next.run(request).await),
    };

    // Cache hit — return the original response without re-processing.
    if let Some(cached) = IDEMPOTENCY_STORE.get(&scoped_key) {
        let mut response = Response::new(axum::body::Body::from(cached.body));
        *response.status_mut() = cached.status;
        if let Some(ct) = &cached.content_type
            && let Ok(val) = axum::http::HeaderValue::from_str(ct)
        {
            response
                .headers_mut()
                .insert(axum::http::header::CONTENT_TYPE, val);
        }
        return Ok(response);
    }

    // Cache miss — run the handler.
    let response = next.run(request).await;

    // Only cache successful responses (2xx). Errors are not deduplicated so
    // the caller can fix the request and retry with the same key.
    if !response.status().is_success() {
        return Ok(response);
    }

    // Decompose the response for caching.
    let status = response.status();
    let content_type = response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    let (parts, body) = response.into_parts();
    let body_bytes = axum::body::to_bytes(body, 1024 * 1024)
        .await
        .unwrap_or_default();

    IDEMPOTENCY_STORE.store(scoped_key, status, content_type, body_bytes.to_vec());

    Ok(Response::from_parts(
        parts,
        axum::body::Body::from(body_bytes),
    ))
}

/// Build a user-scoped idempotency key from the authenticated user and the
/// caller-supplied `Idempotency-Key` header value.
///
/// This prevents cross-user response replay (IDOR): two different users
/// submitting the same `Idempotency-Key` value will receive their own
/// distinct cached responses because the keys are scoped to
/// `tenant_id:user_id`.
fn idempotency_key_for_user(user: &AuthUser, raw_key: &str) -> String {
    format!("{}:{}:{}", user.tenant_id, user.user_id, raw_key)
}

// ── HTTP RED metrics middleware (BUG-OBS-05) ─────────────────────────────────────

/// Returns `true` for endpoints excluded from HTTP RED-metric instrumentation.
///
/// The Prometheus scrape endpoint (`/metrics`) is excluded so a scrape never
/// counts itself — self-counting would also make `/metrics` output depend on how
/// many times it had been scraped, breaking byte-stable snapshots. The liveness
/// (`/live`) and readiness (`/health`) probes are excluded as high-frequency,
/// low-signal noise that would dominate the per-route series.
fn is_metrics_excluded_path(path: &str) -> bool {
    matches!(path, "/metrics" | "/health" | "/live")
}

/// Axum middleware: record per-route HTTP RED metrics (rate, errors, duration).
///
/// Times the inner stack, then records [`kb_metrics::record_http_request`]
/// labelled by method, matched-route path, and response status. The route label
/// is taken from the [`MatchedPath`] request extension so dynamic segments
/// (e.g. document ids) collapse to their template (`/api/documents/{id}`) and do
/// not explode label cardinality; requests that match no route (404s) are
/// bucketed under `path="unmatched"`. Scrape and probe endpoints are excluded
/// via [`is_metrics_excluded_path`].
pub async fn http_metrics_middleware(request: Request<axum::body::Body>, next: Next) -> Response {
    let method = request.method().as_str().to_owned();
    let matched = request
        .extensions()
        .get::<MatchedPath>()
        .map(|m| m.as_str().to_owned());
    let raw_path = request.uri().path().to_owned();

    let start = Instant::now();
    let response = next.run(request).await;
    let elapsed = start.elapsed().as_secs_f64();

    // Exclude scrape/probe endpoints — match on the route template when known,
    // otherwise the raw path (so `/metrics` is excluded even if unmatched).
    let excluded_key = matched.as_deref().unwrap_or(raw_path.as_str());
    if !is_metrics_excluded_path(excluded_key) {
        let label_path = matched.as_deref().unwrap_or("unmatched");
        kb_metrics::record_http_request(&method, label_path, response.status().as_u16(), elapsed);
    }

    response
}

// ── CORS layer ─────────────────────────────────────────────────────────────────

/// Build a Cross-Origin Resource Sharing (CORS) layer for browser-based API
/// consumers.
///
/// The layer allows any origin ([`Any`]) but does **not** allow credentials.
/// This is a safe default for a local-first application: browsers can send
/// cross-origin requests to the API, but the dangerous `*` + `credentials`
/// combination is prohibited (browsers refuse to send cookies with a wildcard
/// origin).
///
/// Preflight (`OPTIONS`) requests are handled automatically — the layer
/// intercepts them before they reach the auth middleware, so unauthenticated
/// preflights succeed.
///
/// # Security
///
/// The `Access-Control-Allow-Origin: *` header is set on responses for
/// non-credentialed requests. Cookie-based and Bearer-based authentication
/// still gate access to protected routes — CORS only governs whether a
/// *browser* may issue the request, not whether the server will fulfil it.
pub fn cors_layer() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any)
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::Arc;
    use std::time::Duration;

    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::get;
    use kb_core::api_token::DEFAULT_API_TOKEN_TTL_SECS;
    use kb_core::session::{DEFAULT_SESSION_TTL_SECS, SessionStore};
    use kb_core::user::UserRole;
    use kb_store::session_store::InMemorySessionStore;
    use tower::ServiceExt;

    use super::*;
    use crate::AppState;

    /// Helper: build a test state with InMemorySessionStore and a self-contained mock PgStore
    /// that will never be called (the middleware only touches the session store).
    fn test_state() -> Arc<AppState> {
        let session_store: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
        let pg_store = Arc::new(kb_store::PgStore::new(
            "postgres://localhost/test?sslmode=disable",
        ));
        Arc::new(AppState::new(
            session_store,
            pg_store,
            Some(Duration::from_secs(DEFAULT_SESSION_TTL_SECS)),
            false, // secure_cookies = false for tests
        ))
    }

    /// Helper: create a valid session and return its token.
    async fn create_session(state: &AppState, role: UserRole) -> String {
        state
            .session_store
            .create(1, 42, role, true, Duration::from_secs(3600))
            .await
            .unwrap()
    }

    /// Build a request with the session cookie set.
    fn request_with_cookie(token: &str) -> Request<Body> {
        Request::builder()
            .uri("/protected")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::empty())
            .unwrap()
    }

    /// Build a request without any cookies.
    fn request_without_cookie() -> Request<Body> {
        Request::builder()
            .uri("/protected")
            .body(Body::empty())
            .unwrap()
    }

    // ── extract_session_cookie ─────────────────────────────────────────────

    #[test]
    fn parse_valid_cookie() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::COOKIE,
            axum::http::HeaderValue::from_static("__Host-session=abc123def456"),
        );
        let token = extract_session_cookie(&headers);
        assert_eq!(token.as_deref(), Some("abc123def456"));
    }

    #[test]
    fn parse_cookie_with_multiple_values() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::COOKIE,
            axum::http::HeaderValue::from_static("other=value; __Host-session=mytoken; x=y"),
        );
        let token = extract_session_cookie(&headers);
        assert_eq!(token.as_deref(), Some("mytoken"));
    }

    #[test]
    fn parse_missing_cookie_returns_none() {
        let headers = axum::http::HeaderMap::new();
        assert_eq!(extract_session_cookie(&headers), None);
    }

    #[test]
    fn parse_empty_cookie_value_returns_none() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::COOKIE,
            axum::http::HeaderValue::from_static("__Host-session="),
        );
        // Empty value → no token.
        assert_eq!(extract_session_cookie(&headers), None);
    }

    #[test]
    fn parse_malformed_cookie_header() {
        // Non-UTF-8 cookie headers are rejected by HeaderValue, so we test
        // with a header that lacks the session cookie entirely.
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::COOKIE,
            axum::http::HeaderValue::from_static("other_cookie=somevalue"),
        );
        assert_eq!(extract_session_cookie(&headers), None);
    }

    #[test]
    fn parse_cookie_with_surrounding_whitespace() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::COOKIE,
            axum::http::HeaderValue::from_static("  __Host-session = spaced-token  "),
        );
        let token = extract_session_cookie(&headers);
        assert_eq!(token.as_deref(), Some("spaced-token"));
    }

    // ── Middleware integration tests ───────────────────────────────────────

    /// Build a minimal test app: a protected route behind the middleware that
    /// returns the AuthUser details as a string.
    async fn protected_handler(axum::Extension(user): axum::Extension<AuthUser>) -> String {
        format!(
            "{}:{}:{}:{}",
            user.tenant_id,
            user.user_id,
            user.role.as_str(),
            user.email_verified
        )
    }

    fn test_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/protected", get(protected_handler))
            .route("/api/ingest", get(protected_handler))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ))
            .with_state(state)
    }

    #[tokio::test]
    async fn valid_session_passes_through() {
        let state = test_state();
        let token = create_session(&state, UserRole::Admin).await;
        let router = test_router(state);

        let response = router.oneshot(request_with_cookie(&token)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert_eq!(std::str::from_utf8(&body).unwrap(), "1:42:admin:true");
    }

    #[tokio::test]
    async fn missing_cookie_returns_401() {
        let state = test_state();
        let router = test_router(state);

        let response = router.oneshot(request_without_cookie()).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn invalid_token_returns_401() {
        let state = test_state();
        let router = test_router(state);

        let response = router
            .oneshot(request_with_cookie("nonexistent-token-00000000000000"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn expired_session_returns_401() {
        let state = test_state();
        // Create a session with a very short TTL.
        let token = state
            .session_store
            .create(1, 99, UserRole::Member, true, Duration::from_millis(1))
            .await
            .unwrap();
        // Wait for the session to expire.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let router = test_router(state);
        let response = router.oneshot(request_with_cookie(&token)).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn middleware_injects_correct_user_role() {
        let state = test_state();

        // Admin session.
        let t_admin = create_session(&state, UserRole::Admin).await;
        let r1 = test_router(state.clone())
            .oneshot(request_with_cookie(&t_admin))
            .await
            .unwrap();
        assert_eq!(r1.status(), StatusCode::OK);
        let body = axum::body::to_bytes(r1.into_body(), 1024).await.unwrap();
        assert_eq!(std::str::from_utf8(&body).unwrap(), "1:42:admin:true");

        // Owner session.
        let t_owner = create_session(&state, UserRole::Owner).await;
        let r2 = test_router(state.clone())
            .oneshot(request_with_cookie(&t_owner))
            .await
            .unwrap();
        assert_eq!(r2.status(), StatusCode::OK);
        let body = axum::body::to_bytes(r2.into_body(), 1024).await.unwrap();
        assert_eq!(std::str::from_utf8(&body).unwrap(), "1:42:owner:true");
    }

    #[tokio::test]
    async fn middleware_slides_expiry() {
        let state = test_state();
        let token = state
            .session_store
            .create(1, 1, UserRole::Member, true, Duration::from_millis(200))
            .await
            .unwrap();

        // First request: should succeed.
        let r1 = test_router(state.clone())
            .oneshot(request_with_cookie(&token))
            .await
            .unwrap();
        assert_eq!(r1.status(), StatusCode::OK);

        // Wait until past the original TTL but within the slid TTL.
        tokio::time::sleep(Duration::from_millis(250)).await;

        // Second request: should still succeed because the first request slid the expiry.
        // Note: the slide extends to state.session_ttl (24h by default), so it will
        // definitely still be valid.
        let r2 = test_router(state.clone())
            .oneshot(request_with_cookie(&token))
            .await
            .unwrap();
        assert_eq!(r2.status(), StatusCode::OK);
    }

    // ── extract_bearer_token ───────────────────────────────────────────────

    #[test]
    fn parse_valid_bearer_token() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            axum::http::HeaderValue::from_static("Bearer my-api-token-abc123"),
        );
        let token = extract_bearer_token(&headers);
        assert_eq!(token.as_deref(), Some("my-api-token-abc123"));
    }

    #[test]
    fn parse_bearer_case_insensitive_prefix() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            axum::http::HeaderValue::from_static("bearer lowercase-token"),
        );
        let token = extract_bearer_token(&headers);
        assert_eq!(token.as_deref(), Some("lowercase-token"));
    }

    #[test]
    fn parse_bearer_with_extra_whitespace() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            axum::http::HeaderValue::from_static("Bearer   extra-spaces-token  "),
        );
        let token = extract_bearer_token(&headers);
        assert_eq!(token.as_deref(), Some("extra-spaces-token"));
    }

    #[test]
    fn parse_missing_auth_header_returns_none() {
        let headers = axum::http::HeaderMap::new();
        assert_eq!(extract_bearer_token(&headers), None);
    }

    #[test]
    fn parse_non_bearer_scheme_returns_none() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            axum::http::HeaderValue::from_static("Basic dXNlcjpwYXNz"),
        );
        assert_eq!(extract_bearer_token(&headers), None);
    }

    #[test]
    fn parse_empty_bearer_token_returns_none() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            axum::http::HeaderValue::from_static("Bearer "),
        );
        assert_eq!(extract_bearer_token(&headers), None);
    }

    // ── Bearer-auth middleware integration tests ────────────────────────────

    /// Build a test state that also has an API token store configured.
    fn test_state_with_api_tokens() -> Arc<AppState> {
        let session_store: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
        let pg_store = Arc::new(kb_store::PgStore::new(
            "postgres://localhost/test?sslmode=disable",
        ));
        let token_store: Arc<dyn kb_core::api_token::ApiTokenStore> =
            Arc::new(kb_store::api_token_store::InMemoryApiTokenStore::new());
        let mut state = AppState::new(
            session_store,
            pg_store,
            Some(Duration::from_secs(DEFAULT_SESSION_TTL_SECS)),
            false,
        );
        state.api_token_store = Some(token_store);
        Arc::new(state)
    }

    /// Build a request with a Bearer authorization header.
    fn request_with_bearer(token: &str) -> Request<Body> {
        Request::builder()
            .uri("/protected")
            .header("Authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn valid_bearer_token_passes_through() {
        let state = test_state_with_api_tokens();

        // Create an API token via the store.
        let result = state
            .api_token_store
            .as_ref()
            .unwrap()
            .create_token(
                1,
                42,
                UserRole::Admin,
                true,
                "test-token",
                Duration::from_secs(DEFAULT_API_TOKEN_TTL_SECS),
            )
            .await
            .unwrap();

        let router = test_router(state);
        let response = router
            .oneshot(request_with_bearer(&result.raw_token))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert_eq!(std::str::from_utf8(&body).unwrap(), "1:42:admin:true");
    }

    #[tokio::test]
    async fn invalid_bearer_token_returns_401() {
        let state = test_state_with_api_tokens();
        let router = test_router(state);

        let response = router
            .oneshot(request_with_bearer("invalid-token-000000000000000000"))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn bearer_token_without_store_configured_returns_401() {
        // When an Authorization header is present but api_token_store is None,
        // the middleware returns 401 (doesn't fall through to cookies).
        let state = test_state(); // no api_token_store
        let router = test_router(state);

        let response = router
            .oneshot(request_with_bearer("some-token"))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn revoked_bearer_token_returns_401() {
        let state = test_state_with_api_tokens();

        let result = state
            .api_token_store
            .as_ref()
            .unwrap()
            .create_token(
                1,
                42,
                UserRole::Member,
                false,
                "will-be-revoked",
                Duration::from_secs(DEFAULT_API_TOKEN_TTL_SECS),
            )
            .await
            .unwrap();

        // Revoke it.
        state
            .api_token_store
            .as_ref()
            .unwrap()
            .revoke_token(1, result.id)
            .await
            .unwrap();

        let router = test_router(state);
        let response = router
            .oneshot(request_with_bearer(&result.raw_token))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn expired_bearer_token_returns_401() {
        let state = test_state_with_api_tokens();

        let result = state
            .api_token_store
            .as_ref()
            .unwrap()
            .create_token(
                1,
                42,
                UserRole::Admin,
                true,
                "short-lived",
                Duration::from_millis(1),
            )
            .await
            .unwrap();

        // Wait for expiration.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let router = test_router(state);
        let response = router
            .oneshot(request_with_bearer(&result.raw_token))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn bearer_token_inherits_user_role() {
        let state = test_state_with_api_tokens();

        // Token with Owner role.
        let result = state
            .api_token_store
            .as_ref()
            .unwrap()
            .create_token(
                1,
                99,
                UserRole::Owner,
                true,
                "owner-token",
                Duration::from_secs(DEFAULT_API_TOKEN_TTL_SECS),
            )
            .await
            .unwrap();

        let router = test_router(state);
        let response = router
            .oneshot(request_with_bearer(&result.raw_token))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert_eq!(std::str::from_utf8(&body).unwrap(), "1:99:owner:true");
    }

    // ── LoginBruteForceLimiter unit tests ─────────────────────────────────

    #[test]
    fn limiter_allows_under_max_attempts() {
        let limiter = LoginBruteForceLimiter::new(Duration::from_secs(60));
        let ip = IpAddr::from([10, 0, 0, 1]);
        for _ in 0..3 {
            assert!(limiter.check(ip, 3).is_ok());
        }
    }

    #[test]
    fn limiter_blocks_after_max_attempts() {
        let limiter = LoginBruteForceLimiter::new(Duration::from_secs(60));
        let ip = IpAddr::from([10, 0, 0, 2]);
        assert!(limiter.check(ip, 2).is_ok());
        assert!(limiter.check(ip, 2).is_ok());
        // Third attempt — rate-limited.
        let err = limiter.check(ip, 2).unwrap_err();
        assert!(err > 0, "retry-after must be positive, got {err}");
    }

    #[test]
    fn limiter_tracks_different_ips_independently() {
        let limiter = LoginBruteForceLimiter::new(Duration::from_secs(60));
        let ip_a = IpAddr::from([10, 0, 0, 1]);
        let ip_b = IpAddr::from([10, 0, 0, 2]);
        // Exhaust ip_a.
        assert!(limiter.check(ip_a, 1).is_ok());
        assert!(limiter.check(ip_a, 1).is_err());
        // ip_b still has attempts.
        assert!(limiter.check(ip_b, 1).is_ok());
    }

    #[test]
    fn limiter_window_expires() {
        let limiter = LoginBruteForceLimiter::new(Duration::from_millis(10));
        let ip = IpAddr::from([10, 0, 0, 3]);
        assert!(limiter.check(ip, 2).is_ok());
        assert!(limiter.check(ip, 2).is_ok());
        assert!(limiter.check(ip, 2).is_err());
        // Wait for the window to expire.
        std::thread::sleep(Duration::from_millis(15));
        assert!(limiter.check(ip, 2).is_ok());
    }

    #[test]
    fn limiter_returns_reasonable_retry_after() {
        let limiter = LoginBruteForceLimiter::new(Duration::from_secs(60));
        let ip = IpAddr::from([10, 0, 0, 4]);
        assert!(limiter.check(ip, 1).is_ok());
        let err = limiter.check(ip, 1).unwrap_err();
        // Retry-After must be in [1, 60] range.
        assert!(err >= 1, "retry-after too small: {err}");
        assert!(err <= 60, "retry-after too large: {err}");
    }

    #[test]
    fn parse_login_rate_max_attempts_values() {
        // Unset / non-numeric / zero all fall back to the strict default of 5.
        assert_eq!(parse_login_rate_max_attempts(None), 5);
        assert_eq!(parse_login_rate_max_attempts(Some("abc".into())), 5);
        assert_eq!(parse_login_rate_max_attempts(Some("0".into())), 5);
        // A positive override is honoured (e.g. e2e / trusted-proxy environments).
        assert_eq!(
            parse_login_rate_max_attempts(Some("100000".into())),
            100_000
        );
        assert_eq!(parse_login_rate_max_attempts(Some("5".into())), 5);
    }

    // ── TokenRateLimiter unit tests ──────────────────────────────────────

    #[test]
    fn token_limiter_allows_under_max_requests() {
        let limiter = TokenRateLimiter::new(3, Duration::from_secs(60));
        for _ in 0..3 {
            assert!(limiter.check("tok-abc").is_ok());
        }
    }

    #[test]
    fn token_limiter_blocks_after_max_requests() {
        let limiter = TokenRateLimiter::new(2, Duration::from_secs(60));
        assert!(limiter.check("tok-xyz").is_ok());
        assert!(limiter.check("tok-xyz").is_ok());
        let err = limiter.check("tok-xyz").unwrap_err();
        assert!(err > 0, "retry-after must be positive, got {err}");
    }

    #[test]
    fn token_limiter_tracks_different_tokens_independently() {
        let limiter = TokenRateLimiter::new(1, Duration::from_secs(60));
        assert!(limiter.check("token-a").is_ok());
        assert!(limiter.check("token-a").is_err());
        // token-b still has attempts.
        assert!(limiter.check("token-b").is_ok());
    }

    #[test]
    fn token_limiter_window_expires() {
        let limiter = TokenRateLimiter::new(2, Duration::from_millis(10));
        assert!(limiter.check("tok-1").is_ok());
        assert!(limiter.check("tok-1").is_ok());
        assert!(limiter.check("tok-1").is_err());
        std::thread::sleep(Duration::from_millis(15));
        assert!(limiter.check("tok-1").is_ok());
    }

    #[test]
    fn token_limiter_returns_reasonable_retry_after() {
        let limiter = TokenRateLimiter::new(1, Duration::from_secs(60));
        assert!(limiter.check("tok-r").is_ok());
        let err = limiter.check("tok-r").unwrap_err();
        assert!(err >= 1, "retry-after too small: {err}");
        assert!(err <= 60, "retry-after too large: {err}");
    }

    #[test]
    fn token_limiter_reset_clears_all() {
        let limiter = TokenRateLimiter::new(2, Duration::from_secs(60));
        assert!(limiter.check("a").is_ok());
        assert!(limiter.check("b").is_ok());
        limiter.reset();
        // After reset, both tokens are allowed again.
        assert!(limiter.check("a").is_ok());
        assert!(limiter.check("b").is_ok());
    }

    // ── PlanRateLimiter unit tests ───────────────────────────────────────

    #[test]
    fn plan_limiter_allows_under_cap() {
        let limiter = PlanRateLimiter::new(Duration::from_secs(60));
        for _ in 0..3 {
            assert!(limiter.check(1, 5).is_ok());
        }
    }

    #[test]
    fn plan_limiter_blocks_after_cap() {
        let limiter = PlanRateLimiter::new(Duration::from_secs(60));
        for _ in 0..5 {
            assert!(limiter.check(2, 5).is_ok());
        }
        let err = limiter.check(2, 5).unwrap_err();
        assert!(err > 0, "retry-after must be positive, got {err}");
    }

    #[test]
    fn plan_limiter_tracks_tenants_independently() {
        let limiter = PlanRateLimiter::new(Duration::from_secs(60));
        // Exhaust tenant 1's cap (1/min).
        assert!(limiter.check(1, 1).is_ok());
        assert!(limiter.check(1, 1).is_err());
        // Tenant 2 still has attempts at a higher cap.
        for _ in 0..3 {
            assert!(limiter.check(2, 5).is_ok());
        }
    }

    #[test]
    fn plan_limiter_window_expires() {
        let limiter = PlanRateLimiter::new(Duration::from_millis(10));
        for _ in 0..2 {
            assert!(limiter.check(3, 2).is_ok());
        }
        assert!(limiter.check(3, 2).is_err());
        std::thread::sleep(Duration::from_millis(15));
        assert!(limiter.check(3, 2).is_ok());
    }

    #[test]
    fn plan_limiter_returns_reasonable_retry_after() {
        let limiter = PlanRateLimiter::new(Duration::from_secs(60));
        assert!(limiter.check(4, 1).is_ok());
        let err = limiter.check(4, 1).unwrap_err();
        assert!(err >= 1, "retry-after too small: {err}");
        assert!(err <= 60, "retry-after too large: {err}");
    }

    #[test]
    fn plan_limiter_zero_cap_always_blocks() {
        let limiter = PlanRateLimiter::new(Duration::from_secs(60));
        // Zero cap means no requests allowed — first attempt is blocked.
        assert!(limiter.check(5, 0).is_err());
    }

    #[test]
    fn plan_limiter_reset_clears_all() {
        let limiter = PlanRateLimiter::new(Duration::from_secs(60));
        assert!(limiter.check(10, 1).is_ok());
        assert!(limiter.check(20, 1).is_ok());
        limiter.reset();
        assert!(limiter.check(10, 1).is_ok());
        assert!(limiter.check(20, 1).is_ok());
    }

    // ── extract_client_ip ────────────────────────────────────────────────

    #[test]
    fn extract_ip_from_x_forwarded_for() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            axum::http::HeaderValue::from_static("192.168.1.100, 10.0.0.1"),
        );
        let req = Request::builder()
            .uri("/auth/login")
            .body(axum::body::Body::empty())
            .unwrap();
        // Manually set headers since builder doesn't support custom headers easily.
        let (mut parts, body) = req.into_parts();
        parts.headers = headers;
        let req = Request::from_parts(parts, body);
        let ip = extract_client_ip(&req);
        assert_eq!(ip, IpAddr::from([192, 168, 1, 100]));
    }

    #[test]
    fn extract_ip_from_x_real_ip() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "x-real-ip",
            axum::http::HeaderValue::from_static("10.20.30.40"),
        );
        let (mut parts, body) = Request::builder()
            .uri("/auth/login")
            .body(axum::body::Body::empty())
            .unwrap()
            .into_parts();
        parts.headers = headers;
        let req = Request::from_parts(parts, body);
        let ip = extract_client_ip(&req);
        assert_eq!(ip, IpAddr::from([10, 20, 30, 40]));
    }

    #[test]
    fn extract_ip_falls_back_to_localhost() {
        let req = Request::builder()
            .uri("/auth/login")
            .body(axum::body::Body::empty())
            .unwrap();
        let ip = extract_client_ip(&req);
        assert_eq!(ip, IpAddr::from([127, 0, 0, 1]));
    }

    #[test]
    fn extract_ip_x_forwarded_for_priority_over_x_real_ip() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            axum::http::HeaderValue::from_static("1.2.3.4"),
        );
        headers.insert("x-real-ip", axum::http::HeaderValue::from_static("5.6.7.8"));
        let (mut parts, body) = Request::builder()
            .uri("/auth/login")
            .body(axum::body::Body::empty())
            .unwrap()
            .into_parts();
        parts.headers = headers;
        let req = Request::from_parts(parts, body);
        let ip = extract_client_ip(&req);
        // X-Forwarded-For takes precedence.
        assert_eq!(ip, IpAddr::from([1, 2, 3, 4]));
    }

    // ── login_rate_limit_middleware integration tests ────────────────────

    /// Build a minimal test router with the login rate limiter applied to
    /// a /auth/login endpoint. The handler returns 200 OK for any POST.
    ///
    /// Each call wires a **fresh, router-local** [`LoginBruteForceLimiter`]
    /// rather than the process-global one. This is what makes the login-route
    /// tests deterministic under parallel execution: no shared window state and
    /// no cross-test `reset()` can clobber another test's accumulated attempts,
    /// so distinct-IP isolation actually holds.
    fn rate_limit_test_router() -> Router {
        let limiter = Arc::new(LoginBruteForceLimiter::new(Duration::from_secs(60)));
        Router::new()
            .route(
                "/auth/login",
                axum::routing::post(|| async { StatusCode::OK }),
            )
            .layer(axum::middleware::from_fn(
                move |req: Request<Body>, next: Next| {
                    let limiter = Arc::clone(&limiter);
                    async move { login_rate_limit_with(&limiter, req, next).await }
                },
            ))
    }

    #[tokio::test]
    async fn middleware_passes_non_login_paths_through() {
        // Router-local limiter (never reaches `.check()` for a non-login path);
        // using a fresh instance avoids touching the process-global limiter, so
        // this test cannot clobber another login-route test's window state.
        let limiter = Arc::new(LoginBruteForceLimiter::new(Duration::from_secs(60)));
        let router = Router::new()
            .route("/other", axum::routing::post(|| async { StatusCode::OK }))
            .layer(axum::middleware::from_fn(
                move |req: Request<Body>, next: Next| {
                    let limiter = Arc::clone(&limiter);
                    async move { login_rate_limit_with(&limiter, req, next).await }
                },
            ));

        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/other")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn middleware_rate_limits_login_route() {
        // `rate_limit_test_router` wires a router-local limiter, so this test's
        // window state is fully isolated from every other test regardless of the
        // source IP or parallel scheduling.
        let router = rate_limit_test_router();
        let mut statuses = Vec::new();

        for _ in 0..10 {
            let response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/auth/login")
                        .header("x-forwarded-for", "198.51.100.10")
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            statuses.push(response.status());
        }

        // First 5 should pass (200 OK), subsequent should be 429.
        let early_oks = statuses
            .iter()
            .take(5)
            .filter(|s| **s == StatusCode::OK)
            .count();
        assert_eq!(
            early_oks, 5,
            "first 5 attempts should be allowed, got: {statuses:?}"
        );
        assert!(
            statuses.contains(&StatusCode::TOO_MANY_REQUESTS),
            "should contain at least one 429, got: {statuses:?}",
        );
    }

    /// Parse the integer value of the `kb_rate_limit_rejections_total{kind="…"}`
    /// counter from a Prometheus exposition document, returning 0 when the series
    /// has no data line yet. Lets the login-limiter test take a before/after delta
    /// of the process-global counter without racing other tests.
    fn rate_limit_rejection_count(text: &str, kind: &str) -> u64 {
        let needle = format!("kb_rate_limit_rejections_total{{kind=\"{kind}\"}} ");
        text.lines()
            .find_map(|l| l.trim_start().strip_prefix(&needle))
            .and_then(|v| v.trim().parse::<f64>().ok())
            .map(|v| v as u64)
            .unwrap_or(0)
    }

    #[tokio::test]
    async fn login_limiter_429_increments_rate_limit_rejections_counter() {
        // P14-T10: the login brute-force 429 path must bump
        // kb_rate_limit_rejections_total{kind="login"}. The router-local limiter
        // keeps the 429 count deterministic; the counter itself is process-global,
        // so we assert it rises by at least the number of 429s we observe.
        let _ = kb_metrics::init_metrics();
        let before = rate_limit_rejection_count(&kb_metrics::render(), "login");

        let router = rate_limit_test_router();
        let mut rejections = 0u64;
        for _ in 0..10 {
            let response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/auth/login")
                        .header("x-forwarded-for", "198.51.100.42")
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            if response.status() == StatusCode::TOO_MANY_REQUESTS {
                rejections += 1;
            }
        }
        assert!(rejections >= 1, "test must produce at least one login 429");

        let after = rate_limit_rejection_count(&kb_metrics::render(), "login");
        assert!(
            after >= before + rejections,
            "login rejection counter must rise by >= the {rejections} 429(s): {before} -> {after}"
        );
    }

    // ── is_api_path unit tests ────────────────────────────────────────────

    #[test]
    fn is_api_path_matches_api_prefix() {
        assert!(is_api_path("/api/ingest"));
        assert!(is_api_path("/api/search"));
        assert!(is_api_path("/api/tokens"));
        assert!(is_api_path("/api/documents/42"));
        assert!(is_api_path("/api/jobs/abc-123"));
    }

    #[test]
    fn is_api_path_rejects_non_api_paths() {
        assert!(!is_api_path("/login"));
        assert!(!is_api_path("/search"));
        assert!(!is_api_path("/upload"));
        assert!(!is_api_path("/auth/login"));
        assert!(!is_api_path("/account/team/invite"));
        assert!(!is_api_path("/admin"));
        assert!(!is_api_path("/"));
    }

    // ── Email verification gate in auth_middleware ────────────────────────

    /// Build a request to an API path with the session cookie set.
    fn api_request_with_cookie(token: &str) -> Request<Body> {
        Request::builder()
            .uri("/api/ingest")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn unverified_user_blocked_on_api_route() {
        let state = test_state();
        // Create a session with email_verified = false.
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, false, Duration::from_secs(3600))
            .await
            .unwrap();

        let router = test_router(state);
        let response = router
            .oneshot(api_request_with_cookie(&token))
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::FORBIDDEN,
            "unverified user must be 403 Forbidden on /api/* routes"
        );
    }

    #[tokio::test]
    async fn verified_user_passes_api_route() {
        let state = test_state();
        // Create a session with email_verified = true.
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();

        let router = test_router(state);
        let response = router
            .oneshot(api_request_with_cookie(&token))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn unverified_user_passes_non_api_web_route() {
        let state = test_state();
        // Create a session with email_verified = false targeting a non-API path.
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, false, Duration::from_secs(3600))
            .await
            .unwrap();

        let router = test_router(state);
        // Use the default /protected path (NOT /api/*).
        let response = router.oneshot(request_with_cookie(&token)).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "unverified user must still be able to access non-API routes (web UI)"
        );
    }

    // ── require_email_verified_middleware tests ───────────────────────────

    /// Handler for the require-email-verified middleware tests.
    async fn require_verified_handler(axum::Extension(user): axum::Extension<AuthUser>) -> String {
        format!("verified:{}", user.email_verified)
    }

    #[tokio::test]
    async fn require_verified_allows_verified_user() {
        let state = test_state();
        let user = AuthUser {
            tenant_id: 1,
            user_id: 42,
            role: UserRole::Member,
            email_verified: true,
        };
        let router = Router::new()
            .route("/sensitive", get(require_verified_handler))
            .layer(axum::middleware::from_fn(require_email_verified_middleware))
            .layer(axum::middleware::from_fn(
                move |mut req: Request<Body>, next: Next| {
                    let u = user;
                    async move {
                        req.extensions_mut().insert(u);
                        let resp: Response = next.run(req).await;
                        Ok::<_, StatusCode>(resp)
                    }
                },
            ))
            .with_state(state);

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/sensitive")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert_eq!(std::str::from_utf8(&body).unwrap(), "verified:true");
    }

    #[tokio::test]
    async fn require_verified_blocks_unverified_user() {
        let state = test_state();
        let user = AuthUser {
            tenant_id: 1,
            user_id: 42,
            role: UserRole::Member,
            email_verified: false,
        };
        let router = Router::new()
            .route("/sensitive", get(require_verified_handler))
            .layer(axum::middleware::from_fn(require_email_verified_middleware))
            .layer(axum::middleware::from_fn(
                move |mut req: Request<Body>, next: Next| {
                    let u = user;
                    async move {
                        req.extensions_mut().insert(u);
                        let resp: Response = next.run(req).await;
                        Ok::<_, StatusCode>(resp)
                    }
                },
            ))
            .with_state(state);

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/sensitive")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn require_verified_no_authuser_returns_403() {
        let state = test_state();
        // No auth-user-injecting middleware — AuthUser is absent from extensions.
        let router = Router::new()
            .route("/sensitive", get(|| async { "ok" }))
            .layer(axum::middleware::from_fn(require_email_verified_middleware))
            .with_state(state);

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/sensitive")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // When no AuthUser is present, email_verified defaults to false → 403.
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn middleware_includes_retry_after_header() {
        // Router-local limiter (see `rate_limit_test_router`): the exhaust loop
        // and the over-cap assertion below share isolated state that no sibling
        // test can clear mid-run, so the final 429 is deterministic.
        let router = rate_limit_test_router();

        // Exhaust the limit (default cap 5).
        for _ in 0..5 {
            let _ = router
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/auth/login")
                        .header("x-forwarded-for", "198.51.100.20")
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await;
        }

        // This request should be rate-limited.
        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/auth/login")
                    .header("x-forwarded-for", "198.51.100.20")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(
            response.headers().get("retry-after").is_some(),
            "429 response must include Retry-After header"
        );
    }

    // ── auth_error_response unit tests ─────────────────────────────────

    #[tokio::test]
    async fn auth_error_response_has_correct_status_and_content_type() {
        let resp = auth_error_response(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Authentication required",
        );
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            resp.headers()
                .get(axum::http::header::CONTENT_TYPE)
                .unwrap()
                .to_str()
                .unwrap(),
            "application/json"
        );
    }

    #[tokio::test]
    async fn auth_error_response_body_has_error_and_message_keys() {
        let resp = auth_error_response(
            StatusCode::FORBIDDEN,
            "email_not_verified",
            "Email verification required",
        );
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        let body_bytes = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(json["error"], "email_not_verified");
        assert_eq!(json["message"], "Email verification required");
        // Only the two canonical keys are present.
        assert_eq!(json.as_object().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn auth_error_response_custom_status_has_correct_body() {
        let resp = auth_error_response(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Invalid or expired API token",
        );
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        let body_bytes = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(json["error"], "unauthorized");
        assert_eq!(json["message"], "Invalid or expired API token");
    }

    /// Integration: the middleware 401 response has a JSON {error, message} body,
    /// not an empty body — the /api/* error envelope contract.
    #[tokio::test]
    async fn middleware_401_has_json_error_envelope() {
        let state = test_state();
        let router = test_router(state);

        let response = router.oneshot(request_without_cookie()).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"], "unauthorized");
        assert_eq!(json["message"], "Authentication required");
        assert!(
            json.as_object().unwrap().len() == 2,
            "error envelope must only contain 'error' and 'message' keys"
        );
    }

    // ── add_private_cache_control_if_html unit tests ──────────────────────

    /// Build a bare response carrying the given `Content-Type` header.
    fn response_with_content_type(content_type: &'static str) -> Response {
        let mut response = Response::new(Body::empty());
        response.headers_mut().insert(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static(content_type),
        );
        response
    }

    #[test]
    fn cache_control_set_private_no_store_on_html() {
        let response = add_private_cache_control_if_html(response_with_content_type(
            "text/html; charset=utf-8",
        ));
        let cc = response
            .headers()
            .get(axum::http::header::CACHE_CONTROL)
            .expect("HTML response must carry Cache-Control")
            .to_str()
            .unwrap()
            .to_lowercase();
        // Mirrors the e2e contract: present, not public, and private/no-store.
        assert!(
            !cc.contains("public"),
            "must not be publicly cacheable: {cc}"
        );
        assert!(
            cc.contains("private") || cc.contains("no-store"),
            "must be private/no-store, got {cc}"
        );
    }

    #[test]
    fn cache_control_not_set_on_json_response() {
        let response =
            add_private_cache_control_if_html(response_with_content_type("application/json"));
        assert!(
            response
                .headers()
                .get(axum::http::header::CACHE_CONTROL)
                .is_none(),
            "JSON API responses must not receive the HTML private-cache directive"
        );
    }

    #[test]
    fn cache_control_not_set_when_content_type_absent() {
        let response = add_private_cache_control_if_html(Response::new(Body::empty()));
        assert!(
            response
                .headers()
                .get(axum::http::header::CACHE_CONTROL)
                .is_none()
        );
    }

    #[test]
    fn cache_control_replaces_existing_public_directive() {
        let mut response = response_with_content_type("text/html");
        response.headers_mut().insert(
            axum::http::header::CACHE_CONTROL,
            axum::http::HeaderValue::from_static("public, max-age=3600"),
        );
        let response = add_private_cache_control_if_html(response);
        let cc = response
            .headers()
            .get(axum::http::header::CACHE_CONTROL)
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(cc, "private, no-store");
    }

    /// Integration: an authenticated HTML route, fetched through the real
    /// `auth_middleware`, comes back marked non-shared-cacheable. This is the
    /// in-crate analogue of the e2e `test_authenticated_html_not_shared_cacheable`.
    #[tokio::test]
    async fn authenticated_html_route_is_not_shared_cacheable() {
        async fn account_page() -> axum::response::Html<&'static str> {
            axum::response::Html("<html><body>account</body></html>")
        }

        let state = test_state();
        let token = create_session(&state, UserRole::Admin).await;
        let router = Router::new()
            .route("/account", get(account_page))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ))
            .with_state(state);

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/account")
                    .header("Cookie", format!("__Host-session={token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let cc = response
            .headers()
            .get(axum::http::header::CACHE_CONTROL)
            .expect("authenticated HTML page must set Cache-Control")
            .to_str()
            .unwrap()
            .to_lowercase();
        assert!(
            !cc.contains("public"),
            "must not be publicly cacheable: {cc}"
        );
        assert!(
            cc.contains("private") || cc.contains("no-store"),
            "must be private/no-store, got {cc}"
        );
    }

    // ── IdempotencyStore unit tests ────────────────────────────────────────

    #[test]
    fn idempotency_store_get_unknown_key() {
        let store = IdempotencyStore::new(Duration::from_secs(60));
        assert!(store.get("nonexistent").is_none());
    }

    #[test]
    fn idempotency_store_store_and_get() {
        let store = IdempotencyStore::new(Duration::from_secs(60));
        store.store(
            "key-1".into(),
            StatusCode::ACCEPTED,
            Some("application/json".into()),
            br#"{"document_id":42}"#.to_vec(),
        );
        let cached = store.get("key-1").unwrap();
        assert_eq!(cached.status, StatusCode::ACCEPTED);
        assert_eq!(cached.content_type.as_deref(), Some("application/json"));
        assert_eq!(cached.body, br#"{"document_id":42}"#);
    }

    #[test]
    fn idempotency_store_overwrites_existing_key() {
        let store = IdempotencyStore::new(Duration::from_secs(60));
        store.store("key".into(), StatusCode::OK, None, b"first".to_vec());
        store.store("key".into(), StatusCode::ACCEPTED, None, b"second".to_vec());
        let cached = store.get("key").unwrap();
        assert_eq!(cached.body, b"second".to_vec());
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn idempotency_store_entry_expires() {
        let store = IdempotencyStore::new(Duration::from_millis(10));
        store.store("ephemeral".into(), StatusCode::OK, None, b"data".to_vec());
        assert!(store.get("ephemeral").is_some());
        // Wait past TTL.
        std::thread::sleep(Duration::from_millis(20));
        assert!(store.get("ephemeral").is_none());
    }

    #[test]
    fn idempotency_store_reset_clears_all() {
        let store = IdempotencyStore::new(Duration::from_secs(60));
        store.store("a".into(), StatusCode::OK, None, vec![]);
        store.store("b".into(), StatusCode::OK, None, vec![]);
        assert_eq!(store.len(), 2);
        store.reset();
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn idempotency_store_get_prunes_expired() {
        let store = IdempotencyStore::new(Duration::from_millis(10));
        store.store("old".into(), StatusCode::OK, None, b"old".to_vec());
        std::thread::sleep(Duration::from_millis(20));
        store.store("new".into(), StatusCode::OK, None, b"new".to_vec());
        // get() prunes expired entries.
        let result = store.get("new").unwrap();
        assert_eq!(result.body, b"new".to_vec());
        // The expired entry should be gone.
        assert_eq!(store.len(), 1);
        assert!(store.get("old").is_none());
    }

    // ── extract_idempotency_key unit tests ─────────────────────────────────

    #[test]
    fn extract_idempotency_key_present() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "idempotency-key",
            axum::http::HeaderValue::from_static("abc-123-def"),
        );
        assert_eq!(
            extract_idempotency_key(&headers).as_deref(),
            Some("abc-123-def")
        );
    }

    #[test]
    fn extract_idempotency_key_missing() {
        let headers = axum::http::HeaderMap::new();
        assert_eq!(extract_idempotency_key(&headers), None);
    }

    #[test]
    fn extract_idempotency_key_empty_value() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("idempotency-key", axum::http::HeaderValue::from_static(""));
        assert_eq!(extract_idempotency_key(&headers), None);
    }

    // ── idempotency_key_for_user unit tests ─────────────────────────────────

    use super::idempotency_key_for_user;

    #[test]
    fn user_scoped_key_includes_tenant_and_user() {
        let user = AuthUser {
            tenant_id: 1,
            user_id: 42,
            role: UserRole::Member,
            email_verified: true,
        };
        let key = idempotency_key_for_user(&user, "abc-123");
        assert_eq!(key, "1:42:abc-123");
    }

    #[test]
    fn different_users_produce_different_keys_same_raw_key() {
        let alice = AuthUser {
            tenant_id: 1,
            user_id: 1,
            role: UserRole::Owner,
            email_verified: true,
        };
        let bob = AuthUser {
            tenant_id: 1,
            user_id: 2,
            role: UserRole::Member,
            email_verified: true,
        };
        let raw = "same-key";
        let key_a = idempotency_key_for_user(&alice, raw);
        let key_b = idempotency_key_for_user(&bob, raw);
        assert_ne!(key_a, key_b);
        assert_eq!(key_a, "1:1:same-key");
        assert_eq!(key_b, "1:2:same-key");
    }

    #[test]
    fn cross_tenant_users_produce_different_keys() {
        let t1 = AuthUser {
            tenant_id: 1,
            user_id: 1,
            role: UserRole::Owner,
            email_verified: true,
        };
        let t2 = AuthUser {
            tenant_id: 2,
            user_id: 1,
            role: UserRole::Owner,
            email_verified: true,
        };
        let key1 = idempotency_key_for_user(&t1, "x");
        let key2 = idempotency_key_for_user(&t2, "x");
        assert_ne!(key1, key2);
        assert_eq!(key1, "1:1:x");
        assert_eq!(key2, "2:1:x");
    }

    // ── request_id_middleware / resolve_request_id ───────────────────────────

    /// Build a `HeaderMap` with a single `X-Request-Id` value.
    fn headers_with_request_id(value: &'static str) -> axum::http::HeaderMap {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            HeaderName::from_static(REQUEST_ID_HEADER),
            HeaderValue::from_static(value),
        );
        headers
    }

    #[test]
    fn resolve_request_id_generates_uuid_when_absent() {
        let id = resolve_request_id(&axum::http::HeaderMap::new());
        // A freshly minted id parses as a UUID.
        assert!(
            uuid::Uuid::parse_str(&id).is_ok(),
            "generated id should be a UUID, got {id:?}"
        );
    }

    #[test]
    fn resolve_request_id_generates_distinct_ids() {
        let a = resolve_request_id(&axum::http::HeaderMap::new());
        let b = resolve_request_id(&axum::http::HeaderMap::new());
        assert_ne!(a, b, "two generated ids must differ");
    }

    #[test]
    fn resolve_request_id_adopts_valid_inbound() {
        let headers = headers_with_request_id("client-trace-abc123");
        assert_eq!(resolve_request_id(&headers), "client-trace-abc123");
    }

    #[test]
    fn resolve_request_id_trims_inbound() {
        let headers = headers_with_request_id("  trimmed-id  ");
        assert_eq!(resolve_request_id(&headers), "trimmed-id");
    }

    #[test]
    fn resolve_request_id_rejects_empty_inbound() {
        let headers = headers_with_request_id("   ");
        // Blank → mint a fresh UUID instead.
        let id = resolve_request_id(&headers);
        assert!(uuid::Uuid::parse_str(&id).is_ok());
    }

    #[test]
    fn resolve_request_id_rejects_inbound_with_spaces() {
        let headers = headers_with_request_id("has internal space");
        // Internal whitespace is not header/log-safe → mint a fresh UUID.
        let id = resolve_request_id(&headers);
        assert!(uuid::Uuid::parse_str(&id).is_ok());
    }

    #[test]
    fn resolve_request_id_rejects_overlong_inbound() {
        // A value longer than MAX_REQUEST_ID_LEN is discarded.
        let long: &'static str = Box::leak("a".repeat(MAX_REQUEST_ID_LEN + 1).into_boxed_str());
        let headers = headers_with_request_id(long);
        let id = resolve_request_id(&headers);
        assert!(uuid::Uuid::parse_str(&id).is_ok());
    }

    #[test]
    fn resolve_request_id_accepts_max_len_inbound() {
        // Exactly MAX_REQUEST_ID_LEN graphic chars is accepted verbatim.
        let exact: &'static str = Box::leak("a".repeat(MAX_REQUEST_ID_LEN).into_boxed_str());
        let headers = headers_with_request_id(exact);
        assert_eq!(resolve_request_id(&headers).len(), MAX_REQUEST_ID_LEN);
    }

    /// A bare router carrying only the request-id middleware. The handler echoes
    /// the value of the active request id by reading nothing — the test asserts
    /// the response header contract, which is the observable surface.
    fn request_id_test_router() -> Router {
        Router::new()
            .route("/anything", get(|| async { StatusCode::OK }))
            .layer(axum::middleware::from_fn(request_id_middleware))
    }

    #[tokio::test]
    async fn request_id_middleware_generates_header_when_absent() {
        let router = request_id_test_router();
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/anything")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let id = response
            .headers()
            .get(REQUEST_ID_HEADER)
            .expect("response must carry X-Request-Id")
            .to_str()
            .unwrap();
        assert!(
            uuid::Uuid::parse_str(id).is_ok(),
            "generated response id should be a UUID, got {id:?}"
        );
    }

    #[tokio::test]
    async fn request_id_middleware_propagates_inbound_header() {
        let router = request_id_test_router();
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/anything")
                    .header(REQUEST_ID_HEADER, "incoming-correlation-id")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(REQUEST_ID_HEADER)
                .and_then(|v| v.to_str().ok()),
            Some("incoming-correlation-id"),
            "a valid inbound X-Request-Id must be echoed unchanged"
        );
    }

    #[tokio::test]
    async fn request_id_middleware_replaces_unsafe_inbound_header() {
        let router = request_id_test_router();
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/anything")
                    .header(REQUEST_ID_HEADER, "bad id with spaces")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let echoed = response
            .headers()
            .get(REQUEST_ID_HEADER)
            .and_then(|v| v.to_str().ok())
            .expect("response must still carry a request id");
        assert_ne!(echoed, "bad id with spaces");
        assert!(
            uuid::Uuid::parse_str(echoed).is_ok(),
            "unsafe inbound id should be replaced by a UUID, got {echoed:?}"
        );
    }

    /// The request-id middleware composes with the auth middleware in the real
    /// nesting order (request-id outermost, auth inner): an authenticated
    /// request still succeeds and the response carries the correlation header.
    /// This exercises that `run_in_tenant_span` instruments the inner future
    /// without altering the response contract.
    #[tokio::test]
    async fn request_id_nests_around_auth_middleware() {
        let state = test_state();
        let token = create_session(&state, UserRole::Admin).await;

        let router = Router::new()
            .route("/protected", get(protected_handler))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ))
            .layer(axum::middleware::from_fn(request_id_middleware))
            .with_state(state);

        let response = router.oneshot(request_with_cookie(&token)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response.headers().get(REQUEST_ID_HEADER).is_some(),
            "the outer request-id layer must stamp the response even when auth runs inside it"
        );
    }
}
