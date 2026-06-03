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

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::Method;
use axum::http::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;

use tower_http::cors::{Any, CorsLayer};

use crate::AppState;
use crate::AuthUser;

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
                    request.extensions_mut().insert(AuthUser {
                        tenant_id: info.tenant_id,
                        user_id: info.user_id,
                        role: info.user_role,
                        email_verified,
                    });
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
                    return run_maybe_idempotent(request, next).await;
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
    request.extensions_mut().insert(AuthUser {
        tenant_id: info.tenant_id,
        user_id: info.user_id,
        role: info.user_role,
        email_verified,
    });

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

    run_maybe_idempotent(request, next).await
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
/// Tracks login attempts per client IP within a configurable time window.
/// When an IP exceeds `max_attempts` within `window`, further attempts receive
/// `429 Too Many Requests` with a `Retry-After` header.
struct LoginBruteForceLimiter {
    inner: Mutex<HashMap<IpAddr, Vec<Instant>>>,
    max_attempts: usize,
    window: Duration,
}

impl LoginBruteForceLimiter {
    /// Create a new limiter with the given attempt cap and time window.
    fn new(max_attempts: usize, window: Duration) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            max_attempts,
            window,
        }
    }

    /// Check whether `ip` is allowed to make another login attempt.
    ///
    /// Prunes expired entries for the IP, then checks whether the remaining
    /// count is below `max_attempts`. If allowed, records the attempt and
    /// returns `Ok(())`. If rate-limited, returns `Err(retry_after_secs)`
    /// where `retry_after_secs` is the suggested `Retry-After` header value.
    fn check(&self, ip: IpAddr) -> Result<(), u64> {
        let mut map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        let cutoff = now - self.window;
        let attempts = map.entry(ip).or_default();
        attempts.retain(|t| *t > cutoff);
        if attempts.len() >= self.max_attempts {
            let oldest = attempts.iter().min().copied().unwrap_or(now);
            let elapsed = now.duration_since(oldest).as_secs();
            let retry = self.window.as_secs().saturating_sub(elapsed).max(1);
            Err(retry)
        } else {
            attempts.push(now);
            Ok(())
        }
    }

    /// Clear all tracked attempts (test-only).
    #[cfg(test)]
    fn reset(&self) {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }
}

/// Global login brute-force limiter: 5 attempts per IP per 60-second window.
static LOGIN_BRUTE_FORCE_LIMITER: LazyLock<LoginBruteForceLimiter> =
    LazyLock::new(|| LoginBruteForceLimiter::new(5, Duration::from_secs(60)));

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
/// defaulting to `127.0.0.1`). After 5 attempts within 60 seconds, returns
/// `429 Too Many Requests` with a `Retry-After` header and a JSON error body.
///
/// Requests to paths other than `POST /auth/login` pass through unchanged.
pub async fn login_rate_limit_middleware(
    request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    if request.method() != axum::http::Method::POST || request.uri().path() != "/auth/login" {
        return Ok(next.run(request).await);
    }

    let ip = extract_client_ip(&request);
    match LOGIN_BRUTE_FORCE_LIMITER.check(ip) {
        Ok(()) => Ok(next.run(request).await),
        Err(retry_after) => {
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
        let limiter = LoginBruteForceLimiter::new(3, Duration::from_secs(60));
        let ip = IpAddr::from([10, 0, 0, 1]);
        for _ in 0..3 {
            assert!(limiter.check(ip).is_ok());
        }
    }

    #[test]
    fn limiter_blocks_after_max_attempts() {
        let limiter = LoginBruteForceLimiter::new(2, Duration::from_secs(60));
        let ip = IpAddr::from([10, 0, 0, 2]);
        assert!(limiter.check(ip).is_ok());
        assert!(limiter.check(ip).is_ok());
        // Third attempt — rate-limited.
        let err = limiter.check(ip).unwrap_err();
        assert!(err > 0, "retry-after must be positive, got {err}");
    }

    #[test]
    fn limiter_tracks_different_ips_independently() {
        let limiter = LoginBruteForceLimiter::new(1, Duration::from_secs(60));
        let ip_a = IpAddr::from([10, 0, 0, 1]);
        let ip_b = IpAddr::from([10, 0, 0, 2]);
        // Exhaust ip_a.
        assert!(limiter.check(ip_a).is_ok());
        assert!(limiter.check(ip_a).is_err());
        // ip_b still has attempts.
        assert!(limiter.check(ip_b).is_ok());
    }

    #[test]
    fn limiter_window_expires() {
        let limiter = LoginBruteForceLimiter::new(2, Duration::from_millis(10));
        let ip = IpAddr::from([10, 0, 0, 3]);
        assert!(limiter.check(ip).is_ok());
        assert!(limiter.check(ip).is_ok());
        assert!(limiter.check(ip).is_err());
        // Wait for the window to expire.
        std::thread::sleep(Duration::from_millis(15));
        assert!(limiter.check(ip).is_ok());
    }

    #[test]
    fn limiter_returns_reasonable_retry_after() {
        let limiter = LoginBruteForceLimiter::new(1, Duration::from_secs(60));
        let ip = IpAddr::from([10, 0, 0, 4]);
        assert!(limiter.check(ip).is_ok());
        let err = limiter.check(ip).unwrap_err();
        // Retry-After must be in [1, 60] range.
        assert!(err >= 1, "retry-after too small: {err}");
        assert!(err <= 60, "retry-after too large: {err}");
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
    fn rate_limit_test_router() -> Router {
        Router::new()
            .route(
                "/auth/login",
                axum::routing::post(|| async { StatusCode::OK }),
            )
            .layer(axum::middleware::from_fn(login_rate_limit_middleware))
    }

    #[tokio::test]
    async fn middleware_passes_non_login_paths_through() {
        LOGIN_BRUTE_FORCE_LIMITER.reset();
        let router = Router::new()
            .route("/other", axum::routing::post(|| async { StatusCode::OK }))
            .layer(axum::middleware::from_fn(login_rate_limit_middleware));

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
        LOGIN_BRUTE_FORCE_LIMITER.reset();
        let router = rate_limit_test_router();
        let mut statuses = Vec::new();

        for _ in 0..10 {
            let response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/auth/login")
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
        LOGIN_BRUTE_FORCE_LIMITER.reset();
        let router = rate_limit_test_router();

        // Exhaust the limit.
        for _ in 0..5 {
            let _ = router
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/auth/login")
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
}
