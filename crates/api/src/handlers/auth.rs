//! Auth handlers: login (password verification + session creation), register
//! (user creation + session creation), and logout (session revocation).
//!
//! All three handlers set or clear the `__Host-session` cookie according to the
//! contract documented in [`kb_core::session`].

use std::sync::Arc;

use axum::Extension;
use axum::extract::State;
use axum::http::header::SET_COOKIE;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Json;
use kb_core::user::UserRole;
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::AuthUser;

// ── Request / response types ───────────────────────────────────────────────────

/// Login request body.
#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    /// Tenant identifier (the short slug).
    pub tenant_slug: String,
    /// User's login email (unique per tenant).
    pub email: String,
    /// Plaintext password.
    pub password: String,
    /// If `true`, the session TTL is extended to 30 days (P12-T2).
    #[serde(default)]
    pub remember_me: bool,
}

/// Registration request body.
#[derive(Debug, Deserialize)]
pub struct RegisterRequest {
    /// Tenant identifier (the short slug).
    pub tenant_slug: String,
    /// User's login email (unique per tenant).
    pub email: String,
    /// Plaintext password (will be Argon2id-hashed before storage).
    pub password: String,
}

/// Successful auth response body.
#[derive(Debug, Serialize)]
pub struct AuthResponse {
    /// The authenticated user's id.
    pub user_id: i64,
    /// The tenant id.
    pub tenant_id: i64,
    /// Human-readable role.
    pub role: String,
    /// Brief status message.
    pub message: String,
}

/// Error response body (returned on 401, 409, etc.).
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    /// Machine-readable error code.
    pub error: String,
    /// Human-readable description.
    pub message: String,
}

// ── Handlers ────────────────────────────────────────────────────────────────────

/// `POST /auth/login` — verify a password and create a session.
///
/// On success returns `200 OK` with [`AuthResponse`] and a `Set-Cookie` header
/// carrying the `__Host-session` token.
///
/// # Errors
/// * `401` — wrong email, wrong password, or unknown tenant.
/// * `500` — database or session store failure.
pub async fn login(
    State(state): State<Arc<AppState>>,
    Json(req): Json<LoginRequest>,
) -> Result<(StatusCode, HeaderMap, Json<AuthResponse>), (StatusCode, Json<ErrorResponse>)> {
    // ── 1. Resolve tenant ──────────────────────────────────────────────────
    let tenant_id = match state
        .pg_store
        .find_tenant_by_slug(&req.tenant_slug)
        .await
        .map_err(internal_error)?
    {
        Some(id) => id,
        None => {
            // Do NOT distinguish tenant-not-found from invalid credentials —
            // prevents tenant enumeration.
            return Err(unauthorized(
                "invalid_credentials",
                "invalid email or password",
            ));
        }
    };

    // ── 2. Find user ───────────────────────────────────────────────────────
    let user = state
        .pg_store
        .find_user_by_email(tenant_id, &req.email)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| unauthorized("invalid_credentials", "invalid email or password"))?;

    // ── 3. Verify password ─────────────────────────────────────────────────
    let ok = kb_core::auth::verify_password(&req.password, &user.password_hash)
        .map_err(internal_error)?;
    if !ok {
        return Err(unauthorized(
            "invalid_credentials",
            "invalid email or password",
        ));
    }

    // ── 4. Create session ──────────────────────────────────────────────────
    let ttl = if req.remember_me {
        std::time::Duration::from_secs(kb_core::session::REMEMBER_ME_SESSION_TTL_SECS)
    } else {
        state.session_ttl
    };
    let token = state
        .session_store
        .create(user.tenant_id, user.id, user.role, user.email_verified, ttl)
        .await
        .map_err(internal_error)?;

    // ── 5. Build response ──────────────────────────────────────────────────
    let mut headers = HeaderMap::new();
    headers.insert(
        SET_COOKIE,
        session_cookie_value(&token, ttl, state.secure_cookies),
    );

    let body = AuthResponse {
        user_id: user.id,
        tenant_id: user.tenant_id,
        role: user.role.as_str().to_owned(),
        message: "login successful".into(),
    };

    Ok((StatusCode::OK, headers, Json(body)))
}

/// `POST /auth/register` — create a new user and start a session.
///
/// On success returns `201 Created` with [`AuthResponse`] and a `Set-Cookie`
/// header carrying the `__Host-session` token. New users always get the `Member`
/// role; the first user of a tenant created via the bootstrap path gets `Owner`.
///
/// This handler is intended for open-registration tenants. For invite-only or
/// admin-created users, a future admin endpoint (P6) will provide that path.
///
/// # Errors
/// * `409` — a user with this email already exists in the tenant.
/// * `404` — the tenant slug does not exist.
/// * `500` — database or session store failure.
pub async fn register(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterRequest>,
) -> Result<(StatusCode, HeaderMap, Json<AuthResponse>), (StatusCode, Json<ErrorResponse>)> {
    // ── 1. Resolve tenant ──────────────────────────────────────────────────
    let tenant_id = match state
        .pg_store
        .find_tenant_by_slug(&req.tenant_slug)
        .await
        .map_err(internal_error)?
    {
        Some(id) => id,
        None => {
            // Do NOT distinguish tenant-not-found from other failures —
            // a generic message prevents tenant enumeration.
            return Err(registration_failed());
        }
    };

    // ── 2. Reject common/breached passwords ─────────────────────────────────
    if kb_core::auth::is_common_password(&req.password) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "weak_password".into(),
                message: "This password is too common. Please choose a stronger password.".into(),
            }),
        ));
    }

    // ── 3. Hash password ───────────────────────────────────────────────────
    let password_hash = kb_core::auth::hash_password(&req.password).map_err(internal_error)?;

    // ── 4. Create user (default role: Member) ──────────────────────────────
    let user_id = state
        .pg_store
        .create_user(tenant_id, &req.email, &password_hash, UserRole::Member)
        .await
        .map_err(|e| {
            if e.to_string().contains("already exists") {
                // Do NOT return 409 or include the email — prevents user enumeration.
                // An attacker cannot tell whether the tenant slug was invalid or the
                // email was already taken.
                registration_failed()
            } else {
                internal_error(e)
            }
        })?;

    // ── 5. Create session ──────────────────────────────────────────────────
    let token = state
        .session_store
        .create(
            tenant_id,
            user_id,
            UserRole::Member,
            false,
            state.session_ttl,
        )
        .await
        .map_err(internal_error)?;

    // ── 6. Build response ──────────────────────────────────────────────────
    let mut headers = HeaderMap::new();
    headers.insert(
        SET_COOKIE,
        session_cookie_value(&token, state.session_ttl, state.secure_cookies),
    );

    let body = AuthResponse {
        user_id,
        tenant_id,
        role: UserRole::Member.as_str().to_owned(),
        message: "registration successful".into(),
    };

    Ok((StatusCode::CREATED, headers, Json(body)))
}

/// `POST /auth/logout` — revoke the current session and clear the cookie.
///
/// Requires the auth middleware (i.e. a valid `__Host-session` cookie). The
/// session is destroyed server-side, and the response includes a `Set-Cookie`
/// header that immediately expires the cookie in the browser.
///
/// # Errors
/// * `401` — no valid session (rejected by middleware before this handler runs).
/// * `500` — session store failure.
pub async fn logout(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    headers: HeaderMap,
) -> Result<(StatusCode, HeaderMap, Json<AuthResponse>), (StatusCode, Json<ErrorResponse>)> {
    // Extract the token from the request cookie.
    let token = crate::middleware::extract_session_cookie(&headers).ok_or_else(|| {
        // This should not happen — the middleware already validated the session.
        internal_error(anyhow::anyhow!(
            "session cookie missing after middleware validation"
        ))
    })?;

    // Revoke the session.
    state
        .session_store
        .revoke(&token)
        .await
        .map_err(internal_error)?;

    // Clear the cookie in the browser.
    let mut response_headers = HeaderMap::new();
    response_headers.insert(SET_COOKIE, cleared_cookie_value(state.secure_cookies));

    let body = AuthResponse {
        user_id: auth_user.user_id,
        tenant_id: auth_user.tenant_id,
        role: auth_user.role.as_str().to_owned(),
        message: "logout successful".into(),
    };

    Ok((StatusCode::OK, response_headers, Json(body)))
}

// ── Cookie helpers ──────────────────────────────────────────────────────────────

/// Format a `Set-Cookie` header value for a new session.
///
/// Produces:
/// `__Host-session=<token>; HttpOnly; SameSite=Lax; Path=/; Max-Age=<ttl>[; Secure]`
fn session_cookie_value(
    token: &str,
    ttl: std::time::Duration,
    secure: bool,
) -> axum::http::HeaderValue {
    let max_age = ttl.as_secs();
    let value = if secure {
        format!("__Host-session={token}; HttpOnly; SameSite=Lax; Path=/; Max-Age={max_age}; Secure")
    } else {
        format!("__Host-session={token}; HttpOnly; SameSite=Lax; Path=/; Max-Age={max_age}")
    };
    axum::http::HeaderValue::from_str(&value).unwrap_or(axum::http::HeaderValue::from_static(""))
}

/// Format a `Set-Cookie` header value that clears the session cookie
/// (Max-Age=0, empty value).
fn cleared_cookie_value(secure: bool) -> axum::http::HeaderValue {
    let value = if secure {
        "__Host-session=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0; Secure"
    } else {
        "__Host-session=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0"
    };
    axum::http::HeaderValue::from_str(value).unwrap_or(axum::http::HeaderValue::from_static(""))
}

// ═══════════════════════════════════════════════════════════════════════════════
// Error helpers
// ═══════════════════════════════════════════════════════════════════════════════

/// Build a `401 Unauthorized` error with the given code and message.
fn unauthorized(code: &str, msg: &str) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::UNAUTHORIZED,
        Json(ErrorResponse {
            error: code.into(),
            message: msg.into(),
        }),
    )
}

/// Build a generic `400 Bad Request` for registration failures.
///
/// Deliberately does **not** distinguish between "tenant not found", "email
/// taken", etc. — a generic message prevents tenant and user enumeration.
fn registration_failed() -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorResponse {
            error: "registration_failed".into(),
            message:
                "Unable to complete registration. Please check your information and try again."
                    .into(),
        }),
    )
}

/// Build a `500 Internal Server Error` from any error.
fn internal_error(e: impl std::fmt::Display) -> (StatusCode, Json<ErrorResponse>) {
    tracing::error!(error = %e, "internal server error");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            error: "internal_error".into(),
            message: "an unexpected error occurred".into(),
        }),
    )
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::Arc;
    use std::time::Duration;

    use axum::Router;
    use axum::body::Body;
    use axum::http::StatusCode;
    use axum::middleware;
    use axum::routing::post;
    use kb_core::user::UserRole;
    use kb_store::PgStore;
    use kb_store::session_store::InMemorySessionStore;
    use tower::ServiceExt;

    use super::*;
    use crate::AppState;

    /// Build a test state with InMemorySessionStore and a disconnected PgStore.
    /// The PgStore won't be called in handler unit tests (we mock at the handler
    /// level for pure-logic tests; the PgStore calls are tested via integration).
    fn test_state() -> Arc<AppState> {
        let session_store: Arc<dyn kb_core::session::SessionStore> =
            Arc::new(InMemorySessionStore::new());
        let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
        Arc::new(AppState::new(
            session_store,
            pg_store,
            Some(Duration::from_secs(3600)),
            false,
        ))
    }

    /// Build a router with just the auth routes (no middleware except on logout).
    fn auth_router(state: Arc<AppState>) -> Router {
        let public = Router::new()
            .route("/auth/login", post(login))
            .route("/auth/register", post(register));

        let protected = Router::new().route("/auth/logout", post(logout)).layer(
            middleware::from_fn_with_state(state.clone(), crate::middleware::auth_middleware),
        );

        public.merge(protected).with_state(state)
    }

    // ── Cookie helpers ─────────────────────────────────────────────────────

    #[test]
    fn session_cookie_has_correct_properties_non_secure() {
        let val = session_cookie_value("abc123", Duration::from_secs(86400), false);
        let s = val.to_str().unwrap();
        assert!(s.starts_with("__Host-session=abc123"));
        assert!(s.contains("HttpOnly"));
        assert!(s.contains("SameSite=Lax"));
        assert!(s.contains("Path=/"));
        assert!(s.contains("Max-Age=86400"));
        assert!(!s.contains("Secure"));
    }

    #[test]
    fn session_cookie_has_secure_when_enabled() {
        let val = session_cookie_value("abc123", Duration::from_secs(60), true);
        let s = val.to_str().unwrap();
        assert!(s.contains("Secure"));
        assert!(s.contains("Max-Age=60"));
    }

    #[test]
    fn cleared_cookie_has_max_age_zero() {
        let val = cleared_cookie_value(false);
        let s = val.to_str().unwrap();
        assert!(s.contains("Max-Age=0"));
        assert!(s.contains("__Host-session="));
        assert!(s.ends_with("=") || s.contains("=;"));
    }

    #[test]
    fn cleared_cookie_has_secure_when_enabled() {
        let val = cleared_cookie_value(true);
        assert!(val.to_str().unwrap().contains("Secure"));
    }

    // ── error helpers ──────────────────────────────────────────────────────

    #[test]
    fn unauthorized_error_has_correct_status() {
        let (status, body) = unauthorized("bad_password", "wrong password");
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body.error, "bad_password");
        assert_eq!(body.message, "wrong password");
    }

    #[test]
    fn internal_error_has_500_status() {
        let (status, body) = internal_error(anyhow::anyhow!("db down"));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body.error, "internal_error");
    }

    // ── logout tests (pure session logic) ──────────────────────────────────

    #[tokio::test]
    async fn logout_revokes_session_and_clears_cookie() {
        let state = test_state();

        // Create a session.
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();

        // Make a logout request with the session cookie.
        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/auth/logout")
            .header("Cookie", format!("__Host-session={token}"))
            .header("Content-Type", "application/json")
            .body(Body::empty())
            .unwrap();

        let router = auth_router(state.clone());
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Verify the session was revoked.
        let result = state.session_store.validate(&token).await.unwrap();
        assert!(result.is_none(), "session should be revoked");

        // Verify the response sets a clearing cookie.
        let cookie_headers: Vec<_> = response.headers().get_all("set-cookie").iter().collect();
        assert!(!cookie_headers.is_empty(), "logout must clear the cookie");
        let cookie_str = cookie_headers[0].to_str().unwrap();
        assert!(
            cookie_str.contains("Max-Age=0"),
            "clearing cookie must have Max-Age=0"
        );
    }

    #[tokio::test]
    async fn logout_without_cookie_is_rejected_by_middleware() {
        let state = test_state();
        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/auth/logout")
            .header("Content-Type", "application/json")
            .body(Body::empty())
            .unwrap();

        let router = auth_router(state);
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
