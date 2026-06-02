//! The cookie-based authentication middleware.
//!
//! [`auth_middleware`] runs on every request to protected routes. It extracts the
//! `__Host-session` cookie, validates it against the [`SessionStore`], and injects an
//! [`AuthUser`](crate::AuthUser) into request extensions. Requests without a valid
//! session receive a `401 Unauthorized`.

use std::sync::Arc;

use axum::extract::State;
use axum::http::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;

use crate::AppState;
use crate::AuthUser;

/// Axum middleware: extract and validate the session cookie, injecting
/// [`AuthUser`] into request extensions on success.
///
/// # Cookie extraction
///
/// Parses the `Cookie` header, looking for `__Host-session=<token>`.
/// If the cookie is missing, malformed, or the token is unknown/expired/revoked,
/// the middleware returns `401 Unauthorized`.
///
/// # Sliding expiration
///
/// On every valid request the middleware calls
/// [`SessionStore::extend`](kb_core::session::SessionStore::extend) to slide the
/// session expiry forward. Failures to extend are logged but do **not** reject the
/// request — a session that can't be extended is still valid for the current request.
pub async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let token = extract_session_cookie(request.headers());

    let token = match token {
        Some(t) => t,
        None => return Err(StatusCode::UNAUTHORIZED),
    };

    let info = match state.session_store.validate(&token).await {
        Ok(Some(info)) => info,
        Ok(None) => return Err(StatusCode::UNAUTHORIZED),
        Err(e) => {
            tracing::warn!(error = %e, "session store error during validation");
            return Err(StatusCode::UNAUTHORIZED);
        }
    };

    // Inject the authenticated user into request extensions.
    request.extensions_mut().insert(AuthUser {
        tenant_id: info.tenant_id,
        user_id: info.user_id,
        role: info.user_role,
        email_verified: info.email_verified,
    });

    // Slide the session expiry (best-effort — failure does not reject the request).
    if let Err(e) = state.session_store.extend(&token, state.session_ttl).await {
        tracing::warn!(error = %e, "failed to extend session (sliding expiration)");
    }

    Ok(next.run(request).await)
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
}
