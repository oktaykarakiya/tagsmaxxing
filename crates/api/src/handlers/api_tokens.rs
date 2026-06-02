//! API token handlers: create (POST /api/tokens), list (GET /api/tokens),
//! and revoke (DELETE /api/tokens/:id).
//!
//! All three handlers require cookie-based authentication (via the middleware).
//! The created token can then be used for Bearer-auth on subsequent API requests.

use std::sync::Arc;
use std::time::Duration;

use axum::Extension;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Json;
use kb_core::api_token::DEFAULT_API_TOKEN_TTL_SECS;
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::AuthUser;

// ── Request / response types ───────────────────────────────────────────────────

/// Request body for creating an API token.
#[derive(Debug, Deserialize)]
pub struct CreateTokenRequest {
    /// Human-readable label for the token (e.g. "CLI upload script").
    pub name: String,
    /// Optional TTL in seconds. If absent or 0, defaults to
    /// [`DEFAULT_API_TOKEN_TTL_SECS`] (365 days).
    #[serde(default)]
    pub ttl_secs: u64,
}

/// Response body for creating an API token.
///
/// The `token` field contains the **full plaintext token** — this is the only
/// time it is returned. The caller must capture it now; it cannot be recovered later.
#[derive(Debug, Serialize)]
pub struct CreateTokenResponse {
    /// Primary key of the created token row.
    pub id: i64,
    /// The full plaintext token (64 hex chars). **Show once.**
    pub token: String,
    /// Human-readable label.
    pub name: String,
    /// Expiration time (ISO 8601).
    pub expires_at: String,
}

/// Response body for listing API tokens (metadata only — no raw tokens).
#[derive(Debug, Serialize)]
pub struct ListTokensResponse {
    /// Metadata for every active (non-revoked, non-expired) token.
    pub tokens: Vec<TokenMetaItem>,
}

/// A single token's metadata in the list response.
#[derive(Debug, Serialize)]
pub struct TokenMetaItem {
    /// Primary key.
    pub id: i64,
    /// Human-readable label.
    pub name: String,
    /// When this token was last used for authentication, if ever.
    pub last_used_at: Option<String>,
    /// Expiration time (ISO 8601).
    pub expires_at: String,
    /// Creation time (ISO 8601).
    pub created_at: String,
}

/// Response body for revoking a token.
#[derive(Debug, Serialize)]
pub struct RevokeTokenResponse {
    /// The revoked token's id.
    pub id: i64,
    /// Status message.
    pub message: String,
}

// ── Handlers ────────────────────────────────────────────────────────────────────

/// POST /api/tokens — create a new API token.
///
/// The full plaintext token is returned **once** in the response. It is never
/// stored in the database (only its SHA-256 hash) and cannot be recovered later.
///
/// # Authentication
///
/// Requires a valid session cookie. The token inherits the authenticated user's
/// tenant, user id, role, and email-verification status at creation time.
///
/// # Response
///
/// * `201 Created` — token created successfully, full token in response body.
/// * `401 Unauthorized` — no valid session (handled by middleware).
/// * `500 Internal Server Error` — token store unavailable or misconfigured.
pub async fn create_token(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Json(req): Json<CreateTokenRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<serde_json::Value>)> {
    let store = match &state.api_token_store {
        Some(s) => s,
        None => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "not_configured",
                    "message": "API token store is not configured"
                })),
            ));
        }
    };

    let ttl = if req.ttl_secs == 0 {
        Duration::from_secs(DEFAULT_API_TOKEN_TTL_SECS)
    } else {
        Duration::from_secs(req.ttl_secs)
    };

    let result = match store
        .create_token(
            auth_user.tenant_id,
            auth_user.user_id,
            auth_user.role,
            auth_user.email_verified,
            &req.name,
            ttl,
        )
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "failed to create API token");
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "token_creation_failed",
                    "message": format!("Failed to create API token: {e}")
                })),
            ));
        }
    };

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "id": result.id,
            "token": result.raw_token,
            "name": result.name,
            "expires_at": result.expires_at.to_rfc3339(),
        })),
    ))
}

/// GET /api/tokens — list all active (non-revoked, non-expired) API tokens
/// for the authenticated user's tenant.
///
/// Returns metadata only — **never** the full token value.
///
/// # Response
///
/// * `200 OK` — list of token metadata (may be empty).
/// * `401 Unauthorized` — no valid session (handled by middleware).
/// * `500 Internal Server Error` — token store unavailable.
pub async fn list_tokens(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let store = match &state.api_token_store {
        Some(s) => s,
        None => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "not_configured",
                    "message": "API token store is not configured"
                })),
            ));
        }
    };

    let tokens = match store.list_tokens(auth_user.tenant_id).await {
        Ok(t) => t,
        Err(e) => {
            tracing::error!(error = %e, "failed to list API tokens");
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "list_failed",
                    "message": format!("Failed to list API tokens: {e}")
                })),
            ));
        }
    };

    let items: Vec<serde_json::Value> = tokens
        .into_iter()
        .map(|t| {
            serde_json::json!({
                "id": t.id,
                "name": t.name,
                "last_used_at": t.last_used_at.map(|d| d.to_rfc3339()),
                "expires_at": t.expires_at.to_rfc3339(),
                "created_at": t.created_at.to_rfc3339(),
            })
        })
        .collect();

    Ok(Json(serde_json::json!({ "tokens": items })))
}

/// DELETE /api/tokens/:id — revoke an API token by id.
///
/// After revocation, the token immediately stops working for Bearer-auth requests.
/// This is idempotent: revoking an already-revoked or non-existent token succeeds
/// (200 with appropriate message).
///
/// # Response
///
/// * `200 OK` — token revoked (or was already revoked / doesn't exist).
/// * `401 Unauthorized` — no valid session (handled by middleware).
/// * `500 Internal Server Error` — token store unavailable.
pub async fn revoke_token(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(token_id): Path<i64>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let store = match &state.api_token_store {
        Some(s) => s,
        None => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "not_configured",
                    "message": "API token store is not configured"
                })),
            ));
        }
    };

    if let Err(e) = store.revoke_token(auth_user.tenant_id, token_id).await {
        tracing::error!(error = %e, "failed to revoke API token");
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "revoke_failed",
                "message": format!("Failed to revoke API token: {e}")
            })),
        ));
    }

    Ok(Json(serde_json::json!({
        "id": token_id,
        "message": "Token revoked"
    })))
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
    use axum::routing::{delete, post};
    use kb_core::api_token::ApiTokenStore;
    use kb_core::session::{DEFAULT_SESSION_TTL_SECS, SessionStore};
    use kb_core::user::UserRole;
    use kb_store::api_token_store::InMemoryApiTokenStore;
    use kb_store::session_store::InMemorySessionStore;
    use tower::ServiceExt;

    use super::*;
    use crate::AppState;
    use crate::middleware::auth_middleware;

    /// Helper: build a test `AppState` with an in-memory session store and API
    /// token store, plus an unconnected PgStore (never called by token handlers).
    fn test_state() -> Arc<AppState> {
        let session_store: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
        let pg_store = Arc::new(kb_store::PgStore::new(
            "postgres://localhost/test?sslmode=disable",
        ));
        let token_store: Arc<dyn ApiTokenStore> = Arc::new(InMemoryApiTokenStore::new());

        let mut state = AppState::new(
            session_store,
            pg_store,
            Some(Duration::from_secs(DEFAULT_SESSION_TTL_SECS)),
            false,
        );
        state.api_token_store = Some(token_store);
        Arc::new(state)
    }

    /// Helper: create a valid session and return the token.
    async fn create_session(state: &AppState) -> String {
        state
            .session_store
            .create(1, 42, UserRole::Admin, true, Duration::from_secs(3600))
            .await
            .unwrap()
    }

    /// Build a request with the session cookie set.
    fn request_with_cookie(method: &str, uri: &str, token: &str, body: Body) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("Cookie", format!("__Host-session={token}"))
            .header("Content-Type", "application/json")
            .body(body)
            .unwrap()
    }

    /// Build the test router for API token endpoints.
    fn test_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/api/tokens", post(create_token).get(list_tokens))
            .route("/api/tokens/{id}", delete(revoke_token))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ))
            .with_state(state)
    }

    // ── Create token tests ─────────────────────────────────────────────────

    #[tokio::test]
    async fn create_token_returns_201_with_token() {
        let state = test_state();
        let session = create_session(&state).await;
        let router = test_router(state);

        let response = router
            .oneshot(request_with_cookie(
                "POST",
                "/api/tokens",
                &session,
                Body::from(
                    serde_json::to_string(&serde_json::json!({
                        "name": "my-cli-token"
                    }))
                    .unwrap(),
                ),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);

        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["name"], "my-cli-token");
        assert!(json["token"].as_str().unwrap().len() == 64);
        assert!(json["id"].as_i64().unwrap() > 0);
    }

    #[tokio::test]
    async fn create_token_unauthenticated_returns_401() {
        let state = test_state();
        let router = test_router(state);

        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tokens")
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        serde_json::to_string(&serde_json::json!({
                            "name": "test"
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    // ── List tokens tests ──────────────────────────────────────────────────

    #[tokio::test]
    async fn list_tokens_returns_metadata_only() {
        let state = test_state();
        let session = create_session(&state).await;

        // First create a token.
        let router = test_router(state.clone());
        let create_resp = router
            .clone()
            .oneshot(request_with_cookie(
                "POST",
                "/api/tokens",
                &session,
                Body::from(
                    serde_json::to_string(&serde_json::json!({
                        "name": "token-1"
                    }))
                    .unwrap(),
                ),
            ))
            .await
            .unwrap();
        assert_eq!(create_resp.status(), StatusCode::CREATED);

        // Now list.
        let list_resp = router
            .oneshot(request_with_cookie(
                "GET",
                "/api/tokens",
                &session,
                Body::empty(),
            ))
            .await
            .unwrap();

        assert_eq!(list_resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(list_resp.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let tokens = json["tokens"].as_array().unwrap();
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0]["name"], "token-1");
        // Listing must NEVER include the raw token.
        assert!(tokens[0].get("token").is_none());
    }

    #[tokio::test]
    async fn list_tokens_is_tenant_scoped() {
        let state = test_state();

        // Create a session for tenant 1, user 42 (the default in test_state).
        let session = state
            .session_store
            .create(1, 42, UserRole::Admin, true, Duration::from_secs(3600))
            .await
            .unwrap();

        // Create a token for tenant 1 via the store directly.
        state
            .api_token_store
            .as_ref()
            .unwrap()
            .create_token(
                1,
                42,
                UserRole::Admin,
                true,
                "t1-token",
                Duration::from_secs(DEFAULT_API_TOKEN_TTL_SECS),
            )
            .await
            .unwrap();

        // Create a token for tenant 2 via the store directly.
        state
            .api_token_store
            .as_ref()
            .unwrap()
            .create_token(
                2,
                99,
                UserRole::Member,
                true,
                "t2-token",
                Duration::from_secs(DEFAULT_API_TOKEN_TTL_SECS),
            )
            .await
            .unwrap();

        let router = test_router(state);
        let list_resp = router
            .oneshot(request_with_cookie(
                "GET",
                "/api/tokens",
                &session,
                Body::empty(),
            ))
            .await
            .unwrap();

        let body = axum::body::to_bytes(list_resp.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let tokens = json["tokens"].as_array().unwrap();
        // Should only see tenant 1's token, not tenant 2's.
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0]["name"], "t1-token");
    }

    // ── Revoke token tests ─────────────────────────────────────────────────

    #[tokio::test]
    async fn revoke_token_returns_200() {
        let state = test_state();
        let session = create_session(&state).await;

        // Create a token first.
        let router = test_router(state.clone());
        let create_resp = router
            .clone()
            .oneshot(request_with_cookie(
                "POST",
                "/api/tokens",
                &session,
                Body::from(
                    serde_json::to_string(&serde_json::json!({
                        "name": "to-revoke"
                    }))
                    .unwrap(),
                ),
            ))
            .await
            .unwrap();
        let create_body = axum::body::to_bytes(create_resp.into_body(), 4096)
            .await
            .unwrap();
        let create_json: serde_json::Value = serde_json::from_slice(&create_body).unwrap();
        let token_id = create_json["id"].as_i64().unwrap();

        // Revoke it.
        let revoke_resp = router
            .oneshot(request_with_cookie(
                "DELETE",
                &format!("/api/tokens/{token_id}"),
                &session,
                Body::empty(),
            ))
            .await
            .unwrap();

        assert_eq!(revoke_resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(revoke_resp.into_body(), 4096)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["id"], token_id);
        assert!(json["message"].as_str().unwrap().contains("revoked"));
    }

    #[tokio::test]
    async fn revoke_nonexistent_token_still_returns_200() {
        let state = test_state();
        let session = create_session(&state).await;
        let router = test_router(state);

        let response = router
            .oneshot(request_with_cookie(
                "DELETE",
                "/api/tokens/99999",
                &session,
                Body::empty(),
            ))
            .await
            .unwrap();

        // Revoking a non-existent token is idempotent — still 200.
        assert_eq!(response.status(), StatusCode::OK);
    }

    // ── Store-not-configured tests ────────────────────────────────────────

    #[tokio::test]
    async fn create_token_returns_500_when_not_configured() {
        let state = {
            let session_store: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
            let pg_store = Arc::new(kb_store::PgStore::new(
                "postgres://localhost/test?sslmode=disable",
            ));
            let mut s = AppState::new(
                session_store,
                pg_store,
                Some(Duration::from_secs(DEFAULT_SESSION_TTL_SECS)),
                false,
            );
            s.api_token_store = None; // not configured
            Arc::new(s)
        };

        let session = state
            .session_store
            .create(1, 42, UserRole::Admin, true, Duration::from_secs(3600))
            .await
            .unwrap();

        let router = test_router(state);
        let response = router
            .oneshot(request_with_cookie(
                "POST",
                "/api/tokens",
                &session,
                Body::from(
                    serde_json::to_string(&serde_json::json!({
                        "name": "test"
                    }))
                    .unwrap(),
                ),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
