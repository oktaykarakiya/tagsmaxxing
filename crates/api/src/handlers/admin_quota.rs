// SPDX-License-Identifier: AGPL-3.0-or-later

//! Super-admin quota-override endpoint (ledger P14-T14, plan §29).
//!
//! `POST /admin/tenants/:id/quota-override` lets the **platform operator**
//! (super-admin) clamp any tenant's effective storage and token caps via an
//! admin override. Because [`kb_store::PgStore::resolve_effective_quotas`] gives
//! the override precedence over the plan-derived value, setting
//! `quota_override_tokens` enforces a budget **even on a grandfathered (no-plan)
//! tenant** — which is what the limits-e2e harness relies on to shrink a tenant's
//! caps and provoke `413`/`429` enforcement.
//!
//! This is a JSON endpoint (no CSRF) so an out-of-band operator/harness can drive
//! it like the rest of the `/api`-style surface, but it sits behind the
//! auth middleware and a **super-admin** gate: only an Owner/Admin of the
//! platform operator's tenant (the bootstrap / lowest-id tenant, resolved at call
//! time via [`kb_store::PgStore::is_platform_admin_tenant`]) may mutate another
//! tenant's quota. Every successful call is audited by `set_admin_quota_override`
//! (an `admin_override_quota` event).

use std::sync::Arc;

use axum::Extension;
use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use kb_core::user::UserRole;
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::AuthUser;

/// Request body for [`set_quota_override`].
///
/// Both fields are optional. A `Some(n)` value installs that override (taking
/// precedence over the plan value at enforcement time); a `null`/omitted value
/// clears that dimension's override back to the plan-derived limit. Sending both
/// as `null` reverts the tenant entirely to its plan limits.
#[derive(Debug, Deserialize)]
pub struct QuotaOverrideRequest {
    /// Storage cap override in bytes (`None`/omitted clears it).
    #[serde(default)]
    pub quota_override_bytes: Option<i64>,
    /// Token-budget override (`None`/omitted clears it).
    #[serde(default)]
    pub quota_override_tokens: Option<i64>,
}

/// Success body for [`set_quota_override`].
#[derive(Debug, Serialize)]
pub struct QuotaOverrideResponse {
    /// The tenant whose override was set.
    pub tenant_id: i64,
    /// The storage override now in effect (echoed back).
    pub quota_override_bytes: Option<i64>,
    /// The token override now in effect (echoed back).
    pub quota_override_tokens: Option<i64>,
}

/// JSON error body for this endpoint.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    /// Machine-readable error code.
    error: String,
    /// Human-readable message.
    message: String,
}

/// `POST /admin/tenants/:id/quota-override` — set a tenant's admin quota override
/// (super-admin only).
///
/// # Response
///
/// * `200 OK` — [`QuotaOverrideResponse`]; the override was written and audited.
/// * `401 Unauthorized` — rejected by the auth middleware.
/// * `403 Forbidden` — the caller is not a super-admin (not an Owner/Admin of the
///   platform operator's tenant).
/// * `404 Not Found` — no tenant with the given id exists.
/// * `500 Internal Server Error` — database failure.
pub async fn set_quota_override(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(tenant_id): Path<i64>,
    Json(req): Json<QuotaOverrideRequest>,
) -> Result<(StatusCode, Json<QuotaOverrideResponse>), (StatusCode, Json<ErrorResponse>)> {
    let pg = &state.pg_store;

    // ── Super-admin gate ─────────────────────────────────────────────────────
    require_super_admin(pg, &auth_user).await?;

    // ── Target must exist (distinguish 404 from a silent no-op UPDATE) ───────
    if !pg.tenant_exists(tenant_id).await.map_err(internal_error)? {
        return Err(not_found("tenant_not_found", "tenant not found"));
    }

    // ── Apply + audit (override precedence enforces even grandfathered tenants) ─
    pg.set_admin_quota_override(
        tenant_id,
        req.quota_override_bytes,
        req.quota_override_tokens,
        auth_user.user_id,
    )
    .await
    .map_err(internal_error)?;

    Ok((
        StatusCode::OK,
        Json(QuotaOverrideResponse {
            tenant_id,
            quota_override_bytes: req.quota_override_bytes,
            quota_override_tokens: req.quota_override_tokens,
        }),
    ))
}

/// Return `403` unless the caller is a **super-admin**: an Owner/Admin of the
/// platform operator's tenant (the lowest-id / bootstrap tenant). This is the
/// only principal allowed to clamp *another* tenant's quota, so it matches the
/// cross-tenant guard used for the global routing infra
/// ([`crate::web::admin_routing_handlers`]). Fails **closed** — a DB error
/// resolving the platform tenant yields `403`, never an open door.
async fn require_super_admin(
    pg: &kb_store::PgStore,
    auth_user: &AuthUser,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    // Must at least hold an elevated role within the tenant.
    if !matches!(auth_user.role, UserRole::Owner | UserRole::Admin) {
        return Err(forbidden());
    }
    match pg.is_platform_admin_tenant(auth_user.tenant_id).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(forbidden()),
        Err(e) => {
            tracing::warn!(error = %e, "failed to resolve platform-admin tenant; denying");
            Err(forbidden())
        }
    }
}

// ── Error helpers ──────────────────────────────────────────────────────────────

/// Build a `403 Forbidden` JSON error.
fn forbidden() -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::FORBIDDEN,
        Json(ErrorResponse {
            error: "forbidden".into(),
            message: "super-admin privileges required".into(),
        }),
    )
}

/// Build a `404 Not Found` JSON error.
fn not_found(code: &str, msg: &str) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: code.into(),
            message: msg.into(),
        }),
    )
}

/// Build a `500 Internal Server Error` JSON error (logging the cause).
fn internal_error(e: impl std::fmt::Display) -> (StatusCode, Json<ErrorResponse>) {
    tracing::error!(error = %e, "admin quota-override handler error");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            error: "internal_error".into(),
            message: "an unexpected error occurred".into(),
        }),
    )
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::Arc;
    use std::time::Duration;

    use tower::ServiceExt;

    use super::*;
    use crate::middleware::auth_middleware;

    /// An `AppState` wired to a never-connected `PgStore` (DB calls error out).
    fn test_app_state() -> Arc<AppState> {
        let session_store: Arc<dyn kb_core::session::SessionStore> =
            Arc::new(kb_store::session_store::InMemorySessionStore::new());
        let pg_store = Arc::new(kb_store::PgStore::new(
            "postgres://localhost/test?sslmode=disable",
        ));
        Arc::new(AppState::new(
            session_store,
            pg_store,
            Some(Duration::from_secs(3600)),
            false,
        ))
    }

    /// Build a one-route router for the quota-override endpoint behind auth.
    fn router(state: Arc<AppState>) -> axum::Router {
        axum::Router::new()
            .route(
                "/admin/tenants/{id}/quota-override",
                axum::routing::post(set_quota_override),
            )
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ))
            .with_state(state)
    }

    /// Issue a POST with a session cookie + JSON body.
    fn post_with_session(token: &str, body: &str) -> axum::http::Request<axum::body::Body> {
        axum::http::Request::builder()
            .method(axum::http::Method::POST)
            .uri("/admin/tenants/7/quota-override")
            .header("Content-Type", "application/json")
            .header("Cookie", format!("__Host-session={token}"))
            .body(axum::body::Body::from(body.to_owned()))
            .unwrap()
    }

    #[tokio::test]
    async fn unauthenticated_returns_401() {
        let state = test_app_state();
        let req = axum::http::Request::builder()
            .method(axum::http::Method::POST)
            .uri("/admin/tenants/7/quota-override")
            .header("Content-Type", "application/json")
            .body(axum::body::Body::from(
                r#"{"quota_override_tokens":1000}"#.to_owned(),
            ))
            .unwrap();
        let resp = router(state).oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn member_role_returns_403() {
        let state = test_app_state();
        // A Member is never a super-admin: rejected on the role check before any
        // DB lookup (so a disconnected store still yields a deterministic 403).
        let token = state
            .session_store
            .create(1, 50, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let resp = router(state)
            .oneshot(post_with_session(
                &token,
                r#"{"quota_override_tokens":1000}"#,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn owner_but_db_unavailable_fails_closed_403() {
        let state = test_app_state();
        // An Owner of *some* tenant, but the platform-admin lookup can't run
        // (DB down) → fail closed with 403, never an open door.
        let token = state
            .session_store
            .create(2, 9, UserRole::Owner, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let resp = router(state)
            .oneshot(post_with_session(
                &token,
                r#"{"quota_override_bytes":1000000,"quota_override_tokens":1000}"#,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn error_helpers_have_expected_statuses() {
        assert_eq!(forbidden().0, StatusCode::FORBIDDEN);
        assert_eq!(not_found("x", "y").0, StatusCode::NOT_FOUND);
        assert_eq!(internal_error("boom").0, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn request_deserializes_with_missing_fields() {
        // Empty object → both overrides cleared (None).
        let req: QuotaOverrideRequest = serde_json::from_str("{}").unwrap();
        assert_eq!(req.quota_override_bytes, None);
        assert_eq!(req.quota_override_tokens, None);

        // Only tokens provided.
        let req: QuotaOverrideRequest =
            serde_json::from_str(r#"{"quota_override_tokens":1000}"#).unwrap();
        assert_eq!(req.quota_override_bytes, None);
        assert_eq!(req.quota_override_tokens, Some(1000));

        // Both provided.
        let req: QuotaOverrideRequest = serde_json::from_str(
            r#"{"quota_override_bytes":1000000,"quota_override_tokens":1000}"#,
        )
        .unwrap();
        assert_eq!(req.quota_override_bytes, Some(1_000_000));
        assert_eq!(req.quota_override_tokens, Some(1000));
    }

    #[test]
    fn response_serializes_echoing_inputs() {
        let body = serde_json::to_string(&QuotaOverrideResponse {
            tenant_id: 7,
            quota_override_bytes: Some(1_000_000),
            quota_override_tokens: Some(1000),
        })
        .unwrap();
        assert!(body.contains("\"tenant_id\":7"));
        assert!(body.contains("\"quota_override_bytes\":1000000"));
        assert!(body.contains("\"quota_override_tokens\":1000"));
    }
}
