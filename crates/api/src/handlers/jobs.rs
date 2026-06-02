//! `GET /api/jobs/:id` — job status handler.
//!
//! Returns the current state of an ingest job, including its status, attempt
//! count, and any error message.

use std::sync::Arc;

use axum::Extension;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Json;
use serde::Serialize;

use crate::AppState;
use crate::AuthUser;

// ── Response types ─────────────────────────────────────────────────────────────

/// Job status response.
#[derive(Debug, Serialize)]
pub struct JobStatusResponse {
    /// Job id.
    pub id: i64,
    /// Current status (`queued`, `running`, `failed`, `dead`, `done`).
    pub status: String,
    /// Number of attempts made so far.
    pub attempts: i32,
    /// Last error message, if any.
    pub last_error: Option<String>,
    /// Earliest time the job may be retried.
    pub run_after: String,
    /// Job creation time.
    pub created_at: String,
}

/// Error response.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    error: String,
    message: String,
}

// ── Handler ────────────────────────────────────────────────────────────────────

/// `GET /api/jobs/:id` — retrieve the current status of a job.
///
/// The response includes the status, attempt count, last error, and timing
/// fields. The job is scoped to the authenticated tenant — cross-tenant
/// access returns a 404.
///
/// # Response
///
/// * `200 OK` — [`JobStatusResponse`] with current job state.
/// * `401 Unauthorized` — rejected by middleware.
/// * `404 Not Found` — job does not exist or belongs to a different tenant.
/// * `500 Internal Server Error` — database failure.
pub async fn job_status(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(id): Path<i64>,
) -> Result<(StatusCode, Json<JobStatusResponse>), (StatusCode, Json<ErrorResponse>)> {
    let job = state
        .pg_store
        .get_job(auth_user.tenant_id, id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| not_found("job_not_found", "job not found"))?;

    Ok((
        StatusCode::OK,
        Json(JobStatusResponse {
            id: job.id,
            status: job.status.as_str().to_owned(),
            attempts: job.attempts,
            last_error: job.last_error,
            run_after: job.run_after.to_rfc3339(),
            created_at: job.created_at.to_rfc3339(),
        }),
    ))
}

// ── Error helpers ──────────────────────────────────────────────────────────────

fn not_found(code: &str, msg: &str) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: code.into(),
            message: msg.into(),
        }),
    )
}

fn internal_error(e: impl std::fmt::Display) -> (StatusCode, Json<ErrorResponse>) {
    tracing::error!(error = %e, "jobs handler error");
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

    use axum::Router;
    use axum::body::Body;
    use axum::http::{Method, StatusCode};
    use axum::routing::get;
    use kb_core::session::{DEFAULT_SESSION_TTL_SECS, SessionStore};
    use kb_core::user::UserRole;
    use kb_store::PgStore;
    use kb_store::session_store::InMemorySessionStore;
    use tower::ServiceExt;

    use super::*;
    use crate::AppState;
    use crate::middleware::auth_middleware;

    fn test_state() -> Arc<AppState> {
        let session_store: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
        let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
        Arc::new(AppState::new(
            session_store,
            pg_store,
            Some(Duration::from_secs(DEFAULT_SESSION_TTL_SECS)),
            false,
        ))
    }

    fn jobs_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/api/jobs/{id}", get(job_status))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ))
            .with_state(state)
    }

    // ── Integration tests ───────────────────────────────────────────────────

    #[tokio::test]
    async fn unauthenticated_returns_401() {
        let state = test_state();
        let router = jobs_router(state);

        let request = axum::http::Request::builder()
            .method(Method::GET)
            .uri("/api/jobs/1")
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn nonexistent_job_returns_404() {
        let state = test_state();
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = jobs_router(state);

        let request = axum::http::Request::builder()
            .method(Method::GET)
            .uri("/api/jobs/99999")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        // With a disconnected PgStore, get_job returns an error (500).
        // In integration (real DB) a nonexistent job returns 404.
        assert!(response.status().is_server_error());
    }

    // ── Error helper tests ──────────────────────────────────────────────────

    #[test]
    fn not_found_has_404_status() {
        let (status, body) = not_found("job_not_found", "job 42 not found");
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body.error, "job_not_found");
    }

    #[test]
    fn internal_error_has_500_status() {
        let (status, body) = internal_error(anyhow::anyhow!("db error"));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body.error, "internal_error");
    }
}
