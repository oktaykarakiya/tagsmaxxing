//! `kb-api`: the axum HTTP API for the Local File Knowledge Base.
//!
//! This crate provides the auth middleware (cookie-based session validation),
//! the auth endpoints (`/auth/login`, `/auth/register`, `/auth/logout`),
//! the bootstrap seed (first-run tenant + admin user creation), the ingest /
//! search / document / job status endpoints (plan §10, §12, §13), and the
//! [`AppState`] handle shared across all request handlers.
//!
//! The router constructed by [`build_router`] is intended to be served on
//! **port 9999** (the configured default, per plan §12).

pub mod bootstrap;
pub mod cli;
pub mod handlers;
pub mod metrics_collector;
pub mod middleware;
pub mod web;

use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::http::StatusCode;
use axum::middleware::from_fn_with_state;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use kb_core::blob::Blob;
use kb_core::session::SessionStore;
use kb_pipeline::RetrievalPipeline;
use kb_pipeline::ingest::IngestPipeline;
use kb_pipeline::job_queue::JobQueue;
use kb_scheduler::Pool;
use kb_store::PgStore;

use crate::middleware::auth_middleware;

// ── AppState ───────────────────────────────────────────────────────────────────

/// Shared application state passed to every axum handler via [`axum::extract::State`].
///
/// All fields are `Arc`-wrapped so the state can be cheaply cloned across axum worker
/// tasks. The session secret is validated at construction (≥32 bytes, per
/// [`SessionSecret`](kb_core::auth::SessionSecret)).
///
/// # Pipeline components
///
/// Fields added in P6-T2 (`ingest_pipeline`, `retrieval_pipeline`, `blob`, `job_queue`)
/// are `Option`al — existing tests that construct a minimal `AppState` for auth-only
/// routes will leave them as `None`. Handlers that require them return `500` with a
/// clear message when the component is missing.
#[derive(Clone)]
pub struct AppState {
    /// Session persistence backend (in-memory for dev, Postgres for prod).
    pub session_store: Arc<dyn SessionStore>,
    /// Postgres store for user / tenant CRUD and document/file/job queries.
    pub pg_store: Arc<PgStore>,
    /// Session TTL for new sessions and sliding expiration.
    pub session_ttl: Duration,
    /// Whether `Secure` is set on session cookies (`false` for local dev without TLS).
    pub secure_cookies: bool,
    /// Ingestion pipeline (P6-T2+). Handlers return 500 when `None`.
    pub ingest_pipeline: Option<Arc<IngestPipeline>>,
    /// Retrieval pipeline (P6-T2+). Handlers return 500 when `None`.
    pub retrieval_pipeline: Option<Arc<RetrievalPipeline>>,
    /// Content-addressed blob store for file upload/download (P6-T2+).
    pub blob: Option<Arc<dyn Blob>>,
    /// Durable job queue for async ingest jobs (P6-T2+).
    pub job_queue: Option<Arc<JobQueue>>,
    /// Scheduler backend pool for admin backend status (P6-T8+).
    pub backend_pool: Option<Arc<Pool>>,
}

impl AppState {
    /// Build a minimal `AppState` suitable for auth-only routes and existing tests.
    ///
    /// `session_ttl` defaults to [`DEFAULT_SESSION_TTL_SECS`](kb_core::session::DEFAULT_SESSION_TTL_SECS)
    /// when `None` is passed. Pipeline fields are initialised to `None` — use
    /// the `with_*` builder methods or [`AppState::full`] to set them.
    #[must_use]
    pub fn new(
        session_store: Arc<dyn SessionStore>,
        pg_store: Arc<PgStore>,
        session_ttl: Option<Duration>,
        secure_cookies: bool,
    ) -> Self {
        Self {
            session_store,
            pg_store,
            session_ttl: session_ttl.unwrap_or(Duration::from_secs(
                kb_core::session::DEFAULT_SESSION_TTL_SECS,
            )),
            secure_cookies,
            ingest_pipeline: None,
            retrieval_pipeline: None,
            blob: None,
            job_queue: None,
            backend_pool: None,
        }
    }

    /// Build a fully-populated `AppState` with all pipeline components.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn full(
        session_store: Arc<dyn SessionStore>,
        pg_store: Arc<PgStore>,
        session_ttl: Option<Duration>,
        secure_cookies: bool,
        ingest_pipeline: Arc<IngestPipeline>,
        retrieval_pipeline: Arc<RetrievalPipeline>,
        blob: Arc<dyn Blob>,
        job_queue: Arc<JobQueue>,
    ) -> Self {
        Self {
            session_store,
            pg_store,
            session_ttl: session_ttl.unwrap_or(Duration::from_secs(
                kb_core::session::DEFAULT_SESSION_TTL_SECS,
            )),
            secure_cookies,
            ingest_pipeline: Some(ingest_pipeline),
            retrieval_pipeline: Some(retrieval_pipeline),
            blob: Some(blob),
            job_queue: Some(job_queue),
            backend_pool: None,
        }
    }

    /// Builder: attach the ingest pipeline.
    #[must_use]
    pub fn with_ingest_pipeline(mut self, p: Arc<IngestPipeline>) -> Self {
        self.ingest_pipeline = Some(p);
        self
    }

    /// Builder: attach the retrieval pipeline.
    #[must_use]
    pub fn with_retrieval_pipeline(mut self, p: Arc<RetrievalPipeline>) -> Self {
        self.retrieval_pipeline = Some(p);
        self
    }

    /// Builder: attach the blob store.
    #[must_use]
    pub fn with_blob(mut self, b: Arc<dyn Blob>) -> Self {
        self.blob = Some(b);
        self
    }

    /// Builder: attach the job queue.
    #[must_use]
    pub fn with_job_queue(mut self, q: Arc<JobQueue>) -> Self {
        self.job_queue = Some(q);
        self
    }

    /// Builder: attach the scheduler backend pool for admin backend status (P6-T8+).
    #[must_use]
    pub fn with_backend_pool(mut self, p: Arc<Pool>) -> Self {
        self.backend_pool = Some(p);
        self
    }
}

// ── Router ─────────────────────────────────────────────────────────────────────

/// Build the full axum [`Router`] for the API + Web UI, mounted at the given
/// `AppState`.
///
/// Route table — JSON API:
///
/// | Method | Path                           | Auth? | Description                          |
/// |--------|--------------------------------|-------|--------------------------------------|
/// | POST   | `/auth/login`                  | no    | Verify password → session cookie     |
/// | POST   | `/auth/register`               | no    | Create user → session cookie         |
/// | POST   | `/auth/logout`                 | yes   | Revoke session → clear cookie        |
/// | POST   | `/api/ingest`                  | yes   | Multipart upload → enqueue job       |
/// | GET    | `/api/search`                  | yes   | Hybrid search → ranked hits          |
/// | GET    | `/api/documents/:id`           | yes   | Document detail + files + tags       |
/// | GET    | `/api/documents/:id/file/:file_id` | yes | Download a file's blob             |
/// | GET    | `/api/jobs/:id`                | yes   | Job status                           |
/// | GET    | `/metrics`                     | no    | Prometheus metrics (plan §15)        |
///
/// Route table — Web UI (P6-T4):
///
/// | Method | Path        | Auth? | Description                     |
/// |--------|-------------|-------|---------------------------------|
/// | GET    | `/`         | yes   | Redirect to /login or /search   |
/// | GET    | `/login`    | no    | Login form (HTML)               |
/// | POST   | `/login`    | no    | Login form submission           |
/// | GET    | `/register` | no    | Registration form (HTML)        |
/// | POST   | `/register` | no    | Registration form submission    |
/// | POST   | `/logout`  | yes   | Revoke session + redirect       |
pub fn build_router(state: AppState) -> Router {
    let state = Arc::new(state);

    // Public JSON API routes (no auth required).
    let public = Router::new()
        .route("/auth/login", post(handlers::auth::login))
        .route("/auth/register", post(handlers::auth::register))
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler));

    // Protected API routes (auth required, P6-T2+).
    let api = Router::new()
        .route("/api/ingest", post(handlers::ingest::ingest))
        .route("/api/search", get(handlers::search::search))
        .route(
            "/api/documents/{id}",
            get(handlers::documents::document_detail),
        )
        .route(
            "/api/documents/{id}/file/{file_id}",
            get(handlers::documents::file_download),
        )
        .route("/api/jobs/{id}", get(handlers::jobs::job_status))
        .route("/auth/logout", post(handlers::auth::logout))
        .layer(from_fn_with_state(state.clone(), auth_middleware));

    // Web UI routes (P6-T4).
    let web = web::build_web_router(state.clone());

    // Merge public + protected API + Web UI, attach shared state.
    public.merge(api).merge(web).with_state(state)
}

// ── Health handler ────────────────────────────────────────────────────────────

/// Liveness probe (plan §14, P7-T1).
///
/// Returns 200 OK with a minimal JSON body. This endpoint is used by the
/// Containerfile HEALTHCHECK instruction and by orchestrator probes. It
/// asserts only that the HTTP server is alive and accepting connections —
/// it does **not** check upstream dependencies (database, backends, blob store).
///
/// No authentication is required.
async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({"status": "ok"})))
}

// ── Metrics handler ──────────────────────────────────────────────────────────

/// Prometheus metrics endpoint (plan §15).
///
/// Returns the metrics in Prometheus text exposition format. No authentication
/// is required — secure this endpoint via network-level access control (firewall)
/// or a reverse-proxy in production.
async fn metrics_handler() -> impl IntoResponse {
    (StatusCode::OK, kb_metrics::render())
}

// ── AuthUser extractor ─────────────────────────────────────────────────────────

/// The authenticated user context, injected into request extensions by
/// [`auth_middleware`] and available to downstream handlers via
/// `Extension<AuthUser>`.
///
/// Handlers **must** not receive this type without the auth middleware layer —
/// missing extensions are a 500, not a 401 (the middleware gates access).
#[derive(Debug, Clone, Copy)]
pub struct AuthUser {
    /// The tenant this request is scoped to.
    pub tenant_id: i64,
    /// The authenticated user's id.
    pub user_id: i64,
    /// The user's role within the tenant.
    pub role: kb_core::user::UserRole,
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// The health handler returns 200 OK with a JSON `{"status":"ok"}` body.
    #[tokio::test]
    async fn health_handler_returns_ok() {
        let response = health_handler().await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
