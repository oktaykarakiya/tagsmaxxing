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

pub mod backpressure;
pub mod bootstrap;
pub mod cli;
pub mod degradation_middleware;
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
use kb_core::degradation::DegradationState;
use kb_core::session::SessionStore;
use kb_pipeline::RetrievalPipeline;
use kb_pipeline::ingest::IngestPipeline;
use kb_pipeline::job_queue::JobQueue;
use kb_scheduler::Pool;
use kb_store::PgStore;

use crate::backpressure::InflightLimiter;
use crate::middleware::auth_middleware;

/// Default presigned-URL TTL in seconds (1 hour, plan §20).
pub const DEFAULT_PRESIGNED_TTL_SECS: u64 = 3600;

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
    /// TTL for presigned download URLs (P8-T3). Default: 3600 seconds (1 hour).
    pub blob_presigned_ttl: Duration,
    /// Degradation state for graceful-degradation header + health endpoint (P8-T9).
    pub degradation: Option<Arc<DegradationState>>,
    /// In-flight ingest limiter for backpressure (P8-T9). `None` when disabled.
    pub inflight_limiter: Option<Arc<InflightLimiter>>,
}

impl AppState {
    /// Build a minimal `AppState` suitable for auth-only routes and existing tests.
    ///
    /// `session_ttl` defaults to [`DEFAULT_SESSION_TTL_SECS`](kb_core::session::DEFAULT_SESSION_TTL_SECS)
    /// when `None` is passed. Pipeline fields are initialised to `None` — use
    /// the `with_*` builder methods or [`AppState::full`] to set them.
    ///
    /// `blob_presigned_ttl` defaults to [`DEFAULT_PRESIGNED_TTL_SECS`] (3600 s = 1 hour).
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
            blob_presigned_ttl: Duration::from_secs(DEFAULT_PRESIGNED_TTL_SECS),
            degradation: None,
            inflight_limiter: None,
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
            blob_presigned_ttl: Duration::from_secs(DEFAULT_PRESIGNED_TTL_SECS),
            degradation: None,
            inflight_limiter: None,
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

    /// Builder: set the presigned-URL TTL (P8-T3). Default is
    /// [`DEFAULT_PRESIGNED_TTL_SECS`] (3600 s).
    #[must_use]
    pub fn with_blob_presigned_ttl(mut self, ttl: Duration) -> Self {
        self.blob_presigned_ttl = ttl;
        self
    }

    /// Builder: attach the degradation state (P8-T9).
    #[must_use]
    pub fn with_degradation(mut self, ds: Arc<DegradationState>) -> Self {
        self.degradation = Some(ds);
        self
    }

    /// Builder: attach the in-flight ingest limiter (P8-T9).
    #[must_use]
    pub fn with_inflight_limiter(mut self, limiter: Arc<InflightLimiter>) -> Self {
        self.inflight_limiter = Some(limiter);
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
/// | GET    | `/api/documents/:id/file/:file_id` | yes | Redirect to presigned download URL  |
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
    // The degradation middleware wraps the entire router so every
    // response carries X-Degraded when applicable.
    public
        .merge(api)
        .merge(web)
        .layer(from_fn_with_state(
            state.clone(),
            degradation_middleware::degradation_middleware,
        ))
        .with_state(state)
}

// ── Health handler ────────────────────────────────────────────────────────────

/// Readiness probe (plan §22, P8-T8 / P8-T9).
///
/// Returns 200 OK when the application is ready to serve traffic:
/// - Database is reachable (a trivial `SELECT 1` succeeds),
/// - Schema migrations have been applied,
/// - At least one healthy backend is registered for every configured role,
/// - No subsystem is in a degraded state (blob store, backend roles).
///
/// Returns 503 Service Unavailable with a JSON body describing which checks
/// failed when upstream dependencies are not ready. This endpoint is used by
/// load-balancer health checks (Caddy, orchestrators) and by the Containerfile
/// HEALTHCHECK instruction.
///
/// No authentication is required.
async fn health_handler(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
) -> (StatusCode, Json<serde_json::Value>) {
    let db_ok = state.pg_store.is_db_reachable().await;
    let migrations_ok = state.pg_store.are_migrations_applied().await;

    // Check that every configured role has ≥1 healthy backend.
    let backends_ok = if let Some(pool) = &state.backend_pool {
        check_backend_readiness(pool)
    } else {
        // No backend pool configured — skip the check (e.g. auth-only configs).
        true
    };

    // Check degradation state (P8-T9).
    let degradation_ok = state
        .degradation
        .as_ref()
        .is_none_or(|ds| !ds.is_degraded());
    let degraded_subsystems = state
        .degradation
        .as_ref()
        .map(|ds| ds.degraded_subsystems())
        .unwrap_or_default();

    let all_ok = db_ok && migrations_ok && backends_ok && degradation_ok;

    let body = serde_json::json!({
        "status": if all_ok { "ok" } else { "degraded" },
        "database": db_ok,
        "migrations": migrations_ok,
        "backends": backends_ok,
        "degradation": {
            "ok": degradation_ok,
            "subsystems": degraded_subsystems,
        },
    });

    let status = if all_ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (status, Json(body))
}

/// Returns `true` when every role the pool knows about has at least one healthy
/// backend.
fn check_backend_readiness(pool: &kb_scheduler::Pool) -> bool {
    for role in pool.roles() {
        let healthy_count = pool
            .backends_for(role)
            .iter()
            .filter(|b| b.healthy.load(std::sync::atomic::Ordering::Acquire))
            .count();
        if healthy_count == 0 {
            return false;
        }
    }
    true
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
    use axum::body::Body;
    use axum::http::Request;
    use kb_core::session::SessionStore;
    use kb_scheduler::test_backend;
    use kb_store::{InMemorySessionStore, PgStore};
    use std::sync::atomic::Ordering;
    use tower::ServiceExt;

    /// Build a minimal `AppState` for health-endpoint tests, with an
    /// unconnected `PgStore` so the database and migration checks fail.
    fn unconnected_state() -> Arc<AppState> {
        Arc::new(AppState {
            session_store: Arc::new(InMemorySessionStore::new()) as Arc<dyn SessionStore>,
            pg_store: Arc::new(PgStore::new("postgres://localhost/nonexistent")),
            session_ttl: Duration::from_secs(3600),
            secure_cookies: false,
            ingest_pipeline: None,
            retrieval_pipeline: None,
            blob: None,
            job_queue: None,
            backend_pool: None,
            blob_presigned_ttl: Duration::from_secs(DEFAULT_PRESIGNED_TTL_SECS),
            degradation: None,
            inflight_limiter: None,
        })
    }

    /// The readiness endpoint returns 503 when the database is unreachable and
    /// no migrations have been applied (unconnected PgStore).
    #[tokio::test]
    async fn health_returns_503_when_db_unreachable() {
        let state = unconnected_state();
        let router = Router::new()
            .route("/health", get(health_handler))
            .with_state(state);

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    /// `check_backend_readiness` returns `true` when every role has at least one
    /// healthy backend.
    #[test]
    fn check_backend_readiness_all_healthy() {
        let b1 = Arc::new(test_backend(
            "b1",
            "http://x:1",
            vec![kb_core::role::Role::Text],
            0,
            1,
        ));
        let b2 = Arc::new(test_backend(
            "b2",
            "http://x:2",
            vec![kb_core::role::Role::Embed],
            0,
            1,
        ));
        let pool = kb_scheduler::Pool::new(
            vec![Arc::clone(&b1), Arc::clone(&b2)],
            Duration::from_secs(5),
        );
        assert!(check_backend_readiness(&pool));
    }

    /// `check_backend_readiness` returns `false` when a role has no healthy backends.
    #[test]
    fn check_backend_readiness_one_unhealthy() {
        let b = Arc::new(test_backend(
            "b1",
            "http://x:1",
            vec![kb_core::role::Role::Text],
            0,
            1,
        ));
        b.healthy.store(false, Ordering::Release);
        let pool = kb_scheduler::Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));
        assert!(!check_backend_readiness(&pool));
    }

    /// `check_backend_readiness` returns `true` when a role has at least one
    /// healthy backend even if another of the same role is unhealthy.
    #[test]
    fn check_backend_readiness_partial_healthy() {
        let healthy = Arc::new(test_backend(
            "healthy",
            "http://x:1",
            vec![kb_core::role::Role::Text],
            0,
            1,
        ));
        let unhealthy = Arc::new(test_backend(
            "unhealthy",
            "http://x:2",
            vec![kb_core::role::Role::Text],
            1,
            1,
        ));
        unhealthy.healthy.store(false, Ordering::Release);
        let pool = kb_scheduler::Pool::new(
            vec![Arc::clone(&healthy), Arc::clone(&unhealthy)],
            Duration::from_secs(5),
        );
        assert!(check_backend_readiness(&pool));
    }

    /// `check_backend_readiness` with no backends at all returns `true` (no roles
    /// to fail).
    #[test]
    fn check_backend_readiness_empty_pool() {
        let pool = kb_scheduler::Pool::new(vec![], Duration::from_secs(5));
        assert!(check_backend_readiness(&pool));
    }

    /// The health endpoint returns 200 with a properly wired but unconnected DB when
    /// backends are healthy (the DB check fails, so overall 503 is expected).
    #[tokio::test]
    async fn health_includes_check_details_in_body() {
        let state = unconnected_state();
        let router = Router::new()
            .route("/health", get(health_handler))
            .with_state(state);

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body_bytes = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

        assert_eq!(body["status"], "degraded");
        assert_eq!(body["database"], false);
        assert_eq!(body["migrations"], false);
        assert_eq!(body["backends"], true); // no pool → check skipped → true
    }
}
