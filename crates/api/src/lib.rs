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
pub mod email_jobs;
pub mod handlers;
pub mod metrics_collector;
pub mod middleware;
pub mod stripe_client;
pub mod web;

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use axum::Json;
use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::middleware::{from_fn, from_fn_with_state};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use kb_core::api_token::ApiTokenStore;
use kb_core::blob::Blob;
use kb_core::degradation::DegradationState;
use kb_core::email::{EmailProvider, EmailSender};
use kb_core::session::SessionStore;
use kb_pipeline::RetrievalPipeline;
use kb_pipeline::ingest::IngestPipeline;
use kb_pipeline::job_queue::JobQueue;
use kb_scheduler::Pool;
use kb_store::PgStore;

use crate::backpressure::InflightLimiter;
use crate::middleware::{
    auth_middleware, cors_layer, http_metrics_middleware, login_rate_limit_middleware,
    request_id_middleware,
};
use crate::stripe_client::StripeClient;

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
    /// Stripe client for billing operations (P11-T2+). Handlers return 500 when
    /// `None` and billing is requested.
    pub stripe_client: Option<Arc<dyn StripeClient>>,
    /// Stripe webhook signing secret (`whsec_…`), hot-swappable so an operator can
    /// rotate it without restarting (P11-T3+). Wrapped in `Arc` so [`AppState`]
    /// stays `Clone`. When `None`, the webhook endpoint returns 500.
    pub stripe_webhook_secret: Option<Arc<ArcSwap<String>>>,
    /// Publicly-reachable base URL of this server, used to construct Stripe
    /// Checkout success / cancel callback URLs (P11-T2+).
    /// Default: `http://localhost:9999`.
    pub public_base_url: String,
    /// Email sender for verification, password reset, and notifications (P12-T2).
    /// When `None`, email-sending handlers return 500. Set via [`AppState::with_email_sender`].
    pub email_sender: Option<Arc<dyn EmailSender>>,
    /// Low-level email provider used by the email job worker (P12-T7).
    /// Separate from `email_sender` — the job worker calls [`EmailProvider::send`]
    /// directly with pre-rendered HTML. When `None`, email jobs will dead-letter.
    /// Set via [`AppState::with_email_provider`].
    pub email_provider: Option<Arc<dyn EmailProvider>>,
    /// API token store for Bearer-auth (P12-T5).
    /// When `None`, Bearer authentication is disabled (cookie-only mode) and the
    /// /api/tokens CRUD endpoints return 500. Set via [`AppState::with_api_token_store`].
    pub api_token_store: Option<Arc<dyn ApiTokenStore>>,
    /// Hot-swappable configuration handle (P15-T4). Handlers resolve the
    /// current ingest mode / queue caps **per request** via
    /// `app_config.current()` (the hot-swap rule); `None` (tests, auth-only
    /// configs) falls back to compiled defaults.
    pub app_config: Option<kb_config::AppConfig>,
}

impl AppState {
    /// Build a minimal `AppState` suitable for auth-only routes and existing tests.
    ///
    /// `session_ttl` defaults to [`DEFAULT_SESSION_TTL_SECS`](kb_core::session::DEFAULT_SESSION_TTL_SECS)
    /// when `None` is passed. Pipeline fields are initialised to `None` — use
    /// the `with_*` builder methods or [`AppState::full`] to set them.
    ///
    /// `blob_presigned_ttl` defaults to [`DEFAULT_PRESIGNED_TTL_SECS`] (3600 s = 1 hour).
    /// `public_base_url` defaults to `"http://localhost:9999"`.
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
            stripe_client: None,
            stripe_webhook_secret: None,
            public_base_url: "http://localhost:9999".into(),
            email_sender: None,
            email_provider: None,
            api_token_store: None,
            app_config: None,
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
            stripe_client: None,
            stripe_webhook_secret: None,
            public_base_url: "http://localhost:9999".into(),
            email_sender: None,
            email_provider: None,
            api_token_store: None,
            app_config: None,
        }
    }

    /// Builder: attach the hot-swappable config handle (P15-T4).
    #[must_use]
    pub fn with_app_config(mut self, cfg: kb_config::AppConfig) -> Self {
        self.app_config = Some(cfg);
        self
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

    /// Builder: attach the Stripe client for billing operations (P11-T2).
    #[must_use]
    pub fn with_stripe_client(mut self, client: Arc<dyn StripeClient>) -> Self {
        self.stripe_client = Some(client);
        self
    }

    /// Builder: set the publicly-reachable base URL for Stripe callback
    /// construction (P11-T2). Default is `http://localhost:9999`.
    #[must_use]
    pub fn with_public_base_url(mut self, url: impl Into<String>) -> Self {
        self.public_base_url = url.into();
        self
    }

    /// Builder: attach the email sender for verification, password reset, and
    /// notifications (P12-T2). When `None`, email endpoints return 500.
    #[must_use]
    pub fn with_email_sender(mut self, sender: Arc<dyn EmailSender>) -> Self {
        self.email_sender = Some(sender);
        self
    }

    /// Builder: attach the low-level email provider for the email job worker
    /// (P12-T7). When `None`, email jobs will dead-letter.
    #[must_use]
    pub fn with_email_provider(mut self, provider: Arc<dyn EmailProvider>) -> Self {
        self.email_provider = Some(provider);
        self
    }

    /// Builder: attach the Stripe webhook signing secret for signature
    /// verification (P11-T3). The secret is stored behind an [`ArcSwap`] so
    /// it can be rotated at runtime without restarting the server
    /// (CLAUDE.md hot-swappable rule).
    #[must_use]
    pub fn with_stripe_webhook_secret(mut self, secret: impl Into<String>) -> Self {
        self.stripe_webhook_secret = Some(Arc::new(ArcSwap::new(Arc::new(secret.into()))));
        self
    }

    /// Builder: attach the API token store for Bearer-auth (P12-T5).
    /// When not set, Bearer authentication is disabled and /api/tokens
    /// endpoints return 500.
    #[must_use]
    pub fn with_api_token_store(mut self, store: Arc<dyn ApiTokenStore>) -> Self {
        self.api_token_store = Some(store);
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
/// | GET    | `/billing/checkout`            | yes   | Stripe Checkout via query param (BUG-UPGR-01) |
/// | POST   | `/billing/checkout`            | yes   | Stripe Checkout Session (P11-T2)     |
/// | GET    | `/billing/success`             | no    | Post-Checkout interstitial (P11-T2)  |
/// | GET    | `/billing/cancel`              | no    | Checkout-cancel redirect (P11-T2)    |
/// | GET    | `/billing/portal`              | yes   | Stripe Customer Portal (P11-T6)      |
/// | POST   | `/billing/portal`              | yes   | Stripe Customer Portal HTMX (P11-T6) |
/// | POST   | `/stripe/webhook`              | no    | Stripe webhook handler (P11-T3)      |
/// | GET    | `/metrics`                     | no    | Prometheus metrics (plan §15)        |
/// | GET    | `/health`                      | no    | Readiness probe (deep, dependency-aware) |
/// | GET    | `/live`                        | no    | Liveness probe (process-alive only)  |
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
        .route("/billing/success", get(handlers::billing::get_success))
        .route("/billing/cancel", get(handlers::billing::get_cancel))
        .route("/stripe/webhook", post(handlers::webhook::post_webhook))
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .route("/live", get(live_handler))
        .layer(from_fn(login_rate_limit_middleware));

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
        .route(
            "/api/tokens",
            post(handlers::api_tokens::create_token).get(handlers::api_tokens::list_tokens),
        )
        .route(
            "/api/tokens/{id}",
            axum::routing::delete(handlers::api_tokens::revoke_token),
        )
        .route("/auth/logout", post(handlers::auth::logout))
        .route(
            "/billing/checkout",
            axum::routing::get(handlers::billing::get_checkout)
                .post(handlers::billing::post_checkout),
        )
        .route(
            "/billing/portal",
            get(handlers::billing::get_portal).post(handlers::billing::post_portal),
        )
        // Super-admin quota override (P14-T14): JSON, no CSRF (matches the
        // `/api`-style surface); gated to the platform operator's tenant inside
        // the handler. Lets the operator/harness clamp any tenant's caps.
        .route(
            "/admin/tenants/{id}/quota-override",
            post(handlers::admin_quota::set_quota_override),
        )
        .layer(DefaultBodyLimit::max(
            (handlers::ingest::MAX_PAYLOAD_BYTES + handlers::ingest::MULTIPART_FRAMING_HEADROOM)
                as usize,
        ))
        .layer(from_fn_with_state(state.clone(), auth_middleware));

    // Web UI routes (P6-T4).
    let web = web::build_web_router(state.clone());

    // Merge public + protected API + Web UI, attach shared state.
    // The degradation middleware wraps the entire router so every
    // response carries X-Degraded when applicable.
    // The CORS layer intercepts OPTIONS preflight requests before they reach
    // any route-level middleware (auth, etc.).
    // The request-id layer is outermost so every request (including CORS
    // preflights and degradation handling) runs inside the correlation span
    // and carries `request_id` in its logs (plan §18, P14-T11).
    public
        .merge(api)
        .merge(web)
        // Innermost added layer → runs closest to routing, so the `MatchedPath`
        // extension is populated and HTTP RED metrics get per-route labels.
        .layer(from_fn(http_metrics_middleware))
        .layer(from_fn_with_state(
            state.clone(),
            degradation_middleware::degradation_middleware,
        ))
        .layer(cors_layer())
        // Outermost: stamp/propagate the correlation id and open the request span.
        .layer(from_fn(request_id_middleware))
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

    // Roles with no healthy backend, named (campaign finding F7). Empty when no
    // pool is configured (e.g. auth-only configs), so the check is skipped.
    let unhealthy_roles = state
        .backend_pool
        .as_ref()
        .map(|pool| unhealthy_backend_roles(pool))
        .unwrap_or_default();
    let backends_ok = unhealthy_roles.is_empty();

    // Degraded subsystems = the in-memory DegradationState set (circuit breakers,
    // P8-T9) ∪ any backend role with no healthy backend (F7), so /health names
    // *which* subsystem is degraded instead of leaving the list empty.
    let mut degraded_subsystems = state
        .degradation
        .as_ref()
        .map(|ds| ds.degraded_subsystems())
        .unwrap_or_default();
    for role in unhealthy_roles {
        if !degraded_subsystems.contains(&role) {
            degraded_subsystems.push(role);
        }
    }
    // `degradation.ok` mirrors the (now backend-inclusive) subsystem list, so it
    // can never read `ok:true` while a subsystem is named.
    let degradation_ok = degraded_subsystems.is_empty();

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

// ── Liveness handler ────────────────────────────────────────────────────────────

/// Liveness probe (k8s-style, BUG-OBS-02).
///
/// Returns 200 OK whenever the process is alive and able to respond to HTTP.
/// Unlike [`health_handler`] (the *readiness* probe), this endpoint performs
/// **no** dependency checks — it never inspects the database, backends,
/// migrations, or degradation state, and therefore never returns 503.
///
/// This separation matters for orchestrators: a Kubernetes liveness probe
/// pointed at `/health` (which returns 503 when any dependency is degraded)
/// would restart-loop the pod during a transient backend or DB outage,
/// defeating restart-based recovery. A liveness probe must only confirm the
/// process itself is alive and responding; readiness — "can it serve traffic?"
/// — belongs on `/health`.
///
/// The body deliberately omits any readiness keys (`database`, `backends`,
/// `migrations`, `degradation`) so callers cannot mistake it for a readiness
/// check.
///
/// No authentication is required.
async fn live_handler() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::OK,
        Json(serde_json::json!({ "status": "alive" })),
    )
}

/// Names of the roles the pool serves that currently have **no** healthy backend
/// — the canonical role names (`"text"`, `"vision"`, `"code"`, `"embed"`,
/// `"rerank"`), matching the subsystem names published as `kb_subsystem_degraded`.
///
/// [`health_handler`] folds these into `degradation.subsystems` so an operator can
/// see *which* role is down. Without it, a dead backend surfaced only as
/// `backends:false` while `degradation` reported `{ok:true, subsystems:[]}`
/// (campaign finding F7).
fn unhealthy_backend_roles(pool: &kb_scheduler::Pool) -> Vec<&'static str> {
    let mut out = Vec::new();
    for role in pool.roles() {
        // Capture the name before `backends_for` consumes `role` by value.
        let name = role.as_str();
        let any_healthy = pool
            .backends_for(role)
            .iter()
            .any(|b| b.healthy.load(std::sync::atomic::Ordering::Acquire));
        if !any_healthy {
            out.push(name);
        }
    }
    out
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
    /// Whether the user's email has been verified (P12-T2).
    pub email_verified: bool,
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
            stripe_client: None,
            stripe_webhook_secret: None,
            public_base_url: "http://localhost:9999".into(),
            email_sender: None,
            email_provider: None,
            api_token_store: None,
            app_config: None,
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

    /// The liveness endpoint returns 200 even with an unconnected DB and no
    /// backends — it must never depend on readiness.
    #[tokio::test]
    async fn live_returns_200_regardless_of_dependencies() {
        let state = unconnected_state();
        let router = Router::new()
            .route("/live", get(live_handler))
            .with_state(state);

        let response = router
            .oneshot(Request::builder().uri("/live").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    /// The liveness body must not carry readiness-level dependency checks
    /// (`database`, `backends`, `migrations`, `degradation`).
    #[tokio::test]
    async fn live_body_omits_readiness_keys() {
        let (status, Json(body)) = live_handler().await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "alive");
        for key in ["database", "backends", "migrations", "degradation"] {
            assert!(
                body.get(key).is_none(),
                "liveness body must not include readiness key {key:?}"
            );
        }
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
        assert!(unhealthy_backend_roles(&pool).is_empty());
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
        assert!(!unhealthy_backend_roles(&pool).is_empty());
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
        assert!(unhealthy_backend_roles(&pool).is_empty());
    }

    /// `check_backend_readiness` with no backends at all returns `true` (no roles
    /// to fail).
    #[test]
    fn check_backend_readiness_empty_pool() {
        let pool = kb_scheduler::Pool::new(vec![], Duration::from_secs(5));
        assert!(unhealthy_backend_roles(&pool).is_empty());
    }

    /// F7: a role with no healthy backend is named (so `/health` can fold it into
    /// `degradation.subsystems` instead of leaving the list empty).
    #[test]
    fn unhealthy_backend_roles_names_only_the_down_role() {
        let text = Arc::new(test_backend(
            "t",
            "http://x:1",
            vec![kb_core::role::Role::Text],
            0,
            1,
        ));
        let embed = Arc::new(test_backend(
            "e",
            "http://x:2",
            vec![kb_core::role::Role::Embed],
            0,
            1,
        ));
        text.healthy.store(false, Ordering::Release);
        let pool = kb_scheduler::Pool::new(
            vec![Arc::clone(&text), Arc::clone(&embed)],
            Duration::from_secs(5),
        );
        assert_eq!(
            unhealthy_backend_roles(&pool),
            vec!["text"],
            "only the down role is named; the healthy embed role is not"
        );
    }

    /// An all-healthy pool names no degraded subsystem.
    #[test]
    fn unhealthy_backend_roles_empty_when_all_healthy() {
        let text = Arc::new(test_backend(
            "t",
            "http://x:1",
            vec![kb_core::role::Role::Text],
            0,
            1,
        ));
        let pool = kb_scheduler::Pool::new(vec![Arc::clone(&text)], Duration::from_secs(5));
        assert!(unhealthy_backend_roles(&pool).is_empty());
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
