//! `kb-api`: the axum HTTP API for the Local File Knowledge Base.
//!
//! This crate provides the auth middleware (cookie-based session validation),
//! the auth endpoints (`/auth/login`, `/auth/register`, `/auth/logout`),
//! the bootstrap seed (first-run tenant + admin user creation), and the
//! [`AppState`] handle shared across all request handlers.
//!
//! The router constructed by [`build_router`] is intended to be served on
//! **port 9999** (the configured default, per plan §12).

pub mod bootstrap;
pub mod cli;
pub mod commands;
pub mod handlers;
pub mod middleware;

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::middleware::from_fn_with_state;
use axum::routing::post;
use kb_core::session::SessionStore;
use kb_store::PgStore;

use crate::middleware::auth_middleware;

// ── AppState ───────────────────────────────────────────────────────────────────

/// Shared application state passed to every axum handler via [`axum::extract::State`].
///
/// All fields are `Arc`-wrapped so the state can be cheaply cloned across axum worker
/// tasks. The session secret is validated at construction (≥32 bytes, per
/// [`SessionSecret`](kb_core::auth::SessionSecret)).
#[derive(Clone)]
pub struct AppState {
    /// Session persistence backend (in-memory for dev, Postgres for prod).
    pub session_store: Arc<dyn SessionStore>,
    /// Postgres store for user / tenant CRUD (register, login lookup, bootstrap).
    pub pg_store: Arc<PgStore>,
    /// Session TTL for new sessions and sliding expiration.
    pub session_ttl: Duration,
    /// Whether `Secure` is set on session cookies (`false` for local dev without TLS).
    pub secure_cookies: bool,
}

impl AppState {
    /// Build a new `AppState` value.
    ///
    /// `session_ttl` defaults to [`DEFAULT_SESSION_TTL_SECS`](kb_core::session::DEFAULT_SESSION_TTL_SECS)
    /// when `None` is passed.
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
        }
    }
}

// ── Router ─────────────────────────────────────────────────────────────────────

/// Build the full axum [`Router`] for the API, mounted at the given `AppState`.
///
/// Route table:
///
/// | Method | Path             | Auth? | Description                          |
/// |--------|------------------|-------|--------------------------------------|
/// | POST   | `/auth/login`    | no    | Verify password → session cookie     |
/// | POST   | `/auth/register` | no    | Create user → session cookie         |
/// | POST   | `/auth/logout`   | yes   | Revoke session → clear cookie        |
///
/// Future endpoints (search, ingest, admin) are added in later phases behind the
/// auth middleware.
pub fn build_router(state: AppState) -> Router {
    let state = Arc::new(state);

    // Public routes (no auth required).
    let public = Router::new()
        .route("/auth/login", post(handlers::auth::login))
        .route("/auth/register", post(handlers::auth::register));

    // Protected routes (auth required).
    let protected = Router::new()
        .route("/auth/logout", post(handlers::auth::logout))
        .layer(from_fn_with_state(state.clone(), auth_middleware));

    // Merge public + protected, attach shared state.
    public.merge(protected).with_state(state)
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
