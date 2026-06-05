//! Web UI module: Askama templates, CSRF protection, security headers, and
//! HTML page handlers for the Local File Knowledge Base.
//!
//! This module provides the server-rendered Web UI (plan §12) using:
//! - **Askama** for type-safe Jinja-like template rendering
//! - **Tailwind CSS** (CDN) for styling
//! - **HTMX** for async partial updates (search results, P6-T5)
//! - **CSRF tokens** on all mutating forms (double-submit cookie pattern)
//! - **Security headers** (CSP, X-Content-Type-Options, X-Frame-Options, etc.)
//!
//! # Architecture
//!
//! The web router lives alongside the JSON API router. When the application starts,
//! both routers are merged: web routes serve HTML pages at `/login`, `/register`,
//! `/logout`, `/search`, `/upload`, etc.; API routes serve JSON at `/api/*` and
//! `/auth/*`. The auth middleware gates the web's protected routes identically
//! to the API routes.
//!
//! # CSRF pattern
//!
//! Each page load generates a random 32-byte token, stores it in a `__Host-csrf`
//! cookie, and renders it in a hidden form field. On submission, the handler
//! compares the cookie and field values. Mismatches receive `403 Forbidden`.

pub(crate) mod admin_handlers;
pub(crate) mod admin_routing_handlers;
pub(crate) mod admin_routing_templates;
pub(crate) mod admin_templates;
pub(crate) mod csrf;
pub(crate) mod handlers;
pub(crate) mod handlers_account;
pub(crate) mod handlers_auth_web;
pub(crate) mod handlers_dashboard;
pub(crate) mod handlers_documents;
pub(crate) mod handlers_legal;
pub(crate) mod handlers_marketing;
pub(crate) mod security;
pub(crate) mod templates;

/// Build the web UI sub-router with all middleware and state.
///
/// The returned router carries its own [`Arc<AppState>`] and can be merged
/// into the main router via [`axum::Router::merge`].
///
/// Route table:
///
/// | Method | Path          | Auth? | Description                        |
/// |--------|---------------|-------|------------------------------------|
/// | GET    | `/`           | no*   | Landing page (or redirect to /search if authed) |
/// | GET    | `/pricing`    | no    | Pricing page with plans from DB    |
/// | GET    | `/features`   | no    | Features page with file type matrix|
/// | GET    | `/faq`        | no    | FAQ page with expandable questions |
/// | GET    | `/login`      | no    | Login form (HTML)                  |
/// | POST   | `/login`      | no    | Login form submission              |
/// | GET    | `/register`   | no    | Registration form (HTML)           |
/// | POST   | `/register`   | no    | Registration form submission       |
/// | POST   | `/logout`     | yes   | Revoke session, redirect           |
/// | GET    | `/search`     | yes   | Search page (P6-T5)                |
/// | POST   | `/search`     | yes   | HTMX search fragment (P6-T5)       |
/// | GET    | `/upload`     | yes   | Upload page (P6-T6)                |
/// | POST   | `/upload`   | yes   | Multipart upload submit (P6-T6) |
/// | GET    | `/documents/:id` | yes | Document detail page (P6-T7)  |
/// | POST   | `/documents/:id/tags/:tag_id/remove` | yes | Remove tag (P6-T7) |
/// | POST   | `/documents/:id/add-tag` | yes | Add tag by name (P6-T7) |
/// | POST   | `/documents/:id/retag` | yes | Enqueue retag job (P6-T7) |
/// | POST   | `/documents/:id/reorder` | yes | Reorder pages (P6-T7) |
/// | POST   | `/documents/:id/add-page` | yes | Upload additional page (P6-T7) |
pub(crate) fn build_web_router(
    state: std::sync::Arc<crate::AppState>,
) -> axum::Router<std::sync::Arc<crate::AppState>> {
    use axum::middleware::from_fn_with_state;
    use axum::routing::{get, post};

    use axum::extract::DefaultBodyLimit;
    use crate::handlers::ingest::MAX_PAYLOAD_BYTES;

    // Public web routes (no auth required).
    let public = axum::Router::new()
        .route("/", get(handlers_marketing::landing_page))
        .route("/pricing", get(handlers_marketing::pricing_page))
        .route("/features", get(handlers_marketing::features_page))
        .route("/faq", get(handlers_marketing::faq_page))
        // Legal pages (P12-T6): public, no auth required.
        .route("/terms", get(handlers_legal::terms_page))
        .route("/privacy", get(handlers_legal::privacy_page))
        .route("/dpa", get(handlers_legal::dpa_page))
        .route("/aup", get(handlers_legal::aup_page))
        .route("/cookies", get(handlers_legal::cookies_page))
        .route("/retention", get(handlers_legal::retention_page))
        .route(
            "/login",
            get(handlers::login_page).post(handlers::login_submit),
        )
        .route(
            "/register",
            get(handlers::register_page).post(handlers::register_submit),
        )
        // Auth flow (P12-T2): signup, email verification, password reset.
        .route(
            "/signup",
            get(handlers_auth_web::signup_page).post(handlers_auth_web::signup_submit),
        )
        .route("/verify", get(handlers_auth_web::verify_email))
        .route(
            "/forgot",
            get(handlers_auth_web::forgot_password_page)
                .post(handlers_auth_web::forgot_password_submit),
        )
        .route(
            "/reset",
            get(handlers_auth_web::reset_password_page)
                .post(handlers_auth_web::reset_password_submit),
        );

    // Protected web routes (auth required).
    let protected = axum::Router::new()
        .route(
            "/search",
            get(handlers::search_page).post(handlers::search_submit),
        )
        .route(
            "/upload",
            get(handlers::upload_page).post(handlers::upload_submit),
        )
        .route(
            "/documents/{id}",
            get(handlers_documents::document_detail_page),
        )
        .route(
            "/documents/{id}/tags/{tag_id}/remove",
            post(handlers_documents::remove_document_tag),
        )
        .route(
            "/documents/{id}/add-tag",
            post(handlers_documents::add_document_tag),
        )
        .route(
            "/documents/{id}/retag",
            post(handlers_documents::retag_document),
        )
        .route(
            "/documents/{id}/reorder",
            post(handlers_documents::reorder_pages),
        )
        .route(
            "/documents/{id}/add-page",
            post(handlers_documents::add_page),
        )
        .route("/logout", post(handlers::logout_web))
        // Dashboard (P12-T4)
        .route("/dashboard", get(handlers_dashboard::dashboard_page))
        // Account & billing pages (P12-T3)
        .route("/account", get(handlers_account::account_page))
        .route("/account/team", get(handlers_account::account_team_page))
        .route(
            "/account/team/invite",
            post(handlers_account::account_invite_user),
        )
        .route(
            "/account/team/role",
            post(handlers_account::account_change_role),
        )
        .route(
            "/account/team/remove",
            post(handlers_account::account_remove_user),
        )
        .route("/account/plan", get(handlers_account::account_plan_page))
        .route(
            "/account/danger",
            get(handlers_account::account_danger_page),
        )
        .route(
            "/account/danger/delete",
            post(handlers_account::account_delete),
        )
        .layer(from_fn_with_state(
            std::sync::Arc::clone(&state),
            crate::middleware::auth_middleware,
        ));

    // Admin panel routes (auth required + role-based access enforced in handlers).
    let admin = axum::Router::new()
        .route("/admin", get(admin_handlers::admin_dashboard))
        .route("/admin/tenants", get(admin_handlers::admin_tenants_page))
        .route("/admin/users", get(admin_handlers::admin_users_page))
        .route(
            "/admin/users/invite",
            post(admin_handlers::admin_invite_user),
        )
        .route(
            "/admin/users/role",
            post(admin_handlers::admin_change_user_role),
        )
        .route("/admin/jobs", get(admin_handlers::admin_jobs_page))
        .route(
            "/admin/jobs/{id}/retry",
            post(admin_handlers::admin_retry_job),
        )
        .route(
            "/admin/jobs/{id}/delete",
            post(admin_handlers::admin_delete_job),
        )
        .route("/admin/tags", get(admin_handlers::admin_tags_page))
        .route("/admin/tags/merge", post(admin_handlers::admin_merge_tags))
        // Decrypt-access audit viewer (P10-T5)
        .route(
            "/admin/decrypt-audit",
            get(admin_handlers::admin_decrypt_audit_page),
        )
        // Provider / Model / Route CRUD (P9-T8)
        .route(
            "/admin/providers",
            get(admin_routing_handlers::admin_providers_page),
        )
        .route(
            "/admin/providers/new",
            get(admin_routing_handlers::admin_provider_new_form),
        )
        .route(
            "/admin/providers",
            post(admin_routing_handlers::admin_provider_create),
        )
        .route(
            "/admin/providers/{id}/edit",
            get(admin_routing_handlers::admin_provider_edit_form),
        )
        .route(
            "/admin/providers/{id}",
            post(admin_routing_handlers::admin_provider_update),
        )
        .route(
            "/admin/providers/{id}/toggle",
            post(admin_routing_handlers::admin_provider_toggle),
        )
        .route(
            "/admin/providers/{id}/test",
            post(admin_routing_handlers::admin_provider_test),
        )
        .route(
            "/admin/models",
            get(admin_routing_handlers::admin_models_page),
        )
        .route(
            "/admin/models/new",
            get(admin_routing_handlers::admin_model_new_form),
        )
        .route(
            "/admin/models",
            post(admin_routing_handlers::admin_model_create),
        )
        .route(
            "/admin/models/{id}/edit",
            get(admin_routing_handlers::admin_model_edit_form),
        )
        .route(
            "/admin/models/{id}",
            post(admin_routing_handlers::admin_model_update),
        )
        .route(
            "/admin/models/{id}/toggle",
            post(admin_routing_handlers::admin_model_toggle),
        )
        .route(
            "/admin/routes",
            get(admin_routing_handlers::admin_routes_page),
        )
        .route(
            "/admin/routes/new",
            get(admin_routing_handlers::admin_route_new_form),
        )
        .route(
            "/admin/routes",
            post(admin_routing_handlers::admin_route_create),
        )
        .route(
            "/admin/routes/{id}/edit",
            get(admin_routing_handlers::admin_route_edit_form),
        )
        .route(
            "/admin/routes/{id}",
            post(admin_routing_handlers::admin_route_update),
        )
        .route(
            "/admin/routes/{id}/delete",
            post(admin_routing_handlers::admin_route_delete),
        )
        .layer(from_fn_with_state(
            std::sync::Arc::clone(&state),
            crate::middleware::auth_middleware,
        ));

    public
        .merge(protected)
        .merge(admin)
        .layer(DefaultBodyLimit::max(MAX_PAYLOAD_BYTES as usize))
        .layer(axum::middleware::from_fn(
            security::security_headers_middleware,
        ))
        .with_state(state)
}
