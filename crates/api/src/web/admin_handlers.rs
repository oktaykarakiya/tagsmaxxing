//! Admin-panel Web UI handlers (plan §15, ledger P6-T8).
//!
//! All handlers require authentication (the auth middleware runs first). Role-based
//! access is enforced inline:
//! - [`require_admin`] allows `Owner` and `Admin` roles (dashboard, users, jobs, tags).
//! - [`require_owner`] allows only `Owner` (tenant CRUD — super-admin).
//!
//! Every mutating handler writes an audit event via [`PgStore::admin_insert_audit_event`].

use std::sync::Arc;

use axum::Extension;
use axum::Form;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect, Response};
use kb_core::audit::AuditAction;
use kb_core::job::JobStatus;
use kb_core::user::UserRole;
use serde::Deserialize;

use crate::AppState;
use crate::AuthUser;

use super::admin_templates::*;
use super::csrf;
use super::handlers::{generate_fresh_csrf, render_ok, render_template};

// ── Form types ────────────────────────────────────────────────────────────────

/// Minimal form carrying only a CSRF token (used by retry/delete job buttons).
#[derive(Debug, Deserialize)]
pub struct CsrfOnlyForm {
    pub csrf_token: String,
}

/// Form: invite/create a user.
#[derive(Debug, Deserialize)]
pub struct InviteUserForm {
    pub csrf_token: String,
    pub email: String,
    pub role: String,
    pub password: String,
}

/// Form: change a user's role.
#[derive(Debug, Deserialize)]
pub struct ChangeRoleForm {
    pub csrf_token: String,
    pub user_id: i64,
    pub role: String,
}

/// Form: merge two tags.
#[derive(Debug, Deserialize)]
pub struct MergeTagsForm {
    pub csrf_token: String,
    pub from_tag_id: i64,
    pub to_tag_id: i64,
}

/// Query params for the jobs page filter.
#[derive(Debug, Deserialize)]
pub struct JobsFilterQuery {
    #[serde(default)]
    pub status: Option<String>,
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Return 403 if the authenticated user is not Owner or Admin.
fn require_admin(auth_user: &AuthUser) -> Result<(), StatusCode> {
    match auth_user.role {
        UserRole::Owner | UserRole::Admin => Ok(()),
        _ => Err(StatusCode::FORBIDDEN),
    }
}

/// Return 403 if the authenticated user is not Owner.
fn require_owner(auth_user: &AuthUser) -> Result<(), StatusCode> {
    match auth_user.role {
        UserRole::Owner => Ok(()),
        _ => Err(StatusCode::FORBIDDEN),
    }
}

/// Map `UserRole` from a form string, defaulting to Member on unknown values.
fn role_from_form(role_str: &str) -> UserRole {
    match role_str {
        "owner" => UserRole::Owner,
        "admin" => UserRole::Admin,
        _ => UserRole::Member,
    }
}

/// Look up the tenant name for the given id (or "Unknown" on error).
async fn tenant_name(pg: &kb_store::PgStore, tenant_id: i64) -> String {
    pg.get_tenant_slug(tenant_id)
        .await
        .unwrap_or_else(|_| "unknown".to_string())
}

/// Format optional quota as a display string.
fn quota_display(val: Option<i64>) -> String {
    match val {
        None => "unlimited".to_string(),
        Some(v) => v.to_string(),
    }
}

// ── GET /admin — dashboard ────────────────────────────────────────────────────

/// `GET /admin` — the admin dashboard: summary counts, backend status, recent
/// jobs, and recent audit events.
///
/// * `Owner` sees cross-tenant data (all tenant counts, all audit events).
/// * `Admin` sees only their own tenant's data.
pub async fn admin_dashboard(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Response, StatusCode> {
    require_admin(&auth_user)?;
    let pg = &state.pg_store;
    let is_super = matches!(auth_user.role, UserRole::Owner);
    let tid = auth_user.tenant_id;

    let tname = tenant_name(pg, tid).await;

    let tenant_count = if is_super {
        pg.tenant_count().await.unwrap_or(0)
    } else {
        0
    };

    let user_count = pg.admin_user_count(tid).await.unwrap_or(0);
    let document_count = pg.admin_document_count(tid).await.unwrap_or(0);
    let storage_used = pg.get_storage_usage(tid).await.unwrap_or(0);
    let token_usage = pg.get_token_usage(tid).await.unwrap_or(0);
    let monthly_spend_micros = pg.admin_monthly_spend(tid).await.unwrap_or(0);
    let budget_cents = pg.get_tenant_budget(tid).await.unwrap_or(None);
    let budget_display = match budget_cents {
        None => "unlimited".to_string(),
        Some(0) => "unlimited".to_string(),
        Some(c) => format!("${:.2}", c as f64 / 100.0),
    };

    // Recent jobs: up to 10
    let recent_jobs = match pg.admin_list_jobs(tid, None, 10).await {
        Ok(jobs) => jobs
            .into_iter()
            .map(|j| AdminJobRow {
                id: j.id,
                kind: j.kind.as_str().to_owned(),
                status: j.status.as_str().to_owned(),
                attempts: j.attempts,
                last_error: j.last_error.unwrap_or_default(),
                created_at: j.created_at.to_rfc3339(),
            })
            .collect(),
        Err(_) => vec![],
    };

    // Recent audit events: up to 10 (cross-tenant if super-admin)
    let audit_tid = if is_super { 0 } else { tid };
    let recent_audit = match pg.admin_list_audit_events(audit_tid, 10).await {
        Ok(events) => events
            .into_iter()
            .map(|e| AdminAuditRow {
                id: e.id,
                actor_user_id: e.actor_user_id,
                action: e.action.as_str().to_owned(),
                target_type: e.target_type,
                target_id_display: e
                    .target_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "—".to_string()),
                created_at: e.created_at.to_rfc3339(),
            })
            .collect(),
        Err(_) => vec![],
    };

    // Backend status: from the scheduler pool
    let backends = if let Some(ref pool) = state.backend_pool {
        pool.all_backends()
            .into_iter()
            .map(|b| {
                use kb_core::role::Role;
                let roles = Role::all()
                    .iter()
                    .filter(|r| b.supports(**r))
                    .map(|r| r.as_str().to_owned())
                    .collect::<Vec<_>>()
                    .join(", ");
                use std::sync::atomic::Ordering;
                let endpoint = b.endpoint.clone().unwrap_or_default();
                AdminBackendRow {
                    id: b.id.clone(),
                    base_url: endpoint,
                    healthy: b.healthy.load(Ordering::Relaxed),
                    free_slots: b.free(),
                    total_slots: b.max_concurrency,
                    roles,
                }
            })
            .collect()
    } else {
        vec![]
    };

    let csrf_token = csrf::generate_csrf_token().unwrap_or_default();
    let page = AdminDashboardPage {
        csrf_token: csrf_token.clone(),
        error: String::new(),
        tenant_name: tname,
        is_super_admin: is_super,
        tenant_count,
        user_count,
        document_count,
        storage_used_bytes: storage_used,
        token_usage,
        monthly_spend_micros,
        budget_display,
        recent_jobs,
        recent_audit,
        backends,
    };

    let mut resp = render_ok(&page);
    resp.headers_mut().insert(
        axum::http::header::SET_COOKIE,
        csrf::csrf_cookie_value(&csrf_token, state.session_ttl, state.secure_cookies),
    );
    Ok(resp)
}

// ── GET /admin/tenants ────────────────────────────────────────────────────────

/// `GET /admin/tenants` — list all tenants (Owner only).
pub async fn admin_tenants_page(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Response, StatusCode> {
    require_owner(&auth_user)?;
    let pg = &state.pg_store;

    let tenants = match pg.admin_list_tenants().await {
        Ok(list) => list
            .into_iter()
            .map(|t| {
                let budget = match t.budget_monthly_cents {
                    None => "unlimited".to_string(),
                    Some(c) if c <= 0 => "unlimited".to_string(),
                    Some(c) => format!("${:.2}", c as f64 / 100.0),
                };
                AdminTenantRow {
                    id: t.id,
                    slug: t.slug,
                    name: t.name,
                    quota_bytes_display: quota_display(t.quota_bytes),
                    quota_tokens_display: quota_display(t.quota_tokens),
                    budget_display: budget,
                    created_at: t.created_at.to_rfc3339(),
                }
            })
            .collect(),
        Err(e) => {
            tracing::error!(error = %e, "failed to list tenants for admin");
            vec![]
        }
    };

    let csrf_token = csrf::generate_csrf_token().unwrap_or_default();
    let page = AdminTenantsPage {
        csrf_token: csrf_token.clone(),
        error: String::new(),
        tenants,
    };

    let mut resp = render_ok(&page);
    resp.headers_mut().insert(
        axum::http::header::SET_COOKIE,
        csrf::csrf_cookie_value(&csrf_token, state.session_ttl, state.secure_cookies),
    );
    Ok(resp)
}

// ── GET /admin/users ──────────────────────────────────────────────────────────

/// `GET /admin/users` — list all users in the current tenant.
pub async fn admin_users_page(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Response, StatusCode> {
    require_admin(&auth_user)?;
    let pg = &state.pg_store;
    let tid = auth_user.tenant_id;
    let tname = tenant_name(pg, tid).await;

    let users = match pg.admin_list_users(tid).await {
        Ok(list) => list
            .into_iter()
            .map(|u| AdminUserRow {
                id: u.id,
                email: u.email,
                role: u.role.as_str().to_owned(),
                created_at: u.created_at.to_rfc3339(),
            })
            .collect(),
        Err(e) => {
            tracing::error!(error = %e, "failed to list users for admin");
            vec![]
        }
    };

    let is_super = matches!(auth_user.role, UserRole::Owner);
    let csrf_token = csrf::generate_csrf_token().unwrap_or_default();
    let page = AdminUsersPage {
        csrf_token: csrf_token.clone(),
        error: String::new(),
        tenant_name: tname,
        is_super_admin: is_super,
        users,
    };

    let mut resp = render_ok(&page);
    resp.headers_mut().insert(
        axum::http::header::SET_COOKIE,
        csrf::csrf_cookie_value(&csrf_token, state.session_ttl, state.secure_cookies),
    );
    Ok(resp)
}

// ── POST /admin/users/invite ──────────────────────────────────────────────────

/// `POST /admin/users/invite` — create a new user in the current tenant.
pub async fn admin_invite_user(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    headers: axum::http::HeaderMap,
    Form(form): Form<InviteUserForm>,
) -> Result<Response, StatusCode> {
    require_admin(&auth_user)?;
    let pg = &state.pg_store;
    let tid = auth_user.tenant_id;
    let is_super = matches!(auth_user.role, UserRole::Owner);

    // CSRF validation
    super::csrf::validate_csrf(&headers, &form.csrf_token).map_err(|_| StatusCode::FORBIDDEN)?;

    let role = role_from_form(&form.role);

    // Only Owners may grant the Owner role (privilege-escalation guard).
    if matches!(role, UserRole::Owner) && !matches!(auth_user.role, UserRole::Owner) {
        return admin_rerender_users(
            pg,
            tid,
            &state,
            "Only tenant owners can create owner accounts.",
        )
        .await;
    }

    if form.email.is_empty() || form.password.len() < 8 {
        let tname = tenant_name(pg, tid).await;
        let csrf_token = generate_fresh_csrf();
        let users = match pg.admin_list_users(tid).await {
            Ok(list) => list
                .into_iter()
                .map(|u| AdminUserRow {
                    id: u.id,
                    email: u.email,
                    role: u.role.as_str().to_owned(),
                    created_at: u.created_at.to_rfc3339(),
                })
                .collect(),
            Err(_) => vec![],
        };
        let page = AdminUsersPage {
            csrf_token: csrf_token.clone(),
            error: "Email is required and password must be at least 8 characters.".into(),
            tenant_name: tname,
            is_super_admin: is_super,
            users,
        };
        let mut resp = render_template(&page, StatusCode::OK);
        resp.headers_mut().insert(
            axum::http::header::SET_COOKIE,
            csrf::csrf_cookie_value(&csrf_token, state.session_ttl, state.secure_cookies),
        );
        return Ok(resp);
    }

    // Role was already parsed above (after CSRF validation) — reuse it.
    let password_hash = kb_core::auth::hash_password(&form.password).map_err(|e| {
        tracing::error!(error = %e, "failed to hash password in admin invite");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    match pg.create_user(tid, &form.email, &password_hash, role).await {
        Ok(user_id) => {
            // Audit log
            let _ = pg
                .admin_insert_audit_event(
                    auth_user.user_id,
                    tid,
                    AuditAction::UserCreated.as_str(),
                    "user",
                    Some(user_id),
                    &serde_json::json!({
                        "email": form.email,
                        "role": role.as_str()
                    }),
                )
                .await;
        }
        Err(e) => {
            tracing::warn!(error = %e, "admin invite user failed");
            let tname = tenant_name(pg, tid).await;
            let csrf_token = generate_fresh_csrf();
            let users = match pg.admin_list_users(tid).await {
                Ok(list) => list
                    .into_iter()
                    .map(|u| AdminUserRow {
                        id: u.id,
                        email: u.email,
                        role: u.role.as_str().to_owned(),
                        created_at: u.created_at.to_rfc3339(),
                    })
                    .collect(),
                Err(_) => vec![],
            };
            let page = AdminUsersPage {
                csrf_token: csrf_token.clone(),
                error: format!("Failed to create user: {e}"),
                tenant_name: tname,
                is_super_admin: is_super,
                users,
            };
            let mut resp = render_template(&page, StatusCode::OK);
            resp.headers_mut().insert(
                axum::http::header::SET_COOKIE,
                csrf::csrf_cookie_value(&csrf_token, state.session_ttl, state.secure_cookies),
            );
            return Ok(resp);
        }
    }

    Ok(Redirect::to("/admin/users").into_response())
}

// ── POST /admin/users/role ────────────────────────────────────────────────────

/// `POST /admin/users/role` — change a user's role.
pub async fn admin_change_user_role(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    headers: axum::http::HeaderMap,
    Form(form): Form<ChangeRoleForm>,
) -> Result<Response, StatusCode> {
    require_admin(&auth_user)?;
    let pg = &state.pg_store;
    let tid = auth_user.tenant_id;

    super::csrf::validate_csrf(&headers, &form.csrf_token).map_err(|_| StatusCode::FORBIDDEN)?;

    let new_role = role_from_form(&form.role);

    // Only Owners may grant the Owner role (privilege-escalation guard).
    if matches!(new_role, UserRole::Owner) && !matches!(auth_user.role, UserRole::Owner) {
        return Err(StatusCode::FORBIDDEN);
    }

    match pg.admin_update_user_role(tid, form.user_id, new_role).await {
        Ok(affected) => {
            if affected > 0 {
                let _ = pg
                    .admin_insert_audit_event(
                        auth_user.user_id,
                        tid,
                        AuditAction::UserRoleChanged.as_str(),
                        "user",
                        Some(form.user_id),
                        &serde_json::json!({
                            "new_role": new_role.as_str()
                        }),
                    )
                    .await;
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, user_id = form.user_id, "admin change user role failed");
        }
    }

    Ok(Redirect::to("/admin/users").into_response())
}

// ── GET /admin/jobs ───────────────────────────────────────────────────────────

/// `GET /admin/jobs` — list jobs, optionally filtered by status.
pub async fn admin_jobs_page(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(filter): Query<JobsFilterQuery>,
) -> Result<Response, StatusCode> {
    require_admin(&auth_user)?;
    let pg = &state.pg_store;
    let tid = auth_user.tenant_id;
    let is_super = matches!(auth_user.role, UserRole::Owner);

    let job_status = filter.status.as_deref().and_then(|s| {
        use std::str::FromStr;
        JobStatus::from_str(s).ok()
    });

    let filter_display = job_status
        .as_ref()
        .map(|s| s.as_str().to_owned())
        .unwrap_or_default();

    let jobs = match pg.admin_list_jobs(tid, job_status, 100).await {
        Ok(list) => list
            .into_iter()
            .map(|j| AdminJobRow {
                id: j.id,
                kind: j.kind.as_str().to_owned(),
                status: j.status.as_str().to_owned(),
                attempts: j.attempts,
                last_error: j.last_error.unwrap_or_default(),
                created_at: j.created_at.to_rfc3339(),
            })
            .collect(),
        Err(e) => {
            tracing::error!(error = %e, "failed to list jobs for admin");
            vec![]
        }
    };

    let csrf_token = csrf::generate_csrf_token().unwrap_or_default();
    let page = AdminJobsPage {
        csrf_token: csrf_token.clone(),
        error: String::new(),
        is_super_admin: is_super,
        filter_status: filter_display,
        jobs,
    };

    let mut resp = render_ok(&page);
    resp.headers_mut().insert(
        axum::http::header::SET_COOKIE,
        csrf::csrf_cookie_value(&csrf_token, state.session_ttl, state.secure_cookies),
    );
    Ok(resp)
}

// ── POST /admin/jobs/:id/retry ────────────────────────────────────────────────

/// `POST /admin/jobs/:id/retry` — reset a dead-letter job back to queued.
pub async fn admin_retry_job(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(job_id): Path<i64>,
    headers: axum::http::HeaderMap,
    Form(form): Form<CsrfOnlyForm>,
) -> Result<Response, StatusCode> {
    require_admin(&auth_user)?;
    super::csrf::validate_csrf(&headers, &form.csrf_token).map_err(|_| StatusCode::FORBIDDEN)?;

    let pg = &state.pg_store;
    let tid = auth_user.tenant_id;

    match pg.admin_retry_job(tid, job_id).await {
        Ok(affected) => {
            if affected > 0 {
                let _ = pg
                    .admin_insert_audit_event(
                        auth_user.user_id,
                        tid,
                        AuditAction::JobRetried.as_str(),
                        "job",
                        Some(job_id),
                        &serde_json::json!({}),
                    )
                    .await;
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, job_id = job_id, "admin retry job failed");
        }
    }

    Ok(Redirect::to("/admin/jobs?status=dead").into_response())
}

// ── POST /admin/jobs/:id/delete ───────────────────────────────────────────────

/// `POST /admin/jobs/:id/delete` — delete a dead-letter job.
pub async fn admin_delete_job(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(job_id): Path<i64>,
    headers: axum::http::HeaderMap,
    Form(form): Form<CsrfOnlyForm>,
) -> Result<Response, StatusCode> {
    require_admin(&auth_user)?;
    super::csrf::validate_csrf(&headers, &form.csrf_token).map_err(|_| StatusCode::FORBIDDEN)?;

    let pg = &state.pg_store;
    let tid = auth_user.tenant_id;

    match pg.admin_delete_job(tid, job_id).await {
        Ok(affected) => {
            if affected > 0 {
                let _ = pg
                    .admin_insert_audit_event(
                        auth_user.user_id,
                        tid,
                        AuditAction::JobDeleted.as_str(),
                        "job",
                        Some(job_id),
                        &serde_json::json!({}),
                    )
                    .await;
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, job_id = job_id, "admin delete job failed");
        }
    }

    Ok(Redirect::to("/admin/jobs?status=dead").into_response())
}

// ── GET /admin/tags ───────────────────────────────────────────────────────────

/// `GET /admin/tags` — tag merge page: select two tags to merge.
pub async fn admin_tags_page(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Response, StatusCode> {
    require_admin(&auth_user)?;
    let pg = &state.pg_store;
    let tid = auth_user.tenant_id;
    let is_super = matches!(auth_user.role, UserRole::Owner);

    let tags = match pg.list_tags(tid).await {
        Ok(list) => list
            .into_iter()
            .map(|t| AdminTagOption {
                tag_id: t.id,
                tag_name: t.name,
            })
            .collect(),
        Err(e) => {
            tracing::error!(error = %e, "failed to list tags for admin");
            vec![]
        }
    };

    let csrf_token = csrf::generate_csrf_token().unwrap_or_default();
    let page = AdminTagsPage {
        csrf_token: csrf_token.clone(),
        error: String::new(),
        success: String::new(),
        is_super_admin: is_super,
        tags,
    };

    let mut resp = render_ok(&page);
    resp.headers_mut().insert(
        axum::http::header::SET_COOKIE,
        csrf::csrf_cookie_value(&csrf_token, state.session_ttl, state.secure_cookies),
    );
    Ok(resp)
}

// ── POST /admin/tags/merge ────────────────────────────────────────────────────

/// `POST /admin/tags/merge` — merge `from_tag_id` into `to_tag_id`.
pub async fn admin_merge_tags(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    headers: axum::http::HeaderMap,
    Form(form): Form<MergeTagsForm>,
) -> Result<Response, StatusCode> {
    require_admin(&auth_user)?;
    let pg = &state.pg_store;
    let tid = auth_user.tenant_id;
    let is_super = matches!(auth_user.role, UserRole::Owner);

    super::csrf::validate_csrf(&headers, &form.csrf_token).map_err(|_| StatusCode::FORBIDDEN)?;

    if form.from_tag_id == form.to_tag_id {
        let csrf_token = generate_fresh_csrf();
        let tags = page_tags_for_merge(pg, tid).await;
        let page = AdminTagsPage {
            csrf_token: csrf_token.clone(),
            error: "Cannot merge a tag into itself. Select two different tags.".into(),
            success: String::new(),
            is_super_admin: is_super,
            tags,
        };
        let mut resp = render_template(&page, StatusCode::OK);
        resp.headers_mut().insert(
            axum::http::header::SET_COOKIE,
            csrf::csrf_cookie_value(&csrf_token, state.session_ttl, state.secure_cookies),
        );
        return Ok(resp);
    }

    match pg
        .admin_merge_tags(tid, form.from_tag_id, form.to_tag_id)
        .await
    {
        Ok((aliases_moved, docs_repointed)) => {
            let _ = pg
                .admin_insert_audit_event(
                    auth_user.user_id,
                    tid,
                    AuditAction::TagMerged.as_str(),
                    "tag",
                    Some(form.to_tag_id),
                    &serde_json::json!({
                        "from_tag_id": form.from_tag_id,
                        "to_tag_id": form.to_tag_id,
                        "aliases_moved": aliases_moved,
                        "doc_tags_repointed": docs_repointed,
                    }),
                )
                .await;

            let csrf_token = generate_fresh_csrf();
            let tags = page_tags_for_merge(pg, tid).await;
            let page = AdminTagsPage {
                csrf_token: csrf_token.clone(),
                error: String::new(),
                success: format!(
                    "Merged tag into target. {} aliases moved, {} document-tag links repointed.",
                    aliases_moved, docs_repointed
                ),
                is_super_admin: is_super,
                tags,
            };
            let mut resp = render_template(&page, StatusCode::OK);
            resp.headers_mut().insert(
                axum::http::header::SET_COOKIE,
                csrf::csrf_cookie_value(&csrf_token, state.session_ttl, state.secure_cookies),
            );
            Ok(resp)
        }
        Err(e) => {
            tracing::warn!(error = %e, from = form.from_tag_id, to = form.to_tag_id, "admin merge tags failed");
            let csrf_token = generate_fresh_csrf();
            let tags = page_tags_for_merge(pg, tid).await;
            let page = AdminTagsPage {
                csrf_token: csrf_token.clone(),
                error: format!("Failed to merge tags: {e}"),
                success: String::new(),
                is_super_admin: is_super,
                tags,
            };
            let mut resp = render_template(&page, StatusCode::OK);
            resp.headers_mut().insert(
                axum::http::header::SET_COOKIE,
                csrf::csrf_cookie_value(&csrf_token, state.session_ttl, state.secure_cookies),
            );
            Ok(resp)
        }
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/// Fetch tags for the merge page (used on re-renders after error/success).
async fn page_tags_for_merge(pg: &kb_store::PgStore, tid: i64) -> Vec<AdminTagOption> {
    pg.list_tags(tid)
        .await
        .map(|list| {
            list.into_iter()
                .map(|t| AdminTagOption {
                    tag_id: t.id,
                    tag_name: t.name,
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Render the users page with an error message (used by invite handler on validation failure).
async fn admin_rerender_users(
    pg: &kb_store::PgStore,
    tid: i64,
    state: &Arc<AppState>,
    error: &str,
) -> Result<Response, StatusCode> {
    let tname = tenant_name(pg, tid).await;
    let is_super = false; // This helper is called from invite (admin-scoped), not from super-admin flows.
    let csrf_token = generate_fresh_csrf();
    let users = pg
        .admin_list_users(tid)
        .await
        .map(|list| {
            list.into_iter()
                .map(|u| AdminUserRow {
                    id: u.id,
                    email: u.email,
                    role: u.role.as_str().to_owned(),
                    created_at: u.created_at.to_rfc3339(),
                })
                .collect()
        })
        .unwrap_or_default();
    let page = AdminUsersPage {
        csrf_token: csrf_token.clone(),
        error: error.to_owned(),
        tenant_name: tname,
        is_super_admin: is_super,
        users,
    };
    let mut resp = render_template(&page, StatusCode::OK);
    resp.headers_mut().insert(
        axum::http::header::SET_COOKIE,
        csrf::csrf_cookie_value(&csrf_token, state.session_ttl, state.secure_cookies),
    );
    Ok(resp)
}

// ── GET /admin/decrypt-audit ────────────────────────────────────────────────────

/// `GET /admin/decrypt-audit` — read-only decrypt-access audit log viewer
/// (plan §28, P10-T5).
///
/// Shows every DEK unwrap and provider key decrypt operation with success/failure
/// status. Tenant-admins see only their own tenant's entries; super-admins (Owner)
/// see all tenants. Filterable by operation type via query parameter `?operation=`.
///
/// The audit log is append-only — no rows are ever updated or deleted by any
/// admin action on this page.
pub async fn admin_decrypt_audit_page(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Response, StatusCode> {
    require_admin(&auth_user)?;
    let pg = &state.pg_store;
    let tid = auth_user.tenant_id;
    let is_super = matches!(auth_user.role, UserRole::Owner);
    let tname = tenant_name(pg, tid).await;

    let filter_op = params.get("operation").cloned().unwrap_or_default();

    let operation_filter = if filter_op.is_empty() || filter_op == "all" {
        None
    } else {
        use std::str::FromStr;
        kb_core::audit::DecryptAuditAction::from_str(&filter_op).ok()
    };

    // Super-admins see cross-tenant (tenant_id=0), tenant-admins see own tenant.
    let query_tid = if is_super { 0 } else { tid };

    let events: Vec<AdminDecryptAuditRow> = match pg
        .admin_list_decrypt_audit(query_tid, operation_filter, 100)
        .await
    {
        Ok(list) => list
            .into_iter()
            .map(|e| {
                let success_display = if e.success {
                    "✓ Success".to_string()
                } else {
                    "✗ Failed".to_string()
                };
                let user_id_display = e
                    .user_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "—".to_string());
                AdminDecryptAuditRow {
                    id: e.id,
                    tenant_id: e.tenant_id,
                    operation: e.operation.as_str().to_owned(),
                    key_id: e.key_id,
                    user_id_display,
                    success: e.success,
                    success_display,
                    created_at: e.created_at.to_rfc3339(),
                }
            })
            .collect(),
        Err(e) => {
            tracing::error!(error = %e, "failed to list decrypt audit events");
            vec![]
        }
    };

    let csrf_token = csrf::generate_csrf_token().unwrap_or_default();
    let page = AdminDecryptAuditPage {
        csrf_token: csrf_token.clone(),
        error: String::new(),
        tenant_name: tname,
        is_super_admin: is_super,
        filter_operation: filter_op,
        events,
    };

    let mut resp = render_ok(&page);
    resp.headers_mut().insert(
        axum::http::header::SET_COOKIE,
        csrf::csrf_cookie_value(&csrf_token, state.session_ttl, state.secure_cookies),
    );
    Ok(resp)
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::Arc;
    use std::time::Duration;

    use askama::Template;
    use tower::ServiceExt;

    use super::*;
    use crate::middleware::auth_middleware;

    // ── role_from_form ────────────────────────────────────────────────────

    #[test]
    fn role_from_form_maps_all_roles() {
        assert_eq!(role_from_form("owner"), UserRole::Owner);
        assert_eq!(role_from_form("admin"), UserRole::Admin);
        assert_eq!(role_from_form("member"), UserRole::Member);
    }

    #[test]
    fn role_from_form_defaults_to_member() {
        assert_eq!(role_from_form("unknown"), UserRole::Member);
        assert_eq!(role_from_form(""), UserRole::Member);
    }

    // ── quota_display ────────────────────────────────────────────────────

    #[test]
    fn quota_display_formats_values() {
        assert_eq!(quota_display(None), "unlimited");
        assert_eq!(quota_display(Some(0)), "0");
        assert_eq!(quota_display(Some(1_000_000)), "1000000");
    }

    // ── require_admin / require_owner ─────────────────────────────────────

    #[test]
    fn owner_can_access_admin_and_owner_routes() {
        let u = AuthUser {
            tenant_id: 1,
            user_id: 1,
            role: UserRole::Owner,
            email_verified: true,
        };
        assert!(require_admin(&u).is_ok());
        assert!(require_owner(&u).is_ok());
    }

    #[test]
    fn admin_can_access_admin_but_not_owner_routes() {
        let u = AuthUser {
            tenant_id: 1,
            user_id: 2,
            role: UserRole::Admin,
            email_verified: true,
        };
        assert!(require_admin(&u).is_ok());
        assert!(require_owner(&u).is_err());
    }

    #[test]
    fn member_cannot_access_admin_or_owner_routes() {
        let u = AuthUser {
            tenant_id: 1,
            user_id: 3,
            role: UserRole::Member,
            email_verified: true,
        };
        assert!(require_admin(&u).is_err());
        assert!(require_owner(&u).is_err());
    }

    // ── Template rendering smoke tests ────────────────────────────────────

    #[test]
    fn admin_dashboard_page_renders() {
        let page = AdminDashboardPage {
            csrf_token: "test-csrf".into(),
            error: String::new(),
            tenant_name: "demo".into(),
            is_super_admin: true,
            tenant_count: 3,
            user_count: 42,
            document_count: 99,
            storage_used_bytes: 1_048_576,
            token_usage: 5000,
            monthly_spend_micros: 1_250_000,
            budget_display: "$10.00".to_string(),
            recent_jobs: vec![AdminJobRow {
                id: 1,
                kind: "ingest".into(),
                status: "done".into(),
                attempts: 1,
                last_error: String::new(),
                created_at: "2026-06-01T00:00:00Z".into(),
            }],
            recent_audit: vec![AdminAuditRow {
                id: 1,
                actor_user_id: 42,
                action: "tag_merged".into(),
                target_type: "tag".into(),
                target_id_display: "99→100".into(),
                created_at: "2026-06-01T00:00:00Z".into(),
            }],
            backends: vec![AdminBackendRow {
                id: "local-vl".into(),
                base_url: "http://127.0.0.1:8080/v1".into(),
                healthy: true,
                free_slots: 3,
                total_slots: 4,
                roles: "text, vision, code".into(),
            }],
        };
        let html = page.render().unwrap();
        assert!(html.contains("Admin Dashboard"));
        assert!(html.contains("demo"));
        assert!(html.contains("local-vl"));
    }

    #[test]
    fn admin_dashboard_renders_with_zero_counts() {
        let page = AdminDashboardPage {
            csrf_token: "csrf".into(),
            error: String::new(),
            tenant_name: "empty".into(),
            is_super_admin: false,
            tenant_count: 0,
            user_count: 0,
            document_count: 0,
            storage_used_bytes: 0,
            token_usage: 0,
            monthly_spend_micros: 0,
            budget_display: "unlimited".to_string(),
            recent_jobs: vec![],
            recent_audit: vec![],
            backends: vec![],
        };
        let html = page.render().unwrap();
        assert!(html.contains("empty"));
        assert!(html.contains("No recent jobs"));
        assert!(html.contains("No backends configured"));
    }

    #[test]
    fn admin_tenants_page_renders() {
        let page = AdminTenantsPage {
            csrf_token: "tcsrf".into(),
            error: String::new(),
            tenants: vec![AdminTenantRow {
                id: 1,
                slug: "demo".into(),
                name: "Demo Tenant".into(),
                quota_bytes_display: "unlimited".into(),
                quota_tokens_display: "1000000".into(),
                budget_display: "$5.00".to_string(),
                created_at: "2026-06-01T00:00:00Z".into(),
            }],
        };
        let html = page.render().unwrap();
        assert!(html.contains("Tenant Management"));
        assert!(html.contains("demo"));
    }

    #[test]
    fn admin_tenants_page_renders_empty() {
        let page = AdminTenantsPage {
            csrf_token: "csrf".into(),
            error: String::new(),
            tenants: vec![],
        };
        let html = page.render().unwrap();
        assert!(html.contains("No tenants found"));
    }

    #[test]
    fn admin_users_page_renders() {
        let page = AdminUsersPage {
            csrf_token: "ucsrf".into(),
            error: String::new(),
            tenant_name: "demo".into(),
            is_super_admin: true,
            users: vec![AdminUserRow {
                id: 1,
                email: "admin@demo.com".into(),
                role: "owner".into(),
                created_at: "2026-06-01T00:00:00Z".into(),
            }],
        };
        let html = page.render().unwrap();
        assert!(html.contains("Users"));
        assert!(html.contains("admin@demo.com"));
    }

    #[test]
    fn admin_users_page_renders_empty() {
        let page = AdminUsersPage {
            csrf_token: "csrf".into(),
            error: String::new(),
            tenant_name: "new-tenant".into(),
            is_super_admin: false,
            users: vec![],
        };
        let html = page.render().unwrap();
        assert!(html.contains("No users in this tenant"));
    }

    #[test]
    fn admin_jobs_page_renders() {
        let page = AdminJobsPage {
            csrf_token: "jcsrf".into(),
            error: String::new(),
            is_super_admin: false,
            filter_status: "dead".into(),
            jobs: vec![AdminJobRow {
                id: 7,
                kind: "ingest".into(),
                status: "dead".into(),
                attempts: 5,
                last_error: "timeout".into(),
                created_at: "2026-06-01T00:00:00Z".into(),
            }],
        };
        let html = page.render().unwrap();
        assert!(html.contains("Job Queue"));
        assert!(html.contains("dead"));
        assert!(html.contains("timeout"));
    }

    #[test]
    fn admin_jobs_page_renders_empty() {
        let page = AdminJobsPage {
            csrf_token: "csrf".into(),
            error: String::new(),
            is_super_admin: true,
            filter_status: String::new(),
            jobs: vec![],
        };
        let html = page.render().unwrap();
        assert!(html.contains("No jobs matching this filter"));
    }

    #[test]
    fn admin_tags_page_renders() {
        let page = AdminTagsPage {
            csrf_token: "tcsrf".into(),
            error: String::new(),
            success: String::new(),
            is_super_admin: false,
            tags: vec![AdminTagOption {
                tag_id: 1,
                tag_name: "invoice".into(),
            }],
        };
        let html = page.render().unwrap();
        assert!(html.contains("Tag Merge"));
        assert!(html.contains("invoice"));
    }

    #[test]
    fn admin_tags_page_shows_success_message() {
        let page = AdminTagsPage {
            csrf_token: "csrf".into(),
            error: String::new(),
            success: "Merged tag into target. 2 aliases moved, 5 document-tag links repointed."
                .into(),
            is_super_admin: true,
            tags: vec![],
        };
        let html = page.render().unwrap();
        assert!(html.contains("Merged tag into target"));
    }

    #[test]
    fn admin_tags_page_renders_empty() {
        let page = AdminTagsPage {
            csrf_token: "tcsrf".into(),
            error: String::new(),
            success: String::new(),
            is_super_admin: false,
            tags: vec![],
        };
        let html = page.render().unwrap();
        assert!(html.contains("Tag Merge"));
    }

    // ── Handler integration tests ────────────────────────────────────────

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

    #[tokio::test]
    async fn admin_dashboard_redirects_member() {
        let state = test_app_state();
        let token = state
            .session_store
            .create(1, 50, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let request = axum::http::Request::builder()
            .uri("/admin")
            .header("Cookie", format!("__Host-session={token}"))
            .body(axum::body::Body::empty())
            .unwrap();
        let router = axum::Router::new()
            .route("/admin", axum::routing::get(admin_dashboard))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                crate::middleware::auth_middleware,
            ))
            .with_state(state);
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn admin_tenants_redirects_admin_role() {
        let state = test_app_state();
        let token = state
            .session_store
            .create(1, 50, UserRole::Admin, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let request = axum::http::Request::builder()
            .uri("/admin/tenants")
            .header("Cookie", format!("__Host-session={token}"))
            .body(axum::body::Body::empty())
            .unwrap();
        let router = axum::Router::new()
            .route("/admin/tenants", axum::routing::get(admin_tenants_page))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                crate::middleware::auth_middleware,
            ))
            .with_state(state);
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn admin_tenants_redirects_member() {
        let state = test_app_state();
        let token = state
            .session_store
            .create(1, 50, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let request = axum::http::Request::builder()
            .uri("/admin/tenants")
            .header("Cookie", format!("__Host-session={token}"))
            .body(axum::body::Body::empty())
            .unwrap();
        let router = axum::Router::new()
            .route("/admin/tenants", axum::routing::get(admin_tenants_page))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                crate::middleware::auth_middleware,
            ))
            .with_state(state);
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn admin_retry_job_rejects_member() {
        let state = test_app_state();
        let token = state
            .session_store
            .create(1, 50, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let request = axum::http::Request::builder()
            .method(axum::http::Method::POST)
            .uri("/admin/jobs/1/retry")
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("Cookie", format!("__Host-session={token}"))
            .body(axum::body::Body::from("csrf_token=any"))
            .unwrap();
        let router = axum::Router::new()
            .route(
                "/admin/jobs/{id}/retry",
                axum::routing::post(admin_retry_job),
            )
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                crate::middleware::auth_middleware,
            ))
            .with_state(state);
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn admin_unauthenticated_returns_401() {
        let state = test_app_state();
        let request = axum::http::Request::builder()
            .uri("/admin")
            .body(axum::body::Body::empty())
            .unwrap();
        let router = axum::Router::new()
            .route("/admin", axum::routing::get(admin_dashboard))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                crate::middleware::auth_middleware,
            ))
            .with_state(state);
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn admin_dashboard_owner_renders_despite_db_error() {
        let state = test_app_state();
        let token = state
            .session_store
            .create(1, 50, UserRole::Owner, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let request = axum::http::Request::builder()
            .uri("/admin")
            .header("Cookie", format!("__Host-session={token}"))
            .body(axum::body::Body::empty())
            .unwrap();
        let router = axum::Router::new()
            .route("/admin", axum::routing::get(admin_dashboard))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ))
            .with_state(state);
        let response = router.oneshot(request).await.unwrap();
        // Handler returns OK even with disconnected DB — counts fall back to 0.
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn admin_users_page_admin_renders_despite_db_error() {
        let state = test_app_state();
        let token = state
            .session_store
            .create(1, 50, UserRole::Admin, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let request = axum::http::Request::builder()
            .uri("/admin/users")
            .header("Cookie", format!("__Host-session={token}"))
            .body(axum::body::Body::empty())
            .unwrap();
        let router = axum::Router::new()
            .route("/admin/users", axum::routing::get(admin_users_page))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ))
            .with_state(state);
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn admin_jobs_page_admin_renders_despite_db_error() {
        let state = test_app_state();
        let token = state
            .session_store
            .create(1, 50, UserRole::Admin, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let request = axum::http::Request::builder()
            .uri("/admin/jobs")
            .header("Cookie", format!("__Host-session={token}"))
            .body(axum::body::Body::empty())
            .unwrap();
        let router = axum::Router::new()
            .route("/admin/jobs", axum::routing::get(admin_jobs_page))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ))
            .with_state(state);
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn admin_tags_page_admin_renders_despite_db_error() {
        let state = test_app_state();
        let token = state
            .session_store
            .create(1, 50, UserRole::Admin, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let request = axum::http::Request::builder()
            .uri("/admin/tags")
            .header("Cookie", format!("__Host-session={token}"))
            .body(axum::body::Body::empty())
            .unwrap();
        let router = axum::Router::new()
            .route("/admin/tags", axum::routing::get(admin_tags_page))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ))
            .with_state(state);
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn admin_invite_user_validation_errors_rerender_page() {
        let state = test_app_state();
        let token = state
            .session_store
            .create(1, 50, UserRole::Owner, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let csrf_val = "aaaa1111222233334444555566667777888899990000111122223333444455556666";
        // Missing email and short password → validation error
        let body = serde_urlencoded::to_string([
            ("csrf_token", csrf_val),
            ("email", ""),
            ("role", "member"),
            ("password", "shrt"),
        ])
        .unwrap();
        let request = axum::http::Request::builder()
            .method(axum::http::Method::POST)
            .uri("/admin/users/invite")
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header(
                "Cookie",
                format!("__Host-session={token}; __Host-csrf={csrf_val}"),
            )
            .body(axum::body::Body::from(body))
            .unwrap();
        let router = axum::Router::new()
            .route(
                "/admin/users/invite",
                axum::routing::post(admin_invite_user),
            )
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ))
            .with_state(state);
        let response = router.oneshot(request).await.unwrap();
        // Invalid form re-renders the users page with error
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 65536)
            .await
            .unwrap();
        let html = String::from_utf8_lossy(&body);
        assert!(html.contains("Email is required"));
    }

    #[tokio::test]
    async fn admin_invite_user_owner_creates_user() {
        let state = test_app_state();
        let token = state
            .session_store
            .create(1, 50, UserRole::Owner, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let csrf_val = "bbbb1111222233334444555566667777888899990000111122223333444455556666";
        let body = serde_urlencoded::to_string([
            ("csrf_token", csrf_val),
            ("email", "newuser@demo.com"),
            ("role", "member"),
            ("password", "goodpassword123"),
        ])
        .unwrap();
        let request = axum::http::Request::builder()
            .method(axum::http::Method::POST)
            .uri("/admin/users/invite")
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header(
                "Cookie",
                format!("__Host-session={token}; __Host-csrf={csrf_val}"),
            )
            .body(axum::body::Body::from(body))
            .unwrap();
        let router = axum::Router::new()
            .route(
                "/admin/users/invite",
                axum::routing::post(admin_invite_user),
            )
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ))
            .with_state(state);
        let response = router.oneshot(request).await.unwrap();
        // DB is disconnected → create_user fails → page re-renders with error
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn admin_change_user_role_post_redirects() {
        let state = test_app_state();
        let token = state
            .session_store
            .create(1, 50, UserRole::Owner, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let csrf_val = "cccc1111222233334444555566667777888899990000111122223333444455556666";
        let body = serde_urlencoded::to_string([
            ("csrf_token", csrf_val),
            ("user_id", "99"),
            ("role", "admin"),
        ])
        .unwrap();
        let request = axum::http::Request::builder()
            .method(axum::http::Method::POST)
            .uri("/admin/users/role")
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header(
                "Cookie",
                format!("__Host-session={token}; __Host-csrf={csrf_val}"),
            )
            .body(axum::body::Body::from(body))
            .unwrap();
        let router = axum::Router::new()
            .route(
                "/admin/users/role",
                axum::routing::post(admin_change_user_role),
            )
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ))
            .with_state(state);
        let response = router.oneshot(request).await.unwrap();
        // Redirects to /admin/users even when DB is disconnected (error is swallowed)
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
    }

    #[tokio::test]
    async fn admin_cannot_grant_owner_role_in_invite() {
        let state = test_app_state();
        let token = state
            .session_store
            .create(1, 50, UserRole::Admin, true, Duration::from_secs(3600))
            .await
            .unwrap();
        // Admin tries to invite a new Owner — should be rejected.
        let csrf_val = "dddd1111222233334444555566667777888899990000111122223333444455556666";
        let body = serde_urlencoded::to_string([
            ("csrf_token", csrf_val),
            ("email", "evil@demo.com"),
            ("role", "owner"),
            ("password", "goodpassword123"),
        ])
        .unwrap();
        let request = axum::http::Request::builder()
            .method(axum::http::Method::POST)
            .uri("/admin/users/invite")
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header(
                "Cookie",
                format!("__Host-session={token}; __Host-csrf={csrf_val}"),
            )
            .body(axum::body::Body::from(body))
            .unwrap();
        let router = axum::Router::new()
            .route(
                "/admin/users/invite",
                axum::routing::post(admin_invite_user),
            )
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ))
            .with_state(state);
        let response = router.oneshot(request).await.unwrap();
        // Renders the users page with an error (not a 403 — we re-render the form).
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 65536)
            .await
            .unwrap();
        let html = String::from_utf8_lossy(&body);
        assert!(html.contains("Only tenant owners can create owner accounts"));
    }

    #[tokio::test]
    async fn admin_cannot_grant_owner_role_in_role_change() {
        let state = test_app_state();
        let token = state
            .session_store
            .create(1, 50, UserRole::Admin, true, Duration::from_secs(3600))
            .await
            .unwrap();
        // Admin tries to promote someone to Owner — should be rejected with 403.
        let csrf_val = "eeee1111222233334444555566667777888899990000111122223333444455556666";
        let body = serde_urlencoded::to_string([
            ("csrf_token", csrf_val),
            ("user_id", "99"),
            ("role", "owner"),
        ])
        .unwrap();
        let request = axum::http::Request::builder()
            .method(axum::http::Method::POST)
            .uri("/admin/users/role")
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header(
                "Cookie",
                format!("__Host-session={token}; __Host-csrf={csrf_val}"),
            )
            .body(axum::body::Body::from(body))
            .unwrap();
        let router = axum::Router::new()
            .route(
                "/admin/users/role",
                axum::routing::post(admin_change_user_role),
            )
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ))
            .with_state(state);
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
}
