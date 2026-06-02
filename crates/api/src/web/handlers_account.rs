//! Account & billing page handlers (plan §30, ledger P12-T3).
//!
//! These handlers serve the authenticated account section:
//! - `GET /account` — profile, tenant name, plan badge, quota usage bar, billing status.
//! - `GET /account/team` — list users with roles, invite/change/remove forms.
//! - `GET /account/plan` — current plan details with upgrade/downgrade options.
//! - `GET /account/danger` — delete-account confirmation page.
//!
//! Mutating endpoints (`POST`) validate CSRF, enforce role gates where needed,
//! and redirect back to the referring page on success (or re-render on error).

use std::str::FromStr;
use std::sync::Arc;

use askama::Template;
use axum::Extension;
use axum::Form;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Redirect, Response};
use serde::Deserialize;

use crate::AppState;
use crate::AuthUser;
use kb_core::user::UserRole;
use kb_store::TenantBilling;

use super::csrf;
use super::templates::{
    AccountDangerPage, AccountGoodbyePage, AccountPage, AccountPlanPage, AccountTeamPage,
    BillingBadge, PlanOption, QuotaBar, RoleOptionSel, TeamMember,
};

// ── Helpers ────────────────────────────────────────────────────────────────────

/// Render a template into an HTML response with the given status code.
fn render_template<T: Template>(template: &T, status: StatusCode) -> Response {
    match template.render() {
        Ok(html) => (status, Html(html)).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "Askama template render failure");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Html::<&str>("Internal server error"),
            )
                .into_response()
        }
    }
}

/// Render a template with `StatusCode::OK`.
fn render_ok<T: Template>(template: &T) -> Response {
    render_template(template, StatusCode::OK)
}

/// Generate a fresh CSRF token (falls back to empty string on error).
fn generate_fresh_csrf() -> String {
    csrf::generate_csrf_token().unwrap_or_default()
}

/// Format bytes as a human-readable string (KB, MB, GB).
pub(crate) fn format_bytes(bytes: i64) -> String {
    const KB: i64 = 1024;
    const MB: i64 = KB * 1024;
    const GB: i64 = MB * 1024;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

/// Format a token count in a compact human-readable form.
pub(crate) fn format_tokens(tokens: i64) -> String {
    if tokens >= 1_000_000 {
        format!("{:.1}M", tokens as f64 / 1_000_000.0)
    } else if tokens >= 1_000 {
        format!("{:.1}K", tokens as f64 / 1_000.0)
    } else {
        tokens.to_string()
    }
}

/// Calculate usage percentage, capped at 100.
pub(crate) fn usage_percent(used: i64, limit: i64) -> u8 {
    if limit <= 0 {
        return 0;
    }
    let pct = (used as f64 / limit as f64 * 100.0) as u8;
    u8::min(pct, 100)
}

/// Resolve the billing status badge from a [`TenantBilling`].
pub(crate) fn billing_badge(billing: &TenantBilling) -> BillingBadge {
    match billing.billing_status.as_str() {
        "active" => BillingBadge {
            label: "Active".into(),
            css_class: "bg-green-100 text-green-800".into(),
        },
        "past_due" => BillingBadge {
            label: "Past Due".into(),
            css_class: "bg-red-100 text-red-800".into(),
        },
        "canceled" => BillingBadge {
            label: "Canceled".into(),
            css_class: "bg-yellow-100 text-yellow-800".into(),
        },
        "suspended" => BillingBadge {
            label: "Suspended".into(),
            css_class: "bg-red-100 text-red-800".into(),
        },
        _ => BillingBadge {
            label: "Inactive".into(),
            css_class: "bg-gray-100 text-gray-600".into(),
        },
    }
}

/// Resolve the plan display name, defaulting on free/unset plans.
pub(crate) fn plan_display_name(billing: &TenantBilling) -> String {
    billing
        .plan
        .as_ref()
        .map_or_else(|| "Free".into(), |p| p.code.clone())
}

/// Build a quota bar from current usage and a plan limit.
pub(crate) fn quota_bar(label: &str, used: i64, limit: Option<i64>) -> QuotaBar {
    let used_fmt = format_bytes(used);
    let (limit_fmt, pct) = match limit {
        Some(l) if l > 0 => (format_bytes(l), usage_percent(used, l)),
        _ => ("Unlimited".into(), 0u8),
    };
    QuotaBar {
        label: label.into(),
        used_display: used_fmt,
        limit_display: limit_fmt,
        percent: pct,
    }
}

// ── Form types ────────────────────────────────────────────────────────────────

/// Form data for team member role changes.
#[derive(Debug, Deserialize)]
pub struct ChangeRoleForm {
    pub csrf_token: String,
    pub user_id: i64,
    pub new_role: String,
}

/// Form data for inviting a team member.
#[derive(Debug, Deserialize)]
pub struct InviteUserForm {
    pub csrf_token: String,
    pub email: String,
    pub role: String,
}

/// Form data for removing a team member.
#[derive(Debug, Deserialize)]
pub struct RemoveUserForm {
    pub csrf_token: String,
    pub user_id: i64,
}

/// Form data for deleting the account.
#[derive(Debug, Deserialize)]
pub struct DeleteAccountForm {
    pub csrf_token: String,
    /// Must match the tenant slug to confirm.
    pub confirm_slug: String,
}

// ── GET /account ───────────────────────────────────────────────────────────────

/// `GET /account` — the account overview page showing profile, plan, and usage.
///
/// Fetches the tenant's billing state, storage usage, and token usage from the
/// database. Rendering is fallback-safe: if any lookup fails, the page still
/// renders with placeholder values and the error is logged.
pub async fn account_page(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Response, StatusCode> {
    let csrf_token = csrf::generate_csrf_token().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // ── Tenant info ──────────────────────────────────────────────────────
    let (tenant_slug, tenant_name) = match state.pg_store.get_tenant_info(auth_user.tenant_id).await
    {
        Ok(Some(info)) => (info.slug, info.name),
        Ok(None) => {
            tracing::warn!(tenant_id = auth_user.tenant_id, "tenant not found");
            (String::from("unknown"), String::from("Unknown Workspace"))
        }
        Err(e) => {
            tracing::error!(error = %e, tenant_id = auth_user.tenant_id, "failed to look up tenant info");
            (String::from("error"), String::from("Error"))
        }
    };

    // ── User email ──────────────────────────────────────────────────────
    let email = match state
        .pg_store
        .admin_get_user(auth_user.tenant_id, auth_user.user_id)
        .await
    {
        Ok(Some(user)) => user.email,
        Ok(None) => {
            tracing::warn!(
                user_id = auth_user.user_id,
                "user not found during account lookup"
            );
            String::from("unknown")
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to look up user email");
            String::from("error")
        }
    };

    // ── Billing ─────────────────────────────────────────────────────────
    let billing = state.pg_store.get_tenant_billing(auth_user.tenant_id).await;
    let badge = match &billing {
        Ok(Some(b)) => billing_badge(b),
        _ => BillingBadge {
            label: "Unknown".into(),
            css_class: "bg-gray-100 text-gray-600".into(),
        },
    };
    let plan_name = match &billing {
        Ok(Some(b)) => plan_display_name(b),
        _ => "Free".into(),
    };
    let has_stripe_customer = billing
        .as_ref()
        .ok()
        .and_then(|b| b.as_ref())
        .and_then(|b| b.stripe_customer_id.as_ref())
        .is_some();

    // ── Quota bars ───────────────────────────────────────────────────────
    let (storage_used, token_used) = {
        let s = state
            .pg_store
            .get_storage_usage(auth_user.tenant_id)
            .await
            .unwrap_or(0);
        let t = state
            .pg_store
            .get_token_usage(auth_user.tenant_id)
            .await
            .unwrap_or(0);
        (s, t)
    };

    let storage_limit = billing
        .as_ref()
        .ok()
        .and_then(|b| b.as_ref())
        .and_then(|b| b.plan.as_ref())
        .map(|p| p.quota_bytes);
    let token_limit = billing
        .as_ref()
        .ok()
        .and_then(|b| b.as_ref())
        .and_then(|b| b.plan.as_ref())
        .and_then(|p| p.token_budget);

    let storage_bar = quota_bar("Storage", storage_used, storage_limit);
    let token_bar = quota_bar("Tokens (this month)", token_used, token_limit);

    let page = AccountPage {
        csrf_token: csrf_token.clone(),
        email,
        tenant_slug,
        tenant_name,
        plan_name,
        badge,
        has_stripe_customer,
        email_verified: auth_user.email_verified,
        storage_bar,
        token_bar,
        error: String::new(),
    };

    let mut resp = render_ok(&page);
    resp.headers_mut().insert(
        axum::http::header::SET_COOKIE,
        super::csrf::csrf_cookie_value(&csrf_token, state.session_ttl, state.secure_cookies),
    );
    Ok(resp)
}

// ── GET /account/team ──────────────────────────────────────────────────────────

/// `GET /account/team` — the team management page.
///
/// Lists all users in the tenant with their roles. The current user can change
/// roles (Owner/Admin only) or remove members (Owner only; self-deletion and
/// last-Owner removal are prevented).
pub async fn account_team_page(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Response, StatusCode> {
    let csrf_token = csrf::generate_csrf_token().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let users = match state.pg_store.admin_list_users(auth_user.tenant_id).await {
        Ok(list) => list,
        Err(e) => {
            tracing::error!(error = %e, "failed to list users for team page");
            vec![]
        }
    };

    // Count Owners — used by the template to prevent removing the last Owner.
    let owner_count = users.iter().filter(|u| u.role == UserRole::Owner).count() as i32;

    let members: Vec<TeamMember> = users
        .into_iter()
        .map(|u| {
            let rl = u.role.as_str().to_string();
            TeamMember {
                user_id: u.id,
                email: u.email,
                role_label: rl.clone(),
                is_current_user: u.id == auth_user.user_id,
                role_options: role_options_for(&rl),
            }
        })
        .collect();

    let page = AccountTeamPage {
        csrf_token: csrf_token.clone(),
        members,
        owner_count,
        is_owner: auth_user.role == UserRole::Owner,
        is_admin_or_owner: auth_user.role == UserRole::Owner || auth_user.role == UserRole::Admin,
        error: String::new(),
    };

    let mut resp = render_ok(&page);
    resp.headers_mut().insert(
        axum::http::header::SET_COOKIE,
        super::csrf::csrf_cookie_value(&csrf_token, state.session_ttl, state.secure_cookies),
    );
    Ok(resp)
}

/// Return the available role options with pre-computed selection state.
fn role_options_for(current: &str) -> Vec<RoleOptionSel> {
    vec!["owner", "admin", "member"]
        .into_iter()
        .map(|r| RoleOptionSel {
            value: r.into(),
            selected: r == current,
        })
        .collect()
}

// ── POST /account/team/invite ──────────────────────────────────────────────────

/// `POST /account/team/invite` — invite a new user to the tenant.
///
/// Creates the user with a random temporary password and sends a password-reset
/// email so the invitee can set their own password. The `EmailSender` trait
/// currently only supports `send_password_reset_email` (P12-T7 will add a proper
/// invite method).
///
/// Access: Owner or Admin only.
pub async fn account_invite_user(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    headers: HeaderMap,
    Form(form): Form<InviteUserForm>,
) -> Response {
    // ── CSRF ───────────────────────────────────────────────────────────
    if let Err((status, msg)) = csrf::validate_csrf(&headers, &form.csrf_token) {
        return render_team_error(&state, &auth_user, msg, status).await;
    }

    // ── Role gate ──────────────────────────────────────────────────────
    if auth_user.role != UserRole::Owner && auth_user.role != UserRole::Admin {
        return render_team_error(
            &state,
            &auth_user,
            "Only owners and admins can invite users".into(),
            StatusCode::FORBIDDEN,
        )
        .await;
    }

    // ── Validate ──────────────────────────────────────────────────────
    let email = form.email.trim().to_lowercase();
    if email.is_empty() || !email.contains('@') {
        return render_team_error(
            &state,
            &auth_user,
            "A valid email is required".into(),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }

    let role = match UserRole::from_str(&form.role) {
        Ok(r) => r,
        Err(_) => {
            return render_team_error(
                &state,
                &auth_user,
                "Invalid role — choose owner, admin, or member".into(),
                StatusCode::BAD_REQUEST,
            )
            .await;
        }
    };

    // ── Check for duplicate ───────────────────────────────────────────
    if let Ok(Some(_existing)) = state
        .pg_store
        .find_user_by_email(auth_user.tenant_id, &email)
        .await
    {
        return render_team_error(
            &state,
            &auth_user,
            "A user with that email already exists in this workspace".into(),
            StatusCode::CONFLICT,
        )
        .await;
    }

    // ── Create user with a random temporary password ──────────────────
    let temp_password = {
        use rand::TryRng;
        let mut buf = [0u8; 16];
        if rand::rngs::SysRng.try_fill_bytes(&mut buf).is_err() {
            return render_team_error(
                &state,
                &auth_user,
                "Failed to generate temporary password — please try again".into(),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
            .await;
        }
        hex::encode(buf)
    };

    let password_hash = match kb_core::auth::hash_password(&temp_password) {
        Ok(h) => h,
        Err(e) => {
            tracing::error!(error = %e, "failed to hash password for invited user");
            return render_team_error(
                &state,
                &auth_user,
                "Failed to create user — please try again".into(),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
            .await;
        }
    };

    let new_user_id = match state
        .pg_store
        .create_user(auth_user.tenant_id, &email, &password_hash, role)
        .await
    {
        Ok(id) => id,
        Err(e) => {
            tracing::error!(error = %e, "failed to create invited user");
            return render_team_error(
                &state,
                &auth_user,
                "Failed to create user — please try again".into(),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
            .await;
        }
    };

    // ── Send password-reset email (so they can set their own password) ─
    // Generate a reset token, store it, and email.
    if let Some(email_sender) = &state.email_sender {
        let reset_token = match kb_core::auth::generate_email_token() {
            Ok(t) => t,
            Err(_) => {
                tracing::warn!(
                    user_id = new_user_id,
                    "failed to generate reset token for invited user"
                );
                return render_team_success(&state, &auth_user).await;
            }
        };
        let expires = chrono::Utc::now() + chrono::Duration::hours(1);
        if let Err(e) = state
            .pg_store
            .set_password_reset_token(auth_user.tenant_id, &email, &reset_token, expires)
            .await
        {
            tracing::warn!(error = %e, "failed to store reset token for invited user");
        } else {
            let base = state.public_base_url.trim_end_matches('/');
            let reset_url = format!("{base}/reset?token={reset_token}");
            if let Err(e) = email_sender
                .send_password_reset_email(&email, &reset_url)
                .await
            {
                tracing::warn!(error = %e, "failed to send password reset email to invited user");
            }
        }
    }

    render_team_success(&state, &auth_user).await
}

// ── POST /account/team/role ────────────────────────────────────────────────────

/// `POST /account/team/role` — change a team member's role.
///
/// Access: Owner or Admin only. An Admin cannot promote anyone to Owner.
/// Nobody can demote the last remaining Owner.
pub async fn account_change_role(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    headers: HeaderMap,
    Form(form): Form<ChangeRoleForm>,
) -> Response {
    // ── CSRF ───────────────────────────────────────────────────────────
    if let Err((status, msg)) = csrf::validate_csrf(&headers, &form.csrf_token) {
        return render_team_error(&state, &auth_user, msg, status).await;
    }

    // ── Role gate ──────────────────────────────────────────────────────
    if auth_user.role != UserRole::Owner && auth_user.role != UserRole::Admin {
        return render_team_error(
            &state,
            &auth_user,
            "Only owners and admins can change roles".into(),
            StatusCode::FORBIDDEN,
        )
        .await;
    }

    let new_role = match UserRole::from_str(&form.new_role) {
        Ok(r) => r,
        Err(_) => {
            return render_team_error(
                &state,
                &auth_user,
                "Invalid role".into(),
                StatusCode::BAD_REQUEST,
            )
            .await;
        }
    };

    // ── Admin can't promote to Owner ─────────────────────────────────
    if auth_user.role == UserRole::Admin && new_role == UserRole::Owner {
        return render_team_error(
            &state,
            &auth_user,
            "Only owners can promote users to Owner".into(),
            StatusCode::FORBIDDEN,
        )
        .await;
    }

    // ── Don't demote last Owner ───────────────────────────────────────
    if new_role != UserRole::Owner {
        let users = match state.pg_store.admin_list_users(auth_user.tenant_id).await {
            Ok(list) => list,
            Err(e) => {
                tracing::error!(error = %e, "failed to list users during role change");
                return render_team_error(
                    &state,
                    &auth_user,
                    "Failed to verify role change".into(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                )
                .await;
            }
        };
        let owner_count = users.iter().filter(|u| u.role == UserRole::Owner).count();
        // If the target user is currently an Owner and there's only one Owner left.
        let target_is_owner = users
            .iter()
            .any(|u| u.id == form.user_id && u.role == UserRole::Owner);
        if target_is_owner && owner_count <= 1 {
            return render_team_error(
                &state,
                &auth_user,
                "Cannot remove the last Owner — promote someone else to Owner first".into(),
                StatusCode::CONFLICT,
            )
            .await;
        }
    }

    match state
        .pg_store
        .admin_update_user_role(auth_user.tenant_id, form.user_id, new_role)
        .await
    {
        Ok(0) => {
            render_team_error(
                &state,
                &auth_user,
                "User not found".into(),
                StatusCode::NOT_FOUND,
            )
            .await
        }
        Ok(_) => {
            // Success — redirect back to team page.
            Redirect::to("/account/team").into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to update user role");
            render_team_error(
                &state,
                &auth_user,
                "Failed to update role — please try again".into(),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
            .await
        }
    }
}

// ── POST /account/team/remove ──────────────────────────────────────────────────

/// `POST /account/team/remove` — remove a user from the tenant.
///
/// Access: Owner only. Self-deletion is forbidden (use the Danger page).
/// The last remaining Owner cannot be removed.
pub async fn account_remove_user(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    headers: HeaderMap,
    Form(form): Form<RemoveUserForm>,
) -> Response {
    // ── CSRF ───────────────────────────────────────────────────────────
    if let Err((status, msg)) = csrf::validate_csrf(&headers, &form.csrf_token) {
        return render_team_error(&state, &auth_user, msg, status).await;
    }

    // ── Role gate: only Owner can remove ──────────────────────────────
    if auth_user.role != UserRole::Owner {
        return render_team_error(
            &state,
            &auth_user,
            "Only the workspace owner can remove members".into(),
            StatusCode::FORBIDDEN,
        )
        .await;
    }

    // ── Cannot self-delete via team page ──────────────────────────────
    if form.user_id == auth_user.user_id {
        return render_team_error(
            &state,
            &auth_user,
            "You cannot remove yourself — use the Delete Account page if you want to leave".into(),
            StatusCode::FORBIDDEN,
        )
        .await;
    }

    // ── Cannot remove the last Owner ──────────────────────────────────
    {
        let users = match state.pg_store.admin_list_users(auth_user.tenant_id).await {
            Ok(list) => list,
            Err(e) => {
                tracing::error!(error = %e, "failed to list users during remove");
                return render_team_error(
                    &state,
                    &auth_user,
                    "Failed to verify removal".into(),
                    StatusCode::INTERNAL_SERVER_ERROR,
                )
                .await;
            }
        };
        let owner_count = users.iter().filter(|u| u.role == UserRole::Owner).count();
        let target_is_owner = users
            .iter()
            .any(|u| u.id == form.user_id && u.role == UserRole::Owner);
        if target_is_owner && owner_count <= 1 {
            return render_team_error(
                &state,
                &auth_user,
                "Cannot remove the last Owner — promote someone else to Owner first".into(),
                StatusCode::CONFLICT,
            )
            .await;
        }
    }

    match state
        .pg_store
        .admin_delete_user(auth_user.tenant_id, form.user_id)
        .await
    {
        Ok(0) => {
            render_team_error(
                &state,
                &auth_user,
                "User not found".into(),
                StatusCode::NOT_FOUND,
            )
            .await
        }
        Ok(_) => Redirect::to("/account/team").into_response(),
        Err(e) => {
            tracing::error!(error = %e, "failed to delete user");
            render_team_error(
                &state,
                &auth_user,
                "Failed to remove user — please try again".into(),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
            .await
        }
    }
}

// ── GET /account/plan ──────────────────────────────────────────────────────────

/// `GET /account/plan` — the plan details page with upgrade/downgrade options.
///
/// Shows the current plan alongside all available plans. Each plan card has a
/// CTA button that links to Stripe Checkout (or shows a current-plan badge).
pub async fn account_plan_page(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Response, StatusCode> {
    let csrf_token = csrf::generate_csrf_token().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Current plan id (if any).
    let current_plan_code = state
        .pg_store
        .get_tenant_billing(auth_user.tenant_id)
        .await
        .ok()
        .flatten()
        .and_then(|b| b.plan)
        .map(|p| p.code)
        .unwrap_or_else(|| "free".into());

    // All plans.
    let all_plans = state.pg_store.list_plans().await.unwrap_or_else(|e| {
        tracing::error!(error = %e, "failed to list plans for plan page");
        vec![]
    });

    let plans: Vec<PlanOption> = all_plans
        .into_iter()
        .map(|p| {
            let is_current = p.code == current_plan_code;
            PlanOption {
                code: p.code.clone(),
                name: plan_code_label(&p.code),
                price_display: price_display(p.price_cents, p.currency.as_deref()),
                quota_display: format_bytes(p.quota_bytes),
                token_display: match p.token_budget {
                    Some(t) => format_tokens(t),
                    None => "Unlimited".into(),
                },
                is_current,
                cta_label: if is_current {
                    "Current Plan".into()
                } else if p.price_cents == Some(0) || p.price_cents.is_none() {
                    "Switch".into()
                } else {
                    "Upgrade".into()
                },
                checkout_url: if is_current {
                    String::new()
                } else {
                    format!("/billing/checkout?plan={}", p.code)
                },
            }
        })
        .collect();

    let page = AccountPlanPage {
        csrf_token: csrf_token.clone(),
        current_plan: current_plan_code.clone(),
        plans,
        error: String::new(),
    };

    let mut resp = render_ok(&page);
    resp.headers_mut().insert(
        axum::http::header::SET_COOKIE,
        super::csrf::csrf_cookie_value(&csrf_token, state.session_ttl, state.secure_cookies),
    );
    Ok(resp)
}

// ── GET /account/danger ────────────────────────────────────────────────────────

/// `GET /account/danger` — the danger-zone page with the delete-account
/// confirmation form.
///
/// The user must type their tenant slug to confirm deletion.
pub async fn account_danger_page(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Response, StatusCode> {
    let csrf_token = csrf::generate_csrf_token().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let tenant_slug = state
        .pg_store
        .get_tenant_slug(auth_user.tenant_id)
        .await
        .unwrap_or_else(|_| String::from("unknown"));

    let page = AccountDangerPage {
        csrf_token: csrf_token.clone(),
        tenant_slug: tenant_slug.clone(),
        error: String::new(),
    };

    let mut resp = render_ok(&page);
    resp.headers_mut().insert(
        axum::http::header::SET_COOKIE,
        super::csrf::csrf_cookie_value(&csrf_token, state.session_ttl, state.secure_cookies),
    );
    Ok(resp)
}

// ── POST /account/danger/delete ───────────────────────────────────────────────

/// `POST /account/danger/delete` — confirm account deletion.
///
/// Validates CSRF, checks the confirmation slug, enqueues a [`JobKind::DeleteTenant`]
/// job, and returns a goodbye page. The actual crypto-shredding runs asynchronously
/// via the job queue; this handler returns immediately.
pub async fn account_delete(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    headers: HeaderMap,
    Form(form): Form<DeleteAccountForm>,
) -> Response {
    // ── CSRF ───────────────────────────────────────────────────────────
    if let Err((_status, msg)) = csrf::validate_csrf(&headers, &form.csrf_token) {
        return render_danger_error(&state, &auth_user, &msg).await;
    }

    // ── Role gate: only Owners can delete the entire workspace ─────────
    if auth_user.role != UserRole::Owner {
        return render_danger_error(
            &state,
            &auth_user,
            "Only the workspace owner can delete the account",
        )
        .await;
    }

    // ── Verify confirmation slug ──────────────────────────────────────
    let actual_slug = match state.pg_store.get_tenant_slug(auth_user.tenant_id).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "failed to look up tenant slug for deletion");
            return render_danger_error(
                &state,
                &auth_user,
                "Failed to verify account — please try again",
            )
            .await;
        }
    };

    if form.confirm_slug.trim() != actual_slug {
        return render_danger_error(
            &state,
            &auth_user,
            "Confirmation slug does not match — account was NOT deleted",
        )
        .await;
    }

    // ── Enqueue the shred job ──────────────────────────────────────────
    let job_queue = match &state.job_queue {
        Some(q) => q,
        None => {
            tracing::error!("job queue not configured — cannot enqueue account deletion");
            return render_danger_error(
                &state,
                &auth_user,
                "Account deletion is currently unavailable — please contact support",
            )
            .await;
        }
    };

    if let Err(e) = job_queue
        .enqueue(
            auth_user.tenant_id,
            None,
            None,
            kb_core::job::JobKind::DeleteTenant,
            10, // high priority — deletion is urgent
        )
        .await
    {
        tracing::error!(error = %e, tenant_id = auth_user.tenant_id, "failed to enqueue account deletion job");
        return render_danger_error(
            &state,
            &auth_user,
            "Failed to schedule account deletion — please try again or contact support",
        )
        .await;
    }

    tracing::info!(
        tenant_id = auth_user.tenant_id,
        user_id = auth_user.user_id,
        "account deletion enqueued — returning goodbye page"
    );

    // ── Render the goodbye page ────────────────────────────────────────
    let page = AccountGoodbyePage {
        tenant_slug: actual_slug,
    };
    render_ok(&page)
}

// ── Error re-render helpers ─────────────────────────────────────────────────────

/// Re-render the team page with an error banner.
async fn render_team_error(
    state: &Arc<AppState>,
    auth_user: &AuthUser,
    error: String,
    status: StatusCode,
) -> Response {
    let csrf_token = generate_fresh_csrf();
    let users = state
        .pg_store
        .admin_list_users(auth_user.tenant_id)
        .await
        .unwrap_or_default();
    let owner_count = users.iter().filter(|u| u.role == UserRole::Owner).count() as i32;

    let members: Vec<TeamMember> = users
        .into_iter()
        .map(|u| {
            let rl = u.role.as_str().to_string();
            TeamMember {
                user_id: u.id,
                email: u.email,
                role_label: rl.clone(),
                is_current_user: u.id == auth_user.user_id,
                role_options: role_options_for(&rl),
            }
        })
        .collect();

    let page = AccountTeamPage {
        csrf_token,
        members,
        owner_count,
        is_owner: auth_user.role == UserRole::Owner,
        is_admin_or_owner: auth_user.role == UserRole::Owner || auth_user.role == UserRole::Admin,
        error,
    };
    render_template(&page, status)
}

/// Re-render the team page after a successful action (redirect).
async fn render_team_success(_state: &Arc<AppState>, _auth_user: &AuthUser) -> Response {
    Redirect::to("/account/team").into_response()
}

/// Re-render the danger page with an error banner.
async fn render_danger_error(state: &Arc<AppState>, auth_user: &AuthUser, error: &str) -> Response {
    let csrf_token = generate_fresh_csrf();
    let tenant_slug = state
        .pg_store
        .get_tenant_slug(auth_user.tenant_id)
        .await
        .unwrap_or_else(|_| String::from("unknown"));

    let page = AccountDangerPage {
        csrf_token,
        tenant_slug,
        error: error.to_string(),
    };
    render_template(&page, StatusCode::BAD_REQUEST)
}

// ── Pure helpers ───────────────────────────────────────────────────────────────

/// Map a plan code to a human-readable label.
fn plan_code_label(code: &str) -> String {
    match code {
        "free" => "Free".into(),
        "pro" => "Pro".into(),
        "team" => "Team".into(),
        other => {
            // Capitalize first letter.
            let mut c = other.chars();
            match c.next() {
                Some(first) => first.to_uppercase().collect::<String>() + c.as_str(),
                None => String::new(),
            }
        }
    }
}

/// Format a price in cents as a display string (e.g. "$9", "$29/mo", "Free").
fn price_display(cents: Option<i32>, _currency: Option<&str>) -> String {
    match cents {
        Some(0) | None => "Free".into(),
        Some(c) => format!("${:.0}/mo", c as f64 / 100.0),
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use kb_store::Plan;

    // ── format_bytes ──────────────────────────────────────────────────────

    #[test]
    fn format_bytes_zero() {
        assert_eq!(format_bytes(0), "0 B");
    }

    #[test]
    fn format_bytes_bytes() {
        assert_eq!(format_bytes(500), "500 B");
    }

    #[test]
    fn format_bytes_kb() {
        assert_eq!(format_bytes(2048), "2.0 KB");
    }

    #[test]
    fn format_bytes_mb() {
        assert_eq!(format_bytes(5 * 1024 * 1024), "5.0 MB");
    }

    #[test]
    fn format_bytes_gb() {
        assert_eq!(format_bytes(3i64 * 1024 * 1024 * 1024), "3.0 GB");
    }

    #[test]
    fn format_bytes_fractional_gb() {
        let one_and_half_gb = (1.5 * 1024.0 * 1024.0 * 1024.0) as i64;
        assert_eq!(format_bytes(one_and_half_gb), "1.5 GB");
    }

    // ── format_tokens ─────────────────────────────────────────────────────

    #[test]
    fn format_tokens_small() {
        assert_eq!(format_tokens(0), "0");
        assert_eq!(format_tokens(500), "500");
    }

    #[test]
    fn format_tokens_k() {
        assert_eq!(format_tokens(5_000), "5.0K");
        assert_eq!(format_tokens(10_000), "10.0K");
    }

    #[test]
    fn format_tokens_m() {
        assert_eq!(format_tokens(500_000), "500.0K");
        assert_eq!(format_tokens(1_000_000), "1.0M");
    }

    // ── usage_percent ─────────────────────────────────────────────────────

    #[test]
    fn usage_percent_zero_used() {
        assert_eq!(usage_percent(0, 100), 0);
    }

    #[test]
    fn usage_percent_half() {
        assert_eq!(usage_percent(50, 100), 50);
    }

    #[test]
    fn usage_percent_full() {
        assert_eq!(usage_percent(100, 100), 100);
    }

    #[test]
    fn usage_percent_over_capped() {
        assert_eq!(usage_percent(150, 100), 100);
    }

    #[test]
    fn usage_percent_zero_limit() {
        assert_eq!(usage_percent(50, 0), 0);
    }

    #[test]
    fn usage_percent_negative_limit() {
        assert_eq!(usage_percent(50, -1), 0);
    }

    // ── billing_badge ─────────────────────────────────────────────────────

    #[test]
    fn billing_badge_active() {
        let billing = TenantBilling {
            plan: None,
            stripe_customer_id: None,
            subscription_id: None,
            billing_status: "active".into(),
            current_period_end: None,
        };
        let badge = billing_badge(&billing);
        assert_eq!(badge.label, "Active");
        assert!(badge.css_class.contains("green"));
    }

    #[test]
    fn billing_badge_past_due() {
        let billing = TenantBilling {
            plan: None,
            stripe_customer_id: None,
            subscription_id: None,
            billing_status: "past_due".into(),
            current_period_end: None,
        };
        let badge = billing_badge(&billing);
        assert_eq!(badge.label, "Past Due");
        assert!(badge.css_class.contains("red"));
    }

    #[test]
    fn billing_badge_canceled() {
        let billing = TenantBilling {
            plan: None,
            stripe_customer_id: None,
            subscription_id: None,
            billing_status: "canceled".into(),
            current_period_end: None,
        };
        let badge = billing_badge(&billing);
        assert_eq!(badge.label, "Canceled");
    }

    #[test]
    fn billing_badge_unknown_defaults_inactive() {
        let billing = TenantBilling {
            plan: None,
            stripe_customer_id: None,
            subscription_id: None,
            billing_status: "trialing".into(),
            current_period_end: None,
        };
        let badge = billing_badge(&billing);
        assert_eq!(badge.label, "Inactive");
        assert!(badge.css_class.contains("gray"));
    }

    // ── plan_display_name ──────────────────────────────────────────────────

    #[test]
    fn plan_display_name_with_plan() {
        let plan = Plan {
            id: 1,
            code: "pro".into(),
            stripe_price: "price_pro".into(),
            quota_bytes: 5_368_709_120,
            token_budget: Some(500_000),
            features: serde_json::json!({}),
            price_cents: Some(900),
            currency: Some("usd".into()),
        };
        let billing = TenantBilling {
            plan: Some(plan),
            stripe_customer_id: None,
            subscription_id: None,
            billing_status: "active".into(),
            current_period_end: None,
        };
        assert_eq!(plan_display_name(&billing), "pro");
    }

    #[test]
    fn plan_display_name_no_plan_defaults_free() {
        let billing = TenantBilling {
            plan: None,
            stripe_customer_id: None,
            subscription_id: None,
            billing_status: "inactive".into(),
            current_period_end: None,
        };
        assert_eq!(plan_display_name(&billing), "Free");
    }

    // ── plan_code_label ────────────────────────────────────────────────────

    #[test]
    fn plan_code_label_free_pro_team() {
        assert_eq!(plan_code_label("free"), "Free");
        assert_eq!(plan_code_label("pro"), "Pro");
        assert_eq!(plan_code_label("team"), "Team");
    }

    #[test]
    fn plan_code_label_capitalizes_unknown() {
        assert_eq!(plan_code_label("enterprise"), "Enterprise");
    }

    #[test]
    fn plan_code_label_empty_unknown() {
        assert_eq!(plan_code_label(""), "");
    }

    // ── price_display ──────────────────────────────────────────────────────

    #[test]
    fn price_display_free() {
        assert_eq!(price_display(Some(0), None), "Free");
        assert_eq!(price_display(None, None), "Free");
    }

    #[test]
    fn price_display_dollars() {
        assert_eq!(price_display(Some(900), Some("usd")), "$9/mo");
        assert_eq!(price_display(Some(2900), Some("usd")), "$29/mo");
    }

    // ── quota_bar ──────────────────────────────────────────────────────────

    #[test]
    fn quota_bar_with_limit() {
        let bar = quota_bar("Storage", 50 * 1024 * 1024, Some(100 * 1024 * 1024));
        assert_eq!(bar.label, "Storage");
        assert_eq!(bar.percent, 50);
        assert!(bar.limit_display.contains("MB"));
    }

    #[test]
    fn quota_bar_unlimited() {
        let bar = quota_bar("Storage", 1024 * 1024 * 1024, None);
        assert_eq!(bar.limit_display, "Unlimited");
        assert_eq!(bar.percent, 0);
    }

    #[test]
    fn quota_bar_zero_usage_with_limit() {
        let bar = quota_bar("Tokens", 0, Some(1000));
        assert_eq!(bar.percent, 0);
        assert_eq!(bar.used_display, "0 B");
    }

    #[test]
    fn quota_bar_full_usage() {
        let bar = quota_bar("Tokens", 5000, Some(5000));
        assert_eq!(bar.percent, 100);
    }

    #[test]
    fn quota_bar_over_limit_capped() {
        let bar = quota_bar("Storage", 200 * 1024 * 1024, Some(100 * 1024 * 1024));
        assert_eq!(bar.percent, 100);
    }

    // ── billing_badge edge cases ─────────────────────────────────────────

    #[test]
    fn billing_badge_suspended() {
        let billing = TenantBilling {
            plan: None,
            stripe_customer_id: None,
            subscription_id: None,
            billing_status: "suspended".into(),
            current_period_end: None,
        };
        let badge = billing_badge(&billing);
        assert_eq!(badge.label, "Suspended");
    }

    // ── Template rendering tests (P12-T3) ─────────────────────────────────

    #[test]
    fn account_page_template_renders() {
        let page = AccountPage {
            csrf_token: "test-csrf".into(),
            email: "user@example.com".into(),
            tenant_slug: "myworkspace".into(),
            tenant_name: "My Workspace".into(),
            plan_name: "Pro".into(),
            badge: BillingBadge {
                label: "Active".into(),
                css_class: "bg-green-100 text-green-800".into(),
            },
            has_stripe_customer: true,
            email_verified: true,
            storage_bar: QuotaBar {
                label: "Storage".into(),
                used_display: "2.5 GB".into(),
                limit_display: "5.0 GB".into(),
                percent: 50,
            },
            token_bar: QuotaBar {
                label: "Tokens (this month)".into(),
                used_display: "250.0K".into(),
                limit_display: "500.0K".into(),
                percent: 50,
            },
            error: String::new(),
        };
        let html = page.render().expect("account template must render");
        assert!(html.contains("My Workspace"));
        assert!(html.contains("myworkspace.lokb.dev"));
        assert!(html.contains("user@example.com"));
        assert!(html.contains("Pro"));
        assert!(html.contains("Active"));
        assert!(html.contains("Manage Billing"));
        assert!(html.contains("2.5 GB"));
    }

    #[test]
    fn account_page_template_renders_unverified() {
        let page = AccountPage {
            csrf_token: "test-csrf".into(),
            email: "user@example.com".into(),
            tenant_slug: "myworkspace".into(),
            tenant_name: "My Workspace".into(),
            plan_name: "Free".into(),
            badge: BillingBadge {
                label: "Inactive".into(),
                css_class: "bg-gray-100 text-gray-600".into(),
            },
            has_stripe_customer: false,
            email_verified: false,
            storage_bar: QuotaBar {
                label: "Storage".into(),
                used_display: "0 B".into(),
                limit_display: "50.0 MB".into(),
                percent: 0,
            },
            token_bar: QuotaBar {
                label: "Tokens (this month)".into(),
                used_display: "0".into(),
                limit_display: "10.0K".into(),
                percent: 0,
            },
            error: String::new(),
        };
        let html = page.render().expect("account template must render");
        assert!(html.contains("Verify your email"));
        // With has_stripe_customer=false, the Upgrade button shows instead of Manage Billing.
        assert!(html.contains("Upgrade"));
    }

    #[test]
    fn account_team_page_template_renders() {
        let page = AccountTeamPage {
            csrf_token: "test-csrf".into(),
            members: vec![
                TeamMember {
                    user_id: 1,
                    email: "owner@example.com".into(),
                    role_label: "owner".into(),
                    is_current_user: true,
                    role_options: vec![
                        RoleOptionSel {
                            value: "owner".into(),
                            selected: true,
                        },
                        RoleOptionSel {
                            value: "admin".into(),
                            selected: false,
                        },
                        RoleOptionSel {
                            value: "member".into(),
                            selected: false,
                        },
                    ],
                },
                TeamMember {
                    user_id: 2,
                    email: "member@example.com".into(),
                    role_label: "member".into(),
                    is_current_user: false,
                    role_options: vec![
                        RoleOptionSel {
                            value: "owner".into(),
                            selected: false,
                        },
                        RoleOptionSel {
                            value: "admin".into(),
                            selected: false,
                        },
                        RoleOptionSel {
                            value: "member".into(),
                            selected: true,
                        },
                    ],
                },
            ],
            owner_count: 1,
            is_owner: true,
            is_admin_or_owner: true,
            error: String::new(),
        };
        let html = page.render().expect("team template must render");
        assert!(html.contains("owner@example.com"));
        assert!(html.contains("member@example.com"));
        assert!(html.contains("(you)"));
        assert!(html.contains("Invite a Team Member"));
    }

    #[test]
    fn account_team_page_template_renders_non_owner() {
        let page = AccountTeamPage {
            csrf_token: "test-csrf".into(),
            members: vec![],
            owner_count: 1,
            is_owner: false,
            is_admin_or_owner: false,
            error: String::new(),
        };
        let html = page.render().expect("team template must render");
        // Non-owner should not see invite form.
        assert!(!html.contains("Invite a Team Member"));
    }

    #[test]
    fn account_team_page_template_renders_with_error() {
        let page = AccountTeamPage {
            csrf_token: "test-csrf".into(),
            members: vec![],
            owner_count: 0,
            is_owner: false,
            is_admin_or_owner: false,
            error: "Something went wrong".into(),
        };
        let html = page.render().expect("team template must render");
        assert!(html.contains("Something went wrong"));
    }

    #[test]
    fn account_plan_page_template_renders() {
        let page = AccountPlanPage {
            csrf_token: "test-csrf".into(),
            current_plan: "free".into(),
            plans: vec![
                PlanOption {
                    code: "free".into(),
                    name: "Free".into(),
                    price_display: "Free".into(),
                    quota_display: "50.0 MB".into(),
                    token_display: "10.0K".into(),
                    is_current: true,
                    cta_label: "Current Plan".into(),
                    checkout_url: String::new(),
                },
                PlanOption {
                    code: "pro".into(),
                    name: "Pro".into(),
                    price_display: "$9/mo".into(),
                    quota_display: "5.0 GB".into(),
                    token_display: "500.0K".into(),
                    is_current: false,
                    cta_label: "Upgrade".into(),
                    checkout_url: "/billing/checkout?plan=pro".into(),
                },
            ],
            error: String::new(),
        };
        let html = page.render().expect("plan template must render");
        assert!(html.contains("Current Plan"));
        assert!(html.contains("Upgrade"));
        assert!(html.contains("50.0 MB"));
        assert!(html.contains("5.0 GB"));
        assert!(html.contains("$9/mo"));
    }

    #[test]
    fn account_plan_page_template_renders_empty_plans() {
        let page = AccountPlanPage {
            csrf_token: "test-csrf".into(),
            current_plan: "free".into(),
            plans: vec![],
            error: String::new(),
        };
        let html = page
            .render()
            .expect("plan template must render with empty plans");
        assert!(html.contains("Plans are being loaded"));
    }

    #[test]
    fn account_danger_page_template_renders() {
        let page = AccountDangerPage {
            csrf_token: "test-csrf".into(),
            tenant_slug: "myworkspace".into(),
            error: String::new(),
        };
        let html = page.render().expect("danger template must render");
        assert!(html.contains("myworkspace"));
        assert!(html.contains("Delete Workspace"));
        assert!(html.contains("cannot be undone"));
    }

    #[test]
    fn account_danger_page_template_renders_with_error() {
        let page = AccountDangerPage {
            csrf_token: "test-csrf".into(),
            tenant_slug: "myworkspace".into(),
            error: "Confirmation slug does not match".into(),
        };
        let html = page.render().expect("danger template must render");
        assert!(html.contains("Confirmation slug does not match"));
    }

    #[test]
    fn account_goodbye_page_template_renders() {
        let page = AccountGoodbyePage {
            tenant_slug: "myworkspace".into(),
        };
        let html = page.render().expect("goodbye template must render");
        assert!(html.contains("myworkspace"));
        assert!(html.contains("Workspace Deleted"));
        assert!(html.contains("crypto-shredded"));
    }

    // ── Handler smoke tests (verifies routing + auth middleware wiring) ─────

    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use kb_core::session::SessionStore;
    use kb_store::PgStore;
    use kb_store::session_store::InMemorySessionStore;
    use std::time::Duration;
    use tower::ServiceExt;

    fn make_state() -> Arc<AppState> {
        let session_store: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
        let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
        Arc::new(AppState::new(
            session_store,
            pg_store,
            Some(Duration::from_secs(3600)),
            false,
        ))
    }

    fn account_router(state: Arc<AppState>) -> axum::Router {
        use axum::middleware::from_fn_with_state;
        use axum::routing::{get, post};

        let protected = axum::Router::new()
            .route("/account", get(account_page))
            .route("/account/team", get(account_team_page))
            .route("/account/team/invite", post(account_invite_user))
            .route("/account/team/role", post(account_change_role))
            .route("/account/team/remove", post(account_remove_user))
            .route("/account/plan", get(account_plan_page))
            .route("/account/danger", get(account_danger_page))
            .route("/account/danger/delete", post(account_delete))
            .layer(from_fn_with_state(
                state.clone(),
                crate::middleware::auth_middleware,
            ));

        protected.with_state(state)
    }

    async fn make_authed_request(
        state: &Arc<AppState>,
        method: &str,
        uri: &str,
    ) -> (StatusCode, String) {
        // Create a valid session.
        let token = state
            .session_store
            .create(
                1,
                1,
                kb_core::user::UserRole::Owner,
                true,
                Duration::from_secs(3600),
            )
            .await
            .expect("create session");

        let body = Body::empty();
        let req = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::COOKIE, format!("__Host-session={token}"))
            .body(body)
            .expect("build request");

        let router = account_router(state.clone());
        let resp = router.oneshot(req).await.expect("send request");
        let status = resp.status();
        let body_bytes = axum::body::to_bytes(resp.into_body(), 10 * 1024 * 1024)
            .await
            .unwrap_or_default();
        let body_str = String::from_utf8_lossy(&body_bytes).to_string();
        (status, body_str)
    }

    #[tokio::test]
    async fn account_page_requires_auth() {
        let state = make_state();
        let router = account_router(state);
        let req = Request::builder()
            .uri("/account")
            .body(Body::empty())
            .expect("build request");
        let resp = router.oneshot(req).await.expect("send request");
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn account_page_renders_with_disconnected_db() {
        let state = make_state();
        let (status, body) = make_authed_request(&state, "GET", "/account").await;
        // The handler is fallback-safe: DB errors are caught, and the template
        // renders with placeholder values ("Error", "unknown") instead of crashing.
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("Error")); // fallback tenant name
        assert!(!body.is_empty());
    }

    #[tokio::test]
    async fn account_team_page_requires_auth() {
        let state = make_state();
        let router = account_router(state);
        let req = Request::builder()
            .uri("/account/team")
            .body(Body::empty())
            .expect("build request");
        let resp = router.oneshot(req).await.expect("send request");
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn account_plan_page_requires_auth() {
        let state = make_state();
        let router = account_router(state);
        let req = Request::builder()
            .uri("/account/plan")
            .body(Body::empty())
            .expect("build request");
        let resp = router.oneshot(req).await.expect("send request");
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn account_danger_page_requires_auth() {
        let state = make_state();
        let router = account_router(state);
        let req = Request::builder()
            .uri("/account/danger")
            .body(Body::empty())
            .expect("build request");
        let resp = router.oneshot(req).await.expect("send request");
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn account_delete_requires_owner_role() {
        let state = make_state();
        // Create a session with Member role (not Owner).
        let token = state
            .session_store
            .create(
                1,
                1,
                kb_core::user::UserRole::Member,
                true,
                Duration::from_secs(3600),
            )
            .await
            .expect("create session");

        // Set up a proper CSRF cookie + hidden field match.
        let csrf_val = crate::web::csrf::generate_csrf_token().expect("gen csrf");
        // Store the CSRF value on the cookie header.
        let req = Request::builder()
            .method("POST")
            .uri("/account/danger/delete")
            .header(
                header::COOKIE,
                format!("__Host-session={token};__Host-csrf={csrf_val}"),
            )
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(format!(
                "csrf_token={csrf_val}&confirm_slug=test"
            )))
            .expect("build request");

        let router = account_router(state);
        let resp = router.oneshot(req).await.expect("send request");
        // Non-Owner should be rejected with BAD_REQUEST (renders danger page with error).
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn account_delete_requires_csrf() {
        let state = make_state();
        let token = state
            .session_store
            .create(
                1,
                1,
                kb_core::user::UserRole::Owner,
                true,
                Duration::from_secs(3600),
            )
            .await
            .expect("create session");

        let req = Request::builder()
            .method("POST")
            .uri("/account/danger/delete")
            .header(header::COOKIE, format!("__Host-session={token}"))
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from("csrf_token=bad&confirm_slug=nope"))
            .expect("build request");

        let router = account_router(state);
        let resp = router.oneshot(req).await.expect("send request");
        // CSRF validation failure renders the danger page with an error (400 BAD_REQUEST),
        // not a bare 403, since the handler re-renders the form.
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn account_invite_requires_post() {
        let state = make_state();
        let token = state
            .session_store
            .create(
                1,
                1,
                kb_core::user::UserRole::Owner,
                true,
                Duration::from_secs(3600),
            )
            .await
            .expect("create session");

        let req = Request::builder()
            .method("POST")
            .uri("/account/team/invite")
            .header(header::COOKIE, format!("__Host-session={token}"))
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(
                "csrf_token=test&email=new@example.com&role=member",
            ))
            .expect("build request");

        let router = account_router(state);
        let resp = router.oneshot(req).await.expect("send request");
        // With invalid CSRF, should return 403.
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn account_change_role_requires_post() {
        let state = make_state();
        let token = state
            .session_store
            .create(
                1,
                1,
                kb_core::user::UserRole::Owner,
                true,
                Duration::from_secs(3600),
            )
            .await
            .expect("create session");

        let req = Request::builder()
            .method("POST")
            .uri("/account/team/role")
            .header(header::COOKIE, format!("__Host-session={token}"))
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from("csrf_token=test&user_id=2&new_role=admin"))
            .expect("build request");

        let router = account_router(state);
        let resp = router.oneshot(req).await.expect("send request");
        // With invalid CSRF, should return 403.
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn account_remove_user_requires_post() {
        let state = make_state();
        let token = state
            .session_store
            .create(
                1,
                1,
                kb_core::user::UserRole::Owner,
                true,
                Duration::from_secs(3600),
            )
            .await
            .expect("create session");

        let req = Request::builder()
            .method("POST")
            .uri("/account/team/remove")
            .header(header::COOKIE, format!("__Host-session={token}"))
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from("csrf_token=test&user_id=2"))
            .expect("build request");

        let router = account_router(state);
        let resp = router.oneshot(req).await.expect("send request");
        // With invalid CSRF, should return 403.
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn account_team_page_renders_with_disconnected_db() {
        let state = make_state();
        let (status, body) = make_authed_request(&state, "GET", "/account/team").await;
        // Handler is fallback-safe; renders with empty team list.
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("Team Members (0)"));
    }

    #[tokio::test]
    async fn account_plan_page_renders_with_disconnected_db() {
        let state = make_state();
        let (status, body) = make_authed_request(&state, "GET", "/account/plan").await;
        // Handler is fallback-safe; renders with empty plans and a placeholder message.
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("Plans are being loaded"));
    }

    #[tokio::test]
    async fn account_danger_page_renders_with_disconnected_db() {
        let state = make_state();
        let (status, body) = make_authed_request(&state, "GET", "/account/danger").await;
        // Handler is fallback-safe; renders with "unknown" tenant slug.
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("Danger Zone"));
    }

    #[test]
    fn account_page_template_renders_with_error() {
        let page = AccountPage {
            csrf_token: "test-csrf".into(),
            email: "a@b.com".into(),
            tenant_slug: "ws".into(),
            tenant_name: "Workspace".into(),
            plan_name: "Free".into(),
            badge: BillingBadge {
                label: "Past Due".into(),
                css_class: "bg-red-100 text-red-800".into(),
            },
            has_stripe_customer: false,
            email_verified: true,
            storage_bar: QuotaBar {
                label: "Storage".into(),
                used_display: "100 B".into(),
                limit_display: "Unlimited".into(),
                percent: 0,
            },
            token_bar: QuotaBar {
                label: "Tokens".into(),
                used_display: "1.0K".into(),
                limit_display: "Unlimited".into(),
                percent: 0,
            },
            error: "Something broke".into(),
        };
        let html = page
            .render()
            .expect("account template must render with error");
        assert!(html.contains("Something broke"));
        assert!(html.contains("Past Due"));
        assert!(html.contains("Unlimited"));
    }

    #[test]
    fn account_danger_page_template_renders_no_error() {
        let page = AccountDangerPage {
            csrf_token: "test-csrf".into(),
            tenant_slug: "myorg".into(),
            error: String::new(),
        };
        let html = page
            .render()
            .expect("danger template must render without error");
        // The error div should NOT be present when error is empty.
        assert!(!html.contains("rounded-md bg-red-50 p-4 border border-red-200"));
    }

    // ── Form deserialization tests ────────────────────────────────────────

    #[test]
    fn change_role_form_deserialization() {
        let form: ChangeRoleForm =
            serde_urlencoded::from_str("csrf_token=abc123&user_id=42&new_role=admin")
                .expect("deserialize ChangeRoleForm");
        assert_eq!(form.csrf_token, "abc123");
        assert_eq!(form.user_id, 42);
        assert_eq!(form.new_role, "admin");
    }

    #[test]
    fn invite_user_form_deserialization() {
        let form: InviteUserForm =
            serde_urlencoded::from_str("csrf_token=abc123&email=user@example.com&role=member")
                .expect("deserialize InviteUserForm");
        assert_eq!(form.csrf_token, "abc123");
        assert_eq!(form.email, "user@example.com");
        assert_eq!(form.role, "member");
    }

    #[test]
    fn remove_user_form_deserialization() {
        let form: RemoveUserForm = serde_urlencoded::from_str("csrf_token=abc123&user_id=99")
            .expect("deserialize RemoveUserForm");
        assert_eq!(form.csrf_token, "abc123");
        assert_eq!(form.user_id, 99);
    }

    #[test]
    fn delete_account_form_deserialization() {
        let form: DeleteAccountForm =
            serde_urlencoded::from_str("csrf_token=abc123&confirm_slug=myworkspace")
                .expect("deserialize DeleteAccountForm");
        assert_eq!(form.csrf_token, "abc123");
        assert_eq!(form.confirm_slug, "myworkspace");
    }

    // ── format_bytes edge cases ──────────────────────────────────────────

    #[test]
    fn format_bytes_gb_boundary() {
        // Exactly 1 GB
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.0 GB");
    }

    #[test]
    fn format_bytes_mb_boundary() {
        // Exactly 1 MB
        assert_eq!(format_bytes(1024 * 1024), "1.0 MB");
    }

    #[test]
    fn format_bytes_kb_boundary() {
        // Exactly 1 KB
        assert_eq!(format_bytes(1024), "1.0 KB");
    }

    #[test]
    fn format_bytes_large_value() {
        // 100 GB
        let hundred_gb = 100i64 * 1024 * 1024 * 1024;
        assert_eq!(format_bytes(hundred_gb), "100.0 GB");
    }

    // ── format_tokens edge cases ─────────────────────────────────────────

    #[test]
    fn format_tokens_at_k_boundary() {
        assert_eq!(format_tokens(999), "999");
        assert_eq!(format_tokens(1000), "1.0K");
    }

    #[test]
    fn format_tokens_at_m_boundary() {
        assert_eq!(format_tokens(999_999), "1000.0K");
        assert_eq!(format_tokens(1_000_000), "1.0M");
    }

    #[test]
    fn format_tokens_large() {
        assert_eq!(format_tokens(50_000_000), "50.0M");
    }

    // ── usage_percent edge cases ─────────────────────────────────────────

    #[test]
    fn usage_percent_rounding() {
        // 1/3 = 33.33... → 33 (truncated)
        assert_eq!(usage_percent(1, 3), 33);
        // 2/3 = 66.66... → 66
        assert_eq!(usage_percent(2, 3), 66);
    }

    #[test]
    fn usage_percent_one_byte() {
        assert_eq!(usage_percent(1, 100), 1);
    }

    // ── Template: account page with past_due badge ───────────────────────

    #[test]
    fn account_page_template_past_due_badge() {
        let page = AccountPage {
            csrf_token: "t".into(),
            email: "x@y.com".into(),
            tenant_slug: "s".into(),
            tenant_name: "n".into(),
            plan_name: "Free".into(),
            badge: BillingBadge {
                label: "Past Due".into(),
                css_class: "bg-red-100 text-red-800".into(),
            },
            has_stripe_customer: true,
            email_verified: true,
            storage_bar: QuotaBar {
                label: "S".into(),
                used_display: "0 B".into(),
                limit_display: "Unlimited".into(),
                percent: 0,
            },
            token_bar: QuotaBar {
                label: "T".into(),
                used_display: "0".into(),
                limit_display: "Unlimited".into(),
                percent: 0,
            },
            error: String::new(),
        };
        let html = page.render().expect("render");
        assert!(html.contains("Past Due"));
        assert!(html.contains("bg-red-100"));
    }

    // ── Template: account team page with error ────────────────────────────

    #[test]
    fn account_team_page_template_with_members_and_error() {
        let page = AccountTeamPage {
            csrf_token: "c".into(),
            members: vec![TeamMember {
                user_id: 1,
                email: "one@test.com".into(),
                role_label: "admin".into(),
                is_current_user: false,
                role_options: vec![RoleOptionSel {
                    value: "admin".into(),
                    selected: true,
                }],
            }],
            owner_count: 0,
            is_owner: true,
            is_admin_or_owner: true,
            error: "Cannot remove last Owner".into(),
        };
        let html = page.render().expect("render");
        assert!(html.contains("Cannot remove last Owner"));
        assert!(html.contains("one@test.com"));
    }

    // ── Template: account plan with error ────────────────────────────────

    #[test]
    fn account_plan_page_template_with_error() {
        let page = AccountPlanPage {
            csrf_token: "c".into(),
            current_plan: "pro".into(),
            plans: vec![],
            error: "Failed to load plans".into(),
        };
        let html = page.render().expect("render");
        assert!(html.contains("Failed to load plans"));
    }

    // ── Additional template variant coverage ─────────────────────────────

    #[test]
    fn account_page_template_with_error_banner() {
        let page = AccountPage {
            csrf_token: "t".into(),
            email: "e@e.com".into(),
            tenant_slug: "s".into(),
            tenant_name: "N".into(),
            plan_name: "Free".into(),
            badge: BillingBadge {
                label: "Canceled".into(),
                css_class: "bg-yellow-100 text-yellow-800".into(),
            },
            has_stripe_customer: true,
            email_verified: true,
            storage_bar: QuotaBar {
                label: "S".into(),
                used_display: "0 B".into(),
                limit_display: "50.0 MB".into(),
                percent: 0,
            },
            token_bar: QuotaBar {
                label: "T".into(),
                used_display: "500".into(),
                limit_display: "10.0K".into(),
                percent: 5,
            },
            error: "Plan lookup failed".into(),
        };
        let html = page.render().expect("render");
        assert!(html.contains("Plan lookup failed"));
        assert!(html.contains("Canceled"));
    }

    #[test]
    fn account_team_page_template_admin_not_owner() {
        // Admin (non-owner) should not see owner promotion options.
        let page = AccountTeamPage {
            csrf_token: "c".into(),
            members: vec![TeamMember {
                user_id: 1,
                email: "admin@test.com".into(),
                role_label: "admin".into(),
                is_current_user: true,
                role_options: vec![
                    RoleOptionSel {
                        value: "owner".into(),
                        selected: false,
                    },
                    RoleOptionSel {
                        value: "admin".into(),
                        selected: true,
                    },
                    RoleOptionSel {
                        value: "member".into(),
                        selected: false,
                    },
                ],
            }],
            owner_count: 1,
            is_owner: false,
            is_admin_or_owner: true,
            error: String::new(),
        };
        let html = page.render().expect("render");
        // Admin should be able to invite.
        assert!(html.contains("Invite a Team Member"));
        // But owner option should NOT be available since is_owner=false.
        assert!(!html.contains("<option value=\"owner\">"));
    }

    // ── Template goodby with error path ──────────────────────────────────

    #[test]
    fn account_goodbye_page_template_has_return_link() {
        let page = AccountGoodbyePage {
            tenant_slug: "deleted-workspace".into(),
        };
        let html = page.render().expect("render");
        assert!(html.contains("Return Home"));
        assert!(html.contains("/"));
    }

    // ── price_display edge ──────────────────────────────────────────────

    #[test]
    fn price_display_large_amount() {
        assert_eq!(price_display(Some(99_00), Some("usd")), "$99/mo");
        assert_eq!(price_display(Some(19_900), Some("usd")), "$199/mo");
    }

    // ── format_tokens additional ────────────────────────────────────────

    #[test]
    fn format_tokens_tenth_of_k() {
        assert_eq!(format_tokens(100), "100");
        assert_eq!(format_tokens(9_999), "10.0K");
    }
}
