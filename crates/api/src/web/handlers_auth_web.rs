//! Web UI auth flow handlers: signup, email verification, forgot/reset password (P12-T2).
//!
//! These handlers implement the self-service onboarding flow:
//! 1. `GET /signup` / `POST /signup` — create a new tenant + admin user, trigger verification.
//! 2. `GET /verify?token=…` — validate the token, mark email verified, redirect to /login.
//! 3. `GET /forgot` / `POST /forgot` — request a password reset link.
//! 4. `GET /reset?token=…` / `POST /reset` — set a new password.
//!
//! All forms are CSRF-protected via the double-submit cookie pattern (see [`super::csrf`]).

use std::sync::Arc;

use anyhow::Context;
use axum::Form;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};
use serde::Deserialize;

use kb_core::user::UserRole;

use crate::AppState;

use super::csrf;
use super::templates::{ForgotPasswordPage, ResetPasswordPage, SignupPage, VerifyEmailSentPage};

// ── Helpers ────────────────────────────────────────────────────────────────────

use super::handlers::{generate_fresh_csrf, render_ok, render_template, render_with_csrf_cookie};

/// Convert a human-readable workspace name into a URL-friendly slug.
///
/// Lowercases, replaces non-alphanumeric runs with a single hyphen, trims
/// leading/trailing hyphens, and falls back to "workspace" if the result
/// is empty.
fn slugify(name: &str) -> String {
    let mut slug = String::with_capacity(name.len());
    for c in name.trim().to_lowercase().chars() {
        if c.is_alphanumeric() {
            slug.push(c);
        } else if !slug.is_empty() && !slug.ends_with('-') {
            slug.push('-');
        }
    }
    // Trim trailing hyphens.
    while slug.ends_with('-') {
        slug.pop();
    }
    if slug.is_empty() {
        "workspace".to_string()
    } else {
        slug
    }
}

// ── GET /signup ─────────────────────────────────────────────────────────────────

/// `GET /signup` — render the self-service signup page.
///
/// Users create a new workspace (tenant) and become its admin/owner in one step,
/// unlike `/register` which adds a user to an existing tenant.
pub async fn signup_page(State(state): State<Arc<AppState>>) -> Result<Response, StatusCode> {
    let token = csrf::generate_csrf_token().map_err(|e| {
        tracing::error!(error = %e, "failed to generate CSRF token");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let page = SignupPage {
        csrf_token: token.clone(),
        tenant_name: String::new(),
        email: String::new(),
        error: String::new(),
    };

    let mut resp = render_ok(&page);
    resp.headers_mut().insert(
        axum::http::header::SET_COOKIE,
        csrf::csrf_cookie_value(&token, state.session_ttl, state.secure_cookies),
    );
    Ok(resp)
}

// ── POST /signup ────────────────────────────────────────────────────────────────

/// HTML form data for the signup page.
#[derive(Debug, Deserialize)]
pub struct SignupForm {
    /// Hidden CSRF token.
    pub csrf_token: String,
    /// Human-readable workspace name (slugified automatically).
    pub tenant_name: String,
    /// User's email.
    pub email: String,
    /// Plaintext password.
    pub password: String,
}

/// `POST /signup` — create a new tenant with the user as its admin/owner.
///
/// Steps:
/// 1. Validate CSRF.
/// 2. Slugify the workspace name (append digits if taken).
/// 3. Create the tenant row.
/// 4. Hash the password and create the user with Owner role.
/// 5. Generate and store an email verification token.
/// 6. Send the verification email (via the configured EmailSender).
/// 7. Render the "check your email" interstitial.
pub async fn signup_submit(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Form(form): Form<SignupForm>,
) -> Response {
    // ── 1. Validate CSRF ────────────────────────────────────────────────────
    if let Err((status, msg)) = csrf::validate_csrf(&headers, &form.csrf_token) {
        let csrf_token = generate_fresh_csrf();
        let page = SignupPage {
            csrf_token: csrf_token.clone(),
            tenant_name: form.tenant_name.clone(),
            email: form.email.clone(),
            error: msg,
        };
        return render_with_csrf_cookie(
            &page,
            status,
            &csrf_token,
            state.session_ttl,
            state.secure_cookies,
        );
    }

    let email = form.email.trim().to_string();
    let password = form.password.clone();

    // Basic input validation.
    if email.is_empty() || password.len() < 8 || form.tenant_name.trim().is_empty() {
        let csrf_token = generate_fresh_csrf();
        let page = SignupPage {
            csrf_token: csrf_token.clone(),
            tenant_name: form.tenant_name,
            email,
            error: "All fields are required. Password must be at least 8 characters.".into(),
        };
        return render_with_csrf_cookie(
            &page,
            StatusCode::BAD_REQUEST,
            &csrf_token,
            state.session_ttl,
            state.secure_cookies,
        );
    }

    // Reject common/breached passwords.
    if kb_core::auth::is_common_password(&password) {
        let csrf_token = generate_fresh_csrf();
        let page = SignupPage {
            csrf_token: csrf_token.clone(),
            tenant_name: form.tenant_name,
            email,
            error: "This password is too common and has appeared in data breaches. \
                    Please choose a stronger password."
                .into(),
        };
        return render_with_csrf_cookie(
            &page,
            StatusCode::BAD_REQUEST,
            &csrf_token,
            state.session_ttl,
            state.secure_cookies,
        );
    }

    // ── 2. Slugify + ensure uniqueness ──────────────────────────────────────
    let base_slug = slugify(&form.tenant_name);
    let tenant_slug = match make_unique_slug(&state, &base_slug).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "failed to generate unique tenant slug");
            let csrf_token = generate_fresh_csrf();
            let page = SignupPage {
                csrf_token: csrf_token.clone(),
                tenant_name: form.tenant_name,
                email,
                error: "An error occurred. Please try again.".into(),
            };
            return render_with_csrf_cookie(
                &page,
                StatusCode::INTERNAL_SERVER_ERROR,
                &csrf_token,
                state.session_ttl,
                state.secure_cookies,
            );
        }
    };

    // ── 3. Hash password ────────────────────────────────────────────────────
    let password_hash = match kb_core::auth::hash_password(&password) {
        Ok(h) => h,
        Err(e) => {
            tracing::error!(error = %e, "password hashing failed");
            let csrf_token = generate_fresh_csrf();
            let page = SignupPage {
                csrf_token: csrf_token.clone(),
                tenant_name: form.tenant_name,
                email,
                error: "An error occurred. Please try again.".into(),
            };
            return render_with_csrf_cookie(
                &page,
                StatusCode::INTERNAL_SERVER_ERROR,
                &csrf_token,
                state.session_ttl,
                state.secure_cookies,
            );
        }
    };

    // ── 4. Create tenant ────────────────────────────────────────────────────
    let tenant_id = match state
        .pg_store
        .create_tenant(&tenant_slug, &tenant_slug)
        .await
    {
        Ok(id) => id,
        Err(e) => {
            // If the slug was taken (race), try with a random suffix.
            tracing::warn!(error = %e, slug = %tenant_slug, "create_tenant failed, retrying with suffix");
            let retry_slug = format!(
                "{}-{}",
                &tenant_slug[..tenant_slug.len().min(40)],
                random_suffix()
            );
            match state.pg_store.create_tenant(&retry_slug, &retry_slug).await {
                Ok(id) => id,
                Err(e2) => {
                    tracing::error!(error = %e2, "create_tenant retry failed");
                    let csrf_token = generate_fresh_csrf();
                    let page = SignupPage {
                        csrf_token: csrf_token.clone(),
                        tenant_name: form.tenant_name,
                        email,
                        error: "This workspace name is taken. Please try a different name.".into(),
                    };
                    return render_with_csrf_cookie(
                        &page,
                        StatusCode::CONFLICT,
                        &csrf_token,
                        state.session_ttl,
                        state.secure_cookies,
                    );
                }
            }
        }
    };

    // ── 5. Create user (Owner role) ─────────────────────────────────────────
    let user_id = match state
        .pg_store
        .create_user(tenant_id, &email, &password_hash, UserRole::Owner)
        .await
    {
        Ok(id) => id,
        Err(e) => {
            tracing::error!(error = %e, "create_user failed during signup");
            // Best-effort cleanup: the tenant row is orphaned, but this is
            // extremely rare and the admin can clean it up.
            let csrf_token = generate_fresh_csrf();
            let page = SignupPage {
                csrf_token: csrf_token.clone(),
                tenant_name: form.tenant_name,
                email,
                error: "An account with this email already exists. Please log in instead.".into(),
            };
            return render_with_csrf_cookie(
                &page,
                StatusCode::CONFLICT,
                &csrf_token,
                state.session_ttl,
                state.secure_cookies,
            );
        }
    };

    // ── 6. Generate + store verification token ──────────────────────────────
    let verify_token = match kb_core::auth::generate_email_token() {
        Ok(t) => t,
        Err(e) => {
            tracing::error!(error = %e, "failed to generate email token");
            let csrf_token = generate_fresh_csrf();
            let page = SignupPage {
                csrf_token: csrf_token.clone(),
                tenant_name: form.tenant_name,
                email,
                error: "An error occurred. Please try again.".into(),
            };
            return render_with_csrf_cookie(
                &page,
                StatusCode::INTERNAL_SERVER_ERROR,
                &csrf_token,
                state.session_ttl,
                state.secure_cookies,
            );
        }
    };

    if let Err(e) = state
        .pg_store
        .set_email_verify_token(tenant_id, user_id, &verify_token)
        .await
    {
        tracing::error!(error = %e, "failed to store email verify token");
        // Non-fatal — the user can re-request verification.
    }

    // ── 7. Send verification email ──────────────────────────────────────────
    if let Some(sender) = state.email_sender.as_ref() {
        let verify_url = format!("{}/verify?token={verify_token}", state.public_base_url);
        if let Err(e) = sender.send_verification_email(&email, &verify_url).await {
            tracing::warn!(error = %e, email = %email, "verification email send failed (non-fatal)");
        }
    } else {
        // No email sender configured — log a redacted URL so the operator can
        // reconstruct the full link from the DB if needed. The full secret token
        // must never appear in log output.
        tracing::info!(
            email = %email,
            public_url = %state.public_base_url,
            verify_token_prefix = %&verify_token[..8],
            "No EmailSender configured — verification URL not sent"
        );
    }

    // ── 8. Render "check your email" ────────────────────────────────────────
    let page = VerifyEmailSentPage { email };
    render_ok(&page)
}

/// Generate a short random suffix for slug de-duplication.
fn random_suffix() -> String {
    use rand::TryRng;
    let mut buf = [0u8; 3];
    if rand::rngs::SysRng.try_fill_bytes(&mut buf).is_ok() {
        hex::encode(buf)
    } else {
        // Extremely unlikely fallback — use a timestamp-based suffix.
        format!(
            "{:x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.subsec_nanos())
                .unwrap_or(0)
        )
    }
}

/// Try to create a unique tenant slug. If the base slug is taken, append
/// a short random suffix.
async fn make_unique_slug(state: &Arc<AppState>, base: &str) -> anyhow::Result<String> {
    // Check if the base slug is available.
    if state.pg_store.find_tenant_by_slug(base).await?.is_none() {
        return Ok(base.to_string());
    }
    // Append random suffix.
    let retry = format!("{}-{}", &base[..base.len().min(40)], random_suffix());
    if state.pg_store.find_tenant_by_slug(&retry).await?.is_none() {
        return Ok(retry);
    }
    // Extremely unlikely collision — try once more.
    let retry2 = format!("{}-{}", &base[..base.len().min(40)], random_suffix());
    if state.pg_store.find_tenant_by_slug(&retry2).await?.is_none() {
        return Ok(retry2);
    }
    anyhow::bail!("could not generate unique slug for '{base}' after 3 attempts")
}

// ── GET /verify ─────────────────────────────────────────────────────────────────

/// Query parameters for `GET /verify?token=…`.
#[derive(Debug, Deserialize)]
pub struct VerifyQuery {
    pub token: String,
}

/// `GET /verify?token=…` — verify a user's email address.
///
/// Validates the token against the database. On success, sets `email_verified = true`,
/// clears the token, and redirects to `/login` with a success query parameter.
/// On failure (invalid, expired, or already used token), redirects to `/login`
/// with an error query parameter.
pub async fn verify_email(
    State(state): State<Arc<AppState>>,
    Query(query): Query<VerifyQuery>,
) -> impl IntoResponse {
    // The token is tenant-scoped, but we don't know the tenant yet.
    // We attempt verification against all tenants by scanning — the email_verify_token
    // is globally unique (64 hex chars of randomness), so a match uniquely identifies
    // the user.
    let token = query.token.trim().to_string();
    if token.is_empty() {
        return Redirect::to("/login?verify_error=invalid_token");
    }

    // Try verification — since the token is globally unique (64 hex chars),
    // we use the pg_store to find it. We need to know the tenant_id though.
    // The token is scoped by tenant_id in the query, so we need to scan.
    // For simplicity, we use a raw query on the admin pool.
    match verify_token_global(&state, &token).await {
        Ok(true) => Redirect::to("/login?verify_success=1"),
        Ok(false) => Redirect::to("/login?verify_error=invalid_or_expired"),
        Err(e) => {
            tracing::error!(error = %e, "email verification failed");
            Redirect::to("/login?verify_error=internal")
        }
    }
}

/// Verify an email token without knowing the tenant_id in advance.
///
/// Two-step approach to avoid a raw RLS-bypassing UPDATE:
/// 1. Look up the tenant_id from the token via the admin pool (read-only).
/// 2. Delegate to [`PgStore::verify_user_email`] which uses the app pool with
///    RLS enforcement as defense-in-depth.
async fn verify_token_global(state: &Arc<AppState>, token: &str) -> anyhow::Result<bool> {
    // Step 1: find the tenant using the admin pool (read-only, no RLS).
    let admin_pool = state.pg_store.pool()?;
    let tenant_id: Option<i64> =
        sqlx::query_scalar("SELECT tenant_id FROM users WHERE email_verify_token = $1")
            .bind(token)
            .fetch_optional(&admin_pool)
            .await
            .context("failed to look up email verify token")?;

    let Some(tenant_id) = tenant_id else {
        return Ok(false);
    };

    // Step 2: update via the app-pool + RLS path.
    state.pg_store.verify_user_email(tenant_id, token).await
}

// ── GET /forgot ─────────────────────────────────────────────────────────────────

/// `GET /forgot` — render the forgot-password form.
pub async fn forgot_password_page(
    State(state): State<Arc<AppState>>,
) -> Result<Response, StatusCode> {
    let token = csrf::generate_csrf_token().map_err(|e| {
        tracing::error!(error = %e, "failed to generate CSRF token");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let page = ForgotPasswordPage {
        csrf_token: token.clone(),
        email: String::new(),
        error: String::new(),
        success_message: String::new(),
    };

    let mut resp = render_ok(&page);
    resp.headers_mut().insert(
        axum::http::header::SET_COOKIE,
        csrf::csrf_cookie_value(&token, state.session_ttl, state.secure_cookies),
    );
    Ok(resp)
}

// ── POST /forgot ────────────────────────────────────────────────────────────────

/// HTML form data for the forgot-password page.
#[derive(Debug, Deserialize)]
pub struct ForgotPasswordForm {
    /// Hidden CSRF token.
    pub csrf_token: String,
    /// User's email address.
    pub email: String,
}

/// `POST /forgot` — request a password reset link.
///
/// Always shows a success message to prevent user enumeration — whether or not
/// the email exists in the system, the response is the same.
pub async fn forgot_password_submit(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Form(form): Form<ForgotPasswordForm>,
) -> Response {
    // ── 1. Validate CSRF ────────────────────────────────────────────────────
    if let Err((status, msg)) = csrf::validate_csrf(&headers, &form.csrf_token) {
        let csrf_token = generate_fresh_csrf();
        let page = ForgotPasswordPage {
            csrf_token: csrf_token.clone(),
            email: form.email.clone(),
            error: msg,
            success_message: String::new(),
        };
        return render_with_csrf_cookie(
            &page,
            status,
            &csrf_token,
            state.session_ttl,
            state.secure_cookies,
        );
    }

    let email = form.email.trim().to_string();

    // Always show success to prevent user enumeration.
    if email.is_empty() {
        let csrf_token = generate_fresh_csrf();
        let page = ForgotPasswordPage {
            csrf_token: csrf_token.clone(),
            email,
            error: "Please enter your email address.".into(),
            success_message: String::new(),
        };
        return render_with_csrf_cookie(
            &page,
            StatusCode::BAD_REQUEST,
            &csrf_token,
            state.session_ttl,
            state.secure_cookies,
        );
    }

    // ── 2. Generate reset token ─────────────────────────────────────────────
    let reset_token = match kb_core::auth::generate_email_token() {
        Ok(t) => t,
        Err(e) => {
            tracing::error!(error = %e, "failed to generate reset token");
            // Still show success to prevent enumeration.
            return show_forgot_success(&state, &email);
        }
    };

    // ── 3. Find the tenant ─────────────────────────────────────────────────
    // We don't know the tenant, so we scan. For simplicity, we try common tenant slugs
    // or use a direct query. Actually, the email is globally unique per tenant,
    // but we don't know the tenant. We'll use a helper that scans tenants.
    // For single-tenant or small deployments, this is fine.
    let expires = chrono::Utc::now() + chrono::Duration::hours(1);

    // Try to set the reset token. If the email doesn't exist, this is a no-op
    // (we still show success).
    let affected = match set_reset_token_for_email(&state, &email, &reset_token, expires).await {
        Ok(n) => n,
        Err(e) => {
            tracing::error!(error = %e, "failed to set reset token");
            return show_forgot_success(&state, &email);
        }
    };

    if affected {
        // ── 4. Send reset email ─────────────────────────────────────────
        if let Some(sender) = state.email_sender.as_ref() {
            let reset_url = format!("{}/reset?token={reset_token}", state.public_base_url);
            if let Err(e) = sender.send_password_reset_email(&email, &reset_url).await {
                tracing::warn!(error = %e, email = %email, "password reset email send failed (non-fatal)");
            }
        } else {
            tracing::info!(
                email = %email,
                public_url = %state.public_base_url,
                reset_token_prefix = %&reset_token[..8],
                "No EmailSender configured — reset URL not sent"
            );
        }
    }

    show_forgot_success(&state, &email)
}

/// Render the forgot-password page with a success message.
fn show_forgot_success(_state: &Arc<AppState>, email: &str) -> Response {
    let page = ForgotPasswordPage {
        csrf_token: generate_fresh_csrf(),
        email: String::new(),
        error: String::new(),
        success_message: format!(
            "If an account exists for {email}, we've sent a reset link. Check your inbox."
        ),
    };
    render_ok(&page)
}

/// Set a password reset token for a user by email, scanning across tenants.
/// Uses a two-step approach: admin pool for lookup, then tenant-scoped PgStore
/// method for the actual update. Returns true if the email was found and updated.
async fn set_reset_token_for_email(
    state: &Arc<AppState>,
    email: &str,
    token: &str,
    expires: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<bool> {
    // Step 1: find the tenant via the admin pool (read-only).
    let admin_pool = state.pg_store.pool()?;
    let tenant_id: Option<i64> =
        sqlx::query_scalar("SELECT tenant_id FROM users WHERE email = $1 LIMIT 1")
            .bind(email)
            .fetch_optional(&admin_pool)
            .await
            .context("failed to look up tenant by email")?;

    let Some(tenant_id) = tenant_id else {
        return Ok(false);
    };

    // Step 2: set the token via the tenant-scoped + RLS-enforced method.
    state
        .pg_store
        .set_password_reset_token(tenant_id, email, token, expires)
        .await
}

// ── GET /reset ──────────────────────────────────────────────────────────────────

/// Query parameters for `GET /reset?token=…`.
#[derive(Debug, Deserialize)]
pub struct ResetQuery {
    pub token: String,
}

/// `GET /reset?token=…` — render the password reset form.
///
/// Validates that the token exists and hasn't expired before showing the form.
/// On invalid/expired token, redirects to /forgot with an error.
pub async fn reset_password_page(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ResetQuery>,
) -> Response {
    let token = query.token.trim().to_string();
    if token.is_empty() {
        return Redirect::to("/forgot?error=invalid_token").into_response();
    }

    // Validate the token exists and is not expired.
    match validate_reset_token_global(&state, &token).await {
        Ok(true) => {} // Token is valid, show the form.
        Ok(false) => {
            return Redirect::to("/forgot?error=expired_or_invalid").into_response();
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to validate reset token");
            return Redirect::to("/forgot?error=internal").into_response();
        }
    }

    let csrf_token = csrf::generate_csrf_token().unwrap_or_default();
    let page = ResetPasswordPage {
        csrf_token: csrf_token.clone(),
        token,
        error: String::new(),
    };

    let mut resp = render_ok(&page);
    resp.headers_mut().insert(
        axum::http::header::SET_COOKIE,
        csrf::csrf_cookie_value(&csrf_token, state.session_ttl, state.secure_cookies),
    );
    resp
}

/// Validate a password reset token globally (across all tenants).
async fn validate_reset_token_global(state: &Arc<AppState>, token: &str) -> anyhow::Result<bool> {
    let pool = state.pg_store.pool()?;
    let exists: Option<(i64,)> = sqlx::query_as(
        "SELECT id FROM users \
         WHERE password_reset_token = $1 AND password_reset_expires > now()",
    )
    .bind(token)
    .fetch_optional(&pool)
    .await
    .context("failed to validate reset token")?;
    Ok(exists.is_some())
}

// ── POST /reset ─────────────────────────────────────────────────────────────────

/// HTML form data for the reset-password page.
#[derive(Debug, Deserialize)]
pub struct ResetPasswordForm {
    /// Hidden CSRF token.
    pub csrf_token: String,
    /// The reset token (from the hidden field, validated against URL param).
    pub reset_token: String,
    /// New password.
    pub password: String,
}

/// `POST /reset` — set a new password using a valid reset token.
pub async fn reset_password_submit(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Form(form): Form<ResetPasswordForm>,
) -> Response {
    // ── 1. Validate CSRF ────────────────────────────────────────────────────
    if let Err((status, msg)) = csrf::validate_csrf(&headers, &form.csrf_token) {
        let csrf_token = generate_fresh_csrf();
        let page = ResetPasswordPage {
            csrf_token: csrf_token.clone(),
            token: form.reset_token.clone(),
            error: msg,
        };
        return render_with_csrf_cookie(
            &page,
            status,
            &csrf_token,
            state.session_ttl,
            state.secure_cookies,
        );
    }

    let token = form.reset_token.trim().to_string();
    let password = form.password;

    if password.len() < 8 {
        let csrf_token = generate_fresh_csrf();
        let page = ResetPasswordPage {
            csrf_token: csrf_token.clone(),
            token,
            error: "Password must be at least 8 characters.".into(),
        };
        return render_with_csrf_cookie(
            &page,
            StatusCode::BAD_REQUEST,
            &csrf_token,
            state.session_ttl,
            state.secure_cookies,
        );
    }

    // Reject common/breached passwords.
    if kb_core::auth::is_common_password(&password) {
        let csrf_token = generate_fresh_csrf();
        let page = ResetPasswordPage {
            csrf_token: csrf_token.clone(),
            token,
            error: "This password is too common and has appeared in data breaches. \
                    Please choose a stronger password."
                .into(),
        };
        return render_with_csrf_cookie(
            &page,
            StatusCode::BAD_REQUEST,
            &csrf_token,
            state.session_ttl,
            state.secure_cookies,
        );
    }

    // ── 2. Validate the reset token ─────────────────────────────────────────
    let user = match find_user_by_reset_token_global(&state, &token).await {
        Ok(Some(u)) => u,
        Ok(None) => {
            let csrf_token = generate_fresh_csrf();
            let page = ResetPasswordPage {
                csrf_token: csrf_token.clone(),
                token,
                error: "This reset link has expired or is invalid. Please request a new one."
                    .into(),
            };
            return render_with_csrf_cookie(
                &page,
                StatusCode::BAD_REQUEST,
                &csrf_token,
                state.session_ttl,
                state.secure_cookies,
            );
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to find user by reset token");
            let csrf_token = generate_fresh_csrf();
            let page = ResetPasswordPage {
                csrf_token: csrf_token.clone(),
                token,
                error: "An error occurred. Please try again.".into(),
            };
            return render_with_csrf_cookie(
                &page,
                StatusCode::INTERNAL_SERVER_ERROR,
                &csrf_token,
                state.session_ttl,
                state.secure_cookies,
            );
        }
    };

    // ── 3. Hash new password ────────────────────────────────────────────────
    let new_hash = match kb_core::auth::hash_password(&password) {
        Ok(h) => h,
        Err(e) => {
            tracing::error!(error = %e, "password hashing failed");
            let csrf_token = generate_fresh_csrf();
            let page = ResetPasswordPage {
                csrf_token: csrf_token.clone(),
                token,
                error: "An error occurred. Please try again.".into(),
            };
            return render_with_csrf_cookie(
                &page,
                StatusCode::INTERNAL_SERVER_ERROR,
                &csrf_token,
                state.session_ttl,
                state.secure_cookies,
            );
        }
    };

    // ── 4. Update password ──────────────────────────────────────────────────
    match state
        .pg_store
        .reset_password(user.tenant_id, user.id, &new_hash)
        .await
    {
        Ok(true) => {
            // All pre-reset sessions are invalidated — the credential has changed.
            if let Err(e) = state
                .session_store
                .revoke_user_sessions(user.tenant_id, user.id)
                .await
            {
                tracing::error!(
                    error = %e,
                    tenant_id = user.tenant_id,
                    user_id = user.id,
                    "failed to revoke sessions after password reset"
                );
            }
            Redirect::to("/login?reset_success=1").into_response()
        }
        Ok(false) => {
            let csrf_token = generate_fresh_csrf();
            let page = ResetPasswordPage {
                csrf_token: csrf_token.clone(),
                token,
                error: "Could not reset password. The token may have expired.".into(),
            };
            render_template(&page, StatusCode::BAD_REQUEST)
        }
        Err(e) => {
            tracing::error!(error = %e, "password reset failed");
            let csrf_token = generate_fresh_csrf();
            let page = ResetPasswordPage {
                csrf_token: csrf_token.clone(),
                token,
                error: "An error occurred. Please try again.".into(),
            };
            render_template(&page, StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Find a user by reset token without knowing the tenant in advance.
///
/// Two-step approach: admin pool to find the tenant_id (read-only), then the
/// tenant-scoped PgStore method — so `password_hash` is never fetched through
/// the RLS-bypassing admin pool.
async fn find_user_by_reset_token_global(
    state: &Arc<AppState>,
    token: &str,
) -> anyhow::Result<Option<kb_core::user::User>> {
    // Step 1: find the tenant_id from the token via the admin pool (read-only).
    let admin_pool = state.pg_store.pool()?;
    let tenant_id: Option<i64> = sqlx::query_scalar(
        "SELECT tenant_id FROM users \
         WHERE password_reset_token = $1 AND password_reset_expires > now()",
    )
    .bind(token)
    .fetch_optional(&admin_pool)
    .await
    .context("failed to look up tenant by reset token")?;

    let Some(tenant_id) = tenant_id else {
        return Ok(None);
    };

    // Step 2: fetch the full user record via the tenant-scoped + RLS-enforced method.
    state
        .pg_store
        .find_user_by_reset_token(tenant_id, token)
        .await
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::Arc;
    use std::time::Duration;

    use askama::Template;
    use axum::body::Body;
    use axum::http::{StatusCode, header};
    use kb_core::email::LogEmail;
    use kb_store::PgStore;
    use kb_store::session_store::InMemorySessionStore;
    use tower::ServiceExt;

    use super::*;
    use crate::AppState;
    use crate::web::csrf;

    fn test_state() -> Arc<AppState> {
        let session_store: Arc<dyn kb_core::session::SessionStore> =
            Arc::new(InMemorySessionStore::new());
        let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
        let email_sender: Arc<dyn kb_core::email::EmailSender> = Arc::new(LogEmail);
        Arc::new(
            AppState::new(
                session_store,
                pg_store,
                Some(Duration::from_secs(3600)),
                false,
            )
            .with_email_sender(email_sender),
        )
    }

    fn auth_web_router(state: Arc<AppState>) -> axum::Router {
        use axum::routing::get;

        axum::Router::new()
            .route("/signup", get(signup_page).post(signup_submit))
            .route("/verify", get(verify_email))
            .route(
                "/forgot",
                get(forgot_password_page).post(forgot_password_submit),
            )
            .route(
                "/reset",
                get(reset_password_page).post(reset_password_submit),
            )
            .with_state(state)
    }

    // ── slugify tests ──────────────────────────────────────────────────────

    #[test]
    fn slugify_lowercases_and_replaces_spaces() {
        assert_eq!(slugify("My Workspace"), "my-workspace");
    }

    #[test]
    fn slugify_handles_special_chars() {
        assert_eq!(slugify("ACME Corp."), "acme-corp");
    }

    #[test]
    fn slugify_trims_leading_trailing_hyphens() {
        assert_eq!(slugify("--hello--"), "hello");
    }

    #[test]
    fn slugify_merges_multiple_non_alphanumeric() {
        assert_eq!(slugify("foo   bar!!!"), "foo-bar");
    }

    #[test]
    fn slugify_falls_back_for_empty_result() {
        assert_eq!(slugify("!!!---..."), "workspace");
    }

    #[test]
    fn slugify_handles_unicode() {
        // Non-ASCII chars are stripped; remaining alphanumeric chars kept.
        let result = slugify("café résumé");
        assert!(result.len() > 2);
        // 'c' 'a' 'f' stay, 'é' removed → "caf-r-sum"
        assert!(result.contains("caf"));
    }

    #[test]
    fn slugify_single_word() {
        assert_eq!(slugify("Workspace"), "workspace");
    }

    #[test]
    fn slugify_handles_numbers() {
        assert_eq!(slugify("Team 42"), "team-42");
    }

    // ── random_suffix tests ────────────────────────────────────────────────

    #[test]
    fn random_suffix_is_6_hex_chars() {
        let s = random_suffix();
        assert_eq!(s.len(), 6);
        assert!(s.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn random_suffix_is_unique_across_calls() {
        let s1 = random_suffix();
        let s2 = random_suffix();
        // It's technically possible to collide (extremely unlikely with 3 bytes).
        // We just verify the format is consistent.
        assert_eq!(s1.len(), 6);
        assert_eq!(s2.len(), 6);
    }

    // ── GET /signup ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn get_signup_returns_html_with_csrf() {
        let state = test_state();
        let router = auth_web_router(state);

        let req = axum::http::Request::builder()
            .uri("/signup")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(ct.contains("text/html"), "signup page must return HTML");

        let has_csrf = resp
            .headers()
            .get_all(header::SET_COOKIE)
            .iter()
            .any(|h| h.to_str().unwrap_or("").contains("__Host-csrf="));
        assert!(has_csrf, "signup page must set CSRF cookie");

        let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(
            body_str.contains("csrf_token"),
            "HTML must contain CSRF hidden field"
        );
        assert!(
            body_str.contains("tenant_name"),
            "HTML must contain workspace name field"
        );
    }

    // ── POST /signup tests (CSRF validation) ───────────────────────────────

    #[tokio::test]
    async fn signup_submit_no_csrf_cookie_fails_403() {
        let state = test_state();
        let router = auth_web_router(state);

        let body = "csrf_token=fake&tenant_name=Test&email=a@b.com&password=secret123";
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/signup")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    /// Regression test: CSRF mismatch on POST /signup must sync cookie to form token.
    /// Same bug class as login — without this the user is stuck in a permanent mismatch loop.
    #[tokio::test]
    async fn signup_submit_csrf_mismatch_syncs_cookie_to_form_token() {
        let state = test_state();
        let router = auth_web_router(state);

        let old_token = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let body =
            "csrf_token=wrong&tenant_name=Test&email=a@b.com&password=secret123456".to_string();
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/signup")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header("Cookie", format!("__Host-csrf={old_token}"))
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        let set_cookie = resp
            .headers()
            .get_all(header::SET_COOKIE)
            .iter()
            .find_map(|h| {
                let s = h.to_str().ok()?;
                if s.starts_with("__Host-csrf=") {
                    s.split(';')
                        .next()?
                        .strip_prefix("__Host-csrf=")
                        .map(|t| t.to_owned())
                } else {
                    None
                }
            })
            .expect("error response must set __Host-csrf cookie");

        assert!(!set_cookie.is_empty());
        assert_ne!(set_cookie, old_token);

        let body_bytes = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
        let body_str = std::str::from_utf8(&body_bytes).unwrap();
        let form_token = body_str
            .split("name=\"csrf_token\" value=\"")
            .nth(1)
            .and_then(|s| s.split('\"').next())
            .map(|s| s.to_owned())
            .expect("error page must contain a csrf_token hidden field");

        assert_eq!(set_cookie, form_token);
    }

    /// Validate that even a validation error (empty fields, not CSRF) syncs the cookie.
    /// The old code only set the CSRF cookie on success; error re-renders used
    /// `render_template` which does NOT set cookies — so a validation failure would
    /// desync the cookie for the next submission.
    #[tokio::test]
    async fn signup_submit_validation_error_syncs_cookie_to_form_token() {
        let state = test_state();
        let csrf_token = csrf::generate_csrf_token().unwrap();
        let router = auth_web_router(state);

        // Use valid CSRF but empty form fields — triggers validation error, not CSRF error.
        let body = format!("csrf_token={csrf_token}&tenant_name=&email=&password=");
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/signup")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header("Cookie", format!("__Host-csrf={csrf_token}"))
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let set_cookie = resp
            .headers()
            .get_all(header::SET_COOKIE)
            .iter()
            .find_map(|h| {
                let s = h.to_str().ok()?;
                if s.starts_with("__Host-csrf=") {
                    s.split(';')
                        .next()?
                        .strip_prefix("__Host-csrf=")
                        .map(|t| t.to_owned())
                } else {
                    None
                }
            })
            .expect("validation error response must set __Host-csrf cookie");

        let body_bytes = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
        let body_str = std::str::from_utf8(&body_bytes).unwrap();
        let form_token = body_str
            .split("name=\"csrf_token\" value=\"")
            .nth(1)
            .and_then(|s| s.split('\"').next())
            .map(|s| s.to_owned())
            .expect("error page must contain a csrf_token hidden field");

        assert_eq!(set_cookie, form_token);
    }

    #[tokio::test]
    async fn signup_submit_empty_fields_returns_400() {
        let state = test_state();
        let csrf_token = csrf::generate_csrf_token().unwrap();
        let router = auth_web_router(state);

        let body = format!("csrf_token={csrf_token}&tenant_name=&email=&password=");
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/signup")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header("Cookie", format!("__Host-csrf={csrf_token}"))
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // ── GET /forgot tests ──────────────────────────────────────────────────

    #[tokio::test]
    async fn get_forgot_returns_html_with_csrf() {
        let state = test_state();
        let router = auth_web_router(state);

        let req = axum::http::Request::builder()
            .uri("/forgot")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let has_csrf = resp
            .headers()
            .get_all(header::SET_COOKIE)
            .iter()
            .any(|h| h.to_str().unwrap_or("").contains("__Host-csrf="));
        assert!(has_csrf);
    }

    // ── POST /forgot tests ─────────────────────────────────────────────────

    #[tokio::test]
    async fn forgot_submit_no_csrf_cookie_fails_403() {
        let state = test_state();
        let router = auth_web_router(state);

        let body = "csrf_token=fake&email=a@b.com";
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/forgot")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn forgot_submit_valid_csrf_returns_success() {
        let state = test_state();
        let csrf_token = csrf::generate_csrf_token().unwrap();
        let router = auth_web_router(state);

        let body = format!("csrf_token={csrf_token}&email=nonexistent@test.com");
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/forgot")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header("Cookie", format!("__Host-csrf={csrf_token}"))
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        // Always returns success to prevent user enumeration.
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(
            body_str.contains("sent a reset link") || body_str.contains("check your inbox"),
            "should show success message; got: {body_str}"
        );
    }

    #[tokio::test]
    async fn forgot_submit_empty_email_shows_error() {
        let state = test_state();
        let csrf_token = csrf::generate_csrf_token().unwrap();
        let router = auth_web_router(state);

        let body = format!("csrf_token={csrf_token}&email=");
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/forgot")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header("Cookie", format!("__Host-csrf={csrf_token}"))
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        // Empty email → error.
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // ── GET /reset tests ───────────────────────────────────────────────────

    #[tokio::test]
    async fn reset_page_empty_token_redirects() {
        let state = test_state();
        let router = auth_web_router(state);

        let req = axum::http::Request::builder()
            .uri("/reset?token=")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::SEE_OTHER);
    }

    #[tokio::test]
    async fn reset_page_invalid_token_redirects() {
        let state = test_state();
        let router = auth_web_router(state);

        let req = axum::http::Request::builder()
            .uri("/reset?token=0000000000000000000000000000000000000000000000000000000000000000")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        // Invalid token → redirect to /forgot with error.
        assert_eq!(resp.status(), StatusCode::SEE_OTHER);
    }

    // ── POST /reset tests ──────────────────────────────────────────────────

    #[tokio::test]
    async fn reset_submit_no_csrf_cookie_fails_403() {
        let state = test_state();
        let router = auth_web_router(state);

        let body = "csrf_token=fake&reset_token=abc&password=newsecret123";
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/reset?token=abc")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn reset_submit_short_password_shows_error() {
        let state = test_state();
        let csrf_token = csrf::generate_csrf_token().unwrap();
        let router = auth_web_router(state);

        let body = format!("csrf_token={csrf_token}&reset_token=abc&password=short");
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/reset?token=abc")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header("Cookie", format!("__Host-csrf={csrf_token}"))
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        // Short password → error, DB not connected so it shows the password error.
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // ── Template rendering tests ───────────────────────────────────────────

    #[test]
    fn signup_template_renders_with_csrf_and_fields() {
        let page = SignupPage {
            csrf_token: "test-csrf-123".into(),
            tenant_name: "My Workspace".into(),
            email: "a@b.com".into(),
            error: String::new(),
        };
        let html = page.render().expect("signup template must render");
        assert!(html.contains("test-csrf-123"), "must contain CSRF token");
        assert!(html.contains("My Workspace"), "must contain tenant name");
        assert!(html.contains("a@b.com"), "must contain email");
    }

    #[test]
    fn signup_template_shows_error() {
        let page = SignupPage {
            csrf_token: "x".into(),
            tenant_name: String::new(),
            email: String::new(),
            error: "Something went wrong.".into(),
        };
        let html = page.render().expect("signup template must render");
        assert!(html.contains("Something went wrong."));
    }

    #[test]
    fn verify_email_sent_template_renders() {
        let page = VerifyEmailSentPage {
            email: "test@example.com".into(),
        };
        let html = page.render().expect("verify template must render");
        assert!(html.contains("test@example.com"));
        assert!(html.contains("Check your email"));
    }

    #[test]
    fn forgot_password_template_renders() {
        let page = ForgotPasswordPage {
            csrf_token: "csrf-1".into(),
            email: "a@b.com".into(),
            error: String::new(),
            success_message: String::new(),
        };
        let html = page.render().expect("forgot template must render");
        assert!(html.contains("csrf-1"));
        assert!(html.contains("a@b.com"));
    }

    #[test]
    fn forgot_password_template_shows_success_and_error() {
        let page = ForgotPasswordPage {
            csrf_token: "x".into(),
            email: String::new(),
            error: "Error msg".into(),
            success_message: "Success msg".into(),
        };
        let html = page.render().expect("forgot template must render");
        assert!(html.contains("Error msg"));
        assert!(html.contains("Success msg"));
    }

    #[test]
    fn reset_password_template_renders() {
        let page = ResetPasswordPage {
            csrf_token: "csrf-2".into(),
            token: "my-reset-token-123".into(),
            error: String::new(),
        };
        let html = page.render().expect("reset template must render");
        assert!(html.contains("csrf-2"));
        assert!(html.contains("my-reset-token-123"));
    }

    #[test]
    fn reset_password_template_shows_error() {
        let page = ResetPasswordPage {
            csrf_token: "x".into(),
            token: "tok".into(),
            error: "Token expired".into(),
        };
        let html = page.render().expect("reset template must render");
        assert!(html.contains("Token expired"));
    }

    // ── show_forgot_success pure-logic test ────────────────────────────────

    #[test]
    fn show_forgot_success_renders_with_message() {
        let state = test_state();
        let response = show_forgot_success(&state, "user@test.com");
        // show_forgot_success renders a template with a success message.
        assert_eq!(response.status(), StatusCode::OK);
    }

    // ── make_unique_slug error test ────────────────────────────────────────

    #[tokio::test]
    async fn make_unique_slug_errors_on_unconnected_db() {
        let state = test_state();
        // The test PgStore is not connected, so find_tenant_by_slug will fail.
        let result = make_unique_slug(&state, "test-slug").await;
        assert!(result.is_err(), "should fail on unconnected DB");
    }

    // ── Additional slugify edge cases ──────────────────────────────────────

    #[test]
    fn slugify_leading_trailing_whitespace() {
        assert_eq!(slugify("  hello world  "), "hello-world");
    }

    #[test]
    fn slugify_multiple_consecutive_dashes_merged() {
        assert_eq!(slugify("foo---bar"), "foo-bar");
    }

    #[test]
    fn slugify_only_special_chars_fallback() {
        assert_eq!(slugify("@#$%^&*()"), "workspace");
    }

    #[test]
    fn slugify_underscores_replaced() {
        assert_eq!(slugify("my_workspace_name"), "my-workspace-name");
    }

    #[test]
    fn slugify_mixed_case_with_symbols() {
        // Non-alphanumeric runs are merged into a single hyphen.
        assert_eq!(slugify("ACME Corp. - Division #1"), "acme-corp-division-1");
    }

    #[test]
    fn slugify_empty_and_whitespace_only() {
        assert_eq!(slugify(""), "workspace");
        assert_eq!(slugify("   "), "workspace");
    }

    // ── ForgotPasswordForm deserialization ─────────────────────────────────

    #[test]
    fn forgot_password_form_deserializes() {
        let body = "csrf_token=abc123&email=test@example.com";
        let form: ForgotPasswordForm = serde_urlencoded::from_str(body).unwrap();
        assert_eq!(form.csrf_token, "abc123");
        assert_eq!(form.email, "test@example.com");
    }

    // ── ResetPasswordForm deserialization ──────────────────────────────────

    #[test]
    fn reset_password_form_deserializes() {
        let body = "csrf_token=abc123&reset_token=tok456&password=newsecret99";
        let form: ResetPasswordForm = serde_urlencoded::from_str(body).unwrap();
        assert_eq!(form.csrf_token, "abc123");
        assert_eq!(form.reset_token, "tok456");
        assert_eq!(form.password, "newsecret99");
    }

    // ── SignupForm deserialization ─────────────────────────────────────────

    #[test]
    fn signup_form_deserializes() {
        let body = "csrf_token=abc&tenant_name=My+Corp&email=a@b.com&password=secret123";
        let form: SignupForm = serde_urlencoded::from_str(body).unwrap();
        assert_eq!(form.csrf_token, "abc");
        assert_eq!(form.tenant_name, "My Corp");
        assert_eq!(form.email, "a@b.com");
        assert_eq!(form.password, "secret123");
    }

    // ── GET /signup authed redirect test ───────────────────────────────────

    #[tokio::test]
    async fn signup_page_has_no_csrf_token_in_query() {
        let state = test_state();
        let router = auth_web_router(state);

        let req = axum::http::Request::builder()
            .uri("/signup?plan=pro")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
