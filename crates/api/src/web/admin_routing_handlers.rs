//! Admin-panel Web UI handlers for provider / model / route CRUD (plan §26.5,
//! ledger P9-T8).
//!
//! All handlers require authentication + an Admin/Owner role (enforced via
//! [`super::admin_handlers::require_admin`]). Every mutating handler writes an
//! audit event and sends a `NOTIFY routing_changed` so the scheduler picks up
//! the change without waiting for the next poll cycle.

use std::str::FromStr;
use std::sync::Arc;

use axum::Extension;
use axum::Form;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect, Response};
use kb_core::audit::AuditAction;
use kb_core::data_class::DataClass;
use kb_core::role::Role;
use kb_core::routing::{ProviderKind, RoutingStrategy};
use serde::Deserialize;

use crate::AppState;
use crate::AuthUser;

use super::admin_routing_templates::*;
use super::csrf;
use super::handlers::render_ok;

/// Return 403 if the authenticated user is not Owner or Admin.
fn require_admin(auth_user: &AuthUser) -> Result<(), StatusCode> {
    match auth_user.role {
        kb_core::user::UserRole::Owner | kb_core::user::UserRole::Admin => Ok(()),
        _ => Err(StatusCode::FORBIDDEN),
    }
}

// ── Form types ────────────────────────────────────────────────────────────────

/// Form: create or update a provider.
#[derive(Debug, Deserialize)]
pub struct ProviderForm {
    pub csrf_token: String,
    pub name: String,
    pub kind: String,
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    /// API key field (masked — encrypt+store lands in P10-T3).
    #[allow(dead_code)]
    pub api_key: String,
    #[serde(default)]
    pub headers_json: String,
}

/// Form: create or update a model.
#[derive(Debug, Deserialize)]
pub struct ModelForm {
    pub csrf_token: String,
    pub provider_id: i64,
    pub model_id: String,
    #[serde(default)]
    pub ctx_tokens: String,
    #[serde(default)]
    pub max_conc: String,
    #[serde(default)]
    pub rpm: String,
    #[serde(default)]
    pub tpm: String,
    #[serde(default)]
    pub price_in: String,
    #[serde(default)]
    pub price_out: String,
    pub data_class: String,
    // Capabilities are parsed from individual checkbox fields.
    #[serde(default)]
    pub cap_text: String,
    #[serde(default)]
    pub cap_vision: String,
    #[serde(default)]
    pub cap_code: String,
    #[serde(default)]
    pub cap_embed: String,
    #[serde(default)]
    pub cap_rerank: String,
}

/// Form: create or update a route.
#[derive(Debug, Deserialize)]
pub struct RouteForm {
    pub csrf_token: String,
    #[serde(default)]
    pub tenant_id: String,
    pub role: String,
    #[serde(default)]
    pub tier_str: String,
    pub strategy: String,
    #[serde(default)]
    pub spill_ms_str: String,
    #[serde(default)]
    pub model_id_str: String,
}

/// Form: CSRF-only (toggle / test / delete actions).
#[derive(Debug, Deserialize)]
pub struct CsrfForm {
    pub csrf_token: String,
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Parse an optional integer from a form field string, returning `None` on empty
/// or invalid input.
fn parse_optional_i32(s: &str) -> Option<i32> {
    let s = s.trim();
    if s.is_empty() { None } else { s.parse().ok() }
}

/// Parse an optional f64 from a form field string, returning `None` on empty or
/// invalid input.
fn parse_optional_f64(s: &str) -> Option<f64> {
    let s = s.trim();
    if s.is_empty() {
        None
    } else {
        s.parse::<f64>().ok()
    }
}

/// Collect capability strings from checkbox form fields.
fn caps_from_form(form: &ModelForm) -> Vec<String> {
    let mut caps = Vec::new();
    if form.cap_text == "on" {
        caps.push("text".into());
    }
    if form.cap_vision == "on" {
        caps.push("vision".into());
    }
    if form.cap_code == "on" {
        caps.push("code".into());
    }
    if form.cap_embed == "on" {
        caps.push("embed".into());
    }
    if form.cap_rerank == "on" {
        caps.push("rerank".into());
    }
    caps
}

/// Check whether a given cap is present in the cap list.
fn has_cap(caps: &[String], name: &str) -> bool {
    caps.iter().any(|c| c == name)
}

/// Format optional pricing fields as display strings.
#[allow(dead_code)]
fn price_display(val: Option<f64>) -> String {
    match val {
        None => "—".into(),
        Some(v) => format!("${v:.2}/Mt"),
    }
}

/// Format optional integer as display string.
fn opt_display(val: Option<i32>) -> String {
    match val {
        None => "—".into(),
        Some(v) => v.to_string(),
    }
}

/// Format optional f64 as display string.
fn opt_f64_str(val: Option<f64>) -> String {
    match val {
        None => String::new(),
        Some(v) => v.to_string(),
    }
}

/// Write an audit event + send a NOTIFY after a routing-table mutation.
/// Errors are logged but not propagated — the CRUD operation already succeeded.
async fn audit_and_notify(
    pg: &kb_store::PgStore,
    auth_user: &AuthUser,
    action: AuditAction,
    target_type: &str,
    target_id: Option<i64>,
    details: &serde_json::Value,
) {
    let _ = pg
        .admin_insert_audit_event(
            auth_user.user_id,
            auth_user.tenant_id,
            action.as_str(),
            target_type,
            target_id,
            details,
        )
        .await;
    if let Err(e) = pg.notify_routing_change().await {
        tracing::warn!(error = %e, "failed to send routing NOTIFY");
    }
}

/// Build provider options for model/route form `<select>` dropdowns.
async fn provider_options(pg: &kb_store::PgStore) -> Vec<(i64, String)> {
    pg.list_providers()
        .await
        .map(|list| list.into_iter().map(|p| (p.id, p.name)).collect())
        .unwrap_or_default()
}

/// Build model options for route form `<select>` dropdowns.
async fn model_options(pg: &kb_store::PgStore) -> Vec<(i64, String)> {
    let providers = pg.list_providers().await.unwrap_or_default();
    let prov_names: std::collections::HashMap<i64, String> =
        providers.into_iter().map(|p| (p.id, p.name)).collect();
    pg.list_models()
        .await
        .map(|list| {
            list.into_iter()
                .map(|m| {
                    let label = format!(
                        "{} / {}",
                        prov_names.get(&m.provider_id).map_or("?", |n| n.as_str()),
                        m.model_id
                    );
                    (m.id, label)
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Build a fresh CSRF token + set-cookie header value.
fn fresh_csrf(state: &AppState) -> (String, axum::http::HeaderValue) {
    let token = csrf::generate_csrf_token().unwrap_or_default();
    let cookie = csrf::csrf_cookie_value(&token, state.session_ttl, state.secure_cookies);
    (token, cookie)
}

// ── Provider list page ────────────────────────────────────────────────────────

/// `GET /admin/providers` — list all providers with toggle/test/edit actions.
pub async fn admin_providers_page(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Response, StatusCode> {
    require_admin(&auth_user)?;
    render_providers_list(&state, &auth_user, "", "").await
}

// ── Provider create form ──────────────────────────────────────────────────────

/// `GET /admin/providers/new` — show the provider creation form.
pub async fn admin_provider_new_form(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Response, StatusCode> {
    require_admin(&auth_user)?;
    let is_super = matches!(auth_user.role, kb_core::user::UserRole::Owner);
    let kind_options = ProviderKind::all()
        .iter()
        .map(|k| k.as_str().to_owned())
        .collect();
    let (csrf_token, cookie) = fresh_csrf(&state);

    let page = AdminProviderFormPage {
        csrf_token,
        error: String::new(),
        is_super_admin: is_super,
        provider_id: None,
        name: String::new(),
        kind: String::new(),
        endpoint: String::new(),
        headers_json: String::from("{}"),
        enabled: true,
        api_key_placeholder: String::new(),
        kind_options,
    };

    let mut resp = render_ok(&page);
    resp.headers_mut()
        .insert(axum::http::header::SET_COOKIE, cookie);
    Ok(resp)
}

// ── Provider create handler ───────────────────────────────────────────────────

/// `POST /admin/providers` — create a new provider.
pub async fn admin_provider_create(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    headers: axum::http::HeaderMap,
    Form(form): Form<ProviderForm>,
) -> Result<Response, StatusCode> {
    require_admin(&auth_user)?;
    csrf::validate_csrf(&headers, &form.csrf_token).map_err(|_| StatusCode::FORBIDDEN)?;

    let kind = match ProviderKind::from_str(&form.kind) {
        Ok(k) => k,
        Err(_) => {
            return render_providers_list(&state, &auth_user, "Invalid provider kind.", "").await;
        }
    };

    let endpoint = if form.endpoint.trim().is_empty() {
        None
    } else {
        Some(form.endpoint.trim())
    };

    // Validate: non-local providers need an endpoint.
    if endpoint.is_none() && kind != ProviderKind::Local {
        return render_providers_list(
            &state,
            &auth_user,
            &format!(
                "Provider kind '{}' requires an endpoint URL. Only 'local' providers can omit it.",
                kind.as_str()
            ),
            "",
        )
        .await;
    }

    let headers_json: serde_json::Value = if form.headers_json.trim().is_empty() {
        serde_json::json!({})
    } else {
        match serde_json::from_str(&form.headers_json) {
            Ok(v) => v,
            Err(e) => {
                return render_providers_list(
                    &state,
                    &auth_user,
                    &format!("Invalid headers JSON: {e}"),
                    "",
                )
                .await;
            }
        }
    };

    let pg = &state.pg_store;
    match pg
        .create_provider(&form.name, kind, endpoint, &headers_json)
        .await
    {
        Ok(pid) => {
            audit_and_notify(
                pg,
                &auth_user,
                AuditAction::ProviderCreated,
                "provider",
                Some(pid),
                &serde_json::json!({"name": form.name, "kind": kind.as_str()}),
            )
            .await;
        }
        Err(e) => {
            return render_providers_list(
                &state,
                &auth_user,
                &format!("Failed to create provider: {e}"),
                "",
            )
            .await;
        }
    }

    Ok(Redirect::to("/admin/providers").into_response())
}

// ── Provider edit form ────────────────────────────────────────────────────────

/// `GET /admin/providers/{id}/edit` — show the provider edit form.
pub async fn admin_provider_edit_form(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(id): Path<i64>,
) -> Result<Response, StatusCode> {
    require_admin(&auth_user)?;
    let pg = &state.pg_store;
    let provider = match pg.get_provider(id).await {
        Ok(Some(p)) => p,
        Ok(None) => return Err(StatusCode::NOT_FOUND),
        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };

    let kind_options = ProviderKind::all()
        .iter()
        .map(|k| k.as_str().to_owned())
        .collect();
    let is_super = matches!(auth_user.role, kb_core::user::UserRole::Owner);
    let (csrf_token, cookie) = fresh_csrf(&state);

    let endpoint_str = provider.endpoint.clone().unwrap_or_default();
    let headers_str = serde_json::to_string(&provider.headers).unwrap_or_else(|_| "{}".into());

    let page = AdminProviderFormPage {
        csrf_token,
        error: String::new(),
        is_super_admin: is_super,
        provider_id: Some(id),
        name: provider.name,
        kind: provider.kind.as_str().to_owned(),
        endpoint: endpoint_str,
        headers_json: headers_str,
        enabled: provider.enabled,
        api_key_placeholder: "•••••••• (leave blank to keep current)".into(),
        kind_options,
    };

    let mut resp = render_ok(&page);
    resp.headers_mut()
        .insert(axum::http::header::SET_COOKIE, cookie);
    Ok(resp)
}

// ── Provider update handler ───────────────────────────────────────────────────

/// `POST /admin/providers/{id}` — update a provider.
pub async fn admin_provider_update(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(id): Path<i64>,
    headers: axum::http::HeaderMap,
    Form(form): Form<ProviderForm>,
) -> Result<Response, StatusCode> {
    require_admin(&auth_user)?;
    csrf::validate_csrf(&headers, &form.csrf_token).map_err(|_| StatusCode::FORBIDDEN)?;

    let pg = &state.pg_store;

    let kind = ProviderKind::from_str(&form.kind).ok();
    let endpoint: Option<Option<&str>> = if form.endpoint.trim().is_empty() {
        Some(None)
    } else {
        Some(Some(form.endpoint.trim()))
    };

    let headers_json: Option<serde_json::Value> = if form.headers_json.trim().is_empty() {
        None
    } else {
        match serde_json::from_str(&form.headers_json) {
            Ok(v) => Some(v),
            Err(e) => {
                return render_providers_list(
                    &state,
                    &auth_user,
                    &format!("Invalid headers JSON: {e}"),
                    "",
                )
                .await;
            }
        }
    };

    match pg
        .update_provider(id, Some(&form.name), kind, endpoint, headers_json.as_ref())
        .await
    {
        Ok(()) => {
            audit_and_notify(
                pg,
                &auth_user,
                AuditAction::ProviderUpdated,
                "provider",
                Some(id),
                &serde_json::json!({"name": form.name}),
            )
            .await;
            render_providers_list(&state, &auth_user, "", "Provider updated.").await
        }
        Err(e) => {
            render_providers_list(
                &state,
                &auth_user,
                &format!("Failed to update provider: {e}"),
                "",
            )
            .await
        }
    }
}

// ── Provider toggle ───────────────────────────────────────────────────────────

/// `POST /admin/providers/{id}/toggle` — enable or disable a provider.
pub async fn admin_provider_toggle(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(id): Path<i64>,
    headers: axum::http::HeaderMap,
    Form(form): Form<CsrfForm>,
) -> Result<Response, StatusCode> {
    require_admin(&auth_user)?;
    csrf::validate_csrf(&headers, &form.csrf_token).map_err(|_| StatusCode::FORBIDDEN)?;

    let pg = &state.pg_store;
    match pg.get_provider(id).await {
        Ok(Some(p)) => {
            if p.enabled {
                let _ = pg.disable_provider(id).await;
            } else {
                let _ = pg.enable_provider(id).await;
            }
            audit_and_notify(
                pg,
                &auth_user,
                AuditAction::ProviderToggled,
                "provider",
                Some(id),
                &serde_json::json!({"enabled": !p.enabled}),
            )
            .await;
            render_providers_list(&state, &auth_user, "", "Provider toggled.").await
        }
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

// ── Provider test connection ──────────────────────────────────────────────────

/// `POST /admin/providers/{id}/test` — test a provider's endpoint connectivity.
pub async fn admin_provider_test(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(id): Path<i64>,
    headers: axum::http::HeaderMap,
    Form(form): Form<CsrfForm>,
) -> Result<Response, StatusCode> {
    require_admin(&auth_user)?;
    csrf::validate_csrf(&headers, &form.csrf_token).map_err(|_| StatusCode::FORBIDDEN)?;

    let pg = &state.pg_store;
    match pg.test_connection(id).await {
        Ok(()) => {
            audit_and_notify(
                pg,
                &auth_user,
                AuditAction::ProviderTested,
                "provider",
                Some(id),
                &serde_json::json!({"result": "ok"}),
            )
            .await;
            render_providers_list(
                &state,
                &auth_user,
                "",
                &format!("Connection to provider {id} successful ✓"),
            )
            .await
        }
        Err(e) => {
            render_providers_list(
                &state,
                &auth_user,
                &format!("Connection test failed for provider {id}: {e}"),
                "",
            )
            .await
        }
    }
}

// ── Model list page ───────────────────────────────────────────────────────────

/// `GET /admin/models` — list all models with toggle/edit actions.
pub async fn admin_models_page(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Response, StatusCode> {
    require_admin(&auth_user)?;
    render_models_list(&state, &auth_user, "", "").await
}

// ── Model create form ─────────────────────────────────────────────────────────

/// `GET /admin/models/new` — show the model creation form.
pub async fn admin_model_new_form(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Response, StatusCode> {
    require_admin(&auth_user)?;
    let is_super = matches!(auth_user.role, kb_core::user::UserRole::Owner);
    let prov_opts = provider_options(&state.pg_store).await;
    let cap_names = vec![
        "text".to_string(),
        "vision".to_string(),
        "code".to_string(),
        "embed".to_string(),
        "rerank".to_string(),
    ];
    let (csrf_token, cookie) = fresh_csrf(&state);

    let page = AdminModelFormPage {
        csrf_token,
        error: String::new(),
        is_super_admin: is_super,
        model_id_db: None,
        provider_id_val: prov_opts.first().map(|(id, _)| *id).unwrap_or(0),
        model_id_str: String::new(),
        caps_text: false,
        caps_vision: false,
        caps_code: false,
        caps_embed: false,
        caps_rerank: false,
        ctx_tokens: String::new(),
        max_conc: String::new(),
        rpm: String::new(),
        tpm: String::new(),
        price_in: String::new(),
        price_out: String::new(),
        data_class: "local".into(),
        enabled: true,
        provider_options: prov_opts,
        cap_names,
    };

    let mut resp = render_ok(&page);
    resp.headers_mut()
        .insert(axum::http::header::SET_COOKIE, cookie);
    Ok(resp)
}

// ── Model create handler ──────────────────────────────────────────────────────

/// `POST /admin/models` — create a new model.
pub async fn admin_model_create(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    headers: axum::http::HeaderMap,
    Form(form): Form<ModelForm>,
) -> Result<Response, StatusCode> {
    require_admin(&auth_user)?;
    csrf::validate_csrf(&headers, &form.csrf_token).map_err(|_| StatusCode::FORBIDDEN)?;

    let pg = &state.pg_store;
    let caps = caps_from_form(&form);
    let data_class = DataClass::from_str(&form.data_class).unwrap_or(DataClass::Local);

    let ctx_tokens = parse_optional_i32(&form.ctx_tokens);
    let max_conc = parse_optional_i32(&form.max_conc);
    let rpm = parse_optional_i32(&form.rpm);
    let tpm = parse_optional_i32(&form.tpm);
    let price_in = parse_optional_f64(&form.price_in);
    let price_out = parse_optional_f64(&form.price_out);

    match pg
        .create_model(
            form.provider_id,
            &form.model_id,
            &caps,
            ctx_tokens,
            max_conc,
            rpm,
            tpm,
            price_in,
            price_out,
            data_class,
        )
        .await
    {
        Ok(mid) => {
            audit_and_notify(
                pg,
                &auth_user,
                AuditAction::ModelCreated,
                "model",
                Some(mid),
                &serde_json::json!({"model_id": form.model_id, "provider_id": form.provider_id}),
            )
            .await;
        }
        Err(e) => {
            return render_models_list(
                &state,
                &auth_user,
                &format!("Failed to create model: {e}"),
                "",
            )
            .await;
        }
    }

    Ok(Redirect::to("/admin/models").into_response())
}

// ── Model edit form ───────────────────────────────────────────────────────────

/// `GET /admin/models/{id}/edit` — show the model edit form.
pub async fn admin_model_edit_form(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(id): Path<i64>,
) -> Result<Response, StatusCode> {
    require_admin(&auth_user)?;
    let pg = &state.pg_store;
    let model = match pg.get_model(id).await {
        Ok(Some(m)) => m,
        Ok(None) => return Err(StatusCode::NOT_FOUND),
        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };

    let prov_opts = provider_options(pg).await;
    let cap_names = vec![
        "text".to_string(),
        "vision".to_string(),
        "code".to_string(),
        "embed".to_string(),
        "rerank".to_string(),
    ];
    let is_super = matches!(auth_user.role, kb_core::user::UserRole::Owner);
    let (csrf_token, cookie) = fresh_csrf(&state);

    let page = AdminModelFormPage {
        csrf_token,
        error: String::new(),
        is_super_admin: is_super,
        model_id_db: Some(id),
        provider_id_val: model.provider_id,
        model_id_str: model.model_id,
        caps_text: has_cap(&model.caps, "text"),
        caps_vision: has_cap(&model.caps, "vision"),
        caps_code: has_cap(&model.caps, "code"),
        caps_embed: has_cap(&model.caps, "embed"),
        caps_rerank: has_cap(&model.caps, "rerank"),
        ctx_tokens: opt_display(model.ctx_tokens),
        max_conc: opt_display(model.max_conc),
        rpm: opt_display(model.rpm),
        tpm: opt_display(model.tpm),
        price_in: opt_f64_str(model.price_in),
        price_out: opt_f64_str(model.price_out),
        data_class: model.data_class.as_str().to_owned(),
        enabled: model.enabled,
        provider_options: prov_opts,
        cap_names,
    };

    let mut resp = render_ok(&page);
    resp.headers_mut()
        .insert(axum::http::header::SET_COOKIE, cookie);
    Ok(resp)
}

// ── Model update handler ──────────────────────────────────────────────────────

/// `POST /admin/models/{id}` — update a model.
pub async fn admin_model_update(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(id): Path<i64>,
    headers: axum::http::HeaderMap,
    Form(form): Form<ModelForm>,
) -> Result<Response, StatusCode> {
    require_admin(&auth_user)?;
    csrf::validate_csrf(&headers, &form.csrf_token).map_err(|_| StatusCode::FORBIDDEN)?;

    let pg = &state.pg_store;
    let caps = caps_from_form(&form);
    let data_class = DataClass::from_str(&form.data_class).ok();

    let ctx_tokens: Option<Option<i32>> = Some(parse_optional_i32(&form.ctx_tokens));
    let max_conc: Option<Option<i32>> = Some(parse_optional_i32(&form.max_conc));
    let rpm: Option<Option<i32>> = Some(parse_optional_i32(&form.rpm));
    let tpm: Option<Option<i32>> = Some(parse_optional_i32(&form.tpm));
    let price_in: Option<Option<f64>> = Some(parse_optional_f64(&form.price_in));
    let price_out: Option<Option<f64>> = Some(parse_optional_f64(&form.price_out));

    match pg
        .update_model(
            id,
            Some(form.provider_id),
            Some(&form.model_id),
            Some(&caps),
            ctx_tokens,
            max_conc,
            rpm,
            tpm,
            price_in,
            price_out,
            data_class,
        )
        .await
    {
        Ok(()) => {
            audit_and_notify(
                pg,
                &auth_user,
                AuditAction::ModelUpdated,
                "model",
                Some(id),
                &serde_json::json!({"model_id": form.model_id}),
            )
            .await;
            render_models_list(&state, &auth_user, "", "Model updated.").await
        }
        Err(e) => {
            render_models_list(
                &state,
                &auth_user,
                &format!("Failed to update model: {e}"),
                "",
            )
            .await
        }
    }
}

// ── Model toggle ──────────────────────────────────────────────────────────────

/// `POST /admin/models/{id}/toggle` — enable or disable a model.
pub async fn admin_model_toggle(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(id): Path<i64>,
    headers: axum::http::HeaderMap,
    Form(form): Form<CsrfForm>,
) -> Result<Response, StatusCode> {
    require_admin(&auth_user)?;
    csrf::validate_csrf(&headers, &form.csrf_token).map_err(|_| StatusCode::FORBIDDEN)?;

    let pg = &state.pg_store;
    match pg.get_model(id).await {
        Ok(Some(m)) => {
            if m.enabled {
                let _ = pg.disable_model(id).await;
            } else {
                let _ = pg.enable_model(id).await;
            }
            audit_and_notify(
                pg,
                &auth_user,
                AuditAction::ModelToggled,
                "model",
                Some(id),
                &serde_json::json!({"enabled": !m.enabled}),
            )
            .await;
            render_models_list(&state, &auth_user, "", "Model toggled.").await
        }
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

// ── Route list page ───────────────────────────────────────────────────────────

/// `GET /admin/routes` — list all routes with edit/delete actions.
pub async fn admin_routes_page(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Response, StatusCode> {
    require_admin(&auth_user)?;
    render_routes_list(&state, &auth_user, "", "").await
}

// ── Route create form ─────────────────────────────────────────────────────────

/// `GET /admin/routes/new` — show the route creation form.
pub async fn admin_route_new_form(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Response, StatusCode> {
    require_admin(&auth_user)?;
    let is_super = matches!(auth_user.role, kb_core::user::UserRole::Owner);
    let model_opts = model_options(&state.pg_store).await;
    let role_options = Role::all().iter().map(|r| r.as_str().to_owned()).collect();
    let strategy_options = RoutingStrategy::all()
        .iter()
        .map(|s| s.as_str().to_owned())
        .collect();
    let (csrf_token, cookie) = fresh_csrf(&state);

    let page = AdminRouteFormPage {
        csrf_token,
        error: String::new(),
        is_super_admin: is_super,
        route_id: None,
        tenant_id_str: String::new(),
        role: String::new(),
        tier: 0,
        strategy: "least_loaded".into(),
        spill_ms: 800,
        model_id: model_opts.first().map(|(id, _)| *id).unwrap_or(0),
        role_options,
        strategy_options,
        model_options: model_opts,
    };

    let mut resp = render_ok(&page);
    resp.headers_mut()
        .insert(axum::http::header::SET_COOKIE, cookie);
    Ok(resp)
}

// ── Route create handler ──────────────────────────────────────────────────────

/// `POST /admin/routes` — create a new route.
pub async fn admin_route_create(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    headers: axum::http::HeaderMap,
    Form(form): Form<RouteForm>,
) -> Result<Response, StatusCode> {
    require_admin(&auth_user)?;
    csrf::validate_csrf(&headers, &form.csrf_token).map_err(|_| StatusCode::FORBIDDEN)?;

    let pg = &state.pg_store;
    let tenant_id: Option<i64> = if form.tenant_id.trim().is_empty() {
        None
    } else {
        match form.tenant_id.trim().parse() {
            Ok(id) => Some(id),
            Err(_) => {
                return render_routes_list(
                    &state,
                    &auth_user,
                    "Invalid tenant ID (must be a number or empty).",
                    "",
                )
                .await;
            }
        }
    };

    let role = match Role::from_str(&form.role) {
        Ok(r) => r,
        Err(_) => {
            return render_routes_list(&state, &auth_user, "Invalid role.", "").await;
        }
    };

    let tier: i32 = form.tier_str.trim().parse().unwrap_or(0);
    let strategy =
        RoutingStrategy::from_str(&form.strategy).unwrap_or(RoutingStrategy::LeastLoaded);
    let spill_ms: i32 = form.spill_ms_str.trim().parse().unwrap_or(800);
    let model_id: i64 = match form.model_id_str.trim().parse() {
        Ok(id) => id,
        Err(_) => {
            return render_routes_list(&state, &auth_user, "Invalid model ID.", "").await;
        }
    };

    match pg
        .create_route(tenant_id, role, tier, strategy, spill_ms, model_id)
        .await
    {
        Ok(rid) => {
            audit_and_notify(
                pg,
                &auth_user,
                AuditAction::RouteCreated,
                "route",
                Some(rid),
                &serde_json::json!({"role": role.as_str(), "tier": tier, "model_id": model_id}),
            )
            .await;
        }
        Err(e) => {
            return render_routes_list(
                &state,
                &auth_user,
                &format!("Failed to create route: {e}"),
                "",
            )
            .await;
        }
    }

    Ok(Redirect::to("/admin/routes").into_response())
}

// ── Route edit form ───────────────────────────────────────────────────────────

/// `GET /admin/routes/{id}/edit` — show the route edit form.
pub async fn admin_route_edit_form(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(id): Path<i64>,
) -> Result<Response, StatusCode> {
    require_admin(&auth_user)?;
    let pg = &state.pg_store;
    let route = match pg.get_route(id).await {
        Ok(Some(r)) => r,
        Ok(None) => return Err(StatusCode::NOT_FOUND),
        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };

    let model_opts = model_options(pg).await;
    let role_options = Role::all().iter().map(|r| r.as_str().to_owned()).collect();
    let strategy_options = RoutingStrategy::all()
        .iter()
        .map(|s| s.as_str().to_owned())
        .collect();
    let is_super = matches!(auth_user.role, kb_core::user::UserRole::Owner);
    let (csrf_token, cookie) = fresh_csrf(&state);

    let tenant_str = route.tenant_id.map(|t| t.to_string()).unwrap_or_default();

    let page = AdminRouteFormPage {
        csrf_token,
        error: String::new(),
        is_super_admin: is_super,
        route_id: Some(id),
        tenant_id_str: tenant_str,
        role: route.role.as_str().to_owned(),
        tier: route.tier,
        strategy: route.strategy.as_str().to_owned(),
        spill_ms: route.spill_ms,
        model_id: route.model_id,
        role_options,
        strategy_options,
        model_options: model_opts,
    };

    let mut resp = render_ok(&page);
    resp.headers_mut()
        .insert(axum::http::header::SET_COOKIE, cookie);
    Ok(resp)
}

// ── Route update handler ──────────────────────────────────────────────────────

/// `POST /admin/routes/{id}` — update a route.
pub async fn admin_route_update(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(id): Path<i64>,
    headers: axum::http::HeaderMap,
    Form(form): Form<RouteForm>,
) -> Result<Response, StatusCode> {
    require_admin(&auth_user)?;
    csrf::validate_csrf(&headers, &form.csrf_token).map_err(|_| StatusCode::FORBIDDEN)?;

    let pg = &state.pg_store;
    let tier: Option<i32> = parse_optional_i32(&form.tier_str);
    let strategy = RoutingStrategy::from_str(&form.strategy).ok();
    let spill_ms: Option<i32> = parse_optional_i32(&form.spill_ms_str);
    let model_id: Option<i64> = parse_optional_i32(&form.model_id_str).map(i64::from);

    match pg
        .update_route(id, tier, strategy, spill_ms, model_id)
        .await
    {
        Ok(()) => {
            audit_and_notify(
                pg,
                &auth_user,
                AuditAction::RouteUpdated,
                "route",
                Some(id),
                &serde_json::json!({"tier": tier}),
            )
            .await;
            render_routes_list(&state, &auth_user, "", "Route updated.").await
        }
        Err(e) => {
            render_routes_list(
                &state,
                &auth_user,
                &format!("Failed to update route: {e}"),
                "",
            )
            .await
        }
    }
}

// ── Route delete handler ──────────────────────────────────────────────────────

/// `POST /admin/routes/{id}/delete` — hard-delete a route.
pub async fn admin_route_delete(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(id): Path<i64>,
    headers: axum::http::HeaderMap,
    Form(form): Form<CsrfForm>,
) -> Result<Response, StatusCode> {
    require_admin(&auth_user)?;
    csrf::validate_csrf(&headers, &form.csrf_token).map_err(|_| StatusCode::FORBIDDEN)?;

    let pg = &state.pg_store;
    match pg.delete_route(id).await {
        Ok(affected) => {
            if affected > 0 {
                audit_and_notify(
                    pg,
                    &auth_user,
                    AuditAction::RouteDeleted,
                    "route",
                    Some(id),
                    &serde_json::json!({}),
                )
                .await;
                render_routes_list(&state, &auth_user, "", "Route deleted.").await
            } else {
                Err(StatusCode::NOT_FOUND)
            }
        }
        Err(e) => {
            render_routes_list(
                &state,
                &auth_user,
                &format!("Failed to delete route: {e}"),
                "",
            )
            .await
        }
    }
}

// ── Internal render helpers ───────────────────────────────────────────────────

/// Fetch providers from the DB and render the list page.
async fn render_providers_list(
    state: &AppState,
    auth_user: &AuthUser,
    error: &str,
    success: &str,
) -> Result<Response, StatusCode> {
    let pg = &state.pg_store;
    let is_super = matches!(auth_user.role, kb_core::user::UserRole::Owner);

    let models = pg.list_models().await.unwrap_or_default();
    let model_counts: std::collections::HashMap<i64, usize> =
        models
            .into_iter()
            .fold(std::collections::HashMap::new(), |mut acc, m| {
                *acc.entry(m.provider_id).or_default() += 1;
                acc
            });

    let providers = pg
        .list_providers()
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|p| {
            let endpoint = p.endpoint.clone().unwrap_or_default();
            let mc = model_counts.get(&p.id).copied().unwrap_or(0);
            AdminProviderRow {
                id: p.id,
                name: p.name,
                kind: p.kind.as_str().to_owned(),
                endpoint,
                enabled: p.enabled,
                model_count: mc,
            }
        })
        .collect::<Vec<_>>();

    let (csrf_token, cookie) = fresh_csrf(state);
    let page = AdminProvidersPage {
        csrf_token,
        error: error.to_owned(),
        success: success.to_owned(),
        is_super_admin: is_super,
        providers,
    };

    let mut resp = render_ok(&page);
    resp.headers_mut()
        .insert(axum::http::header::SET_COOKIE, cookie);
    Ok(resp)
}

/// Fetch models from the DB and render the list page.
async fn render_models_list(
    state: &AppState,
    auth_user: &AuthUser,
    error: &str,
    success: &str,
) -> Result<Response, StatusCode> {
    let pg = &state.pg_store;
    let is_super = matches!(auth_user.role, kb_core::user::UserRole::Owner);

    let providers = pg.list_providers().await.unwrap_or_default();
    let prov_names: std::collections::HashMap<i64, String> =
        providers.into_iter().map(|p| (p.id, p.name)).collect();

    let models = pg
        .list_models()
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|m| AdminModelRow {
            id: m.id,
            provider_name: prov_names
                .get(&m.provider_id)
                .cloned()
                .unwrap_or_else(|| "—".into()),
            model_id: m.model_id,
            caps: m.caps.join(", "),
            ctx_tokens: opt_display(m.ctx_tokens),
            max_conc: opt_display(m.max_conc),
            pricing: {
                let p_in = m
                    .price_in
                    .map(|v| format!("${v:.2}/Mt in"))
                    .unwrap_or_default();
                let p_out = m
                    .price_out
                    .map(|v| format!("${v:.2}/Mt out"))
                    .unwrap_or_default();
                if p_in.is_empty() && p_out.is_empty() {
                    "—".into()
                } else {
                    format!("{p_in} {p_out}").trim().to_string()
                }
            },
            data_class: m.data_class.as_str().to_owned(),
            enabled: m.enabled,
        })
        .collect();

    let (csrf_token, cookie) = fresh_csrf(state);
    let page = AdminModelsPage {
        csrf_token,
        error: error.to_owned(),
        success: success.to_owned(),
        is_super_admin: is_super,
        models,
    };

    let mut resp = render_ok(&page);
    resp.headers_mut()
        .insert(axum::http::header::SET_COOKIE, cookie);
    Ok(resp)
}

/// Fetch routes from the DB and render the list page.
async fn render_routes_list(
    state: &AppState,
    auth_user: &AuthUser,
    error: &str,
    success: &str,
) -> Result<Response, StatusCode> {
    let pg = &state.pg_store;
    let is_super = matches!(auth_user.role, kb_core::user::UserRole::Owner);

    let providers = pg.list_providers().await.unwrap_or_default();
    let prov_names: std::collections::HashMap<i64, String> =
        providers.into_iter().map(|p| (p.id, p.name)).collect();

    let models = pg.list_models().await.unwrap_or_default();
    let model_info: std::collections::HashMap<i64, (i64, String)> = models
        .into_iter()
        .map(|m| (m.id, (m.provider_id, m.model_id)))
        .collect();

    let routes = pg
        .list_routes()
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|r| {
            let tenant_display = r
                .tenant_id
                .map(|t| format!("Tenant {t}"))
                .unwrap_or_else(|| "Global".into());
            let model_display = model_info
                .get(&r.model_id)
                .map(|(pid, mid)| {
                    let pname = prov_names.get(pid).map(|n| n.as_str()).unwrap_or("?");
                    format!("{pname} / {mid}")
                })
                .unwrap_or_else(|| format!("Model #{}", r.model_id));
            AdminRouteRow {
                id: r.id,
                tenant_display,
                role: r.role.as_str().to_owned(),
                tier: r.tier,
                strategy: r.strategy.as_str().to_owned(),
                spill_ms: r.spill_ms,
                model_display,
            }
        })
        .collect();

    let (csrf_token, cookie) = fresh_csrf(state);
    let page = AdminRoutesPage {
        csrf_token,
        error: error.to_owned(),
        success: success.to_owned(),
        is_super_admin: is_super,
        routes,
    };

    let mut resp = render_ok(&page);
    resp.headers_mut()
        .insert(axum::http::header::SET_COOKIE, cookie);
    Ok(resp)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    // ── parse_optional_i32 ─────────────────────────────────────────────────

    #[test]
    fn parse_optional_i32_handles_empty() {
        assert_eq!(parse_optional_i32(""), None);
        assert_eq!(parse_optional_i32("  "), None);
    }

    #[test]
    fn parse_optional_i32_handles_valid() {
        assert_eq!(parse_optional_i32("42"), Some(42));
        assert_eq!(parse_optional_i32(" 123 "), Some(123));
        assert_eq!(parse_optional_i32("-5"), Some(-5));
    }

    #[test]
    fn parse_optional_i32_handles_invalid() {
        assert_eq!(parse_optional_i32("abc"), None);
        assert_eq!(parse_optional_i32("12.5"), None);
    }

    // ── parse_optional_f64 ─────────────────────────────────────────────────

    #[test]
    fn parse_optional_f64_handles_empty() {
        assert_eq!(parse_optional_f64(""), None);
        assert_eq!(parse_optional_f64("  "), None);
    }

    #[test]
    fn parse_optional_f64_handles_valid() {
        let v = parse_optional_f64("3.5").unwrap();
        assert!((v - 3.5).abs() < 0.01);
        assert_eq!(parse_optional_f64("42"), Some(42.0));
    }

    #[test]
    fn parse_optional_f64_handles_invalid() {
        assert_eq!(parse_optional_f64("abc"), None);
    }

    // ── caps_from_form ─────────────────────────────────────────────────────

    #[test]
    fn caps_from_form_all_off() {
        let form = ModelForm {
            csrf_token: "x".into(),
            provider_id: 1,
            model_id: "m".into(),
            ctx_tokens: String::new(),
            max_conc: String::new(),
            rpm: String::new(),
            tpm: String::new(),
            price_in: String::new(),
            price_out: String::new(),
            data_class: "local".into(),
            cap_text: String::new(),
            cap_vision: String::new(),
            cap_code: String::new(),
            cap_embed: String::new(),
            cap_rerank: String::new(),
        };
        assert!(caps_from_form(&form).is_empty());
    }

    #[test]
    fn caps_from_form_text_and_embed_on() {
        let form = ModelForm {
            csrf_token: "x".into(),
            provider_id: 1,
            model_id: "m".into(),
            ctx_tokens: String::new(),
            max_conc: String::new(),
            rpm: String::new(),
            tpm: String::new(),
            price_in: String::new(),
            price_out: String::new(),
            data_class: "local".into(),
            cap_text: "on".into(),
            cap_vision: String::new(),
            cap_code: String::new(),
            cap_embed: "on".into(),
            cap_rerank: String::new(),
        };
        let caps = caps_from_form(&form);
        assert_eq!(caps.len(), 2);
        assert!(caps.contains(&"text".to_string()));
        assert!(caps.contains(&"embed".to_string()));
    }

    #[test]
    fn caps_from_form_all_on() {
        let form = ModelForm {
            csrf_token: "x".into(),
            provider_id: 1,
            model_id: "m".into(),
            ctx_tokens: String::new(),
            max_conc: String::new(),
            rpm: String::new(),
            tpm: String::new(),
            price_in: String::new(),
            price_out: String::new(),
            data_class: "local".into(),
            cap_text: "on".into(),
            cap_vision: "on".into(),
            cap_code: "on".into(),
            cap_embed: "on".into(),
            cap_rerank: "on".into(),
        };
        assert_eq!(caps_from_form(&form).len(), 5);
    }

    // ── has_cap ────────────────────────────────────────────────────────────

    #[test]
    fn has_cap_matches() {
        let caps = vec!["text".to_string(), "vision".to_string()];
        assert!(has_cap(&caps, "text"));
        assert!(has_cap(&caps, "vision"));
        assert!(!has_cap(&caps, "code"));
    }

    #[test]
    fn has_cap_empty_list() {
        assert!(!has_cap(&[], "text"));
    }

    // ── price_display ──────────────────────────────────────────────────────

    #[test]
    fn price_display_formats() {
        assert_eq!(price_display(None), "—");
        assert_eq!(price_display(Some(0.0)), "$0.00/Mt");
        assert_eq!(price_display(Some(1.5)), "$1.50/Mt");
    }

    // ── opt_display ────────────────────────────────────────────────────────

    #[test]
    fn opt_display_formats() {
        assert_eq!(opt_display(None), "—");
        assert_eq!(opt_display(Some(32768)), "32768");
    }

    // ── opt_f64_str ────────────────────────────────────────────────────────

    #[test]
    fn opt_f64_str_formats() {
        assert_eq!(opt_f64_str(None), "");
        assert_eq!(opt_f64_str(Some(1.5)), "1.5");
    }

    // ── Template rendering smoke tests ────────────────────────────────────

    #[test]
    fn admin_providers_page_renders() {
        let page = AdminProvidersPage {
            csrf_token: "csrf".into(),
            error: String::new(),
            success: String::new(),
            is_super_admin: false,
            providers: vec![AdminProviderRow {
                id: 1,
                name: "llama-gpu".into(),
                kind: "local".into(),
                endpoint: "http://localhost:8080/v1".into(),
                enabled: true,
                model_count: 2,
            }],
        };
        let html = page.render().unwrap();
        assert!(html.contains("llama-gpu"));
        assert!(html.contains("http://localhost:8080/v1"));
    }

    #[test]
    fn admin_providers_page_renders_empty() {
        let page = AdminProvidersPage {
            csrf_token: "csrf".into(),
            error: String::new(),
            success: String::new(),
            is_super_admin: true,
            providers: vec![],
        };
        let html = page.render().unwrap();
        assert!(html.contains("No providers configured"));
    }

    #[test]
    fn admin_providers_page_shows_error() {
        let page = AdminProvidersPage {
            csrf_token: "csrf".into(),
            error: "Something went wrong.".into(),
            success: String::new(),
            is_super_admin: false,
            providers: vec![],
        };
        let html = page.render().unwrap();
        assert!(html.contains("Something went wrong"));
    }

    #[test]
    fn admin_providers_page_shows_success() {
        let page = AdminProvidersPage {
            csrf_token: "csrf".into(),
            error: String::new(),
            success: "Connection to provider 1 successful ✓".into(),
            is_super_admin: false,
            providers: vec![],
        };
        let html = page.render().unwrap();
        assert!(html.contains("Connection to provider 1 successful"));
    }

    #[test]
    fn admin_provider_form_page_create_mode() {
        let page = AdminProviderFormPage {
            csrf_token: "csrf".into(),
            error: String::new(),
            is_super_admin: false,
            provider_id: None,
            name: String::new(),
            kind: String::new(),
            endpoint: String::new(),
            headers_json: "{}".into(),
            enabled: true,
            api_key_placeholder: String::new(),
            kind_options: vec!["local".into(), "openai_compat".into()],
        };
        let html = page.render().unwrap();
        assert!(html.contains("New Provider"));
        assert!(!html.contains("Save Changes"));
    }

    #[test]
    fn admin_provider_form_page_edit_mode() {
        let page = AdminProviderFormPage {
            csrf_token: "csrf".into(),
            error: String::new(),
            is_super_admin: true,
            provider_id: Some(5),
            name: "my-prov".into(),
            kind: "local".into(),
            endpoint: "http://localhost:8080/v1".into(),
            headers_json: "{}".into(),
            enabled: true,
            api_key_placeholder: "•••••••• (leave blank to keep current)".into(),
            kind_options: vec!["local".into(), "openai_compat".into()],
        };
        let html = page.render().unwrap();
        assert!(html.contains("Edit Provider"));
        assert!(html.contains("Save Changes"));
        assert!(html.contains("my-prov"));
    }

    #[test]
    fn admin_models_page_renders() {
        let page = AdminModelsPage {
            csrf_token: "csrf".into(),
            error: String::new(),
            success: String::new(),
            is_super_admin: false,
            models: vec![AdminModelRow {
                id: 1,
                provider_name: "llama-gpu".into(),
                model_id: "Qwen3-VL".into(),
                caps: "text, vision".into(),
                ctx_tokens: "32768".into(),
                max_conc: "4".into(),
                pricing: "$1.50/Mt in $3.00/Mt out".into(),
                data_class: "local".into(),
                enabled: true,
            }],
        };
        let html = page.render().unwrap();
        assert!(html.contains("llama-gpu"));
        assert!(html.contains("Qwen3-VL"));
    }

    #[test]
    fn admin_models_page_renders_empty() {
        let page = AdminModelsPage {
            csrf_token: "csrf".into(),
            error: String::new(),
            success: String::new(),
            is_super_admin: true,
            models: vec![],
        };
        let html = page.render().unwrap();
        assert!(html.contains("No models configured"));
    }

    #[test]
    fn admin_model_form_page_renders() {
        let page = AdminModelFormPage {
            csrf_token: "csrf".into(),
            error: String::new(),
            is_super_admin: false,
            model_id_db: None,
            provider_id_val: 1,
            model_id_str: String::new(),
            caps_text: true,
            caps_vision: false,
            caps_code: false,
            caps_embed: false,
            caps_rerank: false,
            ctx_tokens: String::new(),
            max_conc: String::new(),
            rpm: String::new(),
            tpm: String::new(),
            price_in: String::new(),
            price_out: String::new(),
            data_class: "local".into(),
            enabled: true,
            provider_options: vec![(1, "llama-gpu".into())],
            cap_names: vec!["text".into(), "vision".into()],
        };
        let html = page.render().unwrap();
        assert!(html.contains("New Model"));
        assert!(html.contains("llama-gpu"));
    }

    #[test]
    fn admin_routes_page_renders() {
        let page = AdminRoutesPage {
            csrf_token: "csrf".into(),
            error: String::new(),
            success: String::new(),
            is_super_admin: false,
            routes: vec![AdminRouteRow {
                id: 1,
                tenant_display: "Global".into(),
                role: "text".into(),
                tier: 0,
                strategy: "least_loaded".into(),
                spill_ms: 800,
                model_display: "llama-gpu / Qwen3-VL".into(),
            }],
        };
        let html = page.render().unwrap();
        assert!(html.contains("Global"));
        assert!(html.contains("least_loaded"));
        assert!(html.contains("Qwen3-VL"));
    }

    #[test]
    fn admin_routes_page_renders_empty() {
        let page = AdminRoutesPage {
            csrf_token: "csrf".into(),
            error: String::new(),
            success: String::new(),
            is_super_admin: true,
            routes: vec![],
        };
        let html = page.render().unwrap();
        assert!(html.contains("No routes configured"));
    }

    #[test]
    fn admin_route_form_page_create_mode() {
        let page = AdminRouteFormPage {
            csrf_token: "csrf".into(),
            error: String::new(),
            is_super_admin: false,
            route_id: None,
            tenant_id_str: String::new(),
            role: "text".into(),
            tier: 0,
            strategy: "least_loaded".into(),
            spill_ms: 800,
            model_id: 1,
            role_options: vec!["text".into(), "embed".into()],
            strategy_options: vec!["least_loaded".into(), "round_robin".into()],
            model_options: vec![(1, "llama-gpu / Qwen3-VL".into())],
        };
        let html = page.render().unwrap();
        assert!(html.contains("New Route"));
        assert!(html.contains("least_loaded"));
    }

    #[test]
    fn admin_route_form_page_edit_mode() {
        let page = AdminRouteFormPage {
            csrf_token: "csrf".into(),
            error: String::new(),
            is_super_admin: true,
            route_id: Some(3),
            tenant_id_str: "7".into(),
            role: "text".into(),
            tier: 1,
            strategy: "cost_asc".into(),
            spill_ms: 1500,
            model_id: 2,
            role_options: vec!["text".into()],
            strategy_options: vec!["cost_asc".into()],
            model_options: vec![(2, "openai / gpt".into())],
        };
        let html = page.render().unwrap();
        assert!(html.contains("Edit Route"));
        assert!(html.contains("cost_asc"));
        assert!(html.contains("1500"));
    }

    // ── Handler integration tests ────────────────────────────────────────

    use std::sync::Arc;
    use std::time::Duration;

    use askama::Template;
    use axum::body::Body;
    use kb_core::session::SessionStore;
    use kb_core::user::UserRole;
    use kb_store::{InMemorySessionStore, PgStore};
    use tower::ServiceExt;

    fn test_app_state() -> Arc<AppState> {
        let session_store: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
        let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
        Arc::new(AppState::new(
            session_store,
            pg_store,
            Some(Duration::from_secs(3600)),
            false,
        ))
    }

    #[tokio::test]
    async fn admin_providers_redirects_member() {
        let state = test_app_state();
        let token = state
            .session_store
            .create(1, 50, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = axum::Router::new()
            .route("/admin/providers", axum::routing::get(admin_providers_page))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                crate::middleware::auth_middleware,
            ))
            .with_state(state);
        let request = axum::http::Request::builder()
            .uri("/admin/providers")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn admin_models_redirects_member() {
        let state = test_app_state();
        let token = state
            .session_store
            .create(1, 50, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = axum::Router::new()
            .route("/admin/models", axum::routing::get(admin_models_page))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                crate::middleware::auth_middleware,
            ))
            .with_state(state);
        let request = axum::http::Request::builder()
            .uri("/admin/models")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn admin_routes_redirects_member() {
        let state = test_app_state();
        let token = state
            .session_store
            .create(1, 50, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = axum::Router::new()
            .route("/admin/routes", axum::routing::get(admin_routes_page))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                crate::middleware::auth_middleware,
            ))
            .with_state(state);
        let request = axum::http::Request::builder()
            .uri("/admin/routes")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn admin_providers_page_renders_despite_db_error() {
        let state = test_app_state();
        let token = state
            .session_store
            .create(1, 50, UserRole::Admin, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = axum::Router::new()
            .route("/admin/providers", axum::routing::get(admin_providers_page))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                crate::middleware::auth_middleware,
            ))
            .with_state(state);
        let request = axum::http::Request::builder()
            .uri("/admin/providers")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        // DB is disconnected → providers list is empty but page renders OK.
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn admin_models_page_renders_despite_db_error() {
        let state = test_app_state();
        let token = state
            .session_store
            .create(1, 50, UserRole::Admin, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = axum::Router::new()
            .route("/admin/models", axum::routing::get(admin_models_page))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                crate::middleware::auth_middleware,
            ))
            .with_state(state);
        let request = axum::http::Request::builder()
            .uri("/admin/models")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn admin_routes_page_renders_despite_db_error() {
        let state = test_app_state();
        let token = state
            .session_store
            .create(1, 50, UserRole::Admin, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = axum::Router::new()
            .route("/admin/routes", axum::routing::get(admin_routes_page))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                crate::middleware::auth_middleware,
            ))
            .with_state(state);
        let request = axum::http::Request::builder()
            .uri("/admin/routes")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn admin_unauthenticated_returns_401() {
        let state = test_app_state();
        let router = axum::Router::new()
            .route("/admin/providers", axum::routing::get(admin_providers_page))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                crate::middleware::auth_middleware,
            ))
            .with_state(state);
        let request = axum::http::Request::builder()
            .uri("/admin/providers")
            .body(Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    // ── Audit action strings ──────────────────────────────────────────────

    #[test]
    fn audit_action_wire_strings_are_locked_for_routing() {
        assert_eq!(AuditAction::ProviderCreated.as_str(), "provider_created");
        assert_eq!(AuditAction::ProviderUpdated.as_str(), "provider_updated");
        assert_eq!(AuditAction::ProviderToggled.as_str(), "provider_toggled");
        assert_eq!(AuditAction::ModelCreated.as_str(), "model_created");
        assert_eq!(AuditAction::ModelUpdated.as_str(), "model_updated");
        assert_eq!(AuditAction::ModelToggled.as_str(), "model_toggled");
        assert_eq!(AuditAction::RouteCreated.as_str(), "route_created");
        assert_eq!(AuditAction::RouteUpdated.as_str(), "route_updated");
        assert_eq!(AuditAction::RouteDeleted.as_str(), "route_deleted");
        assert_eq!(AuditAction::ProviderTested.as_str(), "provider_tested");
    }
}
