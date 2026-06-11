// SPDX-License-Identifier: AGPL-3.0-or-later

//! Askama template types for the provider / model / route admin CRUD pages
//! (plan §26.5, ledger P9-T8).
//!
//! Each struct corresponds to a template file under `templates/`.

use askama::Template;

// ── Provider list ────────────────────────────────────────────────────────────

/// `GET /admin/providers` — list all providers with toggle/test/edit actions.
#[derive(Debug, Template)]
#[template(path = "admin_providers.html")]
#[allow(dead_code)]
pub(crate) struct AdminProvidersPage {
    pub csrf_token: String,
    pub error: String,
    pub success: String,
    pub is_super_admin: bool,
    pub providers: Vec<AdminProviderRow>,
}

/// A single provider row in the list view.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct AdminProviderRow {
    pub id: i64,
    pub name: String,
    pub kind: String,
    pub endpoint: String,
    pub enabled: bool,
    pub model_count: usize,
}

// ── Provider form ────────────────────────────────────────────────────────────

/// `GET /admin/providers/new` and `GET /admin/providers/{id}/edit` — the
/// create/edit provider form.
#[derive(Debug, Template)]
#[template(path = "admin_provider_form.html")]
#[allow(dead_code)]
pub(crate) struct AdminProviderFormPage {
    pub csrf_token: String,
    pub error: String,
    pub is_super_admin: bool,
    /// `None` for create, `Some(id)` for edit.
    pub provider_id: Option<i64>,
    pub name: String,
    pub kind: String,
    pub endpoint: String,
    pub headers_json: String,
    pub enabled: bool,
    pub api_key_placeholder: String,
    /// All provider kinds for the select dropdown.
    pub kind_options: Vec<String>,
}

// ── Model list ───────────────────────────────────────────────────────────────

/// `GET /admin/models` — list all models with toggle/edit actions.
#[derive(Debug, Template)]
#[template(path = "admin_models.html")]
#[allow(dead_code)]
pub(crate) struct AdminModelsPage {
    pub csrf_token: String,
    pub error: String,
    pub success: String,
    pub is_super_admin: bool,
    pub models: Vec<AdminModelRow>,
}

/// A single model row in the list view.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct AdminModelRow {
    pub id: i64,
    pub provider_name: String,
    pub model_id: String,
    pub caps: String,
    pub ctx_tokens: String,
    pub max_conc: String,
    pub pricing: String,
    pub data_class: String,
    pub enabled: bool,
}

// ── Model form ───────────────────────────────────────────────────────────────

/// `GET /admin/models/new` and `GET /admin/models/{id}/edit` — the
/// create/edit model form.
#[derive(Debug, Template)]
#[template(path = "admin_model_form.html")]
#[allow(dead_code)]
pub(crate) struct AdminModelFormPage {
    pub csrf_token: String,
    pub error: String,
    pub is_super_admin: bool,
    /// `None` for create, `Some(id)` for edit.
    pub model_id_db: Option<i64>,
    pub provider_id_val: i64,
    pub model_id_str: String,
    pub caps_text: bool,
    pub caps_vision: bool,
    pub caps_code: bool,
    pub caps_embed: bool,
    pub caps_rerank: bool,
    pub ctx_tokens: String,
    pub max_conc: String,
    pub rpm: String,
    pub tpm: String,
    pub price_in: String,
    pub price_out: String,
    pub data_class: String,
    pub enabled: bool,
    /// Provider options for the dropdown: (id, name).
    pub provider_options: Vec<(i64, String)>,
    /// All capability names for checkbox generation.
    pub cap_names: Vec<String>,
}

// ── Route list ───────────────────────────────────────────────────────────────

/// `GET /admin/routes` — list all routes with edit/delete actions.
#[derive(Debug, Template)]
#[template(path = "admin_routes.html")]
#[allow(dead_code)]
pub(crate) struct AdminRoutesPage {
    pub csrf_token: String,
    pub error: String,
    pub success: String,
    pub is_super_admin: bool,
    pub routes: Vec<AdminRouteRow>,
}

/// A single route row in the list view.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct AdminRouteRow {
    pub id: i64,
    pub tenant_display: String,
    pub role: String,
    pub tier: i32,
    pub strategy: String,
    pub spill_ms: i32,
    pub model_display: String,
}

// ── Route form ───────────────────────────────────────────────────────────────

/// `GET /admin/routes/new` and `GET /admin/routes/{id}/edit` — the
/// create/edit route form.
#[derive(Debug, Template)]
#[template(path = "admin_route_form.html")]
#[allow(dead_code)]
pub(crate) struct AdminRouteFormPage {
    pub csrf_token: String,
    pub error: String,
    pub is_super_admin: bool,
    /// `None` for create, `Some(id)` for edit.
    pub route_id: Option<i64>,
    pub tenant_id_str: String,
    pub role: String,
    pub tier: i32,
    pub strategy: String,
    pub spill_ms: i32,
    pub model_id: i64,
    /// All roles for the select dropdown.
    pub role_options: Vec<String>,
    /// All strategies for the select dropdown.
    pub strategy_options: Vec<String>,
    /// Model options for the dropdown: (id, label).
    pub model_options: Vec<(i64, String)>,
}
