//! Askama template types for the Web UI.
//!
//! Each struct corresponds to a template file under `templates/`. Fields use
//! simple types (`String`, `bool`) so Askama's HTML escaping works directly
//! without `Option` display issues. An empty `String` means "not set / not shown."
//!
//! All templates are compiled at build time via askama's `derive(Template)` macro.

use askama::Template;

/// `GET /login` — the login page HTML form.
///
/// When `error` is non-empty, an alert banner is displayed above the form.
/// `tenant_slug` and `email` are pre-filled from a previous failed attempt
/// (empty string = empty field).
#[derive(Debug, Template)]
#[template(path = "login.html")]
pub(crate) struct LoginPage {
    /// CSRF token (always present, generated per page load).
    pub csrf_token: String,
    /// Pre-fill the tenant slug field.
    pub tenant_slug: String,
    /// Pre-fill the email field.
    pub email: String,
    /// Error message to display (empty = no error).
    pub error: String,
}

/// `GET /register` — the registration page HTML form.
///
/// Identical structure to [`LoginPage`], renders into `register.html`.
#[derive(Debug, Template)]
#[template(path = "register.html")]
pub(crate) struct RegisterPage {
    /// CSRF token (always present, generated per page load).
    pub csrf_token: String,
    /// Pre-fill the tenant slug field.
    pub tenant_slug: String,
    /// Pre-fill the email field.
    pub email: String,
    /// Error message to display (empty = no error).
    pub error: String,
}

// ── Search page types (P6-T5) ──────────────────────────────────────────────────

/// `GET /search` — the main search page with the search bar, filters, and results
/// area (initially showing an empty-state illustration).
///
/// The CSRF token is included in the hidden form field so HTMX POSTs pass validation.
/// `kind_filters` lists every known [`DocKind`](kb_core::kind::DocKind) as a checkbox
/// for the filter sidebar.
#[derive(Debug, Template)]
#[template(path = "search.html")]
pub(crate) struct SearchPage {
    /// CSRF token for the HTMX search form.
    pub csrf_token: String,
    /// Pre-filled query text (carried forward on re-render).
    pub query: String,
    /// The kind filter checkboxes with selection state.
    pub kind_filters: Vec<KindFilter>,
    /// Pre-filled tag filter text.
    pub selected_tags: String,
    /// Current search results (empty on initial load).
    pub hits: Vec<SearchResultHit>,
}

/// A single kind filter checkbox entry in the search sidebar.
#[derive(Debug, Clone)]
pub(crate) struct KindFilter {
    /// The DocKind wire string (checked attribute value).
    pub value: String,
    /// Human-readable label for the checkbox.
    pub label: String,
    /// Whether this kind is currently selected (checked).
    pub selected: bool,
}

/// A single search result for template rendering (one row per document).
///
/// Mirrors [`kb_core::query::Hit`] but carries `kind` as an `Option<String>`
/// for rendering the kind badge.
#[derive(Debug, Clone)]
pub(crate) struct SearchResultHit {
    /// Document id (links to `/documents/{id}`).
    pub document_id: i64,
    /// Reranked relevance score, 0.0–1.0.
    pub score: f32,
    /// Document title, if set.
    pub title: Option<String>,
    /// Winning chunk snippet.
    pub snippet: String,
    /// File id for deep-link provenance.
    pub file_id: i64,
    /// Page number, if applicable.
    pub page_no: Option<i32>,
    /// Seconds offset for audio/video chunks.
    pub ts_offset: Option<f64>,
    /// DocKind string for the kind badge.
    pub kind: Option<String>,
}

/// `POST /search` HTMX fragment — rendered inside the `#results` container
/// without a full page reload.
///
/// This template is the partial returned when the search form is submitted
/// via HTMX. It includes either the results list or the empty-state CTA.
#[derive(Debug, Template)]
#[template(path = "search_results.html")]
pub(crate) struct SearchResultsPartial {
    /// Ranked search results.
    pub hits: Vec<SearchResultHit>,
    /// The query that produced these results (for display).
    pub query: String,
}
