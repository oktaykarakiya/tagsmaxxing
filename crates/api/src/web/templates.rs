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

/// `GET /upload` — the upload page with drag-and-drop zone, file picker,
/// group-as-document toggle, user-note textarea with mic button, and
/// progress bar (P6-T5 search page sibling, P6-T6).
///
/// The page uses JavaScript for file selection/preview/reorder, voice
/// recording via MediaRecorder, and async form submission with progress
/// polling. CSRF token is included in the hidden form field.
#[derive(Debug, Template)]
#[template(path = "upload.html")]
pub(crate) struct UploadPage {
    /// CSRF token (always present, generated per page load).
    pub csrf_token: String,
    /// Error message to display (empty = no error). Reserved for re-render
    /// after a failed form submission; the current upload flow returns JSON
    /// errors instead.
    #[allow(dead_code)]
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

// ── Document detail page types (P6-T7) ────────────────────────────────────────────

/// `GET /documents/:id` — the document detail page showing metadata, tags,
/// page thumbnail strip, and action buttons (add/remove tag, reorder pages,
/// add page, re-tag).
///
/// The CSRF token is included for all mutating form submissions on the page.
/// All tag names from the tenant are provided for the add-tag autocomplete
/// `<datalist>`.
#[derive(Debug, Template)]
#[template(path = "document_detail.html")]
pub(crate) struct DocumentDetailPage {
    /// CSRF token for mutating forms.
    pub csrf_token: String,
    /// Error message to display (empty when none).
    pub error: String,
    /// Document id.
    pub doc_id: i64,
    /// LLM-generated title.
    pub title: String,
    /// LLM-generated summary.
    pub summary: String,
    /// User-provided note at upload time.
    pub user_note: String,
    /// Document kind wire string.
    pub kind: String,
    /// Combined metadata as JSON (rendered in a `<details>` block).
    pub meta_json: String,
    /// Number of member pages.
    pub page_count: i32,
    /// Processing status wire string.
    pub status: String,
    /// Human-readable creation time.
    pub created_at: String,
    /// Canonical tags attached to this document.
    pub tags: Vec<DocumentTagChip>,
    /// All tenant tag names for autocomplete `<datalist>`.
    pub tenant_tag_names: Vec<String>,
    /// Files belonging to this document, ordered by page_no.
    pub files: Vec<DocumentFileEntry>,
}

/// A tag chip rendered on the document detail page (P6-T7).
///
/// Each chip displays the canonical tag name and an "X" button that submits
/// a POST to remove the tag.
#[derive(Debug, Clone)]
pub(crate) struct DocumentTagChip {
    /// Canonical tag id.
    pub tag_id: i64,
    /// Canonical tag name.
    pub tag_name: String,
}

/// A file entry rendered in the page thumbnail strip (P6-T7).
///
/// Displays page number, label, and a link to download the original file.
#[derive(Debug, Clone)]
pub(crate) struct DocumentFileEntry {
    /// File id.
    pub file_id: i64,
    /// Page number (1-based).
    pub page_no: i32,
    /// Page label (e.g. "front", original filename).
    pub label: String,
    /// Emoji icon representing the file type.
    pub icon: String,
    /// File size in a human-readable format (e.g. "2.4 MB").
    pub size_display: String,
}

// ── Auth flow page types (P12-T2) ─────────────────────────────────────────────────

/// `GET /signup` — the self-service signup page creating a new tenant + admin user.
#[derive(Debug, Template)]
#[template(path = "signup.html")]
pub(crate) struct SignupPage {
    /// CSRF token.
    pub csrf_token: String,
    /// Pre-fill the workspace name field.
    pub tenant_name: String,
    /// Pre-fill the email field.
    pub email: String,
    /// Error message to display (empty = no error).
    pub error: String,
}

/// `GET /verify?token=…` — the "check your email" interstitial shown after signup
/// or when a verification email is re-sent.
#[derive(Debug, Template)]
#[template(path = "verify_email_sent.html")]
pub(crate) struct VerifyEmailSentPage {
    /// The email address the verification link was sent to.
    pub email: String,
}

/// `GET /forgot` — the forgot-password form (P12-T2).
#[derive(Debug, Template)]
#[template(path = "forgot_password.html")]
pub(crate) struct ForgotPasswordPage {
    /// CSRF token.
    pub csrf_token: String,
    /// Pre-fill the email field.
    pub email: String,
    /// Error message (empty = no error).
    pub error: String,
    /// Success message (empty = no success notification).
    pub success_message: String,
}

/// `GET /reset?token=…` — the reset-password form (P12-T2).
#[derive(Debug, Template)]
#[template(path = "reset_password.html")]
pub(crate) struct ResetPasswordPage {
    /// CSRF token.
    pub csrf_token: String,
    /// The reset token from the URL query parameter.
    pub token: String,
    /// Error message (empty = no error).
    pub error: String,
}

// ── Marketing page types (P12-T1) ────────────────────────────────────────────────

/// `GET /` — the public landing page with hero, how-it-works (3 steps),
/// social proof placeholder, and CTA.
///
/// When `is_authenticated` is true, the nav shows links to the app instead
/// of login/signup. Marketing pages are always public but detect auth from
/// the session cookie for correct navigation.
#[derive(Debug, Template)]
#[template(path = "landing.html")]
pub(crate) struct LandingPage {
    /// Whether a valid session cookie was detected.
    pub is_authenticated: bool,
}

/// `GET /pricing` — the public pricing page rendered from the plans database.
///
/// Each [`PlanCard`] is derived from a [`Plan`](kb_store::plans::Plan) row.
/// When the database is not reachable, `plans` is empty and the template
/// shows a loading placeholder.
#[derive(Debug, Template)]
#[template(path = "pricing.html")]
pub(crate) struct PricingPage {
    /// Whether a valid session cookie was detected.
    pub is_authenticated: bool,
    /// Plan cards for display, ordered by price (free first).
    pub plans: Vec<PlanCard>,
}

/// A single plan card rendered on the pricing page.
///
/// Fields are pre-formatted for template display — no logic in the template.
#[derive(Debug, Clone)]
pub(crate) struct PlanCard {
    /// Machine-readable code: `free`, `pro`, `team`.
    pub code: String,
    /// Human-readable name: `Free`, `Pro`, `Team`.
    pub name: String,
    /// Formatted price string (e.g. `"Free"`, `"$9/mo"`).
    pub price_display: String,
    /// Human-readable storage quota (e.g. `"50 MB"`, `"5 GB"`).
    pub quota_display: String,
    /// Feature list items for the plan card.
    pub features: Vec<String>,
    /// URL the CTA button links to.
    pub cta_url: String,
    /// CTA button text (e.g. `"Get Started"`, `"Subscribe"`).
    pub cta_text: String,
    /// Whether this is the highlighted/recommended plan.
    pub highlighted: bool,
}

/// `GET /features` — the public features page listing supported file types
/// and core capabilities.
#[derive(Debug, Template)]
#[template(path = "features.html")]
pub(crate) struct FeaturesPage {
    /// Whether a valid session cookie was detected.
    pub is_authenticated: bool,
    /// Per-filetype support entries.
    pub file_types: Vec<FileTypeSupport>,
}

/// A single row in the supported-file-types grid.
#[derive(Debug, Clone)]
pub(crate) struct FileTypeSupport {
    /// Comma-separated extensions (e.g. `".pdf, .docx"`).
    pub extension: String,
    /// Human-readable name (e.g. `"PDF & Office documents"`).
    pub name: String,
    /// Whether Local KB can extract text and metadata.
    pub supported: bool,
}

/// `GET /faq` — the public FAQ page with expandable question/answer items.
#[derive(Debug, Template)]
#[template(path = "faq.html")]
pub(crate) struct FaqPage {
    /// Whether a valid session cookie was detected.
    pub is_authenticated: bool,
    /// Question-and-answer pairs.
    pub faqs: Vec<FaqItem>,
}

/// A single question-and-answer pair for the FAQ page.
#[derive(Debug, Clone)]
pub(crate) struct FaqItem {
    /// The question text.
    pub question: String,
    /// The answer text (may contain basic HTML like links).
    pub answer: String,
}
