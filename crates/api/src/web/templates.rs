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
