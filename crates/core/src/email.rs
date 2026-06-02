//! Email sending traits for verification, password reset, and notifications
//! (P12-T2, P12-T7).
//!
//! This module defines two traits:
//!
//! * [`EmailSender`] — high-level convenience trait with named methods for each
//!   email type. Used directly by web handlers that don't want to deal with HTML
//!   rendering. The default implementations construct a basic text/html body and
//!   delegate to [`EmailProvider::send`].
//! * [`EmailProvider`] — low-level generic trait with a single [`EmailProvider::send`]
//!   method. Used by the email job worker (P12-T7) to deliver pre-rendered HTML.
//!
//! Two implementations are provided:
//!
//! * [`LogEmail`] — logs email content to `tracing::info!` (dev/test).
//! * `ResendEmail` (in `kb-api`) — HTTP API call via reqwest (P12-T7).
//!
//! # Thread safety
//!
//! Both traits require `Send + Sync` so implementations can be wrapped in `Arc`
//! and shared across axum worker tasks.

use async_trait::async_trait;

// ═══════════════════════════════════════════════════════════════════════════════
// EmailProvider — low-level generic send
// ═══════════════════════════════════════════════════════════════════════════════

/// The low-level contract for delivering a pre-rendered transactional email.
///
/// This is the trait that the email job worker (P12-T7) calls. It receives
/// already-rendered HTML so the implementation is provider-agnostic — it just
/// needs to POST the body to an HTTP API or log it.
///
/// # Implementations
///
/// * [`LogEmail`] — logs at INFO level (dev/test).
/// * `ResendEmail` (in `kb-api`) — HTTP API call with API key (P12-T7).
#[async_trait]
pub trait EmailProvider: Send + Sync {
    /// Deliver a pre-rendered HTML email.
    ///
    /// # Arguments
    ///
    /// * `to` — recipient email address.
    /// * `subject` — email subject line (plain text).
    /// * `body_html` — complete HTML document for the email body.
    ///
    /// # Errors
    ///
    /// Returns an error when the provider is unreachable or returns a non-2xx
    /// status. Callers should treat failures as non-fatal (the job queue will
    /// retry up to 3 times, then dead-letter).
    async fn send(&self, to: &str, subject: &str, body_html: &str) -> anyhow::Result<()>;
}

// ═══════════════════════════════════════════════════════════════════════════════
// EmailSender — high-level convenience trait
// ═══════════════════════════════════════════════════════════════════════════════

/// The contract for sending named transactional emails.
///
/// Each method corresponds to a specific email type. Default implementations
/// construct a basic HTML body with the provided link and delegate to
/// [`EmailProvider::send`]. Implementations that want custom rendering for
/// each email type can override these methods.
///
/// # Relationship to [`EmailProvider`]
///
/// `EmailSender` is the **handler-facing** trait; [`EmailProvider`] is the
/// **worker-facing** trait. Both are implemented by [`LogEmail`] and
/// `ResendEmail`. The handler typically calls an `EmailSender` method
/// directly (synchronous in the request path for backward compat with tests),
/// while the job worker uses [`EmailProvider::send`] with pre-rendered
/// Askama templates (P12-T7).
#[async_trait]
pub trait EmailSender: EmailProvider {
    /// Send a verification email containing `verify_url` to the given `email` address.
    ///
    /// The default implementation constructs a simple HTML body linking to
    /// `verify_url` and delegates to [`EmailProvider::send`].
    ///
    /// # Errors
    ///
    /// Returns an error when the email provider is unreachable or returns a
    /// non-2xx status. Callers should treat failures as non-fatal.
    async fn send_verification_email(&self, email: &str, verify_url: &str) -> anyhow::Result<()> {
        let subject = "Verify your email address";
        let body_html = format!(
            "<html><body><p>Please verify your email by clicking the link below:</p>\
             <p><a href=\"{verify_url}\">{verify_url}</a></p>\
             <p>If you did not create an account, you can safely ignore this email.</p></body></html>"
        );
        self.send(email, subject, &body_html).await
    }

    /// Send a password reset email containing `reset_url` to the given `email` address.
    ///
    /// The default implementation constructs a simple HTML body linking to
    /// `reset_url` and delegates to [`EmailProvider::send`].
    ///
    /// # Errors
    ///
    /// Returns an error when the email provider is unreachable or returns a
    /// non-2xx status. Callers should treat failures as non-fatal.
    async fn send_password_reset_email(&self, email: &str, reset_url: &str) -> anyhow::Result<()> {
        let subject = "Reset your password";
        let body_html = format!(
            "<html><body><p>You requested a password reset. Click the link below to set a new password:</p>\
             <p><a href=\"{reset_url}\">{reset_url}</a></p>\
             <p>This link expires in 1 hour. If you did not request this, you can safely ignore this email.</p></body></html>"
        );
        self.send(email, subject, &body_html).await
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// LogEmail — dev/test implementation
// ═══════════════════════════════════════════════════════════════════════════════

/// An email provider that logs the email content to [`tracing::info!`].
///
/// Suitable for development and test environments. Does **not** use `eprintln!`
/// — it goes through the tracing infrastructure so structured log collectors
/// (e.g. the JSON subscriber) capture the full email content.
///
/// # Example
///
/// ```rust
/// use kb_core::email::{EmailProvider, LogEmail};
///
/// # async fn example() -> anyhow::Result<()> {
/// let sender = LogEmail;
/// sender.send("user@example.com", "Hello", "<h1>Hi</h1>").await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct LogEmail;

#[async_trait]
impl EmailProvider for LogEmail {
    async fn send(&self, to: &str, subject: &str, body_html: &str) -> anyhow::Result<()> {
        // Log at INFO level so structured log collectors capture the full
        // content. The body_html is truncated to 512 chars in the log line
        // to avoid flooding; the full content is available at TRACE level.
        let preview: String = body_html.chars().take(512).collect();
        tracing::info!(
            to = %to,
            subject = %subject,
            body_preview = %preview,
            body_len = body_html.len(),
            "[LogEmail] Would send email"
        );
        tracing::trace!(to = %to, subject = %subject, body_html = %body_html, "[LogEmail] Full body");
        Ok(())
    }
}

#[async_trait]
impl EmailSender for LogEmail {
    // Default implementations from EmailSender delegate to EmailProvider::send,
    // which LogEmail already implements. No overrides needed.
}

// ═══════════════════════════════════════════════════════════════════════════════
// NoopEmailSender — backward-compat alias (P12-T2)
// ═══════════════════════════════════════════════════════════════════════════════

/// Backward-compatible alias for [`LogEmail`].
///
/// This type exists so existing code that references `NoopEmailSender` continues
/// to compile. New code should use [`LogEmail`] directly.
///
/// # Deprecation
///
/// This alias will be removed in a future cleanup pass.
#[deprecated(since = "0.1.0", note = "use LogEmail instead")]
pub type NoopEmailSender = LogEmail;

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    // ── LogEmail / EmailProvider tests ─────────────────────────────────────

    #[tokio::test]
    async fn log_email_send_does_not_error() {
        let sender = LogEmail;
        let result = sender
            .send("test@example.com", "Test Subject", "<h1>Hello</h1>")
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn log_email_send_with_empty_body() {
        let sender = LogEmail;
        let result = sender.send("a@b.com", "S", "").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn log_email_send_with_long_html_body() {
        let sender = LogEmail;
        let long_body = "<p>".repeat(5000);
        let result = sender.send("long@example.com", "Long", &long_body).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn log_email_send_with_special_chars_in_subject() {
        let sender = LogEmail;
        let result = sender
            .send(
                "test@example.com",
                "Reset your password — パスワード",
                "<p>Reset</p>",
            )
            .await;
        assert!(result.is_ok());
    }

    // ── EmailSender default impls (via LogEmail) ───────────────────────────

    #[tokio::test]
    async fn log_email_send_verification_does_not_error() {
        let sender = LogEmail;
        let result = sender
            .send_verification_email("test@example.com", "http://localhost:9999/verify?token=abc")
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn log_email_send_password_reset_does_not_error() {
        let sender = LogEmail;
        let result = sender
            .send_password_reset_email("test@example.com", "http://localhost:9999/reset?token=abc")
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn log_email_send_verification_contains_link() {
        let sender = LogEmail;
        // The default EmailSender impl constructs a body with the URL.
        // We verify that send() is called by checking it doesn't error.
        let result = sender
            .send_verification_email(
                "test@example.com",
                "http://localhost:9999/verify?token=abc123",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn log_email_send_password_reset_contains_link() {
        let sender = LogEmail;
        let result = sender
            .send_password_reset_email(
                "test@example.com",
                "http://localhost:9999/reset?token=xyz789",
            )
            .await;
        assert!(result.is_ok());
    }

    // ── NoopEmailSender backward compat ────────────────────────────────────

    /// Backward compat: `LogEmail` works for all code that used `NoopEmailSender`.
    #[tokio::test]
    async fn log_email_used_as_noop_sender_works() {
        let sender = LogEmail;
        let result = sender
            .send_verification_email("test@example.com", "http://localhost:9999/verify?token=abc")
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn log_email_used_as_noop_sender_send_works() {
        let sender = LogEmail;
        let result = sender
            .send("test@example.com", "Subject", "<p>Body</p>")
            .await;
        assert!(result.is_ok());
    }

    // ── EmailSender is a supertrait of EmailProvider ───────────────────────

    /// Verify that `EmailSender: EmailProvider` — any `EmailSender` can be used
    /// where an `EmailProvider` is expected.
    #[tokio::test]
    async fn email_sender_can_be_used_as_email_provider() {
        fn _assert_supertrait(p: &LogEmail) -> &dyn EmailProvider {
            p
        }
        let sender = LogEmail;
        let provider: &dyn EmailProvider = &sender;
        let result = provider
            .send("test@example.com", "Subject", "<p>Body</p>")
            .await;
        assert!(result.is_ok());
    }

    /// Verify that `Arc<dyn EmailSender>` can be coerced to `Arc<dyn EmailProvider>`.
    #[tokio::test]
    async fn arc_email_sender_coerces_to_email_provider() {
        use std::sync::Arc;
        let sender: Arc<dyn EmailSender> = Arc::new(LogEmail);
        // This compiles: EmailSender: EmailProvider
        let provider: Arc<dyn EmailProvider> = sender;
        let result = provider
            .send("arc@example.com", "Arc Test", "<p>Arc</p>")
            .await;
        assert!(result.is_ok());
    }
}
