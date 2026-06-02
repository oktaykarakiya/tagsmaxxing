//! Email sending trait for verification, password reset, and notifications (P12-T2, P12-T7).
//!
//! This module defines the [`EmailSender`] async trait that decouples the web handlers
//! from a specific email provider. A no-op implementation that logs to tracing is provided
//! for dev/test use until P12-T7 wires a real provider (Resend/Postmark/SES).

use async_trait::async_trait;

/// The contract for sending transactional emails (verification, password reset,
/// quota warnings, payment failures, etc.).
///
/// # Implementations
///
/// * [`NoopEmailSender`] — logs email content at INFO level (dev/tests).
/// * `ResendEmail` / `PostmarkEmail` — real HTTP-based senders (P12-T7).
///
/// # Thread safety
///
/// The trait requires `Send + Sync` so it can be wrapped in an `Arc` and shared
/// across axum worker tasks.
#[async_trait]
pub trait EmailSender: Send + Sync {
    /// Send a verification email containing `verify_url` to the given `email` address.
    ///
    /// The caller is responsible for generating the token and constructing the
    /// absolute URL. The implementation delivers the email; it does not generate tokens.
    ///
    /// # Errors
    ///
    /// Returns an error when the email provider is unreachable or returns a non-2xx
    /// status. Callers should treat failures as non-fatal (the user can re-request
    /// verification from the "check your email" page).
    async fn send_verification_email(&self, email: &str, verify_url: &str) -> anyhow::Result<()>;

    /// Send a password reset email containing `reset_url` to the given `email` address.
    ///
    /// The caller is responsible for generating the token and constructing the
    /// absolute URL. The implementation delivers the email; it does not generate tokens.
    ///
    /// # Errors
    ///
    /// Returns an error when the email provider is unreachable or returns a non-2xx
    /// status. Callers should treat failures as non-fatal.
    async fn send_password_reset_email(&self, email: &str, reset_url: &str) -> anyhow::Result<()>;
}

/// A no-op email sender that logs the email content at INFO level.
///
/// Suitable for development and test environments. When P12-T7 adds real email
/// providers, this implementation can be replaced without changing handler code.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopEmailSender;

#[async_trait]
impl EmailSender for NoopEmailSender {
    async fn send_verification_email(&self, email: &str, verify_url: &str) -> anyhow::Result<()> {
        // The no-op sender logs to stderr for dev visibility. Production code
        // should use a real sender (P12-T7).
        eprintln!("[NoopEmailSender] Would send verification email to {email}: {verify_url}");
        Ok(())
    }

    async fn send_password_reset_email(&self, email: &str, reset_url: &str) -> anyhow::Result<()> {
        eprintln!("[NoopEmailSender] Would send password reset email to {email}: {reset_url}");
        Ok(())
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[tokio::test]
    async fn noop_send_verification_does_not_error() {
        let sender = NoopEmailSender;
        let result = sender
            .send_verification_email("test@example.com", "http://localhost:9999/verify?token=abc")
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn noop_send_password_reset_does_not_error() {
        let sender = NoopEmailSender;
        let result = sender
            .send_password_reset_email("test@example.com", "http://localhost:9999/reset?token=abc")
            .await;
        assert!(result.is_ok());
    }
}
