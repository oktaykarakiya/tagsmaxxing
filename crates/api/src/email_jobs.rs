//! Transactional email: job-queue integration, template rendering, and provider impls
//! (P12-T7).
//!
//! This module wires email delivery through the durable job queue so that
//! sending an email never blocks a request handler. The flow is:
//!
//! 1. A handler calls [`enqueue_email_job`] with the email kind and template
//!    parameters, which renders the Askama template, serializes the payload,
//!    and inserts a `send_email` job row.
//! 2. The job worker claims the row, deserializes the payload, and calls
//!    [`EmailProvider::send`] on the configured provider.
//! 3. On failure the job is retried with exponential backoff (3 attempts,
//!    then dead-lettered per plan §16 / P12-T7).
//!
//! # Provider implementations
//!
//! * [`LogEmail`](kb_core::email::LogEmail) — logs to tracing (dev/test).
//! * [`ResendEmail`] — HTTP API client for [Resend](https://resend.com).

use std::sync::Arc;

use anyhow::Context;
use arc_swap::ArcSwap;
use async_trait::async_trait;
use kb_core::email::EmailProvider;
use kb_core::job::Job;
use serde::{Deserialize, Serialize};

use super::web::templates::{
    IngestFailedEmail, InviteEmail, InvoiceEmail, PasswordResetEmail, PaymentFailedEmail,
    QuotaWarningEmail, VerificationEmail, WelcomeEmail,
};

// ═══════════════════════════════════════════════════════════════════════════════
// Email job payload
// ═══════════════════════════════════════════════════════════════════════════════

/// Identifies which kind of transactional email this job delivers.
///
/// The variant determines which Askama template to render and which parameters
/// to expect in the payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmailKind {
    /// Email verification (signup flow).
    Verification,
    /// Password reset (forgot-password flow).
    PasswordReset,
    /// Welcome email after signup + verification.
    Welcome,
    /// Payment receipt / invoice (Stripe `invoice.paid`).
    Invoice,
    /// Payment failure / dunning (Stripe `invoice.payment_failed`).
    PaymentFailed,
    /// Quota warning at 80% or 100%.
    QuotaWarning,
    /// Ingest failed after exhausting retries.
    IngestFailed,
    /// Team member invitation.
    Invite,
}

/// Serializable payload stored as the job's `last_error` column (JSON text).
///
/// The job worker deserializes this, renders the appropriate Askama template,
/// and calls [`EmailProvider::send`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailJobPayload {
    /// Which email template to use.
    pub kind: EmailKind,
    /// Recipient email address.
    pub to: String,
    /// Email subject line (plain text).
    pub subject: String,
    /// Template parameters — the keys depend on `kind`.
    /// The worker uses these to populate the template struct.
    #[serde(flatten)]
    pub params: serde_json::Value,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Resend.com HTTP email provider
// ═══════════════════════════════════════════════════════════════════════════════

/// Sends emails through the [Resend](https://resend.com) HTTP API.
///
/// The API key is hot-swappable via [`ArcSwap`] (CLAUDE.md hot-swap rule).
/// The `from` address is configurable and defaults to
/// `"Local KB <noreply@local-kb.dev>"`.
///
/// # Wire format
///
/// Resend expects `POST https://api.resend.com/emails` with:
/// ```json
/// {
///   "from": "Sender <from@example.com>",
///   "to": ["recipient@example.com"],
///   "subject": "…",
///   "html": "<h1>…</h1>"
/// }
/// ```
///
/// Authentication is via `Authorization: Bearer <api_key>`.
pub struct ResendEmail {
    /// Hot-swappable Resend API key (`re_…`).
    api_key: Arc<ArcSwap<String>>,
    /// The `From:` address for outgoing emails.
    from: String,
    /// HTTP client used for all requests.
    client: reqwest::Client,
}

impl ResendEmail {
    /// Create a new Resend email provider.
    ///
    /// `api_key` is wrapped in an [`ArcSwap`] so it can be rotated without
    /// restarting the server. `from` is the `From:` address (e.g.
    /// `"Local KB <noreply@local-kb.dev>"`).
    #[must_use]
    pub fn new(api_key: impl Into<String>, from: impl Into<String>) -> Self {
        let key = api_key.into();
        Self {
            api_key: Arc::new(ArcSwap::new(Arc::new(key))),
            from: from.into(),
            client: reqwest::Client::new(),
        }
    }

    /// Return a handle to the hot-swappable API key so an external reload
    /// mechanism can update it.
    #[must_use]
    pub fn api_key_handle(&self) -> Arc<ArcSwap<String>> {
        Arc::clone(&self.api_key)
    }
}

impl std::fmt::Debug for ResendEmail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResendEmail")
            .field("from", &self.from)
            .field("api_key", &"<redacted>")
            .finish()
    }
}

#[async_trait]
impl EmailProvider for ResendEmail {
    async fn send(&self, to: &str, subject: &str, body_html: &str) -> anyhow::Result<()> {
        let api_key = self.api_key.load();
        let api_key = api_key.as_ref().clone();

        let body = serde_json::json!({
            "from": self.from,
            "to": [to],
            "subject": subject,
            "html": body_html,
        });

        let resp = self
            .client
            .post("https://api.resend.com/emails")
            .header("Authorization", format!("Bearer {api_key}"))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .context("failed to send Resend API request")?;

        let status = resp.status();
        if status.is_success() {
            tracing::info!(
                to = %to,
                subject = %subject,
                status = %status,
                "Resend email sent"
            );
            Ok(())
        } else {
            let body_text = resp.text().await.unwrap_or_default();
            anyhow::bail!("Resend API returned {status}: {body_text}",)
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Template rendering
// ═══════════════════════════════════════════════════════════════════════════════

/// Render the appropriate Askama template based on the email kind and parameters.
///
/// Returns `(subject, body_html)` on success. The `params` JSON object must
/// contain the fields expected by the template for the given `kind`.
///
/// # Errors
///
/// Returns an error if a required parameter is missing or the template fails
/// to render.
fn render_email(kind: EmailKind, params: &serde_json::Value) -> anyhow::Result<String> {
    match kind {
        EmailKind::Verification => {
            let verify_url = get_str(params, "verify_url")?;
            let tmpl = VerificationEmail {
                verify_url: verify_url.to_string(),
            };
            askama::Template::render(&tmpl).context("failed to render verification email")
        }
        EmailKind::PasswordReset => {
            let reset_url = get_str(params, "reset_url")?;
            let tmpl = PasswordResetEmail {
                reset_url: reset_url.to_string(),
            };
            askama::Template::render(&tmpl).context("failed to render password reset email")
        }
        EmailKind::Welcome => {
            let workspace_name = get_str(params, "workspace_name")?;
            let login_url = get_str(params, "login_url")?;
            let tmpl = WelcomeEmail {
                workspace_name: workspace_name.to_string(),
                login_url: login_url.to_string(),
            };
            askama::Template::render(&tmpl).context("failed to render welcome email")
        }
        EmailKind::Invoice => {
            let plan_name = get_str(params, "plan_name")?;
            let amount_display = get_str(params, "amount_display")?;
            let invoice_date = get_str(params, "invoice_date")?;
            let account_url = get_str(params, "account_url")?;
            let tmpl = InvoiceEmail {
                plan_name: plan_name.to_string(),
                amount_display: amount_display.to_string(),
                invoice_date: invoice_date.to_string(),
                account_url: account_url.to_string(),
            };
            askama::Template::render(&tmpl).context("failed to render invoice email")
        }
        EmailKind::PaymentFailed => {
            let plan_name = get_str(params, "plan_name")?;
            let failure_reason = get_str(params, "failure_reason")?;
            let billing_url = get_str(params, "billing_url")?;
            let tmpl = PaymentFailedEmail {
                plan_name: plan_name.to_string(),
                failure_reason: failure_reason.to_string(),
                billing_url: billing_url.to_string(),
            };
            askama::Template::render(&tmpl).context("failed to render payment failed email")
        }
        EmailKind::QuotaWarning => {
            let quota_label = get_str(params, "quota_label")?;
            let percent_used: u8 = params
                .get("percent_used")
                .and_then(|v| v.as_u64())
                .map(|n| n as u8)
                .context("missing or invalid 'percent_used' for quota warning")?;
            let used_display = get_str(params, "used_display")?;
            let limit_display = get_str(params, "limit_display")?;
            let workspace_name = get_str(params, "workspace_name")?;
            let upgrade_url = get_str(params, "upgrade_url")?;
            let tmpl = QuotaWarningEmail {
                quota_label: quota_label.to_string(),
                percent_used,
                used_display: used_display.to_string(),
                limit_display: limit_display.to_string(),
                workspace_name: workspace_name.to_string(),
                upgrade_url: upgrade_url.to_string(),
            };
            askama::Template::render(&tmpl).context("failed to render quota warning email")
        }
        EmailKind::IngestFailed => {
            let file_name = get_str(params, "file_name")?;
            let error_message = get_str(params, "error_message")?;
            let upload_url = get_str(params, "upload_url")?;
            let tmpl = IngestFailedEmail {
                file_name: file_name.to_string(),
                error_message: error_message.to_string(),
                upload_url: upload_url.to_string(),
            };
            askama::Template::render(&tmpl).context("failed to render ingest failed email")
        }
        EmailKind::Invite => {
            let workspace_name = get_str(params, "workspace_name")?;
            let invited_by = get_str(params, "invited_by")?;
            let invite_url = get_str(params, "invite_url")?;
            let tmpl = InviteEmail {
                workspace_name: workspace_name.to_string(),
                invited_by: invited_by.to_string(),
                invite_url: invite_url.to_string(),
            };
            askama::Template::render(&tmpl).context("failed to render invite email")
        }
    }
}

/// Extract a string field from a JSON value, returning an error with the field
/// name if it's missing or not a string.
fn get_str<'a>(params: &'a serde_json::Value, field: &str) -> anyhow::Result<&'a str> {
    params
        .get(field)
        .and_then(|v| v.as_str())
        .with_context(|| format!("missing or invalid string field '{field}' in email params"))
}

// ═══════════════════════════════════════════════════════════════════════════════
// Job queue helpers
// ═══════════════════════════════════════════════════════════════════════════════

/// Maximum number of retry attempts for email jobs before dead-lettering.
///
/// Per P12-T7 acceptance criteria: email job failure retries (3×) then dead-letters.
pub const EMAIL_JOB_MAX_RETRIES: i32 = 3;

/// Email job priority — email delivery is low-priority relative to user ingest
/// work but above maintenance jobs. Higher is more urgent.
const EMAIL_JOB_PRIORITY: i32 = -5;

/// Enqueue a transactional email job using a direct PgPool reference.
///
/// Renders the Askama template from `kind` and `params`, serializes the payload,
/// and inserts a `send_email` job row. Returns immediately — the email is
/// delivered asynchronously by the job worker.
///
/// The `tenant_id` is used for tenant scoping; the `to` address is embedded
/// in the payload so the worker can deliver to the correct recipient.
///
/// The payload is stored in the `last_error` column as JSON. When the worker
/// picks up the job, it deserializes the payload, extracts the pre-rendered
/// HTML, and calls [`EmailProvider::send`]. On failure, the worker overwrites
/// `last_error` with the actual error message (standard job-queue semantics).
///
/// # Errors
///
/// Returns an error if the template fails to render or the database operation
/// fails.
pub async fn enqueue_email_job_via_pool(
    pool: &sqlx::PgPool,
    tenant_id: i64,
    to: &str,
    subject: &str,
    kind: EmailKind,
    params: serde_json::Value,
) -> anyhow::Result<i64> {
    let body_html = render_email(kind, &params).context("failed to render email template")?;

    let payload_json = serde_json::json!({
        "email": {
            "to": to,
            "subject": subject,
            "body_html": body_html,
            "kind": kind,
        }
    });

    let row = sqlx::query_scalar::<_, i64>(
        "INSERT INTO jobs \
         (tenant_id, file_id, document_id, kind, priority, status, attempts, \
          last_error, run_after, created_at) \
         VALUES ($1, NULL, NULL, 'send_email', $2, 'queued', 0, \
                 $3, now(), now()) \
         RETURNING id",
    )
    .bind(tenant_id)
    .bind(EMAIL_JOB_PRIORITY)
    .bind(serde_json::to_string(&payload_json).unwrap_or_default())
    .fetch_one(pool)
    .await
    .context("failed to enqueue email job")?;

    tracing::info!(
        job_id = row,
        kind = ?kind,
        to = %to,
        subject = %subject,
        "Email job enqueued"
    );

    Ok(row)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Job worker handler
// ═══════════════════════════════════════════════════════════════════════════════

/// Process a claimed `send_email` job: deserialize the payload, extract the
/// rendered HTML, and deliver it via the configured [`EmailProvider`].
///
/// This function is the handler passed to
/// [`run_worker_pool`](kb_pipeline::job_queue::run_worker_pool). It returns
/// `Ok(())` on successful delivery or `Err(String)` with the error message
/// on failure (which triggers retry / dead-letter).
///
/// # Payload format
///
/// The job's `last_error` column contains a JSON object:
/// ```json
/// {
///   "email": {
///     "to": "user@example.com",
///     "subject": "Verify your email",
///     "body_html": "<html>…</html>",
///     "kind": "verification"
///   }
/// }
/// ```
pub async fn process_email_job(provider: Arc<dyn EmailProvider>, job: Job) -> Result<(), String> {
    let payload_str = job.last_error.as_deref().unwrap_or("{}");

    let wrapper: serde_json::Value =
        serde_json::from_str(payload_str).map_err(|e| format!("invalid email job payload: {e}"))?;

    let email = wrapper
        .get("email")
        .ok_or_else(|| "missing 'email' key in job payload".to_string())?;

    let to = email_str(email, "to")?;
    let subject = email_str(email, "subject")?;
    let body_html = email_str(email, "body_html")?;

    provider
        .send(to, subject, body_html)
        .await
        .map_err(|e| format!("email delivery failed: {e}"))
}

/// Extract a string field from an email payload JSON object.
fn email_str<'a>(val: &'a serde_json::Value, field: &str) -> Result<&'a str, String> {
    val.get(field)
        .and_then(|v| v.as_str())
        .ok_or_else(|| format!("missing or invalid '{field}' in email payload"))
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use kb_core::email::LogEmail;
    use kb_core::job::{Job, JobKind, JobStatus};
    use serde_json::json;

    // ── render_email tests ─────────────────────────────────────────────────

    #[test]
    fn render_verification_email_contains_link() {
        let params = json!({"verify_url": "http://localhost:9999/verify?token=abc123"});
        let html = render_email(EmailKind::Verification, &params).unwrap();
        assert!(html.contains("http://localhost:9999/verify?token=abc123"));
        assert!(html.contains("Verify Email Address"));
        assert!(html.contains("verify your email"));
    }

    #[test]
    fn render_verification_email_missing_url_errors() {
        let params = json!({});
        let result = render_email(EmailKind::Verification, &params);
        assert!(result.is_err());
    }

    #[test]
    fn render_password_reset_email_contains_link() {
        let params = json!({"reset_url": "http://localhost:9999/reset?token=xyz789"});
        let html = render_email(EmailKind::PasswordReset, &params).unwrap();
        assert!(html.contains("http://localhost:9999/reset?token=xyz789"));
        assert!(html.contains("Reset Password"));
    }

    #[test]
    fn render_password_reset_email_missing_url_errors() {
        let params = json!({});
        let result = render_email(EmailKind::PasswordReset, &params);
        assert!(result.is_err());
    }

    #[test]
    fn render_welcome_email_contains_workspace_name() {
        let params = json!({
            "workspace_name": "ACME Corp",
            "login_url": "http://localhost:9999/login"
        });
        let html = render_email(EmailKind::Welcome, &params).unwrap();
        assert!(html.contains("ACME Corp"));
        assert!(html.contains("http://localhost:9999/login"));
    }

    #[test]
    fn render_welcome_email_missing_fields_errors() {
        let params = json!({"workspace_name": "Test"});
        let result = render_email(EmailKind::Welcome, &params);
        assert!(result.is_err());
    }

    #[test]
    fn render_invoice_email_contains_details() {
        let params = json!({
            "plan_name": "Pro",
            "amount_display": "$9.00",
            "invoice_date": "2026-06-02",
            "account_url": "http://localhost:9999/account"
        });
        let html = render_email(EmailKind::Invoice, &params).unwrap();
        assert!(html.contains("Pro"));
        assert!(html.contains("$9.00"));
        assert!(html.contains("2026-06-02"));
        assert!(html.contains("/account"));
    }

    #[test]
    fn render_payment_failed_email_contains_reason() {
        let params = json!({
            "plan_name": "Team",
            "failure_reason": "card declined",
            "billing_url": "http://localhost:9999/account/plan"
        });
        let html = render_email(EmailKind::PaymentFailed, &params).unwrap();
        assert!(html.contains("card declined"));
        assert!(html.contains("Team"));
        assert!(html.contains("Update Payment Method"));
    }

    #[test]
    fn render_quota_warning_at_80_percent() {
        let params = json!({
            "quota_label": "Storage",
            "percent_used": 80,
            "used_display": "4.0 GB",
            "limit_display": "5.0 GB",
            "workspace_name": "My Workspace",
            "upgrade_url": "http://localhost:9999/account/plan"
        });
        let html = render_email(EmailKind::QuotaWarning, &params).unwrap();
        assert!(html.contains("80%"));
        assert!(html.contains("4.0 GB"));
        assert!(html.contains("5.0 GB"));
        assert!(html.contains("My Workspace"));
    }

    #[test]
    fn render_quota_warning_at_100_percent() {
        let params = json!({
            "quota_label": "Tokens",
            "percent_used": 100,
            "used_display": "500K",
            "limit_display": "500K",
            "workspace_name": "ACME",
            "upgrade_url": "http://localhost:9999/account/plan"
        });
        let html = render_email(EmailKind::QuotaWarning, &params).unwrap();
        assert!(html.contains("100%"));
        assert!(html.contains("exceeded your quota"));
        assert!(html.contains("Upgrade Plan"));
    }

    #[test]
    fn render_ingest_failed_email_contains_details() {
        let params = json!({
            "file_name": "report.pdf",
            "error_message": "unsupported format",
            "upload_url": "http://localhost:9999/upload"
        });
        let html = render_email(EmailKind::IngestFailed, &params).unwrap();
        assert!(html.contains("report.pdf"));
        assert!(html.contains("unsupported format"));
    }

    #[test]
    fn render_invite_email_contains_details() {
        let params = json!({
            "workspace_name": "ACME Corp",
            "invited_by": "admin@acme.com",
            "invite_url": "http://localhost:9999/accept?token=inv123"
        });
        let html = render_email(EmailKind::Invite, &params).unwrap();
        assert!(html.contains("ACME Corp"));
        assert!(html.contains("admin@acme.com"));
        assert!(html.contains("inv123"));
        assert!(html.contains("Accept Invitation"));
    }

    #[test]
    fn render_invite_email_missing_fields_errors() {
        let params = json!({"workspace_name": "T"});
        let result = render_email(EmailKind::Invite, &params);
        assert!(result.is_err());
    }

    // ── get_str tests ──────────────────────────────────────────────────────

    #[test]
    fn get_str_returns_value() {
        let params = json!({"key": "value"});
        assert_eq!(get_str(&params, "key").unwrap(), "value");
    }

    #[test]
    fn get_str_missing_field_errors() {
        let params = json!({"other": "x"});
        assert!(get_str(&params, "key").is_err());
    }

    #[test]
    fn get_str_non_string_errors() {
        let params = json!({"key": 42});
        assert!(get_str(&params, "key").is_err());
    }

    // ── EmailKind serialization ────────────────────────────────────────────

    #[test]
    fn email_kind_serde_roundtrips() {
        let kinds = [
            EmailKind::Verification,
            EmailKind::PasswordReset,
            EmailKind::Welcome,
            EmailKind::Invoice,
            EmailKind::PaymentFailed,
            EmailKind::QuotaWarning,
            EmailKind::IngestFailed,
            EmailKind::Invite,
        ];
        for k in &kinds {
            let json_str = serde_json::to_string(k).unwrap();
            let back: EmailKind = serde_json::from_str(&json_str).unwrap();
            assert_eq!(*k, back);
        }
    }

    #[test]
    fn email_kind_wire_format_is_snake_case() {
        assert_eq!(
            serde_json::to_string(&EmailKind::PasswordReset).unwrap(),
            "\"password_reset\""
        );
        assert_eq!(
            serde_json::to_string(&EmailKind::PaymentFailed).unwrap(),
            "\"payment_failed\""
        );
        assert_eq!(
            serde_json::to_string(&EmailKind::QuotaWarning).unwrap(),
            "\"quota_warning\""
        );
        assert_eq!(
            serde_json::to_string(&EmailKind::IngestFailed).unwrap(),
            "\"ingest_failed\""
        );
    }

    // ── EmailJobPayload tests ──────────────────────────────────────────────

    #[test]
    fn email_job_payload_serde_roundtrip() {
        let payload = EmailJobPayload {
            kind: EmailKind::Verification,
            to: "test@example.com".into(),
            subject: "Verify your email".into(),
            params: json!({"verify_url": "http://localhost:9999/verify?token=abc"}),
        };
        let json_str = serde_json::to_string(&payload).unwrap();
        let back: EmailJobPayload = serde_json::from_str(&json_str).unwrap();
        assert_eq!(back.kind, EmailKind::Verification);
        assert_eq!(back.to, "test@example.com");
        assert_eq!(back.subject, "Verify your email");
        assert_eq!(
            back.params["verify_url"],
            "http://localhost:9999/verify?token=abc"
        );
    }

    // ── ResendEmail tests ──────────────────────────────────────────────────

    #[test]
    fn resend_email_debug_redacts_api_key() {
        let provider = ResendEmail::new("re_test_12345", "Test <test@example.com>");
        let debug_str = format!("{provider:?}");
        assert!(!debug_str.contains("re_test_12345"));
        assert!(debug_str.contains("<redacted>"));
        assert!(debug_str.contains("Test <test@example.com>"));
    }

    #[test]
    fn resend_email_new_stores_from() {
        let provider = ResendEmail::new("re_key", "Local KB <noreply@local-kb.dev>");
        let debug_str = format!("{provider:?}");
        assert!(debug_str.contains("Local KB <noreply@local-kb.dev>"));
    }

    // ── process_email_job tests ────────────────────────────────────────────

    #[tokio::test]
    async fn process_email_job_success() {
        let provider: Arc<dyn EmailProvider> = Arc::new(LogEmail);
        let payload = json!({
            "email": {
                "to": "test@example.com",
                "subject": "Test",
                "body_html": "<p>Hello</p>",
                "kind": "verification"
            }
        });
        let job = Job {
            id: 1,
            tenant_id: 1,
            file_id: None,
            document_id: None,
            kind: JobKind::SendEmail,
            priority: 0,
            status: JobStatus::Running,
            attempts: 0,
            last_error: Some(serde_json::to_string(&payload).unwrap()),
            run_after: chrono::Utc::now(),
            created_at: chrono::Utc::now(),
            created_by: None,
        };
        let result = process_email_job(provider, job).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn process_email_job_missing_email_key() {
        let provider: Arc<dyn EmailProvider> = Arc::new(LogEmail);
        let payload = json!({"other": "data"});
        let job = Job {
            id: 2,
            tenant_id: 1,
            file_id: None,
            document_id: None,
            kind: JobKind::SendEmail,
            priority: 0,
            status: JobStatus::Running,
            attempts: 0,
            last_error: Some(serde_json::to_string(&payload).unwrap()),
            run_after: chrono::Utc::now(),
            created_at: chrono::Utc::now(),
            created_by: None,
        };
        let result = process_email_job(provider, job).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("missing 'email' key"));
    }

    #[tokio::test]
    async fn process_email_job_null_last_error() {
        let provider: Arc<dyn EmailProvider> = Arc::new(LogEmail);
        let job = Job {
            id: 3,
            tenant_id: 1,
            file_id: None,
            document_id: None,
            kind: JobKind::SendEmail,
            priority: 0,
            status: JobStatus::Running,
            attempts: 0,
            last_error: None,
            run_after: chrono::Utc::now(),
            created_at: chrono::Utc::now(),
            created_by: None,
        };
        let result = process_email_job(provider, job).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn process_email_job_invalid_json() {
        let provider: Arc<dyn EmailProvider> = Arc::new(LogEmail);
        let job = Job {
            id: 4,
            tenant_id: 1,
            file_id: None,
            document_id: None,
            kind: JobKind::SendEmail,
            priority: 0,
            status: JobStatus::Running,
            attempts: 0,
            last_error: Some("not json".into()),
            run_after: chrono::Utc::now(),
            created_at: chrono::Utc::now(),
            created_by: None,
        };
        let result = process_email_job(provider, job).await;
        assert!(result.is_err());
    }

    // ── EMAIL_JOB_MAX_RETRIES is 3 ─────────────────────────────────────────

    #[test]
    fn email_job_max_retries_is_three() {
        assert_eq!(EMAIL_JOB_MAX_RETRIES, 3);
    }

    // ── EmailJobPayload with each kind ─────────────────────────────────────

    #[test]
    fn payload_verification_kind() {
        let p = EmailJobPayload {
            kind: EmailKind::Verification,
            to: "a@b.com".into(),
            subject: "Verify".into(),
            params: json!({"verify_url": "http://x.com/v?t=1"}),
        };
        let s = serde_json::to_string(&p).unwrap();
        assert!(s.contains("verification"));
        assert!(s.contains("verify_url"));
    }

    #[test]
    fn payload_quota_warning_kind() {
        let p = EmailJobPayload {
            kind: EmailKind::QuotaWarning,
            to: "a@b.com".into(),
            subject: "Quota at 80%".into(),
            params: json!({"percent_used": 80, "quota_label": "Storage"}),
        };
        let s = serde_json::to_string(&p).unwrap();
        assert!(s.contains("quota_warning"));
        assert!(s.contains("80"));
    }
}
