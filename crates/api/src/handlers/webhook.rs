// SPDX-License-Identifier: AGPL-3.0-or-later

//! Stripe webhook handler (plan §29, P11-T3).
//!
//! `POST /stripe/webhook` — idempotent, signature-verified Stripe webhook endpoint.
//!
//! # Signature verification
//!
//! Stripe signs each webhook payload with HMAC-SHA256 using the webhook signing
//! secret. The signature header contains:
//!
//! ```text
//! stripe-signature: t=<unix_timestamp>,v1=<hex_hmac>[,v0=<legacy>]
//! ```
//!
//! The signed payload is `{timestamp}.{raw_body}`. We extract the `v1` signature,
//! compute our own HMAC, and constant-time compare.
//!
//! # Idempotency
//!
//! Stripe may deliver the same event more than once. Before processing, we INSERT
//! the `event_id` into the `stripe_events` table via `ON CONFLICT DO NOTHING`. If
//! the row already exists (rows_affected == 0), we return 200 immediately without
//! touching any other table.
//!
//! # Supported event types
//!
//! | Event type                       | Action                                            |
//! |----------------------------------|---------------------------------------------------|
//! | `checkout.session.completed`     | Activate subscription (plan, customer, period)     |
//! | `customer.subscription.updated`  | Plan change → update plan_id + quota               |
//! | `customer.subscription.deleted`  | Cancel → billing_status='canceled', free plan      |
//! | `invoice.paid`                   | Update current_period_end                          |
//! | `invoice.payment_failed`         | billing_status='past_due'                           |
//!
//! # Security
//!
//! This handler is **public** (no auth middleware) — Stripe's servers call it
//! directly. Authentication is via HMAC signature verification.
//! This is a **[checkpoint](§31.5)** — billing webhooks require human review.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;

use crate::AppState;

type HmacSha256 = Hmac<Sha256>;

// ── Signature verification ─────────────────────────────────────────────────────

/// Verify a Stripe webhook signature.
///
/// Extracts the timestamp and `v1` signature from the header, computes
/// `HMAC-SHA256(secret, "{timestamp}.{body}")`, and compares in constant time.
///
/// Returns `true` when the signature is valid. Returns `false` when:
/// * the signature header is missing or malformed;
/// * the `v1` scheme is not present;
/// * the HMAC does not match.
///
/// This is a **pure function** — no I/O, trivially unit-testable.
pub fn verify_stripe_signature(payload: &[u8], signature_header: &str, secret: &str) -> bool {
    // The header format is: t=1234567890,v1=<hex>,v0=<hex>
    let mut timestamp: Option<&str> = None;
    let mut v1_sig: Option<&str> = None;

    for part in signature_header.split(',') {
        let part = part.trim();
        if let Some(val) = part.strip_prefix("t=") {
            timestamp = Some(val);
        } else if let Some(val) = part.strip_prefix("v1=") {
            v1_sig = Some(val);
        }
        // v0 is ignored — it is a legacy scheme.
    }

    let (Some(t), Some(sig_hex)) = (timestamp, v1_sig) else {
        return false;
    };

    let sig_bytes = match hex::decode(sig_hex) {
        Ok(b) => b,
        Err(_) => return false,
    };

    // Signed payload = "{timestamp}.{raw_body}"
    let mut mac = match HmacSha256::new_from_slice(secret.as_bytes()) {
        Ok(m) => m,
        Err(_) => return false,
    };
    mac.update(t.as_bytes());
    mac.update(b".");
    mac.update(payload);

    // Constant-time verify.
    mac.verify_slice(&sig_bytes).is_ok()
}

// ── Event parsing ──────────────────────────────────────────────────────────────

/// A parsed Stripe webhook event.
///
/// Only the fields needed for processing are extracted; the full raw data is
/// discarded after parsing.
#[derive(Debug, Clone, PartialEq, Eq)]
struct StripeEvent {
    /// The unique event identifier (e.g. `evt_1Abc...`), used for idempotency.
    pub id: String,
    /// The event type string (e.g. `checkout.session.completed`).
    pub event_type: String,
    /// The `data.object` subtree of the event payload.
    pub data_object: Value,
}

/// Parse the event id, type, and data object from the raw JSON payload.
///
/// Returns `None` when the body is not valid JSON or required fields are missing.
///
/// This is a **pure function** — trivially unit-testable.
fn parse_stripe_event(payload: &[u8]) -> Option<StripeEvent> {
    let body: Value = serde_json::from_slice(payload).ok()?;
    let id = body.get("id")?.as_str()?.to_string();
    let event_type = body.get("type")?.as_str()?.to_string();
    let data_object = body.get("data")?.get("object")?.clone();
    Some(StripeEvent {
        id,
        event_type,
        data_object,
    })
}

// ── Event-specific data extraction (pure functions) ────────────────────────────

/// Relevant fields extracted from a `checkout.session.completed` event.
#[derive(Debug, Clone)]
struct CheckoutSessionCompleted {
    /// The `client_reference_id` we set to `tenant_id` when creating the session.
    tenant_id: i64,
    /// The Stripe customer id.
    stripe_customer_id: String,
    /// The Stripe subscription id.
    subscription_id: String,
    /// The plan code stored in `metadata.plan_code`.
    plan_code: String,
}

/// Extract fields from a `checkout.session.completed` event's data object.
///
/// Returns `None` when required fields are missing or unparseable.
fn extract_checkout_session(data: &Value) -> Option<CheckoutSessionCompleted> {
    let tenant_id_str = data.get("client_reference_id")?.as_str()?;
    let tenant_id: i64 = tenant_id_str.parse().ok()?;
    let stripe_customer_id = data.get("customer")?.as_str()?.to_string();
    let subscription_id = data.get("subscription")?.as_str()?.to_string();
    let plan_code = data
        .get("metadata")?
        .get("plan_code")?
        .as_str()?
        .to_string();

    Some(CheckoutSessionCompleted {
        tenant_id,
        stripe_customer_id,
        subscription_id,
        plan_code,
    })
}

/// Relevant fields extracted from a `customer.subscription.updated` event.
#[derive(Debug, Clone)]
struct SubscriptionUpdated {
    /// The Stripe subscription id (extracted for audit/debug; DB update uses
    /// the existing stored subscription_id).
    #[allow(dead_code)]
    subscription_id: String,
    /// The Stripe customer id.
    stripe_customer_id: String,
    /// The Stripe price id from the first subscription item, used to look up
    /// the new plan.
    stripe_price_id: Option<String>,
    /// The end of the current billing period (Unix timestamp).
    current_period_end: Option<i64>,
}

/// Extract fields from a `customer.subscription.updated` event's data object.
///
/// Returns `None` when required fields (id, customer) are missing.
/// `stripe_price_id` and `current_period_end` are optional — the event may
/// carry a period-end update without a price change.
fn extract_subscription_updated(data: &Value) -> Option<SubscriptionUpdated> {
    let subscription_id = data.get("id")?.as_str()?.to_string();
    let stripe_customer_id = data.get("customer")?.as_str()?.to_string();
    let stripe_price_id = data
        .get("items")
        .and_then(|items| items.get("data"))
        .and_then(|data_items| data_items.as_array())
        .and_then(|arr| arr.first())
        .and_then(|item| item.get("price"))
        .and_then(|price| price.get("id"))
        .and_then(|id| id.as_str())
        .map(String::from);
    let current_period_end = data.get("current_period_end").and_then(|v| v.as_i64());

    Some(SubscriptionUpdated {
        subscription_id,
        stripe_customer_id,
        stripe_price_id,
        current_period_end,
    })
}

/// Relevant fields extracted from a `customer.subscription.deleted` event.
#[derive(Debug, Clone)]
struct SubscriptionDeleted {
    /// The Stripe customer id.
    stripe_customer_id: String,
}

/// Extract fields from a `customer.subscription.deleted` event's data object.
///
/// Returns `None` when required fields are missing.
fn extract_subscription_deleted(data: &Value) -> Option<SubscriptionDeleted> {
    let stripe_customer_id = data.get("customer")?.as_str()?.to_string();
    Some(SubscriptionDeleted { stripe_customer_id })
}

/// Relevant fields extracted from an `invoice.paid` event.
#[derive(Debug, Clone)]
struct InvoicePaid {
    /// The Stripe customer id.
    stripe_customer_id: String,
    /// The end of the billing period covered by this invoice (Unix timestamp).
    period_end: Option<i64>,
}

/// Extract fields from an `invoice.paid` event's data object.
///
/// Returns `None` when required fields are missing.
fn extract_invoice_paid(data: &Value) -> Option<InvoicePaid> {
    let stripe_customer_id = data.get("customer")?.as_str()?.to_string();
    let period_end = data
        .get("lines")?
        .get("data")?
        .as_array()?
        .first()
        .and_then(|line| line.get("period")?.get("end")?.as_i64());
    Some(InvoicePaid {
        stripe_customer_id,
        period_end,
    })
}

/// Relevant fields extracted from an `invoice.payment_failed` event.
#[derive(Debug, Clone)]
struct InvoicePaymentFailed {
    /// The Stripe customer id.
    stripe_customer_id: String,
}

/// Extract fields from an `invoice.payment_failed` event's data object.
///
/// Returns `None` when required fields are missing.
fn extract_invoice_payment_failed(data: &Value) -> Option<InvoicePaymentFailed> {
    let stripe_customer_id = data.get("customer")?.as_str()?.to_string();
    Some(InvoicePaymentFailed { stripe_customer_id })
}

// ── Event handling (dispatches to PgStore) ─────────────────────────────────────

/// Handle a parsed Stripe event by dispatching to the appropriate handler.
///
/// # Idempotency
///
/// The caller must call [`PgStore::record_stripe_event`] before calling this
/// function — the event_id check is the idempotency gate. This function assumes
/// it is processing a **new** event.
async fn handle_stripe_event(
    event: &StripeEvent,
    pg_store: &kb_store::PgStore,
) -> anyhow::Result<()> {
    match event.event_type.as_str() {
        "checkout.session.completed" => {
            let cs = match extract_checkout_session(&event.data_object) {
                Some(cs) => cs,
                None => anyhow::bail!("failed to parse checkout.session.completed event"),
            };

            let plan = pg_store
                .get_plan_by_code(&cs.plan_code)
                .await?
                .ok_or_else(|| anyhow::anyhow!("unknown plan code: {}", cs.plan_code))?;

            pg_store
                .update_tenant_plan(
                    cs.tenant_id,
                    Some(plan.id),
                    Some(&cs.stripe_customer_id),
                    Some(&cs.subscription_id),
                    Some("active"),
                    None,
                )
                .await?;

            // Apply the plan's quotas to the tenant immediately.
            pg_store
                .apply_plan_quotas_to_tenant(cs.tenant_id, plan.id, 0)
                .await?;

            tracing::info!(
                tenant_id = cs.tenant_id,
                plan_code = %cs.plan_code,
                customer = %cs.stripe_customer_id,
                subscription = %cs.subscription_id,
                "subscription activated via checkout.session.completed"
            );

            Ok(())
        }

        "customer.subscription.updated" => {
            let su = match extract_subscription_updated(&event.data_object) {
                Some(su) => su,
                None => anyhow::bail!("failed to parse customer.subscription.updated event"),
            };

            let tenant_id = pg_store
                .find_tenant_by_stripe_customer(&su.stripe_customer_id)
                .await?
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "no tenant found for stripe customer: {}",
                        su.stripe_customer_id
                    )
                })?;

            // If the subscription has a new price, look up the plan and update.
            if let Some(ref price_id) = su.stripe_price_id {
                let plan = pg_store
                    .find_plan_by_stripe_price(price_id)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("unknown stripe price id: {price_id}"))?;

                let period_end = su
                    .current_period_end
                    .map(|ts| chrono::DateTime::from_timestamp(ts, 0).unwrap_or_default());

                pg_store
                    .update_tenant_plan(tenant_id, Some(plan.id), None, None, None, period_end)
                    .await?;

                // Apply the plan's quotas to the tenant immediately (upgrade/downgrade).
                pg_store
                    .apply_plan_quotas_to_tenant(tenant_id, plan.id, 0)
                    .await?;

                tracing::info!(
                    tenant_id,
                    new_plan = %plan.code,
                    price_id = %price_id,
                    "subscription plan changed via customer.subscription.updated"
                );
            } else if let Some(period_end_ts) = su.current_period_end {
                // No price change — just update the period end.
                let period_end =
                    chrono::DateTime::from_timestamp(period_end_ts, 0).unwrap_or_default();

                pg_store
                    .update_tenant_plan(tenant_id, None, None, None, None, Some(period_end))
                    .await?;
            }

            Ok(())
        }

        "customer.subscription.deleted" => {
            let sd = match extract_subscription_deleted(&event.data_object) {
                Some(sd) => sd,
                None => anyhow::bail!("failed to parse customer.subscription.deleted event"),
            };

            let tenant_id = pg_store
                .find_tenant_by_stripe_customer(&sd.stripe_customer_id)
                .await?
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "no tenant found for stripe customer: {}",
                        sd.stripe_customer_id
                    )
                })?;

            // Revert to the free plan.
            let free_plan = pg_store
                .get_plan_by_code("free")
                .await?
                .ok_or_else(|| anyhow::anyhow!("free plan not found in database"))?;

            pg_store
                .update_tenant_plan(
                    tenant_id,
                    Some(free_plan.id),
                    None,
                    None,
                    Some("canceled"),
                    None,
                )
                .await?;

            // Apply the free plan's quotas immediately.
            pg_store
                .apply_plan_quotas_to_tenant(tenant_id, free_plan.id, 0)
                .await?;

            tracing::info!(
                tenant_id,
                customer = %sd.stripe_customer_id,
                "subscription canceled — reverted to free plan"
            );

            Ok(())
        }

        "invoice.paid" => {
            let ip = match extract_invoice_paid(&event.data_object) {
                Some(ip) => ip,
                None => anyhow::bail!("failed to parse invoice.paid event"),
            };

            let tenant_id = pg_store
                .find_tenant_by_stripe_customer(&ip.stripe_customer_id)
                .await?
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "no tenant found for stripe customer: {}",
                        ip.stripe_customer_id
                    )
                })?;

            if let Some(period_end_ts) = ip.period_end {
                let period_end =
                    chrono::DateTime::from_timestamp(period_end_ts, 0).unwrap_or_default();

                pg_store
                    .update_tenant_plan(tenant_id, None, None, None, None, Some(period_end))
                    .await?;

                tracing::info!(
                    tenant_id,
                    %period_end,
                    "invoice paid — current_period_end updated"
                );
            }

            Ok(())
        }

        "invoice.payment_failed" => {
            let ipf = match extract_invoice_payment_failed(&event.data_object) {
                Some(ipf) => ipf,
                None => anyhow::bail!("failed to parse invoice.payment_failed event"),
            };

            let tenant_id = pg_store
                .find_tenant_by_stripe_customer(&ipf.stripe_customer_id)
                .await?
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "no tenant found for stripe customer: {}",
                        ipf.stripe_customer_id
                    )
                })?;

            pg_store
                .update_tenant_plan(tenant_id, None, None, None, Some("past_due"), None)
                .await?;

            tracing::warn!(
                tenant_id,
                customer = %ipf.stripe_customer_id,
                "invoice payment failed — billing status set to past_due"
            );

            Ok(())
        }

        other => {
            // Unknown event types are acknowledged but not processed —
            // Stripe may add new event types in the future. We log them
            // so the operator can decide whether to add support.
            tracing::debug!(
                event_type = other,
                event_id = %event.id,
                "unhandled stripe event type acknowledged (200 OK, no action)"
            );
            Ok(())
        }
    }
}

// ── Axum handler ───────────────────────────────────────────────────────────────

/// `POST /stripe/webhook` — idempotent, signature-verified Stripe webhook
/// endpoint (plan §29, P11-T3).
///
/// # Signature verification
///
/// The handler verifies the `stripe-signature` header against the configured
/// webhook signing secret. Invalid signatures are rejected with `401 Unauthorized`.
///
/// # Idempotency
///
/// Processed event IDs are stored in the `stripe_events` table. Duplicate deliveries
/// return `200 OK` without side effects.
///
/// # Response
///
/// * `200 OK` — webhook processed (or idempotently skipped).
/// * `400 Bad Request` — unreadable body, missing event id/type, or unparseable
///   event data.
/// * `401 Unauthorized` — missing, malformed, or invalid `stripe-signature`
///   header, or a present signature that cannot be verified because no secret is
///   configured (a forged/unverifiable signature is rejected, never processed).
/// * `500 Internal Server Error` — webhook secret not configured *and no
///   signature was supplied* (a pure misconfiguration), database error, or event
///   processing failure.
///
/// This endpoint is **public** (no auth middleware) — Stripe calls it directly.
pub async fn post_webhook(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // ── Extract the Stripe signature header ─────────────────────────────────
    // Read it first so the "secret not configured" branch can tell a
    // misconfigured server (no signature to judge → 500) apart from an
    // unverifiable — and therefore rejected — webhook (signature present but no
    // secret to check it against).
    let signature_header = headers
        .get("stripe-signature")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    // ── Resolve the webhook secret ──────────────────────────────────────────
    // The secret is read per call via `ArcSwap::load_full`, so operator
    // rotation takes effect without a restart (hot-swappable config rule).
    let secret_guard = match &state.stripe_webhook_secret {
        Some(swappable) => swappable.load_full(),
        None => {
            // Without a secret we cannot verify any signature. A request that
            // still carries a `stripe-signature` is, from our side,
            // unverifiable — i.e. a forged signature — and MUST be rejected
            // (401) and never processed, never surfaced as a server error. A
            // request with no signature at all is a pure misconfiguration (500).
            if signature_header.is_empty() {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "webhook secret not configured",
                )
                    .into_response();
            }
            return (StatusCode::UNAUTHORIZED, "invalid signature").into_response();
        }
    };
    // secret_guard is Arc<String>, which derefs to String → .as_str() gives &str.
    let secret: &str = secret_guard.as_str();

    // ── Verify the Stripe signature ─────────────────────────────────────────
    if !verify_stripe_signature(&body, signature_header, secret) {
        return (StatusCode::UNAUTHORIZED, "invalid signature").into_response();
    }

    // ── Parse the event ─────────────────────────────────────────────────────
    let event = match parse_stripe_event(&body) {
        Some(ev) => ev,
        None => {
            return (StatusCode::BAD_REQUEST, "failed to parse stripe event body").into_response();
        }
    };

    // ── Idempotency gate: skip already-processed events ─────────────────────
    match state
        .pg_store
        .record_stripe_event(&event.id, &event.event_type)
        .await
    {
        Ok(false) => {
            // Already processed — idempotent skip.
            tracing::debug!(
                event_id = %event.id,
                event_type = %event.event_type,
                "duplicate stripe event skipped (idempotent)"
            );
            return StatusCode::OK.into_response();
        }
        Ok(true) => {
            // New event — proceed with processing.
        }
        Err(e) => {
            tracing::error!(
                error = %e,
                event_id = %event.id,
                "failed to record stripe event for idempotency"
            );
            return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response();
        }
    }

    // ── Dispatch to the event handler ───────────────────────────────────────
    if let Err(e) = handle_stripe_event(&event, &state.pg_store).await {
        tracing::error!(
            error = %e,
            event_id = %event.id,
            event_type = %event.event_type,
            "stripe event processing failed"
        );
        return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response();
    }

    StatusCode::OK.into_response()
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use axum::Router;
    use axum::body::Body;
    use axum::http::{Method, StatusCode};
    use axum::routing::post;
    use kb_store::PgStore;
    use kb_store::session_store::InMemorySessionStore;
    use tower::ServiceExt;

    use super::*;

    // ── Helpers ────────────────────────────────────────────────────────────

    /// Build a test `AppState` with a webhook secret for signature verification
    /// tests.
    fn test_state(webhook_secret: &str) -> Arc<AppState> {
        let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
        let session_store: Arc<dyn kb_core::session::SessionStore> =
            Arc::new(InMemorySessionStore::new());
        Arc::new(
            AppState::new(
                session_store,
                pg_store,
                Some(Duration::from_secs(3600)),
                false,
            )
            .with_stripe_webhook_secret(webhook_secret),
        )
    }

    /// Build a minimal router with only the webhook endpoint.
    fn webhook_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/stripe/webhook", post(post_webhook))
            .with_state(state)
    }

    // ── Signature verification unit tests ──────────────────────────────────

    /// A valid signature verifies correctly against a known payload.
    #[test]
    fn verify_valid_signature() {
        let secret = "whsec_test_secret";
        let payload = br#"{"id":"evt_123","type":"invoice.paid"}"#;
        let timestamp = "1687988800";

        // Compute the expected signature.
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(timestamp.as_bytes());
        mac.update(b".");
        mac.update(payload);
        let sig_hex = hex::encode(mac.finalize().into_bytes());

        let header = format!("t={timestamp},v1={sig_hex}");
        assert!(
            verify_stripe_signature(payload, &header, secret),
            "valid signature must verify"
        );
    }

    /// A signature computed with a different secret is rejected.
    #[test]
    fn verify_rejects_wrong_secret() {
        let secret = "whsec_test_secret";
        let wrong_secret = "whsec_wrong_secret";
        let payload = br#"{"id":"evt_123"}"#;
        let timestamp = "1687988800";

        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(timestamp.as_bytes());
        mac.update(b".");
        mac.update(payload);
        let sig_hex = hex::encode(mac.finalize().into_bytes());

        let header = format!("t={timestamp},v1={sig_hex}");
        assert!(
            !verify_stripe_signature(payload, &header, wrong_secret),
            "wrong secret must be rejected"
        );
    }

    /// A tampered payload is rejected.
    #[test]
    fn verify_rejects_tampered_payload() {
        let secret = "whsec_test_secret";
        let original = br#"{"id":"evt_123","type":"invoice.paid"}"#;
        let tampered = br#"{"id":"evt_123","type":"invoice.paid","hacked":true}"#;
        let timestamp = "1687988800";

        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(timestamp.as_bytes());
        mac.update(b".");
        mac.update(original);
        let sig_hex = hex::encode(mac.finalize().into_bytes());

        let header = format!("t={timestamp},v1={sig_hex}");
        assert!(
            !verify_stripe_signature(tampered, &header, secret),
            "tampered payload must be rejected"
        );
    }

    /// Missing signature header returns false.
    #[test]
    fn verify_rejects_empty_header() {
        let secret = "whsec_test_secret";
        let payload = br#"{"id":"evt_123"}"#;
        assert!(!verify_stripe_signature(payload, "", secret));
    }

    /// Malformed signature header (missing v1) returns false.
    #[test]
    fn verify_rejects_missing_v1() {
        let secret = "whsec_test_secret";
        let payload = br#"{"id":"evt_123"}"#;
        let header = "t=1687988800";
        assert!(
            !verify_stripe_signature(payload, header, secret),
            "header without v1 must be rejected"
        );
    }

    /// Non-hex v1 signature returns false.
    #[test]
    fn verify_rejects_non_hex_signature() {
        let secret = "whsec_test_secret";
        let payload = br#"{"id":"evt_123"}"#;
        let header = "t=1687988800,v1=not_hex_$$$";
        assert!(
            !verify_stripe_signature(payload, header, secret),
            "non-hex signature must be rejected"
        );
    }

    // ── Event parsing unit tests ───────────────────────────────────────────

    /// A well-formed event parses correctly.
    #[test]
    fn parse_valid_event() {
        let payload = r#"{
            "id": "evt_1AbcDef",
            "type": "checkout.session.completed",
            "data": {
                "object": {
                    "client_reference_id": "42",
                    "customer": "cus_xyz",
                    "subscription": "sub_abc",
                    "metadata": {
                        "plan_code": "pro"
                    }
                }
            }
        }"#;

        let event = parse_stripe_event(payload.as_bytes()).expect("valid event must parse");
        assert_eq!(event.id, "evt_1AbcDef");
        assert_eq!(event.event_type, "checkout.session.completed");
        assert_eq!(
            event.data_object["client_reference_id"].as_str().unwrap(),
            "42"
        );
    }

    /// Missing id field returns None.
    #[test]
    fn parse_event_missing_id() {
        let payload = r#"{"type": "invoice.paid", "data": {"object": {}}}"#;
        assert!(
            parse_stripe_event(payload.as_bytes()).is_none(),
            "missing id must return None"
        );
    }

    /// Missing type field returns None.
    #[test]
    fn parse_event_missing_type() {
        let payload = r#"{"id": "evt_123", "data": {"object": {}}}"#;
        assert!(
            parse_stripe_event(payload.as_bytes()).is_none(),
            "missing type must return None"
        );
    }

    /// Non-JSON input returns None.
    #[test]
    fn parse_event_invalid_json() {
        let payload = b"not valid json!";
        assert!(
            parse_stripe_event(payload).is_none(),
            "non-json input must return None"
        );
    }

    /// Empty body returns None.
    #[test]
    fn parse_event_empty_body() {
        assert!(
            parse_stripe_event(b"").is_none(),
            "empty body must return None"
        );
    }

    // ── Event extraction unit tests ────────────────────────────────────────

    /// Extract checkout.session.completed fields from a valid data object.
    #[test]
    fn extract_checkout_session_valid() {
        let data = serde_json::json!({
            "client_reference_id": "7",
            "customer": "cus_checkout_cust",
            "subscription": "sub_checkout_sub",
            "metadata": {
                "plan_code": "pro",
                "tenant_id": "7"
            }
        });

        let cs = extract_checkout_session(&data).expect("valid checkout session must parse");
        assert_eq!(cs.tenant_id, 7);
        assert_eq!(cs.stripe_customer_id, "cus_checkout_cust");
        assert_eq!(cs.subscription_id, "sub_checkout_sub");
        assert_eq!(cs.plan_code, "pro");
    }

    /// Missing plan_code in metadata returns None.
    #[test]
    fn extract_checkout_session_missing_plan_code() {
        let data = serde_json::json!({
            "client_reference_id": "7",
            "customer": "cus_x",
            "subscription": "sub_x",
            "metadata": {"tenant_id": "7"}
        });

        assert!(
            extract_checkout_session(&data).is_none(),
            "missing plan_code must return None"
        );
    }

    /// Non-numeric client_reference_id returns None.
    #[test]
    fn extract_checkout_session_invalid_tenant_id() {
        let data = serde_json::json!({
            "client_reference_id": "not_a_number",
            "customer": "cus_x",
            "subscription": "sub_x",
            "metadata": {"plan_code": "pro"}
        });

        assert!(
            extract_checkout_session(&data).is_none(),
            "invalid tenant id must return None"
        );
    }

    /// Extract subscription.updated fields from a valid data object.
    #[test]
    fn extract_subscription_updated_valid() {
        let data = serde_json::json!({
            "id": "sub_xyz",
            "customer": "cus_sub_cust",
            "current_period_end": 1690000000,
            "items": {
                "data": [
                    {"price": {"id": "price_team_monthly"}}
                ]
            }
        });

        let su =
            extract_subscription_updated(&data).expect("valid subscription updated must parse");
        assert_eq!(su.subscription_id, "sub_xyz");
        assert_eq!(su.stripe_customer_id, "cus_sub_cust");
        assert_eq!(su.stripe_price_id.as_deref(), Some("price_team_monthly"));
        assert_eq!(su.current_period_end, Some(1690000000));
    }

    /// Subscription with empty items array — price is None but parsing still
    /// succeeds.
    #[test]
    fn extract_subscription_updated_empty_items() {
        let data = serde_json::json!({
            "id": "sub_xyz",
            "customer": "cus_sub_cust",
            "current_period_end": 1690000000,
            "items": {"data": []}
        });

        let su = extract_subscription_updated(&data).expect("parse must succeed");
        assert!(su.stripe_price_id.is_none());
    }

    /// Extract subscription.deleted fields.
    #[test]
    fn extract_subscription_deleted_valid() {
        let data = serde_json::json!({
            "id": "sub_canceled",
            "customer": "cus_cancel_cust",
            "status": "canceled"
        });

        let sd = extract_subscription_deleted(&data).expect("valid deleted event must parse");
        assert_eq!(sd.stripe_customer_id, "cus_cancel_cust");
    }

    /// Extract invoice.paid fields with lines.
    #[test]
    fn extract_invoice_paid_valid() {
        let data = serde_json::json!({
            "customer": "cus_inv_cust",
            "lines": {
                "data": [
                    {"period": {"end": 1690100000}}
                ]
            }
        });

        let ip = extract_invoice_paid(&data).expect("valid invoice.paid must parse");
        assert_eq!(ip.stripe_customer_id, "cus_inv_cust");
        assert_eq!(ip.period_end, Some(1690100000));
    }

    /// Invoice without lines — period_end is None.
    #[test]
    fn extract_invoice_paid_no_lines() {
        let data = serde_json::json!({
            "customer": "cus_inv_cust",
            "lines": {"data": []}
        });

        let ip = extract_invoice_paid(&data).expect("parse must succeed");
        assert!(ip.period_end.is_none());
    }

    /// Extract invoice.payment_failed fields.
    #[test]
    fn extract_invoice_payment_failed_valid() {
        let data = serde_json::json!({
            "customer": "cus_pf_cust"
        });

        let ipf =
            extract_invoice_payment_failed(&data).expect("valid payment_failed event must parse");
        assert_eq!(ipf.stripe_customer_id, "cus_pf_cust");
    }

    // ── Webhook endpoint integration tests ─────────────────────────────────

    /// Helper: create a signed Stripe webhook request body + headers.
    fn sign_payload<'a>(payload: &'a str, secret: &'a str) -> (String, Vec<(&'a str, &'a str)>) {
        let timestamp = "1687988800";
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(timestamp.as_bytes());
        mac.update(b".");
        mac.update(payload.as_bytes());
        let sig_hex = hex::encode(mac.finalize().into_bytes());
        let header = format!("t={timestamp},v1={sig_hex}");
        (header, vec![("content-type", "application/json")])
    }

    /// A request without a Stripe-Signature header is rejected with 401.
    #[tokio::test]
    async fn webhook_no_signature_header_returns_401() {
        let state = test_state("whsec_test");
        let router = webhook_router(state);

        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/stripe/webhook")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"id":"evt_123","type":"invoice.paid","data":{"object":{}}}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    /// An invalid signature is rejected with 401.
    #[tokio::test]
    async fn webhook_invalid_signature_returns_401() {
        let state = test_state("whsec_test");
        let router = webhook_router(state);

        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/stripe/webhook")
            .header("content-type", "application/json")
            .header("stripe-signature", "t=123,v1=badbadbad")
            .body(Body::from(
                r#"{"id":"evt_123","type":"invoice.paid","data":{"object":{}}}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    /// A valid signature but unparseable event body returns 400.
    #[tokio::test]
    async fn webhook_valid_signature_bad_body_returns_400() {
        let secret = "whsec_test";
        let state = test_state(secret);
        let router = webhook_router(state);

        let payload = "not-json";
        let (sig_header, _) = sign_payload(payload, secret);

        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/stripe/webhook")
            .header("content-type", "application/json")
            .header("stripe-signature", &sig_header)
            .body(Body::from(payload))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    /// A webhook with a valid signature and event body hits the idempotency
    /// gate, which fails because the DB is unreachable — resulting in 500.
    /// This confirms the handler reaches the DB step (signature + parse pass).
    #[tokio::test]
    async fn webhook_valid_event_reaches_db() {
        let secret = "whsec_test";
        let state = test_state(secret);
        let router = webhook_router(state);

        let payload = r#"{
            "id": "evt_test_reach",
            "type": "checkout.session.completed",
            "data": {
                "object": {
                    "client_reference_id": "1",
                    "customer": "cus_x",
                    "subscription": "sub_x",
                    "metadata": {"plan_code": "pro"}
                }
            }
        }"#;
        let (sig_header, _) = sign_payload(payload, secret);

        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/stripe/webhook")
            .header("content-type", "application/json")
            .header("stripe-signature", &sig_header)
            .body(Body::from(payload))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        // DB is unreachable → record_stripe_event fails → 500.
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    /// The webhook endpoint is public — no auth required.
    #[tokio::test]
    async fn webhook_no_auth_required() {
        let secret = "whsec_test";
        let state = test_state(secret);
        let router = webhook_router(state);

        let payload = "not-json";
        let (sig_header, _) = sign_payload(payload, secret);

        // No cookie, no auth header — the handler should still run (returns
        // 400 because the body is bad, NOT 401 from auth middleware).
        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/stripe/webhook")
            .header("content-type", "application/json")
            .header("stripe-signature", &sig_header)
            .body(Body::from(payload))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        // Must NOT be 401 (that would be auth middleware, which isn't on this route).
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    /// When the webhook secret is not configured, the handler returns 500.
    #[tokio::test]
    async fn webhook_no_secret_configured_returns_500() {
        let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
        let session_store: Arc<dyn kb_core::session::SessionStore> =
            Arc::new(InMemorySessionStore::new());
        let state = Arc::new(AppState::new(
            session_store,
            pg_store,
            Some(Duration::from_secs(3600)),
            false,
        ));
        let router = Router::new()
            .route("/stripe/webhook", post(post_webhook))
            .with_state(state);

        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/stripe/webhook")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    /// When the webhook secret is not configured but the request still carries a
    /// signature, the handler cannot verify it and must reject it (401) — a
    /// forged/unverifiable signature is never surfaced as a 500 nor processed.
    /// Mirrors the e2e `test_webhook_invalid_signature_rejected` contract, where
    /// the app runs without `STRIPE_WEBHOOK_SECRET`.
    #[tokio::test]
    async fn webhook_no_secret_with_signature_returns_401() {
        let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
        let session_store: Arc<dyn kb_core::session::SessionStore> =
            Arc::new(InMemorySessionStore::new());
        let state = Arc::new(AppState::new(
            session_store,
            pg_store,
            Some(Duration::from_secs(3600)),
            false,
        ));
        let router = Router::new()
            .route("/stripe/webhook", post(post_webhook))
            .with_state(state);

        let forged = format!("t=1700000000,v1={}", "0".repeat(64));
        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/stripe/webhook")
            .header("content-type", "application/json")
            .header("stripe-signature", forged)
            .body(Body::from(
                r#"{"id":"evt_123","type":"invoice.paid","data":{"object":{"customer":"cus_forged"}}}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        // No secret to verify against → forged signature rejected, never a 500.
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
