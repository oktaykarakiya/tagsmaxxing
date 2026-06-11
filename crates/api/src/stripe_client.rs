// SPDX-License-Identifier: AGPL-3.0-or-later

//! Stripe client abstraction for billing integration (plan §29, P11-T2).
//!
//! Provides the [`StripeClient`] trait for creating Stripe Checkout Sessions,
//! a real HTTP implementation using [`reqwest`], and a mock for deterministic
//! testing. The Stripe secret key is hot-swappable via
//! [`arc_swap::ArcSwap`] per the hot-swap rule in CLAUDE.md.

use std::sync::Arc;

use anyhow::Context;
use arc_swap::ArcSwap;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

// ── Request / response types ────────────────────────────────────────────────────

/// Request to create a Stripe Checkout Session for a new subscription.
///
/// The fields map to [Stripe's Checkout Session API](https://docs.stripe.com/api/checkout/sessions/create).
#[derive(Debug, Clone)]
pub struct CreateCheckoutSessionRequest {
    /// The Stripe price ID (e.g. `price_pro_monthly`) identifying the plan's
    /// recurring price object.
    pub price_id: String,
    /// Where Stripe redirects the customer's browser after successful payment.
    pub success_url: String,
    /// Where Stripe redirects the customer's browser if they cancel at Checkout.
    pub cancel_url: String,
    /// The tenant id stored as `client_reference_id` and `metadata[tenant_id]`
    /// for webhook correlation (P11-T3).
    pub tenant_id: i64,
    /// The plan code stored as `metadata[plan_code]` so the webhook handler
    /// (P11-T3) can determine which plan was purchased without expanding the
    /// subscription object.
    pub plan_code: String,
}

/// Response from a successful Stripe Checkout Session creation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreateCheckoutSessionResponse {
    /// The Stripe Checkout Session id (`cs_…`).
    pub session_id: String,
    /// The URL the customer's browser should be redirected to (Stripe-hosted
    /// Checkout page).
    pub url: String,
}

/// Request to create a Stripe Customer Portal session (plan §29, P11-T6).
///
/// The fields map to [Stripe's Customer Portal API](https://docs.stripe.com/api/customer_portal/sessions/create).
#[derive(Debug, Clone)]
pub struct CreatePortalSessionRequest {
    /// The Stripe customer id (`cus_…`).
    pub customer_id: String,
    /// Where Stripe redirects the customer's browser after they leave the portal.
    pub return_url: String,
}

/// Response from a successful Stripe Customer Portal session creation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreatePortalSessionResponse {
    /// The Stripe Customer Portal session id (`bps_…`).
    pub session_id: String,
    /// The URL the customer's browser should be redirected to (Stripe-hosted
    /// Customer Portal).
    pub url: String,
}

// ── StripeClient trait ──────────────────────────────────────────────────────────

/// Abstraction for Stripe API calls, allowing mock substitution in tests.
///
/// The real implementation calls the Stripe HTTP API; the mock returns
/// pre-configured responses so billing handlers can be tested without
/// a Stripe account.
#[async_trait]
pub trait StripeClient: Send + Sync + 'static {
    /// Create a Stripe [Checkout Session](https://docs.stripe.com/api/checkout/sessions/create)
    /// for a new subscription.
    ///
    /// # Errors
    /// Returns an error if the Stripe API call fails (network error, bad API
    /// key, invalid price ID, etc.).
    async fn create_checkout_session(
        &self,
        req: CreateCheckoutSessionRequest,
    ) -> anyhow::Result<CreateCheckoutSessionResponse>;

    /// Create a Stripe [Customer Portal](https://docs.stripe.com/api/customer_portal/sessions/create)
    /// session for managing an existing subscription.
    ///
    /// The portal lets customers update payment methods, view invoices,
    /// cancel subscriptions, and change plans (if configured in the Stripe
    /// dashboard). PCI stays at SAQ-A — card data never touches our servers.
    ///
    /// # Errors
    /// Returns an error if the Stripe API call fails (network error, bad API
    /// key, invalid customer ID, etc.).
    async fn create_customer_portal_session(
        &self,
        req: CreatePortalSessionRequest,
    ) -> anyhow::Result<CreatePortalSessionResponse>;
}

// ── Real Stripe client ──────────────────────────────────────────────────────────

/// Real Stripe API client backed by HTTP.
///
/// The secret key is stored behind an [`ArcSwap`] so it can be rotated at
/// runtime without restarting the server (CLAUDE.md hot-swappable rule).
pub struct RealStripeClient {
    /// Stripe secret key (`sk_live_…` or `sk_test_…`), hot-swappable.
    secret_key: ArcSwap<String>,
    /// Reusable HTTP client (connection pooling, timeout).
    http: reqwest::Client,
}

impl RealStripeClient {
    /// Create a new real Stripe client with the given secret key.
    ///
    /// The `http` client is expected to have a reasonable timeout configured
    /// (e.g. 30–120 s) — the Stripe API should respond quickly, but network
    /// conditions vary.
    #[must_use]
    pub fn new(secret_key: String, http: reqwest::Client) -> Self {
        Self {
            secret_key: ArcSwap::new(Arc::new(secret_key)),
            http,
        }
    }

    /// Replace the Stripe secret key at runtime (hot-swap).
    ///
    /// Subsequent calls to [`create_checkout_session`](StripeClient::create_checkout_session)
    /// and [`create_customer_portal_session`](StripeClient::create_customer_portal_session)
    /// will use the new key without restarting the server.
    pub fn set_secret_key(&self, key: String) {
        self.secret_key.store(Arc::new(key));
    }
}

#[async_trait]
impl StripeClient for RealStripeClient {
    async fn create_checkout_session(
        &self,
        req: CreateCheckoutSessionRequest,
    ) -> anyhow::Result<CreateCheckoutSessionResponse> {
        let key = self.secret_key.load();
        let key_ref: &str = key.as_ref();

        let tenant_id_str = req.tenant_id.to_string();

        let resp = self
            .http
            .post("https://api.stripe.com/v1/checkout/sessions")
            .bearer_auth(key_ref)
            .form(&[
                ("success_url", req.success_url.as_str()),
                ("cancel_url", req.cancel_url.as_str()),
                ("mode", "subscription"),
                ("line_items[0][price]", req.price_id.as_str()),
                ("line_items[0][quantity]", "1"),
                ("client_reference_id", tenant_id_str.as_str()),
                ("metadata[tenant_id]", tenant_id_str.as_str()),
                ("metadata[plan_code]", req.plan_code.as_str()),
            ])
            .send()
            .await
            .context("failed to reach Stripe API")?;

        let status = resp.status();
        let body: serde_json::Value = resp
            .json()
            .await
            .context("failed to parse Stripe response body")?;

        if !status.is_success() {
            let msg = body["error"]["message"]
                .as_str()
                .unwrap_or("unknown Stripe error");
            anyhow::bail!("Stripe error ({}): {msg}", status.as_u16());
        }

        let session_id = body["id"]
            .as_str()
            .context("Stripe response missing 'id' field")?
            .to_string();
        let url = body["url"]
            .as_str()
            .context("Stripe response missing 'url' field")?
            .to_string();

        Ok(CreateCheckoutSessionResponse { session_id, url })
    }

    async fn create_customer_portal_session(
        &self,
        req: CreatePortalSessionRequest,
    ) -> anyhow::Result<CreatePortalSessionResponse> {
        let key = self.secret_key.load();
        let key_ref: &str = key.as_ref();

        let resp = self
            .http
            .post("https://api.stripe.com/v1/billing_portal/sessions")
            .bearer_auth(key_ref)
            .form(&[
                ("customer", req.customer_id.as_str()),
                ("return_url", req.return_url.as_str()),
            ])
            .send()
            .await
            .context("failed to reach Stripe API")?;

        let status = resp.status();
        let body: serde_json::Value = resp
            .json()
            .await
            .context("failed to parse Stripe response body")?;

        if !status.is_success() {
            let msg = body["error"]["message"]
                .as_str()
                .unwrap_or("unknown Stripe error");
            anyhow::bail!("Stripe error ({}): {msg}", status.as_u16());
        }

        let session_id = body["id"]
            .as_str()
            .context("Stripe response missing 'id' field")?
            .to_string();
        let url = body["url"]
            .as_str()
            .context("Stripe response missing 'url' field")?
            .to_string();

        Ok(CreatePortalSessionResponse { session_id, url })
    }
}

// ── Mock Stripe client ──────────────────────────────────────────────────────────

/// Closure type for mock checkout session responses.
type CheckoutHandler = dyn Fn(CreateCheckoutSessionRequest) -> anyhow::Result<CreateCheckoutSessionResponse>
    + Send
    + Sync;

/// Closure type for mock portal session responses.
type PortalHandler =
    dyn Fn(CreatePortalSessionRequest) -> anyhow::Result<CreatePortalSessionResponse> + Send + Sync;

/// Mock Stripe client for deterministic testing.
///
/// The mock wraps user-supplied functions (or built-in presets) so tests can
/// simulate success, Stripe API errors, or network failures without Stripe
/// credentials.
///
/// # Examples
///
/// ```ignore
/// let client = MockStripeClient::new(); // success for both checkout and portal
/// let client = MockStripeClient::failing("stripe down"); // error for both
/// ```
pub struct MockStripeClient {
    checkout_handler: Arc<CheckoutHandler>,
    portal_handler: Arc<PortalHandler>,
}

impl MockStripeClient {
    /// Create a mock that always returns successful responses for both checkout
    /// and portal sessions.
    ///
    /// The checkout `url` embeds the requested `price_id`; the portal `url`
    /// embeds the `customer_id` — both for test assertions.
    #[must_use]
    pub fn new() -> Self {
        Self {
            checkout_handler: Arc::new(|req: CreateCheckoutSessionRequest| {
                Ok(CreateCheckoutSessionResponse {
                    session_id: "cs_test_mock_session".into(),
                    url: format!("https://checkout.stripe.com/c/pay/cs_test_{}", req.price_id),
                })
            }),
            portal_handler: Arc::new(|req: CreatePortalSessionRequest| {
                Ok(CreatePortalSessionResponse {
                    session_id: "bps_test_mock_session".into(),
                    url: format!(
                        "https://billing.stripe.com/p/session/test_{}",
                        req.customer_id
                    ),
                })
            }),
        }
    }

    /// Create a mock that always fails with the given error message for both
    /// checkout and portal methods.
    #[must_use]
    pub fn failing(message: &str) -> Self {
        let msg = message.to_string();
        let msg2 = msg.clone();
        Self {
            checkout_handler: Arc::new(move |_req: CreateCheckoutSessionRequest| {
                anyhow::bail!("{msg}")
            }),
            portal_handler: Arc::new(move |_req: CreatePortalSessionRequest| {
                anyhow::bail!("{msg2}")
            }),
        }
    }

    /// Replace the checkout handler with a custom function.
    ///
    /// This is useful for tests that need to assert on the request fields
    /// (e.g. verifying the tenant_id is passed through).
    #[must_use]
    pub fn with_checkout_handler<F>(mut self, f: F) -> Self
    where
        F: Fn(CreateCheckoutSessionRequest) -> anyhow::Result<CreateCheckoutSessionResponse>
            + Send
            + Sync
            + 'static,
    {
        self.checkout_handler = Arc::new(f);
        self
    }

    /// Replace the portal handler with a custom function.
    ///
    /// This is useful for tests that need to assert on the request fields
    /// (e.g. verifying the customer_id is passed through).
    #[must_use]
    pub fn with_portal_handler<F>(mut self, f: F) -> Self
    where
        F: Fn(CreatePortalSessionRequest) -> anyhow::Result<CreatePortalSessionResponse>
            + Send
            + Sync
            + 'static,
    {
        self.portal_handler = Arc::new(f);
        self
    }
}

impl Default for MockStripeClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StripeClient for MockStripeClient {
    async fn create_checkout_session(
        &self,
        req: CreateCheckoutSessionRequest,
    ) -> anyhow::Result<CreateCheckoutSessionResponse> {
        (self.checkout_handler)(req)
    }

    async fn create_customer_portal_session(
        &self,
        req: CreatePortalSessionRequest,
    ) -> anyhow::Result<CreatePortalSessionResponse> {
        (self.portal_handler)(req)
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // ── MockStripeClient tests ───────────────────────────────────────────────

    /// The default mock always returns a successful response with a URL that
    /// contains the price_id.
    #[tokio::test]
    async fn mock_new_returns_success() {
        let client = MockStripeClient::new();
        let resp = client
            .create_checkout_session(CreateCheckoutSessionRequest {
                price_id: "price_pro".into(),
                success_url: "http://localhost:9999/billing/success".into(),
                cancel_url: "http://localhost:9999/billing/cancel".into(),
                tenant_id: 1,
                plan_code: "pro".into(),
            })
            .await
            .expect("mock should succeed");
        assert!(resp.url.contains("price_pro"));
        assert!(resp.url.starts_with("https://checkout.stripe.com/"));
        assert!(!resp.session_id.is_empty());
    }

    /// `MockStripeClient::failing` returns an error on every call.
    #[tokio::test]
    async fn mock_failing_returns_error() {
        let client = MockStripeClient::failing("stripe api unavailable");
        let err = client
            .create_checkout_session(CreateCheckoutSessionRequest {
                price_id: "price_pro".into(),
                success_url: "http://localhost:9999/billing/success".into(),
                cancel_url: "http://localhost:9999/billing/cancel".into(),
                tenant_id: 1,
                plan_code: "pro".into(),
            })
            .await
            .expect_err("mock should fail");
        assert!(
            err.to_string().contains("stripe api unavailable"),
            "error message should match: {err}"
        );
    }

    /// The mock preserves request fields so tests can assert on them
    /// (e.g. via a custom checkout handler).
    #[tokio::test]
    async fn mock_custom_handler_preserves_tenant_id() {
        use std::sync::Mutex;
        let seen_tenant = Arc::new(Mutex::new(0_i64));
        let seen = Arc::clone(&seen_tenant);
        let client = MockStripeClient::new().with_checkout_handler(
            move |req: CreateCheckoutSessionRequest| {
                *seen.lock().unwrap() = req.tenant_id;
                Ok(CreateCheckoutSessionResponse {
                    session_id: "cs_custom".into(),
                    url: "https://checkout.stripe.com/c/pay/cs_custom".into(),
                })
            },
        );
        client
            .create_checkout_session(CreateCheckoutSessionRequest {
                price_id: "price_team".into(),
                success_url: "http://localhost:9999/billing/success".into(),
                cancel_url: "http://localhost:9999/billing/cancel".into(),
                tenant_id: 42,
                plan_code: "team".into(),
            })
            .await
            .expect("mock should succeed");
        assert_eq!(*seen_tenant.lock().unwrap(), 42);
    }

    // ── CreateCheckoutSessionResponse serialization ──────────────────────────

    #[test]
    fn checkout_response_serializes_correctly() {
        let resp = CreateCheckoutSessionResponse {
            session_id: "cs_test_abc".into(),
            url: "https://checkout.stripe.com/c/pay/cs_test_abc".into(),
        };
        let json = serde_json::to_string(&resp).expect("serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("parse");
        assert_eq!(parsed["session_id"], "cs_test_abc");
        assert_eq!(
            parsed["url"],
            "https://checkout.stripe.com/c/pay/cs_test_abc"
        );
    }

    #[test]
    fn checkout_response_deserializes_correctly() {
        let json =
            r#"{"session_id":"cs_test_xyz","url":"https://checkout.stripe.com/c/pay/cs_test_xyz"}"#;
        let resp: CreateCheckoutSessionResponse = serde_json::from_str(json).expect("deserialize");
        assert_eq!(resp.session_id, "cs_test_xyz");
        assert_eq!(resp.url, "https://checkout.stripe.com/c/pay/cs_test_xyz");
    }

    // ── CreatePortalSessionResponse serialization ──────────────────────────────

    #[test]
    fn portal_response_serializes_correctly() {
        let resp = CreatePortalSessionResponse {
            session_id: "bps_test_abc".into(),
            url: "https://billing.stripe.com/p/session/test_bps_abc".into(),
        };
        let json = serde_json::to_string(&resp).expect("serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("parse");
        assert_eq!(parsed["session_id"], "bps_test_abc");
        assert_eq!(
            parsed["url"],
            "https://billing.stripe.com/p/session/test_bps_abc"
        );
    }

    #[test]
    fn portal_response_deserializes_correctly() {
        let json = r#"{"session_id":"bps_test_xyz","url":"https://billing.stripe.com/p/session/test_bps_xyz"}"#;
        let resp: CreatePortalSessionResponse = serde_json::from_str(json).expect("deserialize");
        assert_eq!(resp.session_id, "bps_test_xyz");
        assert_eq!(
            resp.url,
            "https://billing.stripe.com/p/session/test_bps_xyz"
        );
    }

    // ── Mock portal session tests ──────────────────────────────────────────────

    /// The default mock returns a successful portal response with a URL that
    /// contains the customer_id.
    #[tokio::test]
    async fn mock_portal_new_returns_success() {
        let client = MockStripeClient::new();
        let resp = client
            .create_customer_portal_session(CreatePortalSessionRequest {
                customer_id: "cus_test123".into(),
                return_url: "http://localhost:9999/account".into(),
            })
            .await
            .expect("mock portal should succeed");
        assert!(resp.url.contains("cus_test123"));
        assert!(resp.url.starts_with("https://billing.stripe.com/"));
        assert!(!resp.session_id.is_empty());
    }

    /// `MockStripeClient::failing` returns an error on the portal method too.
    #[tokio::test]
    async fn mock_failing_returns_error_on_portal() {
        let client = MockStripeClient::failing("stripe api unavailable");
        let err = client
            .create_customer_portal_session(CreatePortalSessionRequest {
                customer_id: "cus_test123".into(),
                return_url: "http://localhost:9999/account".into(),
            })
            .await
            .expect_err("mock portal should fail");
        assert!(
            err.to_string().contains("stripe api unavailable"),
            "error message should match: {err}"
        );
    }

    /// The mock preserves portal request fields so tests can assert on them.
    #[tokio::test]
    async fn mock_custom_portal_handler_preserves_customer_id() {
        use std::sync::Mutex;
        let seen_customer = Arc::new(Mutex::new(String::new()));
        let seen = Arc::clone(&seen_customer);
        let client =
            MockStripeClient::new().with_portal_handler(move |req: CreatePortalSessionRequest| {
                *seen.lock().unwrap() = req.customer_id.clone();
                Ok(CreatePortalSessionResponse {
                    session_id: "bps_custom".into(),
                    url: "https://billing.stripe.com/p/session/test_bps_custom".into(),
                })
            });
        client
            .create_customer_portal_session(CreatePortalSessionRequest {
                customer_id: "cus_42".into(),
                return_url: "http://localhost:9999/account".into(),
            })
            .await
            .expect("mock portal should succeed");
        assert_eq!(*seen_customer.lock().unwrap(), "cus_42");
    }
}
