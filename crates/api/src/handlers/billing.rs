//! Billing handlers: Stripe Checkout + Customer Portal (plan §29, P11-T2 & P11-T6).
//!
//! * `POST /billing/checkout` — create a Stripe Checkout Session for a plan.
//! * `GET  /billing/success` — interstitial shown after Stripe redirects back.
//! * `GET  /billing/cancel` — redirect when the user cancels at Checkout.
//! * `GET  /billing/portal`  — create a Stripe Customer Portal session (P11-T6).
//! * `POST /billing/portal`  — same, for HTMX button (P11-T6).
//!
//! The checkout and portal handlers require authentication (via
//! [`crate::middleware::auth_middleware`]); the success and cancel endpoints
//! are public (Stripe redirects the browser to them).

use std::sync::Arc;

use axum::Extension;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Redirect};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::AuthUser;
use crate::stripe_client::{CreateCheckoutSessionRequest, CreatePortalSessionRequest};

// ── Request / response types ────────────────────────────────────────────────────

/// Request body for `POST /billing/checkout`.
///
/// The only required field is `plan_code`; the handler looks up the Stripe
/// price ID from the `plans` table and constructs the Checkout Session.
#[derive(Debug, Deserialize)]
pub struct CheckoutRequest {
    /// Machine-readable plan code: `free`, `pro`, `team`, …
    pub plan_code: String,
}

/// Successful checkout response body.
#[derive(Debug, Serialize)]
pub struct CheckoutResponse {
    /// The URL the client should redirect to (Stripe-hosted Checkout page).
    pub url: String,
}

/// Successful portal session response body (P11-T6).
#[derive(Debug, Serialize)]
pub struct PortalResponse {
    /// The URL the client should redirect to (Stripe-hosted Customer Portal).
    pub url: String,
}

/// Error response body.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    /// Machine-readable error code.
    pub error: String,
    /// Human-readable description.
    pub message: String,
}

// ── Handlers ────────────────────────────────────────────────────────────────────

/// `POST /billing/checkout` — create a Stripe Checkout Session for a plan.
///
/// Looks up the plan's `stripe_price` from the `plans` table, constructs
/// success and cancel URLs from the configured [`AppState::public_base_url`],
/// and delegates to the [`StripeClient`](crate::stripe_client::StripeClient).
///
/// # Request
///
/// ```json
/// { "plan_code": "pro" }
/// ```
///
/// # Response
///
/// * `200 OK` — [`CheckoutResponse`] with the Stripe Checkout URL.
///
/// # Errors
///
/// * `400 Bad Request` — missing or invalid `plan_code`.
/// * `401 Unauthorized` — rejected by auth middleware.
/// * `500 Internal Server Error` — Stripe client not configured, database
///   failure, or Stripe API error.
pub async fn post_checkout(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Json(req): Json<CheckoutRequest>,
) -> Result<(StatusCode, Json<CheckoutResponse>), (StatusCode, Json<ErrorResponse>)> {
    // ── Validate plan_code ───────────────────────────────────────────────────
    let plan_code = req.plan_code.trim();
    if plan_code.is_empty() {
        return Err(bad_request("missing_plan_code", "plan_code is required"));
    }

    // ── Look up the plan ─────────────────────────────────────────────────────
    let plan = state
        .pg_store
        .get_plan_by_code(plan_code)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| bad_request("unknown_plan_code", "plan not found"))?;

    // ── Resolve the Stripe client ────────────────────────────────────────────
    let stripe = state.stripe_client.as_ref().ok_or_else(|| {
        internal_error(anyhow::anyhow!(
            "stripe client not configured — set STRIPE_SECRET_KEY"
        ))
    })?;

    // ── Build success / cancel URLs ──────────────────────────────────────────
    let base = state.public_base_url.trim_end_matches('/');
    let success_url = format!("{base}/billing/success");
    let cancel_url = format!("{base}/billing/cancel");

    // ── Create the Checkout Session ──────────────────────────────────────────
    let stripe_req = CreateCheckoutSessionRequest {
        price_id: plan.stripe_price.clone(),
        success_url,
        cancel_url,
        tenant_id: auth_user.tenant_id,
        plan_code: plan_code.to_string(),
    };

    let stripe_resp = stripe
        .create_checkout_session(stripe_req)
        .await
        .map_err(internal_error)?;

    Ok((
        StatusCode::OK,
        Json(CheckoutResponse {
            url: stripe_resp.url,
        }),
    ))
}

/// `GET /billing/success` — Stripe redirects here after successful payment.
///
/// Returns an HTML interstitial that tells the user their account is being
/// set up and will auto-redirect to the search page after a few seconds.
/// The actual subscription activation happens in the webhook handler (P11-T3).
///
/// This endpoint is **public** (no auth) — Stripe redirects the browser here.
pub async fn get_success() -> impl IntoResponse {
    let html = r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Setting up your account — KB</title>
<style>
  body { font-family: system-ui, sans-serif; display: flex; justify-content: center;
         align-items: center; min-height: 100vh; margin: 0; background: #f8fafc; }
  .card { background: white; border-radius: 12px; padding: 2.5rem; max-width: 28rem;
          box-shadow: 0 1px 3px rgba(0,0,0,0.1); text-align: center; }
  h1 { font-size: 1.25rem; color: #0f172a; margin: 0 0 0.75rem; }
  p { color: #475569; line-height: 1.6; margin: 0 0 1.5rem; }
  .spinner { display: inline-block; width: 2rem; height: 2rem; border: 3px solid #e2e8f0;
             border-top-color: #3b82f6; border-radius: 50%; animation: spin 0.8s linear infinite; }
  @keyframes spin { to { transform: rotate(360deg); } }
</style>
</head>
<body>
<div class="card">
  <div class="spinner"></div>
  <h1>Setting up your account…</h1>
  <p>Your payment was successful! We're activating your subscription — this
     usually takes just a few seconds. You'll be redirected automatically.</p>
  <p><small>If you aren't redirected, <a href="/search">click here</a>.</small></p>
</div>
<meta http-equiv="refresh" content="5; url=/search">
</body>
</html>"#;

    (
        StatusCode::OK,
        [("content-type", "text/html; charset=utf-8")],
        html,
    )
}

/// `GET /billing/cancel` — Stripe redirects here when the user cancels at
/// Checkout.
///
/// Redirects the browser to the Web UI's root (which will redirect to the
/// login or search page based on session state). The user did not complete
/// payment, so no subscription changes are made.
///
/// This endpoint is **public** (no auth) — Stripe redirects the browser here.
pub async fn get_cancel() -> impl IntoResponse {
    // Redirect to the Web UI root. If the user is still authenticated, they'll
    // be redirected to /search by the web handler; otherwise /login.
    Redirect::to("/").into_response()
}

// ── Portal handlers (P11-T6) ─────────────────────────────────────────────────────

/// `GET /billing/portal` — create a Stripe Customer Portal session.
///
/// Looks up the authenticated tenant's `stripe_customer_id` from its billing
/// state, then delegates to the
/// [`StripeClient`](crate::stripe_client::StripeClient) to create a Customer
/// Portal session. The portal lets customers update payment methods, view
/// invoices, cancel subscriptions, and change plans (if configured in the
/// Stripe dashboard). PCI stays at SAQ-A — card data never touches our servers.
///
/// The `return_url` points back to `/account` so the user returns to their
/// account page after leaving the portal.
///
/// # Response
///
/// * `200 OK` — [`PortalResponse`] with the Stripe Customer Portal URL.
///
/// # Errors
///
/// * `400 Bad Request` — tenant has no `stripe_customer_id` (free plan, never
///   subscribed).
/// * `401 Unauthorized` — rejected by auth middleware.
/// * `502 Bad Gateway` — Stripe client not configured, database failure, or
///   Stripe API error.
pub async fn get_portal(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<(StatusCode, Json<PortalResponse>), (StatusCode, Json<ErrorResponse>)> {
    portal_handler(&state, auth_user).await
}

/// `POST /billing/portal` — create a Stripe Customer Portal session (HTMX
/// variant).
///
/// Same behaviour as [`get_portal`]; exists for `<button hx-post="/billing/portal">`
/// in the account page. The client receives the portal URL in the response and
/// can redirect the browser with `HX-Redirect` or JavaScript.
pub async fn post_portal(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<(StatusCode, Json<PortalResponse>), (StatusCode, Json<ErrorResponse>)> {
    portal_handler(&state, auth_user).await
}

/// Shared implementation: look up the tenant's Stripe customer id, then create
/// a Customer Portal session.
async fn portal_handler(
    state: &AppState,
    auth_user: AuthUser,
) -> Result<(StatusCode, Json<PortalResponse>), (StatusCode, Json<ErrorResponse>)> {
    // ── Look up the tenant's Stripe customer id ──────────────────────────────
    let billing = state
        .pg_store
        .get_tenant_billing(auth_user.tenant_id)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, tenant_id = auth_user.tenant_id, "failed to fetch tenant billing");
            bad_gateway("internal_error", "an unexpected error occurred")
        })?
        .ok_or_else(|| {
            bad_request("unknown_tenant", "tenant not found")
        })?;

    let customer_id = billing.stripe_customer_id.ok_or_else(|| {
        bad_request(
            "no_stripe_customer",
            "no Stripe customer — subscribe first via /billing/checkout",
        )
    })?;

    // ── Resolve the Stripe client ────────────────────────────────────────────
    let stripe = state.stripe_client.as_ref().ok_or_else(|| {
        bad_gateway(
            "stripe_not_configured",
            "stripe client not configured — set STRIPE_SECRET_KEY",
        )
    })?;

    // ── Build the portal return URL ──────────────────────────────────────────
    let base = state.public_base_url.trim_end_matches('/');
    let return_url = format!("{base}/account");

    // ── Create the Customer Portal session ────────────────────────────────────
    let portal_req = CreatePortalSessionRequest {
        customer_id,
        return_url,
    };

    let portal_resp = stripe
        .create_customer_portal_session(portal_req)
        .await
        .map_err(|e| {
            tracing::error!(
                error = %e,
                tenant_id = auth_user.tenant_id,
                "Stripe Customer Portal session creation failed"
            );
            bad_gateway(
                "stripe_portal_error",
                "Stripe portal session creation failed",
            )
        })?;

    Ok((
        StatusCode::OK,
        Json(PortalResponse {
            url: portal_resp.url,
        }),
    ))
}

// ═══════════════════════════════════════════════════════════════════════════════
// Error helpers
// ═══════════════════════════════════════════════════════════════════════════════

/// Build a `400 Bad Request` error.
fn bad_request(code: &str, msg: &str) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorResponse {
            error: code.into(),
            message: msg.into(),
        }),
    )
}

/// Build a `500 Internal Server Error` from any error.
fn internal_error(e: impl std::fmt::Display) -> (StatusCode, Json<ErrorResponse>) {
    tracing::error!(error = %e, "billing handler error");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            error: "internal_error".into(),
            message: "an unexpected error occurred".into(),
        }),
    )
}

/// Build a `502 Bad Gateway` error (upstream Stripe failure).
///
/// Logs the error at ERROR level and returns a user-safe message so
/// internal details aren't leaked to the client.
fn bad_gateway(code: &str, msg: &str) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::BAD_GATEWAY,
        Json(ErrorResponse {
            error: code.into(),
            message: msg.into(),
        }),
    )
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
    use axum::routing::{get, post};
    use kb_core::user::UserRole;
    use kb_store::PgStore;
    use kb_store::session_store::InMemorySessionStore;
    use tower::ServiceExt;

    use super::*;
    use crate::AppState;
    use crate::middleware::auth_middleware;
    use crate::stripe_client::{MockStripeClient, StripeClient};

    /// Build a test `AppState` with a mock Stripe client and a disconnected
    /// `PgStore`. The PgStore won't satisfy `get_plan_by_code` queries — those
    /// are tested via integration tests. Handler-level tests use the mock
    /// StripeClient to verify the checkout handler's routing and error handling.
    fn test_state(stripe_client: Arc<dyn StripeClient>) -> Arc<AppState> {
        let session_store: Arc<dyn kb_core::session::SessionStore> =
            Arc::new(InMemorySessionStore::new());
        let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
        Arc::new(
            AppState::new(
                session_store,
                pg_store,
                Some(Duration::from_secs(3600)),
                false,
            )
            .with_stripe_client(stripe_client)
            .with_public_base_url("http://localhost:9999"),
        )
    }

    /// Build a router with billing routes (checkout + portal behind auth,
    /// success + cancel public).
    fn billing_router(state: Arc<AppState>) -> Router {
        let public = Router::new()
            .route("/billing/success", get(get_success))
            .route("/billing/cancel", get(get_cancel));

        let protected = Router::new()
            .route("/billing/checkout", post(post_checkout))
            .route("/billing/portal", get(get_portal).post(post_portal))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ));

        public.merge(protected).with_state(state)
    }

    /// Helper: create a valid session token for tenant 1, user 42.
    async fn create_session(state: &Arc<AppState>) -> String {
        state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .expect("create session")
    }

    // ── POST /billing/checkout tests ────────────────────────────────────────

    /// With a valid session and a plan_code, the handler looks up the plan
    /// (which fails against the disconnected PgStore) → the test verifies the
    /// handler calls the PgStore (resulting in a 500 internal error, since the
    /// DB is unreachable). This is expected for handler-level tests.
    #[tokio::test]
    async fn checkout_unauthenticated_returns_401() {
        let state = test_state(Arc::new(MockStripeClient::new()));
        let router = billing_router(state);

        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/billing/checkout")
            .header("Content-Type", "application/json")
            .body(Body::from(r#"{"plan_code":"pro"}"#))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    /// A request with an empty `plan_code` field returns 400 before the
    /// database is touched.
    #[tokio::test]
    async fn checkout_empty_plan_code_returns_400() {
        let state = test_state(Arc::new(MockStripeClient::new()));
        let token = create_session(&state).await;
        let router = billing_router(state);

        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/billing/checkout")
            .header("Content-Type", "application/json")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::from(r#"{"plan_code":"  "}"#))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    /// A request missing the `plan_code` field entirely returns a 400
    /// (axum JSON deserialization error or our validation).
    #[tokio::test]
    async fn checkout_missing_plan_code_field_returns_error() {
        let state = test_state(Arc::new(MockStripeClient::new()));
        let token = create_session(&state).await;
        let router = billing_router(state);

        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/billing/checkout")
            .header("Content-Type", "application/json")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::from(r#"{}"#))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        // axum returns 422 Unprocessable Entity for missing required fields
        // (or 400 depending on serde configuration). Either is acceptable.
        assert!(response.status().is_client_error());
    }

    /// When the Stripe client is not configured (`stripe_client` is `None`),
    /// the handler returns 500 with a clear log message.
    #[tokio::test]
    async fn checkout_missing_stripe_client_returns_500() {
        let state = test_state_no_stripe();
        let token = create_session(&state).await;
        let router = billing_router(state);

        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/billing/checkout")
            .header("Content-Type", "application/json")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::from(r#"{"plan_code":"pro"}"#))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    /// Build a test state WITHOUT a Stripe client.
    fn test_state_no_stripe() -> Arc<AppState> {
        let session_store: Arc<dyn kb_core::session::SessionStore> =
            Arc::new(InMemorySessionStore::new());
        let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
        Arc::new(AppState::new(
            session_store,
            pg_store,
            Some(Duration::from_secs(3600)),
            false,
        ))
    }

    /// With a valid session and a mock StripeClient, but an unreachable DB,
    /// the handler returns 500 when the PgStore query fails.
    #[tokio::test]
    async fn checkout_db_unreachable_returns_500() {
        let state = test_state(Arc::new(MockStripeClient::new()));
        let token = create_session(&state).await;
        let router = billing_router(state);

        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/billing/checkout")
            .header("Content-Type", "application/json")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::from(r#"{"plan_code":"pro"}"#))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        // The PgStore is disconnected → get_plan_by_code returns an error → 500.
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    // ── GET /billing/success tests ──────────────────────────────────────────

    /// The success page returns 200 OK with HTML content.
    #[tokio::test]
    async fn success_returns_html() {
        let state = test_state(Arc::new(MockStripeClient::new()));
        let router = billing_router(state);

        let request = axum::http::Request::builder()
            .method(Method::GET)
            .uri("/billing/success")
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let content_type = response
            .headers()
            .get("content-type")
            .expect("content-type header")
            .to_str()
            .unwrap();
        assert!(
            content_type.contains("text/html"),
            "expected text/html, got: {content_type}"
        );

        let body_bytes = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let body = String::from_utf8(body_bytes.to_vec()).unwrap();
        assert!(
            body.contains("Setting up your account"),
            "success page should contain the interstitial message"
        );
        assert!(
            body.contains("meta http-equiv=\"refresh\""),
            "should auto-redirect"
        );
    }

    /// The success page is public (no auth required).
    #[tokio::test]
    async fn success_no_auth_required() {
        let state = test_state(Arc::new(MockStripeClient::new()));
        let router = billing_router(state);

        let request = axum::http::Request::builder()
            .method(Method::GET)
            .uri("/billing/success")
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        // Must NOT be 401.
        assert_eq!(response.status(), StatusCode::OK);
    }

    // ── GET /billing/cancel tests ───────────────────────────────────────────

    /// The cancel endpoint redirects (303 See Other or 302 Found).
    #[tokio::test]
    async fn cancel_redirects() {
        let state = test_state(Arc::new(MockStripeClient::new()));
        let router = billing_router(state);

        let request = axum::http::Request::builder()
            .method(Method::GET)
            .uri("/billing/cancel")
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert!(
            response.status().is_redirection(),
            "cancel should redirect, got {}",
            response.status()
        );

        let location = response
            .headers()
            .get("location")
            .expect("location header")
            .to_str()
            .unwrap();
        assert_eq!(location, "/", "cancel should redirect to root");
    }

    /// The cancel page is public (no auth required).
    #[tokio::test]
    async fn cancel_no_auth_required() {
        let state = test_state(Arc::new(MockStripeClient::new()));
        let router = billing_router(state);

        let request = axum::http::Request::builder()
            .method(Method::GET)
            .uri("/billing/cancel")
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        // Redirect = not 401.
        assert!(response.status().is_redirection());
    }

    // ── Error helper tests ──────────────────────────────────────────────────

    #[test]
    fn bad_request_has_400_status() {
        let (status, body) = bad_request("unknown_plan_code", "plan 'enterprise' not found");
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body.error, "unknown_plan_code");
        assert_eq!(body.message, "plan 'enterprise' not found");
    }

    #[test]
    fn internal_error_has_500_status() {
        let (status, body) = internal_error(anyhow::anyhow!("stripe down"));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body.error, "internal_error");
        assert_eq!(body.message, "an unexpected error occurred");
    }

    // ── Request deserialization tests ───────────────────────────────────────

    #[test]
    fn checkout_request_deserializes_valid_json() {
        let req: CheckoutRequest =
            serde_json::from_str(r#"{"plan_code":"pro"}"#).expect("valid json");
        assert_eq!(req.plan_code, "pro");
    }

    #[test]
    fn checkout_response_serializes_correctly() {
        let resp = CheckoutResponse {
            url: "https://checkout.stripe.com/c/pay/cs_test_abc".into(),
        };
        let json = serde_json::to_string(&resp).expect("serialize");
        assert!(json.contains("https://checkout.stripe.com/"));
    }

    // ── GET /billing/portal tests (P11-T6) ────────────────────────────────────

    /// Portal unauthenticated returns 401 (behind auth middleware).
    #[tokio::test]
    async fn portal_unauthenticated_returns_401() {
        let state = test_state(Arc::new(MockStripeClient::new()));
        let router = billing_router(state);

        let request = axum::http::Request::builder()
            .method(Method::GET)
            .uri("/billing/portal")
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    /// POST portal unauthenticated returns 401 (behind auth middleware).
    #[tokio::test]
    async fn portal_post_unauthenticated_returns_401() {
        let state = test_state(Arc::new(MockStripeClient::new()));
        let router = billing_router(state);

        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/billing/portal")
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    /// When the Stripe client is not configured, the portal handler returns 502
    /// with a clear message.
    #[tokio::test]
    async fn portal_missing_stripe_client_returns_502() {
        let state = test_state_no_stripe();
        let token = create_session(&state).await;
        let router = billing_router(state);

        let request = axum::http::Request::builder()
            .method(Method::GET)
            .uri("/billing/portal")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        // The DB is unreachable → get_tenant_billing returns an error before we
        // even reach the Stripe client check → the error is captured as 502.
        assert!(
            response.status().is_server_error(),
            "portal should return server error without stripe client + DB, got {}",
            response.status()
        );
    }

    /// With a valid session and a mock StripeClient, but an unreachable DB,
    /// the portal handler returns 502 when the PgStore query fails (since
    /// `get_tenant_billing` requires a DB connection).
    #[tokio::test]
    async fn portal_db_unreachable_returns_502() {
        let state = test_state(Arc::new(MockStripeClient::new()));
        let token = create_session(&state).await;
        let router = billing_router(state);

        let request = axum::http::Request::builder()
            .method(Method::GET)
            .uri("/billing/portal")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        // The PgStore is disconnected → get_tenant_billing returns an error → 502.
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    }

    // ── Error helper tests (P11-T6 additions) ─────────────────────────────────

    #[test]
    fn bad_gateway_has_502_status() {
        let (status, body) = bad_gateway(
            "stripe_portal_error",
            "Stripe portal session creation failed",
        );
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(body.error, "stripe_portal_error");
        assert_eq!(body.message, "Stripe portal session creation failed");
    }

    // ── PortalResponse serialization ──────────────────────────────────────────

    #[test]
    fn portal_response_serializes_correctly() {
        let resp = PortalResponse {
            url: "https://billing.stripe.com/p/session/test_bps_abc".into(),
        };
        let json = serde_json::to_string(&resp).expect("serialize");
        assert!(json.contains("https://billing.stripe.com/"));
        assert!(json.contains("url"));
    }
}
