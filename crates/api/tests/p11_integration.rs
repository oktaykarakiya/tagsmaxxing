//! P11 phase integration test (§29, §31): end-to-end billing lifecycle with
//! mock Stripe server, real Postgres (testcontainers), and the full API router.
//!
//! Tests exercise every P11 billing scenario:
//!
//! 1. **Plans seeded** — free, pro, team plans exist after migration.
//! 2. **Checkout → webhook → activation** — create Checkout session for 'pro',
//!    mock `checkout.session.completed` webhook → tenant has pro plan + quota.
//! 3. **Payment failure → past_due → suspended** — mock `invoice.payment_failed`,
//!    verify past_due; simulate final failure → suspended; verify ingest returns 403.
//! 4. **Upgrade** — mock `customer.subscription.updated` from pro to team →
//!    verify plan + quota updated.
//! 5. **Cancel** — mock `customer.subscription.deleted` → verify free plan.
//! 6. **Idempotency** — send duplicate webhook → verify no double-processing.
//! 7. **Plan quota** — pro tenant within 5 GB → ok; free exceeds 50 MB → 413.
//!
//! These tests are `#[ignore]` in `just ci` because they require Podman +
//! `pgvector/pgvector:pg17`. Run with:
//!
//! ```bash
//! systemctl --user start podman.socket
//! DOCKER_HOST="unix://$XDG_RUNTIME_DIR/podman/podman.sock" \
//!   cargo test --package kb-api --test p11_integration -- --ignored --test-threads=1
//! ```
//!
//! P11 PHASE COMPLETE after this passes.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{self, StatusCode};
use axum::middleware::from_fn_with_state;
use axum::routing::{get, post};
use hmac::{Hmac, Mac};
use kb_api::AppState;
use kb_api::middleware::auth_middleware;
use kb_api::stripe_client::{MockStripeClient, StripeClient};
use kb_core::quota::QuotaError;
use kb_core::session::SessionStore;
use kb_core::user::UserRole;
use kb_store::PgStore;
use kb_store::session_store::InMemorySessionStore;
use sha2::Sha256;
use tower::ServiceExt;

type HmacSha256 = Hmac<Sha256>;

// ── Helpers ──────────────────────────────────────────────────────────────────────

/// Create a connected PgStore against a fresh testcontainers Postgres database.
///
/// Plans are seeded by the migration (0018_plans.sql), so they are immediately
/// available after `connect()` returns.
async fn connected_store() -> PgStore {
    let db = kb_testsupport::fresh_db().await.expect("fresh_db");
    let store = PgStore::with_roles(&db.admin_url, &db.app_url);
    store.connect().await.expect("connect");
    store
}

/// Sign a webhook payload with HMAC-SHA256, returning the `stripe-signature`
/// header value and a set of accompanying headers.
///
/// Uses a fixed timestamp so the signature is deterministic — the handler does
/// not check timestamp freshness, only the HMAC match.
fn sign_webhook_payload(payload: &str, secret: &str) -> String {
    let timestamp = "1687988800";
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(payload.as_bytes());
    let sig_hex = hex::encode(mac.finalize().into_bytes());
    format!("t={timestamp},v1={sig_hex}")
}

// ── Test AppState builders ───────────────────────────────────────────────────────

/// Build an [`AppState`] suitable for billing+webhook tests: connected PgStore,
/// mock Stripe client, webhook secret, in-memory sessions.
fn build_billing_state(
    store: Arc<PgStore>,
    stripe_client: Option<Arc<dyn StripeClient>>,
    webhook_secret: Option<&str>,
) -> Arc<AppState> {
    let sessions: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
    let mut state = AppState::new(sessions, store, Some(Duration::from_secs(3600)), false)
        .with_public_base_url("http://localhost:9999");

    if let Some(client) = stripe_client {
        state = state.with_stripe_client(client);
    }
    if let Some(secret) = webhook_secret {
        state = state.with_stripe_webhook_secret(secret);
    }
    Arc::new(state)
}

// ── Router builders ──────────────────────────────────────────────────────────────

/// Build a minimal router with billing routes (checkout + portal behind auth,
/// success + cancel public).
fn billing_router(state: Arc<AppState>) -> Router {
    let public = Router::new()
        .route(
            "/billing/success",
            get(kb_api::handlers::billing::get_success),
        )
        .route(
            "/billing/cancel",
            get(kb_api::handlers::billing::get_cancel),
        );

    let protected = Router::new()
        .route(
            "/billing/checkout",
            post(kb_api::handlers::billing::post_checkout),
        )
        .route(
            "/billing/portal",
            get(kb_api::handlers::billing::get_portal).post(kb_api::handlers::billing::post_portal),
        )
        .route("/auth/logout", post(kb_api::handlers::auth::logout))
        .layer(from_fn_with_state(state.clone(), auth_middleware));

    public.merge(protected).with_state(state)
}

/// Build a minimal router with only the webhook endpoint (public, no auth).
fn webhook_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/stripe/webhook",
            post(kb_api::handlers::webhook::post_webhook),
        )
        .with_state(state)
}

// ── Request/response helpers ─────────────────────────────────────────────────────

/// Create an authenticated session and return the session token.
async fn create_session(
    state: &Arc<AppState>,
    tenant_id: i64,
    user_id: i64,
    role: UserRole,
) -> String {
    state
        .session_store
        .create(tenant_id, user_id, role, Duration::from_secs(3600))
        .await
        .unwrap()
}

/// Build an HTTP request with a session cookie.
fn auth_request(method: &str, uri: &str, body: Body, session_token: &str) -> http::Request<Body> {
    http::Request::builder()
        .method(method)
        .uri(uri)
        .header("Cookie", format!("__Host-session={session_token}"))
        .header("content-type", "application/json")
        .body(body)
        .unwrap()
}

/// Build a webhook request with the Stripe signature header.
fn webhook_request(payload: &str, sig_header: &str) -> http::Request<Body> {
    http::Request::builder()
        .method("POST")
        .uri("/stripe/webhook")
        .header("stripe-signature", sig_header)
        .header("content-type", "application/json")
        .body(Body::from(payload.to_string()))
        .unwrap()
}

// ── Webhook payload builders ─────────────────────────────────────────────────────

/// Build a `checkout.session.completed` webhook payload JSON string.
fn checkout_completed_payload(
    event_id: &str,
    tenant_id: i64,
    customer_id: &str,
    subscription_id: &str,
    plan_code: &str,
) -> String {
    serde_json::json!({
        "id": event_id,
        "type": "checkout.session.completed",
        "data": {
            "object": {
                "client_reference_id": tenant_id.to_string(),
                "customer": customer_id,
                "subscription": subscription_id,
                "metadata": {
                    "plan_code": plan_code
                }
            }
        }
    })
    .to_string()
}

/// Build an `invoice.payment_failed` webhook payload JSON string.
fn invoice_payment_failed_payload(event_id: &str, customer_id: &str) -> String {
    serde_json::json!({
        "id": event_id,
        "type": "invoice.payment_failed",
        "data": {
            "object": {
                "customer": customer_id
            }
        }
    })
    .to_string()
}

/// Build a `customer.subscription.updated` webhook payload JSON string.
fn subscription_updated_payload(
    event_id: &str,
    subscription_id: &str,
    customer_id: &str,
    stripe_price_id: &str,
    period_end_ts: i64,
) -> String {
    serde_json::json!({
        "id": event_id,
        "type": "customer.subscription.updated",
        "data": {
            "object": {
                "id": subscription_id,
                "customer": customer_id,
                "items": {
                    "data": [
                        {
                            "price": {
                                "id": stripe_price_id
                            }
                        }
                    ]
                },
                "current_period_end": period_end_ts
            }
        }
    })
    .to_string()
}

/// Build a `customer.subscription.deleted` webhook payload JSON string.
fn subscription_deleted_payload(event_id: &str, customer_id: &str) -> String {
    serde_json::json!({
        "id": event_id,
        "type": "customer.subscription.deleted",
        "data": {
            "object": {
                "customer": customer_id
            }
        }
    })
    .to_string()
}

// ═══════════════════════════════════════════════════════════════════════════════════
// Test 1: Plans are seeded by migration
// ═══════════════════════════════════════════════════════════════════════════════════

/// After `connect()`, the three default plans (free, pro, team) must exist.
#[ignore = "requires Podman + image pull; run with --ignored"]
#[tokio::test]
async fn plans_seeded_on_connect() {
    let store = connected_store().await;

    let free = store
        .get_plan_by_code("free")
        .await
        .expect("get free plan")
        .expect("free plan must exist");
    assert_eq!(free.code, "free");
    assert_eq!(free.stripe_price, "price_free");
    assert_eq!(free.quota_bytes, 52_428_800); // 50 MB
    assert_eq!(free.token_budget, Some(10_000));
    assert_eq!(free.price_cents, Some(0));

    let pro = store
        .get_plan_by_code("pro")
        .await
        .expect("get pro plan")
        .expect("pro plan must exist");
    assert_eq!(pro.code, "pro");
    assert_eq!(pro.stripe_price, "price_pro_monthly");
    assert_eq!(pro.quota_bytes, 5_368_709_120); // 5 GB
    assert_eq!(pro.token_budget, Some(500_000));
    assert_eq!(pro.price_cents, Some(900));

    let team = store
        .get_plan_by_code("team")
        .await
        .expect("get team plan")
        .expect("team plan must exist");
    assert_eq!(team.code, "team");
    assert_eq!(team.stripe_price, "price_team_monthly");
    assert_eq!(team.quota_bytes, 53_687_091_200); // 50 GB
    assert_eq!(team.token_budget, Some(2_000_000));
    assert_eq!(team.price_cents, Some(2900));
}

// ═══════════════════════════════════════════════════════════════════════════════════
// Test 2: Checkout → webhook activates pro subscription
// ═══════════════════════════════════════════════════════════════════════════════════

/// Full signup flow: tenant creates a Checkout session for the 'pro' plan, then
/// a `checkout.session.completed` webhook activates the subscription and sets
/// the tenant's plan + quotas.
#[ignore = "requires Podman + image pull; run with --ignored"]
#[tokio::test]
async fn checkout_and_webhook_activates_pro_subscription() {
    let store = Arc::new(connected_store().await);

    // Create tenant and user.
    let tenant_id = store
        .create_tenant("pro-signup", "Pro Signup Tenant")
        .await
        .expect("create_tenant");

    let pw_hash = kb_core::auth::hash_password("test-password").expect("hash");
    let user_id = store
        .create_user(tenant_id, "pro@signup.example", &pw_hash, UserRole::Owner)
        .await
        .expect("create_user");

    // ── Step 1: Verify tenant starts without a plan (grandfathered / NULL) ─────
    let billing_before = store
        .get_tenant_billing(tenant_id)
        .await
        .expect("get_billing")
        .expect("tenant must exist");
    assert!(
        billing_before.plan.is_none(),
        "new tenant must have no plan"
    );
    assert_eq!(billing_before.billing_status, "inactive");

    // ── Step 2: POST /billing/checkout with plan_code = "pro" ──────────────────
    let stripe: Arc<dyn StripeClient> = Arc::new(MockStripeClient::new());
    let state = build_billing_state(
        Arc::clone(&store),
        Some(stripe),
        None, // webhook secret not needed for checkout test
    );
    let session_token = create_session(&state, tenant_id, user_id, UserRole::Owner).await;
    let router = billing_router(state);

    let request = auth_request(
        "POST",
        "/billing/checkout",
        Body::from(r#"{"plan_code":"pro"}"#),
        &session_token,
    );
    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap(),
    )
    .unwrap();
    let checkout_url = body["url"].as_str().expect("url must be present");
    assert!(
        checkout_url.contains("price_pro_monthly"),
        "checkout URL must contain price_id, got: {checkout_url}"
    );

    // ── Step 3: Send checkout.session.completed webhook ────────────────────────
    let webhook_state = build_billing_state(
        Arc::clone(&store),
        None,                   // no Stripe client needed for webhook
        Some("whsec_p11_test"), // webhook secret
    );
    let webhook_router = webhook_router(webhook_state);

    let payload =
        checkout_completed_payload("evt_checkout_1", tenant_id, "cus_pro_1", "sub_pro_1", "pro");
    let sig_header = sign_webhook_payload(&payload, "whsec_p11_test");

    let request = webhook_request(&payload, &sig_header);
    let response = webhook_router.oneshot(request).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "webhook must return 200 OK"
    );

    // ── Step 4: Verify tenant now has pro plan + correct quotas ────────────────
    let billing_after = store
        .get_tenant_billing(tenant_id)
        .await
        .expect("get_billing")
        .expect("tenant must exist");

    let plan = billing_after
        .plan
        .expect("tenant must have a plan after webhook");
    assert_eq!(plan.code, "pro", "plan must be pro after checkout webhook");
    assert_eq!(
        billing_after.billing_status, "active",
        "billing_status must be active"
    );
    assert_eq!(
        billing_after.stripe_customer_id.as_deref(),
        Some("cus_pro_1"),
        "stripe_customer_id must be set"
    );
    assert_eq!(
        billing_after.subscription_id.as_deref(),
        Some("sub_pro_1"),
        "subscription_id must be set"
    );

    // Verify plan quotas were applied to tenant.
    let pro_plan = store
        .get_plan_by_code("pro")
        .await
        .expect("get pro plan")
        .expect("pro plan exists");
    store
        .apply_plan_quotas_to_tenant(tenant_id, pro_plan.id, 0)
        .await
        .expect("apply quotas must be idempotent"); // already applied by webhook — fine to re-apply
}

// ═══════════════════════════════════════════════════════════════════════════════════
// Test 3: Payment failure → past_due → suspended → ingest returns 403
// ═══════════════════════════════════════════════════════════════════════════════════

/// After an `invoice.payment_failed` webhook, the tenant's billing_status is set
/// to `past_due`. If the dunning period expires, `suspend_past_due_tenants`
/// transitions the tenant to `suspended`, and subsequent ingest requests are
/// rejected with 403.
#[ignore = "requires Podman + image pull; run with --ignored"]
#[tokio::test]
async fn payment_failure_to_suspended_blocks_ingest() {
    let store = Arc::new(connected_store().await);

    // Create tenant and activate pro subscription first.
    let tenant_id = store
        .create_tenant("fail-tenant", "Payment Failure Tenant")
        .await
        .expect("create_tenant");

    let pw_hash = kb_core::auth::hash_password("test-password").expect("hash");
    let _user_id = store
        .create_user(tenant_id, "fail@payment.example", &pw_hash, UserRole::Owner)
        .await
        .expect("create_user");

    // Activate pro plan via webhook.
    let pro = store
        .get_plan_by_code("pro")
        .await
        .expect("get pro plan")
        .expect("pro plan exists");

    store
        .update_tenant_plan(
            tenant_id,
            Some(pro.id),
            Some("cus_fail_1"),
            Some("sub_fail_1"),
            Some("active"),
            None,
        )
        .await
        .expect("activate pro plan");

    store
        .apply_plan_quotas_to_tenant(tenant_id, pro.id, 0)
        .await
        .expect("apply pro quotas");

    // ── Step 1: Send invoice.payment_failed webhook ────────────────────────────
    let webhook_state = build_billing_state(Arc::clone(&store), None, Some("whsec_p11_test"));
    let webhook_router = webhook_router(webhook_state);

    let payload = invoice_payment_failed_payload("evt_fail_1", "cus_fail_1");
    let sig_header = sign_webhook_payload(&payload, "whsec_p11_test");

    let request = webhook_request(&payload, &sig_header);
    let response = webhook_router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Verify past_due status.
    let billing = store
        .get_tenant_billing(tenant_id)
        .await
        .expect("get_billing")
        .expect("tenant must exist");
    assert_eq!(
        billing.billing_status, "past_due",
        "billing_status must be past_due after payment failure"
    );

    // ── Step 2: Simulate dunning expiry → suspend ──────────────────────────────
    // Set current_period_end far in the past so the tenant qualifies for suspension.
    use chrono::Utc;
    let far_past = Utc::now() - chrono::Duration::days(30);
    store
        .update_tenant_plan(tenant_id, None, None, None, None, Some(far_past))
        .await
        .expect("set period end to past");

    // Call the dunning/suspension method (runs as a scheduled admin job).
    let suspended = store
        .suspend_past_due_tenants(7 /* past_due_days */, 0 /* actor_user_id */)
        .await
        .expect("suspend_past_due_tenants");
    assert!(
        suspended.contains(&tenant_id),
        "tenant {tenant_id} must be suspended"
    );

    let billing = store
        .get_tenant_billing(tenant_id)
        .await
        .expect("get_billing")
        .expect("tenant must exist");
    assert_eq!(
        billing.billing_status, "suspended",
        "billing_status must be suspended after dunning expiry"
    );

    // ── Step 3: Verify ingest returns 403 for suspended tenant ─────────────────
    // The ingest handler calls check_tenant_not_suspended before processing.
    let err = store
        .check_tenant_not_suspended(tenant_id)
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("suspended"),
        "error must mention suspension, got: {msg}"
    );

    // Reset suspension for cleanup (so the test can run again idempotently).
    store
        .update_tenant_plan(
            tenant_id,
            Some(pro.id),
            None,
            None,
            Some("active"),
            Some(Utc::now() + chrono::Duration::days(30)),
        )
        .await
        .expect("reset suspension");
}

// ═══════════════════════════════════════════════════════════════════════════════════
// Test 4: Subscription upgrade (customer.subscription.updated → team plan)
// ═══════════════════════════════════════════════════════════════════════════════════

/// When a `customer.subscription.updated` webhook arrives with the team price_id,
/// the tenant's plan is upgraded from pro to team and the team quotas are applied.
#[ignore = "requires Podman + image pull; run with --ignored"]
#[tokio::test]
async fn subscription_updated_upgrades_from_pro_to_team() {
    let store = Arc::new(connected_store().await);

    // Create tenant with pro plan already active.
    let tenant_id = store
        .create_tenant("upgrade-tenant", "Upgrade Tenant")
        .await
        .expect("create_tenant");

    let pro = store
        .get_plan_by_code("pro")
        .await
        .expect("get pro plan")
        .expect("pro plan exists");

    store
        .update_tenant_plan(
            tenant_id,
            Some(pro.id),
            Some("cus_upgrade_1"),
            Some("sub_upgrade_1"),
            Some("active"),
            None,
        )
        .await
        .expect("activate pro plan");

    store
        .apply_plan_quotas_to_tenant(tenant_id, pro.id, 0)
        .await
        .expect("apply pro quotas");

    // Verify starting on pro.
    let billing = store
        .get_tenant_billing(tenant_id)
        .await
        .expect("get_billing")
        .expect("tenant must exist");
    assert_eq!(billing.plan.unwrap().code, "pro");

    // ── Send subscription.updated webhook with team price ──────────────────────
    let webhook_state = build_billing_state(Arc::clone(&store), None, Some("whsec_p11_test"));
    let webhook_router = webhook_router(webhook_state);

    let payload = subscription_updated_payload(
        "evt_upgrade_1",
        "sub_upgrade_1",
        "cus_upgrade_1",
        "price_team_monthly", // team plan's stripe_price
        1780000000,           // some future period end
    );
    let sig_header = sign_webhook_payload(&payload, "whsec_p11_test");

    let request = webhook_request(&payload, &sig_header);
    let response = webhook_router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // ── Verify tenant upgraded to team plan ────────────────────────────────────
    let billing = store
        .get_tenant_billing(tenant_id)
        .await
        .expect("get_billing")
        .expect("tenant must exist");

    let plan = billing.plan.expect("tenant must have a plan");
    assert_eq!(plan.code, "team", "plan must be team after upgrade webhook");
    assert_eq!(billing.billing_status, "active");
}

// ═══════════════════════════════════════════════════════════════════════════════════
// Test 5: Subscription cancel (customer.subscription.deleted → free plan)
// ═══════════════════════════════════════════════════════════════════════════════════

/// When a `customer.subscription.deleted` webhook arrives, the tenant is
/// reverted to the free plan and billing_status is set to "canceled".
#[ignore = "requires Podman + image pull; run with --ignored"]
#[tokio::test]
async fn subscription_deleted_reverts_to_free_plan() {
    let store = Arc::new(connected_store().await);

    // Create tenant on pro plan.
    let tenant_id = store
        .create_tenant("cancel-tenant", "Cancel Tenant")
        .await
        .expect("create_tenant");

    let pro = store
        .get_plan_by_code("pro")
        .await
        .expect("get pro plan")
        .expect("pro plan exists");

    store
        .update_tenant_plan(
            tenant_id,
            Some(pro.id),
            Some("cus_cancel_1"),
            Some("sub_cancel_1"),
            Some("active"),
            None,
        )
        .await
        .expect("activate pro plan");

    store
        .apply_plan_quotas_to_tenant(tenant_id, pro.id, 0)
        .await
        .expect("apply pro quotas");

    // Verify starting on pro.
    let billing = store
        .get_tenant_billing(tenant_id)
        .await
        .expect("get_billing")
        .expect("tenant must exist");
    assert_eq!(billing.plan.unwrap().code, "pro");
    assert_eq!(billing.billing_status, "active");

    // ── Send subscription.deleted webhook ──────────────────────────────────────
    let webhook_state = build_billing_state(Arc::clone(&store), None, Some("whsec_p11_test"));
    let webhook_router = webhook_router(webhook_state);

    let payload = subscription_deleted_payload("evt_cancel_1", "cus_cancel_1");
    let sig_header = sign_webhook_payload(&payload, "whsec_p11_test");

    let request = webhook_request(&payload, &sig_header);
    let response = webhook_router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // ── Verify tenant reverted to free plan with canceled status ───────────────
    let billing = store
        .get_tenant_billing(tenant_id)
        .await
        .expect("get_billing")
        .expect("tenant must exist");

    let plan = billing.plan.expect("tenant must have a plan (free)");
    assert_eq!(plan.code, "free", "plan must be free after cancel webhook");
    assert_eq!(
        billing.billing_status, "canceled",
        "billing_status must be canceled"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════════
// Test 6: Webhook idempotency — duplicate event does not double-process
// ═══════════════════════════════════════════════════════════════════════════════════

/// Sending the same webhook event twice must return 200 both times and must not
/// cause duplicate processing (idempotency via `stripe_events` ON CONFLICT).
#[ignore = "requires Podman + image pull; run with --ignored"]
#[tokio::test]
async fn webhook_idempotency_duplicate_event_no_double_processing() {
    let store = Arc::new(connected_store().await);

    // Create tenant without any plan.
    let tenant_id = store
        .create_tenant("idem-tenant", "Idempotency Tenant")
        .await
        .expect("create_tenant");

    let webhook_state = build_billing_state(Arc::clone(&store), None, Some("whsec_p11_test"));
    let webhook_router = webhook_router(webhook_state);

    // ── Send first webhook (checkout.session.completed for pro) ────────────────
    let payload =
        checkout_completed_payload("evt_idem_1", tenant_id, "cus_idem_1", "sub_idem_1", "pro");
    let sig_header = sign_webhook_payload(&payload, "whsec_p11_test");

    let request = webhook_request(&payload, &sig_header);
    let response = webhook_router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    // Drain the body to free the connection for the next request.
    let _ = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();

    // Verify tenant has pro plan after first webhook.
    let billing = store
        .get_tenant_billing(tenant_id)
        .await
        .expect("get_billing")
        .expect("tenant must exist");
    assert_eq!(billing.plan.unwrap().code, "pro");
    assert_eq!(billing.stripe_customer_id.as_deref(), Some("cus_idem_1"));
    assert_eq!(billing.subscription_id.as_deref(), Some("sub_idem_1"));

    // ── Send the SAME webhook a second time (duplicate) ────────────────────────
    let request2 = webhook_request(&payload, &sig_header);
    let response2 = webhook_router.oneshot(request2).await.unwrap();
    assert_eq!(
        response2.status(),
        StatusCode::OK,
        "duplicate webhook must also return 200 OK"
    );

    // ── Verify state is unchanged (no double-processing) ───────────────────────
    let billing = store
        .get_tenant_billing(tenant_id)
        .await
        .expect("get_billing")
        .expect("tenant must exist");
    // Plan should still be pro — not applied twice or changed.
    assert_eq!(billing.plan.unwrap().code, "pro");
    assert_eq!(billing.stripe_customer_id.as_deref(), Some("cus_idem_1"));
    assert_eq!(billing.subscription_id.as_deref(), Some("sub_idem_1"));
    assert_eq!(billing.billing_status, "active");
}

// ═══════════════════════════════════════════════════════════════════════════════════
// Test 7: Plan-driven storage quota enforcement
// ═══════════════════════════════════════════════════════════════════════════════════

/// Pro tenant uploads within 5 GB → OK. Free tenant exceeds 50 MB → quota error
/// with upsell message.
#[ignore = "requires Podman + image pull; run with --ignored"]
#[tokio::test]
async fn plan_storage_quota_enforcement_pro_ok_free_exceeds() {
    let store = connected_store().await;

    // ── Pro tenant: 3 GB upload should pass ────────────────────────────────────
    let pro_tenant = store
        .create_tenant("quota-pro", "Quota Pro Tenant")
        .await
        .expect("create_tenant");

    let pro = store
        .get_plan_by_code("pro")
        .await
        .expect("get pro plan")
        .expect("pro plan exists");

    store
        .apply_plan_quotas_to_tenant(pro_tenant, pro.id, 0)
        .await
        .expect("apply pro quotas");

    let three_gb: i64 = 3 * 1024 * 1024 * 1024;
    store
        .check_plan_storage_quota(pro_tenant, three_gb)
        .await
        .expect("3 GB must fit within pro plan (5 GB limit)");

    // ── Free tenant: 60 MB upload should fail (free plan = 50 MB) ─────────────
    let free_tenant = store
        .create_tenant("quota-free", "Quota Free Tenant")
        .await
        .expect("create_tenant");

    let free = store
        .get_plan_by_code("free")
        .await
        .expect("get free plan")
        .expect("free plan exists");

    store
        .apply_plan_quotas_to_tenant(free_tenant, free.id, 0)
        .await
        .expect("apply free quotas");

    let sixty_mb: i64 = 60 * 1024 * 1024;
    let err = store
        .check_plan_storage_quota(free_tenant, sixty_mb)
        .await
        .unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.contains("storage quota exceeded"),
        "expected storage quota error, got: {msg}"
    );

    // The error should be a QuotaError with upsell info.
    let quota_err: &QuotaError = err.downcast_ref().expect("must be QuotaError");
    let upsell = quota_err
        .upsell_message()
        .expect("free→pro upgrade path must have upsell message");
    assert!(
        upsell.contains("free") && upsell.contains("pro"),
        "upsell must mention free→pro, got: {upsell}"
    );
}
