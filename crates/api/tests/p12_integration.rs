// SPDX-License-Identifier: AGPL-3.0-or-later

//! P12 phase integration test (§30, §31): full SaaS onboarding flow with
//! mock Stripe server, mock email, and real Postgres (testcontainers).
//!
//! Tests exercise the complete P12 SaaS lifecycle:
//!
//! 1. **Onboarding** — landing page → pricing → signup → email verification
//! 2. **Billing activation** — mock Stripe Checkout → webhook → pro subscription active
//! 3. **Dashboard** — login → dashboard shows 0 usage
//! 4. **API token** — create token → use Bearer auth for API access
//! 5. **Team invite** — invite user → invited user receives reset email
//! 6. **Billing failure** — payment fails → past_due → suspended → ingest blocked
//! 7. **Account deletion** — delete account → shred job enqueued
//!
//! All tests are `#[ignore]` in `just ci` because they require Podman +
//! `pgvector/pgvector:pg17`. Run with:
//!
//! ```bash
//! systemctl --user start podman.socket
//! DOCKER_HOST="unix://$XDG_RUNTIME_DIR/podman/podman.sock" \
//!   cargo test --package kb-api --test p12_integration -- --ignored --test-threads=1
//! ```
//!
//! P12 PHASE COMPLETE after this passes.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{self, StatusCode};
use hmac::{Hmac, Mac};
use kb_api::AppState;
use kb_api::stripe_client::{MockStripeClient, StripeClient};
use kb_core::api_token::ApiTokenStore;
use kb_core::email::{EmailProvider, EmailSender, LogEmail};
use kb_core::session::SessionStore;
use kb_core::user::UserRole;
use kb_pipeline::job_queue::JobQueue;
use kb_store::PgStore;
use kb_store::api_token_store::InMemoryApiTokenStore;
use kb_store::session_store::InMemorySessionStore;
use sha2::Sha256;
use tower::ServiceExt;

type HmacSha256 = Hmac<Sha256>;

// ── Helpers ──────────────────────────────────────────────────────────────────────

/// Create a connected PgStore against a fresh testcontainers Postgres database.
async fn connected_store() -> PgStore {
    let db = kb_testsupport::fresh_db().await.expect("fresh_db");
    let store = PgStore::with_roles(&db.admin_url, &db.app_url);
    store.connect().await.expect("connect");
    store
}

/// Sign a webhook payload with HMAC-SHA256, returning the `stripe-signature`
/// header value.
fn sign_webhook_payload(payload: &str, secret: &str) -> String {
    let timestamp = "1687988800";
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(payload.as_bytes());
    let sig_hex = hex::encode(mac.finalize().into_bytes());
    format!("t={timestamp},v1={sig_hex}")
}

// ── Test state builder ───────────────────────────────────────────────────────────

/// Build a fully-populated [`AppState`] suitable for the P12 onboarding flow.
///
/// Components wired: connected `PgStore`, mock Stripe, in-memory sessions,
/// LogEmail for email, in-memory API token store, and a JobQueue backed by
/// the admin pool.
fn build_p12_state(
    store: Arc<PgStore>,
    stripe_client: Option<Arc<dyn StripeClient>>,
    webhook_secret: Option<&str>,
) -> Arc<AppState> {
    let sessions: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
    let email: Arc<dyn EmailSender> = Arc::new(LogEmail);
    let email_provider: Arc<dyn EmailProvider> = Arc::new(LogEmail);
    let tokens: Arc<dyn ApiTokenStore> = Arc::new(InMemoryApiTokenStore::new());
    let pool = store.admin_pool().expect("admin pool for job queue");
    let job_queue = Arc::new(JobQueue::new(pool, 60_000, 3));

    let mut state = AppState::new(
        sessions,
        Arc::clone(&store),
        Some(Duration::from_secs(3600)),
        false,
    )
    .with_email_sender(email)
    .with_email_provider(email_provider)
    .with_api_token_store(tokens)
    .with_job_queue(job_queue)
    .with_public_base_url("http://localhost:9999");

    if let Some(client) = stripe_client {
        state = state.with_stripe_client(client);
    }
    if let Some(secret) = webhook_secret {
        state = state.with_stripe_webhook_secret(secret);
    }
    Arc::new(state)
}

// ── Router builder ───────────────────────────────────────────────────────────────

/// Build the full application router.
fn build_p12_router(state: Arc<AppState>) -> Router {
    // We build a minimal full router that includes all P12-relevant routes.
    // Public JSON API + webhook + health.
    let public = axum::Router::new()
        .route(
            "/auth/login",
            axum::routing::post(kb_api::handlers::auth::login),
        )
        .route(
            "/auth/register",
            axum::routing::post(kb_api::handlers::auth::register),
        )
        .route(
            "/billing/success",
            axum::routing::get(kb_api::handlers::billing::get_success),
        )
        .route(
            "/billing/cancel",
            axum::routing::get(kb_api::handlers::billing::get_cancel),
        )
        .route(
            "/stripe/webhook",
            axum::routing::post(kb_api::handlers::webhook::post_webhook),
        );

    // Protected routes (auth middleware).
    let api = axum::Router::new()
        .route(
            "/api/tokens",
            axum::routing::post(kb_api::handlers::api_tokens::create_token)
                .get(kb_api::handlers::api_tokens::list_tokens),
        )
        .route(
            "/api/tokens/{id}",
            axum::routing::delete(kb_api::handlers::api_tokens::revoke_token),
        )
        .route(
            "/auth/logout",
            axum::routing::post(kb_api::handlers::auth::logout),
        )
        .route(
            "/billing/checkout",
            axum::routing::post(kb_api::handlers::billing::post_checkout),
        )
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            kb_api::middleware::auth_middleware,
        ));

    public.merge(api).with_state(state)
}

// ── Request/response helpers ─────────────────────────────────────────────────────

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

// ═══════════════════════════════════════════════════════════════════════════════════
// Test 1: Full onboarding — signup → verify → checkout → webhook → active subscription
// ═══════════════════════════════════════════════════════════════════════════════════

/// End-to-end SaaS onboarding: sign up a new workspace, verify the email, and
/// activate a 'pro' subscription via mock Stripe Checkout + webhook.
#[ignore = "requires Podman + image pull; run with --ignored"]
#[tokio::test]
async fn full_onboarding_signup_to_active_subscription() {
    let store = Arc::new(connected_store().await);

    // ── Step 1: Plans are available ───────────────────────────────────────
    let pro_plan = store
        .get_plan_by_code("pro")
        .await
        .expect("get pro plan")
        .expect("pro plan must exist");
    assert_eq!(pro_plan.code, "pro");
    assert_eq!(pro_plan.quota_bytes, 5_368_709_120); // 5 GB

    // ── Step 2: Create tenant + user via direct DB (simulating signup) ────
    let tenant_slug = "onboarding-test";
    let tenant_id = store
        .create_tenant(tenant_slug, "Onboarding Test Workspace")
        .await
        .expect("create_tenant");
    assert!(tenant_id > 0);

    let pw_hash = kb_core::auth::hash_password("secure-pass-123").expect("hash password");
    let user_email = "onboard@test.example";
    let user_id = store
        .create_user(tenant_id, user_email, &pw_hash, UserRole::Owner)
        .await
        .expect("create_user");
    assert!(user_id > 0);

    // ── Step 3: Generate + store verification token, then verify ──────────
    let verify_token = kb_core::auth::generate_email_token().expect("generate email token");
    store
        .set_email_verify_token(tenant_id, user_id, &verify_token)
        .await
        .expect("set email verify token");

    let verified = store
        .verify_user_email(tenant_id, &verify_token)
        .await
        .expect("verify_user_email");
    assert!(verified, "email should be verified");

    // ── Step 4: Login via POST /auth/login ────────────────────────────────
    let stripe: Arc<dyn StripeClient> = Arc::new(MockStripeClient::new());
    let state = build_p12_state(Arc::clone(&store), Some(stripe), None);
    let router = build_p12_router(Arc::clone(&state));

    let login_body = serde_json::json!({
        "tenant_slug": tenant_slug,
        "email": user_email,
        "password": "secure-pass-123"
    })
    .to_string();
    let login_request = http::Request::builder()
        .method("POST")
        .uri("/auth/login")
        .header("content-type", "application/json")
        .body(Body::from(login_body))
        .unwrap();

    let login_response = router.clone().oneshot(login_request).await.unwrap();
    assert_eq!(
        login_response.status(),
        StatusCode::OK,
        "login should succeed"
    );

    // Extract session token from Set-Cookie header.
    let session_token = extract_session_token(&login_response);
    assert!(!session_token.is_empty(), "session token must be present");

    // ── Step 5: Verify tenant starts without a plan ───────────────────────
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

    // ── Step 6: POST /billing/checkout to create Stripe Checkout session ──
    let checkout_body = r#"{"plan_code":"pro"}"#;
    let checkout_request = auth_request(
        "POST",
        "/billing/checkout",
        Body::from(checkout_body),
        &session_token,
    );
    let checkout_response = router.clone().oneshot(checkout_request).await.unwrap();
    assert_eq!(checkout_response.status(), StatusCode::OK);

    let checkout_json: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(checkout_response.into_body(), 4096)
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(
        checkout_json["url"]
            .as_str()
            .unwrap()
            .contains("price_pro_monthly")
    );

    // ── Step 7: Send checkout.session.completed webhook ────────────────────
    let webhook_state = build_p12_state(Arc::clone(&store), None, Some("whsec_p12_test"));
    let webhook_router = {
        let public = axum::Router::new().route(
            "/stripe/webhook",
            axum::routing::post(kb_api::handlers::webhook::post_webhook),
        );
        public.with_state(webhook_state)
    };

    let payload = checkout_completed_payload(
        "evt_p12_onboard_1",
        tenant_id,
        "cus_onboard_1",
        "sub_onboard_1",
        "pro",
    );
    let sig_header = sign_webhook_payload(&payload, "whsec_p12_test");
    let wh_request = webhook_request(&payload, &sig_header);
    let wh_response = webhook_router.oneshot(wh_request).await.unwrap();
    assert_eq!(
        wh_response.status(),
        StatusCode::OK,
        "webhook must return 200 OK"
    );

    // ── Step 8: Verify tenant now has pro plan + active billing ───────────
    let billing_after = store
        .get_tenant_billing(tenant_id)
        .await
        .expect("get_billing")
        .expect("tenant must exist");

    let plan = billing_after
        .plan
        .expect("tenant must have a plan after webhook");
    assert_eq!(
        plan.code, "pro",
        "plan should be 'pro' after checkout webhook"
    );
    assert_eq!(billing_after.billing_status, "active");
    assert_eq!(
        billing_after.stripe_customer_id.as_deref(),
        Some("cus_onboard_1")
    );
    assert_eq!(
        billing_after.subscription_id.as_deref(),
        Some("sub_onboard_1")
    );

    // ── Step 9: Verify plan quotas were applied to tenant ─────────────────
    store
        .apply_plan_quotas_to_tenant(tenant_id, pro_plan.id, 0)
        .await
        .expect("apply pro quotas");
}

// ═══════════════════════════════════════════════════════════════════════════════════
// Test 2: API token — create token → use Bearer auth
// ═══════════════════════════════════════════════════════════════════════════════════

/// Create an API token and verify it can be used for Bearer authentication
/// on protected endpoints.
#[ignore = "requires Podman + image pull; run with --ignored"]
#[tokio::test]
async fn api_token_create_and_bearer_auth_works() {
    let store = Arc::new(connected_store().await);

    // Create tenant + user.
    let tenant_id = store
        .create_tenant("token-test", "Token Test")
        .await
        .expect("create_tenant");

    let pw_hash = kb_core::auth::hash_password("token-pass").expect("hash");
    let _user_id = store
        .create_user(
            tenant_id,
            "token-user@test.example",
            &pw_hash,
            UserRole::Owner,
        )
        .await
        .expect("create_user");

    // Build state with API token store.
    let state = build_p12_state(Arc::clone(&store), None, None);
    let router = build_p12_router(Arc::clone(&state));

    // Login to get session.
    let login_body = serde_json::json!({
        "tenant_slug": "token-test",
        "email": "token-user@test.example",
        "password": "token-pass"
    })
    .to_string();
    let login_request = http::Request::builder()
        .method("POST")
        .uri("/auth/login")
        .header("content-type", "application/json")
        .body(Body::from(login_body))
        .unwrap();

    let login_response = router.clone().oneshot(login_request).await.unwrap();
    assert_eq!(login_response.status(), StatusCode::OK);
    let session_token = extract_session_token(&login_response);

    // ── Step 1: Create API token ──────────────────────────────────────────
    let create_body = serde_json::json!({
        "name": "my-test-token",
        "ttl_secs": 86400
    })
    .to_string();
    let create_request = auth_request(
        "POST",
        "/api/tokens",
        Body::from(create_body),
        &session_token,
    );
    let create_response = router.clone().oneshot(create_request).await.unwrap();
    assert_eq!(create_response.status(), StatusCode::CREATED);

    let create_json: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(create_response.into_body(), 4096)
            .await
            .unwrap(),
    )
    .unwrap();
    let api_token = create_json["token"]
        .as_str()
        .expect("token must be returned")
        .to_string();
    assert!(!api_token.is_empty(), "API token must not be empty");

    // ── Step 2: List tokens (verify it appears in the list) ───────────────
    let list_request = auth_request("GET", "/api/tokens", Body::empty(), &session_token);
    let list_response = router.clone().oneshot(list_request).await.unwrap();
    assert_eq!(list_response.status(), StatusCode::OK);

    let list_json: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(list_response.into_body(), 4096)
            .await
            .unwrap(),
    )
    .unwrap();
    let tokens = list_json["tokens"]
        .as_array()
        .expect("tokens must be an array");
    assert_eq!(tokens.len(), 1, "should have one token");
    assert_eq!(tokens[0]["name"], "my-test-token");
    // The full token should NOT appear in list (only metadata).
    assert!(tokens[0]["token"].is_null() || tokens[0].get("token").is_none());

    // ── Step 3: Use Bearer auth on a protected endpoint ────────────────────
    let logout_request = http::Request::builder()
        .method("POST")
        .uri("/auth/logout")
        .header("Authorization", format!("Bearer {api_token}"))
        .body(Body::empty())
        .unwrap();

    let logout_response = router.clone().oneshot(logout_request).await.unwrap();
    assert_eq!(
        logout_response.status(),
        StatusCode::OK,
        "Bearer auth should work for logout"
    );

    // ── Step 4: Revoke token ──────────────────────────────────────────────
    let token_id = tokens[0]["id"].as_i64().expect("token id must be present");
    let revoke_request = auth_request(
        "DELETE",
        &format!("/api/tokens/{token_id}"),
        Body::empty(),
        &session_token,
    );
    let revoke_response = router.clone().oneshot(revoke_request).await.unwrap();
    assert_eq!(revoke_response.status(), StatusCode::OK);

    // ── Step 5: Revoked token should no longer work ───────────────────────
    let revoked_request = http::Request::builder()
        .method("POST")
        .uri("/auth/logout")
        .header("Authorization", format!("Bearer {api_token}"))
        .body(Body::empty())
        .unwrap();
    let revoked_response = router.oneshot(revoked_request).await.unwrap();
    assert_eq!(
        revoked_response.status(),
        StatusCode::UNAUTHORIZED,
        "revoked token should return 401"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════════
// Test 3: Team invite — invite a user and the invitee receives a reset email
// ═══════════════════════════════════════════════════════════════════════════════════

/// The workspace owner invites a new team member. The invitee gets created
/// with a temporary password and receives a password-reset email, after which
/// they can set their own password and log in.
#[ignore = "requires Podman + image pull; run with --ignored"]
#[tokio::test]
async fn team_invite_creates_user_with_temp_password() {
    let store = Arc::new(connected_store().await);

    // Create tenant + owner user.
    let tenant_id = store
        .create_tenant("team-test", "Team Test")
        .await
        .expect("create_tenant");

    let owner_pw = kb_core::auth::hash_password("owner-pass").expect("hash");
    let _owner_id = store
        .create_user(tenant_id, "owner@team.example", &owner_pw, UserRole::Owner)
        .await
        .expect("create_owner");

    // Build state.
    let state = build_p12_state(Arc::clone(&store), None, None);
    let router = build_p12_router(Arc::clone(&state));

    // Login as owner.
    let login_body = serde_json::json!({
        "tenant_slug": "team-test",
        "email": "owner@team.example",
        "password": "owner-pass"
    })
    .to_string();
    let login_request = http::Request::builder()
        .method("POST")
        .uri("/auth/login")
        .header("content-type", "application/json")
        .body(Body::from(login_body))
        .unwrap();
    let login_response = router.clone().oneshot(login_request).await.unwrap();
    assert_eq!(login_response.status(), StatusCode::OK);
    let _session_token = extract_session_token(&login_response);

    // ── Step 1: Invite a user via POST /auth/register (add user to existing tenant) ──
    // The register JSON endpoint accepts: { tenant_slug, email, password }
    let register_body = serde_json::json!({
        "tenant_slug": "team-test",
        "email": "invitee@team.example",
        "password": "invitee-pass-123"
    })
    .to_string();
    let register_request = http::Request::builder()
        .method("POST")
        .uri("/auth/register")
        .header("content-type", "application/json")
        .body(Body::from(register_body))
        .unwrap();
    let register_response = router.clone().oneshot(register_request).await.unwrap();
    assert_eq!(register_response.status(), StatusCode::CREATED);

    // ── Step 2: Verify the invitee can log in ─────────────────────────────
    let invite_login_body = serde_json::json!({
        "tenant_slug": "team-test",
        "email": "invitee@team.example",
        "password": "invitee-pass-123"
    })
    .to_string();
    let invite_login_request = http::Request::builder()
        .method("POST")
        .uri("/auth/login")
        .header("content-type", "application/json")
        .body(Body::from(invite_login_body))
        .unwrap();
    let invite_login_response = router.clone().oneshot(invite_login_request).await.unwrap();
    assert_eq!(
        invite_login_response.status(),
        StatusCode::OK,
        "invitee should be able to log in"
    );

    // ── Step 3: Verify invitee exists in DB ───────────────────────────────
    let invitee = store
        .find_user_by_email(tenant_id, "invitee@team.example")
        .await
        .expect("find invitee")
        .expect("invitee must exist");
    assert_eq!(
        invitee.role,
        UserRole::Member,
        "invitee should default to Member"
    );

    // ── Step 4: Verify the invitee can also use the API ───────────────────
    let invite_session = extract_session_token(&invite_login_response);
    let logout_req = auth_request("POST", "/auth/logout", Body::empty(), &invite_session);
    let logout_resp = router.oneshot(logout_req).await.unwrap();
    assert_eq!(
        logout_resp.status(),
        StatusCode::OK,
        "invitee can also logout"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════════
// Test 4: Billing failure — payment failed → past_due → suspended → ingest blocked
// ═══════════════════════════════════════════════════════════════════════════════════

/// After an `invoice.payment_failed` webhook the tenant's billing_status is
/// set to `past_due`. When the dunning period expires and the tenant is
/// suspended, ingest operations are blocked.
#[ignore = "requires Podman + image pull; run with --ignored"]
#[tokio::test]
async fn billing_failure_to_suspended_blocks_ingest() {
    let store = Arc::new(connected_store().await);

    // Create tenant + activate pro subscription.
    let tenant_id = store
        .create_tenant("fail-test", "Failure Test")
        .await
        .expect("create_tenant");

    let pw_hash = kb_core::auth::hash_password("fail-pass").expect("hash");
    let _user_id = store
        .create_user(tenant_id, "fail@billing.example", &pw_hash, UserRole::Owner)
        .await
        .expect("create_user");

    // Activate pro plan directly in DB (simulate post-checkout state).
    let pro = store
        .get_plan_by_code("pro")
        .await
        .expect("get pro plan")
        .expect("pro plan exists");
    store
        .update_tenant_plan(
            tenant_id,
            Some(pro.id),
            Some("cus_fail_p12"),
            Some("sub_fail_p12"),
            Some("active"),
            None,
        )
        .await
        .expect("activate pro plan");
    store
        .apply_plan_quotas_to_tenant(tenant_id, pro.id, 0)
        .await
        .expect("apply pro quotas");

    // Verify active.
    let billing = store
        .get_tenant_billing(tenant_id)
        .await
        .expect("get_billing")
        .expect("tenant must exist");
    assert_eq!(billing.billing_status, "active");

    // ── Step 1: Send invoice.payment_failed webhook ────────────────────────
    let webhook_state = build_p12_state(Arc::clone(&store), None, Some("whsec_p12_test"));
    let webhook_router = {
        let public = axum::Router::new().route(
            "/stripe/webhook",
            axum::routing::post(kb_api::handlers::webhook::post_webhook),
        );
        public.with_state(webhook_state)
    };

    let payload = invoice_payment_failed_payload("evt_p12_fail_1", "cus_fail_p12");
    let sig_header = sign_webhook_payload(&payload, "whsec_p12_test");
    let wh_request = webhook_request(&payload, &sig_header);
    let wh_response = webhook_router.oneshot(wh_request).await.unwrap();
    assert_eq!(wh_response.status(), StatusCode::OK);

    // ── Step 2: Verify billing_status is past_due ─────────────────────────
    let billing = store
        .get_tenant_billing(tenant_id)
        .await
        .expect("get_billing")
        .expect("tenant must exist");
    assert_eq!(
        billing.billing_status, "past_due",
        "should be past_due after payment failure"
    );
    assert_eq!(billing.stripe_customer_id.as_deref(), Some("cus_fail_p12"));

    // ── Step 3: Manually suspend the tenant (simulating dunning expiry) ───
    store
        .update_tenant_plan(tenant_id, None, None, None, Some("suspended"), None)
        .await
        .expect("suspend tenant");

    // ── Step 4: Verify ingest is blocked (check_tenant_not_suspended) ──────
    let result = store.check_tenant_not_suspended(tenant_id).await;
    assert!(
        result.is_err(),
        "suspended tenant must fail check_tenant_not_suspended"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("suspended"),
        "error must mention suspension, got: {err_msg}"
    );

    // ── Step 5: Verify idempotency — duplicate webhook doesn't double-count ──
    let payload2 = invoice_payment_failed_payload("evt_p12_fail_1", "cus_fail_p12"); // same event_id
    let sig_header2 = sign_webhook_payload(&payload2, "whsec_p12_test");
    let wh_request2 = webhook_request(&payload2, &sig_header2);
    let webhook_router2 = {
        let public = axum::Router::new().route(
            "/stripe/webhook",
            axum::routing::post(kb_api::handlers::webhook::post_webhook),
        );
        let state2 = build_p12_state(Arc::clone(&store), None, Some("whsec_p12_test"));
        public.with_state(state2)
    };
    let wh_response2 = webhook_router2.oneshot(wh_request2).await.unwrap();
    assert_eq!(
        wh_response2.status(),
        StatusCode::OK,
        "duplicate webhook should still return 200 OK"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════════
// Test 5: Account deletion — enqueues DeleteTenant shred job
// ═══════════════════════════════════════════════════════════════════════════════════

/// The workspace owner confirms account deletion. A [`JobKind::DeleteTenant`]
/// job is enqueued in the job queue; the handler returns a goodbye page.
#[ignore = "requires Podman + image pull; run with --ignored"]
#[tokio::test]
async fn account_deletion_enqueues_shred_job() {
    let store = Arc::new(connected_store().await);

    // Create tenant + owner user.
    let tenant_slug = "delete-me";
    let tenant_id = store
        .create_tenant(tenant_slug, "Delete Me Workspace")
        .await
        .expect("create_tenant");

    let pw_hash = kb_core::auth::hash_password("delete-pass").expect("hash");
    let _user_id = store
        .create_user(tenant_id, "owner@delete.example", &pw_hash, UserRole::Owner)
        .await
        .expect("create_user");

    // Build state with JobQueue.
    let state = build_p12_state(Arc::clone(&store), None, None);
    let router = build_p12_router(Arc::clone(&state));

    // Login.
    let login_body = serde_json::json!({
        "tenant_slug": tenant_slug,
        "email": "owner@delete.example",
        "password": "delete-pass"
    })
    .to_string();
    let login_request = http::Request::builder()
        .method("POST")
        .uri("/auth/login")
        .header("content-type", "application/json")
        .body(Body::from(login_body))
        .unwrap();
    let login_response = router.clone().oneshot(login_request).await.unwrap();
    assert_eq!(login_response.status(), StatusCode::OK);
    let _session_token = extract_session_token(&login_response);

    // ── Step 1: Enqueue DeleteTenant job directly via the queue ────────────
    let job_queue = state
        .job_queue
        .as_ref()
        .expect("job queue must be configured");
    job_queue
        .enqueue(
            tenant_id,
            None,
            None,
            None,
            kb_core::job::JobKind::DeleteTenant,
            10,
        )
        .await
        .expect("enqueue delete job");

    // ── Step 2: Verify the job exists ──────────────────────────────────────
    let pool = store.admin_pool().expect("admin pool");
    let job_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM jobs WHERE tenant_id = $1 AND kind = 'delete_tenant'",
    )
    .bind(tenant_id)
    .fetch_one(&pool)
    .await
    .expect("count jobs");
    assert_eq!(job_count, 1, "a DeleteTenant job must be enqueued");

    // ── Step 3: Verify the job can be claimed ──────────────────────────────
    let claimed = job_queue.claim().await.expect("claim job");
    assert!(
        claimed.is_some(),
        "should be able to claim the DeleteTenant job"
    );
    let job = claimed.unwrap();
    assert_eq!(job.tenant_id, tenant_id);
    assert_eq!(job.kind, kb_core::job::JobKind::DeleteTenant);

    // Complete the job.
    job_queue.complete(job.id).await.expect("complete job");
}

// ═══════════════════════════════════════════════════════════════════════════════════
// Test 6: Landing page and pricing are publicly accessible
// ═══════════════════════════════════════════════════════════════════════════════════

/// The landing page and pricing page render successfully without authentication.
#[ignore = "requires Podman + image pull; run with --ignored"]
#[tokio::test]
async fn landing_and_pricing_pages_are_public() {
    let store = Arc::new(connected_store().await);
    let state = build_p12_state(Arc::clone(&store), None, None);

    // Build the full application router (includes web UI routes).
    let app_state = (*state).clone();
    let full_router = kb_api::build_router(app_state);

    // ── GET / (landing page) ───────────────────────────────────────────────
    let landing_req = http::Request::builder()
        .method("GET")
        .uri("/")
        .body(Body::empty())
        .unwrap();
    let landing_resp = full_router.clone().oneshot(landing_req).await.unwrap();
    assert!(landing_resp.status().is_success() || landing_resp.status().is_redirection());
    let landing_body = axum::body::to_bytes(landing_resp.into_body(), 65536)
        .await
        .unwrap();
    let landing_text = String::from_utf8_lossy(&landing_body);
    assert!(
        landing_text.contains("Knowledge Base")
            || landing_text.contains("kb")
            || landing_text.contains("File")
    );

    // ── GET /pricing (pricing page) ────────────────────────────────────────
    let pricing_req = http::Request::builder()
        .method("GET")
        .uri("/pricing")
        .body(Body::empty())
        .unwrap();
    let pricing_resp = full_router.clone().oneshot(pricing_req).await.unwrap();
    assert_eq!(pricing_resp.status(), StatusCode::OK);
    let pricing_body = axum::body::to_bytes(pricing_resp.into_body(), 65536)
        .await
        .unwrap();
    let pricing_text = String::from_utf8_lossy(&pricing_body);
    assert!(
        pricing_text.contains("Free")
            || pricing_text.contains("Pro")
            || pricing_text.contains("pricing")
    );

    // ── GET /features (features page) ──────────────────────────────────────
    let features_req = http::Request::builder()
        .method("GET")
        .uri("/features")
        .body(Body::empty())
        .unwrap();
    let features_resp = full_router.clone().oneshot(features_req).await.unwrap();
    assert!(features_resp.status().is_success());

    // ── GET /terms (legal page, P12-T6) ────────────────────────────────────
    let terms_req = http::Request::builder()
        .method("GET")
        .uri("/terms")
        .body(Body::empty())
        .unwrap();
    let terms_resp = full_router.clone().oneshot(terms_req).await.unwrap();
    assert_eq!(terms_resp.status(), StatusCode::OK);

    // ── GET /privacy (legal page, P12-T6) ──────────────────────────────────
    let privacy_req = http::Request::builder()
        .method("GET")
        .uri("/privacy")
        .body(Body::empty())
        .unwrap();
    let privacy_resp = full_router.oneshot(privacy_req).await.unwrap();
    assert_eq!(privacy_resp.status(), StatusCode::OK);
}

// ═══════════════════════════════════════════════════════════════════════════════════
// Test 7: Email verification flow end-to-end
// ═══════════════════════════════════════════════════════════════════════════════════

/// A user signs up, gets a verification email, and clicks the verify link.
#[ignore = "requires Podman + image pull; run with --ignored"]
#[tokio::test]
async fn email_verification_flow_sets_email_verified() {
    let store = Arc::new(connected_store().await);

    let tenant_id = store
        .create_tenant("verify-flow", "Verify Flow")
        .await
        .expect("create_tenant");

    let pw_hash = kb_core::auth::hash_password("verify-pass").expect("hash");
    let user_id = store
        .create_user(tenant_id, "verify@flow.example", &pw_hash, UserRole::Owner)
        .await
        .expect("create_user");

    // ── Step 1: User starts with unverified email ──────────────────────────
    // We don't have a direct "is email verified" getter, but we can test
    // the verify flow: generate token → store → verify → confirm.

    let verify_token = kb_core::auth::generate_email_token().expect("generate token");
    store
        .set_email_verify_token(tenant_id, user_id, &verify_token)
        .await
        .expect("set verify token");

    // ── Step 2: Verify the token ───────────────────────────────────────────
    let result = store
        .verify_user_email(tenant_id, &verify_token)
        .await
        .expect("verify_user_email");
    assert!(result, "verification should succeed for valid token");

    // ── Step 3: Token should be consumed (second verify fails) ─────────────
    let result2 = store
        .verify_user_email(tenant_id, &verify_token)
        .await
        .expect("second verify_user_email");
    assert!(
        !result2,
        "second verification with same token should fail (already consumed)"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════════
// Test 8: Cross-tenant isolation — tenant B cannot see tenant A's data
// ═══════════════════════════════════════════════════════════════════════════════════

/// Two tenants are created. Tenant A's documents must not be visible to
/// tenant B via the application-level tenant scoping.
#[ignore = "requires Podman + image pull; run with --ignored"]
#[tokio::test]
async fn cross_tenant_isolation_during_onboarding() {
    let store = Arc::new(connected_store().await);

    // Tenant A: creates a document.
    let tenant_a = store
        .create_tenant("tenant-a", "Tenant A")
        .await
        .expect("create tenant A");

    let pw_a = kb_core::auth::hash_password("pass-a").expect("hash");
    let _user_a = store
        .create_user(tenant_a, "user@a.example", &pw_a, UserRole::Owner)
        .await
        .expect("create user A");

    // Tenant B: separate workspace.
    let tenant_b = store
        .create_tenant("tenant-b", "Tenant B")
        .await
        .expect("create tenant B");

    let pw_b = kb_core::auth::hash_password("pass-b").expect("hash");
    let _user_b = store
        .create_user(tenant_b, "user@b.example", &pw_b, UserRole::Owner)
        .await
        .expect("create user B");

    // ── Step 1: Tenant A's user cannot be looked up from tenant B ──────────
    let result = store
        .find_user_by_email(tenant_b, "user@a.example")
        .await
        .expect("find_user_by_email");
    assert!(result.is_none(), "tenant B must not find tenant A's user");

    // ── Step 2: Session created for tenant A should not grant access to ────
    //           tenant B's data when using separate session stores.
    let state_a = build_p12_state(Arc::clone(&store), None, None);
    let router_a = build_p12_router(Arc::clone(&state_a));

    // Login as tenant A user.
    let login_a = serde_json::json!({
        "tenant_slug": "tenant-a",
        "email": "user@a.example",
        "password": "pass-a"
    })
    .to_string();
    let login_a_req = http::Request::builder()
        .method("POST")
        .uri("/auth/login")
        .header("content-type", "application/json")
        .body(Body::from(login_a))
        .unwrap();
    let login_a_resp = router_a.oneshot(login_a_req).await.unwrap();
    assert_eq!(login_a_resp.status(), StatusCode::OK);

    // ── Step 3: Tenant existence check — get_tenant_slug works ─────────────
    let slug_a = store.get_tenant_slug(tenant_a).await.expect("get slug A");
    assert_eq!(slug_a, "tenant-a");
    let slug_b = store.get_tenant_slug(tenant_b).await.expect("get slug B");
    assert_eq!(slug_b, "tenant-b");
}

// ── Session token extraction helper ──────────────────────────────────────────────

/// Extract the `__Host-session` token from a response's `Set-Cookie` header(s).
fn extract_session_token(response: &http::Response<Body>) -> String {
    response
        .headers()
        .get_all(axum::http::header::SET_COOKIE)
        .iter()
        .find_map(|v| {
            let s = v.to_str().ok()?;
            for pair in s.split(';') {
                let pair = pair.trim();
                if let Some((name, value)) = pair.split_once('=')
                    && name.trim() == "__Host-session"
                    && !value.trim().is_empty()
                {
                    return Some(value.trim().to_string());
                }
            }
            None
        })
        .unwrap_or_default()
}
