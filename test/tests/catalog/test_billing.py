"""Catalog (step-2) — Stripe billing lifecycle.

Webhook handling is a security-sensitive boundary (signature + idempotency).
List with `pytest --collect-only -m billing`.

These tests assert the *intended* billing contract (plan §29):

* `POST /billing/checkout` → a Stripe-hosted Checkout URL for a plan.
* `POST /stripe/webhook` → HMAC-SHA256 signature-verified and idempotent by
  `event_id`; drives the tenant lifecycle (active → past_due → …).
* `GET /billing/portal` → a Stripe Customer Portal URL for a subscribed tenant.
* plan quotas + read-only enforcement take effect once a subscription is active.

A note on what can be driven black-box: forging a *valid* Stripe signature (and
therefore exercising any webhook-driven side effect) requires the shared
`STRIPE_WEBHOOK_SECRET` that the app verifies against. When the harness is not
given that secret, the signed-webhook tests self-block with a clear reason rather
than asserting a weaker (and security-wrong) contract. The two tests that need no
secret — creating a Checkout session and *rejecting* a forged signature — always
run and assert the real contract.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import time
import uuid

import pytest

from lib import config, flows

pytestmark = pytest.mark.billing


# ── Local helpers ────────────────────────────────────────────────────────────
# The thin `ApiClient` (lib.api_client) only wraps a handful of endpoints; the
# billing/webhook surface needs raw requests with custom headers, status-code
# inspection, and authenticated HTML reads. `api._c` is the harness's underlying
# `httpx.Client`, already configured with the app base URL, the session cookie
# jar, and TLS-verification-off for Caddy's local cert — so we reuse it directly
# for the endpoints the wrapper does not expose.


def _webhook_secret() -> str:
    """The Stripe webhook signing secret, read from the environment at call time.

    Empty when unset. Producing a *valid* `stripe-signature` (and thus driving
    any webhook side effect) requires the same secret the app verifies against.
    """
    return (os.environ.get("STRIPE_WEBHOOK_SECRET") or "").strip()


def _require_webhook_secret() -> str:
    """Return the webhook secret, or block the test when it is not available.

    Skipping here marks the test *blocked* (not failed): a black-box client
    genuinely cannot forge a valid Stripe HMAC signature without the shared
    secret, and the e2e app is started without `STRIPE_WEBHOOK_SECRET`.
    """
    secret = _webhook_secret()
    if not secret:
        pytest.skip(
            "blocked: forging a valid Stripe webhook signature requires the shared "
            "STRIPE_WEBHOOK_SECRET, which this black-box harness does not expose "
            "(and the e2e app runs without it). Set STRIPE_WEBHOOK_SECRET for both "
            "the app and the harness to drive signed-webhook flows."
        )
    return secret


def _stripe_signature(body: bytes, secret: str, *, timestamp: int | None = None) -> str:
    """Build a Stripe `stripe-signature` header for `body`.

    Mirrors Stripe's scheme exactly (and the app's `verify_stripe_signature`):
    HMAC-SHA256 over ``"{t}.{raw_body}"``, hex-encoded, as ``t=<ts>,v1=<hex>``.
    """
    t = int(timestamp if timestamp is not None else time.time())
    signed = f"{t}.".encode() + body
    digest = hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()
    return f"t={t},v1={digest}"


def _post_webhook(api, event: dict, secret: str, *, signature: str | None = None):
    """POST a Stripe `event` to `/stripe/webhook` with a (default valid) signature.

    The body is signed over its exact bytes, so it is serialized once and sent
    raw via `content=` (re-serializing would change the bytes the HMAC covers).
    """
    body = json.dumps(event).encode("utf-8")
    sig = signature if signature is not None else _stripe_signature(body, secret)
    return api._c.post(
        "/stripe/webhook",
        content=body,
        headers={"content-type": "application/json", "stripe-signature": sig},
    )


def _checkout_completed_event(
    tenant_id: int,
    plan_code: str,
    *,
    customer: str,
    subscription: str,
    event_id: str | None = None,
) -> dict:
    """A `checkout.session.completed` event payload for `tenant_id` + `plan_code`.

    `client_reference_id` is the tenant id (set when the Checkout session is
    created); `metadata.plan_code` is the plan the webhook should activate.
    """
    return {
        "id": event_id or f"evt_{uuid.uuid4().hex}",
        "type": "checkout.session.completed",
        "data": {
            "object": {
                "client_reference_id": str(tenant_id),
                "customer": customer,
                "subscription": subscription,
                "metadata": {"plan_code": plan_code, "tenant_id": str(tenant_id)},
            }
        },
    }


def _payment_failed_event(customer: str, *, event_id: str | None = None) -> dict:
    """An `invoice.payment_failed` event for the given Stripe `customer`."""
    return {
        "id": event_id or f"evt_{uuid.uuid4().hex}",
        "type": "invoice.payment_failed",
        "data": {"object": {"customer": customer}},
    }


def _register_fresh(api) -> tuple[str, int]:
    """Register a brand-new tenant (unique slug) and log `api` in as it.

    Returns ``(slug, tenant_id)``. Used so each webhook test targets an isolated
    tenant whose billing state we can both mutate and observe.
    """
    slug = flows.unique_marker("bil")
    reg = api.register(slug, f"{slug}@e2e.test", "Pw-" + uuid.uuid4().hex)
    return slug, int(reg["tenant_id"])


def _activate_plan(api, secret: str, *, plan_code: str = "pro") -> tuple[int, str]:
    """Register a fresh tenant and activate `plan_code` via a signed webhook.

    Drives `checkout.session.completed`, asserting it is accepted. Returns
    ``(tenant_id, stripe_customer_id)`` for downstream lifecycle steps.
    """
    _, tenant_id = _register_fresh(api)
    customer = f"cus_{uuid.uuid4().hex[:18]}"
    subscription = f"sub_{uuid.uuid4().hex[:18]}"
    r = _post_webhook(
        api,
        _checkout_completed_event(
            tenant_id, plan_code, customer=customer, subscription=subscription
        ),
        secret,
    )
    assert r.status_code == 200, (
        "checkout.session.completed webhook should activate the subscription "
        f"(200), got {r.status_code}: {r.text[:200]}"
    )
    return tenant_id, customer


def _account_html(api) -> str:
    """Fetch the authenticated `/account` page HTML (plan badge + quota bars)."""
    r = api._c.get("/account")
    assert r.status_code == 200, f"GET /account failed: {r.status_code} {r.text[:200]}"
    return r.text


def _visible(html: str, text: str) -> bool:
    """True when `text` appears as an HTML element's visible text (``>text<``)."""
    return re.search(r">\s*" + re.escape(text) + r"\s*<", html) is not None


# ── Checkout ─────────────────────────────────────────────────────────────────


def test_checkout_session_created(api):
    """POST /billing/checkout returns a Stripe Checkout URL for a plan."""
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    r = api._c.post("/billing/checkout", json={"plan_code": "pro"})
    assert r.status_code == 200, (
        "POST /billing/checkout should create a Checkout Session (200), got "
        f"{r.status_code}: {r.text[:300]}"
    )
    body = r.json()
    url = body.get("url", "")
    assert isinstance(url, str) and url.startswith("https://"), (
        f"checkout response should carry an https Stripe Checkout URL, got: {body!r}"
    )
    assert "stripe.com" in url, f"expected a Stripe-hosted Checkout URL, got: {url!r}"


# ── Webhook: signature verification ──────────────────────────────────────────


def test_webhook_signature_verified(api):
    """A correctly-signed Stripe webhook is accepted (200)."""
    secret = _require_webhook_secret()
    _, tenant_id = _register_fresh(api)

    event = _checkout_completed_event(
        tenant_id,
        "pro",
        customer=f"cus_{uuid.uuid4().hex[:18]}",
        subscription=f"sub_{uuid.uuid4().hex[:18]}",
    )
    r = _post_webhook(api, event, secret)
    assert r.status_code == 200, (
        "a correctly-signed Stripe webhook must be accepted (200), got "
        f"{r.status_code}: {r.text[:200]}"
    )


def test_webhook_invalid_signature_rejected(api):
    """A webhook with an invalid signature is rejected (400/401)."""
    event = {
        "id": f"evt_{uuid.uuid4().hex}",
        "type": "invoice.paid",
        "data": {"object": {"customer": "cus_forged"}},
    }
    body = json.dumps(event).encode("utf-8")
    # A syntactically well-formed header carrying a cryptographically wrong v1.
    forged = "t=1700000000,v1=" + "0" * 64

    r = api._c.post(
        "/stripe/webhook",
        content=body,
        headers={"content-type": "application/json", "stripe-signature": forged},
    )
    assert r.status_code in (400, 401), (
        "a forged Stripe signature must be rejected (400/401) and never processed, "
        f"got {r.status_code}: {r.text[:200]}"
    )


def test_webhook_idempotent_by_event_id(api):
    """Replaying the same event_id does not double-apply the effect."""
    secret = _require_webhook_secret()
    _, tenant_id = _register_fresh(api)

    event_id = f"evt_{uuid.uuid4().hex}"
    customer = f"cus_{uuid.uuid4().hex[:18]}"

    # First delivery: activate the 'pro' plan.
    first = _checkout_completed_event(
        tenant_id,
        "pro",
        customer=customer,
        subscription=f"sub_{uuid.uuid4().hex[:18]}",
        event_id=event_id,
    )
    r1 = _post_webhook(api, first, secret)
    assert r1.status_code == 200, f"first delivery should be processed, got {r1.status_code}"

    # Replay the SAME event_id with a payload that *would* upgrade to 'team'.
    # Idempotency must drop the replay: the tenant stays on 'pro'.
    replay = _checkout_completed_event(
        tenant_id,
        "team",
        customer=customer,
        subscription=f"sub_{uuid.uuid4().hex[:18]}",
        event_id=event_id,
    )
    r2 = _post_webhook(api, replay, secret)
    assert r2.status_code == 200, (
        f"a replayed event_id should be idempotently acknowledged (200), got {r2.status_code}"
    )

    html = _account_html(api)
    assert _visible(html, "pro"), (
        "after a duplicate delivery the tenant's plan should remain 'pro'"
    )
    assert not _visible(html, "team"), (
        "a replayed event_id must not re-apply — the plan must not change to 'team'"
    )


# ── Subscription lifecycle ───────────────────────────────────────────────────


def test_subscription_activated_on_checkout(api):
    """checkout.session.completed activates the tenant's plan."""
    secret = _require_webhook_secret()
    _activate_plan(api, secret, plan_code="pro")

    html = _account_html(api)
    assert _visible(html, "Active"), (
        "after checkout completion the account billing badge should read 'Active'"
    )
    assert _visible(html, "pro"), (
        "the account page should reflect the activated 'pro' plan"
    )


def test_payment_failed_triggers_dunning(api):
    """A failed payment moves the tenant toward past_due."""
    secret = _require_webhook_secret()
    _, customer = _activate_plan(api, secret, plan_code="pro")

    r = _post_webhook(api, _payment_failed_event(customer), secret)
    assert r.status_code == 200, (
        "invoice.payment_failed webhook should be accepted (200), got "
        f"{r.status_code}: {r.text[:200]}"
    )

    html = _account_html(api)
    assert _visible(html, "Past Due"), (
        "after a failed payment the account billing badge should read 'Past Due'"
    )


def test_past_due_blocks_ingest(api):
    """A past_due/suspended tenant cannot ingest (403)."""
    secret = _require_webhook_secret()
    _, customer = _activate_plan(api, secret, plan_code="pro")

    # Drive the tenant into past_due via a failed payment.
    r = _post_webhook(api, _payment_failed_event(customer), secret)
    assert r.status_code == 200, (
        f"invoice.payment_failed webhook should be accepted, got {r.status_code}"
    )

    # A tenant not in good standing must be read-only: ingest is rejected (403).
    files = [("files", ("blocked.txt", b"past-due tenant should not ingest", "text/plain"))]
    ingest = api._c.post("/api/ingest", files=files)
    assert ingest.status_code == 403, (
        "a past_due tenant must be blocked from ingest (403), got "
        f"{ingest.status_code}: {ingest.text[:200]}"
    )


# ── Customer portal ──────────────────────────────────────────────────────────


def test_customer_portal_link(api):
    """GET /billing/portal returns a Stripe Customer Portal URL."""
    secret = _require_webhook_secret()
    # A subscribed tenant has a stripe_customer_id (set by the checkout webhook),
    # which is the precondition for a Customer Portal session.
    _activate_plan(api, secret, plan_code="pro")

    r = api._c.get("/billing/portal")
    assert r.status_code == 200, (
        "GET /billing/portal should return a Customer Portal session (200), got "
        f"{r.status_code}: {r.text[:200]}"
    )
    url = r.json().get("url", "")
    assert isinstance(url, str) and url.startswith("https://"), (
        f"portal response should carry an https Customer Portal URL, got: {url!r}"
    )
    assert "stripe.com" in url, f"expected a Stripe-hosted Customer Portal URL, got: {url!r}"


# ── Plan quotas ──────────────────────────────────────────────────────────────


def test_plan_quotas_applied(api):
    """After activation, the plan's storage/cost quotas take effect."""
    secret = _require_webhook_secret()
    _, tenant_id = _register_fresh(api)

    # A fresh tenant has not yet adopted the Pro storage quota (5.0 GB).
    before = _account_html(api)
    assert "5.0 GB" not in before, (
        "a fresh, unsubscribed tenant should not yet show the Pro storage quota"
    )

    customer = f"cus_{uuid.uuid4().hex[:18]}"
    event = _checkout_completed_event(
        tenant_id,
        "pro",
        customer=customer,
        subscription=f"sub_{uuid.uuid4().hex[:18]}",
    )
    r = _post_webhook(api, event, secret)
    assert r.status_code == 200, (
        f"checkout.session.completed webhook should be accepted, got {r.status_code}"
    )

    # The Pro plan's 5 GB storage cap must now be reflected on the account page.
    after = _account_html(api)
    assert "5.0 GB" in after, (
        "after activating the Pro plan the account page should show the Pro "
        "storage quota (5.0 GB)"
    )
