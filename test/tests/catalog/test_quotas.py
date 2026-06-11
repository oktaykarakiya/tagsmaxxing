"""Catalog — plan quotas, cost budgets, rate limits.

The blocked stubs document gates that were not yet wired; the live tests
(``test_*_quota_*_metric``) drive the P14 enforcement path end-to-end against a
dedicated low-cap tenant (the ``limits_account`` fixture, ~1 MB storage / ~1000
tokens) and assert BOTH the HTTP rejection AND that the matching Prometheus
rejection counter on the public ``/metrics`` endpoint moved.

List with `pytest --collect-only -m quotas`.
"""

from __future__ import annotations

import re

import httpx
import pytest

from lib import config, flows

pytestmark = pytest.mark.quotas

# ── /metrics counter-parse helper ────────────────────────────────────────────
# Shared by the quota-enforcement tests below: scrape the public, unauthenticated
# /metrics endpoint and read a single Prometheus counter value (optionally
# label-filtered) so a test can measure the before/after delta of, e.g.,
# kb_quota_rejections_total{limit="tokens"}.

# A Prometheus data line: name{labels}? value (value may be int/float/exp).
_METRIC_LINE_RE = re.compile(r"^([A-Za-z_:][\w:]*)(\{[^}]*\})?\s+([0-9eE.+-]+)$")


def _metrics_text() -> str:
    """Fetch the public ``/metrics`` exposition once (no auth; raises on non-200)."""
    with httpx.Client(
        base_url=config.base_url(), verify=config.verify_tls(), timeout=20.0
    ) as c:
        r = c.get("/metrics")
        assert r.status_code == 200, f"/metrics not 200: {r.status_code} {r.text[:200]}"
        return r.text


def _counter_value(text: str, name: str, **labels: str) -> float:
    """Sum the value of every ``name`` sample whose labels are a superset of ``labels``.

    A Prometheus counter family the app has never observed is omitted from
    ``/metrics`` entirely, so an absent series reads as ``0.0`` — the convention
    the rest of the harness uses. When ``labels`` are given, only data lines whose
    label set contains every requested ``key="value"`` pair are counted, so
    ``kb_quota_rejections_total{limit="tokens"}`` is isolated from the
    ``limit="storage"`` series sharing the family.
    """
    total = 0.0
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = _METRIC_LINE_RE.match(line)
        if not m or m.group(1) != name:
            continue
        lbls = m.group(2) or ""
        if all(f'{k}="{v}"' in lbls for k, v in labels.items()):
            try:
                total += float(m.group(3))
            except ValueError:
                pass
    return total


@pytest.mark.skip(
    reason="blocked: no HTTP endpoint to assign a plan to a tenant (only Stripe "
    "webhook does this); new tenants are grandfathered with plan_id=NULL → "
    "unlimited storage, so the ingest handler's check_plan_storage_quota always "
    "passes. An admin endpoint for setting plan_id or quota_override_bytes is "
    "needed before this test can be driven."
)
def test_storage_quota_enforced_413(api):
    """Uploading past the plan's storage quota returns 413."""
    # This test asserts intended behaviour:
    #   POST /api/ingest with a file whose bytes would push total storage over the
    #   tenant's plan quota → 413 Payload Too Large with error "storage_quota_exceeded".
    #
    # To reach the check_plan_storage_quota code path, the tenant must have a
    # plan_id that resolves to a finite quota_bytes. The only way to assign a plan
    # today is via the Stripe webhook (checkout.session.completed →
    # update_tenant_plan), which requires a valid Stripe signature. Without an
    # admin HTTP endpoint for plan assignment or quota-override management, this
    # test cannot exercise the quota enforcement path.
    ...


def test_quota_usage_reported_on_dashboard(page):
    """The dashboard shows storage used vs the plan quota."""
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()

    # Log in through the real browser (CSRF cookie is handled).
    flows.browser_login(page, tenant, email, password)

    # Navigate to the dashboard.
    page.goto("/dashboard")
    page.wait_for_load_state("networkidle")

    # The page title is "Dashboard".
    assert page.locator("h1").first.text_content() == "Dashboard", (
        "dashboard page should have an h1 heading 'Dashboard'"
    )

    # The plan name is shown in the sub-header (e.g. "Default Tenant · Free Plan").
    plan_header = page.locator("text=Plan").first
    plan_header.wait_for(state="visible", timeout=10_000)
    plan_text = plan_header.text_content()
    assert "Plan" in plan_text, f"dashboard header should mention the plan; got: {plan_text!r}"

    # Storage quota bar: label "Storage", a used/limit display, and a percent label.
    page.locator("text=Storage").first.wait_for(state="visible", timeout=5_000)
    storage_bar_container = page.locator("text=Storage").first.locator("..")
    usage_text = storage_bar_container.text_content()
    # The bar should show a usage figure (e.g. "0 B / Unlimited" or "2.5 GB / 5.0 GB").
    assert "/" in usage_text, (
        f"storage bar should display 'used / limit'; got: {usage_text!r}"
    )

    # Token quota bar: label "Tokens (this month)", used/limit display, percent label.
    page.locator("text=Tokens (this month)").first.wait_for(state="visible", timeout=5_000)

    # Percent-of-quota label is rendered below each bar.
    page.locator("text=of plan quota").first.wait_for(state="visible", timeout=5_000)
    page.locator("text=of plan budget").first.wait_for(state="visible", timeout=5_000)

    # Stat cards are present: Documents, Files, Tags, Spend (this month).
    for label in ("Documents", "Files", "Tags", "Spend (this month)"):
        page.locator(f"text={label}").first.wait_for(state="visible", timeout=5_000)

    # The document count card shows a number (could be 0 or more).
    doc_card = page.locator("text=Documents").first.locator("..")
    doc_num = doc_card.locator("p.text-2xl").first.text_content()
    assert doc_num is not None and doc_num.strip() != "", "document count should be displayed"


@pytest.mark.skip(
    reason="blocked: no HTTP endpoint to change a tenant's plan or quota override; "
    "plan assignment only happens via Stripe webhooks (checkout.session.completed / "
    "customer.subscription.updated → update_tenant_plan). An admin API endpoint for "
    "setting plan_id or quota_override_bytes on a tenant is needed before this test "
    "can verify that the enforced quota updates atomically with the plan change."
)
def test_plan_change_updates_quota(api):
    """Changing plan changes the enforced storage quota."""
    # This test asserts intended behaviour:
    #   1. Register a tenant on the free plan (50 MB storage, 10K tokens).
    #   2. Upload a file just under 50 MB → 202 (within quota).
    #   3. Change to a higher plan (e.g. pro, 5 GB) via admin or billing.
    #   4. Upload the same size file again → 202 (now well within the new quota).
    #
    # Without an HTTP endpoint to assign or change plans, steps 3-4 cannot be
    # exercised. The Stripe webhook is the only plan-assignment path, and it
    # requires a valid HMAC signature.
    ...


@pytest.mark.skip(
    reason="blocked: check_plan_token_budget (plan_enforcement.rs) is defined but "
    "not wired into any HTTP handler — no endpoint calls it. The ingest handler only "
    "enforces storage quota; search/rerank/embed handlers do not check token budget. "
    "Wiring token-budget checks into the pipeline entry points is needed before this "
    "test can verify that exceeding the cost budget returns 403."
)
def test_cost_budget_exceeded_403(api):
    """Exceeding the per-tenant cost budget blocks further LLM work (403)."""
    # This test asserts intended behaviour:
    #   When the tenant's cumulative token usage (or cost_micros spend) exceeds the
    #   plan's token_budget, further LLM-consuming operations (ingest, re-tag,
    #   re-embed, search with rerank) should return 403 Forbidden with an error
    #   indicating the budget is exhausted and suggesting an upgrade.
    #
    # The pure math (check_token_quota) and the DB query (check_plan_token_budget)
    # are implemented, but no handler calls check_plan_token_budget. The ingest
    # handler only calls check_plan_storage_quota. Until token-budget enforcement
    # is wired into at least one handler, this test cannot drive the code path.
    ...


@pytest.mark.skip(
    reason="blocked: no per-endpoint rate limiting middleware exists. The "
    "InflightLimiter (backpressure.rs) caps concurrent ingest requests but is not "
    "a per-endpoint rate limiter — it controls in-flight concurrency, not request "
    "frequency. The plan features define 'rate_caps' (e.g. 5/min for free, 30/min "
    "for pro) but they are not enforced. A rate-limiting middleware or Tower layer "
    "is needed before this test can verify 429 responses at the plan-defined caps."
)
def test_rate_limit_per_endpoint(api):
    """Hitting an endpoint past its rate limit is throttled (429)."""
    # This test asserts intended behaviour:
    #   Repeated requests to a rate-limited endpoint within the plan's window should
    #   eventually receive 429 Too Many Requests with a Retry-After header. The rate
    #   cap is defined per plan in plans.features.rate_caps (e.g. "per_minute": 5
    #   for the free plan).
    #
    # The plan seed data includes rate_caps JSON, but no middleware reads or enforces
    # them. The only backpressure mechanism is InflightLimiter (concurrent ingest
    # cap), which is a different concern (concurrency, not frequency).
    ...


@pytest.mark.skip(
    reason="blocked: no HTTP API to set job priority or observe priority-based "
    "ordering. The scheduler has a PriorityWaiterQueue internally, but job priority "
    "is set in calls to JobQueue::enqueue(tenant_id, ..., priority) which uses "
    "hardcoded values (100 for ingest, 10 for deletion). There is no API endpoint "
    "to submit jobs with arbitrary priority or to inspect which waiter gets woken "
    "first. An admin endpoint that allows submitting test jobs with configurable "
    "priority, or an observable priority signal, is needed before this test can "
    "verify priority-ordered claiming."
)
def test_per_user_job_priority(api):
    """Jobs from higher-priority users are claimed ahead of others."""
    # This test asserts intended behaviour:
    #   When two tenants (or two users in the same tenant) have different billing
    #   plan priority tiers, the scheduler's PriorityWaiterQueue should wake the
    #   higher-priority waiter first, and higher-priority jobs should be dequeued
    #   from the job queue ahead of lower-priority ones.
    #
    # The scheduler infrastructure (PriorityWaiterQueue, job priority column) exists,
    # but priority is set to hardcoded constants (100 for ingest, 10 for deletion)
    # with no HTTP-accessible way to vary it per tenant or per submission. There is
    # also no endpoint to observe which job was claimed first in a multi-tenant
    # contention scenario.
    ...


# ═══════════════════════════════════════════════════════════════════════════════
# Live quota enforcement (P14) — API status + /metrics rejection counter
#
# Each test drives the dedicated low-cap tenant (limits_account: ~1 MB storage /
# ~1000 tokens) into a single quota and asserts (a) the correct HTTP status with
# the documented error code AND (b) that the matching kb_quota_rejections_total
# series on the public /metrics endpoint incremented (scraped before/after).
# Usage is reset between cases via the fixture's reset_usage() so caps trip
# predictably. A FAILED row here is a real enforcement/observability bug — left
# failing, never weakened.
# ═══════════════════════════════════════════════════════════════════════════════


def _raw_ingest(client, filename: str, content: bytes, note: str | None = None):
    """POST one file to ``/api/ingest`` and return the raw response (never raises).

    Unlike :meth:`lib.api_client.ApiClient.ingest_text`, this does not assert a
    202 — quota tests need to inspect the 413/429 rejection status and body, which
    the convenience wrapper turns into an exception.
    """
    files = [("files", (filename, content, "text/plain"))]
    data = {"user_note": note} if note else {}
    return client._c.post("/api/ingest", files=files, data=data)


def test_storage_quota_413_increments_rejection_metric(limits_account):
    """Over-storage ingest → 413 storage_quota_exceeded AND kb_quota_rejections_total{limit=storage} rises.

    The ingest storage check runs before the token check, so a single upload past
    the tenant's ~1 MB cap deterministically trips storage (P14-T4): 413 Payload
    Too Large with error ``storage_quota_exceeded`` and an upsell message, and the
    ``kb_quota_rejections_total{limit="storage"}`` counter increments.
    """
    acct = limits_account
    acct.reset_usage()  # clean slate: rollup + stored bytes at zero

    before = _counter_value(_metrics_text(), "kb_quota_rejections_total", limit="storage")

    # A payload comfortably over the ~1 MB storage cap (well under the 100 MiB
    # request limit, so it is the plan quota — not the payload limit — that fires).
    marker = flows.unique_marker("storquota")
    oversized = (f"storage-quota probe {marker} ".encode() * 70_000)  # ~1.7 MB
    assert len(oversized) > acct.quota_bytes, "probe must exceed the clamped storage cap"
    resp = _raw_ingest(acct.client, f"{marker}.txt", oversized)

    assert resp.status_code == 413, (
        f"over-storage ingest must return 413 Payload Too Large, got "
        f"{resp.status_code}: {resp.text[:300]}"
    )
    body = resp.json()
    assert body.get("error") == "storage_quota_exceeded", (
        f"413 body must carry error 'storage_quota_exceeded'; got: {body}"
    )

    after = _counter_value(_metrics_text(), "kb_quota_rejections_total", limit="storage")
    assert after > before, (
        f"kb_quota_rejections_total{{limit=\"storage\"}} did not increment "
        f"(before={before}, after={after}); the storage 413 path must record the "
        "rejection so operators can alert on tenants who should upgrade."
    )


def test_token_budget_429_increments_rejection_metric(limits_account):
    """Over-budget ingest → 429 token_budget_exceeded AND kb_quota_rejections_total{limit=tokens} rises.

    The monthly token budget is a UX-first hard block: once the tenant's rollup
    has met/exceeded its ~1000-token cap, the *next* ingest is rejected with 429
    ``token_budget_exceeded`` (P14-T4). We burn budget with successive small text
    ingests (each far under the 1 MB storage cap, so storage never trips), then
    confirm the rejection carries the right status/code and that
    ``kb_quota_rejections_total{limit="tokens"}`` incremented — while the storage
    series did not move (this case is a token rejection, not a storage one).
    """
    acct = limits_account
    acct.reset_usage()  # rollup back to zero so the cap is crossed by our ingests

    base = _metrics_text()
    tokens_before = _counter_value(base, "kb_quota_rejections_total", limit="tokens")
    storage_before = _counter_value(base, "kb_quota_rejections_total", limit="storage")

    # Each real ingest meters tagger + per-chunk embedding tokens; a handful of
    # modest docs crosses the ~1000-token cap. Keep every payload tiny so storage
    # stays far under 1 MB and only the token budget can reject. Bounded loop so a
    # mis-wired budget gate fails fast instead of looping forever.
    rejection = None
    accepted = 0
    for i in range(12):
        marker = flows.unique_marker("tokquota")
        # A few sentences → multiple chunks → enough embed+tag tokens to add up.
        body_text = (
            f"Token budget probe {i} {marker}. "
            "Quarterly revenue, invoices, forecasts, vendors, and reconciliation "
            "notes for the finance archive. " * 8
        ).encode()
        assert len(body_text) < acct.quota_bytes, "probe must stay under the storage cap"
        resp = _raw_ingest(acct.client, f"{marker}.txt", body_text)
        if resp.status_code == 429:
            rejection = resp
            break
        assert resp.status_code == 202, (
            f"pre-budget ingest #{i} should be accepted (202); got "
            f"{resp.status_code}: {resp.text[:300]}"
        )
        accepted += 1
        # Queued ingestion (P15): tokens are metered when the worker processes
        # the job, not at the 202 — wait for processing so the rollup the gate
        # point-selects reflects this probe before the next admission attempt.
        rbody = resp.json()
        flows.wait_until_ready(acct.client, rbody["document_id"], job_id=rbody.get("job_id"))

    assert rejection is not None, (
        f"token budget was never enforced after {accepted} accepted ingests under a "
        f"~{acct.quota_tokens}-token cap; an over-budget tenant's next ingest must "
        "return 429 token_budget_exceeded."
    )
    rbody = rejection.json()
    assert rbody.get("error") == "token_budget_exceeded", (
        f"429 body must carry error 'token_budget_exceeded'; got: {rbody}"
    )

    after = _metrics_text()
    tokens_after = _counter_value(after, "kb_quota_rejections_total", limit="tokens")
    storage_after = _counter_value(after, "kb_quota_rejections_total", limit="storage")
    assert tokens_after > tokens_before, (
        f"kb_quota_rejections_total{{limit=\"tokens\"}} did not increment "
        f"(before={tokens_before}, after={tokens_after}); the token 429 path must "
        "record the rejection."
    )
    assert storage_after == storage_before, (
        f"a token-budget rejection must not touch the storage series "
        f"(before={storage_before}, after={storage_after}); the counter labels must "
        "distinguish which quota was hit."
    )
