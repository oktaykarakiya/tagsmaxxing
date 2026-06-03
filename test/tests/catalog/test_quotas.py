"""Catalog (step-2 stubs) — plan quotas, cost budgets, rate limits.

List with `pytest --collect-only -m quotas`.
"""

from __future__ import annotations

import pytest

from lib import config, flows

pytestmark = pytest.mark.quotas


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
