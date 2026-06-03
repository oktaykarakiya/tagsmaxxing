"""Catalog — enterprise billing & subscription correctness.

Beyond the covered checkout/webhook-signature/idempotency/dunning basics. A code
audit found real enforcement gaps (seat limit surfaces as an opaque 500; plan
token-budget + rate-caps defined but never enforced) and missing enterprise paths
(invoice/PO, annual, tax, trials). Note: the e2e stack runs Stripe **unconfigured**,
so signed-webhook flows are BLOCKED; card-free assertions (seat limit, metering,
rate-cap) are PENDING.
"""

from __future__ import annotations

import json
import re
import time

import pytest

from lib import config, flows

pytestmark = pytest.mark.billing


# ── Local helpers (kept in this file — see harness rules) ────────────────────────

# A substantial document so the inline ingest pipeline (tag + embed) consumes a
# clearly-measurable number of LLM tokens. ``{marker}`` is filled by ingest_and_wait.
METERING_DOC = (
    "Quarterly operations and finance review for the e2e knowledge base. "
    "This memo covers revenue recognition, invoice reconciliation, vendor "
    "purchase orders, headcount planning, and the cloud infrastructure budget "
    "for the upcoming fiscal period. It discusses risk management, compliance "
    "obligations, data-retention policy, and the roadmap for the analytics "
    "platform. The engineering section reviews ingestion throughput, embedding "
    "model selection, retrieval quality, and observability dashboards. "
    "Marketing notes summarize campaign performance and customer acquisition "
    "cost. Unique end-to-end metering marker: {marker}."
) * 3

# Far exceeds every seeded plan's per-minute cap (free 5, pro 30, team 100), so a
# correctly-enforced cap would throttle long before the burst completes.
RATE_BURST = 120


def _fmt_bytes(n: int) -> str:
    """Mirror the app's ``format_bytes`` (handlers_account.rs).

    The dashboard renders the token quota bar's ``used`` value through
    ``format_bytes`` (a 1024-based KB/MB/GB scale), so the displayed token figure
    is byte-formatted; we replicate it here to compare against the recorded total.
    """
    n = int(n)
    kb, mb, gb = 1024, 1024**2, 1024**3
    if n >= gb:
        return f"{n / gb:.1f} GB"
    if n >= mb:
        return f"{n / mb:.1f} MB"
    if n >= kb:
        return f"{n / kb:.1f} KB"
    return f"{n} B"


def _parse_bytes(s: str) -> float:
    """Parse a ``format_bytes``-style display string (e.g. '0 B', '9.8 KB', '1.0 MB')."""
    s = s.strip().replace(",", "")
    m = re.fullmatch(r"([0-9]*\.?[0-9]+)\s*([KMG]?B)", s, re.IGNORECASE)
    assert m, f"unparseable usage display: {s!r}"
    unit = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3}[m.group(2).upper()]
    return float(m.group(1)) * unit


def _dashboard_token_used(html: str) -> str:
    """The rendered 'used' side of the dashboard's 'Tokens (this month)' bar."""
    m = re.search(
        r"Tokens \(this month\)</span>\s*<span[^>]*>([^<]+)</span>",
        html,
        re.IGNORECASE | re.DOTALL,
    )
    assert m, "could not locate the 'Tokens (this month)' usage figure on the dashboard"
    return m.group(1).split("/")[0].strip()


def _dashboard_recorded_tokens(html: str) -> int:
    """Exact monthly token total from the dashboard's embedded daily-usage JSON.

    The chart is fed the per-day prompt/completion breakdown of ``usage_events``
    for the current month — the same rows that back the headline figure — so its
    sum is the exact recorded consumption.
    """
    m = re.search(r"var raw = (\[.*?\]);", html, re.DOTALL)
    assert m, "could not locate the daily-usage chart data on the dashboard"
    points = json.loads(m.group(1))
    return sum(int(p.get("prompt", 0)) + int(p.get("completion", 0)) for p in points)


def _assert_display_matches_recorded(used_display: str, recorded: int) -> None:
    """The displayed token figure must match the exact recorded usage_events total.

    Both come from the same monthly ``usage_events`` sum (the headline bar via
    ``get_monthly_token_usage``, the chart via ``get_daily_token_usage``), so they
    must agree up to the display's rounding step.
    """
    shown = _parse_bytes(used_display)
    # format_bytes rounds to a 0.1 KB/MB/GB step; allow that rounding plus a margin.
    tol = max(1.0, recorded * 0.06)
    assert abs(shown - recorded) <= tol, (
        f"dashboard shows {used_display!r} (≈ {shown:.0f}) but the recorded "
        f"usage_events total is {recorded} — the metered figure shown does not match "
        f"actual consumption (expected ≈ {_fmt_bytes(recorded)})"
    )


def test_seat_limit_exceeded_shows_upgrade_not_500(page):
    """Inviting beyond the plan's seat limit is a clear 4xx upsell, not a generic 500.

    Audit: ``check_tenant_max_users`` is enforced but ``account_invite_user`` maps the
    error to a generic 500 — indistinguishable from a DB failure, no upsell. On a
    free tenant (max_users=1), invite a member and assert the response is a
    seat-limit/upgrade message, not a 500.

    A self-serve signup creates a brand-new free-tier workspace, which the pricing
    page advertises as a single-user (1 seat) plan. Inviting a *second* member must
    therefore be refused with a clear upgrade prompt — never an opaque 500, and never
    a silent success that hands out an extra seat for free.
    """
    marker = flows.unique_marker()
    # The workspace name is all lowercase-alphanumeric, so the auto-generated slug
    # equals the marker (slugify is identity here) — we log in with that slug.
    workspace = marker
    owner_email = f"{marker}-owner@e2e.test"
    invitee_email = f"{marker}-invitee@e2e.test"
    password = f"Seat-{marker}-pw"  # >= 8 chars

    # ── Create the free-tier workspace (its owner is the only/first user). ──
    page.goto("/signup")
    page.fill("input[name='tenant_name']", workspace)
    page.fill("input[name='email']", owner_email)
    page.fill("input[name='password']", password)
    page.click("form[action='/signup'] button[type='submit']")
    page.wait_for_load_state("networkidle")
    assert "check your email" in page.content().lower(), (
        "signup did not create the workspace (precondition failed); cannot test the seat limit"
    )

    # ── Log in as the owner and open the team-management page. ──
    flows.browser_login(page, workspace, owner_email, password)
    page.goto("/account/team")
    page.wait_for_selector("form[action='/account/team/invite']", timeout=30_000)

    # ── Invite a SECOND member — exceeding the free tier's single seat. ──
    page.fill("form[action='/account/team/invite'] input[name='email']", invitee_email)
    with page.expect_response(
        lambda r: r.request.method == "POST" and "/account/team/invite" in r.url
    ) as resp_info:
        page.click("form[action='/account/team/invite'] button[type='submit']")
    resp = resp_info.value
    status = resp.status
    try:
        body = (resp.text() or "").lower()
    except Exception:
        # Playwright cannot read the body of a redirect (3xx success) response.
        body = ""
    page.wait_for_load_state("networkidle")

    # (a) Exceeding the seat limit must never surface as an opaque server error.
    assert status < 500, (
        f"inviting beyond the free seat limit returned a generic {status} server error "
        "(the audited bug) instead of a clear 4xx upgrade prompt"
    )

    # (b) The second seat must actually be refused — a free-tier workspace must not
    #     gain a second member.
    page.goto("/account/team")
    team_html = page.content().lower()
    assert invitee_email.lower() not in team_html, (
        "a free-tier workspace (1 seat) accepted a second member — the plan seat "
        "limit is not enforced"
    )

    # (c) The refusal must read as a clear seat-limit / upgrade message.
    upsell_markers = (
        "seat",
        "upgrade",
        "user limit",
        "maximum",
        "limit reached",
        "too many users",
        "upgrade your plan",
    )
    assert 400 <= status < 500 and any(k in body for k in upsell_markers), (
        f"exceeding the seat limit should show a clear seat-limit/upgrade message "
        f"(4xx); got status={status} with no upsell text in the response body"
    )


def test_usage_metering_matches_actual_consumption(api, page):
    """The metered tokens/spend shown on the dashboard equals actual usage_events.

    Ingest a known workload (drives LLM calls), then assert the dashboard's token/
    spend figure matches the recorded usage (metered must equal actual, since
    enforcement reads the same number).

    Note: in the e2e stack every model runs locally, so ``cost_micros`` is NULL and
    the dollar spend stays $0.00 — token usage is the meaningful metered signal.
    """
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()
    api.login(tenant, email, password)
    flows.browser_login(page, tenant, email, password)

    # Baseline: the displayed monthly token figure must already match the exact
    # recorded usage_events total embedded in the dashboard's daily-usage chart.
    page.goto("/dashboard")
    page.wait_for_selector("text=Tokens (this month)", timeout=30_000)
    html_before = page.content()
    recorded_before = _dashboard_recorded_tokens(html_before)
    _assert_display_matches_recorded(_dashboard_token_used(html_before), recorded_before)

    # Drive a known workload: ingesting a real document runs the inline pipeline
    # (tagging via the text model + embedding), consuming LLM tokens that must be
    # metered into usage_events.
    _marker, doc = flows.ingest_and_wait(api, METERING_DOC)
    assert doc.get("tags"), "ingest produced no tags — the tagging LLM call did not run"

    # After the ingest the recorded usage must have grown, and the displayed figure
    # must still match the (now larger) recorded total.
    page.goto("/dashboard")
    page.wait_for_selector("text=Tokens (this month)", timeout=30_000)
    html_after = page.content()
    recorded_after = _dashboard_recorded_tokens(html_after)

    assert recorded_after > recorded_before, (
        f"recorded token usage did not increase after ingesting a document "
        f"(before={recorded_before}, after={recorded_after}) — real LLM consumption "
        f"is not being metered into usage_events"
    )
    _assert_display_matches_recorded(_dashboard_token_used(html_after), recorded_after)


def test_plan_rate_cap_enforced(api):
    """A tenant exceeding its plan's per-minute request cap is throttled (429).

    Audit: ``rate_caps`` is seeded per plan but no middleware reads it. Burst past the
    cap and assert 429 + Retry-After — expected to FAIL, surfacing the unenforced cap.
    """
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()
    api.login(tenant, email, password)

    statuses: list[int] = []
    retry_after_present = False
    start = time.monotonic()
    for i in range(RATE_BURST):
        # Authenticated, model-free endpoint (a missing job → 404). A per-tenant rate
        # limiter would reject with 429 in middleware before the handler runs, so the
        # handler's own status is irrelevant — we only count throttling responses.
        # (We use the client's underlying httpx session directly to read raw status
        # codes without the helper's non-200 -> exception wrapping.)
        r = api._c.get(f"/api/jobs/{900_000_000 + i}")
        statuses.append(r.status_code)
        if r.status_code == 429 and "retry-after" in {k.lower() for k in r.headers.keys()}:
            retry_after_present = True
    elapsed = time.monotonic() - start

    assert elapsed < 60.0, (
        f"the burst of {RATE_BURST} requests took {elapsed:.1f}s (>= 1 minute), so a "
        "per-minute cap cannot be asserted; rerun against a faster stack"
    )

    n_429 = sum(1 for s in statuses if s == 429)
    assert n_429 > 0, (
        f"sent {RATE_BURST} authenticated requests in {elapsed:.1f}s — far beyond the "
        f"free plan's documented 5/min cap — but none were throttled "
        f"(statuses seen: {sorted(set(statuses))}); the plan per-minute rate cap is not enforced"
    )
    assert retry_after_present, (
        "throttled (429) responses must include a Retry-After header so clients know "
        "when they may retry"
    )


@pytest.mark.skip(reason="blocked: plan token-budget check has no caller + needs over-budget state")
def test_plan_token_budget_enforced_at_llm_boundary(api):
    """LLM work is blocked once the plan token budget is exhausted (budget upsell).

    BLOCKED: ``check_plan_token_budget`` exists with full math but has zero non-test
    callers; ingest only checks storage. Not drivable end-to-end today.
    """
    ...


@pytest.mark.skip(reason="blocked: signed webhook required (Stripe unconfigured in e2e)")
def test_downgrade_over_limit_handling(api):
    """A downgrade below current usage retains data read-only + blocks new ingest at the lower cap.

    BLOCKED: needs a signed ``customer.subscription.updated`` to a lower tier.
    """
    ...


@pytest.mark.skip(reason="blocked: signed webhook required (Stripe unconfigured in e2e)")
def test_subscription_canceled_reverts_to_free(api):
    """``customer.subscription.deleted`` reverts to free, applies free quotas, gates remote models."""
    ...


@pytest.mark.skip(reason="blocked: signed webhook + dunning sweep trigger required")
def test_dunning_past_due_suspend_reactivate(api):
    """Full lifecycle: payment_failed→past_due→(sweep)→suspended (ingest 403)→invoice.paid→active.

    BLOCKED: suspension is a background sweep with no HTTP trigger and ``invoice.paid``
    does not clear past_due/suspended back to active (possible stuck state); also
    needs the webhook secret. Note: code lets past_due THROUGH ingest (only suspended
    is read-only) — an intent/impl mismatch vs the existing past-due test.
    """
    ...


@pytest.mark.skip(reason="blocked: out-of-order webhook convergence needs signed events")
def test_out_of_order_webhook_convergence(api):
    """Stale/out-of-order lifecycle events do not regress plan/current_period_end.

    BLOCKED: ``update_tenant_plan`` uses blind COALESCE with no monotonic period guard;
    needs signed multi-event sequences to drive.
    """
    ...


@pytest.mark.skip(reason="blocked: invoice/PO/ACH billing path not implemented")
def test_invoice_po_ach_billing_path(page):
    """Enterprises can subscribe via invoice/PO/ACH (net_30), not only card.

    BLOCKED: only card Checkout exists — a hard blocker for the enterprise audience.
    """
    ...


@pytest.mark.skip(reason="blocked: annual plans + proration not implemented")
def test_annual_plan_and_proration(page):
    """An annual interval is offered and mid-cycle upgrade prorates correctly.

    BLOCKED: seed plans are monthly-only; no interval/annual price or proration.
    """
    ...


@pytest.mark.skip(reason="blocked: tax/VAT collection not implemented")
def test_tax_vat_collected_at_checkout(api):
    """A VAT-jurisdiction customer sees tax handling / tax-ID capture at checkout.

    BLOCKED: no tax/VAT fields or Stripe Tax config (legal requirement for EU/global).
    """
    ...


@pytest.mark.skip(reason="blocked: trials/coupons/refunds not handled (webhook catch-all)")
def test_trial_lifecycle_and_coupons(api):
    """Trial start/expiry, trial_will_end, refunds, and coupons are handled.

    BLOCKED: the webhook match handles only checkout/updated/deleted/invoice.paid/
    payment_failed; everything else is acknowledged-no-action.
    """
    ...
