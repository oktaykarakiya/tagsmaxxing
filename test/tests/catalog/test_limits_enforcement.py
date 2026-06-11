"""Catalog — P14 limits-enforcement journey (ledger P14-T15, E2).

End-to-end proof that the P14 plan-limit enforcement actually *bites* on a real,
running stack. Drives a dedicated low-cap ``limits-e2e`` tenant (provisioned by
:mod:`lib.limits`, clamped to ~1 MB storage / ~1000 tokens via the super-admin
quota-override endpoint) over each limit in turn and asserts the user-visible
outcome — not a unit-tested code path, but the shipped HTTP/HTML behaviour:

* (a) **Token budget** — once the tenant's monthly token rollup meets/exceeds the
  cap, the *next* ``POST /api/ingest`` is rejected with ``429`` +
  ``token_budget_exceeded`` and a human-readable budget message (the upsell the
  user is shown). The job that crossed the budget runs to completion; only the
  subsequent ingest is blocked (the documented UX-first bounded-overshoot rule).
* (b) **Over-budget search degrades, never errors** — the same exhausted budget
  must NOT fail search. ``GET /api/search`` answers ``200`` with
  ``X-Search-Mode: keyword`` (and ``mode": "keyword"`` in the body), and the
  rendered ``/search`` page shows the "Basic results — AI search budget reached"
  notice (D2).
* (c) **Storage quota** — an upload that would push total storage past the ~1 MB
  cap is rejected with ``413`` + ``storage_quota_exceeded``.
* (d) **Dashboard** — once over budget the dashboard renders the per-user usage
  breakdown ("Usage by User (this month)") and an at-cap warning banner.

Per the suite philosophy a failing assertion means an app bug and is left
failing — we never weaken the test to make a real gap pass.

Notes on this tenant's shape (load-bearing for the assertions):
* It is **grandfathered (no plan)** with an *admin* quota override, so
  ``resolve_effective_quotas`` returns ``plan_code = None`` / ``upsell = None``.
  Therefore the ``429``/``413`` bodies carry the bare ``QuotaError`` detail
  (e.g. "token quota exceeded: … limit is 1000 tokens") with **no** literal
  "Upgrade to <tier>" phrase — that phrase only appears for plan-subscribed
  tenants. The dashboard at-cap banner *does* say "upgrade" (it is plan-agnostic),
  so we assert the upsell wording there, and assert the limit/budget message on
  the ingest rejection.
* The per-minute plan rate cap (a fifth P14 limit) is intentionally NOT exercised
  here — see the ``skip`` at the bottom for why it cannot be driven cleanly from
  this lane.

Requires the running e2e stack (LLM-backed ingest + the app's Postgres container,
reached the same ``podman exec … psql`` way the rest of the harness reads
server-side state). The ``limits_account`` fixture skips cleanly when that DB /
podman is unavailable, so this file never errors a run.
"""

from __future__ import annotations

import re

import httpx
import pytest

from lib import config, flows
from lib.api_client import ApiError

pytestmark = pytest.mark.quotas


# A substantial, taggable document so a single inline ingest (tag + embed) burns a
# clearly-measurable number of LLM tokens — enough that a few of them cross the
# tenant's tiny ~1000-token budget. ``{marker}`` is filled per-ingest so each is a
# distinct document (and distinctly searchable).
TOKEN_DOC = (
    "Quarterly operations and finance review for the limits-enforcement e2e lane. "
    "This memo covers revenue recognition, invoice reconciliation, vendor purchase "
    "orders, headcount planning, and the cloud infrastructure budget for the "
    "upcoming fiscal period. It discusses risk management, compliance obligations, "
    "data-retention policy, and the roadmap for the analytics platform. The "
    "engineering section reviews ingestion throughput, embedding model selection, "
    "retrieval quality, and observability dashboards. Marketing notes summarize "
    "campaign performance and customer acquisition cost across regions and quarters. "
    "Unique limits-enforcement marker: {marker}."
) * 4

# How many over-budget ingests to attempt before giving up waiting for the 429.
# The token gate reads the monthly rollup, which only reflects *already-completed*
# calls, so a couple of warm-up ingests may slip through before the rollup catches
# up. Each ingest runs the full inline pipeline, so keep this small.
_MAX_INGEST_TRIES = 8

# Bytes comfortably above the ~1 MB storage cap (and far below the 100 MiB ingest
# payload limit), so the storage check — which runs *before* the token check in the
# ingest handler — is the one that trips.
_OVER_QUOTA_BYTES = 1_600_000


def _raw_ingest(account, filename: str, content: str) -> httpx.Response:
    """POST one text file to ``/api/ingest`` and return the *raw* response.

    :meth:`ApiClient.ingest_text` raises on any non-202, which hides the 413/429
    status + body we need to assert. We post through the client's underlying
    (already-authenticated, cookie-bearing) httpx session so we can inspect the
    enforcement response directly — the same escape hatch ``test_quotas`` /
    ``test_billing_enterprise`` use for raw status codes.
    """
    files = [("files", (filename, content.encode("utf-8"), "text/plain"))]
    return account.client._c.post("/api/ingest", files=files)


def _drive_over_token_budget(account) -> None:
    """Ingest the workload doc until the tenant's monthly token rollup is exhausted.

    Returns once an ingest succeeds (2xx) AND the budget is known to be met (a
    later ingest will 429). We ingest one rich document at a time: each runs the
    tagging + embedding models, accruing tokens into ``usage_events`` and the
    ``tenant_monthly_usage`` rollup that the ingest gate point-selects. Ingestion
    is queued (P15) — the 202 only stages the document, so we wait for the
    worker to finish processing (that is when the tokens are metered) before
    returning; the caller then asserts the *next* ingest is the one blocked.
    """
    for i in range(_MAX_INGEST_TRIES):
        marker = flows.unique_marker("limtok")
        text = TOKEN_DOC.format(marker=marker)
        resp = _raw_ingest(account, f"{marker}.txt", text)
        if resp.status_code == 202:
            account.remember(marker)
            body = resp.json()
            flows.wait_until_ready(
                account.client, body["document_id"], job_id=body.get("job_id")
            )
            return
        if resp.status_code == 429:
            # Already over budget (a prior iteration's tokens tipped it) — good
            # enough; the case assertion re-confirms the 429 shape below.
            return
        raise AssertionError(
            f"warm-up ingest #{i} returned an unexpected {resp.status_code}: "
            f"{resp.text[:300]}"
        )
    raise AssertionError(
        f"after {_MAX_INGEST_TRIES} ingests of a ~1000+ token document the tenant "
        "still accepted uploads — the monthly token budget is not being accrued/"
        "enforced on the ingest path"
    )


def test_token_budget_blocks_next_ingest(limits_account):
    """(a) Over the token budget, the next ingest is 429 ``token_budget_exceeded`` + upsell.

    Burn through the ~1000-token monthly budget, then assert a subsequent ingest is
    hard-blocked with ``429 Too Many Requests``, the ``token_budget_exceeded`` error
    code, and a human-readable budget/limit message (the guidance shown to the user).
    The crossing job completes; only the *next* ingest is refused — the documented
    UX-first bounded-overshoot semantic.
    """
    account = limits_account
    account.reset_usage()  # start from a clean (zero-usage) rollup

    _drive_over_token_budget(account)

    # The very next ingest must now be refused for budget — retry a bounded number
    # of times because the rollup reflects only completed calls, so one more upload
    # may still squeak through before the counter catches up. Each accepted probe
    # is queued (P15): wait for it to process (and meter its tokens) before the
    # next attempt, or every probe would be admitted against the stale rollup.
    last: httpx.Response | None = None
    for _ in range(_MAX_INGEST_TRIES):
        marker = flows.unique_marker("limtok-blocked")
        last = _raw_ingest(account, f"{marker}.txt", TOKEN_DOC.format(marker=marker))
        if last.status_code == 429:
            break
        if last.status_code == 202:
            body = last.json()
            flows.wait_until_ready(
                account.client, body["document_id"], job_id=body.get("job_id")
            )

    assert last is not None and last.status_code == 429, (
        f"ingesting past the ~1000-token monthly budget should be throttled with "
        f"429, but the tenant kept accepting uploads (last status "
        f"{getattr(last, 'status_code', None)}) — the token budget is not enforced "
        f"on the ingest path"
    )

    body = last.json()
    assert body.get("error") == "token_budget_exceeded", (
        f"a budget rejection must carry the machine code 'token_budget_exceeded'; "
        f"got {body!r}"
    )

    # The message is the user-facing upsell/guidance. For this grandfathered,
    # admin-overridden tenant there is no plan tier to name, so the server returns
    # the bare quota detail (no literal "Upgrade to <tier>"); assert it names the
    # exhausted budget/limit so the user understands *why* they were blocked.
    message = (body.get("message") or "").lower()
    assert message, "the 429 budget rejection must include a human-readable message"
    assert ("budget" in message) or ("token quota" in message) or ("limit" in message), (
        f"the 429 message should explain that the token budget/limit is reached so "
        f"the user knows to upgrade or wait; got: {body.get('message')!r}"
    )

    # The Retry-After header is added to every 429 by the serve middleware so clients
    # know when they may retry.
    assert "retry-after" in {k.lower() for k in last.headers.keys()}, (
        "a 429 throttle response must include a Retry-After header"
    )


def test_over_budget_search_degrades_to_keyword(limits_account):
    """(b) Over budget, search degrades to keyword mode — it must not error.

    The same exhausted token budget that hard-blocks *ingest* must only *degrade*
    search: ``GET /api/search`` answers ``200`` with ``X-Search-Mode: keyword``
    (and ``mode": "keyword"`` in the JSON body), never a 4xx/5xx. We also confirm
    the rendered ``/search`` page surfaces the D2 "Basic results — AI search budget
    reached" notice so the degradation is visible to the user.
    """
    account = limits_account
    account.reset_usage()

    _drive_over_token_budget(account)

    # ── JSON API: 200 + keyword mode (header AND body), not an error ──────────
    # Use the raw session so a non-200 surfaces as a status assertion rather than
    # the helper's ApiError. A query string is required; the term need not match
    # anything — the degrade decision is independent of hits.
    resp = account.client._c.get(
        "/api/search", params={"q": "operations finance review", "limit": 5}
    )
    assert resp.status_code == 200, (
        f"over-budget search must degrade, not error: expected 200, got "
        f"{resp.status_code}: {resp.text[:300]}"
    )
    mode_header = resp.headers.get("X-Search-Mode") or resp.headers.get("x-search-mode")
    assert mode_header == "keyword", (
        f"an over-budget tenant's search must report keyword mode via the "
        f"X-Search-Mode header; got {mode_header!r}"
    )
    assert resp.json().get("mode") == "keyword", (
        f"the search response body must also carry mode='keyword' when degraded; "
        f"got {resp.json().get('mode')!r}"
    )

    # ── Rendered web page: the D2 'Basic results' degrade notice ─────────────
    # The /search POST is an HTMX fragment guarded by the double-submit CSRF cookie;
    # drive it by hand (GET to seed the cookie + hidden token, then POST) so we can
    # assert the server-rendered notice without a browser.
    seed = account.client._c.get("/search")
    assert seed.status_code == 200, (
        f"GET /search to seed the CSRF cookie failed: {seed.status_code}"
    )
    m = re.search(r'name="csrf_token" value="([^"]+)"', seed.text)
    assert m, "no csrf_token hidden field found on /search"
    frag = account.client._c.post(
        "/search",
        data={"csrf_token": m.group(1), "q": "operations finance review"},
    )
    assert frag.status_code == 200, (
        f"the /search HTMX fragment POST should return 200, got {frag.status_code}: "
        f"{frag.text[:300]}"
    )
    assert frag.headers.get("X-Search-Mode") == "keyword", (
        "the rendered /search fragment must also stamp X-Search-Mode: keyword when "
        "the tenant is over budget"
    )
    assert "Basic results" in frag.text and "AI search budget reached" in frag.text, (
        "the rendered /search page must show the D2 'Basic results — AI search "
        f"budget reached' notice when degraded; fragment was: {frag.text[:400]!r}"
    )


def test_storage_quota_blocks_oversized_upload(limits_account):
    """(c) An upload past the ~1 MB storage cap is rejected with 413 ``storage_quota_exceeded``.

    The ingest handler checks storage quota *before* the token budget, so a single
    file larger than the tenant's clamped ~1 MB cap trips the storage gate. Assert
    ``413 Payload Too Large`` + the ``storage_quota_exceeded`` code + a message that
    explains the storage cap.
    """
    account = limits_account
    account.reset_usage()  # clear stored bytes so only this upload counts

    # A single text file well over the ~1 MB cap (and far under the 100 MiB payload
    # limit, so it is the *quota* check — not the payload-size guard — that fires).
    marker = flows.unique_marker("limstore")
    blob = ("storage quota limit probe " * 64).ljust(_OVER_QUOTA_BYTES, "x")
    resp = _raw_ingest(account, f"{marker}.txt", blob)

    assert resp.status_code == 413, (
        f"uploading ~{_OVER_QUOTA_BYTES // 1000} KB past the tenant's ~1 MB storage "
        f"cap should return 413 Payload Too Large; got {resp.status_code}: "
        f"{resp.text[:300]}"
    )
    body = resp.json()
    assert body.get("error") == "storage_quota_exceeded", (
        f"an over-quota upload must carry the 'storage_quota_exceeded' code; "
        f"got {body!r}"
    )
    message = (body.get("message") or "").lower()
    assert ("storage" in message) and ("quota" in message or "limit" in message), (
        f"the 413 message should explain the storage quota was exceeded; "
        f"got: {body.get('message')!r}"
    )


def test_dashboard_shows_per_user_usage_and_atcap_banner(limits_account, page):
    """(d) Once over budget, the dashboard shows per-user usage + an at-cap banner.

    Burn the budget, then load the dashboard in a real browser and assert the D2
    surfaces: the "Usage by User (this month)" per-user breakdown (with a real
    attributed token figure, not just the empty-state), and an at/over-cap warning
    banner pointing the user at an upgrade.
    """
    account = limits_account
    account.reset_usage()

    _drive_over_token_budget(account)

    # Log the browser in as this tenant's Owner (CSRF handled by the form).
    flows.browser_login(page, account.slug, account.email, account.password)
    page.goto("/dashboard")
    page.wait_for_load_state("networkidle")

    # Sanity: this is the dashboard.
    assert page.locator("h1").first.text_content() == "Dashboard", (
        "expected the Dashboard page"
    )

    body_text = " ".join(page.locator("body").inner_text().split())

    # ── Per-user usage breakdown is present (the P14-T13 D2 panel) ───────────
    page.locator("text=Usage by User (this month)").first.wait_for(
        state="visible", timeout=15_000
    )
    # After a real ingest the panel must show attributed token usage — i.e. NOT the
    # "No attributed AI usage this month yet." empty-state — and a "<n> tokens" row.
    assert "No attributed AI usage this month yet." not in body_text, (
        "after ingesting real documents the dashboard's per-user breakdown still "
        "shows the empty-state — usage is not attributed per user"
    )
    assert re.search(r"\btokens\b", body_text, re.IGNORECASE), (
        "the per-user usage breakdown should list a '<n> tokens' figure per user"
    )

    # ── Approaching/at-cap banner is shown now that the tenant is over budget ─
    # The tenant has a finite (admin-override) token cap, so once the monthly rollup
    # meets/exceeds it the dashboard renders the at-cap banner (plan-agnostic copy
    # that does say "upgrade"). Assert the at-cap wording and the upgrade affordance.
    assert "reached your monthly AI budget limit" in body_text, (
        "an over-budget tenant's dashboard must show the at-cap 'reached your "
        f"monthly AI budget limit' banner; page text was: {body_text[:600]!r}"
    )
    upgrade_link = page.locator("a[href='/account/plan']")
    assert upgrade_link.count() >= 1, (
        "the at-cap banner must offer an upgrade affordance linking to /account/plan"
    )


@pytest.mark.skip(
    reason="not drivable from this lane: the plan per-minute rate cap (the fifth "
    "P14 limit) is enforced by the per-tenant request-rate middleware, but this "
    "limits-e2e tenant is grandfathered (plan_id=NULL) so it has no plan-defined "
    "rate_cap to enforce, and the e2e stack runs with a deliberately raised login/"
    "request cap (see the local run setup) — a burst large enough to trip a real "
    "per-minute cap would instead collide with that raised limit and the per-tenant "
    "in-flight ingest limiter, making the signal ambiguous. test_billing_enterprise"
    "::test_plan_rate_cap_enforced already asserts the plan rate-cap behaviour on a "
    "plan-bearing path; duplicating it here would only re-test the raised-cap "
    "collision, not the limit."
)
def test_plan_rate_cap_enforced_for_limits_tenant(limits_account):
    """(e) The plan per-minute request cap throttles a burst with 429 + Retry-After.

    Left skipped — see the reason above: this grandfathered, admin-override tenant
    has no plan-defined per-minute cap, and the e2e stack's raised login/request cap
    would make a burst-based assertion ambiguous. The plan rate-cap itself is
    covered by ``test_billing_enterprise::test_plan_rate_cap_enforced``.
    """
    ...
