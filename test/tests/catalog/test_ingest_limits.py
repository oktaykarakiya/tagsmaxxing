"""Catalog — upload/ingest *limits & access-control* boundary tests.

Black-box proof that the ingest entry points enforce their documented gates on a
real, running stack. Drives a dedicated low-cap ``limits-e2e`` tenant (provisioned
by the ``limits_account`` fixture in :mod:`lib.limits`, clamped to ~1 MB storage /
~1000 tokens via the super-admin quota-override endpoint) and asserts the
user-visible HTTP outcome of each limit / auth boundary:

* **Storage quota** — an upload that would push total stored bytes past the cap
  is rejected ``413 Payload Too Large`` (``check_bytes_quota``: ``total > limit``).
* **At-cap boundary** — a file that fits *under* the cap is accepted (``202``);
  after a reset, a file that pushes *over* it is rejected (``413``).
* **Token budget** — once the tenant's monthly token rollup is exhausted, the next
  ingest is throttled ``429 Too Many Requests`` (the gate point-selects the
  ``tenant_monthly_usage`` rollup; ``reset_usage`` clears it).
* **Token-budget recovery** — after a reset clears the rollup, a fresh upload is
  accepted again (``202``).
* **No session** — an unauthenticated POST to ``/api/ingest`` is ``401``.
* **CSRF on /upload** — the browser upload form is double-submit-cookie protected:
  a bogus ``csrf_token`` is ``403``; the proper handshake yields ``202``/``200``.
* **Suspended tenant** — a tenant whose ``billing_status`` is ``'suspended'`` is
  read-only: ingest is ``403 account_suspended`` (the suspend check runs first in
  the handler). The DB column is set then restored in a ``finally``.

Per the suite philosophy each assertion encodes the *intended* contract: a
failure is a recorded app bug, never weakened to pass, and a ``500`` is never
accepted where a ``413``/``429``/``403``/``401`` is intended. Cases that mutate
usage call ``limits_account.reset_usage()`` so each starts from a clean slate.

Requires the running e2e stack + the app's Postgres container (reached the same
``podman exec … psql`` way the rest of the harness reads server-side state). The
``limits_account`` fixture skips cleanly when that DB/podman is unavailable, so
this file never errors a run.
"""

from __future__ import annotations

import pytest

from lib import config, flows, limits
from lib import file_factory as ff

pytestmark = [pytest.mark.ingest, pytest.mark.quotas]


@pytest.fixture(autouse=True)
def _require_text_backend():
    """Skip when the text/embed backend is degraded (environment outage, not a bug).

    A non-rejected ingest needs the tagger/embed backends; when they are down the
    upload returns ``500 internal_error`` ("tagger failed"), an environment outage
    rather than a limits/auth defect. Skipping keeps the lane from false-failing on
    a backend hiccup; the 413/401/403 reject paths still assert when the backend is
    up (the *intended* graceful-503 behaviour is a separate recorded finding).
    """
    import httpx

    with httpx.Client(base_url=config.base_url(), verify=config.verify_tls(), timeout=10.0) as c:
        if not (ff.subsystem_ok(c, "text") and ff.subsystem_ok(c, "embed")):
            pytest.skip("text/embed backend degraded per /health — environment, not an app bug")


# How many over-budget ingests to attempt before declaring the token gate broken.
# Each ingest runs the full inline pipeline (tag + embed), accruing tokens into the
# monthly rollup the gate reads, so keep this modest. We additionally lower the
# token cap (see below) so the budget trips well within this bound.
_MAX_INGEST_TRIES = 15

# A small, taggable document: each ingest burns a measurable number of embed+tag
# tokens. ``{marker}`` is filled per-ingest so every upload is a distinct document.
_TOKEN_DOC = (
    "Operations and finance review note for the ingest-limits e2e lane. It covers "
    "revenue, invoices, vendor purchase orders, headcount, and the infrastructure "
    "budget, plus risk, compliance, and the analytics roadmap. Unique marker: {marker}."
)

# A token cap low enough that a couple of small ingests trip the monthly budget
# quickly, keeping the loop short. (The fixture default is 1000; we clamp tighter.)
_LOW_TOKEN_CAP = 50


# ── Storage quota ─────────────────────────────────────────────────────────────
def test_oversized_upload_exceeds_storage_quota(limits_account):
    """A file larger than the storage cap is rejected with 413.

    The ingest handler checks storage quota before the token budget, so a single
    file comfortably larger than the tenant's clamped ~1 MB cap trips the storage
    gate (``check_bytes_quota``: ``current + additional > limit``). Assert the
    user-visible ``413 Payload Too Large``.
    """
    account = limits_account
    account.reset_usage()  # zero stored bytes so only this upload counts
    try:
        marker = flows.unique_marker("limstore")
        blob = ff.sized_text(account.quota_bytes + 200_000, marker)
        res = ff.upload(
            account.client._c,
            (f"{marker}.txt", blob, "text/plain"),
            request_id=ff.new_request_id("ingest-limits-store"),
        )
        assert res["status"] == 413, (
            f"uploading ~{(account.quota_bytes + 200_000) // 1000} KB past the "
            f"~{account.quota_bytes // 1000} KB storage cap must return 413; got "
            f"{res['status']}: {res['text']!r}"
        )
    finally:
        account.reset_usage()


def test_storage_quota_boundary_under_then_over(limits_account):
    """Just under the storage cap is accepted (202); pushing over it is rejected (413).

    Exercises both sides of the ``total > limit`` boundary on a fresh tenant: a
    file sized comfortably *below* the cap fits (``202``); after a usage reset, a
    file sized *above* the cap does not (``413``). A modest margin is left on the
    under-cap payload so the case is robust to any small per-upload accounting.
    """
    account = limits_account

    # ── Under the cap → accepted ──────────────────────────────────────────────
    account.reset_usage()
    under_marker = flows.unique_marker("limunder")
    # Leave ~100 KB of headroom below the cap so the upload clearly fits.
    under_blob = ff.sized_text(account.quota_bytes - 100_000, under_marker)
    under = ff.upload(
        account.client._c,
        (f"{under_marker}.txt", under_blob, "text/plain"),
        request_id=ff.new_request_id("ingest-limits-under"),
    )
    assert under["status"] == 202, (
        f"a ~{(account.quota_bytes - 100_000) // 1000} KB file under the "
        f"~{account.quota_bytes // 1000} KB storage cap must be accepted with 202; "
        f"got {under['status']}: {under['text']!r}"
    )

    # ── Over the cap (after reset) → rejected ─────────────────────────────────
    account.reset_usage()
    try:
        over_marker = flows.unique_marker("limover")
        over_blob = ff.sized_text(account.quota_bytes + 200_000, over_marker)
        over = ff.upload(
            account.client._c,
            (f"{over_marker}.txt", over_blob, "text/plain"),
            request_id=ff.new_request_id("ingest-limits-over"),
        )
        assert over["status"] == 413, (
            f"a ~{(account.quota_bytes + 200_000) // 1000} KB file over the "
            f"~{account.quota_bytes // 1000} KB storage cap must return 413; got "
            f"{over['status']}: {over['text']!r}"
        )
    finally:
        account.reset_usage()


# ── Token budget ──────────────────────────────────────────────────────────────
def test_token_budget_blocks_subsequent_ingest(limits_account):
    """Once the monthly token budget is exhausted, a later ingest returns 429.

    Lower the token cap so a handful of small ingests cross it, ingest in a loop
    (each accrues embed+tag tokens into the monthly rollup the gate reads), and
    assert that a subsequent upload is throttled with ``429``. If after
    ``_MAX_INGEST_TRIES`` successful ingests we never see a ``429``, FAIL with the
    count and last status — that records "token budget not enforced on ingest".
    """
    account = limits_account
    # Trip the budget faster with a tighter cap, then start from a clean rollup.
    account.apply_caps(quota_tokens=_LOW_TOKEN_CAP)
    account.reset_usage()
    try:
        last_status = None
        saw_429 = False
        for i in range(_MAX_INGEST_TRIES):
            marker = flows.unique_marker("limtok")
            res = ff.upload(
                account.client._c,
                (f"{marker}.txt", _TOKEN_DOC.format(marker=marker).encode("utf-8"), "text/plain"),
                request_id=ff.new_request_id("ingest-limits-token"),
            )
            last_status = res["status"]
            if last_status == 429:
                saw_429 = True
                break
            assert last_status == 202, (
                f"warm-up ingest #{i} returned an unexpected {last_status} "
                f"(expected 202 until the budget trips): {res['text']!r}"
            )
            # Queued ingestion (P15): tokens are metered when the WORKER
            # processes the job, so wait for completion — the next upload's
            # admission gate then reads the updated monthly rollup.
            flows.wait_until_ready(
                account.client, (res["body"] or {})["document_id"],
                job_id=(res["body"] or {}).get("job_id"),
            )

        assert saw_429, (
            f"after {_MAX_INGEST_TRIES} successful ingests of a token-bearing "
            f"document against a {_LOW_TOKEN_CAP}-token monthly budget, no ingest "
            f"was ever throttled (last status {last_status}) — the token budget is "
            f"not enforced on the ingest path"
        )
    finally:
        # Restore the account's default token cap and clear usage for the next case.
        account.apply_caps(quota_tokens=limits.DEFAULT_QUOTA_TOKENS)
        account.reset_usage()


def test_token_budget_recovers_after_reset(limits_account):
    """After tripping 429 then clearing usage, a fresh upload is accepted (202).

    Drive the (tightened) token budget over its cap to provoke a ``429``, then call
    ``reset_usage()`` (which clears both ``usage_events`` and the
    ``tenant_monthly_usage`` rollup the gate reads). A subsequent small upload must
    succeed with ``202`` — proving the gate cleared, not a one-way trip.
    """
    account = limits_account
    account.apply_caps(quota_tokens=_LOW_TOKEN_CAP)
    account.reset_usage()
    try:
        # Drive over budget until a 429 is observed (bounded).
        tripped = False
        for _ in range(_MAX_INGEST_TRIES):
            marker = flows.unique_marker("limtok-trip")
            res = ff.upload(
                account.client._c,
                (f"{marker}.txt", _TOKEN_DOC.format(marker=marker).encode("utf-8"), "text/plain"),
                request_id=ff.new_request_id("ingest-limits-trip"),
            )
            if res["status"] == 429:
                tripped = True
                break
            assert res["status"] == 202, (
                f"warm-up ingest returned an unexpected {res['status']}: {res['text']!r}"
            )
            # Queued ingestion (P15): wait for the worker to meter this job's
            # tokens before the next admission check (see the blocks test).
            flows.wait_until_ready(
                account.client, (res["body"] or {})["document_id"],
                job_id=(res["body"] or {}).get("job_id"),
            )
        if not tripped:
            pytest.fail(
                f"could not trip the {_LOW_TOKEN_CAP}-token budget within "
                f"{_MAX_INGEST_TRIES} ingests — cannot verify recovery (token budget "
                "not enforced on ingest)"
            )

        # ── Recovery: clear the rollup, then a fresh upload must be accepted ───
        account.reset_usage()
        marker = flows.unique_marker("limtok-recover")
        res = ff.upload(
            account.client._c,
            (f"{marker}.txt", _TOKEN_DOC.format(marker=marker).encode("utf-8"), "text/plain"),
            request_id=ff.new_request_id("ingest-limits-recover"),
        )
        assert res["status"] == 202, (
            f"after reset_usage() cleared the monthly rollup, a fresh ingest must be "
            f"accepted with 202 (the budget gate should have cleared); got "
            f"{res['status']}: {res['text']!r}"
        )
    finally:
        account.apply_caps(quota_tokens=limits.DEFAULT_QUOTA_TOKENS)
        account.reset_usage()


# ── Authentication / CSRF ─────────────────────────────────────────────────────
def test_ingest_without_session_is_unauthorized(api):
    """An unauthenticated POST to /api/ingest is rejected with 401.

    The ``api`` fixture is a fresh, logged-OUT client. ``/api/ingest`` requires a
    session cookie, so the upload must be refused with ``401 Unauthorized`` (not a
    redirect to login, not a 500).
    """
    marker = flows.unique_marker("limnoauth")
    res = ff.upload(
        api._c,
        (f"{marker}.txt", f"unauthenticated upload {marker}".encode("utf-8"), "text/plain"),
        request_id=ff.new_request_id("ingest-limits-noauth"),
    )
    assert res["status"] == 401, (
        f"an upload with no session must be rejected with 401 Unauthorized; got "
        f"{res['status']}: {res['text']!r}"
    )


def test_upload_form_rejects_bad_csrf(limits_account):
    """POST /upload with a bogus csrf_token is rejected with 403.

    The browser upload form is protected by the double-submit-cookie scheme. We
    post the multipart by hand (NOT via ``ff.upload``, which would auto-fix the
    token) with a deliberately wrong ``csrf_token`` and no matching cookie, so the
    CSRF guard must reject it with ``403 Forbidden``.
    """
    account = limits_account
    marker = flows.unique_marker("limbadcsrf")
    files = [("files", (f"{marker}.txt", f"bad csrf upload {marker}".encode("utf-8"), "text/plain"))]
    r = account.client._c.post(
        "/upload",
        files=files,
        data={"csrf_token": "bogus-not-a-real-token"},
        headers={"X-Request-Id": ff.new_request_id("ingest-limits-badcsrf")},
    )
    assert r.status_code == 403, (
        f"a /upload POST with a bogus csrf_token must be rejected with 403 Forbidden; "
        f"got {r.status_code}: {r.text[:300]!r}"
    )


def test_upload_form_accepts_valid_csrf(limits_account):
    """POST /upload with the proper CSRF handshake is accepted (202/200) — positive control.

    Mirror of the bad-CSRF case proving the guard is not simply rejecting *all*
    ``/upload`` posts: ``ff.upload(endpoint="/upload")`` performs the real
    double-submit handshake (GET to seed the cookie + read the hidden token, then
    POST), which must succeed.
    """
    account = limits_account
    account.reset_usage()  # ensure storage/token caps don't mask the CSRF success
    try:
        marker = flows.unique_marker("limokcsrf")
        res = ff.upload(
            account.client._c,
            (f"{marker}.txt", f"valid csrf upload {marker}".encode("utf-8"), "text/plain"),
            endpoint="/upload",
            request_id=ff.new_request_id("ingest-limits-okcsrf"),
        )
        assert res["status"] in (200, 202), (
            f"a /upload POST with a valid CSRF handshake must be accepted (200/202); "
            f"got {res['status']}: {res['text']!r}"
        )
    finally:
        account.reset_usage()


# ── Suspended tenant ──────────────────────────────────────────────────────────
def test_suspended_tenant_ingest_is_forbidden(limits_account):
    """A suspended tenant is read-only: ingest is rejected with 403.

    The ingest handler runs a suspend check first: a tenant whose
    ``tenants.billing_status`` is ``'suspended'`` is read-only and any upload is
    refused with ``403 account_suspended``. We set the column directly in the DB
    (the same ``podman exec … psql`` path the harness uses for server-side state),
    assert the ``403``, and restore the original value in a ``finally`` so the
    tenant is left usable for any later case. If no suspension column can be
    identified, the test skips rather than fails.
    """
    account = limits_account
    tid = account.tenant_id

    # Discover the suspension mechanism from the live schema rather than assuming.
    # The app suspends via tenants.billing_status = 'suspended' (checked first in
    # the ingest handler); confirm that column exists before driving the case.
    cols = limits._db_exec(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'tenants'"
    )
    columns = {c.strip() for c in cols.splitlines() if c.strip()}
    if "billing_status" not in columns:
        pytest.skip(
            "no 'billing_status' suspension column on tenants — cannot drive the "
            f"suspended-tenant case (available columns: {sorted(columns)})"
        )

    # Capture the current value so we can restore it exactly afterwards. The column
    # is nullable; an empty result string means SQL NULL.
    original = limits._db_exec(f"SELECT billing_status FROM tenants WHERE id = {tid}")

    try:
        limits._db_exec(
            f"UPDATE tenants SET billing_status = 'suspended' WHERE id = {tid}"
        )
        marker = flows.unique_marker("limsusp")
        res = ff.upload(
            account.client._c,
            (f"{marker}.txt", f"suspended upload {marker}".encode("utf-8"), "text/plain"),
            request_id=ff.new_request_id("ingest-limits-suspended"),
        )
        assert res["status"] == 403, (
            f"a suspended tenant (billing_status='suspended') must have ingest "
            f"refused with 403 account_suspended; got {res['status']}: {res['text']!r}"
        )
    finally:
        # Restore the original billing_status (NULL when the captured value is empty).
        if original == "":
            limits._db_exec(
                f"UPDATE tenants SET billing_status = NULL WHERE id = {tid}"
            )
        else:
            safe = original.replace("'", "''")
            limits._db_exec(
                f"UPDATE tenants SET billing_status = '{safe}' WHERE id = {tid}"
            )
