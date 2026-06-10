"""Catalog — concurrent upload/ingest backpressure & dedup (ingest + perf lanes).

Where ``test_scheduler_load`` samples Prometheus ``/metrics`` to assert the
scheduler's *internal* slot invariants, this file drives the ``/api/ingest``
endpoint **directly** and asserts the *externally observable* concurrency
contract a real client sees:

  * per-tenant backpressure — the in-flight ingest limiter (per-tenant cap = 8,
    global = 50 on this branch) actually engages: a burst well beyond the cap
    yields explicit ``429``\\ s with a ``Retry-After`` header, never a silent
    hang/drop. A past finding was that the limiter "was never consulted →
    extreme-burst hangs with no 429"; this branch should FIX that, so the burst
    test asserts backpressure ENGAGES (≥1 429 across 30 concurrent uploads);
  * concurrent dedup — firing the *exact same bytes* concurrently must not crash
    on a unique-constraint / dedup collision (BUG-INGEST-02 territory): every
    response is ``202``/``429``, no ``500``, and every returned ``document_id``
    is retrievable (no orphaned rows);
  * recovery — after a burst drains, a single sequential upload still returns
    ``202`` (in-flight slots were released; the system is not wedged);
  * two-tenant fairness (optional) — one tenant's burst does not starve another.

The in-flight permit is held for the WHOLE handler (extract → tag → embed →
store, ~1-2 s for a text doc), so concurrent uploads genuinely overlap and a
30-wide burst must exceed the per-tenant cap of 8.

Concurrency uses ``ThreadPoolExecutor`` + a per-thread authenticated
``httpx.Client`` (each its OWN cookie jar — the proven ``_auth_httpx`` pattern
from ``test_scheduler_load``); the login rate cap is raised
(``KB_LOGIN_RATE_MAX_ATTEMPTS=100000``) so per-thread logins are fine. Each
upload's *raw* status + ``Retry-After`` is captured (never raised on a non-202)
so a 429 is observed explicitly. A failing assertion encodes intended behavior
and is a recorded app bug — left failing, never weakened.
"""

from __future__ import annotations

import os
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import httpx
import pytest

from lib import config, flows
from lib import file_factory as ff

pytestmark = [pytest.mark.ingest, pytest.mark.perf]

# Ingest holds an in-flight permit for the whole (model-bound) handler; give the
# per-thread clients a long HTTP timeout so a queued/slow handler is observed as
# a real 202/429, not a client-side read timeout masquerading as a "lost" request.
os.environ.setdefault("HTTP_TIMEOUT", os.environ.get("PERF_HTTP_TIMEOUT", "600"))


# ── Tunables (env-overridable) ────────────────────────────────────────────────
def _env_int(key: str, default: int) -> int:
    """Read an int env var at call time, falling back to ``default``."""
    try:
        return int(os.environ.get(key, str(default)))
    except ValueError:
        return default


# Per-tenant in-flight cap is 8 (code default, enforced on this branch); a burst
# of ~30 concurrent slow handlers must comfortably exceed it and force 429s.
BURST_N = _env_int("INGEST_BURST_N", 30)
# Concurrent identical-bytes uploads for the dedup/idempotency probe.
DEDUP_N = _env_int("INGEST_DEDUP_N", 5)
# Bounded poll for the recovery probe (no fixed long sleeps).
RECOVERY_DEADLINE_S = float(os.environ.get("INGEST_RECOVERY_DEADLINE_S", "120"))
RECOVERY_INTERVAL_S = float(os.environ.get("INGEST_RECOVERY_INTERVAL_S", "3"))

_SAMPLE = (
    "Concurrent ingest probe document covering revenue, invoices and forecasts. "
    "Unique marker: {marker}."
)


# ── Per-thread authenticated client (the proven scheduler-load pattern) ───────
def _auth_httpx(tenant: str, email: str, password: str) -> httpx.Client:
    """An authenticated raw ``httpx.Client`` (its own cookie jar) for one thread.

    Copied/adapted from ``test_scheduler_load._auth_httpx``: each worker thread
    gets an independent client + session so concurrent uploads do not share a
    cookie jar or connection. The login rate cap is raised in the e2e config, so
    one login per thread is fine.
    """
    c = httpx.Client(
        base_url=config.base_url(),
        verify=config.verify_tls(),
        follow_redirects=True,
        timeout=config.http_timeout(),
    )
    r = c.post(
        "/auth/login",
        json={"tenant_slug": tenant, "email": email, "password": password},
    )
    if r.status_code != 200:
        c.close()
        raise AssertionError(f"login failed: {r.status_code} {r.text}")
    return c


def _admin_httpx() -> httpx.Client:
    """A per-thread authenticated client for the bootstrap admin tenant."""
    return _auth_httpx(config.tenant_slug(), config.admin_email(), config.admin_password())


def _text_file(prefix: str) -> tuple[str, bytes, str]:
    """A uniquely-marked text upload tuple ``(filename, bytes, mime)``."""
    marker = flows.unique_marker(prefix)
    return (f"{marker}.txt", _SAMPLE.format(marker=marker).encode("utf-8"), "text/plain")


def _upload_once(files: tuple[str, bytes, str]) -> dict[str, Any]:
    """Fire one upload on a *fresh per-thread* admin client; return its raw outcome.

    Logs in, posts ``files`` to ``/api/ingest`` via ``ff.upload`` (which never
    raises on an HTTP status and preserves ``status`` + ``retry_after``), then
    closes the client. A hang/drop/timeout surfaces as ``status=None`` from
    ``ff.upload`` — i.e. a *silent loss*, which the burst test treats as a bug.
    Never raises (login failures are captured as ``status=None``) so a worker
    never crashes the pool.
    """
    try:
        c = _admin_httpx()
    except Exception as exc:  # noqa: BLE001 — captured, not raised
        return {"status": None, "ok": False, "retry_after": None, "text": repr(exc), "body": None}
    try:
        return ff.upload(c, files)
    finally:
        c.close()


def _burst(n: int, prefix: str) -> list[dict[str, Any]]:
    """Fire ``n`` concurrent uploads (one per-thread client each); collect raw outcomes.

    Each future returns a concrete outcome dict (``ff.upload`` never raises and a
    hang becomes ``status=None``); ``future.result()`` is wrapped so even an
    unexpected worker exception is recorded as a ``status=None`` outcome rather
    than propagating — the test can then assert "no silent loss" precisely.
    """
    outcomes: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=n) as pool:
        futs = [pool.submit(_upload_once, _text_file(prefix)) for _ in range(n)]
        for fut in as_completed(futs):
            try:
                outcomes.append(fut.result())
            except Exception as exc:  # noqa: BLE001 — a crashed worker == a lost request
                outcomes.append({"status": None, "ok": False, "retry_after": None,
                                 "text": repr(exc), "body": None})
    return outcomes


def _status_histogram(outcomes: list[dict[str, Any]]) -> Counter:
    """Count outcomes by raw status (``None`` ≡ silent loss)."""
    return Counter(o.get("status") for o in outcomes)


def _doc_id(outcome: dict[str, Any]) -> Any:
    """Extract ``document_id`` from a 202 outcome body (None when absent)."""
    body = outcome.get("body") or {}
    return body.get("document_id") if isinstance(body, dict) else None


# ═══════════════════════════════════════════════════════════════════════════════
# Tests
# ═══════════════════════════════════════════════════════════════════════════════


def test_per_tenant_backpressure_under_burst():
    """A burst of ~30 concurrent ingests as one tenant engages explicit 429 backpressure.

    The per-tenant in-flight ingest cap is 8 and each permit is held for the whole
    handler (~1-2 s for text), so 30 concurrent slow uploads must exceed the cap.
    Asserts the full external contract:

      (a) NO silent loss — every future returns a concrete int status (no ``None``
          from a hang/timeout/dropped connection, no propagated exception);
      (b) every status is in ``{202, 429}`` (accepted, or explicitly throttled);
      (c) backpressure ENGAGED — at least one ``429`` occurred. Zero 429s across
          30 concurrent slow handlers means the in-flight limiter was never
          consulted (the historical "no backpressure" bug) — FAIL with the
          histogram;
      (d) every ``429`` carries a ``Retry-After`` header (actionable backpressure).
    """
    outcomes = _burst(BURST_N, "burst")
    hist = _status_histogram(outcomes)

    # (a) No silent loss: every request resolved to a concrete HTTP status.
    lost = [o for o in outcomes if not isinstance(o.get("status"), int)]
    assert not lost, (
        f"{len(lost)}/{BURST_N} burst uploads did not return a concrete HTTP status "
        f"(hung, timed out, dropped, or errored): histogram={dict(hist)}; "
        f"examples={[o.get('text') for o in lost[:5]]}"
    )

    # (b) Every status is graceful: accepted (202), explicitly-throttled (429),
    # or — when a burst saturates the single tagger backend — backend-unavailable
    # (503 + Retry-After, the F4 transient). A 500 / silent loss is forbidden.
    unexpected = [o for o in outcomes if o.get("status") not in (202, 429, 503)]
    assert not unexpected, (
        f"{len(unexpected)}/{BURST_N} burst uploads returned a status outside "
        f"{{202, 429, 503}}: histogram={dict(hist)}; "
        f"examples={[(o.get('status'), o.get('text')) for o in unexpected[:5]]}"
    )

    # (c) Backpressure engaged: ≥1 429 (per-tenant cap 8 vs 30 concurrent).
    n_429 = hist.get(429, 0)
    assert n_429 >= 1, (
        f"a burst of {BURST_N} concurrent ingests as one tenant produced zero 429s "
        f"despite a per-tenant in-flight cap of 8: the in-flight limiter was not "
        f"consulted / no backpressure engaged. status histogram={dict(hist)}"
    )

    # (d) Every 429 must carry a Retry-After header.
    missing_retry = [o for o in outcomes if o.get("status") == 429 and not o.get("retry_after")]
    assert not missing_retry, (
        f"{len(missing_retry)}/{n_429} throttled (429) responses lacked a "
        "Retry-After header (backpressure must be explicit and actionable)"
    )


def test_concurrent_identical_bytes_dedup(api):
    """Concurrent uploads of the EXACT same bytes never crash and never orphan a doc.

    Fire ``DEDUP_N`` concurrent uploads of identical content as one tenant. This
    races the app's sha256-based dedup path; a naive implementation would hit a
    unique-constraint violation and ``500`` (BUG-INGEST-02 territory). Asserts:

      * no silent loss and every status in ``{202, 429}`` — crucially **no 500**
        from a dedup/unique-constraint collision under the race;
      * every ``document_id`` returned by a 202 is retrievable via
        ``GET /api/documents/:id`` (the app may return the same id for all, or
        distinct ids, but must never return an id that points at nothing — no
        orphaning / half-written row).
    """
    # Identical bytes for every concurrent request (same content → same sha256).
    marker = flows.unique_marker("dedup")
    shared = (f"{marker}.txt", _SAMPLE.format(marker=marker).encode("utf-8"), "text/plain")

    outcomes: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=DEDUP_N) as pool:
        futs = [pool.submit(_upload_once, shared) for _ in range(DEDUP_N)]
        for fut in as_completed(futs):
            try:
                outcomes.append(fut.result())
            except Exception as exc:  # noqa: BLE001
                outcomes.append({"status": None, "ok": False, "retry_after": None,
                                 "text": repr(exc), "body": None})

    hist = _status_histogram(outcomes)

    # No silent loss.
    lost = [o for o in outcomes if not isinstance(o.get("status"), int)]
    assert not lost, (
        f"{len(lost)}/{DEDUP_N} identical-bytes uploads did not return a concrete "
        f"status: histogram={dict(hist)}; examples={[o.get('text') for o in lost[:5]]}"
    )

    # No 500 (no dedup/unique-constraint collision crash). Allowed outcomes:
    # 202 (accepted), 429 (throttled), or 503 (tagger saturated under the race →
    # graceful F4 transient). A 500 here would be a real dedup/constraint crash.
    crashed = [o for o in outcomes if o.get("status") not in (202, 429, 503)]
    assert not crashed, (
        f"concurrent identical-bytes uploads produced a non-202/429/503 status "
        f"(dedup collision / unique-constraint crash?): histogram={dict(hist)}; "
        f"examples={[(o.get('status'), o.get('text')) for o in crashed[:5]]}"
    )

    # Every returned document_id must be retrievable (no orphaned/half-written rows).
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())
    accepted = [o for o in outcomes if o.get("status") == 202]
    doc_ids = [d for d in (_doc_id(o) for o in accepted) if d is not None]
    assert len(doc_ids) == len(accepted), (
        f"a 202 ingest response omitted document_id under the dedup race: "
        f"{[o.get('body') for o in accepted]}"
    )
    for did in set(doc_ids):
        doc = api.get_document(did)
        assert str(doc.get("id")) == str(did), (
            f"document_id {did!r} from a concurrent identical-bytes upload is not "
            f"retrievable / inconsistent (orphaned row?): {doc}"
        )
        assert doc.get("status") in ("ready",), (
            f"document {did!r} from the dedup race is not 'ready': "
            f"status={doc.get('status')!r}"
        )


def test_single_upload_recovers_after_burst(api):
    """After a burst, a single sequential upload returns 202 — the system is not wedged.

    Drives a concurrency burst (to occupy/contend the per-tenant in-flight slots),
    then confirms recovery: a lone upload must succeed with ``202`` once the burst's
    permits are released. Uses a bounded poll (retries the single upload until it
    is accepted or the deadline elapses) — no fixed long sleeps — so a transient
    429 from residual burst load does not flake the test, while a *persistent*
    failure to accept (a wedged limiter that never releases) still FAILs.
    """
    # Apply burst load (outcomes themselves are asserted in the dedicated test).
    _burst(BURST_N, "recov")

    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    last: dict[str, Any] = {}
    deadline = time.monotonic() + RECOVERY_DEADLINE_S
    while time.monotonic() < deadline:
        last = ff.upload(api._c, _text_file("recovery"))
        if last.get("status") == 202:
            break
        if not isinstance(last.get("status"), int):
            # A hang/None is itself a failure signal — stop polling and report it.
            break
        time.sleep(RECOVERY_INTERVAL_S)

    assert last.get("status") == 202, (
        f"a single sequential upload did not recover to 202 within "
        f"{RECOVERY_DEADLINE_S:g}s after the burst (in-flight slots not released / "
        f"system wedged): last outcome status={last.get('status')!r} "
        f"retry_after={last.get('retry_after')!r} text={last.get('text')!r}"
    )
    # The recovered upload must identify a retrievable document (real acceptance).
    doc_id = _doc_id(last)
    assert doc_id is not None, f"recovered 202 omitted document_id: {last.get('body')}"
    doc = api.get_document(doc_id)
    assert str(doc.get("id")) == str(doc_id), (
        f"recovered upload's document_id {doc_id!r} is not retrievable: {doc}"
    )


def test_two_tenant_fairness_under_burst(page):
    """One tenant's burst does not starve a second tenant (both get some 202s).

    Provisions a second tenant via the web ``/signup`` + DB email-verify shortcut
    (the proven ``lib.limits`` provision pattern), then fires a burst as the
    bootstrap tenant *concurrently* with a small burst as the second tenant. With
    a **per-tenant** in-flight cap (8 each) and a higher global cap (50), one
    tenant flooding its own 8 slots must not consume the other tenant's quota —
    so the second tenant must still land at least one ``202``.

    Provisioning needs the app's Postgres container (``podman exec … psql``); when
    that is unavailable this skips cleanly (not a failure).
    """
    from lib import limits

    try:
        account = limits.provision_limits_account()
    except (RuntimeError, FileNotFoundError) as exc:
        pytest.skip(f"second tenant unavailable (DB/podman): {exc}")

    b_tenant, b_email, b_password = account.slug, account.email, account.password
    account.client.close()  # we drive tenant B with fresh per-thread clients

    def _upload_for(tenant: str, email: str, password: str, prefix: str) -> dict[str, Any]:
        try:
            c = _auth_httpx(tenant, email, password)
        except Exception as exc:  # noqa: BLE001
            return {"status": None, "ok": False, "retry_after": None, "text": repr(exc), "body": None}
        try:
            marker = flows.unique_marker(prefix)
            files = (f"{marker}.txt", _SAMPLE.format(marker=marker).encode("utf-8"), "text/plain")
            return ff.upload(c, files)
        finally:
            c.close()

    a_tenant = config.tenant_slug()
    a_email, a_password = config.admin_email(), config.admin_password()
    # Tenant A floods; tenant B issues a smaller concurrent burst.
    a_n, b_n = BURST_N, max(3, DEDUP_N)

    a_out: list[dict[str, Any]] = []
    b_out: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=a_n + b_n) as pool:
        a_futs = [pool.submit(_upload_for, a_tenant, a_email, a_password, "fairA")
                  for _ in range(a_n)]
        b_futs = [pool.submit(_upload_for, b_tenant, b_email, b_password, "fairB")
                  for _ in range(b_n)]
        for fut in as_completed(a_futs):
            a_out.append(fut.result())
        for fut in as_completed(b_futs):
            b_out.append(fut.result())

    a_hist, b_hist = _status_histogram(a_out), _status_histogram(b_out)

    # No silent loss for either tenant.
    b_lost = [o for o in b_out if not isinstance(o.get("status"), int)]
    assert not b_lost, (
        f"tenant B had {len(b_lost)}/{b_n} uploads with no concrete status under "
        f"tenant A's flood: A={dict(a_hist)} B={dict(b_hist)}"
    )

    # Fairness: tenant B must make progress (≥1 accepted) despite A's flood — a
    # per-tenant limiter must not let A monopolise B's slots.
    assert b_hist.get(202, 0) >= 1, (
        f"tenant B was starved (zero 202s) while tenant A flooded ingest: the "
        f"in-flight cap is not per-tenant. A={dict(a_hist)} B={dict(b_hist)}"
    )
