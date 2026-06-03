"""Catalog (step 2) — scheduler slot/queue invariants under load (perf lane).

Where ``test_performance`` asserts *behaviour* (uploads succeed, searches return,
throughput/latency floors), this file asserts the **scheduler's internal
invariants** by sampling Prometheus ``/metrics`` while driving contention — the
contracts the scheduler must never violate no matter the load:

  * slot-respect   — per-backend ``kb_backend_in_flight`` never exceeds
                     ``kb_backend_total_slots``;
  * accounting     — ``kb_backend_free_slots + kb_backend_in_flight ==
                     kb_backend_total_slots`` at every sample (no lease drift);
  * queue drain    — ``kb_queue_depth`` rises under a burst then returns to ~0
                     (no stuck/leaked leases), ``in_flight`` settles back to 0;
  * throttle       — extreme bursts push back (``kb_ingest_throttled_total``
                     rises and/or 429s with ``Retry-After``), never silent loss;
  * fairness        — two tenants both make progress when one floods the queue;
  * stability       — backends stay healthy / circuit breakers stay closed under
                     normal heavy load.

These run only in the perf lane (``./test/run.sh perf``). Drive concurrency with
``ThreadPoolExecutor`` + a separate ``ApiClient`` per thread, and sample
``GET /metrics`` from a background poller thread during the load window. Metric
names + labels follow ``test/scripts/scheduler_monitor.py``. A failing invariant
is a real scheduler bug and is left failing.
"""

from __future__ import annotations

import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import httpx
import pytest

from lib import config, flows
from lib.api_client import ApiClient

pytestmark = pytest.mark.perf

# Ingest under load is slow (real models); give workers a long HTTP timeout.
os.environ.setdefault("HTTP_TIMEOUT", os.environ.get("PERF_HTTP_TIMEOUT", "600"))

_SAMPLE = (
    "Scheduler load probe document covering revenue, invoices and forecasts. "
    "Unique marker: {marker}."
)


# ── Tunables (env-overridable; lenient defaults for slow local models) ────────
def _env_int(key: str, default: int) -> int:
    """Read an int env var at call time, falling back to ``default``."""
    try:
        return int(os.environ.get(key, str(default)))
    except ValueError:
        return default


def _env_float(key: str, default: float) -> float:
    """Read a float env var at call time, falling back to ``default``."""
    try:
        return float(os.environ.get(key, str(default)))
    except ValueError:
        return default


# Contention load (slot-respect / accounting / health): enough concurrent work
# to exceed the text backend's slot count (2 in the e2e config) and keep the
# scheduler busy while we sample.
LOAD_INGESTS = _env_int("SCHED_LOAD_INGESTS", 4)
LOAD_SEARCHES = _env_int("SCHED_LOAD_SEARCHES", 6)
# Burst large enough to queue behind the available slots.
BURST_INGESTS = _env_int("SCHED_BURST_INGESTS", 6)
# An "extreme" burst, well beyond capacity, to force explicit shedding.
EXTREME_BURST = _env_int("SCHED_EXTREME_BURST", 12)
# Cross-tenant fairness.
FLOOD_WORKERS = _env_int("SCHED_FLOOD_WORKERS", 3)
B_SEARCHES = _env_int("SCHED_B_SEARCHES", 5)
B_INTERVAL_S = _env_float("SCHED_B_INTERVAL_S", 2.0)
B_SEARCH_BOUND_S = _env_float("SCHED_B_SEARCH_BOUND_S", 120.0)
B_INGEST_BOUND_S = _env_float("SCHED_B_INGEST_BOUND_S", 400.0)
# Sampling + settle.
POLL_INTERVAL_S = _env_float("SCHED_POLL_INTERVAL_S", 1.0)
SETTLE_S = _env_float("SCHED_SETTLE_S", 6.0)
QUIESCE_DEADLINE_S = _env_float("SCHED_QUIESCE_DEADLINE_S", 90.0)


# ── Prometheus /metrics sampling ──────────────────────────────────────────────
_METRIC_RE = re.compile(r"^([A-Za-z_:][\w:]*)(\{[^}]*\})?\s+([0-9eE.+-]+)$")
_LABEL_RE = re.compile(r'(\w+)="([^"]*)"')

# Parsed sample: metric name -> list of (labels, value).
Sample = dict[str, list[tuple[dict[str, str], float]]]


def _metrics_client() -> httpx.Client:
    """A bare HTTP client for the public, unauthenticated ``/metrics`` endpoint."""
    return httpx.Client(
        base_url=config.base_url(),
        verify=config.verify_tls(),
        timeout=20.0,
    )


def _parse_prom(text: str) -> Sample:
    """Parse Prometheus text exposition into ``{name: [(labels, value), …]}``."""
    out: Sample = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line[0] == "#":
            continue
        m = _METRIC_RE.match(line)
        if not m:
            continue
        name, lbls, val = m.group(1), m.group(2) or "", m.group(3)
        labels = dict(_LABEL_RE.findall(lbls))
        try:
            out.setdefault(name, []).append((labels, float(val)))
        except ValueError:
            pass
    return out


def _fetch_metrics(client: httpx.Client) -> Sample:
    """Fetch + parse ``/metrics`` once (raises on a non-200)."""
    r = client.get("/metrics")
    r.raise_for_status()
    return _parse_prom(r.text)


def _by_backend(series: list[tuple[dict[str, str], float]]) -> dict[str, float]:
    """Index a metric's series by its ``backend_id`` label."""
    out: dict[str, float] = {}
    for labels, v in series:
        out[labels.get("backend_id", "?")] = v
    return out


def _global_value(sample: Sample, metric: str) -> float:
    """Return an unlabelled (global) metric's value, defaulting to 0.0 when absent.

    A metric the running app never observes is omitted from ``/metrics`` entirely,
    so "absent" is treated as 0.0 — the same convention as
    ``scripts/scheduler_monitor.py``.
    """
    series = sample.get(metric)
    return series[0][1] if series else 0.0


class _MetricsPoller:
    """Background thread that snapshots ``/metrics`` at a fixed interval.

    Use as a context manager around a load window; afterwards read ``.samples``
    (a list of ``(monotonic_ts, parsed_sample)``).
    """

    def __init__(self, interval: float = POLL_INTERVAL_S) -> None:
        self.interval = interval
        self.samples: list[tuple[float, Sample]] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._client = _metrics_client()

    def start(self) -> _MetricsPoller:
        """Begin polling in a daemon thread."""
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.samples.append((time.monotonic(), _fetch_metrics(self._client)))
            except Exception:
                pass  # transient scrape failure — skip this tick
            self._stop.wait(self.interval)

    def stop(self) -> None:
        """Stop polling and close the client."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=15)
        self._client.close()

    def __enter__(self) -> _MetricsPoller:
        return self.start()

    def __exit__(self, *_exc: object) -> None:
        self.stop()


# ── Load generation ───────────────────────────────────────────────────────────
def _admin_client() -> ApiClient:
    """A fresh ``ApiClient`` logged in as the bootstrap admin (one per thread)."""
    c = ApiClient()
    c.login(config.tenant_slug(), config.admin_email(), config.admin_password())
    return c


def _one_ingest() -> tuple[str, str]:
    """Run a single uniquely-marked ingest as admin; never raises."""
    c = _admin_client()
    try:
        marker = flows.unique_marker("load")
        c.ingest_text(f"{marker}.txt", _SAMPLE.format(marker=marker))
        return ("ingest", "ok")
    except Exception as exc:  # noqa: BLE001 — collected, not raised
        return ("ingest", repr(exc))
    finally:
        c.close()


def _one_search() -> tuple[str, str]:
    """Run a single search as admin; never raises."""
    c = _admin_client()
    try:
        c.search("revenue invoices forecast knowledge base")
        return ("search", "ok")
    except Exception as exc:  # noqa: BLE001 — collected, not raised
        return ("search", repr(exc))
    finally:
        c.close()


def _drive_load_and_sample(
    n_ingests: int,
    n_searches: int,
    *,
    settle_s: float = SETTLE_S,
    interval: float = POLL_INTERVAL_S,
) -> tuple[list[tuple[float, Sample]], list[tuple[str, str]]]:
    """Fire ``n_ingests`` + ``n_searches`` concurrently while sampling ``/metrics``.

    Returns ``(samples, op_results)``. A short ``settle_s`` tail keeps sampling
    after the load returns so callers can observe the queue/in-flight drain.

    The window always begins from a **quiescent baseline** (waits for in-flight to
    drain first) so a preceding test's residual load cannot pollute this test's
    invariants — keeping the scheduler invariants deterministic regardless of the
    order tests run in within the perf lane.
    """
    _wait_quiescent()
    poller = _MetricsPoller(interval).start()
    results: list[tuple[str, str]] = []
    total = max(1, n_ingests + n_searches)
    try:
        with ThreadPoolExecutor(max_workers=total) as pool:
            futs = [pool.submit(_one_ingest) for _ in range(n_ingests)]
            futs += [pool.submit(_one_search) for _ in range(n_searches)]
            for fut in as_completed(futs):
                results.append(fut.result())
        if settle_s > 0:
            time.sleep(settle_s)
    finally:
        poller.stop()
    return poller.samples, results


def _wait_quiescent(
    deadline_s: float = QUIESCE_DEADLINE_S, interval: float = 2.0
) -> tuple[bool, Sample]:
    """Poll ``/metrics`` until the scheduler is idle, or the deadline elapses.

    Quiescent ≡ every backend's ``kb_backend_in_flight`` ≤ 0.5, ``kb_queue_depth``
    ≤ 0.5 and ``kb_queue_oldest_job_age_secs`` ≤ 2.0. Returns ``(ok, last_sample)``.
    """
    client = _metrics_client()
    last: Sample = {}
    deadline = time.monotonic() + deadline_s
    try:
        while time.monotonic() < deadline:
            try:
                last = _fetch_metrics(client)
            except Exception:
                time.sleep(interval)
                continue
            in_flight = _by_backend(last.get("kb_backend_in_flight", []))
            qd = _global_value(last, "kb_queue_depth")
            age = _global_value(last, "kb_queue_oldest_job_age_secs")
            if all(v <= 0.5 for v in in_flight.values()) and qd <= 0.5 and age <= 2.0:
                return True, last
            time.sleep(interval)
        return False, last
    finally:
        client.close()


# ── Raw (status-preserving) ingest for the throttle test ──────────────────────
def _auth_httpx(tenant: str, email: str, password: str) -> httpx.Client:
    """An authenticated raw ``httpx.Client`` (its own cookie jar) for one thread."""
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


def _raw_ingest_outcome() -> dict[str, Any]:
    """Fire one ingest and report its *raw* outcome (status + Retry-After, no raise).

    Unlike ``ApiClient.ingest_text`` (which raises on any non-202), this preserves
    the HTTP status so the throttle test can see a 429 explicitly.
    """
    try:
        c = _auth_httpx(config.tenant_slug(), config.admin_email(), config.admin_password())
    except Exception as exc:  # noqa: BLE001
        return {"status": None, "error": repr(exc)}
    try:
        marker = flows.unique_marker("burst")
        files = [("files", (f"{marker}.txt", _SAMPLE.format(marker=marker).encode("utf-8"), "text/plain"))]
        r = c.post("/api/ingest", files=files)
        return {"status": r.status_code, "retry_after": r.headers.get("retry-after")}
    except Exception as exc:  # noqa: BLE001 — a hang/drop surfaces here
        return {"status": None, "error": repr(exc)}
    finally:
        c.close()


# ── Cross-tenant signup helper (the proven isolation-test pattern) ────────────
def _slugify(name: str) -> str:
    """Replicate the server-side slugify: lowercase, non-alnum → single hyphen."""
    slug: list[str] = []
    for ch in name.strip().lower():
        if ch.isalnum():
            slug.append(ch)
        elif slug and slug[-1] != "-":
            slug.append("-")
    return "".join(slug).rstrip("-") or "workspace"


def _signup_and_login(page: Any, workspace_name: str, email: str, password: str) -> ApiClient:
    """Sign up a fresh tenant + owner via the web ``/signup`` form, return a logged-in client.

    ``/auth/register`` only attaches a user to an *existing* tenant, so a brand-new
    tenant must be created through the signup form; afterwards we authenticate an
    independent ``ApiClient`` over the JSON API.
    """
    page.goto("/signup")
    page.fill("input[name='tenant_name']", workspace_name)
    page.fill("input[name='email']", email)
    page.fill("input[name='password']", password)
    page.click("form button[type='submit']")
    page.wait_for_load_state("networkidle")
    client = ApiClient()
    client.login(_slugify(workspace_name), email, password)
    return client


# ═══════════════════════════════════════════════════════════════════════════════
# Tests
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.mark.perf
def test_slot_respect_under_contention(api):
    """Per-backend in_flight never exceeds total_slots while load is applied.

    Sample ``/metrics`` from a poller thread while firing concurrent ingests +
    searches. At every sample and for every backend, ``kb_backend_in_flight`` must
    be ≤ ``kb_backend_total_slots`` — the scheduler must never over-admit beyond a
    backend's slot count. Assert zero violations across the whole window.
    """
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    samples, _ = _drive_load_and_sample(LOAD_INGESTS, LOAD_SEARCHES)

    assert samples, "no /metrics samples were collected during the load window"
    backends_seen: set[str] = set()
    violations: list[str] = []
    for _ts, snap in samples:
        in_flight = _by_backend(snap.get("kb_backend_in_flight", []))
        total = _by_backend(snap.get("kb_backend_total_slots", []))
        for bid, tot in total.items():
            backends_seen.add(bid)
            inf = in_flight.get(bid, 0.0)
            if inf > tot + 0.5:
                violations.append(f"{bid}: in_flight={inf:g} > total_slots={tot:g}")

    assert backends_seen, "no per-backend slot metrics were exposed on /metrics"
    assert not violations, (
        "scheduler over-admitted beyond a backend's slot capacity:\n  "
        + "\n  ".join(violations[:20])
    )


@pytest.mark.perf
def test_slot_accounting_consistent(api):
    """Slot accounting stays consistent (free + in_flight == total), no lease drift.

    While load is applied, sample ``/metrics`` and assert for every backend and
    every sample that ``kb_backend_free_slots + kb_backend_in_flight ==
    kb_backend_total_slots`` (within rounding). A persistent mismatch means leased
    slots are leaking or being double-counted.
    """
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    samples, _ = _drive_load_and_sample(LOAD_INGESTS, LOAD_SEARCHES)

    assert samples, "no /metrics samples were collected during the load window"
    mismatches: list[str] = []
    backends_seen: set[str] = set()
    for _ts, snap in samples:
        free = _by_backend(snap.get("kb_backend_free_slots", []))
        in_flight = _by_backend(snap.get("kb_backend_in_flight", []))
        total = _by_backend(snap.get("kb_backend_total_slots", []))
        for bid, tot in total.items():
            backends_seen.add(bid)
            fr = free.get(bid, 0.0)
            inf = in_flight.get(bid, 0.0)
            if abs((fr + inf) - tot) > 0.5:
                mismatches.append(
                    f"{bid}: free({fr:g}) + in_flight({inf:g}) != total({tot:g})"
                )

    assert backends_seen, "no per-backend slot metrics were exposed on /metrics"
    assert not mismatches, (
        "slot accounting drifted (leaked / double-counted leases):\n  "
        + "\n  ".join(mismatches[:20])
    )


@pytest.mark.perf
def test_backpressure_saturates_and_drains(api):
    """A burst beyond capacity saturates a backend's slots, then drains to 0.

    Ingest is processed **inline** against the scheduler's per-backend slot
    semaphore (not an async job queue — ``kb_queue_depth`` tracks the async job
    queue, which inline ingest does not feed), so the real backpressure signal for
    a burst is the slot lease: a concurrent burst larger than a backend's
    ``kb_backend_total_slots`` must drive that backend's ``kb_backend_in_flight``
    *up to* ``total_slots`` (every slot engaged — backpressure working), and after
    the burst every backend's in_flight must return to 0 (no leaked leases).

    Distinct from ``test_slot_respect_under_contention`` (which bounds
    ``in_flight <= total``) and ``test_no_leaked_leases_after_load`` (which only
    checks the drain): this asserts the burst actually *reaches* full slot
    utilisation — that backpressure visibly engages rather than the work being
    under-admitted or over-admitted.
    """
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    samples, _ = _drive_load_and_sample(BURST_INGESTS, 0)
    assert samples, "no /metrics samples were collected during the burst"

    # During the burst, at least one backend's in_flight must reach its
    # total_slots — the slot semaphore engaged backpressure (burst > capacity).
    saturated = False
    for _ts, snap in samples:
        in_flight = _by_backend(snap.get("kb_backend_in_flight", []))
        total = _by_backend(snap.get("kb_backend_total_slots", []))
        for backend, tot in total.items():
            if tot >= 1 and in_flight.get(backend, 0.0) >= tot - 0.5:
                saturated = True
                break
        if saturated:
            break
    assert saturated, (
        f"no backend's in_flight reached total_slots during a burst of "
        f"{BURST_INGESTS} concurrent ingests (vs a 2-slot text backend): the slot "
        "semaphore never engaged full backpressure on /metrics"
    )

    # After the burst + settle, every backend's leases must release (no leaks).
    ok, final = _wait_quiescent()
    final_in_flight = _by_backend(final.get("kb_backend_in_flight", []))
    leaked = {b: v for b, v in final_in_flight.items() if v > 0.5}
    assert ok and not leaked, (
        f"in-flight leases did not return to 0 after the burst drained: {leaked}"
    )


@pytest.mark.perf
def test_no_leaked_leases_after_load(api):
    """After the load window ends, in_flight and queue_depth settle back to zero.

    Apply a mixed load, then stop and poll ``/metrics`` until quiescent. Within a
    bounded settle time, every backend's ``kb_backend_in_flight`` must return to 0,
    ``kb_queue_depth`` to 0, and ``kb_queue_oldest_job_age_secs`` to ~0 — proving
    no lease/job is orphaned once demand stops.
    """
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    # Apply a mixed load and let it finish.
    _drive_load_and_sample(LOAD_INGESTS, LOAD_SEARCHES, settle_s=0.0)

    # Now demand has stopped — the scheduler must return to a fully idle state.
    ok, final = _wait_quiescent()
    in_flight = _by_backend(final.get("kb_backend_in_flight", []))
    queue_depth = _global_value(final, "kb_queue_depth")
    oldest_age = _global_value(final, "kb_queue_oldest_job_age_secs")

    leaked = {b: v for b, v in in_flight.items() if v > 0.5}
    assert ok, (
        f"scheduler did not return to idle within {QUIESCE_DEADLINE_S:g}s after load: "
        f"in_flight={in_flight} queue_depth={queue_depth:g} oldest_age={oldest_age:g}"
    )
    assert not leaked, f"orphaned in-flight leases after load stopped: {leaked}"
    assert queue_depth <= 0.5, f"queue did not drain after load: depth={queue_depth:g}"
    assert oldest_age <= 2.0, f"a stale job lingered after load: oldest_age={oldest_age:g}s"


@pytest.mark.perf
def test_extreme_burst_is_throttled_not_lost(api):
    """An extreme ingest burst is pushed back explicitly, never silently dropped.

    Fire an ingest burst well beyond capacity. The scheduler must shed load
    *explicitly*: either ``kb_ingest_throttled_total`` increments and/or some
    requests return HTTP 429 with a ``Retry-After`` header. Every request must end
    in a definite outcome (accepted or an explicit 429) — none may hang or be
    silently dropped — and the app stays healthy afterward.
    """
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    # Begin from a quiescent baseline so a preceding test's residual load does not
    # confound this test's "did the burst get explicitly shed" assertion.
    _wait_quiescent()

    mc = _metrics_client()
    try:
        throttled_before = _global_value(_fetch_metrics(mc), "kb_ingest_throttled_total")
    finally:
        mc.close()

    outcomes: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=EXTREME_BURST) as pool:
        futs = [pool.submit(_raw_ingest_outcome) for _ in range(EXTREME_BURST)]
        for fut in as_completed(futs):
            outcomes.append(fut.result())

    mc = _metrics_client()
    try:
        throttled_after = _global_value(_fetch_metrics(mc), "kb_ingest_throttled_total")
    finally:
        mc.close()

    # 1. No silent loss: every request resolved to a definite HTTP outcome —
    #    either accepted (202) or explicitly throttled (429). A None status means
    #    the request hung, timed out, or the connection was dropped.
    lost = [o for o in outcomes if o.get("status") not in (202, 429)]
    assert not lost, (
        f"{len(lost)}/{EXTREME_BURST} burst requests did not end in a definite "
        f"202/429 outcome (hung, dropped, or errored): {lost[:5]}"
    )

    # 2. Any 429 must carry a Retry-After header (explicit, actionable backpressure).
    missing_retry = [o for o in outcomes if o.get("status") == 429 and not o.get("retry_after")]
    assert not missing_retry, (
        f"{len(missing_retry)} throttled (429) responses lacked a Retry-After header"
    )

    # 3. Explicit shedding: beyond capacity, the scheduler must push back via the
    #    throttle counter and/or 429s — not silently accept an unbounded burst.
    threw_429 = any(o.get("status") == 429 for o in outcomes)
    counter_rose = throttled_after > throttled_before
    assert threw_429 or counter_rose, (
        f"an extreme burst ({EXTREME_BURST} concurrent ingests, well beyond the "
        "2-slot text backend) was admitted without explicit backpressure: no 429 "
        "responses and kb_ingest_throttled_total did not increase "
        f"({throttled_before:g} → {throttled_after:g})"
    )

    # 4. The app stays healthy after the burst.
    assert api.health().status_code == 200, "health check failed after the extreme burst"


@pytest.mark.perf
def test_cross_tenant_fairness_under_load(api, page):
    """Under load from one tenant, a second tenant still makes progress (no starvation).

    Register two tenants. Tenant A floods the scheduler with a sustained ingest
    burst; concurrently tenant B issues a steady trickle of ingests/searches. B's
    operations must keep completing within a reasonable bound (no indefinite
    starvation) — the scheduler must not let one tenant monopolise all slots.
    """
    a_name = flows.unique_marker("flooda")
    b_name = flows.unique_marker("trickb")
    password = "p4ssw0rd!Z"
    api_a = _signup_and_login(page, a_name, f"{a_name}@e2e.local", password)
    api_b = _signup_and_login(page, b_name, f"{b_name}@e2e.local", password)

    a_slug = _slugify(a_name)
    stop = threading.Event()
    flood_errors: list[str] = []

    def _flood() -> None:
        c = ApiClient()
        try:
            c.login(a_slug, f"{a_name}@e2e.local", password)
            while not stop.is_set():
                try:
                    m = flows.unique_marker("flood")
                    c.ingest_text(f"{m}.txt", _SAMPLE.format(marker=m))
                except Exception as exc:  # noqa: BLE001
                    flood_errors.append(repr(exc))
        finally:
            c.close()

    workers = [threading.Thread(target=_flood, daemon=True) for _ in range(FLOOD_WORKERS)]
    b_latencies: list[tuple[str, float]] = []
    b_errors: list[tuple[str, str]] = []
    try:
        for w in workers:
            w.start()
        # Give tenant A a head start so the scheduler is genuinely saturated.
        time.sleep(2.0)

        # Tenant B: a steady trickle of searches (a cheap, frequent progress signal).
        for _ in range(B_SEARCHES):
            t0 = time.monotonic()
            try:
                api_b.search("knowledge base revenue forecast")
                b_latencies.append(("search", time.monotonic() - t0))
            except Exception as exc:  # noqa: BLE001
                b_errors.append(("search", repr(exc)))
            time.sleep(B_INTERVAL_S)

        # Tenant B: one write must also get through under the flood.
        t0 = time.monotonic()
        try:
            _marker, doc = flows.ingest_and_wait(api_b, _SAMPLE)
            b_latencies.append(("ingest", time.monotonic() - t0))
            assert doc.get("tags"), "tenant B's ingest produced no tags under load"
        except Exception as exc:  # noqa: BLE001
            b_errors.append(("ingest", repr(exc)))
    finally:
        stop.set()
        for w in workers:
            w.join(timeout=B_INGEST_BOUND_S)
        api_a.close()
        api_b.close()

    # Tenant B must not be starved: every operation completed (no errors/timeouts)
    # and within a reasonable latency bound.
    assert not b_errors, (
        "tenant B operations failed while tenant A flooded the scheduler "
        f"(possible starvation): {b_errors}"
    )
    search_lats = [d for kind, d in b_latencies if kind == "search"]
    ingest_lats = [d for kind, d in b_latencies if kind == "ingest"]
    assert len(search_lats) == B_SEARCHES, (
        f"tenant B completed only {len(search_lats)}/{B_SEARCHES} searches under load"
    )
    assert max(search_lats) <= B_SEARCH_BOUND_S, (
        f"tenant B search starved under tenant-A flood: slowest "
        f"{max(search_lats):.1f}s > bound {B_SEARCH_BOUND_S:g}s"
    )
    assert ingest_lats and ingest_lats[0] <= B_INGEST_BOUND_S, (
        f"tenant B ingest starved under tenant-A flood: "
        f"{(ingest_lats[0] if ingest_lats else float('inf')):.1f}s > bound {B_INGEST_BOUND_S:g}s"
    )


@pytest.mark.perf
def test_backends_stay_healthy_under_load(api):
    """Healthy backends remain healthy and no circuit breaker opens under load.

    During a normal (not fault-injected) heavy load window, sample ``/metrics``
    and assert ``kb_backend_healthy`` stays 1 for each real backend and
    ``kb_circuit_breaker_open`` stays 0 throughout — heavy-but-healthy traffic must
    not trip the breaker or mark a live backend unhealthy.
    """
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    samples, _ = _drive_load_and_sample(LOAD_INGESTS, LOAD_SEARCHES)
    assert samples, "no /metrics samples were collected during the load window"

    backends_seen: set[str] = set()
    unhealthy: list[str] = []
    breaker_open: list[str] = []
    for _ts, snap in samples:
        for bid, val in _by_backend(snap.get("kb_backend_healthy", [])).items():
            backends_seen.add(bid)
            if val < 1.0:
                unhealthy.append(f"{bid} healthy={val:g}")
        for labels, val in snap.get("kb_circuit_breaker_open", []):
            if val >= 1.0:
                dep = labels.get("dependency", labels.get("backend_id", "?"))
                breaker_open.append(f"{dep} open={val:g}")

    assert backends_seen, "no backend health metrics were exposed on /metrics"
    assert not unhealthy, (
        "a live backend was marked unhealthy under heavy-but-healthy load:\n  "
        + "\n  ".join(sorted(set(unhealthy))[:20])
    )
    assert not breaker_open, (
        "a circuit breaker opened under heavy-but-healthy load:\n  "
        + "\n  ".join(sorted(set(breaker_open))[:20])
    )
