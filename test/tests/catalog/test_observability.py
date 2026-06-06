"""Catalog — health, readiness, Prometheus metrics.

Beyond the health / exposition-format / family-presence basics, the
``test_*_metric_*`` tests below verify the P14 metering observability seam: the
revived counters/gauges are present on the public ``/metrics`` endpoint and
actually MOVE under real traffic (``kb_tokens_total`` rises after an
ingest+search; the per-tenant ``kb_active_users`` / ``kb_tenant_tokens_monthly``
gauges appear; ``kb_subsystem_degraded`` is published).

List with `pytest --collect-only -m observability`.
"""

from __future__ import annotations

import re
import time

import httpx
import pytest

from lib import config, flows

pytestmark = pytest.mark.observability

# ── /metrics counter-parse helper ────────────────────────────────────────────
# A small, self-contained reader for the public, unauthenticated /metrics
# exposition so a test can assert a family is present and/or measure a
# before/after counter delta. (The perf-lane test_scheduler_load.py has a richer
# sampler, but it is file-local to that perf module; this keeps the observability
# lane dependency-free.)

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


def _family_present(text: str, name: str) -> bool:
    """True when ``name`` has at least one real data line (not just HELP/TYPE)."""
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = _METRIC_LINE_RE.match(line)
        if m and m.group(1) == name:
            return True
    return False


def _counter_total(text: str, name: str) -> float:
    """Sum every sample of counter family ``name`` (absent family → ``0.0``)."""
    total = 0.0
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = _METRIC_LINE_RE.match(line)
        if m and m.group(1) == name:
            try:
                total += float(m.group(3))
            except ValueError:
                pass
    return total


def _wait_for_family(name: str, timeout: float = 40.0, interval: float = 2.0) -> str:
    """Poll ``/metrics`` until family ``name`` appears (bounded), returning the text.

    The per-tenant gauges (``kb_active_users``, ``kb_tenant_tokens_monthly``) are
    published only by the periodic metrics collector (30 s poll in the e2e
    config), so a freshly-provisioned tenant's series may lag the request that
    created it. Polling with a deadline — rather than a single fixed sleep —
    avoids both a flaky race and an over-long wait. Returns the last-scraped text
    so the caller can assert on it; on timeout returns the final scrape (the
    caller's presence assertion then fails with a useful diff).
    """
    deadline = time.monotonic() + timeout
    text = _metrics_text()
    while not _family_present(text, name) and time.monotonic() < deadline:
        time.sleep(interval)
        text = _metrics_text()
    return text


def test_health_ok_when_ready(api):
    """GET /health returns 200 with DB + migrations + backends healthy."""
    resp = api.health()
    assert resp.status_code == 200, f"Expected 200 OK, got {resp.status_code}: {resp.text}"
    body = resp.json()
    assert body["status"] == "ok", f"Expected status 'ok', got: {body}"
    assert body["database"] is True, f"database check failed: {body}"
    assert body["migrations"] is True, f"migrations check failed: {body}"
    assert body["backends"] is True, f"backends check failed: {body}"
    assert body["degradation"]["ok"] is True, f"degradation not ok: {body}"


def test_health_reports_degradation(api):
    """The health payload reflects degraded dependencies."""
    resp = api.health()
    body = resp.json()

    # The degradation field must always be present, reporting the current
    # degradation state of every subsystem — even when everything is healthy
    # (ok = true, subsystems = []).
    assert "degradation" in body, (
        f"Health response missing 'degradation' field: {body}"
    )
    deg = body["degradation"]
    assert "ok" in deg, f"Degradation block missing 'ok' flag: {deg}"
    assert "subsystems" in deg, (
        f"Degradation block missing 'subsystems' list: {deg}"
    )
    assert isinstance(deg["subsystems"], list), (
        f"'subsystems' must be a list, got {type(deg['subsystems']).__name__}"
    )

    # When overall status is 'ok', no subsystems should be degraded.
    if body["status"] == "ok":
        assert deg["ok"] is True
        assert deg["subsystems"] == [], (
            f"Expected empty subsystems when healthy, got: {deg['subsystems']}"
        )

    # When degraded, status should be 'degraded' with a 503 status code,
    # and at least one subsystem should be listed.
    if body["status"] == "degraded":
        assert resp.status_code == 503, (
            f"Degraded health must return 503, got {resp.status_code}"
        )
        assert deg["ok"] is False
        assert len(deg["subsystems"]) > 0, (
            "Degraded state must name at least one subsystem"
        )


def test_metrics_prometheus_format(api):
    """GET /metrics returns Prometheus exposition format."""
    r = api._c.get("/metrics")
    assert r.status_code == 200, f"Expected 200 OK, got {r.status_code}: {r.text}"
    text = r.text
    assert len(text) > 0, "Metrics endpoint returned empty body"

    # Prometheus exposition format (plan §15):
    # - Lines starting with '#' are comments, HELP, or TYPE metadata.
    # - Metric data lines follow: name{labels} value  or  name value.
    # - Blank lines separate metric families.
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        # Must be a metric data line with at least a name and a value.
        assert " " in stripped, (
            f"Metric data line missing space separator: {stripped!r}"
        )
        # The metric name (before any '{' or ' ') should start with a letter.
        name = stripped.split("{", 1)[0].split(" ")[0]
        assert name and name[0].isalpha(), (
            f"Invalid metric name in line: {stripped!r}"
        )

    # Prometheus exposition format requires HELP and TYPE metadata lines
    # for every recorded metric family.
    assert "# HELP" in text, "Metrics output missing HELP metadata lines"
    assert "# TYPE" in text, "Metrics output missing TYPE metadata lines"


def test_metrics_families_present(api):
    """Backend health/slots/requests/storage/queue metric families exist."""
    # Ingest a document to trigger LLM calls (tagger + embedder record
    # request metrics), blob storage (storage metrics), and job processing
    # (queue metrics). This ensures all families have been observed at
    # least once so they appear in the Prometheus output.
    tenant, email, password = (
        config.tenant_slug(),
        config.admin_email(),
        config.admin_password(),
    )
    api.login(tenant, email, password)
    flows.ingest_and_wait(api, "Observability metrics test document. {marker}")

    r = api._c.get("/metrics")
    assert r.status_code == 200, f"Expected 200 OK, got {r.status_code}: {r.text}"
    text = r.text

    # ── Backend health / slots families ────────────────────────────────────
    # Populated by the periodic metrics collector (crates/api/src/metrics_collector.rs).
    assert "kb_backend_healthy" in text, (
        "Missing kb_backend_healthy metric family"
    )
    assert "kb_backend_free_slots" in text, (
        "Missing kb_backend_free_slots metric family"
    )
    assert "kb_backend_total_slots" in text, (
        "Missing kb_backend_total_slots metric family"
    )
    assert "kb_backend_in_flight" in text, (
        "Missing kb_backend_in_flight metric family"
    )

    # ── Request metrics ────────────────────────────────────────────────────
    assert "kb_requests_total" in text, (
        "Missing kb_requests_total metric family"
    )

    # ── Storage metrics ────────────────────────────────────────────────────
    assert "kb_storage_bytes_used" in text, (
        "Missing kb_storage_bytes_used metric family"
    )

    # ── Queue metrics ──────────────────────────────────────────────────────
    assert "kb_queue_depth" in text, (
        "Missing kb_queue_depth metric family"
    )


# ═══════════════════════════════════════════════════════════════════════════════
# P14 metering-seam observability — the revived counters/gauges are present and MOVE
#
# These complement the kb-metrics/metrics-collector Rust unit tests (which assert
# the render shape) by proving the *running* app actually feeds them under real
# traffic: tokens are counted, the per-tenant gauges get published by the
# collector, and the degradation gauge is emitted. A FAILED row is a real
# observability regression — left failing.
# ═══════════════════════════════════════════════════════════════════════════════


def test_tokens_total_metric_rises_after_ingest_and_search(limits_account):
    """kb_tokens_total increases after a metered ingest + search (P14-T10).

    Every metered AI call (tagger, per-chunk embedding, query embedding, rerank)
    is counted exactly once at the single metering chokepoint, so the global
    ``kb_tokens_total`` counter must climb across an ingest followed by a search.
    Driven on the dedicated limits tenant with a fresh usage reset so the work
    definitely meters new tokens; asserted on the family total (summed across all
    role/model label series) which is monotonic.
    """
    acct = limits_account
    acct.reset_usage()

    before = _counter_total(_metrics_text(), "kb_tokens_total")

    # One ingest (tagger + per-chunk embeddings) then one search (query embedding
    # + rerank) — both metered. Tiny payload so storage/token caps don't reject.
    marker = flows.unique_marker("tokmetric")
    acct.client.ingest_text(f"{marker}.txt", f"Token metering observability probe {marker}.")
    acct.client.search(marker, limit=5)

    after = _counter_total(_metrics_text(), "kb_tokens_total")
    assert after > before, (
        f"kb_tokens_total did not rise after an ingest+search (before={before}, "
        f"after={after}); the metering seam must count tokens for every AI call "
        "so per-role/model spend is observable."
    )


def test_per_tenant_usage_gauges_published(limits_account):
    """kb_active_users and kb_tenant_tokens_monthly appear for the tenant (P14-T8).

    Both are per-tenant gauges published only by the periodic metrics collector
    (30 s poll in the e2e config). After an ingest establishes monthly token
    usage, poll ``/metrics`` with a bounded deadline (no fixed-sleep race) until
    the families appear, then assert the tenant's own labelled series is present —
    proving the collector reconciles the rollup and publishes the active-user
    count, not just that the family name exists from some other tenant.
    """
    acct = limits_account
    acct.reset_usage()

    # Establish some monthly token usage so kb_tenant_tokens_monthly has a value
    # to publish for this tenant on the next collector tick.
    marker = flows.unique_marker("gaugemetric")
    acct.client.ingest_text(f"{marker}.txt", f"Per-tenant gauge observability probe {marker}.")

    # active-users is published every tick from the start; wait for it first.
    text = _wait_for_family("kb_active_users")
    assert _family_present(text, "kb_active_users"), (
        "kb_active_users gauge family never appeared on /metrics; the collector "
        "must publish a per-tenant active-user count each poll."
    )
    tenant_label = f'tenant_id="{acct.tenant_id}"'
    assert any(
        tenant_label in ln
        for ln in text.splitlines()
        if ln.strip().startswith("kb_active_users") and not ln.strip().startswith("#")
    ), (
        f"kb_active_users has no series for tenant_id={acct.tenant_id}; the "
        "limits tenant's user count must be published, not only other tenants'."
    )

    # The monthly-tokens rollup gauge follows on a subsequent reconcile tick.
    text2 = _wait_for_family("kb_tenant_tokens_monthly")
    assert _family_present(text2, "kb_tenant_tokens_monthly"), (
        "kb_tenant_tokens_monthly gauge family never appeared on /metrics; the "
        "collector must publish each tenant's reconciled monthly token total so "
        "operators can graph consumption per tenant."
    )
    assert any(
        tenant_label in ln
        for ln in text2.splitlines()
        if ln.strip().startswith("kb_tenant_tokens_monthly") and not ln.strip().startswith("#")
    ), (
        f"kb_tenant_tokens_monthly has no series for tenant_id={acct.tenant_id}; "
        "the limits tenant's monthly token usage must be published."
    )


def test_subsystem_degraded_gauge_present(api):
    """kb_subsystem_degraded is published (one 0/1 series per subsystem) (P14-T9).

    Unlike the dead circuit-breaker gauge, the subsystem-degraded gauge has a live
    producer: the metrics collector emits a series for every known subsystem each
    poll (a healthy one still reports 0 so dashboards see a value, not a vanished
    metric). It is an in-memory gauge whose first tick runs at startup, so it must
    be present on a running stack regardless of health.
    """
    # Touch the app so the stack is warm (the gauge is independent of this, but a
    # request keeps the test honest about hitting a live server).
    api.health()

    text = _wait_for_family("kb_subsystem_degraded")
    assert _family_present(text, "kb_subsystem_degraded"), (
        "kb_subsystem_degraded gauge family is absent from /metrics. Unlike the "
        "statically-described-but-unwired circuit-breaker gauge, this one has a "
        "production caller (the metrics collector) and must emit a 0/1 series per "
        "subsystem so degradation is alertable."
    )
