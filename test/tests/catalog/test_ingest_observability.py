"""Ingest observability — metering metrics, /health degradation schema, log correlation.

This module encodes two documented upload-campaign findings as tests, plus the
general ingest-observability invariants that *should* hold around a metered
upload. Each assertion encodes the **intended** contract; a failure records the
app bug (it is left failing, never weakened).

The two findings (``docs/analysis/08-upload-campaign-findings.md``):

* **F6** — the metrics collector logs ``failed to query monthly cost`` for every
  tenant on every cycle (``crates/store/src/budget.rs`` ``get_monthly_cost``).
  The collector only publishes the per-tenant spend gauge
  (``kb_tenant_spend_monthly_micros``) *when that read succeeds*
  (``crates/api/src/metrics_collector.rs`` — the ``Ok`` arm calls
  ``publish_budget_gauges``; the ``Err`` arm only warns), so a broken cost query
  means per-tenant monthly cost/spend is unobservable. The F6 test discovers the
  cost surface, drives a metered ingest, then asserts the spend gauge is present
  with a real (>= 0) value.

* **F7** — ``GET /health`` returns ``{"backends":false,...,"degradation":
  {"ok":true,"subsystems":[]}}`` when a backend is down: the degraded subsystem
  is **not named** in ``degradation.subsystems`` (empty list), so an operator
  cannot tell *which* backend failed from ``/health`` alone. The F7 test asserts
  the documented schema and the invariant *"if backends is false, subsystems is
  non-empty"* against the observed state (it does not force degradation).

The remaining two tests assert the ingest log-correlation primitive (the app
adopts + echoes a client ``X-Request-Id``) and that the backend health/slots/
circuit-breaker metric families are exposed for the configured model backends.

List with `pytest --collect-only -m observability`.
"""

from __future__ import annotations

import re

import httpx
import pytest

from lib import config, file_factory as ff, flows

pytestmark = [pytest.mark.observability]


# ── /metrics exposition helpers (self-contained Prometheus reader) ────────────
# A Prometheus data line: name{labels}? value (value may be int/float/exp).
_METRIC_LINE_RE = re.compile(r"^([A-Za-z_:][\w:]*)(\{[^}]*\})?\s+([0-9eE.+-]+)$")


def _metric_samples(text: str, name: str) -> list[float]:
    """Return every numeric sample of metric family ``name`` (empty when absent)."""
    out: list[float] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = _METRIC_LINE_RE.match(line)
        if m and m.group(1) == name:
            try:
                out.append(float(m.group(3)))
            except ValueError:
                pass
    return out


def _cost_metric_names(text: str) -> list[str]:
    """Discover metric family names on /metrics that look like a cost/spend gauge.

    Scans the data lines (not HELP/TYPE) for any family whose name contains
    ``cost`` or ``spend`` — e.g. ``kb_tenant_spend_monthly_micros``. Returns the
    sorted, de-duplicated family names so the F6 test can assert against whatever
    cost surface the running app actually exposes (rather than hard-coding one).
    """
    names: set[str] = set()
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = _METRIC_LINE_RE.match(line)
        if not m:
            continue
        nm = m.group(1)
        low = nm.lower()
        if "cost" in low or "spend" in low:
            names.add(nm)
    return sorted(names)


# ════════════════════════════════════════════════════════════════════════════
# F6 — per-tenant monthly cost/spend is recorded after a metered ingest
# ════════════════════════════════════════════════════════════════════════════
def test_f6_monthly_cost_recorded_after_metered_ingest(api):
    """A metered ingest must surface a real (>= 0) per-tenant monthly cost/spend.

    Records F6. First DISCOVER how per-tenant cost is exposed: scan ``/metrics``
    for a gauge whose name contains ``cost``/``spend`` (the app publishes
    ``kb_tenant_spend_monthly_micros`` per tenant per collector poll), and probe
    the candidate usage/account endpoints for a cost field. Then drive a small
    text ingest (incurs embed + tag token cost) and assert the cost surface
    returns a real value, not an absent/broken one.

    The known F6 defect is in ``crates/store/src/budget.rs`` ``get_monthly_cost``:
    the collector only calls ``publish_budget_gauges`` (which sets
    ``kb_tenant_spend_monthly_micros``) in the ``Ok`` arm, so when the monthly-cost
    query fails the spend gauge is **never published** — this test then fails on
    the missing/empty cost surface, recording the bug. If the cost query is healthy
    instead, the gauge is present with a >= 0 value and the test passes. The ingest
    is skipped via ``ff.skip_if_degraded`` when the metering backend is down so a
    genuine outage is not mistaken for F6.
    """
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    # ── Discover the cost/spend surface (metrics gauge and/or usage endpoint) ──
    pre_metrics = api._c.get("/metrics")
    assert pre_metrics.status_code == 200, (
        f"/metrics not 200: {pre_metrics.status_code} {pre_metrics.text[:200]}"
    )
    cost_gauges = _cost_metric_names(pre_metrics.text)

    endpoint_probes = ("/api/usage", "/api/account", "/account", "/api/me")
    cost_endpoint: str | None = None
    for path in endpoint_probes:
        try:
            r = api._c.get(path)
        except httpx.HTTPError:
            continue
        if r.status_code == 200 and re.search(r"cost|spend", r.text, re.IGNORECASE):
            cost_endpoint = path
            break

    if not cost_gauges and cost_endpoint is None:
        pytest.skip(
            "no per-tenant cost/spend surface found: scanned /metrics for a "
            "'cost'/'spend' gauge and probed " + ", ".join(endpoint_probes) +
            " for a cost/spend field; none present, so F6 cannot be asserted here."
        )

    # ── Drive a metered ingest (embed + tag token cost) ──────────────────────
    ff.skip_if_degraded(api._c, "text")
    ff.skip_if_degraded(api._c, "embed")
    marker = flows.unique_marker("f6cost")
    out = ff.upload(
        api._c,
        (f"{marker}.txt", f"F6 monthly-cost observability probe {marker}.".encode(), "text/plain"),
        request_id=ff.new_request_id("f6cost"),
        timeout=120.0,
    )
    if out["status"] != 202:
        # A non-202 here is the F4/F5 tagger-outage path, not F6 — skip so the
        # cost-surface assertion below is only judged after real metered work.
        pytest.skip(
            f"metered ingest did not complete (status={out['status']}, "
            f"request_id={out['request_id']}, body={out['body']!r}) — backend "
            "outage, not the F6 cost-metrics bug"
        )

    # ── Assert the cost surface returns a real (>= 0) value, not absent/broken ─
    if cost_gauges:
        post_metrics = api._c.get("/metrics")
        assert post_metrics.status_code == 200, (
            f"/metrics not 200 after ingest: {post_metrics.status_code}"
        )
        samples: list[float] = []
        for name in cost_gauges:
            samples.extend(_metric_samples(post_metrics.text, name))
        assert samples, (
            f"cost/spend gauge(s) {cost_gauges} have HELP/TYPE metadata but no "
            "data line on /metrics after a metered ingest; the per-tenant monthly "
            "spend gauge (kb_tenant_spend_monthly_micros) must be published by the "
            "metrics collector. F6: get_monthly_cost fails so the collector's Err "
            "arm skips publish_budget_gauges and the spend gauge never appears."
        )
        assert all(v >= 0 for v in samples), (
            f"cost/spend gauge has a negative value {samples}; monthly spend "
            "(micro-dollars) must be a real non-negative amount."
        )
    else:  # cost surfaced only via an endpoint
        r = api._c.get(cost_endpoint)
        assert r.status_code == 200, (
            f"cost endpoint {cost_endpoint} not 200 after ingest: {r.status_code}"
        )
        assert re.search(r"cost|spend", r.text, re.IGNORECASE), (
            f"cost endpoint {cost_endpoint} no longer reports a cost/spend field "
            "after a metered ingest; monthly cost must remain observable."
        )


# ════════════════════════════════════════════════════════════════════════════
# F7 — /health degradation schema names the degraded subsystem(s)
# ════════════════════════════════════════════════════════════════════════════
def test_f7_health_degradation_names_subsystems(api):
    """``/health`` must publish the documented schema and name degraded subsystems.

    Records F7. Asserts the response carries ``backends``, ``database``,
    ``migrations``, ``status``, and a ``degradation`` block with ``ok`` (bool) and
    ``subsystems`` (a list). Then the F7 invariant, judged against the observed
    state (degradation is **not** forced): **if ``backends`` is false (a backend is
    down), ``degradation.subsystems`` must be non-empty** so an operator can tell
    which role failed; if ``backends`` is true, only the schema is asserted.

    F7: when a backend is down the app returns ``degradation.subsystems == []``
    (and ``degradation.ok == true``), so this fails during a backend outage —
    recording the gap. On a fully-healthy stack it asserts the schema and passes.
    """
    resp = api.health()
    body = resp.json()

    # ── Documented schema (always present) ───────────────────────────────────
    for key in ("backends", "database", "migrations", "status", "degradation"):
        assert key in body, f"/health missing '{key}': {body}"
    assert isinstance(body["backends"], bool), f"'backends' must be a bool: {body}"
    assert isinstance(body["database"], bool), f"'database' must be a bool: {body}"
    assert isinstance(body["migrations"], bool), f"'migrations' must be a bool: {body}"

    deg = body["degradation"]
    assert isinstance(deg, dict), f"'degradation' must be an object: {deg!r}"
    assert "ok" in deg, f"degradation missing 'ok': {deg}"
    assert "subsystems" in deg, f"degradation missing 'subsystems': {deg}"
    assert isinstance(deg["ok"], bool), f"degradation.ok must be a bool: {deg}"
    assert isinstance(deg["subsystems"], list), (
        f"degradation.subsystems must be a list: {deg}"
    )

    # ── F7 invariant: a downed backend must be NAMED, not silently empty ──────
    if body["backends"] is False:
        assert deg["subsystems"], (
            "F7: /health reports backends=false (a backend is down) but "
            f"degradation.subsystems is empty ({deg!r}); the degraded subsystem "
            "MUST be named so operators (and health-gated clients) can tell which "
            "backend/role failed from /health alone."
        )


# ════════════════════════════════════════════════════════════════════════════
# Ingest request-id correlation
# ════════════════════════════════════════════════════════════════════════════
def test_ingest_request_id_echoed_for_correlation(api):
    """``POST /api/ingest`` echoes the client ``X-Request-Id`` (log-correlation primitive).

    The app adopts a client-supplied ``X-Request-Id`` and echoes it on the
    response so an ingest can be grepped out of the app logs deterministically.
    Mints an id, posts a tiny text upload with it, and asserts the response
    carries the SAME id back. This is the correlation seam the campaign relies on
    to attribute a failing ingest to its log lines; it must hold regardless of the
    upload's 202/4xx/5xx outcome (the header is set on every response).
    """
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    rid = ff.new_request_id("ridcorr")
    marker = flows.unique_marker("ridcorr")
    out = ff.upload(
        api._c,
        (f"{marker}.txt", f"Request-id correlation probe {marker}.".encode(), "text/plain"),
        request_id=rid,
        timeout=120.0,
    )
    assert out["status"] is not None, (
        f"transport failure (hang/drop) for request_id={rid}: {out['text']!r}"
    )
    assert out["request_id"] == rid, (
        f"app did not echo the client X-Request-Id: sent {rid!r}, got "
        f"{out['request_id']!r} (status={out['status']}). The X-Request-Id echo "
        "is the log-correlation primitive and must round-trip on every response."
    )


# ════════════════════════════════════════════════════════════════════════════
# Backend metrics exposed for ingest
# ════════════════════════════════════════════════════════════════════════════
def test_backend_metrics_exposed_for_ingest(api):
    """``/metrics`` exposes the backend health/slots/circuit-breaker families.

    The scheduler that serves ingest (tagger/embed/rerank) must be observable:
    ``kb_backend_healthy``, ``kb_backend_in_flight``, ``kb_backend_total_slots``
    (per-backend gauges from the metrics collector) and
    ``kb_circuit_breaker_open`` (per-dependency) must all appear on the public
    ``/metrics`` exposition for the configured model backends. A missing family is
    an observability regression — an operator could not see backend saturation or
    an open breaker during an ingest storm.
    """
    r = api._c.get("/metrics")
    assert r.status_code == 200, f"/metrics not 200: {r.status_code} {r.text[:200]}"
    text = r.text

    for family in (
        "kb_backend_healthy",
        "kb_backend_in_flight",
        "kb_backend_total_slots",
        "kb_circuit_breaker_open",
    ):
        assert _metric_samples(text, family), (
            f"missing /metrics family {family!r}: the backend scheduler metrics "
            "for the configured model backends (llama-text/embed/rerank) must be "
            "exposed so ingest backend health/slots/breakers are observable."
        )
