"""Catalog (step-2 stubs) — health, readiness, metrics.

List with `pytest --collect-only -m observability`.
"""

from __future__ import annotations

import pytest

from lib import config, flows

pytestmark = pytest.mark.observability


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
