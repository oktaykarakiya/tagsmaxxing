"""Catalog — queued-ingest failure UX + tenant retry (P15-T9, §31.5).

A deterministically-unprocessable upload must surface as a **visible**
failure (document ``status == "failed"``, diagnosable job error), be
retryable by its owner via ``POST /api/documents/:id/retry``, render the
failure UX on the web detail page, and be completely invisible to other
tenants (404 / inert — the jobs table sits outside RLS, so these negative
probes guard the explicit tenant filters).

List with `pytest --collect-only -m ingest`.
"""

from __future__ import annotations

import pytest

from lib import config, flows
from lib import file_factory as ff

pytestmark = pytest.mark.ingest


def _login_admin(api):
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())
    return api


def _upload_unprocessable(api) -> dict:
    """Upload bytes that pass admission but deterministically fail extraction.

    An ASCII-leading payload sniffs as ``text/plain`` (always on the MIME
    allow-list) while an embedded invalid UTF-8 sequence (``0xC3 0x28``)
    makes the text extractor reject it on every attempt — admission passes,
    processing deterministically fails. Returns the 202 body. Skips only if
    admission still rejects it (also a correct outcome, but then there is
    nothing to drive the failure path with).
    """
    marker = flows.unique_marker("kbe2efail")
    bad = (
        f"Deterministic failure probe {marker}. ".encode("ascii")
        + b"\xc3\x28"  # invalid UTF-8 sequence: 0xC3 must be followed by 0x80-0xBF
        + b" trailing ascii so the sniffer still sees text."
    )
    out = ff.upload(api._c, [(f"{marker}.txt", bad, "text/plain")])
    if out["status"] != 202:
        pytest.skip(
            f"unprocessable probe was rejected at admission ({out['status']}) — "
            "no async failure path to exercise"
        )
    body = out["body"] or {}
    assert body.get("document_id"), f"202 without document_id: {body}"
    return body


def _wait_until_failed(api, body: dict) -> tuple[dict, dict]:
    """Wait for the terminal failure and assert the P15-T9 visibility contract."""
    outcome, doc, job = flows.wait_until_terminal(
        api, body["document_id"], job_id=body.get("job_id")
    )
    if outcome == "ready":
        pytest.skip("probe unexpectedly processed to ready — no failure to exercise")
    assert doc.get("status") == "failed", (
        f"a dead-lettered ingest must mark the document failed (P15-T9); "
        f"outcome={outcome}, doc status={doc.get('status')!r}"
    )
    assert job.get("last_error"), "the failure must carry a diagnosable last_error"
    return doc, job


# ── 1. Failure is visible + retry restarts processing ───────────────────────


def test_failed_ingest_is_visible_and_retryable(api):
    """failed status → retry 202 → job requeued → terminal again.

    The retry must reset the document to ``pending`` and requeue the SAME
    ingest job (attempts cleared); since the bytes are still unprocessable it
    deterministically fails again — proving the retry genuinely re-ran
    processing rather than flipping a status label.
    """
    _login_admin(api)
    body = _upload_unprocessable(api)
    doc_id = body["document_id"]
    _wait_until_failed(api, body)

    r = api._c.post(f"/api/documents/{doc_id}/retry")
    assert r.status_code == 202, f"retry of a failed ingest must be 202: {r.status_code} {r.text}"
    rbody = r.json()
    assert rbody.get("document_id") == doc_id
    retry_job_id = rbody.get("job_id")
    assert retry_job_id, f"retry must return the requeued job id: {rbody}"

    # The same deterministic failure terminates the retried job too.
    outcome2, doc2, job2 = flows.wait_until_terminal(api, doc_id, job_id=retry_job_id)
    assert outcome2 in ("failed", "dead"), f"expected terminal failure again, got {outcome2}"
    assert doc2.get("status") == "failed"
    assert job2.get("last_error"), "the re-run failure stays diagnosable"


# ── 2. Retry guards: wrong states are clean 4xx ──────────────────────────────


def test_retry_of_ready_document_is_409(api):
    """Retrying a successfully-processed document is a clean 409 conflict."""
    _login_admin(api)
    _marker, doc = flows.ingest_and_wait(api, "Retry guard document. {marker}")
    r = api._c.post(f"/api/documents/{doc['id']}/retry")
    assert r.status_code == 409, (
        f"retry of a ready document must be 409 (not_retryable/no job): "
        f"{r.status_code} {r.text}"
    )
    assert r.json().get("error") in ("not_retryable", "no_ingest_job")


def test_retry_of_unknown_document_is_404(api):
    """Retrying a nonexistent document id is a 404 (no probe surface)."""
    _login_admin(api)
    r = api._c.post("/api/documents/999999999/retry")
    assert r.status_code == 404, f"expected 404, got {r.status_code}: {r.text}"


# ── 3. §31.5 cross-tenant negatives ──────────────────────────────────────────


def test_cross_tenant_retry_and_status_are_404(api, page):
    """Another tenant cannot retry, read, or learn anything about a failed doc.

    The retry endpoint, the document detail, and the job-status endpoint must
    all answer 404 for a foreign tenant — indistinguishable from a
    nonexistent id (no information leak) — and the victim's failed state
    must remain untouched.
    """
    _login_admin(api)
    body = _upload_unprocessable(api)
    doc_id = body["document_id"]
    job_id = body.get("job_id")
    _wait_until_failed(api, body)

    # A fresh, unrelated tenant via the web /signup form (the black-box path
    # that provisions a new tenant; /auth/register only joins existing ones).
    name = flows.unique_marker("intrud")
    password = "Intruder-Passw0rd!42"
    page.goto("/signup")
    page.fill("input[name='tenant_name']", name)
    page.fill("input[name='email']", f"{name}@e2e.local")
    page.fill("input[name='password']", password)
    page.click("form[action='/signup'] button[type='submit']")
    page.wait_for_load_state("networkidle")
    from lib.api_client import ApiClient

    intruder = ApiClient()
    intruder.login(name, f"{name}@e2e.local", password)
    try:
        r = intruder._c.post(f"/api/documents/{doc_id}/retry")
        assert r.status_code == 404, (
            f"cross-tenant retry must 404, got {r.status_code}: {r.text[:200]}"
        )
        assert intruder._c.get(f"/api/documents/{doc_id}").status_code == 404
        if job_id:
            assert intruder._c.get(f"/api/jobs/{job_id}").status_code == 404, (
                "cross-tenant job status must 404 (jobs sit outside RLS — "
                "explicit tenant filter)"
            )
        # Victim state untouched by the foreign attempts.
        doc = api.get_document(doc_id)
        assert doc.get("status") == "failed", "victim document must stay failed"
    finally:
        intruder.close()


# ── 4. Web failure UX ────────────────────────────────────────────────────────


def test_detail_page_shows_failed_badge_error_and_retry_button(api):
    """The web detail page renders the failure alert, error, and retry form."""
    _login_admin(api)
    body = _upload_unprocessable(api)
    doc_id = body["document_id"]
    _wait_until_failed(api, body)

    r = api._c.get(f"/documents/{doc_id}")
    assert r.status_code == 200, f"detail page failed: {r.status_code}"
    html = r.text
    assert 'id="ingest-failed-alert"' in html, "failed-ingest alert box missing"
    assert 'id="retry-ingest-btn"' in html, "retry button missing"
    assert f'action="/documents/{doc_id}/retry"' in html, "retry form action missing"
    assert 'data-testid="ingest-last-error"' in html, (
        "sanitized last_error missing from the failure alert"
    )
