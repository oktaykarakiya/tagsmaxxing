"""Reusable user-journey helpers shared by tests (API + browser)."""

from __future__ import annotations

import time
import uuid
from typing import Any

from .api_client import ApiClient

# How long ``ingest_and_wait`` polls for a document to reach ``ready`` before
# failing. Generous: the queued worker's tagging call runs on a ~5 tok/s local
# model, and a busy queue may hold several jobs ahead of ours.
READY_TIMEOUT_S = 180.0
# Extra allowance for time the job verifiably spends still *queued* (or
# awaiting a retry) behind the box-wide backlog: the suite's fire-and-forget
# uploads share one queue, so a late test's job may sit behind many others.
# Queued time does not count against the processing budget; the total wait is
# hard-capped at ``timeout_s + QUEUE_WAIT_CAP_S``.
QUEUE_WAIT_CAP_S = 900.0
_POLL_INTERVAL_S = 1.0


def unique_marker(prefix: str = "kbe2e") -> str:
    """A short, unique, search-friendly token to tag an ingested document."""
    return f"{prefix}{uuid.uuid4().hex[:12]}"


def wait_until_terminal(
    api: ApiClient,
    doc_id: int | str,
    *,
    job_id: int | None = None,
    timeout_s: float = READY_TIMEOUT_S,
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    """Poll until queued ingestion reaches a terminal outcome (P15).

    Returns ``(outcome, doc, job)`` where ``outcome`` is one of:

    - ``"ready"`` — the worker finished; ``doc`` is the processed detail.
    - ``"failed"`` — the document reached ``status == "failed"``.
    - ``"dead"`` — the ingest job dead-lettered (pre-T9 the document itself
      may stay ``pending`` forever, so the job state is the terminal signal;
      requires ``job_id``).

    Raises ``AssertionError`` only on timeout (no terminal state reached).

    ``timeout_s`` budgets the *processing* time only. When ``job_id`` is known,
    time the job verifiably spends still ``queued`` (or parked for a retry)
    does not count against it — that is backlog wait, not processing — but the
    total wait is hard-capped at ``timeout_s + QUEUE_WAIT_CAP_S``. Without a
    ``job_id`` the whole wait is bounded by ``timeout_s``.
    """
    start = time.monotonic()
    hard_deadline = start + timeout_s + (QUEUE_WAIT_CAP_S if job_id is not None else 0.0)
    # Armed (= now + timeout_s) once the job leaves the backlog; armed
    # immediately when there is no job to observe.
    processing_deadline: float | None = None if job_id is not None else start + timeout_s
    doc: dict[str, Any] = {}
    job: dict[str, Any] = {}
    while True:
        doc = api.get_document(doc_id)
        status = doc.get("status")
        if status == "ready":
            return "ready", doc, _job_state(api, job_id)
        if status == "failed":
            return "failed", doc, _job_state(api, job_id)
        now = time.monotonic()
        if job_id is not None:
            job = _job_state(api, job_id)
            jstatus = job.get("status")
            if jstatus == "dead":
                return "dead", doc, job
            if jstatus in ("queued", "failed"):
                processing_deadline = None  # waiting in the backlog / for a retry
            elif processing_deadline is None:
                processing_deadline = now + timeout_s
        if processing_deadline is not None and now >= processing_deadline:
            raise AssertionError(
                f"document {doc_id} not ready after {timeout_s:.0f}s of processing "
                f"(status={doc.get('status')!r}, job={job.get('status')!r}, "
                f"last_error={job.get('last_error')!r})"
            )
        if now >= hard_deadline:
            raise AssertionError(
                f"document {doc_id} not ready after the {hard_deadline - start:.0f}s "
                f"hard cap (queue never drained to our job; "
                f"status={doc.get('status')!r}, job={job.get('status')!r}, "
                f"last_error={job.get('last_error')!r})"
            )
        time.sleep(_POLL_INTERVAL_S)


def wait_until_ready(
    api: ApiClient,
    doc_id: int | str,
    *,
    job_id: int | None = None,
    timeout_s: float = READY_TIMEOUT_S,
) -> dict[str, Any]:
    """Poll ``GET /api/documents/:id`` until ``status == "ready"`` (P15).

    Ingestion is **queued** (P15): the 202 stages a pending document and a
    background worker finalizes it, so callers must poll for readiness. Fails
    fast on ``status == "failed"`` or a dead-lettered job. Budgeting semantics
    are those of :func:`wait_until_terminal`, which this wraps.
    """
    outcome, doc, job = wait_until_terminal(api, doc_id, job_id=job_id, timeout_s=timeout_s)
    if outcome == "ready":
        return doc
    if outcome == "dead":
        raise AssertionError(
            f"document {doc_id} ingest job {job_id} dead-lettered: "
            f"last_error={job.get('last_error')!r}"
        )
    raise AssertionError(
        f"document {doc_id} failed during queued processing: "
        f"job={job.get('status')!r} last_error={job.get('last_error')!r}"
    )


def _job_state(api: ApiClient, job_id: int | None) -> dict[str, Any]:
    """Best-effort ``GET /api/jobs/:id`` — diagnostics must never mask the wait."""
    if job_id is None:
        return {}
    try:
        return api.get_job(job_id)
    except Exception:  # noqa: BLE001 — transient job-endpoint errors are non-fatal here
        return {}


def ingest_and_wait(
    api: ApiClient,
    content: str,
    *,
    filename: str | None = None,
    user_note: str | None = None,
) -> tuple[str, dict[str, Any]]:
    """Ingest a uniquely-marked text doc and return its processed detail.

    ``content`` may contain a ``{marker}`` placeholder; otherwise the marker is
    appended. Ingestion is **queued** (P15): ``POST /api/ingest`` returns 202
    with the staged ``document_id`` immediately and a background worker
    finalizes it, so this helper polls until the document is ``ready``.
    Returns ``(marker, doc)`` where ``doc`` is the parsed
    ``GET /api/documents/:id`` body (tags, summary, files, …).
    """
    marker = unique_marker()
    text = content.format(marker=marker) if "{marker}" in content else f"{content}\n\nUnique marker: {marker}."
    resp = api.ingest_text(filename or f"{marker}.txt", text, user_note=user_note)
    doc_id = resp.get("document_id")
    if doc_id is None:
        raise AssertionError(f"ingest returned no document_id: {resp}")
    return marker, wait_until_ready(api, doc_id, job_id=resp.get("job_id"))


def browser_login(page: Any, tenant_slug: str, email: str, password: str) -> None:
    """Log in through the real web `/login` form (CSRF handled by the page)."""
    page.goto("/login")
    page.fill("#tenant_slug", tenant_slug)
    page.fill("#email", email)
    page.fill("#password", password)
    page.click("form[action='/login'] button[type='submit']")
    page.wait_for_load_state("networkidle")
