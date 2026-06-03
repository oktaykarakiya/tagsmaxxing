"""Reusable user-journey helpers shared by tests (API + browser)."""

from __future__ import annotations

import uuid
from typing import Any

from .api_client import ApiClient


def unique_marker(prefix: str = "kbe2e") -> str:
    """A short, unique, search-friendly token to tag an ingested document."""
    return f"{prefix}{uuid.uuid4().hex[:12]}"


def ingest_and_wait(
    api: ApiClient,
    content: str,
    *,
    filename: str | None = None,
    user_note: str | None = None,
) -> tuple[str, dict[str, Any]]:
    """Ingest a uniquely-marked text doc and return its processed detail.

    ``content`` may contain a ``{marker}`` placeholder; otherwise the marker is
    appended. Ingestion is **synchronous** — the inline pipeline (extract → tag →
    embed → store) finishes before ``POST /api/ingest`` returns — so the document
    is ready immediately; no job polling is needed. Returns ``(marker, doc)`` where
    ``doc`` is the parsed ``GET /api/documents/:id`` body (tags, summary, files, …).
    """
    marker = unique_marker()
    text = content.format(marker=marker) if "{marker}" in content else f"{content}\n\nUnique marker: {marker}."
    resp = api.ingest_text(filename or f"{marker}.txt", text, user_note=user_note)
    doc_id = resp.get("document_id")
    if doc_id is None:
        raise AssertionError(f"ingest returned no document_id: {resp}")
    return marker, api.get_document(doc_id)


def browser_login(page: Any, tenant_slug: str, email: str, password: str) -> None:
    """Log in through the real web `/login` form (CSRF handled by the page)."""
    page.goto("/login")
    page.fill("#tenant_slug", tenant_slug)
    page.fill("#email", email)
    page.fill("#password", password)
    page.click("form[action='/login'] button[type='submit']")
    page.wait_for_load_state("networkidle")
