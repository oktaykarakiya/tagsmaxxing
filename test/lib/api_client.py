"""Minimal black-box HTTP client for the KB JSON API.

Mirrors a real API consumer: cookie-based session auth (the `__Host-session`
cookie set by `/auth/login`, persisted in the client's cookie jar), then the
`/api/*` endpoints. The `/auth` and `/api` routers carry no CSRF, so a logged-in
client can call them directly.
"""

from __future__ import annotations

import time
from typing import Any

import httpx

from . import config


class ApiError(RuntimeError):
    """Raised when an API call returns an unexpected status."""


class ApiClient:
    """Thin wrapper over ``httpx.Client`` with helpers for the app's endpoints."""

    def __init__(self, base_url: str | None = None) -> None:
        self.base_url = base_url or config.base_url()
        self._c = httpx.Client(
            base_url=self.base_url,
            verify=config.verify_tls(),
            follow_redirects=True,
            timeout=config.http_timeout(),
        )

    def close(self) -> None:
        """Close the underlying HTTP connection pool."""
        self._c.close()

    # ── Auth ──────────────────────────────────────────────────────────────────
    def login(self, tenant_slug: str, email: str, password: str) -> dict[str, Any]:
        """Authenticate; the session cookie is stored in the client's jar."""
        r = self._c.post(
            "/auth/login",
            json={"tenant_slug": tenant_slug, "email": email, "password": password},
        )
        if r.status_code != 200:
            raise ApiError(f"login failed: {r.status_code} {r.text}")
        return r.json()

    def register(self, tenant_slug: str, email: str, password: str) -> dict[str, Any]:
        """Register a new user/tenant; the session cookie is stored on success."""
        r = self._c.post(
            "/auth/register",
            json={"tenant_slug": tenant_slug, "email": email, "password": password},
        )
        if r.status_code not in (200, 201):
            raise ApiError(f"register failed: {r.status_code} {r.text}")
        return r.json()

    def logout(self) -> None:
        """Revoke the current session."""
        self._c.post("/auth/logout")

    # ── Ingest ──────────────────────────────────────────────────────────────────
    def ingest_text(
        self,
        filename: str,
        content: str,
        *,
        user_note: str | None = None,
        group_as_document: bool = False,
    ) -> dict[str, Any]:
        """Upload a single text file for ingestion; return the parsed response.

        ``POST /api/ingest`` runs the pipeline (extract → tag → embed → store)
        **synchronously** and returns ``202`` with ``{job_id, document_id,
        message}``. ``job_id`` is a sentinel ``0`` — there is no async job to poll;
        the document is already processed and ``document_id`` identifies it. The
        async job queue / :meth:`get_job` is only for re-tag / re-embed / retry,
        not this upload path.
        """
        files = [("files", (filename, content.encode("utf-8"), "text/plain"))]
        data: dict[str, str] = {}
        if user_note:
            data["user_note"] = user_note
        if group_as_document:
            data["group_as_document"] = "true"
        r = self._c.post("/api/ingest", files=files, data=data)
        if r.status_code != 202:
            raise ApiError(f"ingest failed: {r.status_code} {r.text}")
        return r.json()

    def get_job(self, job_id: int) -> dict[str, Any]:
        """Fetch the status of an ingest job."""
        r = self._c.get(f"/api/jobs/{job_id}")
        if r.status_code != 200:
            raise ApiError(f"job fetch failed: {r.status_code} {r.text}")
        return r.json()

    def wait_for_job(
        self, job_id: int, *, timeout: float | None = None, interval: float = 3.0
    ) -> dict[str, Any]:
        """Poll a job until it reaches a terminal state (done/failed/dead) or times out."""
        timeout = timeout if timeout is not None else config.job_done_timeout()
        deadline = time.monotonic() + timeout
        last: dict[str, Any] = {}
        while time.monotonic() < deadline:
            last = self.get_job(job_id)
            if last.get("status") in ("done", "failed", "dead"):
                return last
            time.sleep(interval)
        raise ApiError(f"job {job_id} did not finish within {timeout}s; last seen: {last}")

    # ── Search / documents ───────────────────────────────────────────────────────
    def search(
        self, q: str, *, limit: int = 10, kind: str | None = None, tag: str | None = None
    ) -> dict[str, Any]:
        """Run a hybrid search query and return the parsed response."""
        params: dict[str, Any] = {"q": q, "limit": limit}
        if kind:
            params["kind"] = kind
        if tag:
            params["tag"] = tag
        r = self._c.get("/api/search", params=params)
        if r.status_code != 200:
            raise ApiError(f"search failed: {r.status_code} {r.text}")
        return r.json()

    def get_document(self, doc_id: int | str) -> dict[str, Any]:
        """Fetch a document's detail (files, tags, summary, metadata)."""
        r = self._c.get(f"/api/documents/{doc_id}")
        if r.status_code != 200:
            raise ApiError(f"document fetch failed: {r.status_code} {r.text}")
        return r.json()

    # ── Admin (super-admin only) ──────────────────────────────────────────────────
    def set_quota_override(
        self,
        tenant_id: int,
        *,
        quota_override_bytes: int | None = None,
        quota_override_tokens: int | None = None,
    ) -> dict[str, Any]:
        """Set a tenant's admin quota override (``POST /admin/tenants/:id/quota-override``).

        Super-admin only (the caller must be logged in as an Owner/Admin of the
        platform operator's bootstrap tenant). ``None`` fields clear that
        dimension's override. Raises :class:`ApiError` on a non-200 response so
        gating failures surface loudly.
        """
        r = self._c.post(
            f"/admin/tenants/{tenant_id}/quota-override",
            json={
                "quota_override_bytes": quota_override_bytes,
                "quota_override_tokens": quota_override_tokens,
            },
        )
        if r.status_code != 200:
            raise ApiError(f"set_quota_override failed: {r.status_code} {r.text}")
        return r.json()

    # ── Health ────────────────────────────────────────────────────────────────────
    def health(self) -> httpx.Response:
        """Return the raw `/health` response (200 only when the stack is ready)."""
        return self._c.get("/health")
