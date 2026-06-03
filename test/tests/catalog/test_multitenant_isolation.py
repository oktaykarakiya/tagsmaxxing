"""Catalog (step-2) — multi-tenant isolation / RLS.

High-value security tests: a tenant must never see another tenant's data.
List with `pytest --collect-only -m isolation`.
"""

from __future__ import annotations

import pytest

from lib import config, flows
from lib.api_client import ApiClient

pytestmark = pytest.mark.isolation


# ── Helpers ────────────────────────────────────────────────────────────────────

def _slugify(name: str) -> str:
    """Replicate the server-side slugify: lowercase, replace non-alnum with hyphens."""
    slug: list[str] = []
    for c in name.strip().lower():
        if c.isalnum():
            slug.append(c)
        elif slug and slug[-1] != "-":
            slug.append("-")
    result = "".join(slug).rstrip("-")
    return result or "workspace"


def _signup_and_login(page, workspace_name: str, email: str, password: str) -> ApiClient:
    """Sign up a brand-new tenant + owner via the web /signup form, then return an
    ApiClient that is already logged in for that tenant.

    The Playwright ``page`` is used only for the signup form submission; the
    returned ``ApiClient`` is independent and cookie-authenticated via the JSON
    ``/auth/login`` endpoint.
    """
    page.goto("/signup")
    page.fill("input[name='tenant_name']", workspace_name)
    page.fill("input[name='email']", email)
    page.fill("input[name='password']", password)
    page.click("form button[type='submit']")
    page.wait_for_load_state("networkidle")

    # Tenant + user created (Owner role). Log in via the JSON API.
    slug = _slugify(workspace_name)
    client = ApiClient()
    client.login(slug, email, password)
    return client


# ── Tests ──────────────────────────────────────────────────────────────────────


def test_cross_tenant_document_read_404(page):
    """Tenant A cannot read tenant B's document id (404, not 403)."""
    marker_a = flows.unique_marker("tena")
    marker_b = flows.unique_marker("tenb")

    api_a = _signup_and_login(page, marker_a, f"{marker_a}@e2e.local", "p4ssw0rd!Z")
    api_b = _signup_and_login(page, marker_b, f"{marker_b}@e2e.local", "p4ssw0rd!Z")

    # Tenant A ingests a uniquely-marked document.
    _marker, doc_a = flows.ingest_and_wait(api_a, "Confidential tenant-A document. {marker}")
    doc_id = doc_a["id"]

    # Tenant B tries to read Tenant A's document — must be 404.
    r = api_b._c.get(f"/api/documents/{doc_id}")
    assert r.status_code == 404, (
        f"tenant B reading tenant A's doc {doc_id}: expected 404, got {r.status_code}"
    )

    # Tenant A can still read their own document.
    own = api_a.get_document(doc_id)
    assert own["id"] == doc_id

    api_a.close()
    api_b.close()


def test_cross_tenant_search_no_leak(page):
    """A search as tenant A never returns tenant B's documents."""
    marker_a = flows.unique_marker("tena")
    marker_b = flows.unique_marker("tenb")

    api_a = _signup_and_login(page, marker_a, f"{marker_a}@e2e.local", "p4ssw0rd!Z")
    api_b = _signup_and_login(page, marker_b, f"{marker_b}@e2e.local", "p4ssw0rd!Z")

    # Tenant A ingests a uniquely-marked document.
    marker, _doc_a = flows.ingest_and_wait(api_a, "Tenant-A-only financial report. {marker}")

    # Tenant B searches for A's unique marker — must find nothing.
    results_b = api_b.search(marker)
    assert results_b.get("count", 0) == 0, (
        f"tenant B search for A's marker {marker!r} leaked "
        f"{results_b.get('count')} hits: {results_b.get('hits')}"
    )

    # Tenant A *can* find their own document.
    results_a = api_a.search(marker)
    assert results_a.get("count", 0) >= 1, (
        f"tenant A search for own marker {marker!r} returned "
        f"{results_a.get('count')} hits (expected >= 1)"
    )

    api_a.close()
    api_b.close()


def test_cross_tenant_job_status_404(page):
    """Tenant A cannot read tenant B's job ids."""
    marker_a = flows.unique_marker("tena")
    marker_b = flows.unique_marker("tenb")

    api_a = _signup_and_login(page, marker_a, f"{marker_a}@e2e.local", "p4ssw0rd!Z")
    api_b = _signup_and_login(page, marker_b, f"{marker_b}@e2e.local", "p4ssw0rd!Z")

    # Tenant A ingests a document (synchronous — job_id 0 sentinel is returned;
    # real async job ids are only created for re-tag / re-embed / retry).
    _marker, _doc_a = flows.ingest_and_wait(api_a, "Tenant-A job-isolation document. {marker}")

    # Tenant B tries to access jobs that don't belong to them.
    # Every job id is tenant-scoped, so B gets 404 on all of them.
    for job_id in (0, 1):
        r = api_b._c.get(f"/api/jobs/{job_id}")
        assert r.status_code == 404, (
            f"tenant B accessing job {job_id}: expected 404, got {r.status_code}"
        )

    api_a.close()
    api_b.close()


def test_api_token_scoped_to_tenant(page):
    """A bearer token issued in tenant A cannot act on tenant B."""
    marker_a = flows.unique_marker("tena")
    marker_b = flows.unique_marker("tenb")

    api_a = _signup_and_login(page, marker_a, f"{marker_a}@e2e.local", "p4ssw0rd!Z")
    api_b = _signup_and_login(page, marker_b, f"{marker_b}@e2e.local", "p4ssw0rd!Z")

    # Both tenants ingest uniquely-marked documents.
    m_a, doc_a = flows.ingest_and_wait(api_a, "Tenant A document for token test. {marker}")
    m_b, doc_b = flows.ingest_and_wait(api_b, "Tenant B document for token test. {marker}")

    # Tenant A creates an API token.
    r = api_a._c.post("/api/tokens", json={"name": "isolation-test-token"})
    assert r.status_code == 201, f"create API token failed: {r.status_code} {r.text}"
    token_data = r.json()
    raw_token = token_data["token"]
    assert len(raw_token) == 64, f"token length {len(raw_token)}, expected 64"

    # Use Tenant A's token in a fresh httpx client (no session cookie).
    import httpx

    c = httpx.Client(
        base_url=config.base_url(),
        verify=config.verify_tls(),
        headers={"Authorization": f"Bearer {raw_token}"},
        timeout=config.http_timeout(),
    )
    try:
        # With A's token: can find A's own document.
        r = c.get("/api/search", params={"q": m_a, "limit": 10})
        assert r.status_code == 200, f"search with token failed: {r.status_code}"
        assert r.json().get("count", 0) >= 1, (
            f"token from tenant A should find own doc {m_a!r}, "
            f"got {r.json().get('count')} hits"
        )

        # With A's token: must NOT find B's document.
        r = c.get("/api/search", params={"q": m_b, "limit": 10})
        assert r.status_code == 200, f"search with token failed: {r.status_code}"
        assert r.json().get("count", 0) == 0, (
            f"token from tenant A leaked tenant B's doc: {r.json().get('hits')}"
        )

        # With A's token: cannot read B's document directly.
        r = c.get(f"/api/documents/{doc_b['id']}")
        assert r.status_code == 404, (
            f"token from tenant A reading tenant B's doc {doc_b['id']}: "
            f"expected 404, got {r.status_code}"
        )
    finally:
        c.close()

    api_a.close()
    api_b.close()


def test_cross_tenant_file_download_404(page):
    """Tenant A cannot download tenant B's file ids."""
    marker_a = flows.unique_marker("tena")
    marker_b = flows.unique_marker("tenb")

    api_a = _signup_and_login(page, marker_a, f"{marker_a}@e2e.local", "p4ssw0rd!Z")
    api_b = _signup_and_login(page, marker_b, f"{marker_b}@e2e.local", "p4ssw0rd!Z")

    # Tenant A ingests a document with at least one file.
    _marker, doc_a = flows.ingest_and_wait(api_a, "Tenant-A file-download document. {marker}")
    files = doc_a.get("files", [])
    assert files, "ingested document has no file entries"
    file_id = files[0]["id"]
    doc_id = doc_a["id"]

    # Tenant B tries to download Tenant A's file — must be 404.
    r = api_b._c.get(
        f"/api/documents/{doc_id}/file/{file_id}",
        follow_redirects=False,
    )
    assert r.status_code == 404, (
        f"tenant B downloading tenant A's file {file_id}: "
        f"expected 404, got {r.status_code}"
    )

    api_a.close()
    api_b.close()


def test_admin_scope_limited_to_own_tenant(page):
    """A tenant admin cannot manage users/jobs of another tenant."""
    marker_a = flows.unique_marker("tena")
    marker_b = flows.unique_marker("tenb")

    api_a = _signup_and_login(page, marker_a, f"{marker_a}@e2e.local", "p4ssw0rd!Z")
    api_b = _signup_and_login(page, marker_b, f"{marker_b}@e2e.local", "p4ssw0rd!Z")

    # Tenant A's admin ingests a document.
    marker, doc_a = flows.ingest_and_wait(api_a, "Tenant A admin document. {marker}")

    # Tenant B's admin tries to read A's document via API — must be 404.
    r = api_b._c.get(f"/api/documents/{doc_a['id']}")
    assert r.status_code == 404, (
        f"admin of tenant B reading tenant A's doc {doc_a['id']}: "
        f"expected 404, got {r.status_code}"
    )

    # Tenant B's admin searches — must not find A's document.
    results_b = api_b.search(marker)
    assert results_b.get("count", 0) == 0, (
        f"admin of tenant B search for A's marker leaked "
        f"{results_b.get('count')} hits"
    )

    # Tenant B's admin tries A's job ids — must be 404.
    r = api_b._c.get("/api/jobs/0")
    assert r.status_code == 404, (
        f"admin of tenant B accessing tenant A's job: "
        f"expected 404, got {r.status_code}"
    )

    # Sanity: Tenant A's admin CAN still read their own document.
    own = api_a.get_document(doc_a["id"])
    assert own["id"] == doc_a["id"]

    api_a.close()
    api_b.close()


def test_same_content_isolated_per_tenant(page):
    """Identical uploads in two tenants stay independent (no shared rows)."""
    marker_a = flows.unique_marker("tena")
    marker_b = flows.unique_marker("tenb")

    api_a = _signup_and_login(page, marker_a, f"{marker_a}@e2e.local", "p4ssw0rd!Z")
    api_b = _signup_and_login(page, marker_b, f"{marker_b}@e2e.local", "p4ssw0rd!Z")

    # Same base content, different unique markers for per-tenant searchability.
    base = "Shared template about project planning, budget forecasting, and quarterly review."
    m_a, doc_a = flows.ingest_and_wait(api_a, base + "\n{marker}")
    m_b, doc_b = flows.ingest_and_wait(api_b, base + "\n{marker}")

    # Different document IDs — no shared rows.
    assert doc_a["id"] != doc_b["id"], (
        f"identical content produced same doc id {doc_a['id']} across tenants"
    )

    # Tenant A finds their own doc by marker ...
    results_a = api_a.search(m_a)
    assert results_a.get("count", 0) >= 1, (
        f"tenant A should find own doc by {m_a!r}"
    )

    # ... but NOT Tenant B's doc.
    results_a_b = api_a.search(m_b)
    assert results_a_b.get("count", 0) == 0, (
        f"tenant A search for B's marker {m_b!r} leaked "
        f"{results_a_b.get('count')} hits"
    )

    # Tenant B finds their own doc by marker ...
    results_b = api_b.search(m_b)
    assert results_b.get("count", 0) >= 1, (
        f"tenant B should find own doc by {m_b!r}"
    )

    # ... but NOT Tenant A's doc.
    results_b_a = api_b.search(m_a)
    assert results_b_a.get("count", 0) == 0, (
        f"tenant B search for A's marker {m_a!r} leaked "
        f"{results_b_a.get('count')} hits"
    )

    # Cross-tenant direct document reads must be 404.
    r_ab = api_a._c.get(f"/api/documents/{doc_b['id']}")
    assert r_ab.status_code == 404, (
        f"tenant A reading tenant B's doc: expected 404, got {r_ab.status_code}"
    )
    r_ba = api_b._c.get(f"/api/documents/{doc_a['id']}")
    assert r_ba.status_code == 404, (
        f"tenant B reading tenant A's doc: expected 404, got {r_ba.status_code}"
    )

    api_a.close()
    api_b.close()
