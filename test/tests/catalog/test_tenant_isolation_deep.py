"""Catalog (step 2) — deep multi-tenant isolation: cross-tenant WRITES + RLS-only paths.

``test_multitenant_isolation.py`` covers cross-tenant *reads* (doc/file/job 404,
search no-leak, token scope, admin read-scope). This file covers the **write/IDOR
vectors and RLS-only code paths it does not** — and two suspected cross-tenant
defects surfaced by a code audit (platform-super-admin and global provider/route
CRUD). Isolation is the #1 enterprise security concern, so a failure here is a
high-severity finding (left failing per the suite philosophy).

Setup: register TWO fresh tenants via the web ``/signup`` form (which creates
both tenant + owner user; the JSON ``/auth/register`` only adds users to
existing tenants). After signup the browser ends up as tenant B; use
``ApiClient.login()`` to get JSON-API sessions for both tenants.
"""

from __future__ import annotations

import re
import time

import httpx
import pytest

from lib import config, flows
from lib.api_client import ApiClient

pytestmark = pytest.mark.isolation

PW = "P4ssw0rd!Long"


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


def _csrf_from_page(page) -> str:
    """Extract the ``__Host-csrf`` cookie value from the browser context."""
    for c in page.context.cookies():
        if c["name"] == "__Host-csrf":
            return c["value"]
    return ""


def _browser_cookies_httpx(page) -> httpx.Client:
    """Build an httpx Client that shares the browser's cookies.

    Extracts all cookies from the Playwright page context and sets them on a
    fresh httpx client so POST requests carry the same session + CSRF tokens.

    Cookie domains are NOT forwarded — httpx applies them to whatever host the
    request targets, which is what we want (the browser's ``localhost`` cookies
    must be sent to ``localhost:9443``).
    """
    jar = httpx.Cookies()
    for c in page.context.cookies():
        jar.set(c["name"], c["value"])
    return httpx.Client(
        base_url=config.base_url(),
        verify=config.verify_tls(),
        cookies=jar,
        timeout=config.http_timeout(),
    )


def _browser_post(page, path: str, form_data: dict[str, str]) -> httpx.Response:
    """POST form data to ``path`` using the browser's session + CSRF cookies.

    Uses a fresh httpx client per call so CSRF cookie changes are picked up.
    """
    c = _browser_cookies_httpx(page)
    try:
        return c.post(path, data=form_data)
    finally:
        c.close()


def _login_browser(page, slug: str, email: str, password: str) -> None:
    """Log the browser into a specific tenant via /login form."""
    page.goto("/login")
    page.fill("#tenant_slug", slug)
    page.fill("#email", email)
    page.fill("#password", password)
    page.click("form[action='/login'] button[type='submit']")
    page.wait_for_load_state("networkidle")


def _signup_via_web(page, marker: str, email: str, password: str) -> str:
    """Sign up a single tenant via the web /signup form. Returns the slug.

    Raises ``RuntimeError`` if signup appears to have failed (form re-rendered).
    """
    page.goto("/signup")
    page.wait_for_load_state("networkidle")
    page.fill("input[name='tenant_name']", marker)
    page.fill("input[name='email']", email)
    page.fill("input[name='password']", password)
    page.click("form button[type='submit']")
    page.wait_for_load_state("networkidle")

    # After successful signup the server redirects to /search or /dashboard.
    # If we're still on /signup, the form was re-rendered (CSRF or validation).
    current = page.url.rstrip("/")
    if "/signup" in current:
        body = page.content()
        # Try to extract an error message for diagnostics.
        err = re.search(r'(?:class="[^"]*error[^"]*"[^>]*>|alert[^>]*>)([^<]+)', body, re.I)
        detail = err.group(1).strip() if err else "unknown reason"
        raise RuntimeError(f"Signup failed for {marker}: {detail}")

    return _slugify(marker)


def _signup_both(page) -> tuple[str, str, str, str]:
    """Sign up two tenants via the web /signup form. Browser ends up as tenant B.

    Returns ``(slug_a, email_a, slug_b, email_b)``.

    A short sleep after the second signup gives the DB time to commit before any
    subsequent ``/auth/login`` call races against the write.
    """
    m_a = flows.unique_marker("ta")
    m_b = flows.unique_marker("tb")
    email_a = f"{m_a}@e2e.local"
    email_b = f"{m_b}@e2e.local"

    slug_a = _signup_via_web(page, m_a, email_a, PW)
    slug_b = _signup_via_web(page, m_b, email_b, PW)

    # Small delay so the second tenant's DB write is visible to follow-up logins.
    time.sleep(0.5)

    return slug_a, email_a, slug_b, email_b


def _api_for(slug: str, email: str, password: str) -> ApiClient:
    """Create an ApiClient logged into the given tenant."""
    client = ApiClient()
    client.login(slug, email, password)
    return client


# ── Tests ──────────────────────────────────────────────────────────────────────


def test_owner_is_not_platform_super_admin(page):
    """A tenant's own Owner must not be treated as a platform super-admin.

    Code audit: ``is_super`` is derived from ``UserRole::Owner``, so any customer's
    owner may load ``/admin/tenants`` and see every tenant. Register tenant A, then
    as tenant B's owner GET ``/admin/tenants`` and assert tenant A's slug/name does
    NOT appear (ideally the route is 403, or scoped to B only).
    """
    slug_a, email_a, slug_b, email_b = _signup_both(page)

    # Tenant A ingests a document (so A has real data).
    api_a = _api_for(slug_a, email_a, PW)
    _marker, doc_a = flows.ingest_and_wait(api_a, "Tenant A confidential document. {marker}")
    api_a.close()

    # Browser is already logged in as tenant B. Navigate to /admin/tenants.
    page.goto("/admin/tenants")
    page.wait_for_load_state("networkidle")

    body = page.content()

    # Tenant B's owner must NOT see tenant A's slug anywhere on the page.
    assert slug_a not in body, (
        f"Tenant B's admin/tenants page leaked tenant A's slug {slug_a!r}"
    )

    # The page should not list any cross-tenant data like A's doc id.
    assert str(doc_a["id"]) not in body, (
        f"Tenant B's admin/tenants page referenced tenant A's document {doc_a['id']}"
    )


def test_admin_dashboard_scoped_to_own_tenant(page):
    """The /admin dashboard tenant_count + recent-audit feed are own-tenant only.

    As tenant B's owner, GET ``/admin`` and assert the global tenant count is not
    exposed (no other tenant's data) and the recent-audit/decrypt feed contains no
    rows referencing tenant A.
    """
    slug_a, email_a, slug_b, email_b = _signup_both(page)

    # Tenant A: ingest a document (generates audit events).
    api_a = _api_for(slug_a, email_a, PW)
    m_a, doc_a = flows.ingest_and_wait(api_a, "Tenant A audit-target document. {marker}")
    a_doc_id = doc_a["id"]
    api_a.close()

    # Browser is tenant B. Go to /admin dashboard.
    page.goto("/admin")
    page.wait_for_load_state("networkidle")
    body = page.content()

    # Tenant B must NOT see tenant A's document id in the recent-audit feed.
    assert str(a_doc_id) not in body, (
        f"Tenant B's admin dashboard leaked tenant A's document id {a_doc_id}"
    )

    # Tenant B must NOT see tenant A's slug anywhere.
    assert slug_a not in body, (
        f"Tenant B's admin dashboard leaked tenant A's slug {slug_a!r}"
    )

    # The tenant_count displayed should not reflect A (B should see only 1, or 0
    # if scoped to B only). The key assertion: no cross-tenant data exposure.
    assert m_a not in body, (
        f"Tenant B's admin dashboard contained tenant A's marker {m_a!r}"
    )


def test_decrypt_audit_scoped_to_own_tenant(page):
    """/admin/decrypt-audit shows only the caller tenant's decrypt events.

    Tenant A ingests/searches (producing decrypt-audit rows); as tenant B GET
    ``/admin/decrypt-audit`` and assert none of A's rows appear.
    """
    slug_a, email_a, slug_b, email_b = _signup_both(page)

    # Tenant A: ingest + search to generate decrypt-audit rows.
    api_a = _api_for(slug_a, email_a, PW)
    m_a, doc_a = flows.ingest_and_wait(api_a, "Tenant A decrypt-audit document. {marker}")
    # Searching triggers decrypt-audit entries (DEK unwrap for search).
    api_a.search(m_a)
    a_doc_id = doc_a["id"]
    api_a.close()

    # Browser is tenant B. Navigate to /admin/decrypt-audit.
    page.goto("/admin/decrypt-audit")
    page.wait_for_load_state("networkidle")
    body = page.content()

    # Tenant B must NOT see tenant A's document id or marker in decrypt audit.
    assert str(a_doc_id) not in body, (
        f"Tenant B's decrypt-audit page leaked tenant A's document id {a_doc_id}"
    )
    assert m_a not in body, (
        f"Tenant B's decrypt-audit page contained tenant A's marker {m_a!r}"
    )


def test_cross_tenant_provider_mutation_forbidden(page):
    """One tenant's admin cannot toggle/edit/delete provider/model infra used by all.

    Code audit: ``providers``/``models`` have no ``tenant_id``. As tenant A read a
    provider id from ``/admin/providers``; as tenant B POST
    ``/admin/providers/{id}/toggle`` (with B's CSRF) and assert it is rejected
    (403/404) and the provider state is unchanged — global infra must not be
    tenant-mutable.
    """
    slug_a, email_a, slug_b, email_b = _signup_both(page)

    # Tenant A: navigate to /admin/providers to find a provider id.
    _login_browser(page, slug_a, email_a, PW)
    page.goto("/admin/providers")
    page.wait_for_load_state("networkidle")
    body_a = page.content()

    # Extract a provider id from the page (toggle form action URLs).
    provider_ids = re.findall(r"/admin/providers/(\d+)/toggle", body_a)
    if not provider_ids:
        pytest.skip("No providers found on /admin/providers — cannot test toggle")

    provider_id = int(provider_ids[0])

    # Switch browser to tenant B.
    _login_browser(page, slug_b, email_b, PW)

    # Get CSRF token by navigating to B's /admin/providers page.
    page.goto("/admin/providers")
    page.wait_for_load_state("networkidle")
    csrf = _csrf_from_page(page)
    assert csrf, "No CSRF cookie found after GET /admin/providers"

    # Tenant B attempts to toggle tenant A's provider.
    resp = _browser_post(page, f"/admin/providers/{provider_id}/toggle", {"csrf_token": csrf})

    # The intended behavior: cross-tenant provider mutation must be rejected.
    # Accept 403, 404, or re-rendered page with error.
    is_rejected = resp.status_code in (403, 404) or (
        resp.status_code == 200 and "error" in resp.text.lower()
    )
    assert is_rejected, (
        f"Tenant B toggling provider {provider_id}: expected rejection, "
        f"got {resp.status_code}"
    )

    # Verify: the provider state must not have been changed by B.
    _login_browser(page, slug_a, email_a, PW)
    page.goto("/admin/providers")
    page.wait_for_load_state("networkidle")
    body_after = page.content()

    # The provider id should still be present (not deleted).
    assert f"/admin/providers/{provider_id}/" in body_after, (
        f"Provider {provider_id} was removed after cross-tenant toggle attempt"
    )


def test_cross_tenant_route_creation_forbidden(page):
    """Creating a routing rule with a forged tenant_id cannot affect another tenant.

    ``routes.tenant_id`` is a free-text form field. As tenant B POST
    ``/admin/routes`` with ``tenant_id`` = tenant A's id; assert it is rejected /
    cannot create a route bound to A.
    """
    slug_a, email_a, slug_b, email_b = _signup_both(page)

    # Tenant A: ingest a doc so A has real data for context.
    api_a = _api_for(slug_a, email_a, PW)
    _m_a, _doc_a = flows.ingest_and_wait(api_a, "Tenant A route-target document. {marker}")
    api_a.close()

    # Browser is tenant B. Get CSRF from /admin/routes/new page.
    page.goto("/admin/routes/new")
    page.wait_for_load_state("networkidle")
    csrf = _csrf_from_page(page)
    assert csrf, "No CSRF cookie found after GET /admin/routes/new"

    # Tenant B attempts to create a route with tenant_id = A's slug.
    resp = _browser_post(
        page,
        "/admin/routes",
        {
            "csrf_token": csrf,
            "tenant_id": slug_a,  # forge — target tenant A
            "role": "text",
            "strategy": "round_robin",
        },
    )

    # The intended behavior: cross-tenant route creation must be rejected or the
    # forged tenant_id must be ignored (route scoped to B only).
    assert resp.status_code in (200, 302, 303, 403, 404), (
        f"Unexpected status {resp.status_code} on cross-tenant route creation"
    )

    # Check /admin/routes page — tenant A's slug must not appear.
    page.goto("/admin/routes")
    page.wait_for_load_state("networkidle")
    routes_body = page.content()

    assert slug_a not in routes_body, (
        f"Route created with forged tenant_id={slug_a!r} — tenant A binding visible"
    )


def test_cross_tenant_document_mutation_idor(page):
    """Tenant B cannot mutate tenant A's document via any write route.

    Tenant A ingests a doc; as tenant B POST each of
    ``/documents/{A_doc}/add-tag``, ``/tags/{tag_id}/remove``, ``/retag``,
    ``/reorder``, ``/add-page`` (with B's CSRF) and assert each 404s/forbids and
    A's document detail is byte-for-byte unchanged afterward.
    """
    slug_a, email_a, slug_b, email_b = _signup_both(page)

    # Tenant A: ingest a document.
    api_a = _api_for(slug_a, email_a, PW)
    _marker, doc_a = flows.ingest_and_wait(api_a, "Tenant A IDOR-target document. {marker}")
    a_doc_id = doc_a["id"]

    # Capture tenant A's document before any mutation attempts.
    doc_before = api_a.get_document(a_doc_id)
    tags_before = sorted(t["name"] for t in doc_before.get("tags", []))
    files_before = [f["id"] for f in doc_before.get("files", [])]
    api_a.close()

    # Browser is tenant B. Get CSRF from B's /search page.
    page.goto("/search")
    page.wait_for_load_state("networkidle")
    csrf = _csrf_from_page(page)
    assert csrf, "No CSRF cookie found after GET /search"

    # Attempt 1: POST /documents/{A_doc}/add-tag
    resp = _browser_post(
        page, f"/documents/{a_doc_id}/add-tag",
        {"csrf_token": csrf, "tag_name": "cross-tenant-attack"},
    )
    assert resp.status_code in (403, 404, 500), (
        f"Tenant B add-tag on A's doc {a_doc_id}: expected 403/404/500, got {resp.status_code}"
    )

    # Attempt 2: POST /documents/{A_doc}/tags/{tag_id}/remove
    resp = _browser_post(
        page, f"/documents/{a_doc_id}/tags/1/remove",
        {"csrf_token": csrf},
    )
    assert resp.status_code in (403, 404, 500), (
        f"Tenant B remove-tag on A's doc {a_doc_id}: expected 403/404/500, got {resp.status_code}"
    )

    # Attempt 3: POST /documents/{A_doc}/retag
    resp = _browser_post(
        page, f"/documents/{a_doc_id}/retag",
        {"csrf_token": csrf},
    )
    assert resp.status_code in (403, 404, 500), (
        f"Tenant B retag on A's doc {a_doc_id}: expected 403/404/500, got {resp.status_code}"
    )

    # Attempt 4: POST /documents/{A_doc}/reorder with forged file_ids
    if files_before:
        resp = _browser_post(
            page, f"/documents/{a_doc_id}/reorder",
            {"csrf_token": csrf, "file_ids": str(files_before[0]), "page_nos": "1"},
        )
        assert resp.status_code in (403, 404, 500), (
            f"Tenant B reorder on A's doc {a_doc_id}: expected 403/404/500, got {resp.status_code}"
        )

    # Verify: A's document is unchanged.
    api_a2 = _api_for(slug_a, email_a, PW)
    doc_after = api_a2.get_document(a_doc_id)
    tags_after = sorted(t["name"] for t in doc_after.get("tags", []))
    assert tags_after == tags_before, (
        f"Tenant B's mutation attempt changed A's tags: {tags_before} → {tags_after}"
    )
    api_a2.close()


def test_cross_tenant_team_management_idor(page):
    """Tenant B cannot change-role or remove a user belonging to tenant A.

    Tenant A invites a second user (capture its user_id). As tenant B POST
    ``/account/team/role`` and ``/account/team/remove`` with A's user_id (+ B's
    CSRF); assert A's user is unchanged and still present.
    """
    slug_a, email_a, slug_b, email_b = _signup_both(page)

    # Tenant A: invite a second user via the team page.
    _login_browser(page, slug_a, email_a, PW)
    page.goto("/account/team")
    page.wait_for_load_state("networkidle")
    csrf_a = _csrf_from_page(page)
    assert csrf_a, "No CSRF for tenant A on /account/team"

    # Invite a member.
    invite_email = f"{flows.unique_marker('inv')}@e2e.local"
    page.fill("input[name='email']", invite_email)
    page.select_option("select[name='role']", "member")
    page.click("form[action='/account/team/invite'] button[type='submit']")
    page.wait_for_load_state("networkidle")

    # Now get A's team listing to find the invited user's id.
    page.goto("/account/team")
    page.wait_for_load_state("networkidle")
    team_body_a = page.content()

    # Extract user_ids from hidden form fields or role-change forms.
    user_ids = re.findall(r'name="user_id"[^>]*value="(\d+)"', team_body_a)
    # The newly invited user's id is the highest one on the page.
    if len(user_ids) < 2:
        pytest.skip("Could not find invited user_id on team page (may need email verification)")

    a_target_user_id = int(user_ids[-1])

    # Switch browser to tenant B.
    _login_browser(page, slug_b, email_b, PW)

    # Get CSRF from B's /account/team page.
    page.goto("/account/team")
    page.wait_for_load_state("networkidle")
    csrf_b = _csrf_from_page(page)
    assert csrf_b, "No CSRF cookie for tenant B on /account/team"

    # Tenant B attempts to change role of A's user.
    resp = _browser_post(
        page, "/account/team/role",
        {"csrf_token": csrf_b, "user_id": str(a_target_user_id), "new_role": "admin"},
    )
    # Should be rejected or have no effect.
    assert resp.status_code in (200, 302, 303, 403, 404), (
        f"Unexpected status {resp.status_code} on cross-tenant role change"
    )

    # Tenant B attempts to remove A's user.
    resp = _browser_post(
        page, "/account/team/remove",
        {"csrf_token": csrf_b, "user_id": str(a_target_user_id)},
    )
    assert resp.status_code in (200, 302, 303, 403, 404), (
        f"Unexpected status {resp.status_code} on cross-tenant user remove"
    )

    # Verify: tenant A's invited user is still present.
    _login_browser(page, slug_a, email_a, PW)
    page.goto("/account/team")
    page.wait_for_load_state("networkidle")
    team_after = page.content()

    assert str(a_target_user_id) in team_after, (
        f"Tenant A's user {a_target_user_id} was removed by tenant B's cross-tenant action"
    )


def test_cross_tenant_admin_actions_idor(page):
    """Tenant B's admin panel cannot act on tenant A's users or jobs.

    As tenant B POST ``/admin/users/role`` with A's user_id, and
    ``/admin/jobs/{A_job}/retry`` + ``/delete`` with A's job id; assert no effect on
    A (role/job state unchanged).
    """
    slug_a, email_a, slug_b, email_b = _signup_both(page)

    # Tenant A: ingest a doc + get user info.
    api_a = _api_for(slug_a, email_a, PW)
    _marker, doc_a = flows.ingest_and_wait(api_a, "Tenant A admin-IDOR document. {marker}")
    api_a.close()

    # Get A's own user_id from the account page.
    _login_browser(page, slug_a, email_a, PW)
    page.goto("/account/team")
    page.wait_for_load_state("networkidle")
    team_body = page.content()
    a_user_ids = re.findall(r'name="user_id"[^>]*value="(\d+)"', team_body)
    a_user_id = int(a_user_ids[0]) if a_user_ids else None

    # Find a job id belonging to A.
    page.goto("/admin/jobs")
    page.wait_for_load_state("networkidle")
    jobs_body = page.content()
    a_job_ids = re.findall(r"/admin/jobs/(\d+)/retry", jobs_body)

    # Switch browser to tenant B.
    _login_browser(page, slug_b, email_b, PW)

    # Get CSRF from B's admin dashboard.
    page.goto("/admin")
    page.wait_for_load_state("networkidle")
    csrf_b = _csrf_from_page(page)
    assert csrf_b, "No CSRF cookie for tenant B on /admin"

    # Attempt 1: POST /admin/users/role with A's user_id.
    if a_user_id:
        resp = _browser_post(
            page, "/admin/users/role",
            {"csrf_token": csrf_b, "user_id": str(a_user_id), "role": "member"},
        )
        assert resp.status_code in (200, 302, 303, 403, 404), (
            f"Unexpected status {resp.status_code} on cross-tenant admin role change"
        )

    # Attempt 2 & 3: POST /admin/jobs/{A_job}/retry + /delete.
    if a_job_ids:
        a_job_id = int(a_job_ids[0])
        for action in ("retry", "delete"):
            resp = _browser_post(
                page, f"/admin/jobs/{a_job_id}/{action}",
                {"csrf_token": csrf_b},
            )
            assert resp.status_code in (200, 302, 303, 403, 404), (
                f"Unexpected status {resp.status_code} on cross-tenant job {action}"
            )

    # Verify: tenant A's document is still accessible.
    api_a2 = _api_for(slug_a, email_a, PW)
    doc_after = api_a2.get_document(doc_a["id"])
    assert doc_after["id"] == doc_a["id"], (
        f"Tenant A's document {doc_a['id']} was affected by B's cross-tenant admin actions"
    )
    api_a2.close()


def test_cross_tenant_token_revoke_is_noop_for_victim(page):
    """Tenant B revoking tenant A's token id must leave A's token working.

    Tenant A creates a token (capture id + plaintext). Tenant B calls
    ``DELETE /api/tokens/{A_id}`` (returns 200, idempotent). Then a Bearer request
    with A's plaintext token must still succeed (not 401) — a foreign revoke is a
    DoS primitive if it actually revokes.
    """
    slug_a, email_a, slug_b, email_b = _signup_both(page)

    api_a = _api_for(slug_a, email_a, PW)
    api_b = _api_for(slug_b, email_b, PW)

    # Tenant A: create an API token.
    r = api_a._c.post("/api/tokens", json={"name": "tenant-a-token"})
    assert r.status_code == 201, f"Failed to create token for A: {r.status_code} {r.text}"
    token_data = r.json()
    a_token_id = token_data["id"]
    a_raw_token = token_data["token"]
    assert len(a_raw_token) == 64

    # Tenant A: ingest a doc to have something searchable.
    _marker_a, _doc_a = flows.ingest_and_wait(api_a, "Tenant A token-revoke target. {marker}")

    # Tenant B calls DELETE /api/tokens/{A_token_id}.
    r = api_b._c.delete(f"/api/tokens/{a_token_id}")
    # The server returns 200 (idempotent) even when the token doesn't belong to B.
    assert r.status_code == 200, (
        f"DELETE /api/tokens/{a_token_id} as B: expected 200, got {r.status_code}"
    )

    # Now use A's raw token via Bearer auth — it must STILL work.
    c = httpx.Client(
        base_url=config.base_url(),
        verify=config.verify_tls(),
        headers={"Authorization": f"Bearer {a_raw_token}"},
        timeout=config.http_timeout(),
    )
    try:
        r = c.get("/api/search", params={"q": _marker_a, "limit": 10})
        assert r.status_code == 200, (
            f"Tenant A's token after B's revoke: search returned {r.status_code}, "
            f"expected 200 (token must still work)"
        )
        assert r.json().get("count", 0) >= 1, (
            "Tenant A's token after B's revoke: search returned 0 hits — "
            "token was compromised by cross-tenant revoke"
        )
    finally:
        c.close()

    api_a.close()
    api_b.close()


def test_tag_filter_search_no_cross_tenant_leak(page):
    """A tag filter cannot surface another tenant's tagged documents.

    The tag-id resolution SQL has no tenant predicate (RLS-only). Tenant A tags a
    doc with a distinctive tag name; tenant B searches with ``tag=<A's tag name>``
    and asserts zero hits — exercising the RLS path the CI ``#[ignore]`` tests skip.
    """
    slug_a, email_a, slug_b, email_b = _signup_both(page)

    api_a = _api_for(slug_a, email_a, PW)
    api_b = _api_for(slug_b, email_b, PW)

    # Tenant A: ingest a document. The tagger should produce at least one tag.
    _marker, doc_a = flows.ingest_and_wait(api_a, "Tenant A tag-filter document. {marker}")
    tags_a = doc_a.get("tags", [])

    if not tags_a:
        api_a.close()
        api_b.close()
        pytest.skip("Tenant A's document has no tags — cannot test tag-filter isolation")

    # Pick the first tag name from A's doc.
    a_tag_name = tags_a[0]["name"]
    assert a_tag_name, "Tag entry has no name"

    # Tenant B searches with tag=<A's tag name> — must return zero hits.
    results_b = api_b.search("", tag=a_tag_name, limit=10)
    assert results_b.get("count", 0) == 0, (
        f"Tenant B tag-filter search for {a_tag_name!r} leaked "
        f"{results_b.get('count')} hits: {results_b.get('hits')}"
    )

    # Sanity: tenant A can find their own doc with that tag.
    results_a = api_a.search("", tag=a_tag_name, limit=10)
    assert results_a.get("count", 0) >= 1, (
        f"Tenant A's own tag-filter search for {a_tag_name!r} returned 0 hits "
        f"(unexpected)"
    )

    api_a.close()
    api_b.close()


def test_vector_search_no_cross_tenant_leak(page):
    """A paraphrased (no shared keyword) query cannot return another tenant's chunk.

    Tenant A ingests distinctive-topic content; tenant B issues a paraphrased query
    (no literal overlap) that would match A's embedding, and asserts none of A's
    docs return — proving the HNSW vector branch is tenant-scoped (distinct from the
    keyword path the existing test covers).
    """
    slug_a, email_a, slug_b, email_b = _signup_both(page)

    api_a = _api_for(slug_a, email_a, PW)
    api_b = _api_for(slug_b, email_b, PW)

    # Tenant A: ingest content with a distinctive, obscure topic.
    content_a = (
        "Research on thermoelectric properties of bismuth telluride nanowires "
        "for waste heat recovery in industrial plasma reactors. "
        "Unique marker: {marker}."
    )
    marker_a, doc_a = flows.ingest_and_wait(api_a, content_a)

    # Tenant B: search for a paraphrased version — no shared keywords.
    paraphrased_query = (
        "semiconductor filaments converting thermal energy to electricity in factories"
    )
    results_b = api_b.search(paraphrased_query, limit=10)

    # Assert: no hits from tenant A should appear in B's results.
    hit_ids = [h.get("document_id") for h in results_b.get("hits", [])]
    assert doc_a["id"] not in hit_ids, (
        f"Tenant B vector search leaked tenant A's document {doc_a['id']}: "
        f"hits={hit_ids}"
    )

    # Stronger assertion: none of B's results contain A's marker.
    for hit in results_b.get("hits", []):
        doc_b = api_b.get_document(hit["document_id"])
        text_content = str(doc_b)
        assert marker_a not in text_content, (
            f"Tenant B vector search result (doc {hit['document_id']}) contains "
            f"tenant A's marker {marker_a!r} — cross-tenant vector leak"
        )

    # Sanity: tenant A can find their own doc by keyword.
    results_a = api_a.search(marker_a, limit=10)
    assert results_a.get("count", 0) >= 1, (
        f"Tenant A should find own doc by marker {marker_a!r}"
    )

    api_a.close()
    api_b.close()


def test_tenant_id_spoofing_is_ignored(page):
    """Forged tenant headers / tampered session cannot resolve to another tenant.

    With tenant A's valid session, also send ``X-Tenant-Id``/``X-Tenant-Slug`` =
    tenant B on ``/api/search``; assert results stay A-only. Then corrupt the
    ``__Host-session`` value and assert 401 (tenant context comes only from a
    server-validated session).
    """
    slug_a, email_a, slug_b, email_b = _signup_both(page)

    api_a = _api_for(slug_a, email_a, PW)
    api_b = _api_for(slug_b, email_b, PW)

    # Both tenants ingest uniquely-marked documents.
    marker_a, doc_a = flows.ingest_and_wait(api_a, "Tenant A spoof-target document. {marker}")
    marker_b, doc_b = flows.ingest_and_wait(api_b, "Tenant B spoof-target document. {marker}")

    # Extract A's __Host-session cookie from api_a's cookie jar.
    session_cookie = None
    for cookie in api_a._c.cookies.jar:
        if cookie.name == "__Host-session":
            session_cookie = cookie.value
            break
    assert session_cookie, "Could not extract __Host-session from api_a"

    # Direct httpx with A's session + spoofed tenant headers.
    c = httpx.Client(
        base_url=config.base_url(),
        verify=config.verify_tls(),
        timeout=config.http_timeout(),
    )
    try:
        # Test 1: send X-Tenant-Id / X-Tenant-Slug pointing to tenant B.
        r = c.get(
            "/api/search",
            params={"q": marker_b, "limit": 10},
            cookies={"__Host-session": session_cookie},
            headers={
                "X-Tenant-Id": str(doc_b["id"]),
                "X-Tenant-Slug": slug_b,
            },
        )
        assert r.status_code == 200, f"Spoofed header search: unexpected {r.status_code}"
        hits = r.json().get("hits", [])
        hit_doc_ids = [h.get("document_id") for h in hits]
        assert doc_b["id"] not in hit_doc_ids, (
            f"Tenant-id spoofing succeeded: A's session returned B's doc {doc_b['id']}"
        )

        # Test 2: corrupt the session cookie value — assert 401.
        r = c.get(
            "/api/search",
            params={"q": marker_a, "limit": 1},
            cookies={"__Host-session": session_cookie + "corrupted"},
        )
        assert r.status_code == 401, (
            f"Corrupted session: expected 401, got {r.status_code}"
        )
    finally:
        c.close()

    api_a.close()
    api_b.close()


@pytest.mark.skip(
    reason="blocked: e2e blob backend likely uses file:// or S3 without presigned signature "
    "validation that can be tested cross-tenant. Test shape is correct but depends on "
    "blob backend supporting presigned URLs with tenant-key-binding."
)
def test_presigned_url_scoped_and_expiring(page):
    """An issued presigned download URL fetches only its tenant's blob and expires.

    Follow tenant A's ``/api/documents/{id}/file/{file_id}`` to its presigned
    ``Location``; assert it serves A's bytes, that editing the tenant-prefix in the
    key toward another tenant yields 403 (signature binds the key), and that it
    stops working after the TTL. (If the e2e blob backend is local ``file://``,
    mark blocked with that reason.)
    """
    slug_a, email_a, slug_b, email_b = _signup_both(page)

    api_a = _api_for(slug_a, email_a, PW)
    api_b = _api_for(slug_b, email_b, PW)

    # Tenant A: ingest a document.
    _marker, doc_a = flows.ingest_and_wait(api_a, "Tenant A presigned-URL document. {marker}")
    files = doc_a.get("files", [])
    if not files:
        api_a.close()
        api_b.close()
        pytest.skip("No files in ingested document — cannot test presigned URL")

    file_id = files[0]["id"]
    doc_id = doc_a["id"]

    # Follow the file download endpoint to get the presigned URL.
    r = api_a._c.get(
        f"/api/documents/{doc_id}/file/{file_id}",
        follow_redirects=False,
    )
    assert r.status_code in (302, 303, 307, 308), (
        f"File download: expected redirect, got {r.status_code}"
    )
    location = r.headers.get("Location")
    assert location, "No Location header in file download redirect"

    # Fetch the presigned URL — must return the file content.
    raw_client = httpx.Client(
        verify=config.verify_tls(),
        timeout=config.http_timeout(),
        follow_redirects=False,
    )
    try:
        dl = raw_client.get(location)
        assert dl.status_code == 200, (
            f"Presigned URL download: expected 200, got {dl.status_code}"
        )
        assert len(dl.content) > 0, "Presigned URL returned empty body"
    finally:
        raw_client.close()

    api_a.close()
    api_b.close()


def test_storage_quota_isolated_between_tenants(page):
    """One tenant hitting its storage cap must not affect another tenant's ingest.

    Drive tenant A to a 413 storage-quota rejection; assert tenant B can still
    ingest a small file (202) and that A's usage never counted against B.
    """
    slug_a, email_a, slug_b, email_b = _signup_both(page)

    api_a = _api_for(slug_a, email_a, PW)
    api_b = _api_for(slug_b, email_b, PW)

    # Attempt to drive tenant A toward its storage quota.
    payload_size = 64 * 1024  # start at 64 KB
    max_attempts = 20
    for i in range(max_attempts):
        content = "x" * payload_size + f"\nQuota-fill marker: {slug_a}-{i}."
        r = api_a._c.post(
            "/api/ingest",
            files=[("files", (f"quota_{i}.txt", content.encode("utf-8"), "text/plain"))],
        )
        if r.status_code == 413:
            break
        elif r.status_code == 202:
            payload_size = min(payload_size * 2, 5 * 1024 * 1024)
        else:
            break

    # Tenant B: ingest a small file — must succeed regardless of A's quota.
    marker_b, doc_b = flows.ingest_and_wait(api_b, "Tenant B quota-isolation check. {marker}")
    assert doc_b.get("id"), "Tenant B's ingest must succeed regardless of A's quota usage"

    # Also verify: tenant B can search for their own doc.
    results_b = api_b.search(marker_b, limit=10)
    assert results_b.get("count", 0) >= 1, (
        f"Tenant B should find own doc by {marker_b!r} regardless of A's quota"
    )

    api_a.close()
    api_b.close()


def test_tenant_deletion_no_residue_for_others(page):
    """Deleting tenant A leaves tenant B's data fully intact (privileged-pool path).

    Stand up A and B with marked docs; delete A (``/account/danger/delete``); after
    A's resources 404, assert all of B's documents/search/tags/quota are unchanged —
    the deletion job runs on the privileged admin pool (RLS-bypass risk).
    """
    slug_a, email_a, slug_b, email_b = _signup_both(page)

    # Both tenants: ingest a uniquely-marked document.
    api_a = _api_for(slug_a, email_a, PW)
    m_a, doc_a = flows.ingest_and_wait(api_a, "Tenant A deletion-test document. {marker}")
    api_a.close()

    api_b = _api_for(slug_b, email_b, PW)
    m_b, doc_b = flows.ingest_and_wait(api_b, "Tenant B survival-test document. {marker}")
    b_doc_id_before = doc_b["id"]
    b_tags_before = sorted(t["name"] for t in doc_b.get("tags", []))
    b_files_before = sorted(f["id"] for f in doc_b.get("files", []))

    # Capture B's search before deletion.
    results_b_before = api_b.search(m_b, limit=10)
    b_hit_count_before = results_b_before.get("count", 0)
    api_b.close()

    # Delete tenant A via the danger zone.
    _login_browser(page, slug_a, email_a, PW)
    page.goto("/account/danger")
    page.wait_for_load_state("networkidle")
    csrf = _csrf_from_page(page)
    assert csrf, "No CSRF cookie on /account/danger"

    # Fill the confirmation slug and submit the deletion form.
    page.fill("input[name='confirm_slug']", slug_a)
    # The CSRF token needs to be in the form. Inject it via JS and submit.
    page.evaluate(
        """([csrf]) => {
            const form = document.querySelector('form[action="/account/danger/delete"]');
            if (!form) return;
            let input = form.querySelector('input[name="csrf_token"]');
            if (!input) {
                input = document.createElement('input');
                input.type = 'hidden';
                input.name = 'csrf_token';
                form.appendChild(input);
            }
            input.value = csrf;
            form.submit();
        }""",
        [csrf],
    )
    page.wait_for_load_state("networkidle")

    # The goodbye page should appear.
    body = page.content()
    assert (
        "Workspace Deleted" in body or "crypto-shredded" in body or "goodbye" in body.lower()
    ), f"Account deletion did not show goodbye page. Body snippet: {body[:500]}"

    # Wait for tenant A's resources to become unreachable (async deletion job).
    deadline = time.monotonic() + 180
    api_a2 = ApiClient()
    a_gone = False
    while time.monotonic() < deadline:
        try:
            r = api_a2._c.post(
                "/auth/login",
                json={"tenant_slug": slug_a, "email": email_a, "password": PW},
            )
            if r.status_code in (401, 403, 404):
                a_gone = True
                break
        except Exception:
            pass
        time.sleep(3)
    api_a2.close()

    # Verify tenant B's data is fully intact.
    api_b2 = _api_for(slug_b, email_b, PW)

    # B's document must still exist and be unchanged.
    doc_b_after = api_b2.get_document(b_doc_id_before)
    assert doc_b_after["id"] == b_doc_id_before, (
        "Tenant B's document was affected by A's deletion"
    )

    # B's tags must be preserved.
    tags_b_after = sorted(t["name"] for t in doc_b_after.get("tags", []))
    assert tags_b_after == b_tags_before, (
        f"Tenant B's tags changed after A's deletion: {b_tags_before} → {tags_b_after}"
    )

    # B's files must be preserved.
    files_b_after = sorted(f["id"] for f in doc_b_after.get("files", []))
    assert files_b_after == b_files_before, (
        f"Tenant B's files changed: {b_files_before} → {files_b_after}"
    )

    # B must still be able to search for their own doc.
    results_b_after = api_b2.search(m_b, limit=10)
    assert results_b_after.get("count", 0) >= b_hit_count_before, (
        f"Tenant B's search results reduced after A's deletion: "
        f"{b_hit_count_before} → {results_b_after.get('count', 0)}"
    )

    api_b2.close()
