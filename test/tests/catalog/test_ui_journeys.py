"""Catalog (step 2) — deep browser user-journeys (Playwright).

These tests drive the **rendered web UI** the way a real user does — clicking
through navigation, search results, and document pages — and assert the
*intended* behaviour of each journey. They deliberately cover ground the other
browser tests do **not**:

* the smoke + ``test_search`` browser tests only check that result *links*
  appear — here we **click through** to the document detail page;
* ``test_ingest`` covers the upload journey, ``test_tagging`` covers *adding* a
  tag and *re-tagging* through the UI — so this file covers the **complementary**
  journeys (tag **removal**, the empty-search state, the in-UI **kind filter**,
  the authenticated **navigation**, the web **logout** redirect, and the public
  **marketing** pages), none of which are exercised elsewhere.

Per the suite philosophy a failing assertion means an app bug and is left
failing — never weakened. Web routes are defined in ``crates/api/src/web/mod.rs``
and the Askama templates in ``crates/api/templates/``; read those for exact
selectors and form field names.
"""

from __future__ import annotations

import re

import pytest
from playwright.sync_api import expect

from lib import config, flows
from lib.api_client import ApiClient

pytestmark = pytest.mark.ui

# A document with clear, taggable content; ``{marker}`` is filled by the helpers.
SAMPLE = (
    "Quarterly financial report for Acme Corp covering revenue, invoices, and "
    "budget forecasts for Q3. Unique end-to-end marker: {marker}."
)

# Browser waits that hit the real retrieval pipeline (query embedding + rerank)
# need a generous budget, matching the smoke test.
SEARCH_TIMEOUT_MS = 45_000


def _admin_creds() -> tuple[str, str, str]:
    """(tenant_slug, email, password) for the bootstrap admin user."""
    return config.tenant_slug(), config.admin_email(), config.admin_password()


def _ingest_marked_doc(content: str = SAMPLE) -> tuple[str, dict]:
    """Ingest a uniquely-marked document via a throwaway admin API client.

    Returns ``(marker, doc)`` where ``doc`` is the parsed
    ``GET /api/documents/:id`` body. The pipeline runs synchronously inside
    ``POST /api/ingest`` (extract → tag → embed → store), so the document is
    fully processed on return.
    """
    client = ApiClient()
    try:
        client.login(*_admin_creds())
        return flows.ingest_and_wait(client, content)
    finally:
        client.close()


def test_protected_page_denies_unauthenticated_access(page):
    """An unauthenticated load of a protected app page is denied (401), no content.

    The app's auth contract for protected routes is to reject unauthenticated
    requests with 401 — the app's own unit tests assert this (e.g.
    ``admin_unauthenticated_returns_401``) — rather than serve the page or
    redirect to a login form. Loading ``/dashboard`` with no session must
    therefore be rejected with HTTP 401 and must NOT expose the authenticated
    dashboard (no app shell / nav, no data).
    """
    # No session in this fresh browser context.
    resp = page.goto("/dashboard")

    # Intended security contract: unauthenticated access is rejected (401), not served.
    assert resp is not None and resp.status == 401, (
        f"unauthenticated /dashboard must be denied with 401; "
        f"got {resp.status if resp else 'no response'}"
    )
    # The protected app shell must not be exposed to an anonymous visitor.
    assert page.locator("nav a[href='/account']").count() == 0, (
        "unauthenticated /dashboard leaked the authenticated app shell/nav"
    )


def test_authenticated_nav_reaches_core_pages(page):
    """The shared authenticated nav reaches Search, Upload, Dashboard, Account.

    After logging in, the app shell's navigation (a shared Askama macro) must let
    a user reach each core page. Click/visit the nav links and assert each of
    ``/search``, ``/upload``, ``/dashboard``, ``/account`` loads (HTTP 200, no
    redirect back to /login) and renders its own page, proving the nav is wired
    and the session carries across navigation.
    """
    flows.browser_login(page, *_admin_creds())

    core_pages = [("Dashboard", "/dashboard"), ("Search", "/search"),
                  ("Upload", "/upload"), ("Account", "/account")]

    # 1. The shared nav (rendered on the post-login page) exposes a link to
    #    every core page.
    for _label, path in core_pages:
        assert page.locator(f"nav a[href='{path}']").count() >= 1, (
            f"authenticated nav is missing a link to {path}"
        )

    # 2. Each core page loads (200, its own content) with the session carrying
    #    across navigation — never bouncing back to the login form.
    for _label, path in core_pages:
        resp = page.goto(path)
        assert resp is not None and resp.status == 200, (
            f"GET {path} did not return 200 for an authenticated user"
        )
        expect(page).to_have_url(re.compile(re.escape(path) + r"(?:[?#].*)?$"))
        # The shared nav highlights the active page (aria-current) — proves the
        # page rendered its own authenticated shell, not a redirect/login page.
        expect(page.locator(f"nav a[href='{path}'][aria-current='page']")).to_be_visible()
        assert page.locator("#password").count() == 0, (
            f"GET {path} fell back to the login form"
        )


def test_search_result_click_opens_document_detail(api, page):
    """Clicking a search result link navigates to that document's detail page.

    Ingest a uniquely-marked doc (use a throwaway ``ApiClient`` for the ingest),
    search for the marker in the browser, then **click** the first result link
    (``#results a[href*='/documents/']``). The browser must navigate to
    ``/documents/{id}`` for the ingested document and that detail page must
    render (its URL ends in the document id and the page shows the document).
    """
    tenant, email, password = _admin_creds()
    marker, doc = _ingest_marked_doc()
    doc_id = doc["id"]

    flows.browser_login(page, tenant, email, password)
    page.goto("/search")
    page.fill("input[name='q']", marker)
    page.press("input[name='q']", "Enter")

    # Results swap into #results via HTMX; wait for the ingested doc's result.
    result_link = page.locator(f"#results a[href='/documents/{doc_id}']").first
    expect(result_link).to_be_visible(timeout=SEARCH_TIMEOUT_MS)

    # Clicking the result navigates to that document's detail page.
    result_link.click()
    expect(page).to_have_url(re.compile(rf"/documents/{doc_id}(?:[?#].*)?$"))
    # The detail page rendered its own shell (nav + the Tags section).
    expect(page.locator("nav")).to_be_visible()
    expect(page.get_by_role("heading", name="Tags")).to_be_visible()


def test_document_detail_page_renders_tags_summary_files(api, page):
    """The rendered document-detail page shows tags, a summary, and the file list.

    The other detail coverage is API-level (``GET /api/documents/{id}``); here we
    assert the **HTML page** at ``/documents/{id}`` surfaces, for a freshly
    ingested doc: at least one tag, the (non-empty) summary text, and the
    document's file with a working download link. Behavioural assertion — not the
    exact AI text.
    """
    tenant, email, password = _admin_creds()
    api.login(tenant, email, password)
    marker, doc = flows.ingest_and_wait(api, SAMPLE)
    doc_id = doc["id"]

    flows.browser_login(page, tenant, email, password)
    page.goto(f"/documents/{doc_id}")

    # 1. At least one tag chip (each rendered tag carries a per-tag remove form).
    remove_forms = page.locator(
        f"form[action^='/documents/{doc_id}/tags/'][action$='/remove']"
    )
    expect(remove_forms.first).to_be_visible()

    # 2. The non-empty summary text the pipeline produced is shown on the page.
    summary = " ".join((doc.get("summary") or "").split())
    assert summary, "ingested document has no summary"
    expect(page.locator("main")).to_contain_text(summary[:40], timeout=10_000)

    # 3. The document's file is listed with a download link that actually
    #    resolves (the endpoint 302-redirects to a presigned blob URL).
    files = doc.get("files") or []
    assert files, "ingested document has no files"
    file_id = files[0]["id"]
    download_link = page.locator(
        f"a[href='/api/documents/{doc_id}/file/{file_id}']"
    ).first
    expect(download_link).to_be_visible()
    resp = api._c.get(
        f"/api/documents/{doc_id}/file/{file_id}", follow_redirects=False
    )
    assert resp.status_code in (200, 302, 303, 307), (
        f"download link did not resolve: {resp.status_code}"
    )


def test_search_empty_state_for_no_match(page):
    """Searching for a guaranteed-miss term shows an empty state, not an error.

    Log in and search the browser for a random token that matches nothing. The
    UI must render an explicit 'no results' empty state (and zero document result
    links) — not a 500/error page and not stale results from a prior query.
    """
    # A brand-new empty workspace guarantees zero matches for *any* query
    # (the bootstrap tenant always returns vector neighbours, so a fresh tenant
    # is the only deterministic "no match"). Self-service signup creates one.
    slug = flows.unique_marker("kbe2eempty")  # lowercase alnum → slug == name
    email = f"{slug}@example.test"
    password = "password1234"
    page.goto("/signup")
    page.fill("#tenant_name", slug)
    page.fill("#email", email)
    page.fill("#password", password)
    page.click("form[action='/signup'] button[type='submit']")
    page.wait_for_load_state("networkidle")

    flows.browser_login(page, slug, email, password)
    page.goto("/search")
    page.fill("input[name='q']", flows.unique_marker("zznomatch"))
    page.press("input[name='q']", "Enter")

    # The HTMX fragment must swap in the explicit empty state…
    page.wait_for_selector("#results :text('No results for')",
                           timeout=SEARCH_TIMEOUT_MS)
    # …with zero document result links (no error page, no stale hits).
    expect(page.locator("#results a[href*='/documents/']")).to_have_count(0)


def test_search_kind_filter_control_narrows_results(api, page):
    """The search page's kind filter narrows results to the selected kind.

    Ingest a text document, then in the browser run a search that finds it and
    apply the UI kind filter (the ``kind`` control on the search form). Selecting
    a non-matching kind (e.g. ``image``/``audio``) must drop the text doc from
    the results; selecting ``text`` (or clearing the filter) must include it
    again. Asserts the *UI control* filters, complementing the API-level
    ``test_search_kind_filter``.
    """
    tenant, email, password = _admin_creds()
    api.login(tenant, email, password)
    marker, doc = flows.ingest_and_wait(api, SAMPLE)
    doc_id = doc["id"]  # a text upload → kind "document"

    flows.browser_login(page, tenant, email, password)
    page.goto("/search")
    page.fill("input[name='q']", marker)
    page.press("input[name='q']", "Enter")

    doc_link = page.locator(f"#results a[href='/documents/{doc_id}']")
    # Unfiltered: the text document is among the results.
    expect(doc_link).to_have_count(1, timeout=SEARCH_TIMEOUT_MS)

    # Selecting a non-matching kind (image) must drop the text document.
    image_cb = page.locator("input.kind-cb[data-kind-value='image']")
    image_cb.check()
    expect(doc_link).to_have_count(0, timeout=SEARCH_TIMEOUT_MS)

    # Clearing the kind filter must include it again.
    image_cb.uncheck()
    expect(doc_link).to_have_count(1, timeout=SEARCH_TIMEOUT_MS)


def test_remove_tag_via_document_ui(api, page):
    """A tag removed through the document UI is gone from the document.

    ``test_tagging`` covers *adding* and *re-tagging*; tag **removal** is covered
    here. Ingest a doc, open ``/documents/{id}``, add a distinctive tag, then use
    the per-tag remove control (``form[action$='/remove']`` under
    ``/documents/{id}/tags/{tag_id}/remove``) to delete it. After removal the tag
    must be absent from both the rendered page and ``GET /api/documents/{id}``.
    """
    tenant, email, password = _admin_creds()
    api.login(tenant, email, password)
    marker, doc = flows.ingest_and_wait(api, SAMPLE)
    doc_id = doc["id"]
    distinctive = flows.unique_marker("kbe2etag")

    flows.browser_login(page, tenant, email, password)
    page.goto(f"/documents/{doc_id}")

    # Add a distinctive tag via the document UI's add-tag form.
    page.fill(f"form[action='/documents/{doc_id}/add-tag'] input[name='tag_name']",
              distinctive)
    page.click(f"form[action='/documents/{doc_id}/add-tag'] button[type='submit']")
    page.wait_for_load_state("networkidle")

    # The tag is now linked to the document (find its id for the exact controls).
    added = [t for t in api.get_document(doc_id).get("tags", [])
             if distinctive in t["name"].lower()]
    assert added, "the tag added through the UI is not linked to the document"
    tag_id = added[0]["id"]
    remove_form = f"form[action='/documents/{doc_id}/tags/{tag_id}/remove']"
    expect(page.locator(remove_form)).to_have_count(1)

    # Remove it via the per-tag remove control.
    page.click(f"{remove_form} button[type='submit']")
    page.wait_for_load_state("networkidle")

    # Gone from the API view of the document…
    remaining = api.get_document(doc_id).get("tags", [])
    assert all(t["id"] != tag_id for t in remaining), (
        "tag is still linked to the document after removal"
    )
    # …and gone from the rendered page (its remove control no longer exists).
    expect(page.locator(remove_form)).to_have_count(0)


def test_logout_via_navbar_revokes_session(page):
    """Logging out via the navbar ends the session; protected pages are denied.

    Browser-driven logout (distinct from the API ``test_logout_revokes_session``):
    after login, the navbar logout control (``POST /logout``) revokes the session
    and redirects to ``/login``. Afterwards a protected page (``/dashboard``) must
    be denied with 401 — proving the browser session was actually revoked, not
    merely navigated away from.
    """
    flows.browser_login(page, *_admin_creds())

    # While logged in, a protected page is reachable.
    page.goto("/dashboard")
    expect(page).to_have_url(re.compile(r"/dashboard(?:[?#].*)?$"))

    # Log out via the navbar control (POST /logout → redirect to /login).
    page.click("nav form[action='/logout'] button[type='submit']")
    page.wait_for_load_state("networkidle")
    expect(page).to_have_url(re.compile(r"/login"))

    # The session is revoked: the protected page is now denied (401), not served.
    resp = page.goto("/dashboard")
    assert resp is not None and resp.status == 401, (
        f"after logout, /dashboard must be denied with 401 (session revoked); "
        f"got {resp.status if resp else 'no response'}"
    )


def test_marketing_landing_links_to_register_and_pricing(page):
    """The public landing page renders and links to the account-creation + pricing.

    The marketing pages have no other coverage. Visiting ``/`` (logged out) must
    return 200 and render a real landing page whose primary conversion CTAs reach
    the account-creation flow (the landing's "Get started" CTA points at
    ``/register``) and the ``/pricing`` page. Assert both links are present and
    navigable.
    """
    resp = page.goto("/")
    assert resp is not None and resp.status == 200, "landing page did not return 200"
    # A real landing page (its hero value proposition), not a redirect/blank.
    expect(page.locator("body")).to_contain_text("instantly searchable")

    # The primary conversion CTAs: the account-creation flow and the pricing page.
    assert page.locator("a[href='/register']").count() >= 1, (
        "landing page must link to the account-creation flow (/register)"
    )
    assert page.locator("a[href='/pricing']").count() >= 1, (
        "landing page must link to /pricing"
    )

    # Both links are navigable.
    page.locator("a[href='/register']").first.click()
    expect(page).to_have_url(re.compile(r"/register(?:[?#].*)?$"))
    page.goto("/")
    page.locator("a[href='/pricing']").first.click()
    expect(page).to_have_url(re.compile(r"/pricing(?:[?#].*)?$"))


def test_pricing_page_lists_plans(page):
    """The public /pricing page lists the Free/Pro/Team plans with prices.

    Distinct from the authenticated ``/account/plan`` page: the *public*
    ``/pricing`` marketing page (logged out, 200) must present the three seeded
    plans — Free, Pro, Team — with the paid-tier prices shown ($9/mo Pro,
    $29/mo Team per the seeded plan data).
    """
    resp = page.goto("/pricing")
    assert resp is not None and resp.status == 200, "pricing page did not return 200"

    # The three seeded plans, each as its own card.
    for code in ("free", "pro", "team"):
        expect(page.locator(f"[data-plan='{code}']")).to_have_count(1)
    expect(page.locator("[data-plan='free']")).to_contain_text("Free")
    expect(page.locator("[data-plan='pro']")).to_contain_text("Pro")
    expect(page.locator("[data-plan='team']")).to_contain_text("Team")

    # The paid-tier monthly prices ($9/mo Pro, $29/mo Team).
    expect(page.locator("[data-plan='pro']")).to_contain_text("$9")
    expect(page.locator("[data-plan='pro']")).to_contain_text("month")
    expect(page.locator("[data-plan='team']")).to_contain_text("$29")
    expect(page.locator("[data-plan='team']")).to_contain_text("month")
