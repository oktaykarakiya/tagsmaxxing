"""Catalog (step 2) — search UI depth (rendered result experience).

Asserts the *rendered* search experience that neither ``test_search`` (JSON API)
nor ``test_ui_journeys`` (click-through, empty-state, kind-filter) covers: the
query persists in the box, result cards carry a snippet + tags, the deep-link
provenance is surfaced, the tag filter narrows in-UI, a result count is shown, and
the more-relevant document renders above the less-relevant one.

Per the suite philosophy a failing assertion means an app bug and is left failing.
Browser waits that hit the retrieval pipeline (embed + rerank) need a generous
budget. Search/templates: ``crates/api/src/web`` + ``crates/api/templates/``.
"""

from __future__ import annotations

import re

import pytest
from playwright.sync_api import expect

from lib import config, flows
from lib.api_client import ApiClient

pytestmark = pytest.mark.search

SEARCH_TIMEOUT_MS = 45_000

# A document with clear, taggable content; ``{marker}`` is filled by the helpers.
SAMPLE = (
    "Quarterly financial report for Acme Corp covering revenue, invoices, and "
    "budget forecasts for Q3. Unique end-to-end marker: {marker}."
)


def _admin_creds() -> tuple[str, str, str]:
    """(tenant_slug, email, password) for the bootstrap admin user."""
    return config.tenant_slug(), config.admin_email(), config.admin_password()


def _ingest(content: str) -> tuple[str, dict]:
    """Ingest a uniquely-marked doc via a throwaway admin client; return (marker, doc)."""
    c = ApiClient()
    try:
        c.login(*_admin_creds())
        return flows.ingest_and_wait(c, content)
    finally:
        c.close()


def _ingest_body(marker: str, body: str, suffix: str) -> int:
    """Ingest a text doc carrying the shared ``marker``; return its document id.

    Unlike :func:`flows.ingest_and_wait` (which mints its own marker), this lets
    several documents share one marker so a single query matches them all.
    """
    c = ApiClient()
    try:
        c.login(*_admin_creds())
        text = f"{body}\n\nUnique end-to-end marker: {marker}."
        resp = c.ingest_text(f"{marker}-{suffix}.txt", text)
        doc_id = resp.get("document_id")
        assert doc_id is not None, f"ingest returned no document_id: {resp}"
        # Queued ingestion (P15): callers assert on search ranking, which needs
        # the document fully processed (chunked + embedded), not merely staged.
        flows.wait_until_ready(c, doc_id, job_id=resp.get("job_id"))
        return doc_id
    finally:
        c.close()


def _is_provenance_deeplink(href: str) -> bool:
    """True if ``href`` deep-links into the document at a specific page/timestamp.

    A provenance deep-link points *inside* the source — a non-empty URL fragment
    (e.g. ``#page=2`` / ``#p2`` / ``#t=12``) or a page/offset query parameter
    (``?page=2`` / ``&t=12``). The bare document root ``/documents/{id}`` is not a
    deep-link.
    """
    if not href:
        return False
    fragment = href.split("#", 1)[1] if "#" in href else ""
    if fragment.strip():
        return True
    return bool(re.search(r"[?&](?:page|p|t|ts|time|offset|sec|loc)=", href))


def test_search_query_persists_in_input(page):
    """After a search, the query text remains in the search input for refining.

    Log in, search for a term, and after the results render assert the search
    input (``input[name='q']``) still holds the submitted query — the user can
    refine without retyping. (API tests can't observe this; ``test_ui_journeys``
    doesn't assert it.)
    """
    marker, doc = _ingest(SAMPLE)
    doc_id = doc["id"]

    flows.browser_login(page, *_admin_creds())
    page.goto("/search")
    page.fill("input[name='q']", marker)
    page.press("input[name='q']", "Enter")

    # The HTMX round-trip completes: the matched document renders in the results.
    expect(
        page.locator(f"#results a[href='/documents/{doc_id}']")
    ).to_be_visible(timeout=SEARCH_TIMEOUT_MS)

    # The submitted query is still in the search box, ready to be refined.
    expect(page.locator("input[name='q']")).to_have_value(marker)


def test_search_result_card_shows_snippet_and_tags(api, page):
    """A rendered result card surfaces a content snippet and the document's tags.

    Ingest a doc, search for its marker, and assert the matching result *card*
    (not just its link) shows meaningful context: a non-empty snippet/summary
    excerpt and at least one tag chip. Behavioural — the card is informative, not
    a bare link. (``test_search``'s browser test only checks the link exists.)
    """
    tenant, email, password = _admin_creds()
    api.login(tenant, email, password)
    marker, doc = flows.ingest_and_wait(api, SAMPLE)
    doc_id = doc["id"]

    # The snippet the retrieval pipeline produced for this document (so we assert
    # the card renders *that* content, not merely some text).
    hits = api.search(marker).get("hits", [])
    hit = next((h for h in hits if h["document_id"] == doc_id), None)
    assert hit is not None, f"search for {marker!r} did not return the ingested doc"
    snippet = " ".join((hit.get("snippet") or "").split())
    assert snippet, "the retrieval pipeline produced no snippet for the ingested doc"

    # Tags the pipeline attached (exclude any equal to the kind badge so a chip
    # check can't be satisfied by the kind label rather than a real tag).
    kind = (doc.get("kind") or "").strip().lower()
    tag_names = [
        t["name"] for t in doc.get("tags", []) if t["name"].strip().lower() != kind
    ]
    assert tag_names, "ingested document has no distinct tags to show as chips"

    flows.browser_login(page, tenant, email, password)
    page.goto("/search")
    page.fill("input[name='q']", marker)
    page.press("input[name='q']", "Enter")

    card = page.locator(f"#results li:has(a[href='/documents/{doc_id}'])").first
    expect(card).to_be_visible(timeout=SEARCH_TIMEOUT_MS)

    # (a) A content snippet/summary excerpt — not just the bare link/title.
    expect(card).to_contain_text(snippet[:50], timeout=SEARCH_TIMEOUT_MS)

    # (b) At least one tag chip — a tag name rendered as its own element in the
    #     card (an exact-text element, so prose that merely mentions the word
    #     doesn't count).
    shown = [t for t in tag_names if card.get_by_text(t, exact=True).count() > 0]
    assert shown, (
        f"result card shows no tag chips; none of the document's tags "
        f"{tag_names} appear as a chip in the card"
    )


def test_search_deeplink_shows_provenance(page):
    """A search result exposes a deep-link to its source location (provenance).

    Ingest a doc, search for it, and assert the result card exposes a provenance
    deep-link — a link/anchor to the specific page or timestamp/offset the match
    came from (``#page=``/``?page=``/``t=`` style), not merely the document root.
    Complements the API-level ``test_search_deeplink_*`` with the rendered UI.
    """
    marker, doc = _ingest(SAMPLE)
    doc_id = doc["id"]

    flows.browser_login(page, *_admin_creds())
    page.goto("/search")
    page.fill("input[name='q']", marker)
    page.press("input[name='q']", "Enter")

    card = page.locator(f"#results li:has(a[href='/documents/{doc_id}'])").first
    expect(card).to_be_visible(timeout=SEARCH_TIMEOUT_MS)

    hrefs = card.locator("a").evaluate_all(
        "els => els.map(e => e.getAttribute('href') || '')"
    )
    deeplinks = [h for h in hrefs if _is_provenance_deeplink(h)]
    assert deeplinks, (
        f"result card exposes no provenance deep-link to the matched "
        f"page/timestamp; its links are {hrefs} (document root only), so the user "
        f"cannot jump to where the match came from"
    )


def test_tag_filter_via_ui_narrows_results(api, page):
    """Selecting a tag filter in the search UI restricts results to that tag.

    Ingest a doc and add a distinctive tag to it via the document UI. Then on the
    search page, apply that tag as a UI filter and assert the results are narrowed
    to documents carrying the tag (the doc appears with the filter; an unrelated
    tag filter drops it). Distinct from ``test_ui_journeys`` (kind filter) and
    ``test_search`` (API tag filter).
    """
    tenant, email, password = _admin_creds()
    api.login(tenant, email, password)
    marker, doc = flows.ingest_and_wait(api, SAMPLE)
    doc_id = doc["id"]
    distinctive = flows.unique_marker("kbe2etag")
    unrelated = flows.unique_marker("kbe2enotag")

    flows.browser_login(page, tenant, email, password)

    # Attach a distinctive tag to the document through its UI add-tag form.
    page.goto(f"/documents/{doc_id}")
    page.fill(
        f"form[action='/documents/{doc_id}/add-tag'] input[name='tag_name']",
        distinctive,
    )
    page.click(f"form[action='/documents/{doc_id}/add-tag'] button[type='submit']")
    page.wait_for_load_state("networkidle")

    # Confirm it attached, and capture its canonical name for the filter.
    attached = [
        t["name"]
        for t in api.get_document(doc_id).get("tags", [])
        if distinctive in t["name"].lower()
    ]
    assert attached, "the distinctive tag was not attached to the document via the UI"
    tag_name = attached[0]

    # A query that finds the document (unfiltered baseline).
    page.goto("/search")
    page.fill("input[name='q']", marker)
    page.press("input[name='q']", "Enter")
    doc_link = page.locator(f"#results a[href='/documents/{doc_id}']")
    expect(doc_link).to_have_count(1, timeout=SEARCH_TIMEOUT_MS)

    tag_input = page.locator("#search-form input[name='tag']")

    # Filtering by an unrelated tag narrows the results — the doc drops out.
    tag_input.type(unrelated, delay=20)
    page.wait_for_timeout(300)  # wait for HTMX debounce (150ms)
    expect(doc_link).to_have_count(0, timeout=SEARCH_TIMEOUT_MS)

    # Filtering by the document's own tag includes it again.
    page.locator("#search-form input[name='tag']").clear()
    page.locator("#search-form input[name='tag']").type(tag_name, delay=20)
    page.wait_for_timeout(300)
    expect(doc_link).to_have_count(1, timeout=SEARCH_TIMEOUT_MS)
    expect(doc_link).to_have_count(1, timeout=SEARCH_TIMEOUT_MS)


def test_search_shows_result_count(page):
    """The rendered search results report a result count to the user.

    After a search that matches at least one document, the results region must
    show a human-readable count of how many results were found (e.g. 'N results').
    Assert the count is present and reflects a non-zero number of hits.
    """
    marker, doc = _ingest(SAMPLE)
    doc_id = doc["id"]

    flows.browser_login(page, *_admin_creds())
    page.goto("/search")
    page.fill("input[name='q']", marker)
    page.press("input[name='q']", "Enter")

    # Wait for the matched doc so the results region has finished rendering.
    expect(
        page.locator(f"#results a[href='/documents/{doc_id}']")
    ).to_be_visible(timeout=SEARCH_TIMEOUT_MS)

    results_text = " ".join(page.locator("#results").inner_text().split())
    match = re.search(r"(\d+)\s+results?\b", results_text, re.IGNORECASE)
    assert match, (
        f"the search results show no human-readable result count; got: {results_text!r}"
    )
    assert int(match.group(1)) >= 1, (
        f"the result count should reflect the non-zero hits; got {match.group(0)!r}"
    )


def test_search_ranks_better_match_first_in_ui(page):
    """In the rendered results, the more relevant document appears above the rest.

    Ingest two documents under a shared unique marker: one whose body is strongly
    on-topic for a query phrase and one only weakly related. Search the phrase in
    the browser and assert the strongly-relevant document's result card appears
    *before* the weakly-relevant one in DOM order — the UI reflects ranking.
    (``test_search`` asserts ranking via the API; this asserts the rendered order.)
    """
    marker = flows.unique_marker()
    strong_body = (
        # Opens with the literal query phrase: the 'simple' FTS config keeps
        # stopwords, and plainto AND-semantics require EVERY query term to
        # appear — without this the keyword arm matches nothing and ranking
        # degrades to pure vector similarity, where prior runs' near-identical
        # fixture copies (the persistent e2e corpus) drown this run's pair.
        "How to install rooftop solar panels on my house roof. "
        "Step-by-step instructions for installing rooftop solar panels on a house "
        "roof: mount the photovoltaic panels to the roof rafters with racking, "
        "install flashing to seal the roof penetrations, wire the solar array to "
        "the inverter, and connect the rooftop solar system to the home electrical "
        "panel. A practical rooftop solar installation how-to guide."
    )
    weak_body = (
        "An overview of solar energy as a renewable power source. Solar panels use "
        "photovoltaic cells to turn sunlight into electricity and are part of the "
        "clean-energy transition alongside wind and hydro power. Background on how "
        "solar power benefits the environment."
    )
    strong_id = _ingest_body(marker, strong_body, "strong")
    weak_id = _ingest_body(marker, weak_body, "weak")

    flows.browser_login(page, *_admin_creds())
    page.goto("/search")
    page.fill(
        "input[name='q']",
        f"how to install rooftop solar panels on my house roof {marker}",
    )
    page.press("input[name='q']", "Enter")

    # The strong document must render (its body contains the full query
    # phrase + the marker, so the keyword arm pins it into the results even
    # against the polluted persistent corpus).
    expect(
        page.locator(f"#results a[href='/documents/{strong_id}']")
    ).to_be_visible(timeout=SEARCH_TIMEOUT_MS)
    # The weak document is vector-only for this query; against an accumulated
    # corpus it may legitimately fall below the rendered cutoff — which is
    # itself correct ordering (strong rendered, weak below). Only when both
    # render do we compare their positions.
    if page.locator(f"#results a[href='/documents/{weak_id}']").count() == 0:
        return

    # Compare the two cards' positions in DOM order (one link per result card).
    hrefs = page.locator("#results a[href^='/documents/']").evaluate_all(
        "els => els.map(e => e.getAttribute('href'))"
    )
    idx_strong = hrefs.index(f"/documents/{strong_id}")
    idx_weak = hrefs.index(f"/documents/{weak_id}")
    assert idx_strong < idx_weak, (
        f"the strongly on-topic document {strong_id} should render above the "
        f"weakly-related document {weak_id}; DOM order was {hrefs}"
    )
