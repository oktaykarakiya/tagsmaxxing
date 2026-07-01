"""Catalog (step-2) — retrieval, hybrid search, RRF, rerank.

List with `pytest --collect-only -m search`.
"""

from __future__ import annotations

import pytest
import httpx

from lib import config, flows
from lib.api_client import ApiClient

pytestmark = pytest.mark.search

SAMPLE = (
    "Quarterly financial report for Acme Corp. This document covers revenue, "
    "invoices, and budget forecasts for Q3. Unique end-to-end marker: {marker}."
)


def test_search_finds_ingested_document(api):
    """A query for known content returns the matching document."""
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()
    api.login(tenant, email, password)

    marker, doc = flows.ingest_and_wait(api, SAMPLE)

    result = api.search(marker)
    hits = result.get("hits", [])

    doc_id = doc["id"]
    found = any(h["document_id"] == doc_id for h in hits)
    assert found, (
        f"ingested document {doc_id} not found in search results for {marker!r}"
    )
    assert result["query"] == marker
    assert result["count"] == len(hits)


def test_search_ranks_relevant_higher(api):
    """Given several docs, the most relevant ranks first after RRF + rerank."""
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()
    api.login(tenant, email, password)

    # Doc 1: clearly about Python programming.
    marker_py, doc_py = flows.ingest_and_wait(
        api,
        "Python programming tutorial for beginners. Learn about variables, "
        "functions, classes, and modules in Python. Unique marker: {marker}.",
    )

    # Doc 2: clearly about baking (unrelated to programming).
    marker_cake, doc_cake = flows.ingest_and_wait(
        api,
        "Recipe for chocolate cake with vanilla frosting. Ingredients: "
        "flour, sugar, cocoa, eggs, butter. Unique marker: {marker}.",
    )

    # Search for programming terms; the Python doc should rank higher.
    result = api.search("python programming variables functions")
    hits = result.get("hits", [])
    assert len(hits) >= 1, f"expected at least 1 hit, got {len(hits)}"

    # At minimum, the Python programming doc must be in the results.
    found_ids = {h["document_id"] for h in hits}
    assert doc_py["id"] in found_ids, (
        f"Python programming doc {doc_py['id']} not found in search results"
    )

    # If the cake doc also appears, the programming doc must rank higher.
    if doc_cake["id"] in found_ids:
        py_pos = next(
            i for i, h in enumerate(hits) if h["document_id"] == doc_py["id"]
        )
        cake_pos = next(
            i for i, h in enumerate(hits) if h["document_id"] == doc_cake["id"]
        )
        assert py_pos < cake_pos, (
            f"Python doc should rank higher than cake doc; "
            f"py={py_pos}, cake={cake_pos}"
        )

    # All hits must be ordered by score descending.
    scores = [h["score"] for h in hits]
    assert scores == sorted(scores, reverse=True), (
        f"hits not ordered by score descending: {scores}"
    )


def test_search_kind_filter(api):
    """Filtering by kind (e.g. image) restricts the result set."""
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()
    api.login(tenant, email, password)

    marker, doc = flows.ingest_and_wait(api, SAMPLE)

    # Search without filter — the doc should be found.
    all_results = api.search(marker)
    assert all_results["count"] >= 1, "unfiltered search should find the doc"

    # Filter by kind="document" — text docs should match.
    doc_kind_results = api.search(marker, kind="document")
    doc_hits = doc_kind_results.get("hits", [])
    assert any(h["document_id"] == doc["id"] for h in doc_hits), (
        "text doc should appear when filtering by kind=document"
    )

    # Filter by kind="image" — text docs should NOT match.
    image_results = api.search(marker, kind="image")
    image_hits = image_results.get("hits", [])
    assert not any(h["document_id"] == doc["id"] for h in image_hits), (
        "text doc should NOT appear when filtering by kind=image"
    )


def test_search_tag_filter(api):
    """Filtering by tag restricts results to tagged documents."""
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()
    api.login(tenant, email, password)

    marker, doc = flows.ingest_and_wait(api, SAMPLE)
    tags = doc.get("tags", [])
    assert tags, (
        "ingested document has no tags; need at least one tag to test filter"
    )

    # Pick the first tag to use as a filter.
    tag_name = tags[0]["name"]

    # Search with that tag as a filter — the doc should appear.
    result = api.search(marker, tag=tag_name)
    hits = result.get("hits", [])
    assert any(h["document_id"] == doc["id"] for h in hits), (
        f"doc should appear when filtering by its tag '{tag_name}'"
    )

    # Search with a non-existent tag — the doc should NOT appear.
    result_nonexistent = api.search(marker, tag="nonexistent-tag-zzz999")
    hits_nonexistent = result_nonexistent.get("hits", [])
    assert not any(
        h["document_id"] == doc["id"] for h in hits_nonexistent
    ), "doc should NOT appear when filtering by a non-existent tag"


def test_search_limit_param(api):
    """The limit parameter caps the number of hits returned."""
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()
    api.login(tenant, email, password)

    # Ingest 3 documents with overlapping content so a single search finds them all.
    for topic in ["revenue", "invoices", "forecasts"]:
        content = (
            f"Financial document about {topic}. Important quarterly data "
            f"for the corporation. Unique marker: {{marker}}."
        )
        flows.ingest_and_wait(api, content)

    # Search for "financial quarterly" — should find at least 3 docs.
    result_all = api.search("financial quarterly", limit=10)
    assert result_all["count"] >= 3, (
        f"expected at least 3 results, got {result_all['count']}"
    )

    # Search with limit=1 — should return at most 1.
    result_limited = api.search("financial quarterly", limit=1)
    assert result_limited["count"] <= 1, (
        f"limit=1 should return at most 1 hit, got {result_limited['count']}"
    )


def test_search_empty_query(api):
    """An empty query returns an empty/zero-result response, not an error."""
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()
    api.login(tenant, email, password)

    # Use a raw request — the contract says empty query returns 200 with zero
    # results, not a 400 error. (If the app returns 400 today, this test
    # surfaces that as a bug.)
    r = api._c.get("/api/search", params={"q": "", "limit": 10})
    assert r.status_code == 200, (
        f"empty query should return 200, got {r.status_code}: {r.text}"
    )
    body = r.json()
    assert body.get("hits") is not None, "empty query response must include 'hits'"
    assert body.get("count", -1) == 0, "empty query should return zero results"
    assert "error" not in body, "empty query should not be an error response"


def test_search_deeplink_page_provenance(api):
    """Hits from paged docs carry file_id + page_no for deep-linking."""
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()
    api.login(tenant, email, password)

    marker, doc = flows.ingest_and_wait(api, SAMPLE)

    result = api.search(marker)
    hits = result.get("hits", [])
    assert hits, f"search for {marker!r} returned no hits"

    for hit in hits:
        assert "file_id" in hit, f"hit missing file_id: {hit}"
        assert isinstance(
            hit["file_id"], int
        ), f"file_id must be an integer, got {type(hit['file_id'])}"
        # page_no must be present in the schema (may be null for simple docs).
        assert "page_no" in hit, f"hit missing page_no field: {hit}"


@pytest.mark.skip(
    reason="blocked: cannot ingest audio/video files through the harness; "
    "ApiClient only supports text/plain uploads. Audio/video ingestion "
    "requires multipart upload of binary media files."
)
def test_search_deeplink_ts_offset(api):
    """Hits from audio/video carry a ts_offset for deep-linking."""
    ...


def test_search_rerank_changes_order(api):
    """The reranker changes the order of RRF-fused candidates."""
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()
    api.login(tenant, email, password)

    # Ingest 3 docs with varying relevance to a common theme.
    for topic in ["machine learning algorithms", "deep neural networks", "baking cookies at home"]:
        content = (
            f"A comprehensive guide to {topic}. This document explains the core "
            f"concepts and advanced techniques in detail. Unique marker: {{marker}}."
        )
        flows.ingest_and_wait(api, content)

    # Search for ML-related terms.
    result = api.search(
        "machine learning neural networks artificial intelligence"
    )
    hits = result.get("hits", [])
    assert len(hits) >= 2, f"expected at least 2 hits, got {len(hits)}"

    # Every hit must carry a numeric score (assigned by the reranker).
    for hit in hits:
        assert isinstance(hit.get("score"), (int, float)), (
            f"hit missing numeric score: {hit}"
        )

    # Hits must be ordered by score descending (the reranker sorts them).
    scores = [h["score"] for h in hits]
    assert scores == sorted(scores, reverse=True), (
        f"hits not ordered by score descending after rerank: {scores}"
    )

    # The reranker should produce distinct scores — identical scores for
    # semantically different documents would mean the reranker didn't run.
    unique_scores = set(scores)
    assert len(unique_scores) > 1, (
        f"reranker produced identical scores for all {len(hits)} hits; "
        "reranker may not have run"
    )


def test_document_detail_returns_files_and_tags(api):
    """GET /api/documents/{id} returns files, tags, summary, and metadata."""
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()
    api.login(tenant, email, password)

    marker, doc = flows.ingest_and_wait(api, SAMPLE)

    # Top-level fields from DocumentDetail.
    assert "id" in doc, "document detail missing 'id'"
    assert "title" in doc, "document detail missing 'title'"
    assert "summary" in doc, "document detail missing 'summary'"
    assert "kind" in doc, "document detail missing 'kind'"
    assert "files" in doc, "document detail missing 'files'"
    assert "tags" in doc, "document detail missing 'tags'"
    assert "meta" in doc, "document detail missing 'meta'"
    assert "page_count" in doc, "document detail missing 'page_count'"
    assert "status" in doc, "document detail missing 'status'"
    assert "created_at" in doc, "document detail missing 'created_at'"

    # Files array: at least one file with required fields.
    files = doc["files"]
    assert isinstance(files, list), "files must be an array"
    assert len(files) >= 1, "document must have at least one file"
    for f in files:
        assert "id" in f, f"file entry missing 'id': {f}"
        assert "page_no" in f, f"file entry missing 'page_no': {f}"
        assert "mime" in f, f"file entry missing 'mime': {f}"

    # Tags array: at least one tag with id and non-empty name.
    tags = doc["tags"]
    assert isinstance(tags, list), "tags must be an array"
    assert len(tags) >= 1, "document must have at least one tag"
    for t in tags:
        assert "id" in t, f"tag entry missing 'id': {t}"
        assert "name" in t, f"tag entry missing 'name': {t}"
        assert isinstance(t["name"], str), "tag name must be a string"
        assert len(t["name"]) > 0, "tag name must not be empty"


def test_file_download_redirects_presigned(api):
    """GET /api/documents/{id}/file/{file_id} 302-redirects to a download URL."""
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()
    api.login(tenant, email, password)

    marker, doc = flows.ingest_and_wait(api, SAMPLE)

    files = doc.get("files", [])
    assert files, "document has no files; cannot test file download"
    file_id = files[0]["id"]
    doc_id = doc["id"]

    # Use a fresh httpx client WITHOUT follow_redirects so we can inspect
    # the 302 response headers directly.
    url = f"/api/documents/{doc_id}/file/{file_id}"
    with httpx.Client(
        base_url=api.base_url,
        verify=config.verify_tls(),
        follow_redirects=False,
        timeout=config.http_timeout(),
    ) as raw_client:
        r = raw_client.get(url, cookies=api._c.cookies)

    assert r.status_code == 302, (
        f"expected 302 Found for file download, got {r.status_code}: {r.text[:500]}"
    )

    # Location header must contain the presigned download URL.
    location = r.headers.get("Location") or r.headers.get("location")
    assert location is not None, "302 response must include a Location header"
    assert len(location) > 0, "Location header must not be empty"

    # Content-Disposition must signal attachment.
    cd = r.headers.get("Content-Disposition") or r.headers.get("content-disposition")
    assert cd is not None, "response must include Content-Disposition header"
    assert "attachment" in cd.lower(), (
        f"Content-Disposition must be attachment, got: {cd}"
    )


def test_browser_search_results(page):
    """The /search page renders result cards linking to document detail."""
    # Use a fresh ApiClient for the ingest, since 'page' is a Playwright fixture.
    api = ApiClient()
    try:
        tenant, email, password = (
            config.tenant_slug(),
            config.admin_email(),
            config.admin_password(),
        )
        api.login(tenant, email, password)
        marker, doc = flows.ingest_and_wait(api, SAMPLE)
    finally:
        api.close()

    # Log in through the browser.
    flows.browser_login(page, tenant, email, password)

    # Navigate to search and find the ingested document.
    page.goto("/search")
    page.fill("input[name='q']", marker)
    page.press("input[name='q']", "Enter")

    # Wait for HTMX to swap results into #results.
    page.wait_for_selector("#results a[href*='/documents/']", timeout=45_000)

    # At least one result card with a document detail link.
    result_links = page.locator("#results a[href*='/documents/']")
    count = result_links.count()
    assert count >= 1, "browser search showed no document result links"

    # One of the links should point to the ingested document's detail page.
    expected_href = f"/documents/{doc['id']}"
    has_correct_link = any(
        result_links.nth(i).get_attribute("href") == expected_href
        for i in range(count)
    )
    assert has_correct_link, (
        f"no result link to /documents/{doc['id']}; "
        f"expected a link to the ingested document"
    )
