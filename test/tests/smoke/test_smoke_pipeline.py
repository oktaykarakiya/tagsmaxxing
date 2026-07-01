"""End-to-end smoke test — the one fully-implemented test in step 1.

Exercises the real stack as a user would: ingest a uniquely-marked document via
the API (the inline pipeline runs extract → tag → embed → store synchronously on
the real models), then verify it is retrievable both through the JSON API and the
browser search UI, with a non-empty tag list and summary.

This is deliberately a *behavioral* assertion ("the pipeline did its job"), not a
check on the exact AI text — content quality is graded separately by the judge lane.
"""

from __future__ import annotations

import pytest

from lib import config, flows

pytestmark = pytest.mark.smoke

SAMPLE = (
    "Quarterly financial report for Acme Corp. This document covers revenue, "
    "invoices, and budget forecasts for Q3. Unique end-to-end marker: {marker}."
)


def test_ingest_then_search_roundtrip(api, page):
    """Ingest → pipeline → searchable (API + browser), with tags + summary."""
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()

    # 1. Authenticate as the bootstrap admin (JSON API; no CSRF on /auth or /api).
    api.login(tenant, email, password)

    # 2. Ingest a uniquely-marked text document; the inline pipeline runs
    #    synchronously (extract → tag → embed → store) before returning the doc.
    marker, doc = flows.ingest_and_wait(api, SAMPLE)
    assert doc.get("tags"), "ingested document has no tags (the tagger produced nothing)"
    assert doc.get("summary"), "ingested document has no summary"

    # 3. It is retrievable via the search API.
    hits = api.search(marker).get("hits", [])
    assert hits, f"search for {marker!r} returned no hits"

    # 4. The same document is findable through the browser search UI (as a user).
    flows.browser_login(page, tenant, email, password)
    page.goto("/search")
    page.fill("input[name='q']", marker)
    page.press("input[name='q']", "Enter")
    # Results swap into #results via HTMX; wait for a document link to appear.
    page.wait_for_selector("#results a[href*='/documents/']", timeout=45_000)
    assert page.locator("#results a[href*='/documents/']").count() >= 1, (
        "browser search showed no document results"
    )
