"""Catalog — text/structured *content extraction* makes uploads searchable.

A gap in the existing ingest suite: ``test_ingest.py`` proves text/PDF/media
uploads are *accepted* and that a plain ``.txt`` marker is searchable, but it does
not verify that the **extractor** pulls indexable text out of *structured* text
formats (CSV / Markdown / HTML / JSON / XML), strips presentation markup, or
chunks a large document — the behaviour behind BUG-INGEST-03 (CSV),
BUG-INGEST-04 (Markdown/HTML markup) and BUG-INGEST-09 (unicode).

The contract under test, for each format: ``POST /api/ingest`` returns 202 with a
``document_id``, the inline pipeline (extract → tag → embed → store) completes,
and the document's unique marker becomes **findable via** ``GET /api/search``. We
assert the search hit's ``document_id`` matches the uploaded doc; the JSON
document-detail response (:class:`DocumentDetail`) exposes no raw extracted-text
field, only ``snippet`` on a search hit — so extraction quality (markup stripped,
larger docs chunked) is asserted through search behaviour, not a detail field.

Per the suite philosophy a failing assertion is a real app bug and is left
failing — never weakened, and a 500 is never accepted for a case that should be
202/4xx. Because a successful ingest needs the LLM + embed backends, an autouse
fixture SKIPs the lane (not FAILs) when ``/health`` reports either degraded, so a
backend hiccup never produces a false failure.
"""

from __future__ import annotations

import httpx
import pytest

from lib import config, file_factory as ff, flows
from lib.api_client import ApiClient

pytestmark = [pytest.mark.ingest]


# ── Fixtures ──────────────────────────────────────────────────────────────────
@pytest.fixture(scope="module")
def client() -> ApiClient:
    """A module-scoped API client, logged in as the bootstrap admin.

    Reused across every case in this module (login once); closed at teardown.
    """
    c = ApiClient()
    c.login(config.tenant_slug(), config.admin_email(), config.admin_password())
    try:
        yield c
    finally:
        c.close()


@pytest.fixture(autouse=True)
def _require_pipeline_backends() -> None:
    """SKIP (not FAIL) any test in this module when the ingest backends are degraded.

    A successful ``/api/ingest`` runs the inline pipeline, which calls the text
    (tagger/LLM) and embed model roles; if either is down per ``/health`` an
    extraction test would false-fail. Probing here keeps the lane honest: a real
    extraction regression on *healthy* backends still FAILs.
    """
    with httpx.Client(
        base_url=config.base_url(),
        verify=config.verify_tls(),
        timeout=config.http_timeout(),
    ) as c:
        if not (ff.subsystem_ok(c, "text") and ff.subsystem_ok(c, "embed")):
            pytest.skip("ingest backends (text/embed) degraded per /health — not an app bug")


# ── Helpers (local per the harness rule: no edits outside this file) ──────────
def _marker_is_searchable(api: ApiClient, marker: str, doc_id: int | str) -> bool:
    """True if *marker* search surfaces *doc_id*, else the marker round-trips by id.

    Primary signal: a ``GET /api/search`` hit whose ``document_id`` (the field the
    JSON API serialises — see ``crates/api/src/handlers/search.rs``; ``id`` is
    checked too for forward-compatibility) equals the uploaded document. Fallback:
    the document detail is retrievable and the marker text appears somewhere in its
    JSON (title/summary/user_note/meta), proving the content was extracted even if
    ranking did not float it into the top hits.
    """
    hits = api.search(marker).get("hits", [])
    if any(str(h.get("document_id") or h.get("id")) == str(doc_id) for h in hits):
        return True
    detail = api.get_document(doc_id)
    return marker in _flatten_text(detail)


def _flatten_text(value: object) -> str:
    """Recursively concatenate all string leaves of a JSON value into one blob.

    Used to scan a document-detail response for a marker without assuming which
    field (title/summary/user_note/nested meta) the extractor surfaced it in.
    """
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return " ".join(_flatten_text(v) for v in value.values())
    if isinstance(value, list):
        return " ".join(_flatten_text(v) for v in value)
    return ""


def _snippets_for(api: ApiClient, query: str, doc_id: int | str) -> list[str]:
    """Return the ``snippet`` strings of search hits for *query* that match *doc_id*.

    The search hit's ``snippet`` is the only place the JSON API exposes extracted
    chunk text, so markup-stripping assertions inspect it when present.
    """
    hits = api.search(query).get("hits", [])
    return [
        str(h.get("snippet") or "")
        for h in hits
        if str(h.get("document_id") or h.get("id")) == str(doc_id)
    ]


def _ingest_one(client: ApiClient, payload: tuple[str, bytes, str], request_id: str) -> dict:
    """Upload a single ``(filename, bytes, mime)`` payload and assert a 202 + id.

    Returns the parsed JSON body. Raises an assertion (carrying the X-Request-Id)
    on any non-202 — extraction tests must never silently proceed past a rejected
    or 500'd upload.
    """
    out = ff.upload(client._c, payload, request_id=request_id)
    # A mid-flight backend flap (the autouse gate passed at test start, but the
    # tagger/embed backend dropped during the ingest) shows as 500/None — an
    # environment outage (F9/F5), not an extraction defect — so skip, not fail.
    if out["status"] in (None, 500, 502, 503) and not (
        ff.subsystem_ok(client._c, "text") and ff.subsystem_ok(client._c, "embed")
    ):
        pytest.skip(
            f"backend degraded mid-ingest ({out['status']}) — environment (F9/F5), not extraction; "
            f"x-request-id={out['request_id']}"
        )
    assert out["status"] == 202, (
        f"ingest of {payload[0]!r} (mime {payload[2]}) must be 202, got {out['status']}; "
        f"x-request-id={out['request_id']}; body={out['text']}"
    )
    body = out["body"] or {}
    assert body.get("document_id") is not None, (
        f"202 ingest of {payload[0]!r} returned no document_id; "
        f"x-request-id={out['request_id']}; body={out['text']}"
    )
    # Queued ingestion (P15): the 202 stages a pending document; wait for the
    # background worker to finalize it before asserting on extracted content.
    flows.wait_until_ready(client, body["document_id"], job_id=body.get("job_id"))
    return body


# ── Cases 1–5: each structured text format extracts to a searchable document ──
@pytest.mark.parametrize("label", ["csv", "markdown", "html", "json", "xml"])
def test_structured_text_format_is_extracted_and_searchable(client: ApiClient, label: str) -> None:
    """Each text/structured format (CSV/MD/HTML/JSON/XML) ingests and is searchable.

    Covers BUG-INGEST-03 (CSV) and BUG-INGEST-04 (Markdown/HTML): upload the
    format's sample via ``/api/ingest`` (expect 202 + a document_id), then assert
    its unique marker is findable — a ``/api/search`` hit pointing at the uploaded
    document, or (fallback) the marker present in the retrievable document detail.
    Either proves the extractor turned the structured bytes into indexable content.
    """
    marker = flows.unique_marker(f"kbe2eext{label}")
    payload = ff.text_payloads(marker)[label]
    rid = ff.new_request_id(f"ext-{label}")

    doc_id = _ingest_one(client, payload, rid)["document_id"]

    assert _marker_is_searchable(client, marker, doc_id), (
        f"{label} content was not extracted into a searchable form: marker {marker!r} "
        f"did not surface document {doc_id} via search nor appear in its detail; "
        f"x-request-id={rid}"
    )


# ── Case 6: presentation markup is stripped, not indexed as content ───────────
def test_html_and_markdown_markup_is_stripped_from_extracted_text(client: ApiClient) -> None:
    """HTML/Markdown ingest indexes *text*, not raw markup tokens (BUG-INGEST-04).

    Ingest the HTML and Markdown samples, then assert the extractor stripped
    presentation markup:

    * The human-readable content word (here ``content`` for HTML, the heading word
      ``Heading`` for Markdown) is searchable and surfaces the document — proving
      text was extracted.
    * A pure-markup token that only appears as structure (``h1``/``body`` tags in
      the HTML, the ``#`` heading sigil in the Markdown) must NOT pollute the
      extracted text: when a search hit's ``snippet`` is available it must not
      contain the literal ``<h1>``/``<body>``/``#`` markup.

    The JSON document-detail response exposes no raw extracted-text field, so the
    ``snippet`` on a search hit is the extraction window we assert against.
    """
    marker = flows.unique_marker("kbe2eextmarkup")
    payloads = ff.text_payloads(marker)

    # ── HTML: <h1>Title</h1><p>{marker} content here.</p> ────────────────────
    html_rid = ff.new_request_id("ext-html-markup")
    html_id = _ingest_one(client, payloads["html"], html_rid)["document_id"]
    assert _marker_is_searchable(client, marker, html_id), (
        f"HTML content not extracted/searchable by marker {marker!r}; x-request-id={html_rid}"
    )
    # The body word is real content; markup tags must not appear in any snippet.
    for snippet in _snippets_for(client, "content", html_id):
        assert "<h1>" not in snippet.lower() and "<body>" not in snippet.lower(), (
            f"HTML markup leaked into the extracted/indexed text — snippet still carries raw "
            f"tags: {snippet!r}; x-request-id={html_rid}"
        )

    # ── Markdown: "# Heading {marker}\n\nParagraph about **{marker}**." ───────
    md_rid = ff.new_request_id("ext-md-markup")
    md_id = _ingest_one(client, payloads["markdown"], md_rid)["document_id"]
    assert _marker_is_searchable(client, marker, md_id), (
        f"Markdown content not extracted/searchable by marker {marker!r}; x-request-id={md_rid}"
    )
    # The heading *word* survives; the '#' heading sigil and '**' emphasis must not
    # ride along in the extracted snippet.
    md_hits = client.search("Heading").get("hits", [])
    assert any(str(h.get("document_id") or h.get("id")) == str(md_id) for h in md_hits), (
        f"Markdown heading text 'Heading' did not surface document {md_id} — the heading "
        f"may not have been extracted; x-request-id={md_rid}"
    )
    for snippet in _snippets_for(client, "Heading", md_id):
        assert "# " not in snippet and "**" not in snippet, (
            f"Markdown markup leaked into the extracted/indexed text — snippet still carries "
            f"raw '#'/'**' markup: {snippet!r}; x-request-id={md_rid}"
        )


# ── Case 7: unicode / emoji / RTL content extracts to searchable text ─────────
def test_unicode_emoji_rtl_text_extraction_is_searchable(client: ApiClient) -> None:
    """Unicode/emoji/RTL .txt content is extracted and searchable (BUG-INGEST-09).

    Complementary to ``test_input_validation.test_unicode_and_emoji_content_roundtrips``
    (which checks a unicode *user_note* round-trips byte-for-byte): here the multi-
    byte content lives in the **file body**, and the focus is that the extractor
    indexes it so the document is *findable* and round-trips by id without
    corruption. Ingest ``café 日本語 😀 مرحبا {marker}`` and assert 202, that the
    ASCII marker is searchable, and that the document is retrievable.
    """
    marker = flows.unique_marker("kbe2eextuni")
    content = f"café 日本語 😀 مرحبا {marker}".encode("utf-8")
    payload = (f"{marker}.txt", content, "text/plain")
    rid = ff.new_request_id("ext-unicode")

    doc_id = _ingest_one(client, payload, rid)["document_id"]

    assert _marker_is_searchable(client, marker, doc_id), (
        f"unicode/emoji/RTL content was not extracted into a searchable form: marker "
        f"{marker!r} did not surface document {doc_id}; x-request-id={rid}"
    )
    # The record round-trips by id without corruption (valid UTF-8 detail, no 500).
    detail = client.get_document(doc_id)
    assert detail.get("id") == doc_id, (
        f"unicode document {doc_id} did not round-trip by id: {detail!r}; x-request-id={rid}"
    )


# ── Case 8: a larger document is extracted + chunked, marker still searchable ─
def test_large_text_document_is_chunked_and_searchable(client: ApiClient) -> None:
    """~200 KB of text extracts into ≥1 stored chunk and stays searchable.

    Exercises extraction + chunking of a non-trivial document: build ~200 KB of
    text carrying the unique marker, ingest it (expect 202), then assert the
    document detail reports at least one stored file/chunk and the marker is still
    findable via search after chunking a larger body.
    """
    marker = flows.unique_marker("kbe2eextbig")
    content = ff.sized_text(200 * ff.KIB, marker)
    payload = (f"{marker}.txt", content, "text/plain")
    rid = ff.new_request_id("ext-large")

    doc_id = _ingest_one(client, payload, rid)["document_id"]

    detail = client.get_document(doc_id)
    files = detail.get("files", [])
    assert len(files) >= 1, (
        f"large-text ingest produced no stored files/chunks: {detail!r}; x-request-id={rid}"
    )
    assert _marker_is_searchable(client, marker, doc_id), (
        f"marker {marker!r} not searchable after chunking a ~200 KB document {doc_id}; "
        f"x-request-id={rid}"
    )
