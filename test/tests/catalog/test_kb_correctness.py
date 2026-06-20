"""Catalog — core KB correctness: delete/dedup/provenance/ranking/format depth.

The existing suite proves the forward happy path (ingest→tag→embed→search) but has
near-zero coverage of destructive/update correctness and ranking/provenance
*truthfulness*. A code audit found there is no per-document delete, dedup can orphan
documents, and provenance is only structurally checked. Drivable correctness checks
are PENDING; absent capabilities (delete, pagination, OCR, re-embed, replace) are
BLOCKED.
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path
from typing import Any

import pytest

from lib import config, flows
from lib.api_client import ApiClient

pytestmark = pytest.mark.ingest

# A strong password that satisfies the web signup floor used for fresh tenants.
_PASSWORD = "p4ssw0rd!Z"


# ── Local helpers (no lib changes allowed) ──────────────────────────────────────


def _login_admin(api: ApiClient) -> ApiClient:
    """Authenticate the API client as the bootstrap admin (tenant 'default')."""
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())
    return api


def _post_ingest(api: ApiClient, parts: list[tuple[str, tuple[str, bytes, str]]],
                 data: dict[str, str] | None = None):
    """Post a multipart upload directly (so non-202 responses can be inspected).

    ``parts`` is a list of ``("files", (filename, raw_bytes, content_type))`` tuples;
    multiple entries become pages of one document via the inline pipeline.
    Returns the raw ``httpx.Response`` (does not raise on error status).
    """
    return api._c.post("/api/ingest", files=parts, data=data or {})


def _ingest_file(api: ApiClient, filename: str, raw: bytes, content_type: str) -> int:
    """Ingest a single file and return its processed ``document_id``.

    Ingestion is queued (P15): the 202 stages a pending document and a worker
    finalizes it, so wait for readiness — callers assert on searchable state.
    """
    r = _post_ingest(api, [("files", (filename, raw, content_type))])
    assert r.status_code == 202, f"ingest of {filename!r} failed: {r.status_code} {r.text[:300]}"
    body = r.json()
    doc_id = body.get("document_id")
    assert doc_id is not None, f"ingest of {filename!r} returned no document_id: {body}"
    flows.wait_until_ready(api, doc_id, job_id=body.get("job_id"))
    return doc_id


def _search_ids(api: ApiClient, q: str, *, limit: int = 10,
                tag: str | None = None, kind: str | None = None) -> list[int]:
    """Return the ranked list of document ids for a search query (top = first)."""
    resp = api.search(q, limit=limit, tag=tag, kind=kind)
    return [h["document_id"] for h in resp.get("hits", [])]


def _slugify(name: str) -> str:
    """Replicate the server-side slugify: lowercase, non-alnum→hyphen, trim hyphens."""
    out: list[str] = []
    for c in name.strip().lower():
        if c.isalnum():
            out.append(c)
        elif out and out[-1] != "-":
            out.append("-")
    return "".join(out).rstrip("-") or "workspace"


def _signup_and_login(page: Any, workspace_name: str, email: str, password: str) -> ApiClient:
    """Create a brand-new tenant + owner via the web /signup form and return a
    cookie-authenticated :class:`ApiClient` scoped to that fresh tenant.

    ``/auth/register`` only adds users to an *existing* tenant, so the web signup
    form is the black-box path that actually provisions a new tenant.
    """
    page.goto("/signup")
    page.fill("input[name='tenant_name']", workspace_name)
    page.fill("input[name='email']", email)
    page.fill("input[name='password']", password)
    page.click("form[action='/signup'] button[type='submit']")
    page.wait_for_load_state("networkidle")

    client = ApiClient()
    client.login(_slugify(workspace_name), email, password)
    return client


def _add_tag_via_web(page: Any, doc_id: int, tag_name: str) -> None:
    """Add a canonical tag to a document through the real web add-tag form (CSRF-safe)."""
    page.goto(f"/documents/{doc_id}")
    page.fill("form[action$='/add-tag'] input[name='tag_name']", tag_name)
    page.click("form[action$='/add-tag'] button[type='submit']")
    page.wait_for_load_state("networkidle")


def _zip_bytes(entries: list[tuple[str, str]]) -> bytes:
    """Build an in-memory zip from ordered (name, text) entries (OOXML packaging)."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in entries:
            zf.writestr(name, content)
    return buf.getvalue()


def _make_docx(marker: str) -> bytes:
    """A minimal but valid OOXML .docx whose body text contains ``marker`` (Tika-readable)."""
    content_types = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/word/document.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>'
        "</Types>"
    )
    rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="word/document.xml"/></Relationships>'
    )
    document = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        f"<w:body><w:p><w:r><w:t>{marker} quarterly revenue report summary</w:t></w:r></w:p>"
        "</w:body></w:document>"
    )
    return _zip_bytes([
        ("[Content_Types].xml", content_types),
        ("_rels/.rels", rels),
        ("word/document.xml", document),
    ])


def _make_xlsx(marker: str) -> bytes:
    """A minimal but valid OOXML .xlsx whose cell text contains ``marker`` (Tika-readable)."""
    content_types = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        "</Types>"
    )
    rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/></Relationships>'
    )
    workbook = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheets><sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets></workbook>'
    )
    workbook_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        'Target="worksheets/sheet1.xml"/></Relationships>'
    )
    sheet = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>'
        f"{marker} budget total amount"
        "</t></is></c></row></sheetData></worksheet>"
    )
    return _zip_bytes([
        ("[Content_Types].xml", content_types),
        ("_rels/.rels", rels),
        ("xl/workbook.xml", workbook),
        ("xl/_rels/workbook.xml.rels", workbook_rels),
        ("xl/worksheets/sheet1.xml", sheet),
    ])


# ── Tests ───────────────────────────────────────────────────────────────────────


def test_provenance_cited_location_contains_match(api):
    """The winning hit's file_id/page_no points at the page that truly contains the marker.

    Ingest a multi-page/multi-file document where a unique marker appears on exactly
    one page; search the marker and assert the hit's provenance (file_id/page_no) and
    snippet correspond to the page that contains it — not just that the fields exist.
    """
    _login_admin(api)
    marker = flows.unique_marker()

    page_one = "Introduction chapter covering gardening tools and soil preparation methods."
    target = (
        f"Financial appendix chapter. The unique provenance marker {marker} "
        "appears only on this single page and nowhere else in the document."
    )
    page_three = "Closing chapter discussing weather patterns and seasonal planning notes."

    parts = [
        ("files", ("page-one.txt", page_one.encode("utf-8"), "text/plain")),
        ("files", ("page-two.txt", target.encode("utf-8"), "text/plain")),
        ("files", ("page-three.txt", page_three.encode("utf-8"), "text/plain")),
    ]
    r = _post_ingest(api, parts)
    assert r.status_code == 202, f"multi-page ingest failed: {r.status_code} {r.text[:300]}"
    body = r.json()
    doc_id = body["document_id"]

    # Queued ingestion (P15): wait for the worker before asserting on
    # processed (searchable) state.
    doc = flows.wait_until_ready(api, doc_id, job_id=body.get("job_id"))
    files = doc.get("files", [])
    assert len(files) == 3, f"expected a 3-page document, got {len(files)} file(s)"

    target_file = next((f for f in files if f.get("page_label") == "page-two.txt"), None)
    assert target_file is not None, f"could not find the marker page among {files}"
    target_file_id = target_file["id"]

    hits = api.search(marker, limit=10).get("hits", [])
    assert hits, f"search for {marker!r} returned no hits"
    mine = [h for h in hits if h["document_id"] == doc_id]
    assert mine, f"our document {doc_id} was not in the hits for {marker!r}"
    top = mine[0]

    # Provenance must point at the exact page that contains the marker.
    assert top["file_id"] == target_file_id, (
        f"hit provenance file_id={top['file_id']} does not point at the marker page "
        f"(file {target_file_id}, page_no {target_file['page_no']})"
    )
    assert top.get("page_no") == target_file["page_no"], (
        f"hit page_no={top.get('page_no')} != marker page {target_file['page_no']}"
    )
    # The cited snippet must come from that page (contains the marker text).
    assert marker.lower() in (top.get("snippet") or "").lower(), (
        f"hit snippet does not contain the marker it cites: {top.get('snippet')!r}"
    )


def test_exact_match_outranks_semantic_only(api):
    """A doc containing the literal query phrase ranks above a paraphrase-only doc.

    Ingest doc A with the literal phrase and doc B with only synonyms; search the
    exact phrase and assert A outranks B (hybrid fusion/rerank correctness).
    """
    _login_admin(api)
    scope = flows.unique_marker("exact")
    phrase = "budget variance reconciliation appendix"

    content_a = (
        f"Scope token {scope}. {phrase}. This section presents the {phrase} in full, "
        f"and the {phrase} table follows below."
    )
    content_b = (
        f"Scope token {scope}. Spending deviation settlement supplement overview. "
        "An income difference matching section with no literal repetition of the phrase."
    )
    doc_a = _ingest_file(api, f"{scope}-a.txt", content_a.encode("utf-8"), "text/plain")
    doc_b = _ingest_file(api, f"{scope}-b.txt", content_b.encode("utf-8"), "text/plain")

    # limit=50: the persistent e2e corpus accumulates near-identical copies of
    # these fixtures from prior runs; they legitimately compete in the vector
    # arm and can push the weaker doc past a small window. The assertion that
    # matters — A outranks B — is relative and pollution-immune.
    ids = _search_ids(api, f"{scope} {phrase}", limit=50)
    assert doc_a in ids and doc_b in ids, (
        f"both docs should be retrievable for the scoped exact query; got {ids}"
    )
    assert ids.index(doc_a) < ids.index(doc_b), (
        f"exact-match doc {doc_a} should outrank semantic-only doc {doc_b}; order={ids}"
    )


def test_multiterm_all_terms_doc_ranks_first(api):
    """A multi-term query ranks the doc containing all terms above subset docs."""
    _login_admin(api)
    scope = flows.unique_marker("multi")
    t1, t2, t3 = "zylophonic", "quadrifoil", "brontomyte"

    content_all = (
        f"Scope token {scope}. This memo discusses {t1}, {t2}, and {t3} together. "
        f"The combined {t1} {t2} {t3} cluster is analysed in detail."
    )
    content_subset = (
        f"Scope token {scope}. This memo discusses only {t1} and nothing else of note."
    )
    doc_all = _ingest_file(api, f"{scope}-all.txt", content_all.encode("utf-8"), "text/plain")
    doc_subset = _ingest_file(api, f"{scope}-sub.txt", content_subset.encode("utf-8"), "text/plain")

    # limit=50: prior runs' copies of these fixtures (same invented terms,
    # different scope tokens) compete in the vector arm — see the note in
    # test_exact_match_outranks_semantic_only.
    ids = _search_ids(api, f"{scope} {t1} {t2} {t3}", limit=50)
    assert doc_all in ids and doc_subset in ids, (
        f"both docs should be retrievable for the scoped multi-term query; got {ids}"
    )
    assert ids.index(doc_all) < ids.index(doc_subset), (
        f"all-terms doc {doc_all} should rank above subset doc {doc_subset}; order={ids}"
    )


def test_dedup_collision_does_not_orphan_document(api):
    """Uploading identical bytes as a new file does not detach them from the first doc.

    Audit: the (tenant, sha256) ON CONFLICT repoints the file row to the new document,
    orphaning the first. Ingest bytes B as a.txt (doc1), then identical B as b.txt
    (doc2); assert BOTH documents still own their file and remain searchable/
    downloadable — guards a real orphaning bug.
    """
    _login_admin(api)
    marker = flows.unique_marker()
    content = (
        f"Dedup orphan guard document. Shared content carrying marker {marker} "
        f"repeated so it stays searchable {marker}."
    )
    raw = content.encode("utf-8")

    r1 = _post_ingest(api, [("files", ("alpha.txt", raw, "text/plain"))])
    assert r1.status_code == 202, f"first ingest failed: {r1.status_code} {r1.text[:300]}"
    b1 = r1.json()
    doc1 = b1["document_id"]
    # Queued ingestion (P15): let the worker finish doc1 before re-uploading the
    # same bytes — the orphan guard is about the *processed* dedup state, and
    # the later search asserts both documents are fully ingested.
    flows.wait_until_ready(api, doc1, job_id=b1.get("job_id"))

    r2 = _post_ingest(api, [("files", ("bravo.txt", raw, "text/plain"))])
    assert r2.status_code == 202, f"second ingest failed: {r2.status_code} {r2.text[:300]}"
    b2 = r2.json()
    doc2 = b2["document_id"]

    assert doc1 != doc2, f"identical bytes under a new name collapsed into one doc id {doc1}"
    flows.wait_until_ready(api, doc2, job_id=b2.get("job_id"))

    d1 = api.get_document(doc1)
    d2 = api.get_document(doc2)
    assert d1.get("files"), f"document {doc1} was orphaned — it owns no file after re-upload"
    assert d2.get("files"), f"document {doc2} owns no file"

    # Both documents remain downloadable (a presigned redirect is issued).
    for d in (d1, d2):
        fid = d["files"][0]["id"]
        rr = api._c.get(f"/api/documents/{d['id']}/file/{fid}", follow_redirects=False)
        assert rr.status_code in (302, 200), (
            f"document {d['id']} file {fid} not downloadable: {rr.status_code}"
        )

    # Both remain searchable by the shared marker.
    ids = _search_ids(api, marker, limit=20)
    assert doc1 in ids and doc2 in ids, (
        f"both documents must stay searchable after dedup; got {ids} (doc1={doc1}, doc2={doc2})"
    )


def test_cross_tenant_identical_bytes_isolated(page):
    """The same content hash in two tenants yields separate, tenant-scoped documents.

    Upload identical bytes as tenant A and tenant B; assert each tenant searches/sees
    and downloads only its own document (dedup key is tenant-prefixed; blobs/hits not
    collapsed across tenants).
    """
    name_a = flows.unique_marker("tena")
    name_b = flows.unique_marker("tenb")
    api_a = _signup_and_login(page, name_a, f"{name_a}@e2e.local", _PASSWORD)
    api_b = _signup_and_login(page, name_b, f"{name_b}@e2e.local", _PASSWORD)
    try:
        marker = flows.unique_marker()
        content = (
            f"Identical cross-tenant bytes. Marker {marker} present verbatim in both "
            f"tenants {marker}."
        )
        raw = content.encode("utf-8")

        ra = _post_ingest(api_a, [("files", ("shared.txt", raw, "text/plain"))])
        assert ra.status_code == 202, f"tenant A ingest failed: {ra.status_code} {ra.text[:300]}"
        ba = ra.json()
        doc_a = ba["document_id"]

        rb = _post_ingest(api_b, [("files", ("shared.txt", raw, "text/plain"))])
        assert rb.status_code == 202, f"tenant B ingest failed: {rb.status_code} {rb.text[:300]}"
        bb = rb.json()
        doc_b = bb["document_id"]

        assert doc_a != doc_b, (
            f"identical bytes across tenants collapsed into one doc id {doc_a}"
        )

        # Queued ingestion (P15): both tenants' search/download asserts below
        # need fully processed documents.
        flows.wait_until_ready(api_a, doc_a, job_id=ba.get("job_id"))
        flows.wait_until_ready(api_b, doc_b, job_id=bb.get("job_id"))

        # Each tenant searches and finds only its own document.
        ids_a = _search_ids(api_a, marker, limit=20)
        assert doc_a in ids_a, f"tenant A cannot find its own doc by {marker!r}; got {ids_a}"
        assert doc_b not in ids_a, f"tenant A search leaked tenant B's doc {doc_b}: {ids_a}"

        ids_b = _search_ids(api_b, marker, limit=20)
        assert doc_b in ids_b, f"tenant B cannot find its own doc by {marker!r}; got {ids_b}"
        assert doc_a not in ids_b, f"tenant B search leaked tenant A's doc {doc_a}: {ids_b}"

        # Cross-tenant direct reads are 404.
        assert api_a._c.get(f"/api/documents/{doc_b}").status_code == 404, (
            "tenant A could read tenant B's document"
        )
        assert api_b._c.get(f"/api/documents/{doc_a}").status_code == 404, (
            "tenant B could read tenant A's document"
        )

        # Each can download its own file; cross-tenant download is 404.
        d_a = api_a.get_document(doc_a)
        fid_a = d_a["files"][0]["id"]
        own = api_a._c.get(f"/api/documents/{doc_a}/file/{fid_a}", follow_redirects=False)
        assert own.status_code in (302, 200), f"tenant A own download failed: {own.status_code}"
        cross = api_b._c.get(f"/api/documents/{doc_a}/file/{fid_a}", follow_redirects=False)
        assert cross.status_code == 404, (
            f"tenant B downloaded tenant A's file: expected 404, got {cross.status_code}"
        )
    finally:
        api_a.close()
        api_b.close()


def test_tag_merge_propagates_to_search(page, api):
    """Merging tag X→Y updates the searchable tag set and document detail.

    Tag two docs with X, merge X→Y via /admin/tags/merge, then assert
    ``/api/search?tag=X`` is empty, ``?tag=Y`` returns both, and the detail no longer
    lists X.
    """
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()
    api.login(tenant, email, password)
    flows.browser_login(page, tenant, email, password)

    scope = flows.unique_marker("mergescope")
    tag_x = flows.unique_marker("xtag")
    tag_y = flows.unique_marker("ytag")

    doc1 = _ingest_file(
        api, f"{scope}-a.txt",
        f"Document A about quarterly planning. Scope token {scope}.".encode("utf-8"),
        "text/plain",
    )
    doc2 = _ingest_file(
        api, f"{scope}-b.txt",
        f"Document B about budget review. Scope token {scope}.".encode("utf-8"),
        "text/plain",
    )

    # Tag both docs with X, and create Y by tagging doc1 with it as well.
    _add_tag_via_web(page, doc1, tag_x)
    _add_tag_via_web(page, doc2, tag_x)
    _add_tag_via_web(page, doc1, tag_y)

    # Precondition: before the merge, ?tag=X returns both docs.
    ids_x_before = _search_ids(api, scope, limit=50, tag=tag_x)
    assert doc1 in ids_x_before and doc2 in ids_x_before, (
        f"setup failed — both docs should carry tag X before merge; got {ids_x_before}"
    )

    # Merge X → Y through the admin UI (CSRF handled by the rendered form).
    page.goto("/admin/tags")
    page.wait_for_selector("select[name='from_tag_id']")
    page.select_option("select[name='from_tag_id']", label=tag_x)
    page.select_option("select[name='to_tag_id']", label=tag_y)
    page.click("form[action='/admin/tags/merge'] button[type='submit']")
    page.wait_for_load_state("networkidle")

    # After the merge: X is gone, Y covers both documents.
    ids_x_after = _search_ids(api, scope, limit=50, tag=tag_x)
    assert ids_x_after == [], f"tag X should be empty after merge, got {ids_x_after}"

    ids_y_after = _search_ids(api, scope, limit=50, tag=tag_y)
    assert doc1 in ids_y_after and doc2 in ids_y_after, (
        f"tag Y should return both documents after merge; got {ids_y_after}"
    )

    names1 = [t["name"] for t in api.get_document(doc1).get("tags", [])]
    names2 = [t["name"] for t in api.get_document(doc2).get("tags", [])]
    assert tag_x not in names1 and tag_x not in names2, (
        f"merged-away tag X still listed on a document: {names1} / {names2}"
    )
    assert tag_y in names1 and tag_y in names2, (
        f"target tag Y missing after merge: {names1} / {names2}"
    )


def test_csv_content_is_searchable(api):
    """A .csv file's cell text is extracted and findable by a unique marker."""
    _login_admin(api)
    marker = flows.unique_marker()
    csv = (
        "id,description,amount\n"
        f"1,{marker} quarterly revenue line item,4200\n"
        "2,office supplies,150\n"
    )
    doc_id = _ingest_file(api, f"{marker}.csv", csv.encode("utf-8"), "text/csv")

    ids = _search_ids(api, marker, limit=20)
    assert doc_id in ids, f"CSV cell text not searchable: marker {marker!r} -> {ids}"


def test_markdown_html_content_is_searchable(api):
    """Markdown and HTML uploads have their text extracted and findable by a marker."""
    _login_admin(api)
    m_md = flows.unique_marker("md")
    m_html = flows.unique_marker("html")

    markdown = (
        f"# Quarterly Report\n\nThis markdown note references {m_md} within the body "
        "text describing revenue and budget figures.\n"
    )
    html = (
        "<html><head><title>Report</title></head><body><h1>Quarterly Report</h1>"
        f"<p>This HTML page references {m_html} inside a paragraph about revenue.</p>"
        "</body></html>"
    )
    doc_md = _ingest_file(api, f"{m_md}.md", markdown.encode("utf-8"), "text/markdown")
    doc_html = _ingest_file(api, f"{m_html}.html", html.encode("utf-8"), "text/html")

    ids_md = _search_ids(api, m_md, limit=20)
    assert doc_md in ids_md, f"Markdown text not searchable: marker {m_md!r} -> {ids_md}"

    ids_html = _search_ids(api, m_html, limit=20)
    assert doc_html in ids_html, f"HTML text not searchable: marker {m_html!r} -> {ids_html}"


def test_office_formats_are_searchable(api):
    """A .docx and .xlsx containing a unique marker are findable after ingest (Tika).

    Construct minimal real OOXML files (build via a library or a stored fixture); if
    neither is available in the harness, mark blocked with that reason.
    """
    _login_admin(api)
    m_docx = flows.unique_marker("docx")
    m_xlsx = flows.unique_marker("xlsx")

    doc_docx = _ingest_file(
        api, f"{m_docx}.docx", _make_docx(m_docx),
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )
    doc_xlsx = _ingest_file(
        api, f"{m_xlsx}.xlsx", _make_xlsx(m_xlsx),
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    ids_docx = _search_ids(api, m_docx, limit=20)
    assert doc_docx in ids_docx, f".docx text not searchable: marker {m_docx!r} -> {ids_docx}"

    ids_xlsx = _search_ids(api, m_xlsx, limit=20)
    assert doc_xlsx in ids_xlsx, f".xlsx text not searchable: marker {m_xlsx!r} -> {ids_xlsx}"


def test_zero_byte_file_rejected(api):
    """POST /api/ingest with a 0-byte file is a clean 400 (detected as x-empty, not allowed)."""
    _login_admin(api)
    r = _post_ingest(api, [("files", ("empty.txt", b"", "text/plain"))])
    assert r.status_code == 400, (
        f"0-byte upload should be a clean 400, got {r.status_code}: {r.text[:300]}"
    )


def test_mime_magic_mismatch_rejected(api):
    """A file whose magic bytes don't match an allowed MIME is rejected (4xx), not 500.

    Pins the documented upload-validation contract so it can't silently regress.
    """
    _login_admin(api)
    # ELF magic → detected as an executable (denied), despite the .txt name/type.
    elf = b"\x7fELF\x02\x01\x01\x00" + b"\x00" * 56
    r = _post_ingest(api, [("files", ("notes.txt", elf, "text/plain"))])
    assert 400 <= r.status_code < 500, (
        f"magic/MIME mismatch should be rejected with a 4xx (not 500), "
        f"got {r.status_code}: {r.text[:300]}"
    )


@pytest.mark.skip(reason="blocked: no per-document delete route exists")
def test_per_document_delete_propagation(api):
    """Deleting one document removes its chunks/embeddings/search-hits/tags/blob.

    BLOCKED: no ``DELETE /api/documents/{id}`` — orphaned chunks that still surface
    after 'delete' would be a correctness + data-leak disaster.
    """
    ...


@pytest.mark.skip(reason="blocked: search has no offset/cursor (top-100 ceiling)")
def test_search_pagination_offset(api):
    """Results beyond the first page are retrievable via offset/cursor.

    BLOCKED: ``SearchParams`` exposes only ``limit`` (<=100); no offset/cursor and no
    document/job LIST endpoint — a tenant cannot enumerate a large corpus.
    """
    ...


@pytest.mark.skip(reason="blocked: no re-embed action after an embedder/model change")
def test_reembed_after_model_change(api):
    """Existing documents are re-embedded after the embedder changes (no stale vectors).

    BLOCKED: the re-embed check only logs a warning; no reembed job is wired, so a
    model swap silently degrades hybrid search.
    """
    ...


_FIXTURES = Path(__file__).resolve().parents[2] / "lib" / "fixtures_data"


def test_ocr_scanned_document_searchable(api):
    """A photo of text yields searchable text — the VLM caption is indexed (F1).

    The image extractor produces no text; the VLM caption (which reads the visible
    text) must be chunked + embedded so the image is findable by its content. The
    committed fixture draws the distinctive words MERIDIAN / NEPTUNE / INVOICE.
    Before the F1 fix the caption only reached the tagger (0 chunks) and the image
    was invisible to search.
    """
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()
    api.login(tenant, email, password)

    png = (_FIXTURES / "text_image.png").read_bytes()
    marker = flows.unique_marker("ocr")
    resp = api._c.post("/api/ingest", files=[("files", (f"{marker}.png", png, "image/png"))])
    assert resp.status_code == 202, f"image ingest failed: {resp.status_code} {resp.text[:200]}"
    body = resp.json()
    doc = flows.wait_until_ready(api, body["document_id"], job_id=body.get("job_id"))

    assert doc.get("summary"), "image produced no caption/summary"
    found = False
    for word in ("MERIDIAN", "NEPTUNE", "invoice"):
        hits = api.search(word).get("hits", [])
        if any(h["document_id"] == doc["id"] for h in hits):
            found = True
            break
    assert found, (
        f"image {doc['id']} is not searchable by its visible text "
        f"(F1: the VLM caption must be indexed as chunks); summary={doc.get('summary')!r}"
    )


def test_archive_contents_are_searchable(api):
    """Text INSIDE a zip archive is extracted and searchable, not just listed (F2).

    Before the F2 fix archives were ingested as a file-listing only, so a unique
    token inside an entry was not findable.
    """
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()
    api.login(tenant, email, password)

    marker = flows.unique_marker("ziptext")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("readme.txt", f"The secret token inside this archive is {marker}.")
        z.writestr("notes/extra.md", "# Notes\nunrelated archived markdown content")
    zbytes = buf.getvalue()

    resp = api._c.post("/api/ingest", files=[("files", (f"{marker}.zip", zbytes, "application/zip"))])
    assert resp.status_code == 202, f"zip ingest failed: {resp.status_code} {resp.text[:200]}"
    body = resp.json()
    doc = flows.wait_until_ready(api, body["document_id"], job_id=body.get("job_id"))

    hits = api.search(marker).get("hits", [])
    assert any(h["document_id"] == doc["id"] for h in hits), (
        f"archive inner-entry text (marker {marker}) is not searchable "
        f"(F2: zip entry contents must be extracted, not just listed)"
    )


@pytest.mark.skip(reason="blocked: no content-replace / version-supersede route")
def test_content_replace_supersedes_old(api):
    """Replacing a document's content makes the new content searchable and the old not.

    BLOCKED: only append-only /add-page exists; no replace/version supersession.
    """
    ...
