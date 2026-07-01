"""Catalog (step 2) — input validation, output escaping, injection safety.

Drives the app with *hostile* input and asserts it is neutralised: HTML/JS is
escaped where untrusted strings are rendered (stored-XSS), search/login inputs
that look like SQL or markup are treated as literal data (no injection, no
reflected-XSS), path-traversal filenames are de-fanged, and pathological sizes /
byte sequences are bounded rather than crashing the app.

This is **distinct from** ``test_tagging``'s prompt-injection test (which checks
the *LLM tagger* isn't hijacked): here the concern is the **web/storage layer** —
escaping in templates and parameterised queries. Per the suite philosophy a
failing assertion is a real security bug and is left failing — never weakened.
Untrusted strings rendered into pages live in ``crates/api/templates/``.
"""

from __future__ import annotations

import html

import pytest

from lib import config, flows
from lib.api_client import ApiClient  # noqa: F401  (kept from stub; ApiClient used via the `api` fixture)

pytestmark = pytest.mark.security

# A canary that, if reflected unescaped into HTML, would execute or break out of
# its context. Tests assert it appears only in an inert/escaped form.
_XSS = "<script>window.__xss_fired=1</script>"

# An <img onerror> probe. Unlike a bare <script>, this fires reliably even when
# injected via ``innerHTML`` (e.g. an HTMX swap) or via an attribute that the
# browser later renders, because the broken image triggers the inline handler.
_IMG_XSS = "<img src=x onerror=window.__xss_fired=1>"


# ── Local helpers (defined here per the harness rule: no edits outside this file) ──


def _admin() -> tuple[str, str, str]:
    """(tenant_slug, email, password) for the bootstrap admin."""
    return config.tenant_slug(), config.admin_email(), config.admin_password()


def _xss_fired(page) -> bool:
    """True iff an injected payload executed and set the ``window.__xss_fired`` flag."""
    return bool(page.evaluate("() => Boolean(window.__xss_fired)"))


def _escaped(payload: str) -> str:
    """The HTML-escaped (text-context) form of ``payload``.

    Matches how a browser serialises an escaped text node (``<``/``>``/``&`` →
    entities). Our payloads contain no quotes or ampersands, so this equals the
    template's escaped output for the text contexts we assert on.
    """
    return html.escape(payload, quote=False)


def _ingest_raw(api, filename: str, content: bytes, *, user_note: str | None = None, timeout=None):
    """POST a single-file multipart ingest via the raw client.

    Used where the test must inspect a *non-202* status that the typed
    :meth:`ApiClient.ingest_text` helper would raise on (4xx/5xx), and where the
    file body must carry raw bytes (NUL / control characters).
    """
    files = [("files", (filename, content, "text/plain"))]
    data: dict[str, str] = {}
    if user_note is not None:
        data["user_note"] = user_note
    return api._c.post("/api/ingest", files=files, data=data, timeout=timeout)


def test_xss_in_user_note_is_escaped_in_ui(api, page):
    """A script payload in the user note is rendered inert (escaped), not executed.

    Ingest a document whose ``user_note`` contains ``<script>…</script>``, then
    open its ``/documents/{id}`` page in the browser. The payload must appear only
    as escaped text (the template must HTML-escape it); the script must not run
    (assert the canary side-effect, e.g. ``window.__xss_fired``, is undefined and
    no ``<script>`` injected by the note exists in the DOM).
    """
    tenant, email, password = _admin()
    api.login(tenant, email, password)

    marker, doc = flows.ingest_and_wait(
        api,
        "Document body for the stored-XSS user-note test. {marker}",
        user_note=_XSS,
    )
    doc_id = doc["id"]
    # The JSON API stores the note verbatim (JSON is not an HTML context).
    assert doc.get("user_note") == _XSS, f"user_note not stored verbatim: {doc.get('user_note')!r}"

    flows.browser_login(page, tenant, email, password)
    page.goto(f"/documents/{doc_id}")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(400)

    assert not _xss_fired(page), "user_note <script> executed in the document page — stored XSS"
    assert _escaped(_XSS) in page.content(), (
        "user_note payload was not HTML-escaped where the note is rendered"
    )


def test_xss_in_filename_is_escaped_in_ui(api, page):
    """A script payload in the uploaded filename is escaped wherever it's shown.

    Upload a file whose *name* contains an HTML/script payload and view it in the
    UI (document detail / search results). The filename must be HTML-escaped
    everywhere it is displayed; the injected script must not execute.
    """
    tenant, email, password = _admin()
    api.login(tenant, email, password)

    marker = flows.unique_marker()
    payload_name = f"<img src=x onerror=window.__xss_fired=1>{marker}.txt"
    resp = api.ingest_text(payload_name, f"Filename XSS test content {marker}.")
    doc_id = resp["document_id"]
    assert doc_id is not None, f"ingest returned no document_id: {resp}"

    flows.browser_login(page, tenant, email, password)
    page.goto(f"/documents/{doc_id}")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(400)

    assert not _xss_fired(page), "filename payload executed in the document page — stored XSS via filename"
    assert _escaped(_IMG_XSS) in page.content(), (
        "filename payload was not HTML-escaped where the filename is displayed"
    )


def test_xss_in_tag_name_is_escaped(api, page):
    """A tag whose name contains markup is rendered escaped, not as live HTML.

    Via the document UI, add a tag whose name contains ``<img onerror=…>`` / a
    ``<script>``. When the tag chip is rendered, the markup must be escaped — no
    element is injected and no handler fires.
    """
    tenant, email, password = _admin()
    api.login(tenant, email, password)

    marker, doc = flows.ingest_and_wait(api, "Document for the tag-XSS test. {marker}")
    doc_id = doc["id"]
    tag_payload = f"<img src=x onerror=window.__xss_fired=1>{marker}"

    flows.browser_login(page, tenant, email, password)
    page.goto(f"/documents/{doc_id}")
    page.wait_for_load_state("networkidle")

    # Add the malicious tag through the real document-detail form (carries its
    # own CSRF cookie + hidden field set on page load).
    page.fill("form[action$='/add-tag'] input[name='tag_name']", tag_payload)
    page.click("form[action$='/add-tag'] button[type='submit']")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(400)

    assert not _xss_fired(page), "tag-name payload executed when the chip rendered — stored XSS via tag"
    assert _escaped(_IMG_XSS) in page.content(), (
        "tag-name payload was not HTML-escaped where the tag chip is rendered"
    )


def test_html_in_search_query_not_reflected_unescaped(page):
    """A search query containing HTML is not reflected unescaped (no reflected XSS).

    Search the browser for a query string that includes ``<script>``. Whatever the
    results page echoes back (the query box, a 'no results for …' message) must be
    HTML-escaped; the canary script must not execute.
    """
    tenant, email, password = _admin()
    flows.browser_login(page, tenant, email, password)

    page.goto("/search")
    page.wait_for_load_state("networkidle")

    page.fill("input[name='q']", _IMG_XSS)
    page.press("input[name='q']", "Enter")
    # The HTMX fragment replaces #results, removing the initial empty-state copy.
    page.wait_for_selector("text=Search across all your documents", state="detached", timeout=60_000)
    page.wait_for_timeout(600)

    assert not _xss_fired(page), "search query was reflected and executed — reflected XSS"
    assert _escaped(_IMG_XSS) in page.content(), (
        "search query was not HTML-escaped where it is echoed back into the results"
    )


def test_sql_injection_like_search_is_safe(api):
    """A SQL-injection-style search query is handled as plain text, safely.

    Authenticated, run ``/api/search`` with payloads like
    ``' OR '1'='1`` and ``'); DROP TABLE documents;--``. The API must respond 200
    (treating it as literal query text → typically zero hits), never 500, and the
    app must remain healthy afterward (no tables dropped — a follow-up benign
    search still works).
    """
    tenant, email, password = _admin()
    api.login(tenant, email, password)

    # A benign, uniquely-marked record we can find again afterward to prove the
    # store survived the injection attempts.
    marker, _doc = flows.ingest_and_wait(api, "Benign searchable record. {marker}")

    payloads = [
        "' OR '1'='1",
        "'); DROP TABLE documents;--",
        '" OR "1"="1',
        "1; DROP TABLE chunks; --",
        "%' OR 1=1 --",
        "') UNION SELECT NULL --",
    ]
    for p in payloads:
        r = api._c.get("/api/search", params={"q": p, "limit": 10})
        assert r.status_code == 200, (
            f"SQLi-style search {p!r} must be treated as literal text (200), "
            f"got {r.status_code}: {r.text[:200]}"
        )

    # The app is healthy and nothing was dropped: the benign record is still there.
    hits = api.search(marker).get("hits", [])
    assert hits, f"benign search for {marker!r} returned no hits — the store may be damaged"


def test_sql_injection_like_login_is_safe(api):
    """SQL-injection-style login fields produce a normal auth failure, not a 500.

    POST ``/auth/login`` with ``tenant_slug``/``email`` set to SQL-injection
    payloads. Parameterised queries must treat them as data: the result is a plain
    401 (no such user) — never a 500 and never an auth bypass (no session cookie).
    """
    payloads = [
        {"tenant_slug": "' OR '1'='1", "email": "' OR '1'='1", "password": "x"},
        {"tenant_slug": "default", "email": "admin@local.kb' OR '1'='1' --", "password": "wrong"},
        {"tenant_slug": "default'; DROP TABLE users;--", "email": "a@b.com", "password": "x"},
        {"tenant_slug": "default", "email": "admin@local.kb'--", "password": "x"},
    ]
    for body in payloads:
        r = api._c.post("/auth/login", json=body)
        assert r.status_code == 401, (
            f"SQLi-style login {body!r} must yield 401 (invalid credentials), "
            f"got {r.status_code}: {r.text[:200]}"
        )
        # No auth bypass: a failed login must not issue a session cookie.
        assert api._c.cookies.get("__Host-session") is None, (
            f"failed login {body!r} issued a session cookie — possible auth bypass"
        )

    # Parameterisation is correct and the users table is intact: the real admin
    # can still authenticate normally.
    res = api.login(config.tenant_slug(), config.admin_email(), config.admin_password())
    assert res.get("message"), "legitimate admin login failed after injection attempts"


def test_path_traversal_filename_neutralized(api):
    """A path-traversal filename is stored/served safely, with no traversal.

    Ingest a file named ``../../../../etc/passwd``. The document must be created
    and reachable only by its id; the stored/displayed filename must be sanitised
    (no leading ``../`` path escaping the blob namespace) and the file download
    must serve the uploaded bytes, not a host file.
    """
    tenant, email, password = _admin()
    api.login(tenant, email, password)

    marker = flows.unique_marker()
    resp = _ingest_raw(api, "../../../../etc/passwd", f"not a host file {marker}".encode())

    # A hostile filename must be neutralised gracefully — never a server error.
    assert resp.status_code < 500, (
        f"path-traversal filename caused a server error ({resp.status_code}) instead of being "
        f"neutralised: {resp.text[:200]}"
    )

    if resp.status_code == 202:
        # Accepted: the document is reachable only by id and the stored filename
        # must not preserve any traversal that escapes the blob namespace.
        doc_id = resp.json().get("document_id")
        assert doc_id is not None, f"202 ingest returned no document_id: {resp.text[:200]}"
        doc = api.get_document(doc_id)
        for f in doc.get("files", []):
            label = f.get("page_label") or ""
            assert ".." not in label and not label.startswith("/") and not label.startswith("\\"), (
                f"stored filename was not sanitised — traversal preserved: {label!r}"
            )
    else:
        # Otherwise it must be a clean client-side rejection.
        assert resp.status_code in (400, 413, 415, 422), (
            "path-traversal upload should be sanitised-and-accepted (202) or cleanly rejected "
            f"(4xx); got {resp.status_code}: {resp.text[:200]}"
        )

    # The endpoint still serves normal traffic afterward.
    health = api._c.get("/api/search", params={"q": marker, "limit": 1})
    assert health.status_code == 200, f"search unhealthy after traversal attempt: {health.status_code}"


def test_unicode_and_emoji_content_roundtrips(api):
    """Unicode, emoji and RTL content survives ingest → store → search intact.

    Ingest a document containing multi-byte unicode, emoji and right-to-left
    script alongside a unique ASCII marker. The pipeline must complete, the
    document must be searchable by the marker, and the stored content/summary must
    come back as valid UTF-8 (no mojibake, no 500).
    """
    tenant, email, password = _admin()
    api.login(tenant, email, password)

    unicode_note = "Café ☕ — 日本語のメモ — العربية مرحبا — Ωμέγα — 😀🎉🚀"
    marker, doc = flows.ingest_and_wait(
        api,
        "Fiscal report with mixed scripts: 日本語 العربية 😀 budget. {marker}",
        user_note=unicode_note,
    )

    assert doc.get("id") is not None, f"ingest produced no document: {doc}"
    # Stored unicode/emoji/RTL round-trips byte-for-byte (no mojibake).
    assert doc.get("user_note") == unicode_note, (
        f"unicode content did not round-trip intact: {doc.get('user_note')!r}"
    )

    # The document is searchable by its ASCII marker (embed + store completed).
    hits = api.search(marker).get("hits", [])
    assert any(h.get("document_id") == doc["id"] for h in hits), (
        f"unicode document not found by marker {marker!r}; hits={hits}"
    )


def test_oversized_note_field_bounded(api):
    """A pathologically large user_note is rejected or bounded, not a 500.

    Submit an ingest whose ``user_note`` is far larger than any sane limit (e.g.
    several MB of text). The app must reject it with a 4xx (413/400) or accept and
    safely bound it — but must not 500, hang, or be driven into resource
    exhaustion. The endpoint must still serve a normal request afterward.
    """
    tenant, email, password = _admin()
    api.login(tenant, email, password)

    marker = flows.unique_marker()
    huge_note = "A" * (5 * 1024 * 1024)  # 5 MiB — far beyond any reasonable note
    resp = _ingest_raw(
        api,
        f"{marker}.txt",
        f"small body for the oversized-note test {marker}".encode(),
        user_note=huge_note,
        timeout=180.0,
    )

    assert resp.status_code < 500, (
        f"oversized user_note caused a server error ({resp.status_code}) — it must be a clean "
        f"4xx or a bounded 2xx: {resp.text[:200]}"
    )
    assert resp.status_code in (202, 400, 413, 422), (
        "oversized user_note should be accepted-and-bounded (202) or rejected (400/413/422); "
        f"got {resp.status_code}: {resp.text[:200]}"
    )

    # No wedging / resource exhaustion: a normal request still works afterward.
    health = api._c.get("/api/search", params={"q": "healthcheck", "limit": 1}, timeout=60.0)
    assert health.status_code == 200, f"endpoint unhealthy after oversized note: {health.status_code}"


def test_null_bytes_and_control_chars_handled(api):
    r"""Null bytes and control characters in filename/content don't crash the app.

    Ingest content and a filename containing NUL (``\x00``) and other C0 control
    characters. The app must handle them gracefully — sanitising/replacing as
    needed — returning a normal 2xx or a clean 4xx, never a 500 or a corrupted
    record that breaks later reads.
    """
    tenant, email, password = _admin()
    api.login(tenant, email, password)

    marker = flows.unique_marker()
    # NUL + C0 control chars in the untrusted file bytes. (A NUL cannot be carried
    # in a multipart filename *header*, so the byte-level surface is the content;
    # the note field also carries control characters.)
    content = b"line1\x00line2\x07\x08\x0b\x1b\x1f end " + marker.encode()
    resp = _ingest_raw(
        api,
        f"control-{marker}.txt",
        content,
        user_note="note\x07\x1bwith-controls",
    )

    assert resp.status_code < 500, (
        f"null/control bytes caused a server error ({resp.status_code}) instead of graceful "
        f"handling: {resp.text[:200]}"
    )
    assert resp.status_code in (202, 400, 413, 415, 422), (
        "null/control input should be sanitised-and-accepted (2xx) or cleanly rejected (4xx); "
        f"got {resp.status_code}: {resp.text[:200]}"
    )

    if resp.status_code == 202:
        doc_id = resp.json().get("document_id")
        assert doc_id is not None, f"202 ingest returned no document_id: {resp.text[:200]}"
        # The record is not corrupted — it reads back cleanly by id.
        doc = api.get_document(doc_id)
        assert doc.get("id") == doc_id

    # Later reads still work (no corrupted record wedged the store).
    health = api._c.get("/api/search", params={"q": marker, "limit": 1})
    assert health.status_code == 200, f"search unhealthy after null/control input: {health.status_code}"
