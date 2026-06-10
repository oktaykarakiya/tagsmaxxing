"""Adversarial upload/ingest contract tests for ``POST /api/ingest``.

This module probes the *hostile-input boundary* of the ingest endpoint: the
extractor untrusted-bytes / prompt-injection surface (plan §17, §31.5). Each
test encodes the **intended** contract and FAILS (records a bug) when the app
violates it. The two contracts under test are:

1. **Content-based detection wins.** The server re-detects the MIME type from
   the leading magic bytes; the client-declared ``Content-Type`` and the file
   *extension* are advisory only. Executables (ELF/PE) and unknown
   ``octet-stream`` bytes are denied with a clean ``400 {"error": ...}``; a
   benign file's true kind is whatever the magic bytes say (a PNG declared as
   text is still routed as an image, etc.).

2. **Malformed / oversized / NUL-laced input is a clean 4xx, never a 500.**
   BUG-INGEST-06..12 (fixed) sanitize path-traversal filenames, truncate
   oversized ``user_note`` and strip NUL/control bytes from content — all of
   which must yield ``202`` (or, where the input is genuinely unacceptable, a
   clean ``400``). A ``500`` on hostile input is the error-mapping bug, so the
   assertions demand the intended 4xx so that regression surfaces.

Confirmed wire contract (``crates/api/src/handlers/ingest.rs``):
``invalid_upload`` (disallowed MIME / executable), ``no_files`` (no file part),
``invalid_multipart`` (un-parseable body). Every upload mints an
``X-Request-Id`` (echoed by the server) that is included in the failure message
for log correlation.
"""

from __future__ import annotations

import pytest

from lib import config, file_factory as ff, flows

pytestmark = [pytest.mark.ingest, pytest.mark.security]


# ── Shared helpers + a logged-in client fixture ──────────────────────────────
@pytest.fixture(scope="module")
def client():
    """A module-scoped, logged-in httpx client (the raw ``api._c``).

    Logging in once per module keeps the login-attempt count low and gives every
    test in this file an authenticated cookie jar to post uploads with. Yields
    the underlying ``httpx.Client`` accepted by :func:`file_factory.upload`.
    """
    from lib.api_client import ApiClient

    api = ApiClient(config.base_url())
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())
    try:
        yield api._c
    finally:
        api.close()


@pytest.fixture(autouse=True)
def _require_text_backend():
    """Skip when the text/embed backend is degraded (environment outage, not a bug).

    Tagger-dependent uploads return ``500 internal_error`` ("tagger failed") when
    the LLM backend is unavailable — an environment outage, not an input-handling
    defect — so the campaign skips rather than false-fails. (The *intended*
    behaviour, a graceful 503/degradation for text like the media path, is a
    separate recorded finding.) Reject-path cases re-run cleanly when healthy.
    """
    import httpx

    with httpx.Client(base_url=config.base_url(), verify=config.verify_tls(), timeout=10.0) as c:
        if not (ff.subsystem_ok(c, "text") and ff.subsystem_ok(c, "embed")):
            pytest.skip("text/embed backend degraded per /health — environment, not an app bug")


def _fail_msg(label: str, res: dict) -> str:
    """Build a failure message carrying the echoed X-Request-Id for log grep."""
    return (
        f"{label}: status={res['status']} request_id={res['request_id']} "
        f"body={res['body']!r} text={res['text']!r}"
    )


def _assert_clean_reject(res: dict, label: str, *, error_codes: tuple[str, ...] | None = None) -> None:
    """Assert *res* is a clean 400 (never a 500), optionally with a known error code.

    The intended contract for an unacceptable upload is ``400`` with a
    machine-readable ``error`` field. A ``500`` means the rejection was wrapped
    as ``internal_error`` (the error-mapping bug) — we assert the 400 so it
    surfaces. When *error_codes* is given, the body's ``error`` must be one of
    them.
    """
    assert res["status"] != 500, _fail_msg(f"{label}: hostile input wrapped as 500", res)
    assert res["status"] == 400, _fail_msg(f"{label}: expected 400", res)
    if error_codes is not None:
        body = res["body"] or {}
        assert body.get("error") in error_codes, _fail_msg(
            f"{label}: expected error in {error_codes}", res
        )


def _assert_accepted(res: dict, label: str) -> int:
    """Assert *res* is a 202 with a ``document_id``; return that id.

    Used by the cases whose hostile aspect must be *neutralised* (sanitized /
    stripped / truncated) and then ingested successfully.
    """
    assert res["status"] != 500, _fail_msg(f"{label}: input wrapped as 500", res)
    assert res["status"] == 202, _fail_msg(f"{label}: expected 202 accept", res)
    body = res["body"] or {}
    doc_id = body.get("document_id")
    assert doc_id is not None, _fail_msg(f"{label}: 202 but no document_id", res)
    return doc_id


def _assert_accepted_or_clean_reject(res: dict, label: str) -> None:
    """Assert *res* is either a 202 or a clean 400 — but never a 500/hang.

    For inputs at the edge of acceptability (e.g. control chars in the
    *filename*, an empty filename) the app may legitimately either sanitize and
    accept (202) or reject cleanly (400); the only forbidden outcomes are a 500
    and a transport-level failure (``status is None``: a hang/drop/timeout).
    """
    assert res["status"] is not None, _fail_msg(f"{label}: transport failure (hang/drop)", res)
    assert res["status"] != 500, _fail_msg(f"{label}: wrapped as 500", res)
    assert res["status"] in (202, 400), _fail_msg(f"{label}: expected 202 or 400", res)


def _doc_kind(client, doc_id: int) -> tuple[str, dict]:
    """Fetch a document via the API and return ``(kind, full_doc)``.

    Reuses the same authenticated client so the tenant scope matches the upload.
    """
    r = client.get(f"/api/documents/{doc_id}")
    assert r.status_code == 200, f"GET /api/documents/{doc_id} -> {r.status_code}: {r.text[:300]}"
    doc = r.json()
    return doc.get("kind", ""), doc


# ════════════════════════════════════════════════════════════════════════════
# 1–4. Executables / unknown bytes are denied (content-based detection)
# ════════════════════════════════════════════════════════════════════════════
def test_elf_executable_denied_400(client):
    """An ELF executable (declared octet-stream) is denied with a clean 400.

    Detected as ``application/x-executable`` → not in the MIME allow-list →
    ``invalid_upload``. A 500 here is the error-mapping bug.
    """
    rid = ff.new_request_id("elf")
    res = ff.upload(client, ("payload.bin", ff.elf_stub(), "application/octet-stream"), request_id=rid)
    _assert_clean_reject(res, "ELF executable", error_codes=("invalid_upload",))


def test_pe_mz_executable_denied_400(client):
    """A DOS/PE ``MZ`` stub is denied with a clean 400 (detected x-dosexec)."""
    rid = ff.new_request_id("pe")
    res = ff.upload(client, ("payload.bin", ff.pe_stub(), "application/octet-stream"), request_id=rid)
    _assert_clean_reject(res, "PE/MZ executable", error_codes=("invalid_upload",))


def test_polyglot_elf_under_zip_name_denied_400(client):
    """ELF bytes under a ``.zip`` filename are denied — detection is content-based.

    The extension says ``.zip`` but the magic bytes are ELF; the server must
    classify by content and deny the executable (not trust the extension).
    """
    content, filename = ff.polyglot_zip_named_but_elf()
    rid = ff.new_request_id("polyglot")
    res = ff.upload(client, (filename, content, "application/zip"), request_id=rid)
    _assert_clean_reject(res, "polyglot ELF-as-.zip", error_codes=("invalid_upload",))


def test_random_octet_stream_denied_400(client):
    """Random binary bytes (unknown magic) are denied with ``invalid_upload``."""
    blob = bytes((i * 31 + 7) & 0xFF for i in range(512))
    rid = ff.new_request_id("octet")
    res = ff.upload(client, ("blob.bin", blob, "application/octet-stream"), request_id=rid)
    _assert_clean_reject(res, "random octet-stream", error_codes=("invalid_upload",))


# ════════════════════════════════════════════════════════════════════════════
# 5–6. MIME spoofing — the declared Content-Type is ignored
# ════════════════════════════════════════════════════════════════════════════
def test_png_declared_as_text_is_detected_image(client):
    """PNG bytes declared ``text/plain`` are re-detected as ``image/png`` (kind image).

    The client-declared MIME is advisory; magic-byte detection wins, so a benign
    PNG is accepted and routed as an image regardless of the lying header.
    """
    rid = ff.new_request_id("png-as-text")
    res = ff.upload(client, ("not_really.txt", ff.minimal_png(), "text/plain"), request_id=rid)
    doc_id = _assert_accepted(res, "PNG declared text/plain")
    kind, _ = _doc_kind(client, doc_id)
    assert kind == "image", f"expected kind 'image' for PNG-as-text (rid={rid}), got {kind!r}"


def test_text_declared_as_png_is_detected_text(client):
    """Plain-text bytes declared ``image/png`` are re-detected as text (kind document).

    The reverse spoof: a text body with a lying ``image/png`` header is detected
    as ``text/plain`` and routed to the document/text pipeline, not the image one.
    """
    marker = flows.unique_marker("kbe2espoof")
    body = f"This is honestly just text. {marker}".encode()
    rid = ff.new_request_id("text-as-png")
    res = ff.upload(client, ("liar.png", body, "image/png"), request_id=rid)
    doc_id = _assert_accepted(res, "text declared image/png")
    kind, _ = _doc_kind(client, doc_id)
    assert kind in ("document", "text"), (
        f"expected kind document/text for text-as-PNG (rid={rid}), got {kind!r}"
    )


# ════════════════════════════════════════════════════════════════════════════
# 7–10. Sanitization: traversal filename, control chars, NUL content, big note
# ════════════════════════════════════════════════════════════════════════════
def _iter_strings(value):
    """Yield every string found in a nested JSON value (dict/list/str)."""
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for v in value.values():
            yield from _iter_strings(v)
    elif isinstance(value, list):
        for v in value:
            yield from _iter_strings(v)


def test_path_traversal_filename_rejected_400(client):
    """A ``../../../../etc/passwd`` filename is rejected with a clean 400.

    Verified against the running app: rather than silently sanitizing to a
    basename, the upload validator refuses path-traversal/unsafe filenames
    outright (``invalid_upload``: "contains path-traversal or unsafe characters")
    — a defense-in-depth choice. The traversal path therefore never reaches the
    pipeline or any extractor's temp-file handling. A 500 here would be the
    error-mapping bug; a 202 would mean the guard is gone.
    """
    marker = flows.unique_marker("kbe2etrav")
    rid = ff.new_request_id("traversal")
    res = ff.upload(
        client,
        ("../../../../etc/passwd", f"traversal content {marker}".encode(), "text/plain"),
        request_id=rid,
    )
    _assert_clean_reject(res, "path-traversal filename", error_codes=("invalid_upload",))


def test_control_chars_in_filename_no_500(client):
    """NUL/control characters in the *filename* yield 202 or a clean 400 — never 500.

    A multipart header cannot carry a raw NUL, so the control bytes are embedded
    via httpx's filename encoding; the app must sanitize-and-accept or reject
    cleanly, but never wrap the malformed name as an internal error.
    """
    marker = flows.unique_marker("kbe2ectlfn")
    nasty_name = "re\x01port\x07\x1f.txt"
    rid = ff.new_request_id("ctl-filename")
    res = ff.upload(
        client,
        (nasty_name, f"clean body {marker}".encode(), "text/plain"),
        request_id=rid,
    )
    _assert_accepted_or_clean_reject(res, "control chars in filename")


def test_nul_control_chars_in_content_no_500(client):
    """NUL + control bytes in text *content* yield a clean outcome — never 500.

    Observed against the running app: content with an early NUL is detected as
    ``application/octet-stream`` and cleanly **rejected** (``400 invalid_upload``)
    at the MIME gate — so the BUG-INGEST-11 contract ("NUL/control bytes never
    cause a 500") holds. The app refuses such content as binary rather than
    stripping-and-ingesting; either is acceptable, a 500 is not. If a variant is
    instead accepted (202), the document must be retrievable.
    """
    marker = flows.unique_marker("kbe2enulc")
    rid = ff.new_request_id("nul-content")
    res = ff.upload(
        client,
        (f"{marker}.txt", ff.nul_control_text(marker), "text/plain"),
        request_id=rid,
    )
    _assert_accepted_or_clean_reject(res, "NUL/control chars in content")
    if res["status"] == 202:
        _, doc = _doc_kind(client, (res["body"] or {})["document_id"])
        assert doc.get("id"), f"accepted NUL-content doc not retrievable (rid={rid})"


def test_oversized_user_note_truncated_202(client):
    """A ``user_note`` larger than the 10 KiB cap is truncated (BUG-INGEST-10); 202, never 500."""
    marker = flows.unique_marker("kbe2enote")
    big_note = "N" * (ff.MAX_USER_NOTE_BYTES + 20 * ff.KIB)
    rid = ff.new_request_id("big-note")
    res = ff.upload(
        client,
        (f"{marker}.txt", f"note overflow body {marker}".encode(), "text/plain"),
        data={"user_note": big_note},
        request_id=rid,
    )
    _assert_accepted(res, "oversized user_note")


# ════════════════════════════════════════════════════════════════════════════
# 11–13. Malformed / mis-shaped multipart bodies
# ════════════════════════════════════════════════════════════════════════════
def test_malformed_multipart_body_400(client):
    """A multipart body whose payload boundary does not match the header boundary -> 400.

    The ``Content-Type`` declares one boundary while the body delimits parts with
    a *different* one, so no parts are parseable. The app must answer 400
    (``invalid_multipart`` or ``no_files``) and must not 500 or hang.
    """
    declared_boundary = "kbAdvDECLARED"
    wrong_boundary = "kbAdvACTUALLYdifferent"
    body = (
        f"--{wrong_boundary}\r\n"
        'Content-Disposition: form-data; name="files"; filename="x.txt"\r\n'
        "Content-Type: text/plain\r\n\r\n"
        "hello\r\n"
        f"--{wrong_boundary}--\r\n"
    ).encode()
    rid = ff.new_request_id("bad-multipart")
    try:
        r = client.post(
            "/api/ingest",
            content=body,
            headers={
                "Content-Type": f"multipart/form-data; boundary={declared_boundary}",
                "X-Request-Id": rid,
            },
        )
    except Exception as exc:  # noqa: BLE001 — a hang/drop surfaces as a transport error
        pytest.fail(f"malformed multipart caused a transport failure (rid={rid}): {exc!r}")
    assert r.status_code != 500, (
        f"malformed multipart wrapped as 500 (rid={rid}): {r.text[:300]}"
    )
    assert r.status_code == 400, (
        f"expected 400 for malformed multipart (rid={rid}), got {r.status_code}: {r.text[:300]}"
    )
    body_json = r.json() if r.text else {}
    assert body_json.get("error") in ("invalid_multipart", "no_files"), (
        f"expected invalid_multipart/no_files (rid={rid}), got {body_json.get('error')!r}: {body_json}"
    )


def test_missing_file_part_only_note_400(client):
    """A multipart body with only a ``user_note`` field (no file part) -> 400 no_files."""
    boundary = "kbAdvNOFILE"
    body = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="user_note"\r\n\r\n'
        "a note but no file\r\n"
        f"--{boundary}--\r\n"
    ).encode()
    rid = ff.new_request_id("no-file-part")
    r = client.post(
        "/api/ingest",
        content=body,
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "X-Request-Id": rid,
        },
    )
    assert r.status_code != 500, f"missing file part wrapped as 500 (rid={rid}): {r.text[:300]}"
    assert r.status_code == 400, (
        f"expected 400 for missing file part (rid={rid}), got {r.status_code}: {r.text[:300]}"
    )
    body_json = r.json() if r.text else {}
    assert body_json.get("error") == "no_files", (
        f"expected error 'no_files' (rid={rid}), got {body_json.get('error')!r}: {body_json}"
    )


def test_wrong_field_name_for_file_400(client):
    """A file uploaded under field ``file`` (not ``files``) is rejected -> 400, never 500.

    The handler only collects parts named ``files``; a mis-named part yields no
    files, so the intended outcome is a clean ``no_files`` 400.
    """
    boundary = "kbAdvWRONGFIELD"
    body = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="file"; filename="x.txt"\r\n'
        "Content-Type: text/plain\r\n\r\n"
        "wrong field name\r\n"
        f"--{boundary}--\r\n"
    ).encode()
    rid = ff.new_request_id("wrong-field")
    r = client.post(
        "/api/ingest",
        content=body,
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "X-Request-Id": rid,
        },
    )
    assert r.status_code != 500, f"wrong field name wrapped as 500 (rid={rid}): {r.text[:300]}"
    assert r.status_code == 400, (
        f"expected 400 for wrong field name (rid={rid}), got {r.status_code}: {r.text[:300]}"
    )
    body_json = r.json() if r.text else {}
    assert body_json.get("error") == "no_files", (
        f"expected error 'no_files' (rid={rid}), got {body_json.get('error')!r}: {body_json}"
    )


# ════════════════════════════════════════════════════════════════════════════
# 14–17. Archive bomb, double extension, content-type lie, empty filename
# ════════════════════════════════════════════════════════════════════════════
def test_zip_with_many_entries_completes_202(client):
    """A ZIP with >10k entries ingests (202) with a truncated listing — no crash/hang.

    The archive-entry cap (10k) must truncate the listing rather than crash or
    hang; a generous timeout guards against a pathological full-enumeration. A
    500 or a transport timeout is the bug.
    """
    rid = ff.new_request_id("zip-bomb")
    res = ff.upload(
        client,
        (f"{rid}.zip", ff.zip_bomb_entries(20_000), "application/zip"),
        request_id=rid,
        timeout=180.0,
    )
    assert res["status"] is not None, _fail_msg("20k-entry ZIP: transport failure (hang)", res)
    _assert_accepted(res, "20k-entry ZIP")


def test_double_extension_text_detected_text_202(client):
    """``report.txt.exe`` carrying TEXT bytes is detected as text -> 202 (extension not authoritative).

    The trailing ``.exe`` must not make the app treat benign text as an
    executable; magic-byte detection sees text and accepts it.
    """
    marker = flows.unique_marker("kbe2edblext")
    rid = ff.new_request_id("double-ext")
    res = ff.upload(
        client,
        ("report.txt.exe", f"just text inside {marker}".encode(), "text/plain"),
        request_id=rid,
    )
    doc_id = _assert_accepted(res, "double-extension report.txt.exe")
    kind, _ = _doc_kind(client, doc_id)
    assert kind in ("document", "text"), (
        f"expected kind document/text for .txt.exe text (rid={rid}), got {kind!r}"
    )


def test_docx_declared_as_text_detected_zip_202(client):
    """A real .docx (zip) declared ``text/plain`` is detected as zip -> Tika -> 202 (kind document).

    The OOXML container's PK magic wins over the lying ``text/plain`` header; the
    file routes through the archive/Tika path and produces a document.
    """
    marker = flows.unique_marker("kbe2edocxlie")
    rid = ff.new_request_id("docx-as-text")
    res = ff.upload(
        client,
        (f"{marker}.txt", ff.minimal_docx(marker), "text/plain"),
        request_id=rid,
    )
    doc_id = _assert_accepted(res, "docx declared text/plain")
    kind, _ = _doc_kind(client, doc_id)
    assert kind in ("document", "docx", "archive"), (
        f"expected document/docx/archive for docx-as-text (rid={rid}), got {kind!r}"
    )


def test_empty_filename_no_500(client):
    """An empty filename ("") with valid text bytes -> 202 or clean 400 — never 500."""
    marker = flows.unique_marker("kbe2eemptyfn")
    rid = ff.new_request_id("empty-filename")
    res = ff.upload(
        client,
        ("", f"body with empty filename {marker}".encode(), "text/plain"),
        request_id=rid,
    )
    _assert_accepted_or_clean_reject(res, "empty filename")


@pytest.mark.skip(
    reason="F8: audio.rs ffmpeg stdin/stdout pipe deadlock — any real audio (>~2 s) hangs the "
    "ingest indefinitely AND leaks the inflight permit, which would wedge the stack for the rest "
    "of the run. See docs/analysis/08. Un-skip once audio.rs drains stdout concurrently (or uses a "
    "temp file like video.rs)."
)
def test_realistic_audio_transcribes_no_deadlock(client):
    """A real (>2 s) audio file should transcribe + ingest (202) — currently DEADLOCKS (F8).

    Regression marker for the audio-extractor pipe deadlock: a ~10 s WAV transcodes to a >64 KiB
    16 kHz mono stream that fills ffmpeg's stdout pipe while the app is still writing stdin
    (``crates/extract/src/audio.rs:137``), so the upload never returns. Verified manually with
    ``whisper.cpp/samples/jfk.wav`` (352 KB) → hung >200 s while whisper-direct transcribed it in
    ~13 s. When fixed, un-skip and assert ``202`` with the transcript searchable.
    """
    rid = ff.new_request_id("realaudio")
    res = ff.upload(client, (f"{rid}.wav", ff.minimal_wav(10.0), "audio/wav"), request_id=rid, timeout=60)
    _assert_accepted(res, "realistic 10s audio (F8 deadlock)")
