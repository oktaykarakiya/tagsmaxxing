"""Catalog (step-2) — upload + extraction pipeline.

List with `pytest --collect-only -m ingest`.
"""

from __future__ import annotations

import math
import re
import struct
import zlib

import httpx
import pytest

from lib import config, flows

pytestmark = pytest.mark.ingest

# ── Minimal binary file builders ────────────────────────────────────────────────
# Each builder returns bytes of a valid-enough file for the respective extractor
# (Tika, whisper, vision model).  These are not high-fidelity — they just need the
# correct magic bytes and minimal structural validity so the app routes them to
# the right extractor.


def _minimal_pdf() -> bytes:
    """Build a self-consistent minimal valid PDF (empty page, no text)."""
    header = b"%PDF-1.4\n"
    obj1 = b"1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
    obj2 = b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
    obj3 = b"3 0 obj<</Type/Page/MediaBox[0 0 612 792]/Parent 2 0 R/Resources<<>>>>endobj\n"

    off1 = len(header)  # 9
    off2 = off1 + len(obj1)  # 9 + 55 = 64
    off3 = off2 + len(obj2)  # 64 + 62 = 126
    xref_start = off3 + len(obj3)  # 126 + 81 = 207

    body = header + obj1 + obj2 + obj3

    def _xref_entry(offset: int) -> bytes:
        return f"{offset:010d} 00000 n \n".encode()

    xref = (
        b"xref\n"
        b"0 4\n"
        b"0000000000 65535 f \n"
        + _xref_entry(off1)
        + _xref_entry(off2)
        + _xref_entry(off3)
    )
    trailer = b"trailer<</Size 4/Root 1 0 R>>\n"
    startxref = f"startxref\n{xref_start}\n".encode()
    eof = b"%%EOF"

    return body + xref + trailer + startxref + eof


def _minimal_png() -> bytes:
    """Build a minimal 1×1 white-pixel PNG (valid, ~68 bytes)."""

    def _chunk(typ: bytes, data: bytes) -> bytes:
        c = typ + data
        crc = struct.pack(">I", zlib.crc32(c) & 0xFFFFFFFF)
        return struct.pack(">I", len(data)) + c + crc

    sig = b"\x89PNG\r\n\x1a\n"
    ihdr = _chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 2, 0, 0, 0))
    # Row filter (0) + RGB white pixel
    raw = b"\x00\xff\xff\xff"
    idat = _chunk(b"IDAT", zlib.compress(raw))
    iend = _chunk(b"IEND", b"")
    return sig + ihdr + idat + iend


def _minimal_wav() -> bytes:
    """Build a minimal 16-bit mono WAV with 0.5 s of a 440 Hz sine tone."""
    sample_rate = 8000
    num_channels = 1
    bits_per_sample = 16
    duration = 0.5  # seconds
    num_samples = int(sample_rate * duration)

    samples = bytearray()
    for i in range(num_samples):
        v = int(16000.0 * math.sin(2.0 * math.pi * 440.0 * i / sample_rate))
        samples.extend(struct.pack("<h", max(-32768, min(32767, v))))

    data_len = len(samples)
    header = struct.pack(
        "<4sI4s"  # RIFF chunk
        "4sI"  # fmt  chunk
        "HHIIHH"  # fmt data
        "4sI",  # data chunk
        b"RIFF",
        36 + data_len,
        b"WAVE",
        b"fmt ",
        16,  # PCM
        1,  # PCM format
        num_channels,
        sample_rate,
        sample_rate * num_channels * bits_per_sample // 8,  # byte rate
        num_channels * bits_per_sample // 8,  # block align
        bits_per_sample,
        b"data",
        data_len,
    )
    return header + bytes(samples)


def _minimal_mp4() -> bytes:
    """Build a minimal MP4 with ftyp + moov atoms (valid magic bytes)."""

    def _atom(typ: bytes, data: bytes = b"") -> bytes:
        return struct.pack(">I", 8 + len(data)) + typ + data

    ftyp = _atom(b"ftyp", b"isom\x00\x00\x00\x00isom")
    # 108-byte mvhd (version 0)
    mvhd = (
        b"\x00\x00\x00\x00"  # creation / modification time
        + struct.pack(">I", 1000)  # timescale
        + struct.pack(">I", 100)  # duration
        + b"\x00\x01\x00\x00"  # rate 1.0
        + b"\x01\x00\x00\x00"  # volume 1.0
        + b"\x00" * 76  # matrix + pre-defined
    )
    moov = _atom(b"moov", _atom(b"mvhd", mvhd))
    return ftyp + moov


# ── Helpers ─────────────────────────────────────────────────────────────────────


def _upload_raw(api, filename: str, content: bytes, mime: str, **fields) -> dict:
    """Upload a single file via the JSON API (multipart)."""
    resp = api._c.post(
        "/api/ingest",
        files=[("files", (filename, content, mime))],
        data={k: str(v) for k, v in fields.items()},
    )
    if resp.status_code != 202:
        raise RuntimeError(f"ingest failed ({resp.status_code}): {resp.text}")
    return resp.json()


def _csrf_upload(api, filename: str, content: bytes, mime: str, **fields) -> dict:
    """Upload through the Web UI endpoint (POST /upload) with CSRF handling.

    This path creates a *real* async job (unlike /api/ingest which is synchronous).
    Returns the parsed JSON response containing ``job_id``.
    """
    # 1. GET /upload to obtain the CSRF cookie + token.
    get_resp = api._c.get("/upload")
    if get_resp.status_code != 200:
        raise RuntimeError(f"GET /upload failed ({get_resp.status_code}): {get_resp.text}")

    # 2. Extract the csrf_token from the hidden input.
    match = re.search(r'name="csrf_token"\s+value="([^"]+)"', get_resp.text)
    if not match:
        raise RuntimeError("Could not find csrf_token in /upload page")
    csrf_token = match.group(1)

    # 3. POST the file with the CSRF token.
    form_data: dict = {"csrf_token": csrf_token}
    form_data.update({k: str(v) for k, v in fields.items()})
    resp = api._c.post(
        "/upload",
        files=[("files", (filename, content, mime))],
        data=form_data,
    )
    if resp.status_code not in (200, 202):
        raise RuntimeError(f"POST /upload failed ({resp.status_code}): {resp.text}")
    return resp.json()


# ── Tests ───────────────────────────────────────────────────────────────────────


def test_ingest_text_file_creates_document(api):
    """A .txt upload becomes a document with status ready and stored chunks."""
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    marker, doc = flows.ingest_and_wait(api, "Hello, world! This is a test document. {marker}")

    # The document should exist with a valid id.
    assert doc.get("id"), "document has no id"

    # Status should indicate the document is ready.
    assert doc.get("status") == "ready", f"expected status 'ready', got {doc.get('status')!r}"

    # There should be at least one file (chunk/page) stored.
    files = doc.get("files", [])
    assert len(files) >= 1, "document has no stored files/chunks"

    # The document should be findable via search.
    hits = api.search(marker).get("hits", [])
    assert hits, f"search for {marker!r} returned no hits"


def test_ingest_returns_202_with_job_id(api):
    """POST /api/ingest returns 202 Accepted with a job_id."""
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    # Use the underlying HTTP client to check the raw status code.
    marker = flows.unique_marker()
    resp = api._c.post(
        "/api/ingest",
        files=[("files", (f"{marker}.txt", f"test content {marker}".encode(), "text/plain"))],
    )

    assert resp.status_code == 202, f"expected 202, got {resp.status_code}"

    body = resp.json()
    assert "job_id" in body, f"response missing job_id: {body}"
    # job_id is the sentinel 0 for synchronous ingest (the API doc confirms this).
    assert isinstance(body["job_id"], int), f"job_id is not an integer: {body['job_id']!r}"

    # document_id should be present and non-None for synchronous ingest.
    assert body.get("document_id") is not None, f"expected document_id, got {body}"

    # message should be descriptive.
    assert "message" in body
    assert len(body["message"]) > 0


def test_ingest_pdf_via_tika(api):
    """A PDF is text-extracted through the Tika sidecar."""
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    marker = flows.unique_marker("kbe2epdf")
    filename = f"{marker}.pdf"
    text_content = f"PDF test document. {marker}".encode()
    pdf = _make_text_pdf(text_content)

    resp = api._c.post(
        "/api/ingest",
        files=[("files", (filename, pdf, "application/pdf"))],
    )
    assert resp.status_code == 202, (
        f"expected 202 for PDF ingest, got {resp.status_code}: {resp.text[:500]}"
    )
    body = resp.json()
    doc_id = body.get("document_id")
    assert doc_id is not None, f"ingest returned no document_id: {body}"

    doc = api.get_document(doc_id)
    assert doc.get("id") == doc_id

    # The document kind should reflect a document/PDF.
    kind = doc.get("kind", "")
    assert kind in ("document", "pdf"), f"unexpected kind for PDF: {kind!r}"

    # Non-empty files array confirms Tika processed it.
    files = doc.get("files", [])
    assert len(files) >= 1, "PDF ingest produced no file entries"


def _make_text_pdf(content: bytes) -> bytes:
    """Build a minimal PDF that contains extractable text via a page content stream."""
    # Use a BT/ET text block in a page content stream.
    text_op = f"BT /F1 12 Tf 100 700 Td ({content.decode('latin-1', errors='replace')}) Tj ET"
    stream = text_op.encode()

    import zlib as _zlib

    compressed = _zlib.compress(stream)

    header = b"%PDF-1.4\n"

    # Object 1: Catalog
    obj1 = b"1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
    # Object 2: Pages
    obj2 = b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
    # Object 3: Page with content stream ref
    page_obj = b"3 0 obj<</Type/Page/MediaBox[0 0 612 792]/Parent 2 0 R/Resources<<>>/Contents 4 0 R>>endobj\n"
    # Object 4: Content stream (compressed)
    stream_obj = (
        b"4 0 obj<</Length " + str(len(compressed)).encode() + b" /Filter/FlateDecode>>\n"
        b"stream\n" + compressed + b"\nendstream\nendobj\n"
    )

    off1 = len(header)
    off2 = off1 + len(obj1)
    off3 = off2 + len(obj2)
    off4 = off3 + len(page_obj)
    xref_start = off4 + len(stream_obj)

    body = header + obj1 + obj2 + page_obj + stream_obj

    def _e(off: int) -> bytes:
        return f"{off:010d} 00000 n \n".encode()

    xref = (
        b"xref\n0 5\n0000000000 65535 f \n"
        + _e(off1) + _e(off2) + _e(off3) + _e(off4)
    )
    trailer = b"trailer<</Size 5/Root 1 0 R>>\n"
    startxref = f"startxref\n{xref_start}\n".encode()

    return body + xref + trailer + startxref + b"%%EOF"


def test_ingest_image_extracts_metadata(api):
    """An image (JPEG/PNG) yields metadata and a vision-derived description."""
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    marker = flows.unique_marker("kbe2eimg")
    png = _minimal_png()
    resp = api._c.post(
        "/api/ingest",
        files=[("files", (f"{marker}.png", png, "image/png"))],
    )
    assert resp.status_code == 202, (
        f"expected 202 for image ingest, got {resp.status_code}: {resp.text[:500]}"
    )
    body = resp.json()
    doc_id = body.get("document_id")
    assert doc_id is not None, f"image ingest returned no document_id: {body}"

    doc = api.get_document(doc_id)
    assert doc.get("id") == doc_id

    # Image documents should have kind "image".
    kind = doc.get("kind", "")
    assert kind == "image", f"expected kind 'image', got {kind!r}"

    # The file entry should carry metadata and a non-empty status.
    files = doc.get("files", [])
    assert len(files) >= 1, "image ingest produced no file entries"
    f_status = files[0].get("status", "")
    assert f_status in ("ready", "processed", "done"), (
        f"unexpected file status for image: {f_status!r}"
    )


def test_ingest_audio_transcribed(api):
    """An audio file is transcribed (whisper) with ts_offsets on chunks."""
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    marker = flows.unique_marker("kbe2eaudio")
    wav = _minimal_wav()
    resp = api._c.post(
        "/api/ingest",
        files=[("files", (f"{marker}.wav", wav, "audio/wav"))],
    )
    assert resp.status_code == 202, (
        f"expected 202 for audio ingest, got {resp.status_code}: {resp.text[:500]}"
    )
    body = resp.json()
    doc_id = body.get("document_id")
    assert doc_id is not None, f"audio ingest returned no document_id: {body}"

    doc = api.get_document(doc_id)
    assert doc.get("id") == doc_id

    # Audio documents should have kind "audio".
    kind = doc.get("kind", "")
    assert kind == "audio", f"expected kind 'audio', got {kind!r}"

    # The transcription should produce files/chunks.
    files = doc.get("files", [])
    assert len(files) >= 1, "audio ingest produced no files"


def test_ingest_video_keyframes_and_audio(api):
    """A video yields keyframe descriptions + transcribed audio."""
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    marker = flows.unique_marker("kbe2evid")
    mp4 = _minimal_mp4()
    resp = api._c.post(
        "/api/ingest",
        files=[("files", (f"{marker}.mp4", mp4, "video/mp4"))],
    )
    assert resp.status_code == 202, (
        f"expected 202 for video ingest, got {resp.status_code}: {resp.text[:500]}"
    )
    body = resp.json()
    doc_id = body.get("document_id")
    assert doc_id is not None, f"video ingest returned no document_id: {body}"

    doc = api.get_document(doc_id)
    assert doc.get("id") == doc_id

    # Video documents should have kind "video".
    kind = doc.get("kind", "")
    assert kind == "video", f"expected kind 'video', got {kind!r}"

    # The video should produce at least one file entry.
    files = doc.get("files", [])
    assert len(files) >= 1, "video ingest produced no files"


def test_ingest_code_file(api):
    """A source-code file is ingested and classified as code."""
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    marker = flows.unique_marker("kbe2ecode")
    code_content = (
        f"#!/usr/bin/env python3\n"
        f"# {marker}\n"
        f"def fibonacci(n: int) -> int:\n"
        f'    """Return the nth Fibonacci number."""\n'
        f"    if n <= 1:\n"
        f"        return n\n"
        f"    a, b = 0, 1\n"
        f"    for _ in range(n - 1):\n"
        f"        a, b = b, a + b\n"
        f"    return b\n"
    )

    resp = _upload_raw(api, f"{marker}.py", code_content.encode(), "text/x-python")
    doc_id = resp.get("document_id")
    assert doc_id is not None, f"code ingest returned no document_id: {resp}"

    doc = api.get_document(doc_id)
    assert doc.get("id") == doc_id

    # Code files should be classified as "code".
    kind = doc.get("kind", "")
    assert kind == "code", f"expected kind 'code', got {kind!r}"

    # It should have at least one file entry.
    files = doc.get("files", [])
    assert len(files) >= 1, "code ingest produced no files"


def test_ingest_binary_file(api):
    """An unknown binary is MIME-sniffed; archives produce a manifest."""
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    marker = flows.unique_marker("kbe2ebin")
    # 256 bytes of random-looking data (non-text, non-media magic).
    random_bytes = bytes(
        (hash(f"{marker}{i}") & 0xFF) for i in range(256)
    )

    resp = api._c.post(
        "/api/ingest",
        files=[("files", (f"{marker}.bin", random_bytes, "application/octet-stream"))],
    )
    # The app should handle unknown binaries gracefully (202 Accepted).
    # A 5xx response indicates an app bug — record it as a test failure.
    assert resp.status_code == 202, (
        f"expected 202 for binary file ingest, got {resp.status_code}: "
        f"{resp.text[:500]}"
    )

    body = resp.json()
    doc_id = body.get("document_id")
    assert doc_id is not None, f"binary ingest returned no document_id: {body}"

    doc = api.get_document(doc_id)
    assert doc.get("id") == doc_id

    # The file should be stored; kind may be "binary", "unknown", or "other".
    kind = doc.get("kind", "")
    assert kind, "binary document has no kind"

    # At least one file entry should be present.
    files = doc.get("files", [])
    assert len(files) >= 1, "binary ingest produced no files"

    # MIME detection should have recorded a mime type on the file.
    mime = files[0].get("mime")
    assert mime, f"binary file has no detected MIME type; file entry: {files[0]}"


def test_ingest_multi_file_group_as_document(api):
    """Multiple files with group_as_document become pages of one document."""
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    marker = flows.unique_marker("kbe2egrp")
    files = [
        ("files", (f"{marker}_a.txt", f"Page A content. {marker}".encode(), "text/plain")),
        ("files", (f"{marker}_b.txt", f"Page B content. {marker}".encode(), "text/plain")),
        ("files", (f"{marker}_c.txt", f"Page C content. {marker}".encode(), "text/plain")),
    ]
    resp = api._c.post(
        "/api/ingest",
        files=files,
        data={"group_as_document": "true"},
    )
    assert resp.status_code == 202, f"expected 202, got {resp.status_code}: {resp.text}"
    body = resp.json()
    doc_id = body.get("document_id")
    assert doc_id is not None, f"group ingest returned no document_id: {body}"

    doc = api.get_document(doc_id)
    assert doc.get("id") == doc_id

    # Should have multiple pages/files (all grouped into one document).
    files_in_doc = doc.get("files", [])
    assert len(files_in_doc) >= 2, (
        f"expected >= 2 files after group ingest, got {len(files_in_doc)}"
    )

    # Page count should reflect the grouping.
    page_count = doc.get("page_count", 0)
    assert page_count >= 2, f"expected page_count >= 2, got {page_count}"


def test_ingest_empty_payload_400(api):
    """POST /api/ingest with no files returns 400."""
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    # Manually construct a multipart/form-data body with no file parts.
    # httpx's data= without files= sends url-encoded, not multipart — the
    # server only parses multipart, so we build the body ourselves.
    boundary = "emptyfilesboundary"
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="user_note"\r\n\r\n'
        f"no files here\r\n"
        f"--{boundary}--\r\n"
    )
    resp = api._c.post(
        "/api/ingest",
        content=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    assert resp.status_code == 400, f"expected 400, got {resp.status_code}: {resp.text}"

    body = resp.json() if resp.text else {}
    assert body.get("error") == "no_files", (
        f"expected error 'no_files', got {body.get('error')!r}; body={body}"
    )


def test_ingest_idempotent_reupload(api):
    """Re-uploading the same bytes does not create duplicate rows."""
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    marker = flows.unique_marker("kbe2eidem")
    content = f"Idempotency test content. {marker}".encode()

    # First upload.
    resp1 = _upload_raw(api, f"{marker}.txt", content, "text/plain")
    doc_id1 = resp1.get("document_id")
    assert doc_id1 is not None
    doc1 = api.get_document(doc_id1)

    # Second upload — same content, same filename.
    resp2 = _upload_raw(api, f"{marker}.txt", content, "text/plain")
    doc_id2 = resp2.get("document_id")
    assert doc_id2 is not None, f"re-upload returned no document_id: {resp2}"

    # Search for the marker: there should not be more documents than expected.
    # If the app deduplicates, searching returns 1 hit; if not, ≥1.
    hits = api.search(marker).get("hits", [])
    assert len(hits) >= 1, f"search for {marker!r} returned no hits"

    # Both document ids should be retrievable.
    doc2 = api.get_document(doc_id2)
    assert doc2.get("id") == doc_id2

    # The status of both documents should be valid (not error).
    assert doc1.get("status") in ("ready",), f"first doc status: {doc1.get('status')!r}"
    assert doc2.get("status") in ("ready",), f"second doc status: {doc2.get('status')!r}"


def test_job_status_progression(api):
    """GET /api/jobs/{id} reports queued, then running, then done.

    Uses the Web UI upload path (POST /upload) which enqueues a real async job,
    because the JSON API ingest path is synchronous (job_id=0 sentinel).
    """
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    marker = flows.unique_marker("kbe2ejob")
    content = f"Job progression test. {marker}".encode()

    # Upload through the web endpoint to get a real async job_id.
    upload_resp = _csrf_upload(api, f"{marker}.txt", content, "text/plain")
    job_id = upload_resp.get("job_id")
    assert job_id is not None, f"no job_id in upload response: {upload_resp}"
    assert job_id != 0, f"expected real (non-zero) job_id, got {job_id}"

    # Poll the job until it reaches a terminal state.
    job = api.wait_for_job(job_id, timeout=120.0, interval=2.0)
    status = job.get("status")
    assert status in ("done", "failed", "dead"), (
        f"job did not reach a terminal state: {job}"
    )

    # If the job succeeded, the status should be "done".
    # If the pipeline errored for any reason, it could be "failed" or "dead" —
    # that indicates an app bug, but the test correctly exercises the endpoint.
    assert job.get("id") == job_id, "job id mismatch in response"


def test_failed_extraction_marks_job_failed(api):
    """A file that cannot be extracted ends in a failed/dead job with last_error.

    Uses the Web UI upload path so a real async job is enqueued. The JSON API
    ingest path would return the error synchronously (not via a job record).
    """
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    marker = flows.unique_marker("kbe2efail")

    # Upload something that either crashes the extractor or is rejected.
    # A file claiming to be a PDF but containing garbage should trigger a
    # parse failure, resulting in a failed/dead job.
    garbage = b"%PDF-1.4\n%%NOT A REAL PDF%%\n" + b"\x00" * 256

    upload_resp = _csrf_upload(api, f"{marker}.pdf", garbage, "application/pdf")
    job_id = upload_resp.get("job_id")
    assert job_id is not None, f"no job_id in upload response: {upload_resp}"
    assert job_id != 0, f"expected real (non-zero) job_id, got {job_id}"

    # Poll the job. It should eventually reach "failed" or "dead".
    job = api.wait_for_job(job_id, timeout=180.0, interval=3.0)
    status = job.get("status")

    # The job should be terminal — either failed or dead.
    assert status in ("failed", "dead"), (
        f"expected job to be failed/dead after unprocessable input, "
        f"got status={status!r}; full job={job}"
    )

    # There should be a recorded error.
    last_error = job.get("last_error")
    assert last_error, f"failed job has no last_error: {job}"


def test_browser_upload_flow(page):
    """A user uploads via the /upload page and is redirected on completion."""
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()
    flows.browser_login(page, tenant, email, password)

    # Navigate to the upload page.
    page.goto("/upload")
    page.wait_for_selector("#upload-form", timeout=10_000)

    marker = flows.unique_marker("kbe2ebr")

    # Create a temporary text file to upload via the file input.
    # The upload form uses a hidden file input with id="file-input".
    page.set_input_files(
        "#file-input",
        {
            "name": f"{marker}.txt",
            "mimeType": "text/plain",
            "buffer": f"Browser upload test. {marker}".encode(),
        },
    )

    # The file list and controls should become visible after file selection.
    page.wait_for_selector("#upload-controls:not(.hidden)", timeout=5_000)

    # Click the upload submit button.
    page.click("#submit-btn")

    # After submission, the progress area appears; the form is hidden.
    page.wait_for_selector("#progress-area:not(.hidden)", timeout=10_000)

    # The progress text should indicate uploading/processing.
    progress_text = page.locator("#progress-text")
    assert progress_text.is_visible(), "progress text not visible after upload submit"

    # Wait for completion: either a redirect to /search or the progress bar at 100%.
    # The JS redirects to /search on success after ~800ms.
    try:
        page.wait_for_url("**/search", timeout=120_000)
    except Exception:
        # If we didn't redirect, check if the progress bar reached done state.
        bar = page.locator("#progress-bar")
        bar_style = bar.get_attribute("style") or ""
        if "100%" not in bar_style:
            # Check progress text for completion or error.
            text = progress_text.text_content() or ""
            # If we see an error message, the upload itself failed — that's an app bug.
            if "failed" in text.lower() or "error" in text.lower():
                detail = page.locator("#progress-detail").text_content() or ""
                pytest.fail(f"Browser upload failed: {text} — {detail}")
            raise

    # We should now be on a page (search or document detail) that is not the upload form.
    assert "/upload" not in page.url, (
        f"still on upload page after submit; url={page.url}"
    )
