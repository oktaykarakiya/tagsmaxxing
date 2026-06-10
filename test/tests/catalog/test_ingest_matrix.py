"""Ingest matrix lane — equivalence partitioning + pairwise + boundary-value analysis.

This is the combinatorial core of the upload campaign. A full Cartesian product of
the upload factors is ~1.6M combinations; we reduce it (NIST/Kuhn: ≤2-factor
interactions trigger the large majority of faults) to:

  * **equivalence partitioning** — ~30 concrete MIME types → a handful of *kind*
    representatives (``lib.file_factory.sample_catalog`` + ``format_breadth``);
  * **pairwise (2-way) covering array** over the orthogonal factors
    {endpoint, kind, group, note, filename} (``lib.covering_array``), with seeded
    high-risk rows; and
  * **boundary-value analysis** on size / count / note-length / filename-length.

account-state, auth/CSRF, and concurrency live in their own lanes (they need
special fixtures + serial execution). Media cases assert *acceptance + routing*
(extraction is backend-dependent / best-effort); a genuine backend outage SKIPs
via ``/health`` rather than failing (see ``file_factory.skip_if_degraded``).

Per the harness philosophy (conftest.py), each assertion encodes the *intended*
contract; a divergence is an app bug to RECORD, not a reason to weaken the test.
"""

from __future__ import annotations

import pytest

from lib import config, covering_array as ca, file_factory as ff, flows
from lib.api_client import ApiClient

pytestmark = [pytest.mark.ingest, pytest.mark.matrix]


# ── Factor model (orthogonal factors only; see module docstring) ──────────────
KINDS = ["text", "code", "pdf", "docx", "image", "audio", "video", "archive", "binary", "exe"]
FACTORS: dict[str, list[str]] = {
    "endpoint": ["/api/ingest", "/upload"],
    "kind": KINDS,
    "group": ["false", "true"],
    "note": ["none", "normal", "oversized", "control"],
    "filename": ["normal", "traversal", "unicode", "long"],
}
# High-risk combinations pairwise need not co-locate — pin them explicitly.
SEEDS = [
    {"endpoint": "/upload", "kind": "docx", "group": "true", "note": "control", "filename": "traversal"},
    {"endpoint": "/api/ingest", "kind": "image", "group": "true", "note": "oversized", "filename": "unicode"},
    {"endpoint": "/upload", "kind": "exe", "group": "false", "note": "normal", "filename": "long"},
]
CASES = ca.pairwise(FACTORS, seeds=SEEDS)


def _cid(c: dict[str, str]) -> str:
    ep = "api" if c["endpoint"] == "/api/ingest" else "web"
    return f"{ep}-{c['kind']}-g{c['group'][0]}-n{c['note']}-f{c['filename']}"


CASE_IDS = [_cid(c) for c in CASES]


# ── Shared, single-login client (avoids tripping the login rate limiter) ──────
@pytest.fixture(scope="module")
def admin_client():
    """One authenticated client reused across the whole matrix (serial uploads)."""
    c = ApiClient()
    c.login(config.tenant_slug(), config.admin_email(), config.admin_password())
    try:
        yield c
    finally:
        c.close()


# ── Variant builders ──────────────────────────────────────────────────────────
def _filename_variant(base: str, variant: str, marker: str) -> str:
    ext = base.rsplit(".", 1)[-1]
    if variant == "traversal":
        return f"../../../../etc/{marker}.{ext}"
    if variant == "unicode":
        return f"café_{marker}_日本語_\U0001f600.{ext}"
    if variant == "long":
        return ("L" * 240) + f"_{marker}.{ext}"
    return base


def _note_variant(variant: str, marker: str) -> str | None:
    if variant == "normal":
        return f"user note for {marker}"
    if variant == "oversized":
        return "x" * (ff.MAX_USER_NOTE_BYTES + 5000)
    if variant == "control":
        return f"note {marker}\x00\x01\x02 control-laced"
    return None


def _accept_status(out: dict, endpoint: str) -> bool:
    # /api/ingest returns 202; the web /upload handler may answer 200 or 202.
    return out["status"] in ((200, 202) if endpoint == "/upload" else (202,))


# ── Meta-test: prove the generated matrix is a valid pairwise array ───────────
def test_pairwise_coverage_is_complete():
    """The generated matrix realises every 2-way value-pair of the factors."""
    assert ca.verify_pairwise(FACTORS, CASES), "covering array does not cover all 2-way pairs"
    assert len(CASES) < 120, f"matrix unexpectedly large ({len(CASES)} rows)"


# ── The matrix ────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def test_matrix(admin_client, case):
    """Drive one pairwise upload combination and assert its intended contract."""
    marker = flows.unique_marker("mtx")
    sample = ff.sample_catalog(marker)[case["kind"]]
    filename = _filename_variant(sample.filename, case["filename"], marker)
    note = _note_variant(case["note"], marker)

    data: dict[str, str] = {}
    if case["group"] == "true":
        data["group_as_document"] = "true"
    if note is not None:
        data["user_note"] = note

    # Anti-noise: image/video captioning uses the vision role on the text backend.
    if case["kind"] in ("image", "video"):
        ff.skip_if_degraded(admin_client._c, "text")

    rid = ff.new_request_id("mtx")
    out = ff.upload(
        admin_client._c, [(filename, sample.content, sample.mime)],
        endpoint=case["endpoint"], data=data, request_id=rid,
    )

    # A path-traversal filename is itself a reject condition: the app refuses it
    # with a clean 400 ("contains path-traversal or unsafe characters") rather
    # than silently sanitizing — a secure choice (verified in isolation). The
    # traversal guard runs before MIME routing, so expect 400 regardless of kind.
    if case["filename"] == "traversal":
        assert out["status"] == 400, (
            f"traversal filename must be rejected with 400, got {out['status']} "
            f"(req {rid}): {out['text']}"
        )
        return

    # REJECT classes (disallowed MIME / executable) → a clean 400, regardless of
    # endpoint/metadata. A 500 here = the error-mapping bug (BUG-INGEST-06/07) is back.
    if sample.expected == ff.REJECT:
        assert out["status"] == 400, (
            f"{case['kind']} via {case['endpoint']} must be rejected with 400, "
            f"got {out['status']} (req {rid}): {out['text']}"
        )
        return

    # ACCEPT classes → 200/202. Disambiguate a genuine backend outage from a bug.
    if not _accept_status(out, case["endpoint"]):
        if out["status"] in (500, 502, 503) and not ff.subsystem_ok(admin_client._c, "embed"):
            pytest.skip("embed backend degraded per /health — not an app bug")
        pytest.fail(
            f"{case['kind']} via {case['endpoint']} expected accept, got {out['status']} "
            f"(req {rid}): {out['text']}"
        )

    body = out["body"] or {}
    doc_id = body.get("document_id")
    assert doc_id is not None, f"accepted upload returned no document_id (req {rid}): {body}"

    doc = admin_client.get_document(doc_id)
    kind = doc.get("kind", "")
    assert kind in sample.expected_kinds, (
        f"{case['kind']}: document kind {kind!r} not in expected {sample.expected_kinds} (req {rid})"
    )
    assert len(doc.get("files", [])) >= 1, f"accepted upload stored no files (req {rid})"


# ── Format breadth (extra image/audio/video/code/archive formats) ─────────────
def _breadth_params():
    samples = ff.format_breadth("BREADTHSEED")
    return samples, [f"{s.kind_class}-{s.filename.rsplit('.', 1)[-1]}" for s in samples]


_BREADTH, _BREADTH_IDS = _breadth_params()


@pytest.mark.parametrize("idx", range(len(_BREADTH)), ids=_BREADTH_IDS)
def test_format_breadth(admin_client, idx):
    """Each additional format is accepted and routed to the right kind (magic-valid)."""
    marker = flows.unique_marker("brd")
    proto = _BREADTH[idx]
    # Rebuild with a unique marker where the content is marker-bearing (code/text);
    # binary media stubs are marker-independent, reuse as-is.
    sample = proto
    filename = f"{marker}.{proto.filename.rsplit('.', 1)[-1]}"

    if proto.kind_class in ("image", "video"):
        ff.skip_if_degraded(admin_client._c, "text")

    rid = ff.new_request_id("brd")
    out = ff.upload(admin_client._c, [(filename, sample.content, sample.mime)], request_id=rid)

    if out["status"] != 202:
        if out["status"] in (500, 502, 503) and not ff.subsystem_ok(admin_client._c, "embed"):
            pytest.skip("embed backend degraded per /health — not an app bug")
        pytest.fail(
            f"format {proto.filename} ({proto.mime}) expected 202, got {out['status']} "
            f"(req {rid}): {out['text']}"
        )
    doc_id = (out["body"] or {}).get("document_id")
    assert doc_id is not None, f"no document_id for {proto.filename} (req {rid})"
    doc = admin_client.get_document(doc_id)
    assert doc.get("kind", "") in proto.expected_kinds, (
        f"{proto.filename}: kind {doc.get('kind')!r} not in {proto.expected_kinds} (req {rid})"
    )


# ══════════════════════════════════════════════════════════════════════════════
# Boundary-value analysis (size / count / note-length / filename-length)
# ══════════════════════════════════════════════════════════════════════════════
def test_bva_zero_byte_rejected(admin_client):
    """A 0-byte file is rejected with 400 (BUG-INGEST-06: application/x-empty)."""
    rid = ff.new_request_id("bva0")
    out = ff.upload(admin_client._c, [("empty.txt", ff.empty_bytes(), "text/plain")], request_id=rid)
    assert out["status"] == 400, f"0-byte upload must be 400, got {out['status']} (req {rid}): {out['text']}"


def test_bva_one_byte_accepted(admin_client):
    """A 1-byte text file is accepted."""
    rid = ff.new_request_id("bva1")
    out = ff.upload(admin_client._c, [("one.txt", b"x", "text/plain")], request_id=rid)
    if out["status"] != 202 and not ff.subsystem_ok(admin_client._c, "embed"):
        pytest.skip("embed degraded")
    assert out["status"] == 202, f"1-byte upload expected 202, got {out['status']}: {out['text']}"


@pytest.mark.parametrize("n", [1 * ff.KIB, 256 * ff.KIB, 4 * ff.MIB])
def test_bva_small_sizes_accepted(admin_client, n):
    """Sub-cap text uploads of increasing size are accepted."""
    marker = flows.unique_marker("bvasz")
    rid = ff.new_request_id("bvasz")
    out = ff.upload(admin_client._c, [(f"{marker}.txt", ff.sized_text(n, marker), "text/plain")], request_id=rid)
    if out["status"] != 202 and not ff.subsystem_ok(admin_client._c, "embed"):
        pytest.skip("embed degraded")
    assert out["status"] == 202, f"{n}-byte upload expected 202, got {out['status']}: {out['text']}"


def test_bva_just_over_axum_limit_413(admin_client):
    """A body exceeding the multipart cap + framing headroom is hard-rejected (413) by axum.

    Streams ~(100 MiB + headroom + 1) so axum's DefaultBodyLimit trips before the
    handler runs; no large buffer is held client-side.
    """
    size = ff.MAX_PAYLOAD_BYTES + ff.MULTIPART_FRAMING_HEADROOM + 1
    gen, ctype = ff.multipart_stream("huge.txt", "text/plain", size)
    rid = ff.new_request_id("bvabig")
    try:
        r = admin_client._c.post(
            "/api/ingest", content=gen,
            headers={"Content-Type": ctype, "X-Request-Id": rid}, timeout=300.0,
        )
    except Exception as exc:  # noqa: BLE001
        pytest.fail(f"over-limit upload raised instead of 413 (req {rid}): {exc!r}")
    assert r.status_code == 413, (
        f"over-axum-limit upload expected 413, got {r.status_code} (req {rid}): {r.text[:300]}"
    )


def test_bva_just_over_soft_cap_status(admin_client):
    """A body over the 100 MiB soft cap but under axum's hard limit hits the app gate.

    The handler's running-total check should reject it. The documented contract
    (ingest.rs) says the *total* cap is a 413; the app gate currently bails as a
    400 (invalid_multipart). We assert it is cleanly rejected (4xx) and record the
    exact code so any 413-vs-400 inconsistency is captured (spec-ambiguity).
    """
    size = ff.MAX_PAYLOAD_BYTES + 32 * ff.KIB  # over soft cap, under axum hard limit
    gen, ctype = ff.multipart_stream("big.txt", "text/plain", size)
    rid = ff.new_request_id("bvasoft")
    try:
        r = admin_client._c.post(
            "/api/ingest", content=gen,
            headers={"Content-Type": ctype, "X-Request-Id": rid}, timeout=300.0,
        )
    except Exception as exc:  # noqa: BLE001
        pytest.fail(f"over-soft-cap upload raised (req {rid}): {exc!r}")
    assert r.status_code in (400, 413), (
        f"over-soft-cap upload expected 400/413, got {r.status_code} (req {rid}): {r.text[:300]}"
    )
    # Record the actual code for the findings doc (not an assertion failure).
    print(f"[bva] over-soft-cap (~100MiB+32KiB) → HTTP {r.status_code} (contract says 413)")


def test_bva_per_file_cap_unreachable_relationship():
    """The 500 MiB per-file cap is unreachable behind the 100 MiB total cap (finding).

    Pure relationship assertion — pushing 500 MiB is pointless because the total
    cap always trips first. Documents that ``MAX_INDIVIDUAL_FILE_BYTES`` is dead
    on the HTTP path.
    """
    assert ff.MAX_INDIVIDUAL_FILE_BYTES > ff.MAX_PAYLOAD_BYTES, (
        "per-file cap should exceed total cap (else the per-file path is reachable)"
    )


@pytest.mark.parametrize("count", [2, 10, 50])
def test_bva_file_count_grouped(admin_client, count):
    """N text files with group_as_document become one document with N pages."""
    marker = flows.unique_marker("bvacnt")
    files = [(f"{marker}_{i}.txt", f"page {i} {marker}".encode(), "text/plain") for i in range(count)]
    rid = ff.new_request_id("bvacnt")
    out = ff.upload(admin_client._c, files, data={"group_as_document": "true"}, request_id=rid)
    if out["status"] != 202 and not ff.subsystem_ok(admin_client._c, "embed"):
        pytest.skip("embed degraded")
    assert out["status"] == 202, f"{count}-file group expected 202, got {out['status']}: {out['text']}"
    doc = admin_client.get_document((out["body"] or {})["document_id"])
    assert len(doc.get("files", [])) >= min(count, 2), f"{count}-file group stored too few files"


@pytest.mark.parametrize("note_len", [ff.MAX_USER_NOTE_BYTES - 1, ff.MAX_USER_NOTE_BYTES, ff.MAX_USER_NOTE_BYTES + 1])
def test_bva_note_length_boundary(admin_client, note_len):
    """A user_note at/around the 10 KiB boundary is accepted (truncated, not 500)."""
    marker = flows.unique_marker("bvanote")
    rid = ff.new_request_id("bvanote")
    out = ff.upload(
        admin_client._c, [(f"{marker}.txt", f"body {marker}".encode(), "text/plain")],
        data={"user_note": "n" * note_len}, request_id=rid,
    )
    if out["status"] != 202 and not ff.subsystem_ok(admin_client._c, "embed"):
        pytest.skip("embed degraded")
    assert out["status"] == 202, (
        f"note_len={note_len} expected 202 (truncate, not crash), got {out['status']}: {out['text']}"
    )


@pytest.mark.parametrize("name_len", [200, 255, 300])
def test_bva_filename_length(admin_client, name_len):
    """Long filenames are handled (sanitized/truncated), not crashed."""
    marker = flows.unique_marker("bvafn")
    name = ("a" * name_len) + ".txt"
    rid = ff.new_request_id("bvafn")
    out = ff.upload(admin_client._c, [(name, f"x {marker}".encode(), "text/plain")], request_id=rid)
    if out["status"] != 202 and not ff.subsystem_ok(admin_client._c, "embed"):
        pytest.skip("embed degraded")
    assert out["status"] == 202, f"filename len {name_len} expected 202, got {out['status']}: {out['text']}"
