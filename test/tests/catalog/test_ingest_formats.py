"""Comprehensive file-format coverage — every allow-listed + denied MIME family.

The core matrix + breadth (``test_ingest_matrix``) cover the common kinds; this lane
closes the gap so **every family in the app's MIME allow/deny lists**
(`crates/extract/src/security.rs`) has a test:

* allow-listed extras — avif/heic/bmp (image), m4a (audio), mov/avi/3gpp (video),
  7z/bz2/xz/zstd (archive), pptx/xlsx/odt/rtf/legacy-doc (office);
* denied — elf/pe/jar/mach-o/octet-stream → must be ``400``.

Contract asserted:
* **Denied families → clean ``400``** (backend-independent; never ``500``).
* **Allow-listed families are handled gracefully** — never ``500``/hang; accepted
  (``202`` with the right kind family) when the magic is recognised, or a clean
  ``400`` when the *minimal hand-built stub* isn't recognised by tree_magic (a
  fixture limitation, recorded, not a crash). The run prints the accept/reject
  breakdown so unrecognised stubs are visible.

Media/doc accept-paths need the tagger; a degraded backend SKIPs (anti-noise),
per the suite philosophy. Built samples come from ``lib.file_factory``.
"""

from __future__ import annotations

import pytest

from lib import config, file_factory as ff, flows
from lib.api_client import ApiClient

pytestmark = [pytest.mark.ingest, pytest.mark.matrix]


@pytest.fixture(scope="module")
def client():
    """One authenticated client for the whole format sweep (serial uploads)."""
    c = ApiClient()
    c.login(config.tenant_slug(), config.admin_email(), config.admin_password())
    try:
        yield c
    finally:
        c.close()


# ── Denied families: every one must be a clean 400 (no backend needed) ────────
_DENIED = ff.denied_formats("DENYSEED")
_DENIED_IDS = [f"{s.filename.rsplit('.', 1)[-1]}" for s in _DENIED]


@pytest.mark.security
@pytest.mark.parametrize("proto", _DENIED, ids=_DENIED_IDS)
def test_denied_format_rejected_400(client, proto):
    """Each denied family (exe/pe/jar/mach-o/octet-stream) is rejected with 400, never 500."""
    marker = flows.unique_marker("deny")
    fn = f"{marker}.{proto.filename.rsplit('.', 1)[-1]}"
    rid = ff.new_request_id("deny")
    out = ff.upload(client._c, [(fn, proto.content, proto.mime)], request_id=rid)
    assert out["status"] == 400, (
        f"denied family {proto.filename} must be 400, got {out['status']} (req {rid}): {out['text']}"
    )
    assert (out["body"] or {}).get("error") in ("invalid_upload", "no_files"), (
        f"denied family {proto.filename}: unexpected error body (req {rid}): {out['body']}"
    )


# ── Allow-listed extras: handled gracefully (202+kind, or clean 400; never 500) ─
_ACCEPT = ff.extended_accept_formats("FMTSEED")
_ACCEPT_IDS = [f"{s.kind_class}-{s.filename.rsplit('.', 1)[-1]}" for s in _ACCEPT]


@pytest.mark.parametrize("proto", _ACCEPT, ids=_ACCEPT_IDS)
def test_allowlisted_format_handled(client, proto):
    """Each remaining allow-listed family is handled without 500/hang.

    Accepted (202, kind in the expected family) when tree_magic recognises the
    magic; a clean 400 is tolerated only as a *fixture* limitation (minimal stub
    not recognised) and printed for the record. A 500/None (crash/hang) fails.
    """
    # Office/archive/legacy accept-paths reach the tagger; skip on a degraded backend.
    if proto.kind_class in ("pptx", "xlsx", "odt", "rtf", "doc"):
        ff.skip_if_degraded(client._c, "text")
    if proto.kind_class in ("image", "video"):
        ff.skip_if_degraded(client._c, "text")  # vision captioning

    marker = flows.unique_marker("fmt")
    fn = f"{marker}.{proto.filename.rsplit('.', 1)[-1]}"
    rid = ff.new_request_id("fmt")
    out = ff.upload(client._c, [(fn, proto.content, proto.mime)], request_id=rid)

    # Robustness contract: an allow-listed format must never crash or hang.
    assert out["status"] is not None, f"{proto.filename}: transport hang/None (req {rid})"
    if out["status"] in (500, 502, 503):
        if not ff.subsystem_ok(client._c, "embed") or not ff.subsystem_ok(client._c, "text"):
            pytest.skip(f"backend degraded — {proto.filename} 500 is backend-outage, not format handling")
        pytest.fail(f"{proto.filename} ({proto.mime}) caused {out['status']} on a healthy backend (req {rid}): {out['text']}")

    assert out["status"] in (200, 202, 400), (
        f"{proto.filename}: unexpected status {out['status']} (req {rid}): {out['text']}"
    )
    if out["status"] in (200, 202):
        doc = client.get_document((out["body"] or {})["document_id"])
        assert doc.get("kind") in proto.expected_kinds, (
            f"{proto.filename}: accepted but kind {doc.get('kind')!r} not in {proto.expected_kinds} (req {rid})"
        )
        print(f"[fmt] {proto.filename} ({proto.mime}) ACCEPTED kind={doc.get('kind')}")
    else:
        # Recorded: tree_magic did not recognise the minimal stub → octet-stream → 400.
        print(f"[fmt] {proto.filename} ({proto.mime}) not recognised from minimal stub → 400 (fixture limitation)")


def test_format_catalog_covers_allowlist():
    """Meta: the accept+deny catalogs together touch every major allow/deny family."""
    fams = {s.mime.split("/")[0] for s in ff.extended_accept_formats("x")}
    assert {"image", "audio", "video", "application"} <= fams, f"missing a top-level family: {fams}"
    assert len(ff.denied_formats("x")) >= 4, "denied catalog too small"
