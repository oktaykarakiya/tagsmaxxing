"""Binary file builders + upload helpers for the upload/ingest test campaign.

The e2e venv ships only pytest/playwright/httpx/dotenv/openai — no Pillow, no
ffmpeg — so every sample here is hand-built from raw bytes, exactly like the
inline builders already in ``test_ingest.py``. Builders return *correct magic
bytes* and minimal structural validity so the app's MIME detection
(``tree_magic_mini``) routes them to the right ``DocKind`` and the security
allow-list (``crates/extract/src/security.rs``) accepts/denies them as intended.

Decodability note: ``png``/``wav``/``text_pdf``/``docx``/``zip`` are genuinely
valid (an extractor can parse them); the other media stubs are magic-valid only
(enough to be *accepted and routed*, not deeply extracted). Matrix breadth tests
therefore assert acceptance + routing; deep-metadata quality is checked
separately on the decodable inputs.

Also provides the status-preserving ``upload`` helper (mirrors
``test_scheduler_load._raw_ingest_outcome`` but for any file/endpoint),
``X-Request-Id`` correlation, a streaming large-body builder, and ``/health``
gating so a genuine backend outage SKIPs a media test instead of FAILing it.
"""

from __future__ import annotations

import gzip as _gzip
import io
import math
import struct
import subprocess
import tarfile
import uuid
import zipfile
import zlib
from dataclasses import dataclass
from typing import Any, Iterator

import httpx
import pytest

from . import config

# ── Limits mirrored from the app (keep in sync with the Rust constants) ───────
KIB = 1024
MIB = 1024 * 1024
# crates/api/src/handlers/ingest.rs
MAX_PAYLOAD_BYTES = 100 * MIB            # total multipart soft cap
MULTIPART_FRAMING_HEADROOM = 64 * KIB    # axum DefaultBodyLimit = payload + headroom
MAX_USER_NOTE_BYTES = 10 * KIB           # user_note truncation boundary
# crates/extract/src/security.rs
MAX_INDIVIDUAL_FILE_BYTES = 500 * MIB    # per-file cap (unreachable behind total cap)

# Outcome tags carried by a Sample so a generic harness can assert correctly.
ACCEPT = "accept"        # expect 202 + a ready document
REJECT = "reject"        # expect 400 (UploadRejected: disallowed MIME / exe / empty)


# ════════════════════════════════════════════════════════════════════════════
# Image builders
# ════════════════════════════════════════════════════════════════════════════
def minimal_png() -> bytes:
    """A valid, decodable 1×1 white-pixel PNG (~68 bytes)."""

    def _chunk(typ: bytes, data: bytes) -> bytes:
        c = typ + data
        crc = struct.pack(">I", zlib.crc32(c) & 0xFFFFFFFF)
        return struct.pack(">I", len(data)) + c + crc

    sig = b"\x89PNG\r\n\x1a\n"
    ihdr = _chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 2, 0, 0, 0))
    idat = _chunk(b"IDAT", zlib.compress(b"\x00\xff\xff\xff"))
    iend = _chunk(b"IEND", b"")
    return sig + ihdr + idat + iend


def minimal_gif() -> bytes:
    """A valid, decodable 1×1 GIF89a (header + LSD + GCT + image + trailer)."""
    header = b"GIF89a"
    lsd = struct.pack("<HHBBB", 1, 1, 0xF0, 0, 0)  # 1x1, GCT flag, 2-colour table
    gct = b"\x00\x00\x00\xff\xff\xff"               # black, white
    # Image descriptor: separator, x, y, w, h, no local table.
    img_desc = b"\x2c" + struct.pack("<HHHHB", 0, 0, 1, 1, 0)
    # LZW: min code size 2, one data sub-block (clear, pixel0, end), terminator.
    img_data = b"\x02\x02\x44\x01\x00"
    trailer = b"\x3b"
    return header + lsd + gct + img_desc + img_data + trailer


def minimal_jpeg() -> bytes:
    """A magic-valid JPEG (SOI + APP0/JFIF + EOI). Detected as image/jpeg."""
    soi = b"\xff\xd8"
    app0 = b"\xff\xe0" + struct.pack(">H", 16) + b"JFIF\x00" + b"\x01\x01\x00" + struct.pack(">HH", 1, 1) + b"\x00\x00"
    eoi = b"\xff\xd9"
    return soi + app0 + eoi


def minimal_webp() -> bytes:
    """A magic-valid WEBP (RIFF…WEBP/VP8 ). Detected as image/webp."""
    vp8 = b"VP8 " + struct.pack("<I", 10) + b"\x00" * 10
    body = b"WEBP" + vp8
    return b"RIFF" + struct.pack("<I", len(body)) + body


def minimal_tiff() -> bytes:
    """A magic-valid little-endian TIFF (II*\\0 + minimal IFD). Detected as image/tiff."""
    # Header: "II", 42, offset to first IFD (8).
    header = b"II" + struct.pack("<HI", 42, 8)
    # One IFD with zero entries + null next-IFD pointer (enough for magic detection).
    ifd = struct.pack("<H", 0) + struct.pack("<I", 0)
    return header + ifd


# ════════════════════════════════════════════════════════════════════════════
# Audio builders
# ════════════════════════════════════════════════════════════════════════════
def minimal_wav(duration_s: float = 0.5, freq: float = 440.0) -> bytes:
    """A valid 16-bit mono PCM WAV with a sine tone (decodable by ffmpeg/whisper)."""
    sample_rate = 8000
    num_samples = int(sample_rate * duration_s)
    samples = bytearray()
    for i in range(num_samples):
        v = int(16000.0 * math.sin(2.0 * math.pi * freq * i / sample_rate))
        samples.extend(struct.pack("<h", max(-32768, min(32767, v))))
    data_len = len(samples)
    header = struct.pack(
        "<4sI4s4sIHHIIHH4sI",
        b"RIFF", 36 + data_len, b"WAVE", b"fmt ", 16, 1, 1,
        sample_rate, sample_rate * 2, 2, 16, b"data", data_len,
    )
    return header + bytes(samples)


def minimal_mp3() -> bytes:
    """A magic-valid MP3: ID3v2 header + an MPEG-1 Layer III frame sync. audio/mpeg."""
    id3 = b"ID3\x03\x00\x00" + b"\x00\x00\x00\x00"        # ID3v2.3, size 0
    frame = b"\xff\xfb\x90\x00" + b"\x00" * 100            # MPEG1 L3 frame sync + padding
    return id3 + frame


def minimal_ogg() -> bytes:
    """A magic-valid OGG container (OggS capture pattern). May detect as audio/ or application/ogg."""
    # OggS, version 0, header type 2 (BOS), granule 0, serial, seq, crc(0), 1 segment of len 0.
    page = b"OggS" + b"\x00" + b"\x02" + b"\x00" * 8 + struct.pack("<I", 1) + struct.pack("<I", 0) + b"\x00" * 4 + b"\x01" + b"\x00"
    return page


def minimal_flac() -> bytes:
    """A magic-valid FLAC stream (fLaC marker + STREAMINFO block header). audio/flac."""
    marker = b"fLaC"
    # METADATA_BLOCK_HEADER: last-block flag + type 0 (STREAMINFO) + 34-byte length.
    streaminfo = b"\x80\x00\x00\x22" + b"\x00" * 34
    return marker + streaminfo


# ════════════════════════════════════════════════════════════════════════════
# Video builders
# ════════════════════════════════════════════════════════════════════════════
def minimal_mp4() -> bytes:
    """A magic-valid MP4 (ftyp + moov/mvhd). Detected as video/mp4."""

    def _atom(typ: bytes, data: bytes = b"") -> bytes:
        return struct.pack(">I", 8 + len(data)) + typ + data

    ftyp = _atom(b"ftyp", b"isom\x00\x00\x00\x00isom")
    mvhd = b"\x00\x00\x00\x00" + struct.pack(">I", 1000) + struct.pack(">I", 100) + b"\x00\x01\x00\x00" + b"\x01\x00\x00\x00" + b"\x00" * 76
    moov = _atom(b"moov", _atom(b"mvhd", mvhd))
    return ftyp + moov


def minimal_mkv() -> bytes:
    """A magic-valid Matroska/WebM EBML header. Detected as video/x-matroska or webm."""
    # EBML magic 1A45DFA3 + a minimal EBML header declaring docType "matroska".
    doctype = b"matroska"
    ebml_body = b"\x42\x82" + bytes([0x80 | len(doctype)]) + doctype
    return b"\x1a\x45\xdf\xa3" + bytes([0x80 | len(ebml_body)]) + ebml_body


def minimal_webm() -> bytes:
    """A magic-valid WebM EBML header (docType "webm"). Detected as video/webm."""
    doctype = b"webm"
    ebml_body = b"\x42\x82" + bytes([0x80 | len(doctype)]) + doctype
    return b"\x1a\x45\xdf\xa3" + bytes([0x80 | len(ebml_body)]) + ebml_body


# ════════════════════════════════════════════════════════════════════════════
# Document builders (PDF + OOXML)
# ════════════════════════════════════════════════════════════════════════════
def minimal_pdf() -> bytes:
    """A self-consistent empty-page PDF (valid, no text)."""
    header = b"%PDF-1.4\n"
    obj1 = b"1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
    obj2 = b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
    obj3 = b"3 0 obj<</Type/Page/MediaBox[0 0 612 792]/Parent 2 0 R/Resources<<>>>>endobj\n"
    off1 = len(header)
    off2 = off1 + len(obj1)
    off3 = off2 + len(obj2)
    xref_start = off3 + len(obj3)
    body = header + obj1 + obj2 + obj3

    def _e(off: int) -> bytes:
        return f"{off:010d} 00000 n \n".encode()

    xref = b"xref\n0 4\n0000000000 65535 f \n" + _e(off1) + _e(off2) + _e(off3)
    trailer = b"trailer<</Size 4/Root 1 0 R>>\n"
    return body + xref + trailer + f"startxref\n{xref_start}\n".encode() + b"%%EOF"


def text_pdf(content: bytes) -> bytes:
    """A minimal PDF whose page content stream carries extractable *text*."""
    text_op = f"BT /F1 12 Tf 100 700 Td ({content.decode('latin-1', errors='replace')}) Tj ET"
    compressed = zlib.compress(text_op.encode())
    header = b"%PDF-1.4\n"
    obj1 = b"1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
    obj2 = b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
    page_obj = b"3 0 obj<</Type/Page/MediaBox[0 0 612 792]/Parent 2 0 R/Resources<<>>/Contents 4 0 R>>endobj\n"
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

    xref = b"xref\n0 5\n0000000000 65535 f \n" + _e(off1) + _e(off2) + _e(off3) + _e(off4)
    trailer = b"trailer<</Size 5/Root 1 0 R>>\n"
    return body + xref + trailer + f"startxref\n{xref_start}\n".encode() + b"%%EOF"


def _ooxml(parts: dict[str, str]) -> bytes:
    """Pack an OOXML document as a real ZIP (PK magic → octet-stream recovery → Tika)."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for name, data in parts.items():
            z.writestr(name, data)
    return buf.getvalue()


def minimal_docx(marker: str) -> bytes:
    """A minimal but valid .docx containing *marker* as body text (Tika-extractable)."""
    ct = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>'
        "</Types>"
    )
    rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>'
        "</Relationships>"
    )
    doc = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        f"<w:body><w:p><w:r><w:t>DOCX test document {marker}</w:t></w:r></w:p></w:body>"
        "</w:document>"
    )
    return _ooxml({"[Content_Types].xml": ct, "_rels/.rels": rels, "word/document.xml": doc})


def minimal_xlsx(marker: str) -> bytes:
    """A minimal but valid .xlsx with *marker* in an inline string cell."""
    ct = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        "</Types>"
    )
    rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
        "</Relationships>"
    )
    wb_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>'
        "</Relationships>"
    )
    wb = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheets><sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets></workbook>'
    )
    sheet = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>XLSX {marker}</t></is></c></row></sheetData></worksheet>'
    )
    return _ooxml({
        "[Content_Types].xml": ct, "_rels/.rels": rels,
        "xl/workbook.xml": wb, "xl/_rels/workbook.xml.rels": wb_rels,
        "xl/worksheets/sheet1.xml": sheet,
    })


# ════════════════════════════════════════════════════════════════════════════
# Archive + code + denied/adversarial builders
# ════════════════════════════════════════════════════════════════════════════
def zip_archive(entries: dict[str, bytes]) -> bytes:
    """A real ZIP archive of *entries* (name → bytes)."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for name, data in entries.items():
            z.writestr(name, data)
    return buf.getvalue()


def tar_archive(entries: dict[str, bytes]) -> bytes:
    """A real (uncompressed) TAR archive of *entries*."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as t:
        for name, data in entries.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            t.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def gzip_blob(data: bytes = b"hello gzip world\n") -> bytes:
    """A gzip-compressed blob (magic 1F 8B). Detected as application/gzip."""
    return _gzip.compress(data)


def zip_bomb_entries(n: int = 20_000) -> bytes:
    """A ZIP with *n* tiny entries — exercises MAX_ARCHIVE_ENTRIES (10k) listing cap."""
    return zip_archive({f"f{i}.txt": b"x" for i in range(n)})


# tree_magic emits text/x-python3 for a shebang'd .py; text/x-csrc for C, etc.
# The new `text/x-*` wildcard (security.rs:310) should accept all of them.
CODE_SAMPLES: dict[str, tuple[str, bytes]] = {
    "py": (
        "py",
        b"#!/usr/bin/env python3\n# fib\n"
        b"def fib(n):\n    a,b=0,1\n    for _ in range(n):\n        a,b=b,a+b\n    return a\n",
    ),
    "c": (
        "c",
        b"#include <stdio.h>\nint main(void){\n    printf(\"hello\\n\");\n    return 0;\n}\n",
    ),
    "sh": (
        "sh",
        b"#!/bin/sh\nset -e\necho \"hello from shell\"\nexit 0\n",
    ),
}


def elf_stub() -> bytes:
    """A tiny ELF header (\\x7fELF). Detected as application/x-executable → DENIED → 400."""
    return b"\x7fELF\x02\x01\x01\x00" + b"\x00" * 8 + struct.pack("<HHI", 2, 0x3E, 1) + b"\x00" * 40


def pe_stub() -> bytes:
    """A DOS/PE 'MZ' stub. Detected as application/x-dosexec → DENIED → 400."""
    return b"MZ" + b"\x90\x00" * 30 + b"This program cannot be run in DOS mode."


def polyglot_zip_named_but_elf() -> tuple[bytes, str]:
    """ELF magic bytes carried under a .zip filename — detection is content-based → 400."""
    return elf_stub(), "totally_a.zip"


def empty_bytes() -> bytes:
    """Zero-length content → application/x-empty → rejected 400 (BUG-INGEST-06)."""
    return b""


def nul_control_text(marker: str) -> bytes:
    """Text laced with NUL + control chars (BUG-INGEST-11) — should be sanitized, not 500."""
    return f"Hello {marker}\x00\x01\x02\x03 world\x7f end".encode()


def sized_text(n: int, marker: str = "") -> bytes:
    """*n* bytes of valid printable text (cheap, in-RAM). Use streaming for >~10 MiB."""
    head = f"size test {marker} ".encode()
    if n <= len(head):
        return head[:n]
    body = b"A" * (n - len(head) - 1) + b"\n"
    return head + body


# ════════════════════════════════════════════════════════════════════════════
# Sample descriptor + the canonical kind catalog
# ════════════════════════════════════════════════════════════════════════════
@dataclass(frozen=True)
class Sample:
    """A buildable upload sample with its declared MIME, expected kind, and outcome."""

    kind_class: str          # equivalence class label (text/code/pdf/image/…)
    filename: str
    content: bytes
    mime: str                # MIME the *client* declares (server re-detects from magic)
    expected: str            # ACCEPT or REJECT
    expected_kinds: tuple[str, ...] = ()  # acceptable doc "kind" values when ACCEPT
    decodable: bool = False  # True when an extractor can fully parse it (deep-metadata)


def sample_catalog(marker: str) -> dict[str, Sample]:
    """The canonical per-kind representative samples, keyed by kind_class.

    One high-confidence representative per equivalence class. ``marker`` makes
    text-bearing samples uniquely searchable.
    """
    return {
        "text": Sample("text", f"{marker}.txt", f"Plain text doc {marker}.".encode(),
                       "text/plain", ACCEPT, ("document", "text"), decodable=True),
        "code": Sample("code", f"{marker}.py", CODE_SAMPLES["py"][1],
                       "text/x-python", ACCEPT, ("code", "document")),
        "pdf": Sample("pdf", f"{marker}.pdf", text_pdf(f"PDF body {marker}".encode()),
                      "application/pdf", ACCEPT, ("document", "pdf"), decodable=True),
        # NOTE: a minimal OOXML zip is detected as application/zip by tree_magic
        # OOXML is a ZIP, but the content-peek classifier (F1 fix) detects the
        # [Content_Types].xml + word/ markers and routes it to the Tika document
        # path → kind="document" (no longer "archive").
        "docx": Sample("docx", f"{marker}.docx", minimal_docx(marker),
                       "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                       ACCEPT, ("document",), decodable=True),
        "image": Sample("image", f"{marker}.png", minimal_png(),
                        "image/png", ACCEPT, ("image",), decodable=True),
        "audio": Sample("audio", f"{marker}.wav", minimal_wav(),
                        "audio/wav", ACCEPT, ("audio",), decodable=True),
        "video": Sample("video", f"{marker}.mp4", minimal_mp4(),
                        "video/mp4", ACCEPT, ("video",)),
        "archive": Sample("archive", f"{marker}.zip", zip_archive({"a.txt": b"inner a", "b.txt": b"inner b"}),
                          "application/zip", ACCEPT, ("archive", "binary", "document")),
        "binary": Sample("binary", f"{marker}.bin", bytes((i * 7) & 0xFF for i in range(256)),
                         "application/octet-stream", REJECT),
        "exe": Sample("exe", f"{marker}.bin", elf_stub(),
                      "application/octet-stream", REJECT),
    }


# Extra image/audio/video format breadth (magic-valid; assert acceptance + routing).
def format_breadth(marker: str) -> list[Sample]:
    """Additional format representatives beyond the catalog, for breadth coverage."""
    return [
        Sample("image", f"{marker}.gif", minimal_gif(), "image/gif", ACCEPT, ("image",), decodable=True),
        Sample("image", f"{marker}.jpg", minimal_jpeg(), "image/jpeg", ACCEPT, ("image",)),
        Sample("image", f"{marker}.webp", minimal_webp(), "image/webp", ACCEPT, ("image",)),
        Sample("image", f"{marker}.tiff", minimal_tiff(), "image/tiff", ACCEPT, ("image",)),
        Sample("audio", f"{marker}.mp3", minimal_mp3(), "audio/mpeg", ACCEPT, ("audio",)),
        Sample("audio", f"{marker}.flac", minimal_flac(), "audio/flac", ACCEPT, ("audio",)),
        # The minimal OggS stub carries no codec streams, so content magic can
        # only see the CONTAINER — shared-mime classifies it video/ogg and
        # nothing decodable exists to refine the kind to audio (real, decodable
        # audio files cover the audio-kind contract elsewhere). Either kind is
        # honest for a bare container.
        Sample("audio", f"{marker}.ogg", minimal_ogg(), "audio/ogg", ACCEPT, ("audio", "video")),
        Sample("video", f"{marker}.mkv", minimal_mkv(), "video/x-matroska", ACCEPT, ("video",)),
        Sample("video", f"{marker}.webm", minimal_webm(), "video/webm", ACCEPT, ("video",)),
        Sample("code", f"{marker}.c", CODE_SAMPLES["c"][1], "text/x-csrc", ACCEPT, ("code", "document")),
        Sample("archive", f"{marker}.tar", tar_archive({"x.txt": b"tar inner"}),
               "application/x-tar", ACCEPT, ("archive", "binary", "document")),
        Sample("archive", f"{marker}.gz", gzip_blob(), "application/gzip", ACCEPT, ("archive", "binary", "document")),
    ]


# ════════════════════════════════════════════════════════════════════════════
# Extended format coverage — one buildable sample per remaining allow-listed /
# denied MIME family (so every type the app claims to support has a test).
# ════════════════════════════════════════════════════════════════════════════
def _bmff(major: bytes, compat: list[bytes], extra: bytes = b"\x00" * 16) -> bytes:
    """An ISO base-media file (MP4/MOV/HEIF/AVIF family): an `ftyp` box with *major* brand."""
    body = major + b"\x00\x00\x00\x00" + b"".join(compat)
    return struct.pack(">I", 8 + len(body)) + b"ftyp" + body + extra


def minimal_avif() -> bytes:
    """AVIF still image (ftyp 'avif'). Detected image/avif."""
    return _bmff(b"avif", [b"avif", b"mif1", b"miaf"])


def minimal_heic() -> bytes:
    """HEIC still image (ftyp 'heic'). Detected image/heic."""
    return _bmff(b"heic", [b"heic", b"mif1"])


def minimal_bmp() -> bytes:
    """A valid 2×2 24-bit BMP. Detected image/bmp / image/x-ms-bmp."""
    px = b"\xff\xff\xff\x00\x00\x00\x00\x00\x00\xff\xff\xff"
    dib = struct.pack("<IiiHHIIiiII", 40, 2, 2, 1, 24, 0, len(px), 2835, 2835, 0, 0)
    data = dib + px
    return b"BM" + struct.pack("<IHHI", 14 + len(data), 0, 0, 54) + data


def minimal_m4a() -> bytes:
    """M4A audio (ftyp 'M4A '). Detected audio/mp4."""
    return _bmff(b"M4A ", [b"M4A ", b"isom", b"mp42"])


def minimal_mov() -> bytes:
    """QuickTime video (ftyp 'qt  '). Detected video/quicktime."""
    return _bmff(b"qt  ", [b"qt  "])


def minimal_3gp() -> bytes:
    """3GPP video (ftyp '3gp4'). Detected video/3gpp."""
    return _bmff(b"3gp4", [b"3gp4", b"isom"])


def minimal_avi() -> bytes:
    """AVI video (RIFF…AVI ). Detected video/x-msvideo."""
    body = b"AVI " + b"LIST" + struct.pack("<I", 4) + b"hdrl"
    return b"RIFF" + struct.pack("<I", len(body)) + body


def minimal_7z() -> bytes:
    """7-zip archive magic (37 7A BC AF 27 1C). Detected application/x-7z-compressed."""
    return b"7z\xbc\xaf\x27\x1c\x00\x04" + b"\x00" * 24


def bzip2_blob(data: bytes = b"hello bzip2 world\n") -> bytes:
    """A real bzip2 stream (BZh). Detected application/x-bzip2."""
    import bz2

    return bz2.compress(data)


def xz_blob(data: bytes = b"hello xz world\n") -> bytes:
    """A real xz stream (FD 37 7A 58 5A 00). Detected application/x-xz."""
    import lzma

    return lzma.compress(data)


def zstd_blob() -> bytes:
    """Zstandard frame magic (28 B5 2F FD). Detected application/zstd."""
    return b"\x28\xb5\x2f\xfd\x00" + b"\x00" * 16


def ole2_blob() -> bytes:
    """OLE2 compound-file magic (D0 CF 11 E0) — legacy .doc/.xls/.ppt container."""
    return b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1" + b"\x00" * 504


def rtf_doc(marker: str) -> bytes:
    """A minimal RTF document. Detected application/rtf / text/rtf."""
    return ("{\\rtf1\\ansi\\deff0 RTF document " + marker + ".}").encode()


def odf_doc(doctype: str, marker: str) -> bytes:
    """An OpenDocument file (a zip whose first, STORED entry is `mimetype`=*doctype*)."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        zi = zipfile.ZipInfo("mimetype")
        zi.compress_type = zipfile.ZIP_STORED
        z.writestr(zi, doctype)
        z.writestr("content.xml", f"<?xml version='1.0'?><doc>ODF body {marker}</doc>")
    return buf.getvalue()


def minimal_pptx(marker: str) -> bytes:
    """A minimal but valid .pptx (OOXML zip with ppt/presentation.xml)."""
    ct = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/ppt/presentation.xml" ContentType="application/vnd.openxmlformats-officedocument.presentationml.presentation.main+xml"/>'
        "</Types>"
    )
    rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="ppt/presentation.xml"/>'
        "</Relationships>"
    )
    pres = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<p:presentation xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main">'
        f"<p:notesSz>PPTX {marker}</p:notesSz></p:presentation>"
    )
    return _ooxml({"[Content_Types].xml": ct, "_rels/.rels": rels, "ppt/presentation.xml": pres})


def jar_blob() -> bytes:
    """A JAR (zip with META-INF/MANIFEST.MF + a .class) — DENIED (application/java-archive)."""
    return zip_archive({"META-INF/MANIFEST.MF": b"Manifest-Version: 1.0\n", "Main.class": b"\xca\xfe\xba\xbe\x00\x00\x00\x34"})


def macho_blob() -> bytes:
    """Mach-O binary magic (FE ED FA CE) — DENIED (application/x-mach-binary)."""
    return b"\xfe\xed\xfa\xce" + b"\x00\x00\x00\x07" + b"\x00" * 56


# Text/structured content whose *extraction* should make it searchable
# (BUG-INGEST-03/04: CSV / Markdown / HTML / JSON / XML).
def text_payloads(marker: str) -> dict[str, tuple[str, bytes, str]]:
    """Return {label: (filename, bytes, mime)} for text/structured extraction tests."""
    return {
        "csv": (f"{marker}.csv", f"name,role,note\nAcme,buyer,{marker}\n".encode(), "text/csv"),
        "markdown": (f"{marker}.md", f"# Heading {marker}\n\nParagraph about **{marker}**.\n".encode(), "text/markdown"),
        "html": (f"{marker}.html", f"<html><body><h1>Title</h1><p>{marker} content here.</p></body></html>".encode(), "text/html"),
        "json": (f"{marker}.json", f'{{"id": 1, "marker": "{marker}", "items": ["a", "b"]}}'.encode(), "application/json"),
        "xml": (f"{marker}.xml", f"<?xml version='1.0'?><root><marker>{marker}</marker></root>".encode(), "application/xml"),
    }


def extended_accept_formats(marker: str) -> list[Sample]:
    """One sample per remaining *allow-listed* family beyond the core catalog + breadth."""
    odoc = "application/vnd.oasis.opendocument.text"
    return [
        Sample("image", f"{marker}.avif", minimal_avif(), "image/avif", ACCEPT, ("image",)),
        Sample("image", f"{marker}.heic", minimal_heic(), "image/heic", ACCEPT, ("image",)),
        Sample("audio", f"{marker}.m4a", minimal_m4a(), "audio/mp4", ACCEPT, ("audio",)),
        Sample("video", f"{marker}.mov", minimal_mov(), "video/quicktime", ACCEPT, ("video",)),
        Sample("video", f"{marker}.avi", minimal_avi(), "video/x-msvideo", ACCEPT, ("video",)),
        Sample("video", f"{marker}.3gp", minimal_3gp(), "video/3gpp", ACCEPT, ("video",)),
        Sample("archive", f"{marker}.7z", minimal_7z(), "application/x-7z-compressed", ACCEPT, ("archive", "binary", "document")),
        Sample("archive", f"{marker}.bz2", bzip2_blob(), "application/x-bzip2", ACCEPT, ("archive", "binary", "document")),
        Sample("archive", f"{marker}.xz", xz_blob(), "application/x-xz", ACCEPT, ("archive", "binary", "document")),
        Sample("archive", f"{marker}.zst", zstd_blob(), "application/zstd", ACCEPT, ("archive", "binary", "document")),
        Sample("pptx", f"{marker}.pptx", minimal_pptx(marker), "application/vnd.openxmlformats-officedocument.presentationml.presentation", ACCEPT, ("document",)),
        Sample("xlsx", f"{marker}.xlsx", minimal_xlsx(marker), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", ACCEPT, ("document",)),
        Sample("odt", f"{marker}.odt", odf_doc(odoc, marker), odoc, ACCEPT, ("document", "archive")),
        Sample("rtf", f"{marker}.rtf", rtf_doc(marker), "application/rtf", ACCEPT, ("document",)),
        Sample("doc", f"{marker}.doc", ole2_blob(), "application/msword", ACCEPT, ("document", "archive", "binary")),
    ]


def denied_formats(marker: str) -> list[Sample]:
    """One sample per *denied* family — each must be rejected with 400."""
    return [
        Sample("exe", f"{marker}.elf", elf_stub(), "application/octet-stream", REJECT),
        Sample("exe", f"{marker}.exe", pe_stub(), "application/octet-stream", REJECT),
        Sample("exe", f"{marker}.dylib", macho_blob(), "application/octet-stream", REJECT),
        Sample("binary", f"{marker}.bin", bytes((i * 7) & 0xFF for i in range(256)), "application/octet-stream", REJECT),
        # A JAR (zip + META-INF/MANIFEST.MF) is now denied: the content-peek
        # classifier (F10 fix) detects the Java manifest and reports
        # `application/java-archive` (deny-listed) rather than accepting it as a
        # plain zip Archive.
        Sample("jar", f"{marker}.jar", jar_blob(), "application/java-archive", REJECT),
    ]


# ════════════════════════════════════════════════════════════════════════════
# Upload helpers (status-preserving + X-Request-Id correlation)
# ════════════════════════════════════════════════════════════════════════════
def new_request_id(tag: str = "e2e") -> str:
    """Mint an X-Request-Id the app will adopt and echo (for log correlation)."""
    return f"{tag}-{uuid.uuid4().hex[:20]}"


def _norm_files(files: Any) -> list[tuple[str, tuple[str, bytes, str]]]:
    """Accept a single (filename, content, mime) or a list thereof → httpx files=."""
    if files and isinstance(files, tuple) and len(files) == 3 and isinstance(files[0], str):
        files = [files]
    out: list[tuple[str, tuple[str, bytes, str]]] = []
    for fn, content, mime in files:
        out.append(("files", (fn, content, mime)))
    return out


def upload(
    client: httpx.Client,
    files: Any,
    *,
    endpoint: str = "/api/ingest",
    data: dict[str, Any] | None = None,
    request_id: str | None = None,
    timeout: float | None = None,
) -> dict[str, Any]:
    """Fire one upload and return its *raw* outcome (never raises on HTTP status).

    ``files`` is a ``(filename, content, mime)`` tuple or a list of them. When
    ``endpoint == "/upload"`` the CSRF double-submit dance is performed
    automatically. Returns ``{status, ok, body, text, request_id, retry_after}``
    where ``request_id`` is the value echoed by the server (for log grep).
    """
    headers: dict[str, str] = {}
    rid = request_id or new_request_id()
    headers["X-Request-Id"] = rid
    form = dict(data or {})

    if endpoint == "/upload":
        try:
            g = client.get("/upload")
            import re
            m = re.search(r'name="csrf_token"\s+value="([^"]+)"', g.text)
            if not m:
                return {"status": g.status_code, "ok": False, "body": None,
                        "text": "no csrf token on /upload", "request_id": rid, "retry_after": None}
            form["csrf_token"] = m.group(1)
        except Exception as exc:  # noqa: BLE001
            return {"status": None, "ok": False, "body": None, "text": repr(exc),
                    "request_id": rid, "retry_after": None}

    try:
        kw: dict[str, Any] = {"files": _norm_files(files), "data": form, "headers": headers}
        if timeout is not None:
            kw["timeout"] = timeout
        r = client.post(endpoint, **kw)
    except Exception as exc:  # noqa: BLE001 — a hang/drop/timeout surfaces here
        return {"status": None, "ok": False, "body": None, "text": repr(exc),
                "request_id": rid, "retry_after": None}

    body: dict[str, Any] | None = None
    try:
        body = r.json()
    except Exception:  # noqa: BLE001
        body = None
    return {
        "status": r.status_code,
        "ok": r.status_code == 202,
        "body": body,
        "text": r.text[:600],
        "request_id": r.headers.get("x-request-id", rid),
        "retry_after": r.headers.get("retry-after"),
    }


def multipart_stream(
    filename: str,
    mime: str,
    file_size: int,
    *,
    boundary: str = "kbE2EstreamBOUNDARY",
    chunk: int = 1 * MIB,
    fill: bytes = b"A",
) -> tuple[Iterator[bytes], str]:
    """Build a streaming multipart body of *file_size* file bytes without buffering it.

    Returns ``(generator, content_type)``. Use as ``client.post(url,
    content=gen, headers={"Content-Type": content_type})`` to push large bodies
    (e.g. the 100-MiB total-cap probes) cheaply.
    """
    preamble = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="files"; filename="{filename}"\r\n'
        f"Content-Type: {mime}\r\n\r\n"
    ).encode()
    closing = f"\r\n--{boundary}--\r\n".encode()

    def _gen() -> Iterator[bytes]:
        yield preamble
        remaining = file_size
        block = fill * chunk
        while remaining > 0:
            n = min(chunk, remaining)
            yield block[:n]
            remaining -= n
        yield closing

    return _gen(), f"multipart/form-data; boundary={boundary}"


# ════════════════════════════════════════════════════════════════════════════
# Health gating + log correlation (distinguish app bugs from backend outages)
# ════════════════════════════════════════════════════════════════════════════
def health_report(client: httpx.Client) -> dict[str, Any]:
    """Fetch /health and return its parsed JSON (or {} on any error)."""
    try:
        r = client.get("/health")
        return r.json() if r.text else {}
    except Exception:  # noqa: BLE001
        return {}


def _degraded_subsystems(report: dict[str, Any]) -> set[str]:
    """Best-effort extraction of degraded subsystem names from a /health body.

    Tolerant of shape drift: scans common containers (``subsystems``,
    ``degradation``, ``checks``) for entries whose status/ok/degraded flags say
    "not healthy", returning the lowercased subsystem keys.
    """
    degraded: set[str] = set()

    def _is_bad(v: Any) -> bool:
        if isinstance(v, bool):
            return v is False
        if isinstance(v, str):
            return v.lower() in ("degraded", "down", "unhealthy", "error", "false", "fail")
        if isinstance(v, dict):
            if v.get("degraded") is True:
                return True
            if "ok" in v and v.get("ok") is False:
                return True
            if "healthy" in v and v.get("healthy") is False:
                return True
            st = v.get("status")
            return isinstance(st, str) and st.lower() not in ("ok", "healthy", "up", "ready")
        return False

    # Primary shape (observed): {"degradation": {"ok": bool, "subsystems": [...]}}.
    deg = report.get("degradation")
    if isinstance(deg, dict):
        subs = deg.get("subsystems")
        if isinstance(subs, list):
            for item in subs:
                if isinstance(item, str):
                    degraded.add(item.lower())
                elif isinstance(item, dict):
                    name = item.get("name") or item.get("subsystem") or item.get("id") or item.get("role")
                    if name:
                        degraded.add(str(name).lower())
        elif isinstance(subs, dict):
            for name, v in subs.items():
                if _is_bad(v):
                    degraded.add(str(name).lower())

    # Generic fallback for alternative shapes (per-subsystem dicts).
    for container_key in ("subsystems", "checks", "components"):
        c = report.get(container_key)
        if isinstance(c, dict):
            for name, v in c.items():
                if name in ("ok", "degraded", "status"):
                    continue
                if _is_bad(v):
                    degraded.add(str(name).lower())

    # A global backends=false means model roles are unreachable — gate them all.
    if report.get("backends") is False:
        degraded.update({"text", "embed", "rerank", "vision", "tagger", "whisper"})
    return degraded


# Map a logical backend → substrings that may name it in /health.
_SUBSYS_ALIASES = {
    "text": ("text", "llm", "tagger", "vision", "vlm", "chat", "generate"),
    "embed": ("embed", "embedding"),
    "rerank": ("rerank", "reranker"),
    "whisper": ("whisper", "audio", "transcrib", "asr"),
    "tika": ("tika", "document"),
}


def subsystem_ok(client: httpx.Client, logical: str) -> bool:
    """True when no degraded /health entry matches *logical* (text/embed/whisper/tika…)."""
    degraded = _degraded_subsystems(health_report(client))
    aliases = _SUBSYS_ALIASES.get(logical, (logical,))
    return not any(any(a in d for a in aliases) for d in degraded)


def skip_if_degraded(client: httpx.Client, logical: str) -> None:
    """pytest.skip the calling test when the *logical* backend is degraded.

    This is the anti-noise guard: a genuine backend outage SKIPs a media test
    (not a failure), while a regression on a *healthy* backend still FAILs.
    """
    if not subsystem_ok(client, logical):
        pytest.skip(f"backend '{logical}' degraded per /health — skipping (not an app bug)")


# Container name for the app (compose project ``local-kb``), matching lib/limits.
_APP_CONTAINER = "local-kb_app_1"


def app_logs_for_request(request_id: str, *, tail: int = 4000) -> str:
    """Return app-log lines correlated to *request_id* (via ``podman logs``).

    Best-effort: returns "" when podman/the container is unavailable. Used by the
    triage step to attach the failing request's tracing lines to a bug report.
    """
    try:
        res = subprocess.run(
            ["podman", "logs", "--tail", str(tail), _APP_CONTAINER],
            capture_output=True, text=True, timeout=15,
        )
    except Exception:  # noqa: BLE001
        return ""
    blob = (res.stdout or "") + (res.stderr or "")
    hits = [ln for ln in blob.splitlines() if request_id in ln]
    return "\n".join(hits)


def tagger_fallback_fired(request_id: str) -> bool:
    """True if the media-tagger fallback warning appears for *request_id* in app logs.

    ``crates/pipeline/src/ingest.rs:383`` logs "tagger failed for media document;
    ingesting with default metadata" when the fallback masks a tagger error. If
    this fires while /health is healthy, a media ingest silently lost its tags.
    """
    return "tagger failed for media document" in app_logs_for_request(request_id)
