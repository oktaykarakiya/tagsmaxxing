// SPDX-License-Identifier: AGPL-3.0-or-later

//! Archive extractor: reads the **text inside** `zip` / `tar` / `tar.gz` archives
//! so their contents are searchable, not just a file listing (the old Tika path
//! returned only a manifest). Each text-like entry's content is concatenated into
//! [`Extracted::text`] under an `--- <name> ---` header; an entry manifest is kept
//! in [`Extracted::meta`].
//!
//! Bounded against archive bombs and path traversal (plan §17, §31.5):
//! - at most [`MAX_ARCHIVE_ENTRIES`] entries are inspected;
//! - at most [`MAX_ARCHIVE_ENTRY_BYTES`] are read from any one entry;
//! - at most [`MAX_ARCHIVE_TOTAL_UNCOMPRESSED_BYTES`] are read in total;
//! - zip entries whose uncompressed/compressed ratio exceeds
//!   [`MAX_ARCHIVE_COMPRESSION_RATIO`] (with a large absolute size) are skipped;
//! - entry names containing `..` or absolute paths are skipped;
//! - nested archives are listed, **not** recursed (v1).
//!
//! Extraction is **best-effort**: a malformed or unsupported (7z/rar) archive
//! yields an empty/partial [`Extracted`] rather than an error, so a bad upload
//! never 500s.

use std::io::{Cursor, Read};

use async_trait::async_trait;
use kb_core::extractor::{Extracted, Extractor, RawFile};
use serde_json::json;

use crate::security::{
    MAX_ARCHIVE_COMPRESSION_RATIO, MAX_ARCHIVE_ENTRIES, MAX_ARCHIVE_ENTRY_BYTES,
    MAX_ARCHIVE_TOTAL_UNCOMPRESSED_BYTES,
};

/// Maximum number of entry names recorded in the metadata manifest (keeps the
/// stored JSON bounded even for archives near [`MAX_ARCHIVE_ENTRIES`]).
const MAX_MANIFEST_NAMES: usize = 256;

/// Extractor for [`DocKind::Archive`](kb_core::kind::DocKind::Archive): zip / tar /
/// tar.gz. Reads text-like entry contents into a single searchable text blob.
pub struct ArchiveExtractor;

/// Accumulated result of walking an archive's entries.
#[derive(Default)]
struct Harvest {
    /// Concatenated text of all text-like entries (`--- name ---\n<content>`).
    text: String,
    /// Names of entries whose text was extracted.
    extracted: Vec<String>,
    /// Names of entries that were skipped (binary, unsafe name, or bomb).
    skipped: Vec<String>,
    /// True if any cap (entry count or total bytes) cut the walk short.
    truncated: bool,
    /// Detected container format label for the manifest.
    format: &'static str,
}

#[async_trait]
impl Extractor for ArchiveExtractor {
    /// Walk the archive and return its entry text + manifest. Never errors on a
    /// malformed/unsupported archive (best-effort; returns empty/partial).
    async fn extract(&self, file: &RawFile) -> anyhow::Result<Extracted> {
        let bytes = file.bytes.clone();
        // Archive readers are synchronous; run them off the async worker so a
        // large decompression can't stall the runtime. A panic in the reader
        // (e.g. a dependency bug) degrades to an empty harvest, never a 500.
        let harvest = tokio::task::spawn_blocking(move || harvest_archive(&bytes))
            .await
            .unwrap_or_default();

        let mut names = harvest.extracted.clone();
        names.truncate(MAX_MANIFEST_NAMES);
        Ok(Extracted {
            text: harvest.text,
            meta: json!({
                "archive:format": harvest.format,
                "archive:extracted_count": harvest.extracted.len(),
                "archive:skipped_count": harvest.skipped.len(),
                "archive:entries": names,
                "archive:truncated": harvest.truncated,
            }),
            page_images: Vec::new(),
        })
    }
}

/// Dispatch on magic bytes. Unknown/unsupported containers yield an empty harvest.
fn harvest_archive(bytes: &[u8]) -> Harvest {
    if bytes.starts_with(b"PK\x03\x04") || bytes.starts_with(b"PK\x05\x06") {
        harvest_zip(bytes)
    } else if bytes.starts_with(&[0x1f, 0x8b]) {
        harvest_gzip(bytes)
    } else if is_tar(bytes) {
        harvest_tar(Cursor::new(bytes), "tar")
    } else {
        Harvest::default()
    }
}

/// Handle a gzip stream: decompress a bounded prefix (gzip-bomb guard), then
/// dispatch — a gzipped tar (`.tar.gz`) is walked as a tar; a single gzipped text
/// file (`.txt.gz`) yields its text directly. Anything else yields no text.
fn harvest_gzip(bytes: &[u8]) -> Harvest {
    let mut buf = Vec::new();
    let read = flate2::read::GzDecoder::new(Cursor::new(bytes))
        .take(MAX_ARCHIVE_TOTAL_UNCOMPRESSED_BYTES as u64)
        .read_to_end(&mut buf);
    if read.is_err() {
        return Harvest {
            format: "gzip",
            ..Default::default()
        };
    }
    if is_tar(&buf) {
        return harvest_tar(Cursor::new(buf), "tar.gz");
    }
    let mut h = Harvest {
        format: "gzip",
        ..Default::default()
    };
    if is_text_like(&buf) {
        push_entry_text(&mut h, "(gzip)", &buf);
    }
    h
}

/// POSIX tar files carry the `ustar` magic at byte offset 257.
fn is_tar(bytes: &[u8]) -> bool {
    bytes.len() >= 262 && &bytes[257..262] == b"ustar"
}

/// True when `bytes` look like UTF-8 text/code/markup: no NUL byte and a high
/// proportion of printable / whitespace / UTF-8 continuation bytes. Deterministic
/// and cheap — avoids per-extension MIME tables for the many source-code types.
fn is_text_like(bytes: &[u8]) -> bool {
    if bytes.is_empty() || bytes.contains(&0) {
        return false;
    }
    let printable = bytes
        .iter()
        .filter(|&&b| matches!(b, b'\t' | b'\n' | b'\r') || (0x20..=0x7e).contains(&b) || b >= 0x80)
        .count();
    printable.saturating_mul(100) / bytes.len() >= 85
}

/// Reject entry names that try to escape the archive root (absolute paths or any
/// `..` component) so a hostile archive can never imply a write outside its tree.
fn is_safe_entry_name(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with('/')
        && !name.starts_with('\\')
        && !name.contains('\0')
        && !name.split(['/', '\\']).any(|seg| seg == "..")
}

/// Append one entry's text under a header, returning the bytes appended.
fn push_entry_text(h: &mut Harvest, name: &str, content: &[u8]) {
    h.extracted.push(name.to_string());
    h.text.push_str("\n--- ");
    h.text.push_str(name);
    h.text.push_str(" ---\n");
    h.text.push_str(&String::from_utf8_lossy(content));
    h.text.push('\n');
}

/// Walk a ZIP archive. Uses both sizes from the central directory for the
/// compression-ratio bomb guard.
fn harvest_zip(bytes: &[u8]) -> Harvest {
    let mut h = Harvest {
        format: "zip",
        ..Default::default()
    };
    let mut archive = match zip::ZipArchive::new(Cursor::new(bytes)) {
        Ok(a) => a,
        Err(_) => return h, // corrupt → empty (best-effort)
    };
    let count = archive.len();
    let limit = count.min(MAX_ARCHIVE_ENTRIES);
    if count > limit {
        h.truncated = true;
    }
    let mut total = 0usize;
    for i in 0..limit {
        let entry = match archive.by_index(i) {
            Ok(e) => e,
            Err(_) => continue,
        };
        if entry.is_dir() {
            continue;
        }
        let name = entry.name().to_string();
        if !is_safe_entry_name(&name) {
            h.skipped.push(name);
            continue;
        }
        // Zip-bomb guard: a huge entry that compressed away to almost nothing.
        let (uncompressed, compressed) = (entry.size(), entry.compressed_size());
        if compressed > 0
            && uncompressed > MAX_ARCHIVE_ENTRY_BYTES as u64
            && uncompressed / compressed > MAX_ARCHIVE_COMPRESSION_RATIO
        {
            h.skipped.push(name);
            continue;
        }
        let budget = MAX_ARCHIVE_TOTAL_UNCOMPRESSED_BYTES.saturating_sub(total);
        if budget == 0 {
            h.truncated = true;
            break;
        }
        let cap = MAX_ARCHIVE_ENTRY_BYTES.min(budget);
        let mut buf = Vec::new();
        if entry.take(cap as u64).read_to_end(&mut buf).is_err() {
            continue;
        }
        total += buf.len();
        if is_text_like(&buf) {
            push_entry_text(&mut h, &name, &buf);
        } else {
            h.skipped.push(name);
        }
    }
    h
}

/// Walk a tar stream (plain or gzip-wrapped). Tar has no per-entry compression,
/// so the total-bytes cap is the bomb guard (it bounds a gzip bomb too, since the
/// decoder is read lazily and we stop once the budget is spent).
fn harvest_tar<R: Read>(reader: R, format: &'static str) -> Harvest {
    let mut h = Harvest {
        format,
        ..Default::default()
    };
    let mut archive = tar::Archive::new(reader);
    let entries = match archive.entries() {
        Ok(e) => e,
        Err(_) => return h,
    };
    let mut total = 0usize;
    let mut seen = 0usize;
    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => break, // stream desync → stop (best-effort)
        };
        seen += 1;
        if seen > MAX_ARCHIVE_ENTRIES {
            h.truncated = true;
            break;
        }
        if !entry.header().entry_type().is_file() {
            continue;
        }
        let name = entry
            .path()
            .ok()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_default();
        if !is_safe_entry_name(&name) {
            h.skipped.push(name);
            continue;
        }
        let budget = MAX_ARCHIVE_TOTAL_UNCOMPRESSED_BYTES.saturating_sub(total);
        if budget == 0 {
            h.truncated = true;
            break;
        }
        let cap = MAX_ARCHIVE_ENTRY_BYTES.min(budget);
        let mut buf = Vec::new();
        if entry.take(cap as u64).read_to_end(&mut buf).is_err() {
            continue;
        }
        total += buf.len();
        if is_text_like(&buf) {
            push_entry_text(&mut h, &name, &buf);
        } else {
            h.skipped.push(name);
        }
    }
    h
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use bytes::Bytes;
    use kb_core::kind::DocKind;
    use std::io::Write;

    fn raw(bytes: Vec<u8>) -> RawFile {
        RawFile {
            bytes: Bytes::from(bytes),
            mime: Some("application/zip".into()),
            kind: DocKind::Archive,
            path: Some("a.zip".into()),
        }
    }

    /// Build a zip in-memory from (name, content) pairs.
    fn make_zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut buf = Vec::new();
        {
            let mut w = zip::ZipWriter::new(Cursor::new(&mut buf));
            let opts: zip::write::FileOptions<'_, ()> = zip::write::FileOptions::default()
                .compression_method(zip::CompressionMethod::Stored);
            for (name, content) in entries {
                w.start_file(*name, opts).unwrap();
                w.write_all(content).unwrap();
            }
            w.finish().unwrap();
        }
        buf
    }

    fn make_targz(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        let mut tar = tar::Builder::new(gz);
        for (name, content) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_size(content.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            tar.append_data(&mut header, name, *content).unwrap();
        }
        tar.into_inner().unwrap().finish().unwrap()
    }

    #[tokio::test]
    async fn zip_text_entries_become_searchable() {
        let zip = make_zip(&[
            (
                "readme.txt",
                b"hello from inside the archive UNIQUEMARKER42",
            ),
            ("notes.md", b"# Notes\nsecond entry content"),
        ]);
        let out = ArchiveExtractor.extract(&raw(zip)).await.unwrap();
        assert!(
            out.text.contains("UNIQUEMARKER42"),
            "inner text must be extracted"
        );
        assert!(out.text.contains("second entry content"));
        assert!(out.text.contains("readme.txt"), "entry name header present");
        assert_eq!(out.meta["archive:format"], "zip");
        assert_eq!(out.meta["archive:extracted_count"], 2);
    }

    #[tokio::test]
    async fn zip_binary_entry_is_skipped_not_errored() {
        // A NUL-laden binary entry must be skipped (not decoded as text).
        let zip = make_zip(&[
            ("doc.txt", b"plain text KEEPME"),
            ("blob.bin", &[0u8, 1, 2, 3, 0, 9, 0]),
        ]);
        let out = ArchiveExtractor.extract(&raw(zip)).await.unwrap();
        assert!(out.text.contains("KEEPME"));
        assert!(
            !out.text.contains("blob.bin"),
            "binary entry text not included"
        );
        assert_eq!(out.meta["archive:extracted_count"], 1);
        assert_eq!(out.meta["archive:skipped_count"], 1);
    }

    #[tokio::test]
    async fn zip_path_traversal_entry_skipped() {
        let zip = make_zip(&[("../../etc/passwd", b"root:x:0:0:")]);
        let out = ArchiveExtractor.extract(&raw(zip)).await.unwrap();
        assert!(
            !out.text.contains("root:x"),
            "traversal entry must be skipped"
        );
        assert_eq!(out.meta["archive:extracted_count"], 0);
        assert_eq!(out.meta["archive:skipped_count"], 1);
    }

    #[tokio::test]
    async fn targz_text_entries_become_searchable() {
        let tgz = make_targz(&[("a.txt", b"tar entry TARMARKER seven")]);
        let mut rf = raw(tgz);
        rf.mime = Some("application/gzip".into());
        let out = ArchiveExtractor.extract(&rf).await.unwrap();
        assert!(
            out.text.contains("TARMARKER"),
            "tar.gz inner text extracted"
        );
        assert_eq!(out.meta["archive:format"], "tar.gz");
    }

    #[tokio::test]
    async fn bare_gzip_text_file_extracted() {
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(b"a single gzipped text file GZSOLO").unwrap();
        let gz = enc.finish().unwrap();
        let mut rf = raw(gz);
        rf.mime = Some("application/gzip".into());
        let out = ArchiveExtractor.extract(&rf).await.unwrap();
        assert!(
            out.text.contains("GZSOLO"),
            "bare .gz text must be extracted"
        );
        assert_eq!(out.meta["archive:format"], "gzip");
    }

    #[tokio::test]
    async fn empty_zip_is_ok_and_empty() {
        let zip = make_zip(&[]);
        let out = ArchiveExtractor.extract(&raw(zip)).await.unwrap();
        assert!(out.text.is_empty());
        assert_eq!(out.meta["archive:extracted_count"], 0);
    }

    #[tokio::test]
    async fn malformed_archive_is_best_effort_empty() {
        // Garbage that is neither zip nor tar → empty harvest, never an error.
        let out = ArchiveExtractor
            .extract(&raw(b"not an archive at all".to_vec()))
            .await
            .unwrap();
        assert!(out.text.is_empty());
        assert_eq!(out.meta["archive:format"], "");
    }

    #[test]
    fn text_like_accepts_utf8_rejects_binary() {
        assert!(is_text_like("hello \u{1f600} world".as_bytes()));
        assert!(is_text_like(b"def f():\n    return 1\n"));
        assert!(!is_text_like(&[0u8, 1, 2, 3]));
        assert!(!is_text_like(b""));
    }

    #[test]
    fn entry_name_safety() {
        assert!(is_safe_entry_name("dir/file.txt"));
        assert!(!is_safe_entry_name("../escape"));
        assert!(!is_safe_entry_name("/abs/path"));
        assert!(!is_safe_entry_name("a/../../b"));
    }
}
