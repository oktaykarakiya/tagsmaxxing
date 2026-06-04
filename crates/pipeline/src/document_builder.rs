//! Document builder: constructs a [`Document`] and ordered [`FileRecord`]s from
//! 1..N raw file bytes (plan §5, §7, §27).
//!
//! A single-file upload auto-creates a 1-page document; multi-file grouping
//! accepts `(bytes, page_label)` pairs in order. No database writes are performed —
//! the caller receives `(Document { status: pending }, Vec<FileRecord>)` to pass
//! downstream to the remaining pipeline steps (§7).
//!
//! # Design
//!
//! - **Content-addressed**: each file's [`Sha256`] is computed from its bytes and
//!   used as the `blob_key` (tenant-prefixed) → per-tenant dedup naturally works
//!   because identical content produces the same key (§5, §20).
//! - **MIME detection** is pure Rust via [`tree_magic_mini`] — no subprocess,
//!   no I/O, deterministic (§2).
//! - **Kind inference** is derived from detected MIME types; when all pages share
//!   the same top-level kind (all `image/*` → [`DocKind::Image`]) that kind is
//!   used; mixed types fall back to [`DocKind::Document`].
//! - **Synchronous**: all operations are CPU-bound (hashing + magic-byte lookup),
//!   so no async is needed.

use chrono::Utc;
use kb_core::document::Document;
use kb_core::file::FileRecord;
use kb_core::hash::Sha256;
use kb_core::kind::DocKind;
use kb_core::status::ProcessingStatus;
use sha2::Digest;

/// A single page/member file to include in a multi-page document (plan §27).
///
/// Each page carries its raw bytes, an optional human label
/// (`"front"` / `"back"` / `"p3"`), and an optional original path.
///
/// # Examples
///
/// ```rust
/// use kb_pipeline::document_builder::PageInput;
///
/// let page = PageInput {
///     bytes: b"PNG image data here",
///     page_label: Some("front"),
///     path: Some("id_front.png"),
/// };
/// ```
#[derive(Debug, Clone)]
pub struct PageInput<'a> {
    /// Raw file bytes for this page.
    pub bytes: &'a [u8],
    /// Human label for the page (e.g. `"front"`, `"back"`, `"p3"`, original filename).
    pub page_label: Option<&'a str>,
    /// Original filename or path, if known.
    pub path: Option<&'a str>,
}

/// Builds a [`Document`] and ordered [`FileRecord`]s from raw file bytes.
///
/// This is the first step of the ingestion pipeline (§7): it computes content
/// hashes, detects MIME types, derives document kind, and assigns blob keys —
/// all before any database or object-store I/O.
///
/// # Examples
///
/// Single file → 1-page document (the common case, §27):
///
/// ```rust
/// use kb_pipeline::document_builder::DocumentBuilder;
///
/// let (doc, files) = DocumentBuilder::build_single(
///     1,
///     b"Hello, world!",
///     Some("hello.txt"),
///     Some("a friendly greeting"),
/// );
/// assert_eq!(doc.page_count, 1);
/// assert_eq!(files.len(), 1);
/// assert_eq!(files[0].page_no, 1);
/// ```
///
/// Multi-file → N-page document with explicit labels:
///
/// ```rust
/// use kb_pipeline::document_builder::{DocumentBuilder, PageInput};
///
/// let pages = [
///     PageInput { bytes: b"front side", page_label: Some("front"), path: Some("id_front.png") },
///     PageInput { bytes: b"back side",  page_label: Some("back"),  path: Some("id_back.png") },
/// ];
/// let (doc, files) = DocumentBuilder::build_multi(1, &pages, None);
/// assert_eq!(doc.page_count, 2);
/// assert_eq!(files[0].page_no, 1);
/// assert_eq!(files[1].page_no, 2);
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct DocumentBuilder;

impl DocumentBuilder {
    /// Build a [`Document`] from a single file — the common case (§27).
    ///
    /// The file is auto-wrapped in a 1-page document. Returns the document
    /// (with `status = pending`) and a single-element `Vec<FileRecord>`.
    #[must_use]
    pub fn build_single(
        tenant_id: i64,
        bytes: &[u8],
        path: Option<&str>,
        user_note: Option<&str>,
    ) -> (Document, Vec<FileRecord>) {
        let page = PageInput {
            bytes,
            page_label: None,
            path,
        };
        Self::build_multi(tenant_id, &[page], user_note)
    }

    /// Build a [`Document`] from multiple files with explicit ordering (§27).
    ///
    /// Pages are ordered as given: the first [`PageInput`] becomes `page_no = 1`,
    /// the second `page_no = 2`, and so on. The document `kind` is inferred
    /// from the files' detected MIME types (all images → [`DocKind::Image`],
    /// mixed types → [`DocKind::Document`], etc.).
    ///
    /// When `pages` is empty, returns a document with `page_count = 0`,
    /// `kind = Binary`, and an empty file list.
    #[must_use]
    pub fn build_multi(
        tenant_id: i64,
        pages: &[PageInput<'_>],
        user_note: Option<&str>,
    ) -> (Document, Vec<FileRecord>) {
        let now = Utc::now();
        let page_count = pages.len() as i32;

        let mut file_records = Vec::with_capacity(pages.len());
        let mut mimes: Vec<String> = Vec::with_capacity(pages.len());

        for (idx, page) in pages.iter().enumerate() {
            let sha256 = compute_sha256(page.bytes);
            let sha256_hex = sha256.to_hex();
            let blob_key = build_blob_key(tenant_id, &sha256_hex);
            let mime = detect_mime(page.bytes).to_owned();
            mimes.push(mime.clone());

            file_records.push(FileRecord {
                id: 0,
                tenant_id,
                document_id: 0,
                page_no: (idx + 1) as i32,
                page_label: page.page_label.map(String::from),
                sha256,
                blob_key,
                path: page.path.map(String::from),
                mime: Some(mime),
                size_bytes: {
                    let len = page.bytes.len();
                    Some(i64::try_from(len).unwrap_or(i64::MAX))
                },
                meta: serde_json::Value::Object(serde_json::Map::new()),
                status: ProcessingStatus::Pending,
                ingested_at: now,
            });
        }

        let kind = infer_doc_kind(&mimes);

        let document = Document {
            id: 0,
            tenant_id,
            title: None,
            summary: None,
            user_note: user_note.map(String::from),
            kind,
            meta: serde_json::Value::Object(serde_json::Map::new()),
            page_count,
            status: ProcessingStatus::Pending,
            created_at: now,
            local_only: false,
        };

        (document, file_records)
    }
}

// ── helper functions ───────────────────────────────────────────────────────────

/// Compute the SHA-256 digest of `bytes` and return it as a [`Sha256`].
///
/// Uses the `sha2` crate. The resulting digest is used for content-addressed
/// blob keys and per-tenant deduplication (§5, §20).
fn compute_sha256(bytes: &[u8]) -> Sha256 {
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    let digest: [u8; 32] = hasher.finalize().into();
    Sha256::from_bytes(digest)
}

/// Detect the MIME type of `bytes` via magic-byte inspection using
/// [`tree_magic_mini`].
///
/// Returns the MIME type as a string (e.g. `"image/png"`, `"application/pdf"`,
/// `"text/plain"`). Empty input yields `"application/x-empty"`. Bytes without a
/// recognised binary magic signature default to `"text/plain"`
/// (tree_magic_mini's fallback).
///
/// This is a pure-Rust operation — no subprocess, no I/O.
fn detect_mime(bytes: &[u8]) -> &'static str {
    if bytes.is_empty() {
        return "application/x-empty";
    }
    let detected = tree_magic_mini::from_u8(bytes);
    // Mirror kb_extract::security::detect_upload_mime: tree_magic_mini 3.2.2
    // classifies some ZIP containers — notably minimal OOXML office documents —
    // as application/octet-stream despite the ZIP local-file-header magic. The
    // upload-edge allow-list normalises these to application/zip; do the same
    // here so the file is *routed* consistently (infer_kind_from_mime ->
    // DocKind::Archive -> Tika) rather than to DocKind::Binary (BUG-INGEST-05).
    if detected == "application/octet-stream" && bytes.starts_with(b"PK\x03\x04") {
        return "application/zip";
    }
    detected
}

/// Build a tenant-prefixed, content-addressed blob key.
///
/// Format: `"{tenant_id}/{hex_sha256}"`. The tenant prefix ensures that two
/// tenants who ingest identical bytes get separate blob keys (and thus separate
/// storage), while retaining content-addressing within each tenant.
fn build_blob_key(tenant_id: i64, sha256_hex: &str) -> String {
    format!("{tenant_id}/{sha256_hex}")
}

/// Infer the [`DocKind`] for a document from its member files' MIME types.
///
/// When all files share the same top-level kind (all `image/*` →
/// [`DocKind::Image`], all `audio/*` → [`DocKind::Audio`], etc.), that kind is
/// used. Mixed MIME types default to [`DocKind::Document`] (the catch-all
/// document kind). An empty MIME list yields [`DocKind::Binary`].
pub(crate) fn infer_doc_kind(mimes: &[String]) -> DocKind {
    if mimes.is_empty() {
        return DocKind::Binary;
    }

    let kinds: Vec<DocKind> = mimes.iter().map(|m| mime_to_doc_kind(m)).collect();
    let first = kinds[0];

    if kinds.iter().all(|&k| k == first) {
        first
    } else {
        DocKind::Document
    }
}

/// Map a single MIME type string to its corresponding [`DocKind`].
///
/// The mapping follows the per-filetype routing table (plan §2):
///
/// | MIME prefix / value         | DocKind     |
/// |-----------------------------|-------------|
/// | `image/*`                   | Image       |
/// | `audio/*`                   | Audio       |
/// | `video/*`                   | Video       |
/// | known archive MIMEs         | Archive     |
/// | known document MIMEs        | Document    |
/// | `text/*`                    | Document    |
/// | everything else             | Binary      |
pub(crate) fn mime_to_doc_kind(mime: &str) -> DocKind {
    match mime {
        s if s.starts_with("image/") => DocKind::Image,
        s if s.starts_with("audio/") => DocKind::Audio,
        s if s.starts_with("video/") => DocKind::Video,
        // Known archive MIME types.
        "application/zip"
        | "application/x-tar"
        | "application/gzip"
        | "application/x-bzip2"
        | "application/x-xz"
        | "application/zstd"
        | "application/x-7z-compressed"
        | "application/x-rar-compressed" => DocKind::Archive,
        // Known document/office MIME types.
        "application/pdf"
        | "application/msword"
        | "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        | "application/vnd.openxmlformats-officedocument.presentationml.presentation"
        | "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        | "application/vnd.oasis.opendocument.text"
        | "application/vnd.oasis.opendocument.presentation"
        | "application/vnd.oasis.opendocument.spreadsheet"
        | "application/rtf"
        | "application/x-empty" => DocKind::Document,
        // Text types — code detection needs file extension (deferred to P3-T4);
        // until then treat all text as Document.
        s if s.starts_with("text/") => DocKind::Document,
        // Everything else is binary/unknown.
        _ => DocKind::Binary,
    }
}

// ── unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    // ── compute_sha256 ─────────────────────────────────────────────────────────

    #[test]
    fn sha256_empty_input() {
        // SHA-256 of empty input (well-known test vector).
        let h = compute_sha256(b"");
        assert_eq!(
            h.to_hex(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn sha256_known_vector() {
        // SHA-256 of "abc" (well-known test vector from FIPS 180-4).
        let h = compute_sha256(b"abc");
        assert_eq!(
            h.to_hex(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn sha256_is_deterministic() {
        let a = compute_sha256(b"same input");
        let b = compute_sha256(b"same input");
        assert_eq!(a, b);
    }

    #[test]
    fn sha256_different_inputs_differ() {
        let a = compute_sha256(b"hello");
        let b = compute_sha256(b"world");
        assert_ne!(a, b);
    }

    #[test]
    fn sha256_is_64_hex_chars() {
        let h = compute_sha256(b"anything");
        assert_eq!(h.to_hex().len(), 64);
    }

    // ── detect_mime ────────────────────────────────────────────────────────────

    #[test]
    fn detects_png() {
        let png = &[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];
        assert_eq!(detect_mime(png), "image/png");
    }

    #[test]
    fn detects_jpeg() {
        let jpeg = &[0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46];
        // tree_magic_mini reports JPEG as "image/jpeg".
        let mime = detect_mime(jpeg);
        assert!(mime.starts_with("image/"), "expected image/*, got: {mime}");
    }

    #[test]
    fn detects_pdf() {
        let pdf = b"%PDF-1.4\n%\xE2\xE3\xCF\xD3\n";
        let mime = detect_mime(pdf);
        assert_eq!(mime, "application/pdf");
    }

    #[test]
    fn detects_gif() {
        let gif = b"GIF89a\x00\x01\x00\x01\x00\x00\x00";
        let mime = detect_mime(gif);
        assert_eq!(mime, "image/gif");
    }

    #[test]
    fn detects_zip() {
        let zip = b"PK\x03\x04\x00\x00\x00\x00\x00\x00\x00\x00";
        assert_eq!(detect_mime(zip), "application/zip");
    }

    #[test]
    fn pk_zip_magic_normalises_octet_stream_to_zip() {
        // BUG-INGEST-05: a ZIP container tree_magic_mini mislabels as
        // octet-stream (e.g. a minimal OOXML doc) must still be routed as
        // application/zip (-> DocKind::Archive -> Tika), matching the
        // upload-edge allow-list normalisation.
        let mut bytes = b"PK\x03\x04".to_vec();
        bytes.extend_from_slice(&[0u8; 80]);
        assert_eq!(detect_mime(&bytes), "application/zip");
    }

    #[test]
    fn detects_plain_text() {
        // tree_magic_mini: no binary magic → "text/plain".
        assert_eq!(detect_mime(b"Hello, world!"), "text/plain");
    }

    #[test]
    fn empty_input_returns_x_empty() {
        assert_eq!(detect_mime(b""), "application/x-empty");
    }

    #[test]
    fn unknown_binary_defaults_to_text_plain() {
        // tree_magic_mini fallback for unrecognised data.
        let mime = detect_mime(b"\xDE\xAD\xBE\xEF");
        assert_eq!(mime, "text/plain");
    }

    // ── build_blob_key ─────────────────────────────────────────────────────────

    #[test]
    fn blob_key_has_tenant_prefix() {
        let key = build_blob_key(
            42,
            "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
        );
        assert!(key.starts_with("42/"), "got: {key}");
    }

    #[test]
    fn blob_key_contains_full_hex() {
        let hex = "abcd".repeat(16);
        let key = build_blob_key(1, &hex);
        assert_eq!(key, format!("1/{hex}"));
    }

    #[test]
    fn blob_key_different_tenants_different_prefixes() {
        let hex = "abcd".repeat(16);
        let a = build_blob_key(1, &hex);
        let b = build_blob_key(2, &hex);
        assert_ne!(a, b);
        assert!(a.starts_with("1/"));
        assert!(b.starts_with("2/"));
    }

    // ── mime_to_doc_kind ───────────────────────────────────────────────────────

    #[test]
    fn image_mime_maps_to_image() {
        assert_eq!(mime_to_doc_kind("image/png"), DocKind::Image);
        assert_eq!(mime_to_doc_kind("image/jpeg"), DocKind::Image);
        assert_eq!(mime_to_doc_kind("image/gif"), DocKind::Image);
        assert_eq!(mime_to_doc_kind("image/webp"), DocKind::Image);
    }

    #[test]
    fn audio_mime_maps_to_audio() {
        assert_eq!(mime_to_doc_kind("audio/mpeg"), DocKind::Audio);
        assert_eq!(mime_to_doc_kind("audio/wav"), DocKind::Audio);
        assert_eq!(mime_to_doc_kind("audio/ogg"), DocKind::Audio);
    }

    #[test]
    fn video_mime_maps_to_video() {
        assert_eq!(mime_to_doc_kind("video/mp4"), DocKind::Video);
        assert_eq!(mime_to_doc_kind("video/webm"), DocKind::Video);
    }

    #[test]
    fn archive_mimes_map_to_archive() {
        assert_eq!(mime_to_doc_kind("application/zip"), DocKind::Archive);
        assert_eq!(mime_to_doc_kind("application/x-tar"), DocKind::Archive);
        assert_eq!(mime_to_doc_kind("application/gzip"), DocKind::Archive);
        assert_eq!(mime_to_doc_kind("application/x-bzip2"), DocKind::Archive);
        assert_eq!(mime_to_doc_kind("application/x-xz"), DocKind::Archive);
        assert_eq!(mime_to_doc_kind("application/zstd"), DocKind::Archive);
    }

    #[test]
    fn document_mimes_map_to_document() {
        assert_eq!(mime_to_doc_kind("application/pdf"), DocKind::Document);
        assert_eq!(mime_to_doc_kind("application/msword"), DocKind::Document);
        assert_eq!(mime_to_doc_kind("application/rtf"), DocKind::Document);
        assert_eq!(mime_to_doc_kind("text/plain"), DocKind::Document);
        assert_eq!(mime_to_doc_kind("text/html"), DocKind::Document);
        assert_eq!(mime_to_doc_kind("text/markdown"), DocKind::Document);
        assert_eq!(mime_to_doc_kind("application/x-empty"), DocKind::Document);
    }

    #[test]
    fn unknown_mime_maps_to_binary() {
        assert_eq!(
            mime_to_doc_kind("application/octet-stream"),
            DocKind::Binary
        );
        assert_eq!(
            mime_to_doc_kind("application/x-msdownload"),
            DocKind::Binary
        );
        assert_eq!(mime_to_doc_kind("font/ttf"), DocKind::Binary);
    }

    #[test]
    fn open_xml_mimes_map_to_document() {
        // DOCX, PPTX, XLSX.
        assert_eq!(
            mime_to_doc_kind(
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            ),
            DocKind::Document
        );
        assert_eq!(
            mime_to_doc_kind("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
            DocKind::Document
        );
    }

    // ── infer_doc_kind ─────────────────────────────────────────────────────────

    #[test]
    fn all_images_yields_image_kind() {
        let mimes: Vec<String> = vec!["image/png".into(), "image/jpeg".into()];
        assert_eq!(infer_doc_kind(&mimes), DocKind::Image);
    }

    #[test]
    fn all_audio_yields_audio_kind() {
        let mimes: Vec<String> = vec!["audio/mpeg".into(), "audio/wav".into()];
        assert_eq!(infer_doc_kind(&mimes), DocKind::Audio);
    }

    #[test]
    fn all_video_yields_video_kind() {
        let mimes: Vec<String> = vec!["video/mp4".into()];
        assert_eq!(infer_doc_kind(&mimes), DocKind::Video);
    }

    #[test]
    fn all_documents_yields_document_kind() {
        let mimes: Vec<String> = vec!["text/plain".into(), "application/pdf".into()];
        assert_eq!(infer_doc_kind(&mimes), DocKind::Document);
    }

    #[test]
    fn mixed_mimes_yields_document_kind() {
        let mimes: Vec<String> = vec!["image/png".into(), "text/plain".into()];
        assert_eq!(infer_doc_kind(&mimes), DocKind::Document);
    }

    #[test]
    fn image_and_video_mixed_yields_document() {
        let mimes: Vec<String> = vec!["image/png".into(), "video/mp4".into()];
        assert_eq!(infer_doc_kind(&mimes), DocKind::Document);
    }

    #[test]
    fn empty_mimes_yields_binary() {
        assert_eq!(infer_doc_kind(&[]), DocKind::Binary);
    }

    #[test]
    fn single_image_yields_image() {
        let mimes: Vec<String> = vec!["image/webp".into()];
        assert_eq!(infer_doc_kind(&mimes), DocKind::Image);
    }

    #[test]
    fn three_images_yields_image() {
        let mimes: Vec<String> = vec!["image/png".into(), "image/jpeg".into(), "image/gif".into()];
        assert_eq!(infer_doc_kind(&mimes), DocKind::Image);
    }

    // ── DocumentBuilder::build_single ───────────────────────────────────────────

    #[test]
    fn build_single_creates_one_page_document() {
        let (doc, files) =
            DocumentBuilder::build_single(1, b"Hello, world!", Some("hello.txt"), None);

        assert_eq!(doc.page_count, 1);
        assert_eq!(doc.status, ProcessingStatus::Pending);
        assert_eq!(doc.tenant_id, 1);
        assert_eq!(doc.id, 0); // No DB write yet.
        assert_eq!(doc.title, None);
        assert_eq!(doc.summary, None);
        assert_eq!(doc.user_note, None);

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].page_no, 1);
        assert_eq!(files[0].page_label, None);
        assert_eq!(files[0].tenant_id, 1);
        assert_eq!(files[0].document_id, 0);
        assert_eq!(files[0].status, ProcessingStatus::Pending);
    }

    #[test]
    fn build_single_computes_sha256() {
        let (doc, files) = DocumentBuilder::build_single(1, b"hello", None, None);
        let h = compute_sha256(b"hello");
        assert_eq!(files[0].sha256, h);
        assert_eq!(files[0].blob_key, build_blob_key(1, &h.to_hex()));
        assert_eq!(doc.page_count, 1);
    }

    #[test]
    fn build_single_detects_text_mime() {
        let (_doc, files) = DocumentBuilder::build_single(1, b"plain text content", None, None);
        let mime = files[0].mime.as_deref().unwrap();
        assert_eq!(mime, "text/plain");
    }

    #[test]
    fn build_single_detects_png_mime() {
        let png = &[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00];
        let (_doc, files) = DocumentBuilder::build_single(1, png, Some("photo.png"), None);
        assert_eq!(files[0].mime.as_deref().unwrap(), "image/png");
    }

    #[test]
    fn build_single_detects_jpeg_mime() {
        let jpeg = &[0xFF, 0xD8, 0xFF, 0xE0];
        let (_doc, files) = DocumentBuilder::build_single(1, jpeg, Some("photo.jpg"), None);
        let mime = files[0].mime.as_deref().unwrap();
        assert!(mime.starts_with("image/"), "expected image/*, got: {mime}");
    }

    #[test]
    fn build_single_detects_pdf_mime() {
        let pdf = b"%PDF-1.4\n%\xE2\xE3\xCF\xD3\n";
        let (_doc, files) = DocumentBuilder::build_single(1, pdf, Some("doc.pdf"), None);
        assert_eq!(files[0].mime.as_deref().unwrap(), "application/pdf");
    }

    #[test]
    fn build_single_preserves_path_and_user_note() {
        let (doc, files) =
            DocumentBuilder::build_single(1, b"data", Some("report.txt"), Some("quarterly report"));
        assert_eq!(files[0].path.as_deref(), Some("report.txt"));
        assert_eq!(doc.user_note.as_deref(), Some("quarterly report"));
    }

    #[test]
    fn build_single_blob_key_has_tenant_prefix() {
        let (_doc, files) = DocumentBuilder::build_single(7, b"test", None, None);
        assert!(
            files[0].blob_key.starts_with("7/"),
            "got: {}",
            files[0].blob_key
        );
    }

    #[test]
    fn build_single_size_bytes_matches_input() {
        let data = vec![0u8; 1024];
        let (_doc, files) = DocumentBuilder::build_single(1, &data, None, None);
        assert_eq!(files[0].size_bytes, Some(1024));
    }

    #[test]
    fn build_single_empty_bytes() {
        let (doc, files) = DocumentBuilder::build_single(1, b"", None, None);
        assert_eq!(doc.page_count, 1);
        assert_eq!(files[0].size_bytes, Some(0));
        assert_eq!(files[0].mime.as_deref(), Some("application/x-empty"));
        // SHA-256 of empty is well-defined.
        assert_eq!(files[0].sha256.to_hex().len(), 64);
    }

    #[test]
    fn build_single_same_content_same_sha256() {
        let (_d1, f1) = DocumentBuilder::build_single(1, b"duplicate", None, None);
        let (_d2, f2) = DocumentBuilder::build_single(1, b"duplicate", None, None);
        // Same content within same tenant → same sha256 and blob_key.
        assert_eq!(f1[0].sha256, f2[0].sha256);
        assert_eq!(f1[0].blob_key, f2[0].blob_key);
    }

    #[test]
    fn build_single_same_content_different_tenants_different_blob_key() {
        let (_d1, f1) = DocumentBuilder::build_single(1, b"duplicate", None, None);
        let (_d2, f2) = DocumentBuilder::build_single(2, b"duplicate", None, None);
        // Same content, same sha256 but different blob keys (tenant-prefixed).
        assert_eq!(f1[0].sha256, f2[0].sha256);
        assert_ne!(f1[0].blob_key, f2[0].blob_key);
    }

    #[test]
    fn build_single_kind_is_document_for_text() {
        let (doc, _files) = DocumentBuilder::build_single(1, b"some text", None, None);
        assert_eq!(doc.kind, DocKind::Document);
    }

    #[test]
    fn build_single_kind_is_image_for_png() {
        let png = &[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];
        let (doc, _files) = DocumentBuilder::build_single(1, png, None, None);
        assert_eq!(doc.kind, DocKind::Image);
    }

    #[test]
    fn build_single_large_file() {
        // 1 MB of data.
        let data = vec![b'A'; 1_048_576];
        let (doc, files) = DocumentBuilder::build_single(1, &data, None, None);
        assert_eq!(doc.page_count, 1);
        assert_eq!(files[0].size_bytes, Some(1_048_576));
        assert_eq!(files[0].sha256.to_hex().len(), 64);
    }

    #[test]
    fn build_single_created_at_is_recent() {
        let before = Utc::now();
        let (_doc, _files) = DocumentBuilder::build_single(1, b"test", None, None);
        let after = Utc::now();
        // created_at should be within [before, after] (allowing 1s tolerance for
        // extremely fast execution).
        assert!(_doc.created_at >= before);
        assert!(_doc.created_at <= after);
    }

    #[test]
    fn build_single_meta_is_empty_object() {
        let (doc, files) = DocumentBuilder::build_single(1, b"test", None, None);
        assert_eq!(doc.meta, serde_json::json!({}));
        assert_eq!(files[0].meta, serde_json::json!({}));
    }

    // ── DocumentBuilder::build_multi ────────────────────────────────────────────

    #[test]
    fn build_multi_two_pages_correct_ordering() {
        let pages = [
            PageInput {
                bytes: b"first page",
                page_label: Some("front"),
                path: Some("front.png"),
            },
            PageInput {
                bytes: b"second page",
                page_label: Some("back"),
                path: Some("back.png"),
            },
        ];
        let (doc, files) = DocumentBuilder::build_multi(1, &pages, Some("my ID card"));

        assert_eq!(doc.page_count, 2);
        assert_eq!(doc.user_note.as_deref(), Some("my ID card"));

        assert_eq!(files.len(), 2);
        assert_eq!(files[0].page_no, 1);
        assert_eq!(files[0].page_label.as_deref(), Some("front"));
        assert_eq!(files[0].path.as_deref(), Some("front.png"));
        assert_eq!(files[1].page_no, 2);
        assert_eq!(files[1].page_label.as_deref(), Some("back"));
        assert_eq!(files[1].path.as_deref(), Some("back.png"));
    }

    #[test]
    fn build_multi_labels_preserved_in_order() {
        let pages = [
            PageInput {
                bytes: b"a",
                page_label: Some("p1"),
                path: None,
            },
            PageInput {
                bytes: b"b",
                page_label: Some("p2"),
                path: None,
            },
            PageInput {
                bytes: b"c",
                page_label: Some("p3"),
                path: None,
            },
        ];
        let (_doc, files) = DocumentBuilder::build_multi(1, &pages, None);
        assert_eq!(files.len(), 3);
        for (i, f) in files.iter().enumerate() {
            assert_eq!(f.page_no, (i + 1) as i32);
            assert_eq!(
                f.page_label.as_deref(),
                Some(format!("p{}", i + 1).as_str())
            );
        }
    }

    #[test]
    fn build_multi_dedup_same_content_same_sha256() {
        let pages = [
            PageInput {
                bytes: b"same",
                page_label: None,
                path: None,
            },
            PageInput {
                bytes: b"same",
                page_label: None,
                path: None,
            },
            PageInput {
                bytes: b"different",
                page_label: None,
                path: None,
            },
        ];
        let (_doc, files) = DocumentBuilder::build_multi(1, &pages, None);

        // First two files have identical content → identical sha256 + blob_key.
        assert_eq!(files[0].sha256, files[1].sha256);
        assert_eq!(files[0].blob_key, files[1].blob_key);
        // Third file is different.
        assert_ne!(files[0].sha256, files[2].sha256);
    }

    #[test]
    fn build_multi_empty_pages_yields_zero_count() {
        let (doc, files) = DocumentBuilder::build_multi(1, &[], None);
        assert_eq!(doc.page_count, 0);
        assert_eq!(doc.kind, DocKind::Binary);
        assert!(files.is_empty());
    }

    #[test]
    fn build_multi_kind_is_image_when_all_images() {
        let png = &[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];
        let pages = [
            PageInput {
                bytes: png,
                page_label: Some("front"),
                path: None,
            },
            PageInput {
                bytes: png,
                page_label: Some("back"),
                path: None,
            },
        ];
        let (doc, _files) = DocumentBuilder::build_multi(1, &pages, None);
        assert_eq!(doc.kind, DocKind::Image);
    }

    #[test]
    fn build_multi_kind_is_document_when_mixed() {
        let png = &[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];
        let pages = [
            PageInput {
                bytes: png,
                page_label: Some("front"),
                path: None,
            },
            PageInput {
                bytes: b"some text",
                page_label: Some("notes"),
                path: None,
            },
        ];
        let (doc, _files) = DocumentBuilder::build_multi(1, &pages, None);
        assert_eq!(doc.kind, DocKind::Document);
    }

    #[test]
    fn build_multi_no_labels_or_paths() {
        let pages = [
            PageInput {
                bytes: b"a",
                page_label: None,
                path: None,
            },
            PageInput {
                bytes: b"b",
                page_label: None,
                path: None,
            },
        ];
        let (doc, files) = DocumentBuilder::build_multi(1, &pages, None);
        assert_eq!(doc.page_count, 2);
        assert!(files.iter().all(|f| f.page_label.is_none()));
        assert!(files.iter().all(|f| f.path.is_none()));
    }

    // ── end-to-end: build then verify shape ────────────────────────────────────

    #[test]
    fn single_file_e2e_shape() {
        let (doc, files) = DocumentBuilder::build_single(
            42,
            b"Project Orion - Q3 status update",
            Some("status.md"),
            Some("quarterly update for the board"),
        );

        // Document-level assertions.
        assert_eq!(doc.tenant_id, 42);
        assert_eq!(doc.page_count, 1);
        assert_eq!(doc.status, ProcessingStatus::Pending);
        assert_eq!(doc.kind, DocKind::Document);
        assert_eq!(
            doc.user_note.as_deref(),
            Some("quarterly update for the board")
        );
        assert!(doc.title.is_none());
        assert!(doc.summary.is_none());
        assert!(doc.created_at <= Utc::now());

        // File-level assertions.
        assert_eq!(files.len(), 1);
        let f = &files[0];
        assert_eq!(f.tenant_id, 42);
        assert_eq!(f.page_no, 1);
        assert_eq!(f.status, ProcessingStatus::Pending);
        assert!(f.blob_key.starts_with("42/"));
        assert_eq!(f.blob_key.len(), 3 + 64); // "42/" + 64 hex chars
        assert_eq!(f.mime.as_deref(), Some("text/plain"));
        assert_eq!(f.path.as_deref(), Some("status.md"));
        assert_eq!(f.size_bytes, Some(32));
    }
}
