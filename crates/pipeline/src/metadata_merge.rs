//! Metadata merge: combine per-page `(FileRecord, Extracted)` pairs into
//! document-level output — merged text, namespaced metadata, and a dominant
//! [`DocKind`] (plan §7 step 3, P3-T6).
//!
//! Pure logic, no I/O. The caller is responsible for sorting pages by
//! `page_no` before calling [`MetadataMerger::merge`].

use kb_core::extractor::{Extracted, PageImage};
use kb_core::file::FileRecord;
use kb_core::kind::DocKind;

use crate::document_builder;

/// The merged document-level output from per-page extractions.
///
/// Consumed by downstream pipeline steps: the merged text feeds the tagger
/// (§7 step 4), the merged meta is stored in `documents.meta` JSONB, and the
/// dominant kind + page count update the [`Document`](kb_core::document::Document).
#[derive(Debug, Clone, PartialEq)]
pub struct MergedDocument {
    /// All pages' extracted text joined with page-separator markers.
    pub merged_text: String,
    /// Per-page meta under `page_N.*` keys. Each `page_N` value merges the
    /// page's [`Extracted::meta`] with top-level [`FileRecord`] fields
    /// (`page_label`, `mime`, `path`, `size_bytes`, `sha256`, `blob_key`).
    pub merged_meta: serde_json::Value,
    /// Dominant document kind inferred from pages' MIME types.
    pub kind: DocKind,
    /// Number of pages (= `pages.len()`).
    pub page_count: i32,
    /// Page images collected from all pages for downstream VLM captioning.
    pub page_images: Vec<PageImage>,
}

/// Merges per-page extraction results into document-level output.
///
/// # Examples
///
/// ```rust
/// use kb_pipeline::metadata_merge::MetadataMerger;
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct MetadataMerger;

impl MetadataMerger {
    /// Merge per-page extraction results into a [`MergedDocument`].
    ///
    /// Pages **must** be ordered by `page_no`. An empty slice returns
    /// `page_count = 0`, `kind = Binary`, empty text, and `{}` meta.
    #[must_use]
    pub fn merge(pages: &[(FileRecord, Extracted)]) -> MergedDocument {
        let page_count = pages.len() as i32;
        let merged_text = merge_text(pages);
        let merged_meta = merge_meta(pages);
        let kind = dominant_kind(pages);
        let page_images = merge_images(pages);

        MergedDocument {
            merged_text,
            merged_meta,
            kind,
            page_count,
            page_images,
        }
    }
}

/// Collect all page images from per-page extraction results.
fn merge_images(pages: &[(FileRecord, Extracted)]) -> Vec<PageImage> {
    pages
        .iter()
        .flat_map(|(_, extracted)| extracted.page_images.clone())
        .collect()
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// Build a page-separator line from the [`FileRecord`]'s `page_no` and
/// optional `page_label`.
fn page_separator(file: &FileRecord) -> String {
    match &file.page_label {
        Some(label) => format!("--- page {} ({}) ---", file.page_no, label),
        None => format!("--- page {} ---", file.page_no),
    }
}

/// Concatenate all pages' extracted text separated by double-newlines, each
/// prefixed with a page-separator marker.
///
/// When a page's text is empty the separator still appears (so page
/// boundaries are always visible), but no trailing text line is appended.
fn merge_text(pages: &[(FileRecord, Extracted)]) -> String {
    if pages.is_empty() {
        return String::new();
    }

    let mut out = String::new();
    for (i, (file, extracted)) in pages.iter().enumerate() {
        if i > 0 {
            out.push_str("\n\n");
        }
        out.push_str(&page_separator(file));
        if !extracted.text.is_empty() {
            out.push('\n');
            out.push_str(&extracted.text);
        }
    }
    out
}

/// Merge per-page metadata into a namespaced JSON object.
///
/// Each page's entry at `page_N` combines:
/// 1. The page's [`Extracted::meta`] (shallow-merged keys).
/// 2. Top-level [`FileRecord`] fields: `page_label`, `mime`, `path`,
///    `size_bytes`, `sha256`, `blob_key`.
fn merge_meta(pages: &[(FileRecord, Extracted)]) -> serde_json::Value {
    let mut map = serde_json::Map::new();

    for (file, extracted) in pages {
        let key = format!("page_{}", file.page_no);
        let mut page_obj = serde_json::Map::new();

        // 1. Shallow-merge Extracted.meta.
        if let serde_json::Value::Object(extracted_map) = &extracted.meta {
            for (k, v) in extracted_map {
                page_obj.insert(k.clone(), v.clone());
            }
        }

        // 2. FileRecord top-level fields.
        if let Some(ref label) = file.page_label {
            page_obj.insert(
                "page_label".to_owned(),
                serde_json::Value::String(label.clone()),
            );
        }
        if let Some(ref mime) = file.mime {
            page_obj.insert("mime".to_owned(), serde_json::Value::String(mime.clone()));
        }
        if let Some(ref path) = file.path {
            page_obj.insert("path".to_owned(), serde_json::Value::String(path.clone()));
        }
        if let Some(size) = file.size_bytes {
            page_obj.insert(
                "size_bytes".to_owned(),
                serde_json::Value::Number(serde_json::Number::from(size)),
            );
        }
        page_obj.insert(
            "sha256".to_owned(),
            serde_json::Value::String(file.sha256.to_hex()),
        );
        page_obj.insert(
            "blob_key".to_owned(),
            serde_json::Value::String(file.blob_key.clone()),
        );

        map.insert(key, serde_json::Value::Object(page_obj));
    }

    serde_json::Value::Object(map)
}

/// Determine the dominant [`DocKind`] from the pages' MIME types.
///
/// Delegates to [`document_builder::infer_doc_kind`] for consistency with
/// the initial document-builder step. Pages with no MIME are treated as
/// `application/octet-stream` → [`DocKind::Binary`].
fn dominant_kind(pages: &[(FileRecord, Extracted)]) -> DocKind {
    if pages.is_empty() {
        return DocKind::Binary;
    }

    let mimes: Vec<String> = pages
        .iter()
        .map(|(file, _)| {
            file.mime
                .clone()
                .unwrap_or_else(|| "application/octet-stream".to_owned())
        })
        .collect();

    document_builder::infer_doc_kind(&mimes)
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use chrono::Utc;
    use kb_core::hash::Sha256;
    use kb_core::status::ProcessingStatus;

    /// Build a minimal [`FileRecord`] for testing.
    fn test_file(page_no: i32, page_label: Option<&str>, mime: Option<&str>) -> FileRecord {
        let mut hash = [0u8; 32];
        hash[0] = page_no as u8;
        FileRecord {
            id: 0,
            tenant_id: 1,
            document_id: 0,
            page_no,
            page_label: page_label.map(String::from),
            sha256: Sha256::from_bytes(hash),
            blob_key: format!("1/{}", "00".repeat(32)),
            path: None,
            mime: mime.map(String::from),
            size_bytes: Some(100),
            meta: serde_json::json!({}),
            status: ProcessingStatus::Pending,
            ingested_at: Utc::now(),
        }
    }

    /// Build [`Extracted`] with text and meta.
    fn test_extracted(text: &str, meta: serde_json::Value) -> Extracted {
        Extracted {
            text: text.to_owned(),
            meta,
            page_images: vec![],
        }
    }

    // ── page_separator ─────────────────────────────────────────────────────────

    #[test]
    fn separator_with_label() {
        let f = test_file(1, Some("front"), None);
        assert_eq!(page_separator(&f), "--- page 1 (front) ---");
    }

    #[test]
    fn separator_without_label() {
        let f = test_file(3, None, None);
        assert_eq!(page_separator(&f), "--- page 3 ---");
    }

    #[test]
    fn separator_uses_actual_page_no_not_index() {
        let f = test_file(5, Some("label"), None);
        assert_eq!(page_separator(&f), "--- page 5 (label) ---");
    }

    // ── merge_text ─────────────────────────────────────────────────────────────

    #[test]
    fn merge_text_empty() {
        assert_eq!(merge_text(&[]), "");
    }

    #[test]
    fn merge_text_single_page_no_label() {
        let f = test_file(1, None, Some("text/plain"));
        let e = test_extracted("Hello.", serde_json::json!({}));
        assert_eq!(merge_text(&[(f, e)]), "--- page 1 ---\nHello.");
    }

    #[test]
    fn merge_text_single_page_with_label() {
        let f = test_file(1, Some("front"), Some("text/plain"));
        let e = test_extracted("Front side.", serde_json::json!({}));
        assert_eq!(merge_text(&[(f, e)]), "--- page 1 (front) ---\nFront side.");
    }

    #[test]
    fn merge_text_single_page_empty_extracted_text() {
        let f = test_file(1, None, Some("image/png"));
        let e = test_extracted("", serde_json::json!({}));
        assert_eq!(merge_text(&[(f, e)]), "--- page 1 ---");
    }

    #[test]
    fn merge_text_two_pages() {
        let f1 = test_file(1, Some("front"), Some("text/plain"));
        let e1 = test_extracted("First.", serde_json::json!({}));
        let f2 = test_file(2, Some("back"), Some("text/plain"));
        let e2 = test_extracted("Second.", serde_json::json!({}));

        let text = merge_text(&[(f1, e1), (f2, e2)]);
        assert_eq!(
            text,
            "--- page 1 (front) ---\nFirst.\n\n--- page 2 (back) ---\nSecond."
        );
    }

    #[test]
    fn merge_text_two_pages_first_empty() {
        let f1 = test_file(1, None, Some("image/png"));
        let e1 = test_extracted("", serde_json::json!({}));
        let f2 = test_file(2, None, Some("text/plain"));
        let e2 = test_extracted("Notes.", serde_json::json!({}));

        let text = merge_text(&[(f1, e1), (f2, e2)]);
        assert_eq!(text, "--- page 1 ---\n\n--- page 2 ---\nNotes.");
    }

    #[test]
    fn merge_text_three_pages() {
        let pages: Vec<(FileRecord, Extracted)> = (1..=3)
            .map(|i| {
                (
                    test_file(i, None, Some("text/plain")),
                    test_extracted(&format!("Content {i}."), serde_json::json!({})),
                )
            })
            .collect();

        let text = merge_text(&pages);
        assert!(text.contains("--- page 1 ---\nContent 1."));
        assert!(text.contains("--- page 2 ---\nContent 2."));
        assert!(text.contains("--- page 3 ---\nContent 3."));
        assert_eq!(text.matches("\n\n").count(), 2);
    }

    // ── merge_meta ─────────────────────────────────────────────────────────────

    #[test]
    fn merge_meta_empty() {
        assert_eq!(merge_meta(&[]), serde_json::json!({}));
    }

    #[test]
    fn merge_meta_single_page_file_fields() {
        let mut f = test_file(1, Some("front"), Some("image/png"));
        f.path = Some("id.png".into());
        f.size_bytes = Some(2048);
        let e = test_extracted("", serde_json::json!({}));

        let meta = merge_meta(&[(f, e)]);
        let p = &meta["page_1"];
        assert_eq!(p["page_label"], "front");
        assert_eq!(p["mime"], "image/png");
        assert_eq!(p["path"], "id.png");
        assert_eq!(p["size_bytes"], 2048);
        assert!(p["sha256"].is_string());
        assert!(p["blob_key"].is_string());
    }

    #[test]
    fn merge_meta_includes_extracted_meta_keys() {
        let f = test_file(1, None, None);
        let e = test_extracted("", serde_json::json!({"camera": "Nikon", "iso": 800}));

        let meta = merge_meta(&[(f, e)]);
        assert_eq!(meta["page_1"]["camera"], "Nikon");
        assert_eq!(meta["page_1"]["iso"], 800);
    }

    #[test]
    fn merge_meta_two_pages_separate_namespaces() {
        let f1 = test_file(1, Some("front"), Some("image/png"));
        let e1 = test_extracted("", serde_json::json!({"side": "front"}));
        let f2 = test_file(2, Some("back"), Some("image/png"));
        let e2 = test_extracted("", serde_json::json!({"side": "back"}));

        let meta = merge_meta(&[(f1, e1), (f2, e2)]);
        assert_eq!(meta["page_1"]["side"], "front");
        assert_eq!(meta["page_1"]["page_label"], "front");
        assert_eq!(meta["page_2"]["side"], "back");
        assert_eq!(meta["page_2"]["page_label"], "back");
        assert!(meta["page_3"].is_null());
    }

    #[test]
    fn merge_meta_extracted_meta_null() {
        let f = test_file(1, Some("p1"), Some("text/plain"));
        let e = Extracted {
            text: "x".into(),
            meta: serde_json::Value::Null,
            page_images: vec![],
        };

        let meta = merge_meta(&[(f, e)]);
        assert!(meta["page_1"].is_object());
        assert_eq!(meta["page_1"]["page_label"], "p1");
    }

    #[test]
    fn merge_meta_size_bytes_none_omitted() {
        let mut f = test_file(1, None, None);
        f.size_bytes = None;
        let e = test_extracted("", serde_json::json!({}));

        let meta = merge_meta(&[(f, e)]);
        assert!(meta["page_1"].get("size_bytes").is_none());
    }

    #[test]
    fn merge_meta_page_label_none_omitted() {
        let f = test_file(1, None, None);
        let e = test_extracted("", serde_json::json!({}));

        let meta = merge_meta(&[(f, e)]);
        assert!(meta["page_1"].get("page_label").is_none());
    }

    // ── dominant_kind ──────────────────────────────────────────────────────────

    #[test]
    fn dominant_kind_empty() {
        assert_eq!(dominant_kind(&[]), DocKind::Binary);
    }

    #[test]
    fn dominant_kind_single_image() {
        let f = test_file(1, None, Some("image/png"));
        let e = test_extracted("", serde_json::json!({}));
        assert_eq!(dominant_kind(&[(f, e)]), DocKind::Image);
    }

    #[test]
    fn dominant_kind_three_images() {
        let mimes = ["image/png", "image/jpeg", "image/gif"];
        let pages: Vec<_> = mimes
            .iter()
            .enumerate()
            .map(|(i, m)| {
                (
                    test_file(i as i32 + 1, None, Some(m)),
                    test_extracted("", serde_json::json!({})),
                )
            })
            .collect();
        assert_eq!(dominant_kind(&pages), DocKind::Image);
    }

    #[test]
    fn dominant_kind_mixed_image_text() {
        let f1 = test_file(1, None, Some("image/png"));
        let e1 = test_extracted("", serde_json::json!({}));
        let f2 = test_file(2, None, Some("text/plain"));
        let e2 = test_extracted("notes", serde_json::json!({}));
        assert_eq!(dominant_kind(&[(f1, e1), (f2, e2)]), DocKind::Document);
    }

    #[test]
    fn dominant_kind_all_documents() {
        let mimes = ["text/plain", "application/pdf", "text/html"];
        let pages: Vec<_> = mimes
            .iter()
            .enumerate()
            .map(|(i, m)| {
                (
                    test_file(i as i32 + 1, None, Some(m)),
                    test_extracted("", serde_json::json!({})),
                )
            })
            .collect();
        assert_eq!(dominant_kind(&pages), DocKind::Document);
    }

    #[test]
    fn dominant_kind_no_mime_is_binary() {
        let f = test_file(1, None, None);
        let e = test_extracted("", serde_json::json!({}));
        assert_eq!(dominant_kind(&[(f, e)]), DocKind::Binary);
    }

    // ── MetadataMerger::merge (end-to-end) ─────────────────────────────────────

    #[test]
    fn merge_single_page_e2e() {
        let mut f = test_file(1, Some("front"), Some("text/plain"));
        f.path = Some("note.txt".into());
        let e = test_extracted(
            "The quick brown fox.",
            serde_json::json!({"author": "test"}),
        );

        let m = MetadataMerger::merge(&[(f, e)]);

        assert_eq!(m.page_count, 1);
        assert_eq!(m.kind, DocKind::Document);
        assert!(m.merged_text.contains("page 1 (front)"));
        assert!(m.merged_text.contains("The quick brown fox."));
        assert_eq!(m.merged_meta["page_1"]["author"], "test");
        assert_eq!(m.merged_meta["page_1"]["path"], "note.txt");
    }

    #[test]
    fn merge_two_pages_e2e() {
        let f1 = test_file(1, Some("front"), Some("image/png"));
        let e1 = test_extracted("", serde_json::json!({"exif": "f/2.8"}));
        let mut f2 = test_file(2, Some("back"), Some("image/png"));
        f2.path = Some("back.png".into());
        let e2 = test_extracted("", serde_json::json!({"exif": "f/4.0"}));

        let m = MetadataMerger::merge(&[(f1, e1), (f2, e2)]);

        assert_eq!(m.page_count, 2);
        assert_eq!(m.kind, DocKind::Image);
        assert_eq!(m.merged_meta["page_1"]["exif"], "f/2.8");
        assert_eq!(m.merged_meta["page_2"]["exif"], "f/4.0");
        assert_eq!(m.merged_meta["page_2"]["path"], "back.png");
        assert!(m.merged_text.contains("--- page 1 (front) ---"));
        assert!(m.merged_text.contains("--- page 2 (back) ---"));
    }

    #[test]
    fn merge_three_pages_e2e() {
        let pages: Vec<(FileRecord, Extracted)> = (1..=3)
            .map(|i| {
                (
                    test_file(i, None, Some("text/plain")),
                    test_extracted(&format!("Content {i}"), serde_json::json!({"num": i})),
                )
            })
            .collect();

        let m = MetadataMerger::merge(&pages);
        assert_eq!(m.page_count, 3);
        assert_eq!(m.kind, DocKind::Document);
        assert_eq!(m.merged_meta["page_1"]["num"], 1);
        assert_eq!(m.merged_meta["page_2"]["num"], 2);
        assert_eq!(m.merged_meta["page_3"]["num"], 3);
    }

    #[test]
    fn merge_empty_e2e() {
        let m = MetadataMerger::merge(&[]);
        assert_eq!(m.page_count, 0);
        assert_eq!(m.kind, DocKind::Binary);
        assert_eq!(m.merged_text, "");
        assert_eq!(m.merged_meta, serde_json::json!({}));
    }

    #[test]
    fn merge_page_count_matches_input() {
        for n in [1, 2, 3, 5, 10] {
            let pages: Vec<_> = (0..n)
                .map(|i| {
                    (
                        test_file(i + 1, None, Some("text/plain")),
                        test_extracted("x", serde_json::json!({})),
                    )
                })
                .collect();
            assert_eq!(MetadataMerger::merge(&pages).page_count, n);
        }
    }

    #[test]
    fn merge_mixed_kinds_document() {
        let f1 = test_file(1, None, Some("image/png"));
        let e1 = test_extracted("", serde_json::json!({}));
        let f2 = test_file(2, None, Some("audio/mpeg"));
        let e2 = test_extracted("transcript", serde_json::json!({}));
        let f3 = test_file(3, None, Some("text/plain"));
        let e3 = test_extracted("notes", serde_json::json!({}));

        let m = MetadataMerger::merge(&[(f1, e1), (f2, e2), (f3, e3)]);
        assert_eq!(m.kind, DocKind::Document);
        assert_eq!(m.page_count, 3);
    }

    #[test]
    fn merge_no_key_collision_between_pages() {
        let f1 = test_file(1, None, Some("text/plain"));
        let e1 = test_extracted("a", serde_json::json!({"key": "v1"}));
        let f2 = test_file(2, None, Some("text/plain"));
        let e2 = test_extracted("b", serde_json::json!({"key": "v2"}));

        let m = MetadataMerger::merge(&[(f1, e1), (f2, e2)]);
        assert_eq!(m.merged_meta["page_1"]["key"], "v1");
        assert_eq!(m.merged_meta["page_2"]["key"], "v2");
    }

    // ── page_images preservation (VLM captioning) ─────────────────────────

    #[test]
    fn merge_preserves_page_images() {
        let f = test_file(1, None, Some("image/jpeg"));
        let mut e = test_extracted("", serde_json::json!({}));
        e.page_images = vec![PageImage {
            data: bytes::Bytes::from_static(b"\xff\xd8\xff"),
            mime: "image/jpeg".into(),
        }];

        let m = MetadataMerger::merge(&[(f, e)]);
        assert_eq!(m.page_images.len(), 1);
        assert_eq!(m.page_images[0].mime, "image/jpeg");
    }

    #[test]
    fn merge_page_images_empty_for_non_image_docs() {
        let f = test_file(1, None, Some("text/plain"));
        let e = test_extracted("hello", serde_json::json!({}));
        // Extracted::default() has empty page_images

        let m = MetadataMerger::merge(&[(f, e)]);
        assert!(m.page_images.is_empty());
    }
}
