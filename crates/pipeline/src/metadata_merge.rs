//! Metadata merge: combine per-page [`Extracted`](kb_core::extractor::Extracted) output
//! into document-level text, namespaced metadata, and a dominant [`DocKind`](kb_core::kind::DocKind)
//! (plan §7, P3-T6).
//!
//! Pure logic — no I/O, no async.

use kb_core::extractor::Extracted;
use kb_core::file::FileRecord;
use kb_core::kind::DocKind;

/// The merged document-level output from all per-page extractions.
#[derive(Debug, Clone)]
pub struct MergedDocument {
    /// All pages' extracted text joined with page separators.
    pub text: String,
    /// Per-page meta under `page_N.*` keys, plus a top-level `page_count`.
    pub meta: serde_json::Value,
    /// The dominant kind (all-same → that kind; mixed → [`DocKind::Document`]).
    pub kind: DocKind,
    /// Number of member pages.
    pub page_count: i32,
}

/// Merge per-page extraction results into document-level output.
///
/// # Panics
///
/// Panics if `pages` is empty (callers must guard against zero-page documents upstream).
pub fn merge(pages: &[(FileRecord, Extracted)]) -> MergedDocument {
    assert!(!pages.is_empty(), "merge requires at least one page");

    let page_count = pages.len() as i32;

    // ── merge text ─────────────────────────────────────────────────────
    let mut texts: Vec<String> = Vec::with_capacity(pages.len());
    for (i, (file, extracted)) in pages.iter().enumerate() {
        let label = file.page_label.as_deref().unwrap_or("").to_string();
        let separator = if label.is_empty() {
            format!("--- page {} ---", i + 1)
        } else {
            format!("--- page {} ({label}) ---", i + 1)
        };
        texts.push(separator);
        if !extracted.text.is_empty() {
            texts.push(extracted.text.clone());
        }
    }
    let text = texts.join("\n");

    // ── merge meta ─────────────────────────────────────────────────────
    let mut merged_meta = serde_json::Map::new();

    for (i, (_file, extracted)) in pages.iter().enumerate() {
        let key = format!("page_{}", i + 1);
        let page_meta = if extracted.meta.is_object() {
            extracted.meta.clone()
        } else if extracted.meta.is_null() {
            serde_json::json!({})
        } else {
            // Scalar/array → wrap under a "value" key so the page slot is
            // always an object.
            serde_json::json!({"value": extracted.meta})
        };
        merged_meta.insert(key, page_meta);
    }
    merged_meta.insert("page_count".to_string(), serde_json::json!(page_count));

    // ── dominant kind ──────────────────────────────────────────────────
    let first_kind = mime_to_kind(pages[0].1.meta.get("mime").and_then(|v| v.as_str()));
    let all_same = pages.iter().all(|(_, e)| {
        let k = mime_to_kind(e.meta.get("mime").and_then(|v| v.as_str()));
        k == first_kind
    });
    let kind = if all_same {
        first_kind
    } else {
        DocKind::Document
    };

    MergedDocument {
        text,
        meta: serde_json::Value::Object(merged_meta),
        kind,
        page_count,
    }
}

/// Map a MIME type string to the broadest [`DocKind`].
///
/// This is used to determine the dominant kind from per-page extraction
/// metadata, where the extractor may have stored a "mime" field.
fn mime_to_kind(mime: Option<&str>) -> DocKind {
    match mime {
        Some(m) if m.starts_with("image/") => DocKind::Image,
        Some(m) if m.starts_with("audio/") => DocKind::Audio,
        Some(m) if m.starts_with("video/") => DocKind::Video,
        Some(m)
            if m == "text/plain"
                || m == "text/html"
                || m == "text/markdown"
                || m.starts_with("application/pdf")
                || m.starts_with("application/msword")
                || m.starts_with("application/vnd.openxmlformats-officedocument")
                || m.starts_with("application/vnd.oasis.opendocument") =>
        {
            DocKind::Document
        }
        Some(m)
            if m == "application/zip"
                || m == "application/x-tar"
                || m == "application/gzip"
                || m == "application/x-bzip2"
                || m == "application/x-xz"
                || m == "application/zstd" =>
        {
            DocKind::Archive
        }
        _ => DocKind::Binary,
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use kb_core::extractor::Extracted;
    use kb_core::file::FileRecord;
    use kb_core::hash::Sha256;
    use kb_core::kind::DocKind;
    use kb_core::status::ProcessingStatus;

    use super::*;

    // ── helpers ────────────────────────────────────────────────────────

    fn file_rec(page_no: i32, label: &str, mime: &str) -> FileRecord {
        let mut hash = [0u8; 32];
        hash[0] = page_no as u8;
        hash[1] = label.as_bytes().first().copied().unwrap_or(0);
        FileRecord {
            id: 0,
            tenant_id: 1,
            document_id: 0,
            page_no,
            page_label: if label.is_empty() {
                None
            } else {
                Some(label.to_string())
            },
            sha256: Sha256::from_bytes(hash),
            blob_key: format!("t1/{label}"),
            path: Some(format!("/tmp/{label}")),
            mime: Some(mime.to_string()),
            size_bytes: Some(100),
            meta: serde_json::json!({}),
            status: ProcessingStatus::Pending,
            ingested_at: chrono::Utc::now(),
        }
    }

    fn extracted(text: &str, mime: &str) -> Extracted {
        Extracted {
            text: text.to_string(),
            meta: serde_json::json!({"mime": mime, "author": "test"}),
            page_images: vec![],
        }
    }

    fn empty_extracted() -> Extracted {
        Extracted {
            text: String::new(),
            meta: serde_json::json!({"mime": "text/plain"}),
            page_images: vec![],
        }
    }

    // ── single page ────────────────────────────────────────────────────

    #[test]
    fn single_page_text_passed_through() {
        let pages = vec![(
            file_rec(1, "front", "text/plain"),
            extracted("Hello world", "text/plain"),
        )];
        let merged = merge(&pages);
        assert!(merged.text.contains("Hello world"));
        assert!(merged.text.contains("--- page 1 (front) ---"));
        assert_eq!(merged.page_count, 1);
    }

    #[test]
    fn single_page_meta_preserved() {
        let pages = vec![(
            file_rec(1, "main", "text/plain"),
            extracted("content", "text/plain"),
        )];
        let merged = merge(&pages);
        let page_1 = &merged.meta["page_1"];
        assert_eq!(page_1["mime"], "text/plain");
        assert_eq!(page_1["author"], "test");
        assert_eq!(merged.meta["page_count"], serde_json::json!(1));
    }

    #[test]
    fn single_page_dominant_kind_matches() {
        let pages = vec![(
            file_rec(1, "img", "image/png"),
            Extracted {
                text: String::new(),
                meta: serde_json::json!({"mime": "image/png"}),
                page_images: vec![],
            },
        )];
        let merged = merge(&pages);
        assert_eq!(merged.kind, DocKind::Image);
    }

    // ── multi page ─────────────────────────────────────────────────────

    #[test]
    fn two_pages_both_texts_present_with_separators() {
        let pages = vec![
            (
                file_rec(1, "front", "text/plain"),
                extracted("First page content", "text/plain"),
            ),
            (
                file_rec(2, "back", "text/plain"),
                extracted("Second page content", "text/plain"),
            ),
        ];
        let merged = merge(&pages);
        assert!(merged.text.contains("--- page 1 (front) ---"));
        assert!(merged.text.contains("First page content"));
        assert!(merged.text.contains("--- page 2 (back) ---"));
        assert!(merged.text.contains("Second page content"));
        assert_eq!(merged.page_count, 2);
    }

    #[test]
    fn two_pages_meta_namespaced() {
        let pages = vec![
            (
                file_rec(1, "p1", "text/plain"),
                extracted("A", "text/plain"),
            ),
            (
                file_rec(2, "p2", "image/png"),
                Extracted {
                    text: String::new(),
                    meta: serde_json::json!({"mime": "image/png", "width": 640}),
                    page_images: vec![],
                },
            ),
        ];
        let merged = merge(&pages);
        assert_eq!(merged.meta["page_1"]["mime"], "text/plain");
        assert_eq!(merged.meta["page_2"]["mime"], "image/png");
        assert_eq!(merged.meta["page_2"]["width"], 640);
        assert_eq!(merged.meta["page_count"], serde_json::json!(2));
    }

    #[test]
    fn three_pages_three_namespaces() {
        let pages: Vec<(FileRecord, Extracted)> = (1..=3)
            .map(|i| {
                (
                    file_rec(i, &format!("p{i}"), "text/plain"),
                    extracted(&format!("content {i}"), "text/plain"),
                )
            })
            .collect();
        let merged = merge(&pages);
        assert!(merged.meta["page_1"].is_object());
        assert!(merged.meta["page_2"].is_object());
        assert!(merged.meta["page_3"].is_object());
        assert_eq!(merged.page_count, 3);
    }

    // ── kind inference ─────────────────────────────────────────────────

    #[test]
    fn all_images_yields_image_kind() {
        let pages: Vec<(FileRecord, Extracted)> = (1..=3)
            .map(|i| {
                (
                    file_rec(i, &format!("img{i}"), "image/jpeg"),
                    Extracted {
                        text: String::new(),
                        meta: serde_json::json!({"mime": "image/jpeg"}),
                        page_images: vec![],
                    },
                )
            })
            .collect();
        let merged = merge(&pages);
        assert_eq!(merged.kind, DocKind::Image);
    }

    #[test]
    fn mixed_image_and_text_yields_document() {
        let pages = vec![
            (
                file_rec(1, "img", "image/png"),
                Extracted {
                    text: String::new(),
                    meta: serde_json::json!({"mime": "image/png"}),
                    page_images: vec![],
                },
            ),
            (
                file_rec(2, "doc", "text/plain"),
                extracted("some text", "text/plain"),
            ),
        ];
        let merged = merge(&pages);
        assert_eq!(merged.kind, DocKind::Document);
    }

    #[test]
    fn all_audio_yields_audio_kind() {
        let pages = vec![(
            file_rec(1, "track", "audio/mpeg"),
            Extracted {
                text: "transcript".to_string(),
                meta: serde_json::json!({"mime": "audio/mpeg"}),
                page_images: vec![],
            },
        )];
        let merged = merge(&pages);
        assert_eq!(merged.kind, DocKind::Audio);
    }

    #[test]
    fn all_video_yields_video_kind() {
        let pages = vec![(
            file_rec(1, "clip", "video/mp4"),
            Extracted {
                text: "transcript".to_string(),
                meta: serde_json::json!({"mime": "video/mp4"}),
                page_images: vec![],
            },
        )];
        let merged = merge(&pages);
        assert_eq!(merged.kind, DocKind::Video);
    }

    #[test]
    fn archive_yields_archive_kind() {
        let pages = vec![(
            file_rec(1, "bundle", "application/zip"),
            Extracted {
                text: String::new(),
                meta: serde_json::json!({"mime": "application/zip"}),
                page_images: vec![],
            },
        )];
        let merged = merge(&pages);
        assert_eq!(merged.kind, DocKind::Archive);
    }

    #[test]
    fn unknown_mime_yields_binary_kind() {
        let pages = vec![(
            file_rec(1, "blob", "application/octet-stream"),
            Extracted {
                text: String::new(),
                meta: serde_json::json!({"mime": "application/octet-stream"}),
                page_images: vec![],
            },
        )];
        let merged = merge(&pages);
        assert_eq!(merged.kind, DocKind::Binary);
    }

    // ── edge cases ─────────────────────────────────────────────────────

    #[test]
    fn empty_text_pages_still_produce_separators() {
        let pages = vec![
            (file_rec(1, "front", "text/plain"), empty_extracted()),
            (file_rec(2, "back", "text/plain"), empty_extracted()),
        ];
        let merged = merge(&pages);
        assert!(merged.text.contains("--- page 1 (front) ---"));
        assert!(merged.text.contains("--- page 2 (back) ---"));
        assert_eq!(merged.page_count, 2);
    }

    #[test]
    fn pages_without_labels_use_default_numbering() {
        let pages = vec![(
            file_rec(1, "", "text/plain"),
            extracted("no label", "text/plain"),
        )];
        let merged = merge(&pages);
        assert!(merged.text.contains("--- page 1 ---"));
        assert!(!merged.text.contains("()"), "should not have empty parens");
    }

    #[test]
    fn empty_meta_page_becomes_empty_object() {
        let pages = vec![(
            file_rec(1, "p1", "text/plain"),
            Extracted {
                text: "x".to_string(),
                meta: serde_json::json!(null),
                page_images: vec![],
            },
        )];
        let merged = merge(&pages);
        assert!(merged.meta["page_1"].is_object());
        assert_eq!(merged.meta["page_1"], serde_json::json!({}));
    }

    #[test]
    fn scalar_meta_value_wrapped_under_value_key() {
        let pages = vec![(
            file_rec(1, "p1", "text/plain"),
            Extracted {
                text: "x".to_string(),
                meta: serde_json::json!("just a string"),
                page_images: vec![],
            },
        )];
        let merged = merge(&pages);
        assert_eq!(merged.meta["page_1"]["value"], "just a string");
    }

    #[test]
    fn mime_to_kind_document_mimes() {
        let docs = &[
            "text/plain",
            "text/html",
            "text/markdown",
            "application/pdf",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ];
        for m in docs {
            assert_eq!(mime_to_kind(Some(m)), DocKind::Document, "failed for {m}");
        }
    }

    #[test]
    fn mime_to_kind_archive_mimes() {
        let archives = &[
            "application/zip",
            "application/x-tar",
            "application/gzip",
            "application/x-xz",
            "application/zstd",
        ];
        for m in archives {
            assert_eq!(mime_to_kind(Some(m)), DocKind::Archive, "failed for {m}");
        }
    }

    #[test]
    fn mime_to_kind_none_is_binary() {
        assert_eq!(mime_to_kind(None), DocKind::Binary);
    }

    #[test]
    #[should_panic(expected = "merge requires at least one page")]
    fn empty_pages_panics() {
        merge(&[]);
    }

    #[test]
    fn page_count_matches_input_length() {
        for n in 1..=5 {
            let pages: Vec<(FileRecord, Extracted)> = (1..=n)
                .map(|i| {
                    (
                        file_rec(i, &format!("p{i}"), "text/plain"),
                        extracted("x", "text/plain"),
                    )
                })
                .collect();
            assert_eq!(merge(&pages).page_count, n);
        }
    }
}
