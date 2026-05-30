//! Plain-text extractor for Document-kind files (.txt, .md, .html, .log, etc.).
//!
//! Reads the raw bytes as UTF-8 and returns the full content as [`Extracted::text`].
//! Does not produce page images — those are for visual/VLM documents (plan §2, §7).

use async_trait::async_trait;
use kb_core::extractor::{Extracted, Extractor, RawFile};

/// Extracts plain text from Document-kind files by interpreting their bytes as UTF-8.
///
/// This is the simplest extractor: it just decodes the bytes into a Rust `String`.
/// Any non-UTF-8 content is rejected with a clear error.
///
/// # Examples
///
/// ```rust
/// use bytes::Bytes;
/// use kb_core::extractor::{Extracted, Extractor, RawFile};
/// use kb_core::kind::DocKind;
/// use kb_extract::text::TextExtractor;
///
/// # async fn example() -> anyhow::Result<()> {
/// let ex = TextExtractor;
/// let raw = RawFile {
///     bytes: Bytes::from("hello world\n"),
///     mime: Some("text/plain".into()),
///     kind: DocKind::Document,
///     path: Some("notes.txt".into()),
/// };
/// let out = ex.extract(&raw).await?;
/// assert_eq!(out.text, "hello world\n");
/// assert!(out.page_images.is_empty());
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct TextExtractor;

#[async_trait]
impl Extractor for TextExtractor {
    async fn extract(&self, file: &RawFile) -> anyhow::Result<Extracted> {
        let text = String::from_utf8(file.bytes.to_vec()).map_err(|e| {
            anyhow::anyhow!(
                "TextExtractor: file '{}' is not valid UTF-8: {e}",
                file.path.as_deref().unwrap_or("<unknown>")
            )
        })?;
        Ok(Extracted {
            text,
            meta: serde_json::Value::Object(Default::default()),
            page_images: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use bytes::Bytes;
    use kb_core::extractor::RawFile;
    use kb_core::kind::DocKind;

    fn doc_raw(bytes: &[u8]) -> RawFile {
        RawFile {
            bytes: Bytes::copy_from_slice(bytes),
            mime: Some("text/plain".into()),
            kind: DocKind::Document,
            path: Some("test.txt".into()),
        }
    }

    #[tokio::test]
    async fn extracts_valid_utf8() {
        let ex = TextExtractor;
        let out = ex.extract(&doc_raw(b"hello world\n")).await.unwrap();
        assert_eq!(out.text, "hello world\n");
    }

    #[tokio::test]
    async fn extracts_empty_input() {
        let ex = TextExtractor;
        let out = ex.extract(&doc_raw(b"")).await.unwrap();
        assert_eq!(out.text, "");
        assert!(out.page_images.is_empty());
    }

    #[tokio::test]
    async fn meta_is_empty_object() {
        let ex = TextExtractor;
        let out = ex.extract(&doc_raw(b"some text")).await.unwrap();
        assert_eq!(out.meta, serde_json::json!({}));
    }

    #[tokio::test]
    async fn page_images_always_empty() {
        let ex = TextExtractor;
        let out = ex.extract(&doc_raw(b"content")).await.unwrap();
        assert!(out.page_images.is_empty());
    }

    #[tokio::test]
    async fn rejects_invalid_utf8() {
        let ex = TextExtractor;
        // Invalid UTF-8: 0xFF is never valid in UTF-8
        let err = ex.extract(&doc_raw(b"\xff\xfe")).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("not valid UTF-8"),
            "error should mention UTF-8, got: {msg}"
        );
    }

    #[tokio::test]
    async fn error_includes_filename() {
        let ex = TextExtractor;
        let raw = RawFile {
            bytes: Bytes::from(vec![0xff]),
            mime: Some("text/plain".into()),
            kind: DocKind::Document,
            path: Some("broken.txt".into()),
        };
        let err = ex.extract(&raw).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("broken.txt"),
            "error should mention filename, got: {msg}"
        );
    }

    #[tokio::test]
    async fn error_handles_missing_path() {
        let ex = TextExtractor;
        let raw = RawFile {
            bytes: Bytes::from(vec![0xff]),
            mime: None,
            kind: DocKind::Document,
            path: None,
        };
        let err = ex.extract(&raw).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("<unknown>"),
            "error should contain <unknown> when path is missing, got: {msg}"
        );
    }

    #[tokio::test]
    async fn handles_markdown_content() {
        let ex = TextExtractor;
        let md = "# Title\n\nSome **bold** text\n";
        let out = ex.extract(&doc_raw(md.as_bytes())).await.unwrap();
        assert_eq!(out.text, md);
    }

    #[tokio::test]
    async fn handles_multiline_utf8_with_special_chars() {
        let ex = TextExtractor;
        let content = "Line 1 — em dash\nLine 2: café résumé\nLine 3: 🦀\n";
        let out = ex.extract(&doc_raw(content.as_bytes())).await.unwrap();
        assert_eq!(out.text, content);
    }
}
