// SPDX-License-Identifier: AGPL-3.0-or-later

//! Source-code extractor for Code-kind files.
//!
//! Reads the raw bytes as UTF-8 and returns the full content as [`Extracted::text`].
//! Tree-sitter chunking is deferred to P3 (this task only does byte→text, per the
//! P2-T4 acceptance criteria in `BUILD_LEDGER.toml`).

use async_trait::async_trait;
use kb_core::extractor::{Extracted, Extractor, RawFile};

/// Extracts source code from Code-kind files by interpreting their bytes as UTF-8.
///
/// This extractor simply reads the raw bytes as a UTF-8 string. Chunking of code
/// into semantic units (via tree-sitter) is a separate step that runs later in the
/// pipeline (plan §2, §7 — deferred to P3).
///
/// # Examples
///
/// ```rust
/// use bytes::Bytes;
/// use kb_core::extractor::{Extracted, Extractor, RawFile};
/// use kb_core::kind::DocKind;
/// use kb_extract::code::CodeExtractor;
///
/// # async fn example() -> anyhow::Result<()> {
/// let ex = CodeExtractor;
/// let raw = RawFile {
///     bytes: Bytes::from("fn main() {\n    println!(\"hello\");\n}\n"),
///     mime: Some("text/plain".into()),
///     kind: DocKind::Code,
///     path: Some("main.rs".into()),
/// };
/// let out = ex.extract(&raw).await?;
/// assert_eq!(out.text, "fn main() {\n    println!(\"hello\");\n}\n");
/// assert!(out.page_images.is_empty());
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct CodeExtractor;

#[async_trait]
impl Extractor for CodeExtractor {
    async fn extract(&self, file: &RawFile) -> anyhow::Result<Extracted> {
        let text = String::from_utf8(file.bytes.to_vec()).map_err(|e| {
            anyhow::anyhow!(
                "CodeExtractor: file '{}' is not valid UTF-8: {e}",
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

    fn code_raw(bytes: &[u8], path: &str) -> RawFile {
        RawFile {
            bytes: Bytes::copy_from_slice(bytes),
            mime: Some("text/plain".into()),
            kind: DocKind::Code,
            path: Some(path.into()),
        }
    }

    #[tokio::test]
    async fn extracts_valid_utf8_source() {
        let ex = CodeExtractor;
        let src = "fn main() {\n    println!(\"hello\");\n}\n";
        let out = ex
            .extract(&code_raw(src.as_bytes(), "main.rs"))
            .await
            .unwrap();
        assert_eq!(out.text, src);
    }

    #[tokio::test]
    async fn extracts_empty_input() {
        let ex = CodeExtractor;
        let out = ex.extract(&code_raw(b"", "empty.rs")).await.unwrap();
        assert_eq!(out.text, "");
        assert!(out.page_images.is_empty());
    }

    #[tokio::test]
    async fn meta_is_empty_object() {
        let ex = CodeExtractor;
        let out = ex
            .extract(&code_raw(b"print('hi')", "script.py"))
            .await
            .unwrap();
        assert_eq!(out.meta, serde_json::json!({}));
    }

    #[tokio::test]
    async fn page_images_always_empty() {
        let ex = CodeExtractor;
        let out = ex
            .extract(&code_raw(b"// comment", "lib.rs"))
            .await
            .unwrap();
        assert!(out.page_images.is_empty());
    }

    #[tokio::test]
    async fn rejects_invalid_utf8() {
        let ex = CodeExtractor;
        let err = ex
            .extract(&code_raw(b"\xff\xfe", "bad.rs"))
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("not valid UTF-8"),
            "error should mention UTF-8, got: {msg}"
        );
    }

    #[tokio::test]
    async fn error_includes_filename() {
        let ex = CodeExtractor;
        let err = ex
            .extract(&code_raw(&[0xff], "broken.py"))
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("broken.py"),
            "error should mention filename, got: {msg}"
        );
    }

    #[tokio::test]
    async fn error_handles_missing_path() {
        let ex = CodeExtractor;
        let raw = RawFile {
            bytes: Bytes::from(vec![0xff]),
            mime: None,
            kind: DocKind::Code,
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
    async fn handles_multiline_utf8_with_special_chars() {
        let ex = CodeExtractor;
        let src = "// コメント\nlet s = \"café\";\n/* 🦀 */\n";
        let out = ex
            .extract(&code_raw(src.as_bytes(), "unicode.rs"))
            .await
            .unwrap();
        assert_eq!(out.text, src);
    }

    #[tokio::test]
    async fn handles_python_source() {
        let ex = CodeExtractor;
        let src = "def greet(name: str) -> str:\n    return f\"Hello, {name}!\"\n";
        let out = ex
            .extract(&code_raw(src.as_bytes(), "hello.py"))
            .await
            .unwrap();
        assert_eq!(out.text, src);
    }
}
