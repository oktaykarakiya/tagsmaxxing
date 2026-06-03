//! Plain-text extractor for Document-kind files (.txt, .md, .html, .csv, .log, etc.).
//!
//! Reads the raw bytes as UTF-8 and returns the full content as [`Extracted::text`].
//! For `text/csv` files, cell text is extracted with field values separated by
//! spaces so that individual cell content is searchable.
//! Does not produce page images — those are for visual/VLM documents (plan §2, §7).

use async_trait::async_trait;
use kb_core::extractor::{Extracted, Extractor, RawFile};

/// Extracts plain text from Document-kind files by interpreting their bytes as UTF-8.
///
/// For most text formats this is a straight decode. For `text/csv` (detected by
/// MIME type or `.csv` file extension) the extractor parses CSV rows and joins
/// cell values with spaces so that individual cell text is findable by full-text
/// search without comma adjacency.
///
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
///
/// ```rust
/// # use bytes::Bytes;
/// # use kb_core::extractor::{Extracted, Extractor, RawFile};
/// # use kb_core::kind::DocKind;
/// # use kb_extract::text::TextExtractor;
/// # async fn csv_example() -> anyhow::Result<()> {
/// let ex = TextExtractor;
/// let csv = "id,name\n1,hello\n2,world\n";
/// let raw = RawFile {
///     bytes: Bytes::from(csv),
///     mime: Some("text/csv".into()),
///     kind: DocKind::Document,
///     path: Some("data.csv".into()),
/// };
/// let out = ex.extract(&raw).await?;
/// assert_eq!(out.text, "id name\n1 hello\n2 world");
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct TextExtractor;

#[async_trait]
impl Extractor for TextExtractor {
    async fn extract(&self, file: &RawFile) -> anyhow::Result<Extracted> {
        let raw = String::from_utf8(file.bytes.to_vec()).map_err(|e| {
            anyhow::anyhow!(
                "TextExtractor: file '{}' is not valid UTF-8: {e}",
                file.path.as_deref().unwrap_or("<unknown>")
            )
        })?;

        let text = if is_csv(file) { csv_to_text(&raw) } else { raw };

        Ok(Extracted {
            text,
            meta: serde_json::Value::Object(Default::default()),
            page_images: Vec::new(),
        })
    }
}

// ── CSV parsing ────────────────────────────────────────────────────────────────

/// Determine whether a [`RawFile`] should be treated as CSV.
///
/// Returns `true` when the MIME type is `text/csv` or when the file path
/// has a `.csv` extension (case-insensitive). This covers both the explicit
/// MIME case and the common real-world case where `tree_magic_mini` detects
/// CSV content as `text/plain` because CSV has no binary magic signature.
fn is_csv(file: &RawFile) -> bool {
    if file.mime.as_deref() == Some("text/csv") {
        return true;
    }
    file.path
        .as_deref()
        .and_then(|p| p.rsplit('.').next())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("csv"))
}

/// Convert raw CSV content to space-separated cell text.
///
/// Parses RFC 4180 compliant CSV with quoted fields (including embedded commas,
/// newlines, and escaped double-quotes). Each row becomes one line; within a row
/// cell values are joined by a single space. This makes individual cell content
/// findable by full-text search without comma-glued tokens.
///
/// Trailing newlines in the input are not reflected in the output (the last row
/// is never followed by a trailing newline).
///
/// # Examples
///
/// ```rust
/// use kb_extract::text::csv_to_text;
///
/// let csv = "id,name,score\n1,Alice,95\n2,Bob,87\n";
/// assert_eq!(csv_to_text(csv), "id name score\n1 Alice 95\n2 Bob 87");
/// ```
///
/// Quoted fields with embedded commas are handled:
///
/// ```rust
/// use kb_extract::text::csv_to_text;
///
/// let csv = "col1,col2\n\"San Francisco, CA\",42\n\"Austin, TX\",17\n";
/// assert_eq!(csv_to_text(csv), "col1 col2\nSan Francisco, CA 42\nAustin, TX 17");
/// ```
pub fn csv_to_text(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while chars.peek().is_some() {
        // Parse one row: collect cells until end-of-line or EOF.
        let mut first = true;
        loop {
            match chars.peek().copied() {
                // End of row — no more cells.
                Some('\r') | Some('\n') | None => break,
                // Space separator between cells (skip for the first cell).
                _ if !first => out.push(' '),
                _ => {}
            }
            first = false;

            match chars.peek().copied() {
                Some('"') => {
                    // Quoted field: read until closing `"`, handling `""` escapes.
                    chars.next(); // consume opening `"`
                    loop {
                        match chars.next() {
                            Some('"') => {
                                if chars.peek().copied() == Some('"') {
                                    // Escaped quote: `""` → literal `"`
                                    chars.next();
                                    out.push('"');
                                } else {
                                    // Closing quote.
                                    break;
                                }
                            }
                            Some(c) => out.push(c),
                            None => break, // EOF in quoted field.
                        }
                    }
                    // After closing quote, skip until comma or EOL/EOF.
                    skip_after_quoted_field(&mut chars);
                }

                Some(',') => {
                    // Empty unquoted field.
                    chars.next();
                }

                _ => {
                    // Unquoted field — read until comma, EOL, or EOF.
                    loop {
                        match chars.peek().copied() {
                            Some(',') => {
                                chars.next();
                                break;
                            }
                            Some('\r') | Some('\n') | None => break,
                            Some(c) => {
                                chars.next();
                                out.push(c);
                            }
                        }
                    }
                }
            }

            // If the next char is EOL or EOF the row is done.
            if matches!(chars.peek().copied(), Some('\r') | Some('\n') | None) {
                break;
            }
        }

        // Skip the row-terminating newline (CR, LF, or CRLF).
        match chars.peek().copied() {
            Some('\r') => {
                chars.next();
                if chars.peek().copied() == Some('\n') {
                    chars.next();
                }
            }
            Some('\n') => {
                chars.next();
            }
            None => {}
            _ => {} // not expected after row parsing.
        }

        // Append newline between rows, but not after the last one.
        if chars.peek().is_some() {
            out.push('\n');
        }
    }

    out
}

/// Skip characters after a quoted field's closing quote until a comma,
/// end-of-line, or EOF. This discards any whitespace between the closing
/// `"` and the next delimiter (RFC 4180 allows optional whitespace here).
fn skip_after_quoted_field(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) {
    loop {
        match chars.peek().copied() {
            Some(',') => {
                chars.next();
                break;
            }
            Some('\r') | Some('\n') | None => break,
            _ => {
                chars.next();
            }
        }
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

    fn csv_raw(bytes: &[u8]) -> RawFile {
        RawFile {
            bytes: Bytes::copy_from_slice(bytes),
            mime: Some("text/csv".into()),
            kind: DocKind::Document,
            path: Some("data.csv".into()),
        }
    }

    // ── TextExtractor (non-CSV) ─────────────────────────────────────────────

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

    // ── CSV extraction through TextExtractor ────────────────────────────────

    #[tokio::test]
    async fn csv_extracts_cell_text() {
        let ex = TextExtractor;
        let out = ex
            .extract(&csv_raw(b"id,name,score\n1,Alice,95\n2,Bob,87\n"))
            .await
            .unwrap();
        assert_eq!(out.text, "id name score\n1 Alice 95\n2 Bob 87");
    }

    #[tokio::test]
    async fn csv_handles_quoted_fields() {
        let ex = TextExtractor;
        let csv = "col1,col2\n\"San Francisco, CA\",42\n";
        let out = ex.extract(&csv_raw(csv.as_bytes())).await.unwrap();
        assert_eq!(out.text, "col1 col2\nSan Francisco, CA 42");
    }

    #[tokio::test]
    async fn csv_preserves_unicode_in_cells() {
        let ex = TextExtractor;
        let csv = "header\ncafé résumé\n";
        let out = ex.extract(&csv_raw(csv.as_bytes())).await.unwrap();
        assert_eq!(out.text, "header\ncafé résumé");
    }

    #[tokio::test]
    async fn csv_single_cell() {
        let ex = TextExtractor;
        let out = ex.extract(&csv_raw(b"only\n")).await.unwrap();
        assert_eq!(out.text, "only");
    }

    #[tokio::test]
    async fn csv_empty_file() {
        let ex = TextExtractor;
        let out = ex.extract(&csv_raw(b"")).await.unwrap();
        assert_eq!(out.text, "");
    }

    // ── csv_to_text unit tests ──────────────────────────────────────────────

    #[test]
    fn csv_simple() {
        assert_eq!(csv_to_text("a,b,c\n1,2,3\n"), "a b c\n1 2 3");
    }

    #[test]
    fn csv_no_trailing_newline_in_input() {
        assert_eq!(csv_to_text("a,b\nc,d"), "a b\nc d");
    }

    #[test]
    fn csv_quoted_field_with_comma() {
        assert_eq!(csv_to_text("\"hello, world\",foo\n"), "hello, world foo");
    }

    #[test]
    fn csv_escaped_quotes() {
        assert_eq!(
            csv_to_text("\"he said \"\"hi\"\"\",bar\n"),
            "he said \"hi\" bar"
        );
    }

    #[test]
    fn csv_empty_fields() {
        assert_eq!(csv_to_text("a,,c\n"), "a  c");
    }

    #[test]
    fn csv_crlf_line_endings() {
        assert_eq!(csv_to_text("x,y\r\n1,2\r\n"), "x y\n1 2");
    }

    #[test]
    fn csv_single_row_no_trailing_newline() {
        assert_eq!(csv_to_text("col1,col2"), "col1 col2");
    }

    #[test]
    fn csv_empty_input() {
        assert_eq!(csv_to_text(""), "");
    }

    #[test]
    fn csv_multiline_quoted_field() {
        // RFC 4180 allows newlines inside quoted fields.
        let csv = "\"line1\nline2\",b\n";
        assert_eq!(csv_to_text(csv), "line1\nline2 b");
    }

    #[test]
    fn csv_preserves_cell_content_with_marker() {
        // Regression test for BUG-INGEST-03: a unique marker inside a CSV cell
        // must survive the conversion to text so full-text search can find it.
        let marker = "z7x9q_marker_abc123";
        let csv = format!("id,description,amount\n1,{marker} quarterly revenue,4200\n");
        let text = csv_to_text(&csv);
        assert!(
            text.contains(marker),
            "CSV cell marker must appear in extracted text: {text}"
        );
    }

    // ── is_csv unit tests ──────────────────────────────────────────────────

    #[test]
    fn is_csv_true_for_text_csv_mime() {
        let raw = RawFile {
            bytes: Bytes::new(),
            mime: Some("text/csv".into()),
            kind: DocKind::Document,
            path: None,
        };
        assert!(is_csv(&raw));
    }

    #[test]
    fn is_csv_true_for_csv_extension() {
        let raw = RawFile {
            bytes: Bytes::new(),
            mime: Some("text/plain".into()),
            kind: DocKind::Document,
            path: Some("data.csv".into()),
        };
        assert!(is_csv(&raw));
    }

    #[test]
    fn is_csv_true_for_uppercase_extension() {
        let raw = RawFile {
            bytes: Bytes::new(),
            mime: Some("text/plain".into()),
            kind: DocKind::Document,
            path: Some("EXPORT.CSV".into()),
        };
        assert!(is_csv(&raw));
    }

    #[test]
    fn is_csv_false_for_txt_file() {
        let raw = RawFile {
            bytes: Bytes::new(),
            mime: Some("text/plain".into()),
            kind: DocKind::Document,
            path: Some("notes.txt".into()),
        };
        assert!(!is_csv(&raw));
    }

    #[test]
    fn is_csv_false_for_no_path_and_no_csv_mime() {
        let raw = RawFile {
            bytes: Bytes::new(),
            mime: None,
            kind: DocKind::Document,
            path: None,
        };
        assert!(!is_csv(&raw));
    }

    #[test]
    fn is_csv_false_for_path_without_extension() {
        let raw = RawFile {
            bytes: Bytes::new(),
            mime: Some("text/plain".into()),
            kind: DocKind::Document,
            path: Some("Makefile".into()),
        };
        assert!(!is_csv(&raw));
    }

    // ── Extension-based CSV extraction through TextExtractor ───────────────

    #[tokio::test]
    async fn csv_by_extension_with_text_plain_mime() {
        // tree_magic_mini detects CSV content as text/plain (BUG-INGEST-03).
        // The extractor must still parse CSV when the path ends in .csv.
        let ex = TextExtractor;
        let raw = RawFile {
            bytes: Bytes::from("id,name\n1,hello\n"),
            mime: Some("text/plain".into()),
            kind: DocKind::Document,
            path: Some("data.csv".into()),
        };
        let out = ex.extract(&raw).await.unwrap();
        assert_eq!(out.text, "id name\n1 hello");
    }

    #[tokio::test]
    async fn csv_by_extension_preserves_marker() {
        // The E2E test test_csv_content_is_searchable uploads a .csv file
        // that tree_magic_mini detects as text/plain. A unique marker inside
        // a CSV cell must appear in the extracted text so full-text search
        // can find it.
        let ex = TextExtractor;
        let marker = "z7x9q_marker_abc123";
        let csv = format!("id,description,amount\n1,{marker} quarterly revenue,4200\n");
        let raw = RawFile {
            bytes: Bytes::from(csv),
            mime: Some("text/plain".into()),
            kind: DocKind::Document,
            path: Some(format!("{marker}.csv")),
        };
        let out = ex.extract(&raw).await.unwrap();
        assert!(
            out.text.contains(marker),
            "CSV cell marker must appear in extracted text even when MIME is text/plain: {}",
            out.text
        );
    }
}
