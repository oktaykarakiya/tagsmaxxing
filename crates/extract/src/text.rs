//! Plain-text extractor for Document-kind files (.txt, .md, .html, .csv, .log, etc.).
//!
//! Reads the raw bytes as UTF-8 and returns the full content as [`Extracted::text`].
//! For `text/csv` files, cell text is extracted with field values separated by
//! spaces so that individual cell content is searchable.
//! For `.md` and `.html` files, markup is stripped to produce clean plain text.
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
/// For Markdown (`.md`, `text/markdown`) and HTML (`.html`, `.htm`, `text/html`)
/// files, formatting markup is stripped and only the plain text body is retained.
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

        let text = if is_csv(file) {
            csv_to_text(&raw)
        } else if is_markdown(file) {
            strip_markdown(&raw)
        } else if is_html(file) {
            strip_html(&raw)
        } else {
            raw
        };

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

// ── Markdown / HTML handling ─────────────────────────────────────────────────

/// Determine whether a [`RawFile`] should be treated as Markdown.
///
/// Returns `true` when the MIME type is `text/markdown` or when the file path
/// has a `.md` or `.markdown` extension (case-insensitive). Like CSV,
/// `tree_magic_mini` detects Markdown content as `text/plain` because it has
/// no binary magic signature, so extension-based fallback is essential.
fn is_markdown(file: &RawFile) -> bool {
    if file.mime.as_deref() == Some("text/markdown") {
        return true;
    }
    file.path
        .as_deref()
        .and_then(|p| p.rsplit('.').next())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("md") || ext.eq_ignore_ascii_case("markdown"))
}

/// Determine whether a [`RawFile`] should be treated as HTML.
///
/// Returns `true` when the MIME type is `text/html` or when the file path
/// has a `.html` or `.htm` extension (case-insensitive).
fn is_html(file: &RawFile) -> bool {
    if file.mime.as_deref() == Some("text/html") {
        return true;
    }
    file.path
        .as_deref()
        .and_then(|p| p.rsplit('.').next())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("html") || ext.eq_ignore_ascii_case("htm"))
}

/// Strip HTML tags and decode common entities to produce plain text.
///
/// Removes all content between `<` and `>` and decodes the HTML entities
/// `&amp;`, `&lt;`, `&gt;`, `&quot;`, `&#39;`, `&apos;`, and `&nbsp;`.
/// Block-level closing tags (`</p>`, `</div>`, `</h1>`–`</h6>`, `</li>`,
/// `</tr>`, `<br>`, `<br/>`) are replaced with a newline to preserve
/// paragraph structure.
///
/// # Examples
///
/// ```rust
/// use kb_extract::text::strip_html;
///
/// let html = "<p>Hello <b>world</b> &amp; friends</p>";
/// assert_eq!(strip_html(html), "Hello world & friends\n");
/// ```
pub fn strip_html(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut in_tag = false;
    let mut tag_buf = String::new();
    // Iterate over `char`s (not raw bytes) so multi-byte UTF-8 — accented
    // Latin, CJK, RTL scripts, emoji — survives intact. Byte-level iteration
    // with `byte as char` would split each multi-byte sequence into separate
    // Latin-1 code points, corrupting the text into mojibake.
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '<' {
            in_tag = true;
            tag_buf.clear();
            continue;
        }
        if c == '>' {
            in_tag = false;
            if is_html_block_tag(&tag_buf) {
                out.push('\n');
            }
            continue;
        }
        if in_tag {
            tag_buf.push(c);
            continue;
        }
        if c == '&'
            && let Some(decoded) = decode_html_entity(&mut chars)
        {
            out.push(decoded);
            continue;
        }
        out.push(c);
    }

    collapse_newlines(&mut out);
    out
}

/// Check whether a raw tag name represents a block-level element that should
/// trigger a line break in the output.
fn is_html_block_tag(raw: &str) -> bool {
    let name = if let Some(stripped) = raw.strip_prefix('/') {
        stripped
    } else {
        raw
    }
    .split_ascii_whitespace()
    .next()
    .unwrap_or("");
    matches!(
        name,
        "p" | "div" | "h1" | "h2" | "h3" | "h4" | "h5" | "h6" | "li" | "tr" | "br"
    )
}

/// Try to decode the HTML entity that begins immediately after an `&`.
///
/// `chars` must be positioned just past the `&`. The entity name is read up to
/// its terminating `;`, accepting only ASCII alphanumerics and `#` (so a stray
/// `&` in running text never triggers a long scan). On success the name and the
/// `;` are consumed from the iterator and the decoded character is returned; on
/// failure the iterator is left untouched (a clone is used to probe) so the
/// caller emits the literal `&` and continues.
///
/// Recognises `&amp;`, `&lt;`, `&gt;`, `&quot;`, `&#39;`, `&apos;`, and
/// `&nbsp;`. Operating on `char`s keeps the surrounding text UTF-8 clean.
fn decode_html_entity(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Option<char> {
    let mut probe = chars.clone();
    let mut name = String::new();
    loop {
        match probe.next() {
            Some(';') => break,
            Some(c) if c.is_ascii_alphanumeric() || c == '#' => name.push(c),
            _ => return None,
        }
    }
    let decoded = match name.as_str() {
        "amp" => '&',
        "lt" => '<',
        "gt" => '>',
        "quot" => '"',
        "apos" | "#39" => '\'',
        "nbsp" => ' ',
        _ => return None,
    };
    // Commit: advance the real iterator past the entity name and the `;`.
    *chars = probe;
    Some(decoded)
}

/// Collapse runs of three or more consecutive newlines into two.
fn collapse_newlines(out: &mut String) {
    let mut result = String::with_capacity(out.len());
    let mut nl_count = 0usize;
    for c in out.chars() {
        if c == '\n' {
            nl_count += 1;
            // Drop leading newlines (a block element at the very start emits one);
            // collapse internal runs to at most two.
            if !result.is_empty() && nl_count <= 2 {
                result.push('\n');
            }
        } else {
            nl_count = 0;
            result.push(c);
        }
    }
    *out = result;
}

/// Strip Markdown formatting and return plain text.
///
/// Removes common Markdown syntax: ATX headings (`#`), bold/italic markers
/// (`**`, `*`, `__`, `_`), link syntax (`[text](url)` → `text`), image syntax
/// (`![alt](url)` → `alt`), inline code backticks, code fences, blockquote
/// markers (`>`), and horizontal rules. Backslash escapes are decoded.
/// Paragraph structure (blank lines) is preserved.
///
/// # Examples
///
/// ```rust
/// use kb_extract::text::strip_markdown;
///
/// let md = "# Title\n\nSome **bold** and *italic* text.\n";
/// let text = strip_markdown(md);
/// assert!(text.contains("Title"));
/// assert!(text.contains("bold"));
/// assert!(text.contains("italic"));
/// assert!(!text.contains('#'));
/// assert!(!text.contains("**"));
/// ```
pub fn strip_markdown(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while chars.peek().is_some() {
        let line_start = out.len();
        md_strip_line(&mut chars, &mut out);
        // Consume the line terminator so the outer loop always makes progress
        // (md_strip_line / md_strip_inline stop AT a '\r'/'\n' without consuming
        // it; without this, a blank line — or the newline after any line — would
        // spin the loop forever).
        let consumed_eol = md_consume_line_terminator(&mut chars);
        if out.len() > line_start && consumed_eol && chars.peek().is_some() {
            out.push('\n');
        }
    }

    collapse_newlines(&mut out);
    out
}

/// Strip formatting from a single Markdown line (block-level prefix + inline).
fn md_strip_line(chars: &mut std::iter::Peekable<std::str::Chars<'_>>, out: &mut String) {
    // Skip leading whitespace to check for block-level prefixes.
    let mut leading = String::new();
    let mut probe = chars.clone();
    while let Some(c) = probe.peek()
        && (*c == ' ' || *c == '\t')
    {
        leading.push(*c);
        probe.next();
    }
    let first = probe.peek().copied();

    match first {
        Some('#') => {
            *chars = probe;
            chars.next(); // first #
            while chars.peek().copied() == Some('#') {
                chars.next();
            }
            if chars.peek().copied() == Some(' ') {
                chars.next();
            }
            md_strip_inline(chars, out);
        }
        Some('>') => {
            *chars = probe;
            chars.next(); // '>'
            if chars.peek().copied() == Some(' ') {
                chars.next();
            }
            md_strip_inline(chars, out);
        }
        Some('`') => {
            let mut fp = probe.clone();
            let mut fence_count = 0usize;
            while fp.peek().copied() == Some('`') {
                fp.next();
                fence_count += 1;
            }
            if fence_count >= 3 {
                *chars = fp;
                md_skip_eol(chars);
                md_consume_fence_body(chars, out);
                return;
            }
            out.push_str(&leading);
            out.push('`');
            chars.next();
            md_strip_inline(chars, out);
        }
        Some('-') | Some('*') | Some('_') => {
            if md_is_hr(&probe) {
                *chars = probe;
                md_skip_eol(chars);
            } else {
                out.push_str(&leading);
                md_strip_inline(chars, out);
            }
        }
        _ => {
            out.push_str(&leading);
            md_strip_inline(chars, out);
        }
    }
}

/// Strip inline Markdown formatting on a single line.
fn md_strip_inline(chars: &mut std::iter::Peekable<std::str::Chars<'_>>, out: &mut String) {
    loop {
        match chars.peek().copied() {
            Some('\r') | Some('\n') | None => break,
            Some('\\') => {
                chars.next();
                if let Some(c) = chars.next() {
                    out.push(c);
                }
            }
            Some('!') => {
                chars.next();
                if chars.peek().copied() == Some('[') {
                    chars.next(); // '['
                    let alt = md_collect_until(chars, ']');
                    if chars.peek().copied() == Some('(') {
                        chars.next();
                        md_skip_parens(chars);
                    }
                    out.push_str(&alt);
                } else {
                    out.push('!');
                }
            }
            Some('[') => {
                chars.next();
                let text = md_collect_until(chars, ']');
                if chars.peek().copied() == Some('(') {
                    chars.next();
                    md_skip_parens(chars);
                }
                out.push_str(&text);
            }
            Some('*') => {
                chars.next();
                if chars.peek().copied() == Some('*') {
                    chars.next();
                    md_consume_until_str(chars, out, "**");
                } else {
                    md_consume_until_str(chars, out, "*");
                }
            }
            Some('_') => {
                // Intra-word underscores are literal in CommonMark (e.g.
                // `foo_bar_baz` is not emphasis). Only treat `_` as an emphasis
                // delimiter at a word boundary — when the preceding output char
                // is not alphanumeric.
                let intra_word = out.chars().next_back().is_some_and(char::is_alphanumeric);
                chars.next(); // consume the opening '_'
                if intra_word {
                    out.push('_');
                } else if chars.peek().copied() == Some('_') {
                    chars.next();
                    md_consume_until_str(chars, out, "__");
                } else {
                    md_consume_until_str(chars, out, "_");
                }
            }
            Some('`') => {
                chars.next();
                let mut code = String::new();
                loop {
                    match chars.next() {
                        Some('`') => break,
                        Some(c) => code.push(c),
                        None => {
                            out.push('`');
                            out.push_str(&code);
                            return;
                        }
                    }
                }
                out.push_str(&code);
            }
            Some(c) => {
                chars.next();
                out.push(c);
            }
        }
    }
}

/// Collect characters until a closing bracket, respecting nesting.
fn md_collect_until(chars: &mut std::iter::Peekable<std::str::Chars<'_>>, closing: char) -> String {
    let opening = if closing == ']' { '[' } else { closing };
    let mut text = String::new();
    let mut depth = 1i32;
    loop {
        match chars.next() {
            Some(c) if c == opening => {
                depth += 1;
                text.push(c);
            }
            Some(c) if c == closing => {
                depth -= 1;
                if depth == 0 {
                    break;
                }
                text.push(c);
            }
            Some(c) => text.push(c),
            None => break,
        }
    }
    text
}

/// Skip balanced parentheses: `( … )`.
fn md_skip_parens(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) {
    let mut depth = 1i32;
    loop {
        match chars.next() {
            Some('(') => depth += 1,
            Some(')') => {
                depth -= 1;
                if depth == 0 {
                    break;
                }
            }
            Some(_) => {}
            None => break,
        }
    }
}

/// Consume characters until the literal delimiter string is found, appending
/// intermediate characters to `out`.
fn md_consume_until_str(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    out: &mut String,
    delim: &str,
) {
    let delim_chars: Vec<char> = delim.chars().collect();
    loop {
        let mut probe = chars.clone();
        let mut matched = true;
        for &dc in &delim_chars {
            if probe.next() != Some(dc) {
                matched = false;
                break;
            }
        }
        if matched {
            for _ in 0..delim_chars.len() {
                chars.next();
            }
            return;
        }
        match chars.next() {
            Some(c) => out.push(c),
            None => return,
        }
    }
}

/// Skip to end of line without collecting characters.
fn md_skip_eol(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) {
    loop {
        match chars.peek().copied() {
            Some('\r') | Some('\n') | None => break,
            _ => {
                chars.next();
            }
        }
    }
}

/// Consume a single line terminator (`\n`, `\r`, or `\r\n`) if present.
///
/// Returns `true` if a terminator was consumed. Callers that strip a line stop
/// *at* the terminator without consuming it; the line loop calls this to advance
/// past it so each iteration makes progress (otherwise a blank line, or the
/// newline after any line, spins forever).
fn md_consume_line_terminator(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> bool {
    match chars.peek().copied() {
        Some('\r') => {
            chars.next();
            if chars.peek().copied() == Some('\n') {
                chars.next();
            }
            true
        }
        Some('\n') => {
            chars.next();
            true
        }
        _ => false,
    }
}

/// Check whether the next characters form a horizontal rule (---, ***, ___).
fn md_is_hr(probe: &std::iter::Peekable<std::str::Chars<'_>>) -> bool {
    let mut p = probe.clone();
    let ch = match p.peek().copied() {
        Some(c @ ('-' | '*' | '_')) => c,
        _ => return false,
    };
    let mut count = 0usize;
    while p.peek().copied() == Some(ch) {
        p.next();
        count += 1;
    }
    while p.peek().is_some_and(|c| *c == ' ' || *c == '\t') {
        p.next();
    }
    count >= 3 && matches!(p.peek().copied(), Some('\r') | Some('\n') | None)
}

/// Consume lines inside a code fence (between ``` and ``` markers).
fn md_consume_fence_body(chars: &mut std::iter::Peekable<std::str::Chars<'_>>, out: &mut String) {
    md_skip_crlf(chars);
    loop {
        if chars.peek().is_none() {
            return;
        }
        // Check for closing fence at start of line (after optional whitespace).
        let mut probe = chars.clone();
        while probe.peek().is_some_and(|c| *c == ' ' || *c == '\t') {
            probe.next();
        }
        let mut bt_count = 0usize;
        while probe.peek().copied() == Some('`') {
            probe.next();
            bt_count += 1;
        }
        if bt_count >= 3 {
            // Closing fence — skip it.
            *chars = probe;
            md_skip_eol(chars);
            return;
        }
        // Regular body line — keep content.
        md_strip_inline(chars, out);
        out.push('\n');
        md_skip_eol(chars);
        md_skip_crlf(chars);
    }
}

/// Skip a CR, LF, or CRLF sequence.
fn md_skip_crlf(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) {
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
        _ => {}
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

    fn md_raw(bytes: &[u8]) -> RawFile {
        RawFile {
            bytes: Bytes::copy_from_slice(bytes),
            mime: Some("text/plain".into()),
            kind: DocKind::Document,
            path: Some("notes.md".into()),
        }
    }
    // ── TextExtractor (non-MD, non-HTML) ──────────────────────────────────

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
        let out = ex.extract(&md_raw(md.as_bytes())).await.unwrap();
        assert!(out.text.contains("Title"), "stripped: {}", out.text);
        assert!(out.text.contains("bold"), "stripped: {}", out.text);
        assert!(
            !out.text.contains('#'),
            "heading # not stripped: {}",
            out.text
        );
        assert!(
            !out.text.contains("**"),
            "bold ** not stripped: {}",
            out.text
        );
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

    // ── is_markdown / is_html unit tests ───────────────────────────────────

    #[test]
    fn is_markdown_true_for_text_markdown_mime() {
        let raw = RawFile {
            bytes: Bytes::new(),
            mime: Some("text/markdown".into()),
            kind: DocKind::Document,
            path: None,
        };
        assert!(is_markdown(&raw));
    }

    #[test]
    fn is_markdown_true_for_md_extension() {
        let raw = RawFile {
            bytes: Bytes::new(),
            mime: Some("text/plain".into()),
            kind: DocKind::Document,
            path: Some("readme.md".into()),
        };
        assert!(is_markdown(&raw));
    }

    #[test]
    fn is_markdown_true_for_markdown_extension() {
        let raw = RawFile {
            bytes: Bytes::new(),
            mime: None,
            kind: DocKind::Document,
            path: Some("CHANGELOG.MARKDOWN".into()),
        };
        assert!(is_markdown(&raw));
    }

    #[test]
    fn is_markdown_false_for_txt_file() {
        let raw = RawFile {
            bytes: Bytes::new(),
            mime: Some("text/plain".into()),
            kind: DocKind::Document,
            path: Some("notes.txt".into()),
        };
        assert!(!is_markdown(&raw));
    }

    #[test]
    fn is_html_true_for_text_html_mime() {
        let raw = RawFile {
            bytes: Bytes::new(),
            mime: Some("text/html".into()),
            kind: DocKind::Document,
            path: None,
        };
        assert!(is_html(&raw));
    }

    #[test]
    fn is_html_true_for_html_extension() {
        let raw = RawFile {
            bytes: Bytes::new(),
            mime: Some("text/plain".into()),
            kind: DocKind::Document,
            path: Some("index.html".into()),
        };
        assert!(is_html(&raw));
    }

    #[test]
    fn is_html_true_for_htm_extension() {
        let raw = RawFile {
            bytes: Bytes::new(),
            mime: None,
            kind: DocKind::Document,
            path: Some("legacy.HTM".into()),
        };
        assert!(is_html(&raw));
    }

    #[test]
    fn is_html_false_for_txt_file() {
        let raw = RawFile {
            bytes: Bytes::new(),
            mime: Some("text/plain".into()),
            kind: DocKind::Document,
            path: Some("notes.txt".into()),
        };
        assert!(!is_html(&raw));
    }

    // ── strip_html unit tests ──────────────────────────────────────────────

    #[test]
    fn strip_html_removes_tags() {
        assert_eq!(strip_html("<p>Hello <b>world</b></p>"), "Hello world\n");
    }

    #[test]
    fn strip_html_decodes_entities() {
        assert_eq!(
            strip_html("a &amp; b &lt; c &gt; d &quot; e &#39; f &nbsp; g"),
            "a & b < c > d \" e ' f   g"
        );
    }

    #[test]
    fn strip_html_block_tags_add_newlines() {
        let html = "<div><h1>A</h1><p>B</p></div><p>C</p><br>D";
        let text = strip_html(html);
        assert!(text.contains('A'));
        assert!(text.contains('B'));
        assert!(text.contains('C'));
        assert!(text.contains('D'));
        assert!(!text.contains('<'));
    }

    #[test]
    fn strip_html_empty_input() {
        assert_eq!(strip_html(""), "");
    }

    #[test]
    fn strip_html_plain_text_passthrough() {
        assert_eq!(strip_html("hello world"), "hello world");
    }

    #[test]
    fn strip_html_preserves_body_marker() {
        let marker = "e2e_html_marker_abc123";
        let html =
            format!("<html><body><p>Content with {marker} inside a paragraph.</p></body></html>");
        let text = strip_html(&html);
        assert!(
            text.contains(marker),
            "HTML body marker must appear in stripped text: {text}"
        );
    }

    #[test]
    fn strip_html_preserves_unicode_emoji_rtl() {
        // Multi-byte UTF-8 must survive intact: accented Latin (2 bytes), CJK
        // (3 bytes), RTL Arabic (2 bytes), and emoji (4 bytes). Byte-level
        // iteration with `byte as char` would mangle every one of these.
        let html = "<p>café résumé 日本語 العربية مرحبا Ωμέγα 😀🎉🚀</p>";
        let text = strip_html(html);
        for needle in ["café résumé", "日本語", "العربية مرحبا", "Ωμέγα", "😀🎉🚀"]
        {
            assert!(text.contains(needle), "lost {needle:?} in: {text:?}");
        }
        // No mojibake leaked through (the classic é → "Ã©" corruption).
        assert!(!text.contains('Ã'), "mojibake in stripped text: {text:?}");
    }

    #[test]
    fn strip_html_unicode_alongside_entities() {
        // Entities decode correctly and the surrounding multi-byte text is
        // preserved byte-for-byte.
        let text = strip_html("日本語 &amp; café &lt;😀&gt;");
        assert_eq!(text, "日本語 & café <😀>");
    }

    #[test]
    fn strip_html_entity_then_multibyte() {
        // Regression: after committing an entity the iterator must still be on a
        // valid char boundary so the following multi-byte char is intact.
        assert_eq!(strip_html("&amp;😀"), "&😀");
    }

    // ── strip_markdown unit tests ──────────────────────────────────────────

    #[test]
    fn strip_md_strips_atx_heading() {
        let text = strip_markdown("# Title\n\nBody\n");
        assert!(text.contains("Title"));
        assert!(!text.contains('#'));
    }

    #[test]
    fn strip_md_strips_bold_and_italic() {
        let text = strip_markdown("Some **bold** and *italic* and __more__ and _em_.");
        assert!(text.contains("bold"));
        assert!(text.contains("italic"));
        assert!(text.contains("more"));
        assert!(text.contains("em"));
        assert!(!text.contains("**"));
        assert!(!text.contains("__"));
    }

    #[test]
    fn strip_md_strips_links() {
        let text = strip_markdown("See [the docs](https://example.com) for info.");
        assert!(text.contains("the docs"));
        assert!(!text.contains("https://example.com"));
        assert!(!text.contains("]("));
    }

    #[test]
    fn strip_md_strips_images() {
        let text = strip_markdown("Logo: ![alt text](logo.png) here.");
        assert!(text.contains("alt text"));
        assert!(!text.contains("logo.png"));
        assert!(!text.contains("!["));
    }

    #[test]
    fn strip_md_strips_inline_code() {
        let text = strip_markdown("Use `cargo build` to compile.");
        assert!(text.contains("cargo build"));
        assert!(!text.contains('`'));
    }

    #[test]
    fn strip_md_strips_code_fence() {
        let md = "Before\n```rust\nfn main() {}\n```\nAfter\n";
        let text = strip_markdown(md);
        assert!(text.contains("Before"));
        assert!(text.contains("fn main() {}"));
        assert!(text.contains("After"));
        assert!(!text.contains("```"));
    }

    #[test]
    fn strip_md_strips_blockquote() {
        let text = strip_markdown("> Quoted text here\n");
        assert!(text.contains("Quoted text here"));
        assert!(!text.contains('>'));
    }

    #[test]
    fn strip_md_strips_horizontal_rules() {
        let md = "Before\n---\nAfter\n";
        let text = strip_markdown(md);
        assert!(text.contains("Before"));
        assert!(text.contains("After"));
        assert!(!text.contains("---"));
    }

    #[test]
    fn strip_md_empty_input() {
        assert_eq!(strip_markdown(""), "");
    }

    #[test]
    fn strip_md_plain_text_passthrough() {
        let text = strip_markdown("Just plain text.");
        assert_eq!(text, "Just plain text.");
    }

    #[test]
    fn strip_md_preserves_body_marker() {
        let marker = "e2e_md_marker_abc123";
        let md = format!(
            "# Quarterly Report\n\nThis markdown note references {marker} within the body.\n"
        );
        let text = strip_markdown(&md);
        assert!(
            text.contains(marker),
            "Markdown body marker must appear in stripped text: {text}"
        );
    }

    #[test]
    fn strip_md_decodes_backslash_escapes() {
        let text = strip_markdown(r"Escaped \*asterisks\* here");
        assert!(text.contains("*asterisks*"));
        assert!(!text.contains("\\*"));
    }

    // ── Markdown/HTML extraction through TextExtractor ─────────────────────

    #[tokio::test]
    async fn markdown_by_extension_with_text_plain_mime() {
        let ex = TextExtractor;
        let raw = RawFile {
            bytes: Bytes::from("# Hello\n\nWorld\n"),
            mime: Some("text/plain".into()),
            kind: DocKind::Document,
            path: Some("readme.md".into()),
        };
        let out = ex.extract(&raw).await.unwrap();
        assert!(out.text.contains("Hello"));
        assert!(out.text.contains("World"));
        assert!(!out.text.contains('#'));
    }

    #[tokio::test]
    async fn html_by_extension_with_text_plain_mime() {
        let ex = TextExtractor;
        let raw = RawFile {
            bytes: Bytes::from("<p>Hello <b>World</b></p>"),
            mime: Some("text/plain".into()),
            kind: DocKind::Document,
            path: Some("page.html".into()),
        };
        let out = ex.extract(&raw).await.unwrap();
        assert!(out.text.contains("Hello"));
        assert!(out.text.contains("World"));
        assert!(!out.text.contains('<'));
    }

    #[tokio::test]
    async fn html_unicode_emoji_rtl_roundtrips_through_extractor() {
        // Ingesting an HTML file with mixed scripts + emoji must yield clean,
        // searchable UTF-8 — not mojibake (BUG-INGEST-09).
        let ex = TextExtractor;
        let raw = RawFile {
            bytes: Bytes::from("<p>café 日本語 العربية مرحبا 😀🎉</p>"),
            mime: Some("text/html".into()),
            kind: DocKind::Document,
            path: Some("note.html".into()),
        };
        let out = ex.extract(&raw).await.unwrap();
        assert!(out.text.contains("café"), "stripped: {:?}", out.text);
        assert!(out.text.contains("日本語"), "stripped: {:?}", out.text);
        assert!(
            out.text.contains("العربية مرحبا"),
            "stripped: {:?}",
            out.text
        );
        assert!(out.text.contains("😀🎉"), "stripped: {:?}", out.text);
        assert!(
            std::str::from_utf8(out.text.as_bytes()).is_ok(),
            "extracted text must be valid UTF-8"
        );
    }
}
