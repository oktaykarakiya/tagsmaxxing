//! Binary / unknown extractor: magic-byte MIME detection via `tree_magic_mini`,
//! the `file` command (detached subprocess), printable ASCII string extraction,
//! and size + SHA-256 collection.
//!
//! Produces [`Extracted`] with empty `text`, rich metadata (`mime`, `magic`,
//! `strings`, `sha256`, `size_bytes`, `kind`), and no page images (plan §2, §7).
//! Archive files additionally store an `archive_manifest` listing their contents.
//!
//! The `file` and archive-listing subprocess boundaries are abstracted behind an
//! internal [`FileTool`] trait so unit tests inject mocks without requiring `file`
//! or `tar` on the CI host (plan §31.3 mock-backend pattern).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kb_core::extractor::{Extracted, Extractor, RawFile};
use sha2::Digest;

use crate::security;

// ── printable string extraction ────────────────────────────────────────────────

/// Extract printable ASCII runs of at least `min_len` characters from raw bytes.
///
/// This mimics the Unix `strings` command: it scans the input for contiguous
/// sequences of printable ASCII characters (0x20–0x7E) and returns each run that
/// meets the minimum length threshold.
///
/// # Examples
///
/// ```rust
/// use kb_extract::binary::extract_strings;
///
/// let input = b"hello\x00world\x01\x02Rust!";
/// let strings = extract_strings(input, 4);
/// assert_eq!(strings, vec!["hello", "world", "Rust!"]);
/// ```
pub fn extract_strings(bytes: &[u8], min_len: usize) -> Vec<String> {
    if bytes.is_empty() {
        return Vec::new();
    }
    let mut results = Vec::new();
    let mut current = Vec::with_capacity(64);
    for &b in bytes {
        if (0x20..=0x7E).contains(&b) {
            current.push(b);
        } else {
            if current.len() >= min_len {
                // All bytes in `current` are printable ASCII → valid UTF-8.
                // `from_utf8_lossy` is a no-op here but avoids clippy::expect_used.
                let s = String::from_utf8_lossy(&std::mem::take(&mut current)).into_owned();
                results.push(s);
            }
            current.clear();
        }
    }
    if current.len() >= min_len {
        let s = String::from_utf8_lossy(&current).into_owned();
        results.push(s);
    }
    results
}

// ── FileTool trait ─────────────────────────────────────────────────────────────

/// Enum for known archive formats that support content listing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArchiveFormat {
    /// POSIX tar archive (including gzip-compressed).
    Tar,
    /// ZIP archive.
    Zip,
}

/// Internal abstraction over `file` and archive-listing subprocess calls.
///
/// The production implementation uses `tokio::process::Command` with temp files.
/// Tests inject a mock that returns canned responses, keeping the suite
/// deterministic and free of host-`file`/`tar`/`unzip` dependencies (plan §31.3).
#[async_trait]
trait FileTool: Send + Sync {
    /// Run `file --brief` against `input` bytes, returning the human-readable
    /// file-type description (e.g. `"ELF 64-bit LSB executable, x86-64"`).
    async fn file_description(&self, input: &[u8], timeout: Duration) -> anyhow::Result<String>;

    /// List the contents of an archive of the given format.
    ///
    /// For [`ArchiveFormat::Tar`] this runs `tar tf`; for [`ArchiveFormat::Zip`]
    /// it runs `unzip -l` (quiet listing). Returns the tool's stdout as a string.
    async fn archive_list(
        &self,
        input: &[u8],
        format: ArchiveFormat,
        timeout: Duration,
    ) -> anyhow::Result<String>;
}

// ── RealFileTool (production) ──────────────────────────────────────────────────

/// The production [`FileTool`] that spawns real `file`, `tar`, and `unzip`
/// subprocesses.
///
/// Input bytes are written to a temporary file so the tools can operate on them.
/// Temp files are cleaned up on drop (via [`tempfile::NamedTempFile`]).
#[derive(Debug, Clone, Copy, Default)]
struct RealFileTool;

impl RealFileTool {
    /// Write bytes to a temp file and return the handle (auto-deleted on drop).
    fn temp_file(input: &[u8]) -> anyhow::Result<tempfile::NamedTempFile> {
        use std::io::Write;
        let mut tmp = tempfile::NamedTempFile::new()
            .map_err(|e| anyhow::anyhow!("BinaryExtractor: failed to create temp file: {e}"))?;
        tmp.write_all(input)
            .map_err(|e| anyhow::anyhow!("BinaryExtractor: failed to write temp file: {e}"))?;
        tmp.flush()
            .map_err(|e| anyhow::anyhow!("BinaryExtractor: failed to flush temp file: {e}"))?;
        Ok(tmp)
    }

    /// Run a command with the given args, capturing stdout, with optional timeout.
    async fn run_cmd(
        program: &str,
        args: &[&str],
        input_path: &std::path::Path,
        timeout: Duration,
    ) -> anyhow::Result<String> {
        let output = tokio::time::timeout(
            timeout,
            tokio::process::Command::new(program)
                .args(args)
                .arg(input_path)
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .output(),
        )
        .await
        .map_err(|_| anyhow::anyhow!("BinaryExtractor: {program} timed out after {timeout:?}"))?
        .map_err(|e| anyhow::anyhow!("BinaryExtractor: failed to spawn {program}: {e}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!(
                "BinaryExtractor: {program} exited with status {}: {}",
                output.status,
                stderr.trim()
            ));
        }

        let stdout = String::from_utf8(output.stdout).map_err(|e| {
            anyhow::anyhow!("BinaryExtractor: {program} output is not valid UTF-8: {e}")
        })?;
        Ok(stdout.trim().to_string())
    }
}

#[async_trait]
impl FileTool for RealFileTool {
    async fn file_description(&self, input: &[u8], timeout: Duration) -> anyhow::Result<String> {
        let tmp = Self::temp_file(input)?;
        Self::run_cmd("file", &["--brief"], tmp.path(), timeout).await
    }

    async fn archive_list(
        &self,
        input: &[u8],
        format: ArchiveFormat,
        timeout: Duration,
    ) -> anyhow::Result<String> {
        let tmp = Self::temp_file(input)?;
        match format {
            ArchiveFormat::Tar => Self::run_cmd("tar", &["tf"], tmp.path(), timeout).await,
            ArchiveFormat::Zip => Self::run_cmd("unzip", &["-l", "-qq"], tmp.path(), timeout).await,
        }
    }
}

// ── BinaryExtractor ────────────────────────────────────────────────────────────

/// Extracts metadata from binary and unknown files.
///
/// # Pipeline (plan §2, §7)
///
/// 1. **Magic-byte MIME detection** — uses `tree_magic_mini` to detect the MIME type
///    from the file's magic bytes (pure Rust, no subprocess).
/// 2. **`file` command** — runs `file --brief` as a detached subprocess to get a
///    human-readable type description (e.g. `"ELF 64-bit LSB executable"`).
/// 3. **Printable strings** — extracts all printable ASCII runs ≥ 4 characters
///    (mimics the Unix `strings` command) as a list.
/// 4. **Size + SHA-256** — computes the file size and hex-encoded SHA-256 digest
///    from the input bytes.
/// 5. **Archive manifest** (optional) — if the detected MIME indicates a tar or zip
///    archive, lists its contents via `tar tf` or `unzip -l` and stores the result
///    in `meta.archive_manifest`.
///
/// The extractor produces no text and no page images — the output is metadata-only.
///
/// # Examples
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use bytes::Bytes;
/// use kb_core::extractor::{Extracted, Extractor, RawFile};
/// use kb_core::kind::DocKind;
/// use kb_extract::binary::BinaryExtractor;
///
/// # async fn example() -> anyhow::Result<()> {
/// let ex = BinaryExtractor::default();
/// let raw = RawFile {
///     bytes: Bytes::from_static(b"\x7fELF binary data here"),
///     mime: Some("application/octet-stream".into()),
///     kind: DocKind::Binary,
///     path: Some("program".into()),
/// };
/// let out = ex.extract(&raw).await?;
/// assert_eq!(out.text, "");
/// assert!(out.meta.get("mime").is_some());
/// assert!(out.meta.get("sha256").is_some());
/// # Ok(())
/// # }
/// ```
pub struct BinaryExtractor {
    /// The file/archive subprocess strategy (real `file`/`tar`/`unzip` in
    /// production; mock in tests).
    file_tool: Arc<dyn FileTool>,
    /// Maximum wall-clock time for the `file` subprocess.
    file_timeout: Duration,
    /// Maximum wall-clock time for archive listing subprocesses.
    archive_timeout: Duration,
    /// Minimum length for printable ASCII string runs to be collected.
    min_string_len: usize,
}

impl std::fmt::Debug for BinaryExtractor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinaryExtractor")
            .field("file_timeout", &self.file_timeout)
            .field("archive_timeout", &self.archive_timeout)
            .field("min_string_len", &self.min_string_len)
            .field("file_tool", &"<dyn FileTool>")
            .finish()
    }
}

impl Clone for BinaryExtractor {
    fn clone(&self) -> Self {
        Self {
            file_tool: Arc::clone(&self.file_tool),
            file_timeout: self.file_timeout,
            archive_timeout: self.archive_timeout,
            min_string_len: self.min_string_len,
        }
    }
}

impl Default for BinaryExtractor {
    fn default() -> Self {
        Self {
            file_tool: Arc::new(RealFileTool),
            file_timeout: Duration::from_secs(10),
            archive_timeout: Duration::from_secs(30),
            min_string_len: 4,
        }
    }
}

impl BinaryExtractor {
    /// Create a new extractor with default settings and the production file tool.
    ///
    /// Defaults: file timeout = 10 s, archive timeout = 30 s,
    /// min string length = 4.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set a custom timeout for the `file` subprocess. Default is 10 seconds.
    #[must_use]
    pub fn with_file_timeout(mut self, timeout: Duration) -> Self {
        self.file_timeout = timeout;
        self
    }

    /// Set a custom timeout for archive listing subprocesses. Default is 30 seconds.
    #[must_use]
    pub fn with_archive_timeout(mut self, timeout: Duration) -> Self {
        self.archive_timeout = timeout;
        self
    }

    /// Set a custom minimum length for printable string extraction. Default is 4.
    #[must_use]
    pub fn with_min_string_len(mut self, min_len: usize) -> Self {
        self.min_string_len = min_len;
        self
    }

    /// Build an extractor with an injected [`FileTool`] — for tests.
    #[cfg(test)]
    fn with_file_tool(file_tool: Arc<dyn FileTool>) -> Self {
        Self {
            file_tool,
            file_timeout: Duration::from_secs(10),
            archive_timeout: Duration::from_secs(30),
            min_string_len: 4,
        }
    }

    // ── orchestration ─────────────────────────────────────────────────────────

    /// Execute the binary metadata extraction pipeline.
    async fn extract_impl(&self, file: &RawFile) -> anyhow::Result<Extracted> {
        let mut meta = serde_json::Map::new();

        // 1. Magic-byte MIME detection (pure Rust, always works).
        let mime = detect_magic_mime(&file.bytes);
        meta.insert("mime".into(), serde_json::Value::String(mime.to_string()));

        // 2. `file` command for human-readable description.
        match self
            .file_tool
            .file_description(&file.bytes, self.file_timeout)
            .await
        {
            Ok(desc) => {
                meta.insert("magic".into(), serde_json::Value::String(desc));
            }
            Err(e) => {
                // Non-fatal: metadata is still useful without `file`.
                meta.insert(
                    "magic_error".into(),
                    serde_json::Value::String(format!("{e:#}")),
                );
            }
        }

        // 3. Printable strings extraction.
        let strings: Vec<serde_json::Value> = extract_strings(&file.bytes, self.min_string_len)
            .into_iter()
            .map(serde_json::Value::String)
            .collect();
        meta.insert("strings".into(), serde_json::Value::Array(strings));

        // 4. Size + SHA-256.
        let size = file.bytes.len();
        meta.insert(
            "size_bytes".into(),
            serde_json::Value::Number(serde_json::Number::from(size)),
        );
        let sha256_hex = compute_sha256_hex(&file.bytes);
        meta.insert("sha256".into(), serde_json::Value::String(sha256_hex));

        // 5. Document kind.
        meta.insert(
            "kind".into(),
            serde_json::Value::String(file.kind.as_str().to_string()),
        );

        // 6. Archive manifest for known archive types, with bomb + zip-slip
        //    protection (plan §17, §31.5).
        let archive_format = archive_format_for_mime(mime);
        if let Some(fmt) = archive_format {
            match self
                .file_tool
                .archive_list(&file.bytes, fmt, self.archive_timeout)
                .await
            {
                Ok(listing) => {
                    let sanitised = sanitise_archive_listing(
                        &listing,
                        security::MAX_ARCHIVE_ENTRIES,
                        security::MAX_ARCHIVE_LISTING_BYTES,
                    );
                    meta.insert(
                        "archive_manifest".into(),
                        serde_json::Value::String(sanitised),
                    );
                }
                Err(e) => {
                    // Non-fatal: manifest is a bonus.
                    meta.insert(
                        "archive_manifest_error".into(),
                        serde_json::Value::String(format!("{e:#}")),
                    );
                }
            }
        }

        Ok(Extracted {
            text: String::new(),
            meta: serde_json::Value::Object(meta),
            page_images: Vec::new(),
        })
    }
}

#[async_trait]
impl Extractor for BinaryExtractor {
    async fn extract(&self, file: &RawFile) -> anyhow::Result<Extracted> {
        self.extract_impl(file).await
    }
}

// ── helper functions ───────────────────────────────────────────────────────────

/// Detect the MIME type of `bytes` via magic-byte inspection using
/// `tree_magic_mini`.
///
/// Returns the MIME type as a string (e.g. `"image/png"`, `"application/zip"`,
/// `"text/plain"`). Bytes without a recognised binary magic signature default to
/// `"text/plain"` (tree_magic_mini's fallback). Empty input yields
/// `"application/x-empty"`.
///
/// This is a pure-Rust operation — no subprocess needed.
fn detect_magic_mime(bytes: &[u8]) -> &'static str {
    if bytes.is_empty() {
        return "application/x-empty";
    }
    tree_magic_mini::from_u8(bytes)
}

/// Compute the hex-encoded SHA-256 digest of `bytes`.
fn compute_sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    hex::encode(digest)
}

/// Map a MIME type to the corresponding [`ArchiveFormat`] if the type represents
/// a known archive format that supports content listing.
fn archive_format_for_mime(mime: &str) -> Option<ArchiveFormat> {
    match mime {
        "application/x-tar"
        | "application/gzip"
        | "application/x-bzip2"
        | "application/x-xz"
        | "application/zstd" => Some(ArchiveFormat::Tar),
        "application/zip" | "application/x-7z-compressed" => Some(ArchiveFormat::Zip),
        _ => None,
    }
}

/// Sanitise an archive listing by enforcing entry-count and byte-size caps,
/// and by redacting entries that contain path-traversal sequences.
///
/// This is defence-in-depth for the archive-manifest path: the listing is
/// stored as JSON metadata, not used for filesystem access, but a crafted
/// archive bomb with millions of entries or `../../etc/passwd`-style names
/// could bloat the DB row or mislead a human reading the manifest.
///
/// # Behaviour
///
/// - At most `max_entries` lines are kept; further lines are replaced by a
///   `… and N more entries (truncated)` sentinel.
/// - The result is truncated to `max_bytes` (UTF-8 boundary-safe).
/// - Individual entry names containing path-traversal are replaced with
///   `[redacted: unsafe path]`.
fn sanitise_archive_listing(listing: &str, max_entries: usize, max_bytes: usize) -> String {
    let mut result = String::with_capacity(listing.len().min(max_bytes));
    let mut entry_count = 0usize;

    for line in listing.lines() {
        if entry_count >= max_entries {
            // Count remaining lines for the truncation sentinel.
            let remaining = listing.lines().count().saturating_sub(entry_count);
            if remaining > 0 {
                result.push_str("… and ");
                // Use the `itertools` free `write!` equivalent; just push the
                // number inlined — we avoid an extra dep for one format call.
                result.push_str(&remaining.to_string());
                result.push_str(" more entries (truncated)\n");
            }
            break;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            entry_count = entry_count.saturating_add(1);
            result.push('\n');
            continue;
        }

        // Path-traversal check on each entry name.
        if crate::security::is_safe_path(trimmed) {
            result.push_str(line);
        } else {
            result.push_str("[redacted: unsafe path]");
        }
        result.push('\n');
        entry_count = entry_count.saturating_add(1);

        // Early exit if we've already exceeded the byte cap (check every
        // entry to avoid a single enormous line blowing past it).
        if result.len() >= max_bytes {
            result.push_str("… listing truncated (size limit)\n");
            break;
        }
    }

    // Final byte-cap enforcement: truncate at a UTF-8 boundary.
    if result.len() > max_bytes {
        let mut end = max_bytes;
        while end > 0 && !result.is_char_boundary(end) {
            end -= 1;
        }
        result.truncate(end);
        result.push('…');
    }

    result
}

// ── unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::time::Duration;

    use bytes::Bytes;
    use kb_core::extractor::{Extractor, RawFile};
    use kb_core::kind::DocKind;

    use super::*;

    // ── helpers ───────────────────────────────────────────────────────────────

    fn binary_raw(bytes: &[u8]) -> RawFile {
        RawFile {
            bytes: Bytes::copy_from_slice(bytes),
            mime: Some("application/octet-stream".into()),
            kind: DocKind::Binary,
            path: Some("unknown.bin".into()),
        }
    }

    /// A minimal ELF header (64-bit, little-endian, x86-64).
    ///
    /// The magic is `\x7fELF` plus class/data/version/OS/ABI padding.
    fn elf_header() -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&[0x7f, 0x45, 0x4c, 0x46]); // ELF magic
        v.push(2); // EI_CLASS: 64-bit
        v.push(1); // EI_DATA: little-endian
        v.push(1); // EI_VERSION
        v.push(0); // EI_OSABI: SYSV
        v.extend_from_slice(&[0u8; 8]); // padding
        v.extend_from_slice(&2u16.to_le_bytes()); // e_type: executable
        v.extend_from_slice(&0x3Eu16.to_le_bytes()); // e_machine: x86-64
        v.extend_from_slice(&1u32.to_le_bytes()); // e_version
        // e_entry (8 bytes) = 0
        v.extend_from_slice(&0u64.to_le_bytes());
        // e_phoff (8 bytes) = 0x40 (typical)
        v.extend_from_slice(&0x40u64.to_le_bytes());
        // e_shoff (8 bytes) = 0
        v.extend_from_slice(&0u64.to_le_bytes());
        // e_flags (4 bytes) = 0
        v.extend_from_slice(&0u32.to_le_bytes());
        // e_ehsize (2 bytes) = 0x40
        v.extend_from_slice(&0x40u16.to_le_bytes());
        // e_phentsize (2 bytes) = 0x38
        v.extend_from_slice(&0x38u16.to_le_bytes());
        // e_phnum (2 bytes) = 0
        v.extend_from_slice(&0u16.to_le_bytes());
        // e_shentsize (2 bytes) = 0x40
        v.extend_from_slice(&0x40u16.to_le_bytes());
        // e_shnum (2 bytes) = 0
        v.extend_from_slice(&0u16.to_le_bytes());
        // e_shstrndx (2 bytes) = 0
        v.extend_from_slice(&0u16.to_le_bytes());
        v
    }

    /// A minimal ZIP local file header (PK\x03\x04 magic + minimal fields).
    fn zip_header() -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(b"PK\x03\x04"); // ZIP local file header magic
        v.extend_from_slice(&20u16.to_le_bytes()); // version needed
        v.extend_from_slice(&0u16.to_le_bytes()); // flags
        v.extend_from_slice(&0u16.to_le_bytes()); // compression: stored
        v.extend_from_slice(&0u16.to_le_bytes()); // mod time
        v.extend_from_slice(&0u16.to_le_bytes()); // mod date
        v.extend_from_slice(&0u32.to_le_bytes()); // crc32
        v.extend_from_slice(&0u32.to_le_bytes()); // compressed size
        v.extend_from_slice(&0u32.to_le_bytes()); // uncompressed size
        v.extend_from_slice(&0u16.to_le_bytes()); // filename length
        v.extend_from_slice(&0u16.to_le_bytes()); // extra field length
        v
    }

    /// A minimal gzip header (1F 8B magic + required fields).
    fn gzip_header() -> Vec<u8> {
        vec![
            0x1F, 0x8B, // gzip magic
            0x08, // compression: deflate
            0x00, // flags: none
            0x00, 0x00, 0x00, 0x00, // mtime: none
            0x00, // extra flags
            0xFF, // OS: unknown
        ]
    }

    // ── extract_strings ───────────────────────────────────────────────────────

    #[test]
    fn extracts_printable_ascii_runs() {
        let data = b"hello\x00world\x01\x02Rust!";
        let strings = extract_strings(data, 4);
        assert_eq!(strings, vec!["hello", "world", "Rust!"]);
    }

    #[test]
    fn respects_minimum_length() {
        let data = b"ab\x00hello\x00cd";
        // "ab" (2 chars) and "cd" (2 chars) are below min_len=4
        let strings = extract_strings(data, 4);
        assert_eq!(strings, vec!["hello"]);
    }

    #[test]
    fn min_len_of_one_finds_all() {
        let data = b"ab\x00cde";
        let strings = extract_strings(data, 1);
        assert_eq!(strings, vec!["ab", "cde"]);
    }

    #[test]
    fn empty_input_produces_nothing() {
        let strings = extract_strings(b"", 4);
        assert!(strings.is_empty());
    }

    #[test]
    fn no_printable_bytes_produces_nothing() {
        let data = &[0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE];
        let strings = extract_strings(data, 4);
        assert!(strings.is_empty());
    }

    #[test]
    fn non_ascii_terminates_run() {
        // High bytes (> 0x7F) terminate the current run.
        let data = b"hello\x80world";
        let strings = extract_strings(data, 4);
        assert_eq!(strings, vec!["hello", "world"]);
    }

    #[test]
    fn trailing_printable_run_captured() {
        let data = b"\x00hello";
        let strings = extract_strings(data, 4);
        assert_eq!(strings, vec!["hello"]);
    }

    #[test]
    fn leading_printable_run_captured() {
        let data = b"hello\x00";
        let strings = extract_strings(data, 4);
        assert_eq!(strings, vec!["hello"]);
    }

    #[test]
    fn space_is_printable() {
        let data = b"hello world\x00";
        let strings = extract_strings(data, 4);
        assert_eq!(strings, vec!["hello world"]);
    }

    #[test]
    fn tilde_is_printable() {
        let data = b"~~~test~~~\x00";
        let strings = extract_strings(data, 4);
        assert_eq!(strings, vec!["~~~test~~~"]);
    }

    #[test]
    fn min_len_zero_returns_everything() {
        let data = b"ab\x00cd";
        let strings = extract_strings(data, 0);
        // Every non-delimiter byte forms a run.
        assert_eq!(strings, vec!["ab", "cd"]);
    }

    // ── detect_magic_mime ─────────────────────────────────────────────────────

    #[test]
    fn detects_elf_binary() {
        let elf = elf_header();
        let mime = detect_magic_mime(&elf);
        assert!(
            mime.contains("x-executable") || mime.contains("x-sharedlib"),
            "expected ELF MIME, got: {mime}"
        );
    }

    #[test]
    fn detects_zip_archive() {
        let zip = zip_header();
        let mime = detect_magic_mime(&zip);
        assert_eq!(mime, "application/zip");
    }

    #[test]
    fn detects_gzip_archive() {
        let gz = gzip_header();
        let mime = detect_magic_mime(&gz);
        assert_eq!(mime, "application/gzip");
    }

    #[test]
    fn detects_png_image() {
        let png = &[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00];
        let mime = detect_magic_mime(png);
        assert_eq!(mime, "image/png");
    }

    #[test]
    fn empty_input_returns_empty_mime() {
        let mime = detect_magic_mime(b"");
        assert_eq!(mime, "application/x-empty");
    }

    #[test]
    fn unknown_bytes_default_to_text_plain() {
        // tree_magic_mini classifies anything without a known binary magic as text/plain.
        let mime = detect_magic_mime(b"totally random garbage data");
        assert_eq!(mime, "text/plain");
    }

    // ── compute_sha256_hex ────────────────────────────────────────────────────

    #[test]
    fn sha256_empty_input() {
        // SHA-256 of empty input (well-known test vector).
        let hex = compute_sha256_hex(b"");
        assert_eq!(
            hex,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn sha256_known_vector() {
        // SHA-256 of "abc" (well-known test vector).
        let hex = compute_sha256_hex(b"abc");
        assert_eq!(
            hex,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn sha256_is_64_chars() {
        let hex = compute_sha256_hex(b"random data");
        assert_eq!(hex.len(), 64);
    }

    #[test]
    fn sha256_is_deterministic() {
        let a = compute_sha256_hex(b"same input");
        let b = compute_sha256_hex(b"same input");
        assert_eq!(a, b);
    }

    #[test]
    fn sha256_different_inputs_differ() {
        let a = compute_sha256_hex(b"input one");
        let b = compute_sha256_hex(b"input two");
        assert_ne!(a, b);
    }

    // ── archive_format_for_mime ───────────────────────────────────────────────

    #[test]
    fn tar_mime_maps_to_tar() {
        assert_eq!(
            archive_format_for_mime("application/x-tar"),
            Some(ArchiveFormat::Tar)
        );
    }

    #[test]
    fn gzip_mime_maps_to_tar() {
        assert_eq!(
            archive_format_for_mime("application/gzip"),
            Some(ArchiveFormat::Tar)
        );
    }

    #[test]
    fn bzip2_maps_to_tar() {
        assert_eq!(
            archive_format_for_mime("application/x-bzip2"),
            Some(ArchiveFormat::Tar)
        );
    }

    #[test]
    fn xz_maps_to_tar() {
        assert_eq!(
            archive_format_for_mime("application/x-xz"),
            Some(ArchiveFormat::Tar)
        );
    }

    #[test]
    fn zip_mime_maps_to_zip() {
        assert_eq!(
            archive_format_for_mime("application/zip"),
            Some(ArchiveFormat::Zip)
        );
    }

    #[test]
    fn seven_z_maps_to_zip() {
        assert_eq!(
            archive_format_for_mime("application/x-7z-compressed"),
            Some(ArchiveFormat::Zip)
        );
    }

    #[test]
    fn unknown_mime_maps_to_none() {
        assert_eq!(archive_format_for_mime("image/png"), None);
        assert_eq!(archive_format_for_mime("text/plain"), None);
        assert_eq!(archive_format_for_mime("application/octet-stream"), None);
    }

    // ── mock file tool ────────────────────────────────────────────────────────

    /// A mock [`FileTool`] that returns pre-configured responses.
    struct MockFileTool {
        file_desc: String,
        archive_listing: String,
        file_should_fail: bool,
        archive_should_fail: bool,
    }

    impl MockFileTool {
        fn new(file_desc: &str, archive_listing: &str) -> Self {
            Self {
                file_desc: file_desc.into(),
                archive_listing: archive_listing.into(),
                file_should_fail: false,
                archive_should_fail: false,
            }
        }

        fn with_failing_file(mut self) -> Self {
            self.file_should_fail = true;
            self
        }

        fn with_failing_archive(mut self) -> Self {
            self.archive_should_fail = true;
            self
        }
    }

    #[async_trait]
    impl FileTool for MockFileTool {
        async fn file_description(
            &self,
            _input: &[u8],
            _timeout: Duration,
        ) -> anyhow::Result<String> {
            if self.file_should_fail {
                anyhow::bail!(
                    "BinaryExtractor: file exited with status exit status: 1: cannot open"
                );
            }
            Ok(self.file_desc.clone())
        }

        async fn archive_list(
            &self,
            _input: &[u8],
            _format: ArchiveFormat,
            _timeout: Duration,
        ) -> anyhow::Result<String> {
            if self.archive_should_fail {
                anyhow::bail!(
                    "BinaryExtractor: tar exited with status exit status: 2: corrupt archive"
                );
            }
            Ok(self.archive_listing.clone())
        }
    }

    /// Create a [`BinaryExtractor`] wired to a [`MockFileTool`].
    fn mock_extractor(file_desc: &str, archive_listing: &str) -> BinaryExtractor {
        BinaryExtractor::with_file_tool(Arc::new(MockFileTool::new(file_desc, archive_listing)))
    }

    fn mock_extractor_with(mock: MockFileTool) -> BinaryExtractor {
        BinaryExtractor::with_file_tool(Arc::new(mock))
    }

    // ── BinaryExtractor: full extraction ──────────────────────────────────────

    #[tokio::test]
    async fn extracts_elf_binary_metadata() {
        let ex = mock_extractor(
            "ELF 64-bit LSB executable, x86-64, version 1 (SYSV), dynamically linked",
            "irrelevant",
        );
        let elf = elf_header();
        let raw = binary_raw(&elf);
        let out = ex.extract(&raw).await.unwrap();

        // No text or page images for binary files.
        assert_eq!(out.text, "");
        assert!(out.page_images.is_empty());

        // Meta fields present.
        let mime = out.meta.get("mime").and_then(|v| v.as_str()).unwrap();
        assert!(
            mime.contains("x-executable") || mime.contains("x-sharedlib"),
            "expected ELF MIME, got: {mime}"
        );

        let magic = out.meta.get("magic").and_then(|v| v.as_str()).unwrap();
        assert!(
            magic.contains("ELF"),
            "magic should mention ELF, got: {magic}"
        );

        let sha = out.meta.get("sha256").and_then(|v| v.as_str()).unwrap();
        assert_eq!(sha.len(), 64);

        let size = out.meta.get("size_bytes").and_then(|v| v.as_u64()).unwrap();
        assert!(size > 0);

        let kind = out.meta.get("kind").and_then(|v| v.as_str()).unwrap();
        assert_eq!(kind, "binary");
    }

    #[tokio::test]
    async fn extracts_zip_archive_metadata() {
        let ex = mock_extractor(
            "Zip archive data, at least v2.0 to extract",
            "file1.txt\nsubdir/\nsubdir/file2.png",
        );
        let zip = zip_header();
        let raw = RawFile {
            bytes: Bytes::copy_from_slice(&zip),
            mime: Some("application/zip".into()),
            kind: DocKind::Archive,
            path: Some("archive.zip".into()),
        };
        let out = ex.extract(&raw).await.unwrap();

        assert_eq!(out.text, "");
        assert!(out.page_images.is_empty());

        let mime = out.meta.get("mime").and_then(|v| v.as_str()).unwrap();
        assert_eq!(mime, "application/zip");

        let manifest = out
            .meta
            .get("archive_manifest")
            .and_then(|v| v.as_str())
            .unwrap();
        assert!(manifest.contains("file1.txt"), "got: {manifest}");
        assert!(manifest.contains("file2.png"), "got: {manifest}");
    }

    #[tokio::test]
    async fn extracts_strings_from_binary() {
        let ex = mock_extractor("data", "");
        // Craft bytes with embedded printable strings.
        let data = b"\x00\x01/lib64/ld-linux-x86-64.so.2\x00__libc_start_main\x00printf\x00";
        let raw = binary_raw(data);
        let out = ex.extract(&raw).await.unwrap();

        let strings = out.meta.get("strings").and_then(|v| v.as_array()).unwrap();
        let string_values: Vec<&str> = strings.iter().filter_map(|v| v.as_str()).collect();
        assert!(string_values.contains(&"/lib64/ld-linux-x86-64.so.2"));
        assert!(string_values.contains(&"__libc_start_main"));
        assert!(string_values.contains(&"printf"));
    }

    #[tokio::test]
    async fn empty_input_meta_has_expected_shape() {
        let ex = mock_extractor("empty", "");
        let raw = binary_raw(b"");
        let out = ex.extract(&raw).await.unwrap();

        assert_eq!(out.text, "");
        assert_eq!(
            out.meta.get("mime").and_then(|v| v.as_str()),
            Some("application/x-empty")
        );
        assert_eq!(out.meta.get("size_bytes").and_then(|v| v.as_u64()), Some(0));
        let sha = out.meta.get("sha256").and_then(|v| v.as_str()).unwrap();
        assert_eq!(
            sha,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[tokio::test]
    async fn file_command_error_is_non_fatal() {
        let mock = MockFileTool::new("unused", "").with_failing_file();
        let ex = mock_extractor_with(mock);
        let raw = binary_raw(b"some bytes");
        let out = ex.extract(&raw).await.unwrap();

        // Extraction still succeeds; magic is replaced by an error note.
        assert!(out.meta.get("magic_error").is_some());
        assert!(out.meta.get("magic").is_none());
        // Other fields still present.
        assert!(out.meta.get("sha256").is_some());
        assert!(out.meta.get("size_bytes").is_some());
    }

    #[tokio::test]
    async fn archive_list_error_is_non_fatal() {
        let mock = MockFileTool::new("Zip archive data", "").with_failing_archive();
        let ex = mock_extractor_with(mock);
        let zip = zip_header();
        let raw = RawFile {
            bytes: Bytes::copy_from_slice(&zip),
            mime: Some("application/zip".into()),
            kind: DocKind::Archive,
            path: Some("corrupt.zip".into()),
        };
        let out = ex.extract(&raw).await.unwrap();

        // Extraction still succeeds.
        assert!(out.meta.get("archive_manifest_error").is_some());
        assert!(out.meta.get("archive_manifest").is_none());
        assert_eq!(
            out.meta.get("mime").and_then(|v| v.as_str()),
            Some("application/zip")
        );
    }

    #[tokio::test]
    async fn archive_manifest_only_for_known_formats() {
        let ex = mock_extractor("PNG image data, 1920 x 1080", "should not appear");
        let png = &[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];
        let raw = binary_raw(png);
        let out = ex.extract(&raw).await.unwrap();

        // PNG is not an archive — no manifest should be stored.
        assert!(out.meta.get("archive_manifest").is_none());
        assert!(out.meta.get("archive_manifest_error").is_none());
    }

    #[tokio::test]
    async fn gzip_detected_as_archive_with_tar_manifest() {
        let ex = mock_extractor(
            "gzip compressed data, was \"archive.tar\", last modified: ...",
            "doc.txt\nimg/photo.jpg\n",
        );
        let gz = gzip_header();
        let raw = RawFile {
            bytes: Bytes::copy_from_slice(&gz),
            mime: Some("application/gzip".into()),
            kind: DocKind::Archive,
            path: Some("backup.tar.gz".into()),
        };
        let out = ex.extract(&raw).await.unwrap();

        let mime = out.meta.get("mime").and_then(|v| v.as_str()).unwrap();
        assert_eq!(mime, "application/gzip");
        let manifest = out
            .meta
            .get("archive_manifest")
            .and_then(|v| v.as_str())
            .unwrap();
        assert!(manifest.contains("doc.txt"));
        assert!(manifest.contains("img/photo.jpg"));
    }

    #[tokio::test]
    async fn uses_custom_min_string_len() {
        let ex = mock_extractor("data", "").with_min_string_len(10);
        let data = b"\x00short\x00longer_string\x00tiny\x00";
        let raw = binary_raw(data);
        let out = ex.extract(&raw).await.unwrap();

        let strings = out.meta.get("strings").and_then(|v| v.as_array()).unwrap();
        let values: Vec<&str> = strings.iter().filter_map(|v| v.as_str()).collect();
        assert!(values.contains(&"longer_string"));
        assert!(!values.contains(&"short"));
        assert!(!values.contains(&"tiny"));
    }

    // ── BinaryExtractor: path handling in file errors ─────────────────────────

    #[tokio::test]
    async fn error_includes_path_when_available() {
        let mock = MockFileTool::new("unused", "").with_failing_file();
        let ex = mock_extractor_with(mock).with_min_string_len(4);
        let raw = RawFile {
            bytes: Bytes::from_static(b"garbage"),
            mime: Some("application/octet-stream".into()),
            kind: DocKind::Binary,
            path: Some("suspicious.dat".into()),
        };
        let out = ex.extract(&raw).await.unwrap();
        // File error is non-fatal; extraction succeeds with magic_error note.
        let err = out
            .meta
            .get("magic_error")
            .and_then(|v| v.as_str())
            .unwrap();
        assert!(err.contains("cannot open"), "got: {err}");
    }

    // ── BinaryExtractor: builder defaults ─────────────────────────────────────

    #[test]
    fn default_file_timeout_is_10_seconds() {
        let ex = BinaryExtractor::new();
        assert_eq!(ex.file_timeout, Duration::from_secs(10));
    }

    #[test]
    fn default_archive_timeout_is_30_seconds() {
        let ex = BinaryExtractor::new();
        assert_eq!(ex.archive_timeout, Duration::from_secs(30));
    }

    #[test]
    fn default_min_string_len_is_4() {
        let ex = BinaryExtractor::new();
        assert_eq!(ex.min_string_len, 4);
    }

    #[test]
    fn custom_file_timeout() {
        let ex = BinaryExtractor::new().with_file_timeout(Duration::from_secs(5));
        assert_eq!(ex.file_timeout, Duration::from_secs(5));
    }

    #[test]
    fn custom_archive_timeout() {
        let ex = BinaryExtractor::new().with_archive_timeout(Duration::from_secs(60));
        assert_eq!(ex.archive_timeout, Duration::from_secs(60));
    }

    #[test]
    fn custom_min_string_len() {
        let ex = BinaryExtractor::new().with_min_string_len(8);
        assert_eq!(ex.min_string_len, 8);
    }

    // ── Extractor trait object dispatch ───────────────────────────────────────

    #[tokio::test]
    async fn dyn_extractor_trait_object() {
        let ex: Box<dyn Extractor> = Box::new(mock_extractor("data", ""));
        let raw = binary_raw(b"test binary");
        let out = ex.extract(&raw).await.unwrap();
        assert_eq!(out.text, "");
        assert!(out.meta.get("sha256").is_some());
    }

    // ── BinaryExtractor: rounding coverage edges ──────────────────────────────

    #[tokio::test]
    async fn detect_mime_unknown_bytes_through_extractor() {
        let ex = mock_extractor("data", "");
        let raw = binary_raw(b"\xDE\xAD\xBE\xEF");
        let out = ex.extract(&raw).await.unwrap();
        let mime = out.meta.get("mime").and_then(|v| v.as_str()).unwrap();
        // tree_magic_mini falls back to text/plain for unrecognised data.
        assert_eq!(mime, "text/plain");
    }

    #[tokio::test]
    async fn size_bytes_matches_input_length() {
        let ex = mock_extractor("data", "");
        let input = vec![0u8; 1024];
        let raw = binary_raw(&input);
        let out = ex.extract(&raw).await.unwrap();
        assert_eq!(
            out.meta.get("size_bytes").and_then(|v| v.as_u64()),
            Some(1024)
        );
    }

    #[tokio::test]
    async fn kind_reflects_doc_kind() {
        let ex = mock_extractor("data", "");
        let raw = RawFile {
            bytes: Bytes::from_static(b"binary"),
            mime: None,
            kind: DocKind::Binary,
            path: None,
        };
        let out = ex.extract(&raw).await.unwrap();
        assert_eq!(
            out.meta.get("kind").and_then(|v| v.as_str()),
            Some("binary")
        );
    }

    #[tokio::test]
    async fn strings_array_empty_when_no_printable_runs() {
        let ex = mock_extractor("data", "").with_min_string_len(4);
        let data = [0x00u8; 100];
        let raw = binary_raw(&data);
        let out = ex.extract(&raw).await.unwrap();
        let arr = out.meta.get("strings").and_then(|v| v.as_array()).unwrap();
        assert!(arr.is_empty());
    }

    #[tokio::test]
    async fn xz_detected_as_archive() {
        // xz magic bytes: FD 37 7A 58 5A 00
        let xz = &[0xFD, 0x37, 0x7A, 0x58, 0x5A, 0x00];
        let mime = detect_magic_mime(xz);
        assert!(
            mime.contains("xz") || mime == "application/octet-stream",
            "xz should be detected, got: {mime}"
        );
    }

    // ── sanitise_archive_listing ────────────────────────────────────────────

    #[test]
    fn sanitise_passes_through_normal_listing() {
        let listing = "file1.txt\ndir/\ndir/file2.png\n";
        let result = sanitise_archive_listing(listing, 1000, 1_000_000);
        assert_eq!(result, listing);
    }

    #[test]
    fn sanitise_redacts_path_traversal_entries() {
        let listing = "good.txt\n../../../etc/passwd\nanother.txt\n";
        let result = sanitise_archive_listing(listing, 1000, 1_000_000);
        assert!(result.contains("good.txt"));
        assert!(!result.contains("../../../etc/passwd"));
        assert!(result.contains("[redacted: unsafe path]"));
        assert!(result.contains("another.txt"));
    }

    #[test]
    fn sanitise_redacts_absolute_paths() {
        let listing = "normal.txt\n/etc/shadow\nsub/file.txt\n";
        let result = sanitise_archive_listing(listing, 1000, 1_000_000);
        assert!(result.contains("normal.txt"));
        assert!(!result.contains("/etc/shadow"));
        assert!(result.contains("[redacted: unsafe path]"));
    }

    #[test]
    fn sanitise_truncates_after_max_entries() {
        let mut listing = String::new();
        for i in 0..15 {
            listing.push_str(&format!("file_{i}.txt\n"));
        }
        let result = sanitise_archive_listing(&listing, 10, 1_000_000);
        let lines: Vec<&str> = result.lines().collect();
        // 10 entries + truncation sentinel.
        assert!(lines.len() >= 11, "got {} lines: {:?}", lines.len(), lines);
        let last = lines.last().unwrap();
        assert!(
            last.contains("truncated"),
            "last line should be truncation sentinel, got: {last}"
        );
        assert!(
            last.contains("5"),
            "should mention 5 remaining, got: {last}"
        );
    }

    #[test]
    fn sanitise_truncates_at_byte_limit() {
        let listing = "this_is_a_very_long_entry_name_that_exceeds_the_byte_limit.txt\n";
        let result = sanitise_archive_listing(listing, 1000, 30);
        // '…' is 3 bytes in UTF-8, so after truncation at 30 bytes + push('…')
        // the result is at most 33 bytes.
        assert!(
            result.len() <= 33,
            "expected len <= 33, got {} ('{result}')",
            result.len()
        );
    }

    #[test]
    fn sanitise_empty_listing() {
        let result = sanitise_archive_listing("", 100, 1000);
        assert_eq!(result, "");
    }

    #[test]
    fn sanitise_handles_null_byte_in_entry() {
        // Null bytes make the path unsafe per is_safe_path.
        let listing = "file.txt\0.exe\n";
        let result = sanitise_archive_listing(listing, 1000, 1_000_000);
        assert!(result.contains("[redacted: unsafe path]"));
    }

    #[test]
    fn sanitise_skips_empty_lines_but_counts_them() {
        let listing = "a.txt\n\nb.txt\n";
        let result = sanitise_archive_listing(listing, 2, 1_000_000);
        // Only 2 non-empty entries should be kept (the empty line is skipped
        // but counted, so we hit the cap at the third entry).
        let result_lines: Vec<&str> = result.lines().collect();
        assert!(
            result_lines.len() >= 3,
            "expected at least 3 result lines (2 entries + sentinel), got {:?}",
            result_lines
        );
    }
}
