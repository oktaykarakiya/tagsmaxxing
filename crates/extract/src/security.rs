// SPDX-License-Identifier: AGPL-3.0-or-later

//! Security guards for the extractor untrusted-bytes boundary (plan §17, §31.5).
//!
//! This module provides upload-edge validation — per-file size limits, MIME-type
//! allow-lists, path-traversal detection, and URL safety checks for the
//! HTTP-based extractors (Tika, whisper). It also exports archive-protection
//! constants used by [`crate::binary`] to defend against archive bombs and
//! zip-slip paths.
//!
//! # Trust boundary
//!
//! Every file that enters the system passes through these guards **before**
//! any extractor or blob-store operation. The guards are pure functions (no
//! I/O, no async) so they can be tested exhaustively and called from any
//! upload path (CLI, REST API, folder watcher, web UI).
//!
//! # Related
//!
//! - [`crate::binary::BinaryExtractor`] — archive-listing path-traversal checks.
//! - [`crate::tika::TikaExtractor`] — HTTP PUT to Tika (URL from operator config).
//! - [`crate::audio::AudioExtractor`] — HTTP POST to whisper (URL from operator config).
//! - `ARCHITECTURE.md` § "Trust boundary & extractor security".

use std::net::{IpAddr, Ipv6Addr};

use thiserror::Error;

// ── Upload rejection error (BUG-INGEST-06/07) ────────────────────────────────────

/// A rejected upload at the validation boundary.
///
/// This is a **client** error: the uploaded bytes are empty, too large, of a
/// disallowed type, or carry an unsafe filename — the caller can correct the
/// request and retry. The validators ([`validate_upload`] and its parts) return
/// it (as an [`anyhow::Error`]) so the API layer can downcast it and respond
/// `400 Bad Request` rather than mapping it to a `500` (which is what an
/// untyped `anyhow` error would otherwise produce). This mirrors how
/// `kb_core::quota::QuotaError` is downcast to `413`.
#[derive(Debug, Error)]
pub enum UploadRejected {
    /// The file exceeds the maximum allowed size.
    #[error("rejected upload: file size {size} bytes exceeds limit of {max} bytes")]
    TooLarge {
        /// Actual size of the rejected file, in bytes.
        size: u64,
        /// Configured maximum, in bytes.
        max: u64,
    },
    /// The filename contains path-traversal sequences or unsafe characters.
    #[error("rejected upload: '{0}' contains path-traversal or unsafe characters")]
    UnsafePath(String),
    /// The detected MIME type is empty, denied, or not in the allow-list.
    ///
    /// A zero-byte upload detects as `application/x-empty`, which is not
    /// allow-listed, and therefore surfaces here.
    #[error(
        "rejected upload: unsupported MIME type '{0}'. \
         Allowed types include documents, images, audio, video, and archives."
    )]
    DisallowedMime(String),
}

// ── Size limits ────────────────────────────────────────────────────────────────

/// Maximum bytes per individual file in an upload: 500 MiB.
///
/// Individual files exceeding this are rejected before blob storage or
/// extractor dispatch. This is intentionally larger than
/// [`MAX_TOTAL_UPLOAD_BYTES`] so that a single large file can still be
/// ingested when uploaded alone.
///
/// Note (campaign finding F2): on the HTTP ingest path this per-file cap is
/// effectively unreachable — the 100 MiB total-multipart cap
/// ([`MAX_TOTAL_UPLOAD_BYTES`], mirrored by the handler's `MAX_PAYLOAD_BYTES`)
/// always trips first. It still bounds non-HTTP ingest paths (CLI, folder
/// watcher) that validate a single file without the multipart total limit.
pub const MAX_INDIVIDUAL_FILE_BYTES: u64 = 500 * 1024 * 1024;

/// Maximum total bytes in a single multipart upload request: 100 MiB.
///
/// Aligned with [`crate::api::handlers::ingest::MAX_PAYLOAD_BYTES`].
pub const MAX_TOTAL_UPLOAD_BYTES: u64 = 100 * 1024 * 1024;

// ── Archive protection ─────────────────────────────────────────────────────────

/// Maximum number of entry lines read from an archive listing before
/// truncation. Defends against archive bombs with millions of entries.
pub const MAX_ARCHIVE_ENTRIES: usize = 10_000;

/// Maximum bytes of archive listing output before the subprocess is
/// terminated. Defends against archives whose listing alone is huge
/// (e.g. a crafted tar with 1 GB of garbage entry names).
pub const MAX_ARCHIVE_LISTING_BYTES: usize = 1_000_000;

/// Maximum depth for nested archive extraction. Archives containing
/// further archives are only recurred into up to this depth.
pub const MAX_ARCHIVE_RECURSION_DEPTH: usize = 3;

/// Maximum bytes read from a single archive entry when extracting its text
/// content. Bounds memory for one pathologically large entry.
pub const MAX_ARCHIVE_ENTRY_BYTES: usize = 4 * 1024 * 1024;

/// Maximum total uncompressed bytes harvested across all entries of one archive.
/// Defends against archive bombs (a tiny archive that expands enormously) by
/// capping the total content the [`crate::archive`] extractor will read.
pub const MAX_ARCHIVE_TOTAL_UNCOMPRESSED_BYTES: usize = 64 * 1024 * 1024;

/// Maximum per-entry uncompressed/compressed size ratio before an entry is
/// treated as a zip bomb and skipped. Classic zip-bomb entries expand by
/// thousands of times; legitimate text compresses well under this bound.
pub const MAX_ARCHIVE_COMPRESSION_RATIO: u64 = 200;

// ── MIME allow-list ────────────────────────────────────────────────────────────

/// MIME types accepted for ingestion.
///
/// Files whose detected MIME is not in this list are rejected with a clear
/// error message. This is a coarse first-pass filter; the magic-byte
/// detection in individual extractors provides finer-grained validation.
/// The list is deliberately generous to avoid being a maintenance burden
/// while still blocking obviously dangerous types (executables, shared
/// libraries, disk images).
pub const ALLOWED_MIMES: &[&str] = &[
    // Plain text and markup
    "text/plain",
    "text/markdown",
    "text/html",
    "text/csv",
    "text/xml",
    "text/css",
    "text/javascript",
    "text/x-python",
    "text/x-python3", // tree_magic_mini returns this for Python 3 files
    "text/x-rust",
    "text/x-c",
    "text/x-c++",
    "text/x-csrc",     // tree_magic_mini returns this for .c / .js files
    "text/x-modelica", // tree_magic_mini returns this for some .js files
    "text/x-java",
    "text/x-go",
    "text/x-sh",
    "text/x-yaml",
    "text/x-toml",
    "text/x-log",
    // Structured data
    "application/json",
    "application/xml",
    // Office documents (Tika)
    "application/pdf",
    "application/msword",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "application/vnd.oasis.opendocument.text",
    "application/rtf",
    // Images
    "image/jpeg",
    "image/png",
    "image/gif",
    "image/webp",
    "image/tiff",
    // Audio
    "audio/mpeg",
    "audio/wav",
    "audio/wave",
    "audio/ogg",
    "audio/flac",
    "audio/mp4",
    "audio/x-wav",
    // Video
    "video/mp4",
    "video/x-matroska",
    "application/x-matroska", // tree_magic naming variant of video/x-matroska
    "video/webm",
    "video/ogg",
    "video/quicktime",
    "video/x-msvideo",
    // Archives (accepted only when the binary extractor can list them)
    "application/zip",
    "application/x-tar",
    "application/gzip",
    "application/x-bzip2",
    "application/x-xz",
    "application/zstd",
    "application/x-7z-compressed",
    // Email files
    "message/rfc822",
];

// ── MIME deny-list ─────────────────────────────────────────────────────────────

/// MIME types that are explicitly blocked even if they appear in
/// [`ALLOWED_MIMES`]. This is a safety net for types that an operator
/// might accidentally allow.
pub const DENIED_MIMES: &[&str] = &[
    "application/x-msdownload",     // Windows executables
    "application/x-dosexec",        // PE32 executables
    "application/x-executable",     // Generic executables
    "application/x-sharedlib",      // Shared libraries (.so)
    "application/x-mach-binary",    // Mach-O binaries
    "application/java-archive",     // JAR files (executable)
    "application/x-iso9660-image",  // Disk images
    "application/x-raw-disk-image", // Raw disk images
];

// ── Path traversal detection ───────────────────────────────────────────────────

/// Check whether `path` is free of path-traversal sequences.
///
/// Rejects strings containing:
/// - `..` directory traversal components
/// - Absolute-path prefixes (`/` or `\`)
/// - Null bytes (can confuse C APIs in subprocesses)
/// - Backslash separators (Windows-style, breaks Unix assumptions)
///
/// An empty path is considered safe (returns `true`).
///
/// # Examples
///
/// ```rust
/// use kb_extract::security::is_safe_path;
///
/// assert!(is_safe_path("document.pdf"));
/// assert!(is_safe_path("subdir/file.txt"));
/// assert!(is_safe_path(""));
///
/// assert!(!is_safe_path("../etc/passwd"));
/// assert!(!is_safe_path("a/../../b"));
/// assert!(!is_safe_path("/absolute/path"));
/// assert!(!is_safe_path("file\0hidden.txt"));
/// assert!(!is_safe_path("C:\\Windows\\file.dll"));
/// ```
pub fn is_safe_path(path: &str) -> bool {
    if path.is_empty() {
        return true;
    }

    // Null bytes are never valid in a filesystem path and indicate an
    // attempt to exploit C-string truncation in a subprocess.
    if path.contains('\0') {
        return false;
    }

    // Absolute paths (Unix or Windows).
    if path.starts_with('/') || path.starts_with('\\') {
        return false;
    }

    // Check each component for ".." traversal.
    for component in path.split('/') {
        if component == ".." || component == "..." {
            return false;
        }
        // Also catch encoded variants: "...." can resolve to ".." in some shells.
        if component.len() >= 2
            && component.chars().all(|c| c == '.')
            && component.chars().filter(|&c| c == '.').count() >= 2
        {
            return false;
        }
    }

    // Backslash in a Unix path is a red flag (Windows path injected into
    // a Unix tool).
    if path.contains('\\') {
        return false;
    }

    true
}

/// Validate that `file_name` is safe to use as a filesystem path component.
///
/// Returns an error with a descriptive message if the path appears
/// malicious. Call this on user-supplied filenames **before** passing them
/// to any subprocess or storing them in metadata.
///
/// # Errors
///
/// Returns `anyhow::Error` if `file_name` contains path-traversal sequences.
pub fn validate_file_path(file_name: &str) -> anyhow::Result<()> {
    if !is_safe_path(file_name) {
        return Err(UploadRejected::UnsafePath(file_name.to_owned()).into());
    }
    Ok(())
}

// ── File size validation ───────────────────────────────────────────────────────

/// Validate that an individual file's byte count is within the allowed maximum.
///
/// Returns an error with the actual and maximum sizes if exceeded.
///
/// # Errors
///
/// Returns `anyhow::Error` when `size > max_size`.
pub fn validate_file_size(size: u64, max_size: u64) -> anyhow::Result<()> {
    if size > max_size {
        return Err(UploadRejected::TooLarge {
            size,
            max: max_size,
        }
        .into());
    }
    Ok(())
}

// ── MIME validation ────────────────────────────────────────────────────────────

/// Check whether `mime` is in the given allow-list.
///
/// Returns `false` for MIME types that are not in the list or that
/// appear in [`DENIED_MIMES`].
///
/// # Examples
///
/// ```rust
/// use kb_extract::security::{is_allowed_mime, ALLOWED_MIMES};
///
/// assert!(is_allowed_mime("text/plain", ALLOWED_MIMES));
/// assert!(!is_allowed_mime("application/x-msdownload", ALLOWED_MIMES));
/// assert!(!is_allowed_mime("not/a-real-type", ALLOWED_MIMES));
/// ```
pub fn is_allowed_mime(mime: &str, allowed: &[&str]) -> bool {
    // Deny-list overrides everything.
    if DENIED_MIMES.contains(&mime) {
        return false;
    }
    // Any text/x-* variant is a source-code or markup type that tree_magic_mini
    // may return (x-csrc, x-python3, x-modelica, x-matlab, …). Playing
    // whack-a-mole with every variant is unsustainable; accepting them all is
    // safe because no text/x-* type appears in DENIED_MIMES and individual
    // extractors validate their own format-specific magic bytes.
    if mime.starts_with("text/x-") {
        return true;
    }
    allowed.contains(&mime)
}

/// Validate that `mime` (as reported by magic-byte detection) is in the
/// allow-list. Returns an error with a clear message if rejected.
///
/// # Errors
///
/// Returns `anyhow::Error` if the MIME is not in the allow-list.
pub fn validate_mime(mime: &str) -> anyhow::Result<()> {
    if is_allowed_mime(mime, ALLOWED_MIMES) {
        return Ok(());
    }
    Err(UploadRejected::DisallowedMime(mime.to_owned()).into())
}

// ── SSRF protection ────────────────────────────────────────────────────────────

/// Validate that a URL does not point to an internal/private network address.
///
/// Extractor HTTP calls (Tika, whisper) use operator-configured URLs, not
/// user-supplied ones. This check is **defence-in-depth**: a misconfigured
/// operator or a compromised config file should not open a Server-Side
/// Request Forgery vector that reaches internal services.
///
/// Rejects URLs whose host component resolves to:
/// - Loopback addresses (`127.0.0.0/8`, `::1`)
/// - Private ranges (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`)
/// - Link-local (`169.254.0.0/16`, `fe80::/10`)
/// - Unspecified (`0.0.0.0`, `::`)
/// - The hostname `localhost` or any hostname ending in `.local` / `.internal`
///
/// Returns `true` when the URL looks safe for an outbound call.
///
/// # Limitations
///
/// This is a **static host check** — it does not perform DNS resolution,
/// so DNS rebinding attacks are not caught. For the current architecture
/// (extractors call only operator-configured sidecar containers on the
/// compose network), this is acceptable. If user-supplied URLs were ever
/// fetched (e.g. a "fetch from URL" feature), a proper DNS-resolving
/// SSRF guard would be required.
///
/// # Examples
///
/// ```rust
/// use kb_extract::security::is_safe_target_url;
///
/// assert!(is_safe_target_url("http://tika:9998/tika"));
/// assert!(is_safe_target_url("http://whisper-server:8080/v1/audio/transcriptions"));
/// assert!(!is_safe_target_url("http://localhost:9998/tika"));
/// assert!(!is_safe_target_url("http://127.0.0.1:8080"));
/// assert!(!is_safe_target_url("http://192.168.1.1/admin"));
/// ```
pub fn is_safe_target_url(url_str: &str) -> bool {
    let host = match extract_url_host(url_str) {
        Some(h) => h,
        None => return false,
    };
    !is_internal_host(&host)
}

/// Extract the host component from a loose URL string.
///
/// Handles `http://host:port/path`, `https://host/path`, and bare
/// `host:port` forms. Strips user-info (`user:pass@host`), brackets
/// from IPv6 literals, and port suffixes.
fn extract_url_host(raw: &str) -> Option<String> {
    // 1. Strip scheme.
    let after_scheme = raw
        .strip_prefix("https://")
        .or_else(|| raw.strip_prefix("http://"))
        .unwrap_or(raw);

    // 2. Strip path, query, fragment.
    let host_port = after_scheme.split('/').next()?;

    // 3. Strip user-info (user:pass@host → host).
    let host_port = host_port.split('@').next_back()?;

    // 4. Handle IPv6 bracket notation: [::1]:8080 → ::1.
    if let Some(rest) = host_port.strip_prefix('[') {
        // The host is everything up to the closing bracket.
        let host = rest.split(']').next()?;
        if host.is_empty() {
            return None;
        }
        return Some(host.to_string());
    }

    // 5. Split host from port (IPv4 / hostname case).
    let host = host_port.split(':').next()?;

    if host.is_empty() {
        return None;
    }
    Some(host.to_string())
}

/// Decide whether `host` refers to an internal network destination.
fn is_internal_host(host: &str) -> bool {
    // Try parsing as an IP address first (quick, no DNS).
    if let Ok(ip) = host.parse::<IpAddr>() {
        return is_internal_ip(ip);
    }

    // For hostnames, check well-known internal names.
    let lower = host.to_lowercase();
    lower == "localhost"
        || lower.ends_with(".local")
        || lower.ends_with(".internal")
        || lower.ends_with(".localhost")
}

/// Determine whether an IP address belongs to an internal network.
fn is_internal_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_broadcast()
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                || is_ipv6_unique_local(v6)
                || is_ipv6_link_local(v6)
        }
    }
}

/// Check if an IPv6 address is in the unique-local range `fc00::/7`.
fn is_ipv6_unique_local(ip: Ipv6Addr) -> bool {
    ip.segments()[0] & 0xfe00 == 0xfc00
}

/// Check if an IPv6 address is in the link-local range `fe80::/10`.
fn is_ipv6_link_local(ip: Ipv6Addr) -> bool {
    ip.segments()[0] & 0xffc0 == 0xfe80
}

// ── Upload edge composable ─────────────────────────────────────────────────────

/// Validate a single uploaded file against all upload-edge guards.
///
/// This is the one-stop function that every upload path (CLI, REST API,
/// folder watcher, web UI) should call before storing bytes or dispatching
/// to an extractor.
///
/// Checks performed, in order:
/// 1. File size is within `max_file_bytes`.
/// 2. File path (if provided) contains no traversal.
/// 3. MIME type (from magic-byte detection) is in the allow-list.
///
/// # Errors
///
/// Returns an error on the first failing check with a message suitable
/// for user-facing error responses.
pub fn validate_upload(
    bytes: &[u8],
    file_path: Option<&str>,
    max_file_bytes: u64,
) -> anyhow::Result<()> {
    // 1. Size check.
    validate_file_size(bytes.len() as u64, max_file_bytes)?;

    // 2. Path safety (if a filename was provided).
    if let Some(path) = file_path {
        validate_file_path(path)?;
    }

    // 3. MIME allow-list (magic-byte detection).
    let mime = detect_upload_mime(bytes);
    validate_mime(mime)?;

    Ok(())
}

/// Detect the MIME type of upload bytes via magic-byte inspection.
///
/// Wraps `tree_magic_mini::from_u8` with a fallback for empty input.
/// This is the same detection used by [`crate::binary::BinaryExtractor`].
fn detect_upload_mime(bytes: &[u8]) -> &'static str {
    if bytes.is_empty() {
        return "application/x-empty";
    }
    let detected = tree_magic_mini::from_u8(bytes);
    // Recover from the two "couldn't classify" results: octet-stream and the
    // generic application/x-riff (tree_magic_mini reports the latter for WAV — it
    // sees the RIFF container but not the WAVE form type).
    let mime = if matches!(detected, "application/octet-stream" | "application/x-riff") {
        normalize_octet_stream_magic(bytes)
    } else {
        detected
    };
    // A bare ZIP container is often actually an OOXML office document or a Java
    // archive — tree_magic_mini reports both as plain `application/zip`. Refine
    // by peeking at the archive's entry names so office docs route to the Tika
    // document path with the correct kind, and JARs are denied as executables
    // rather than silently accepted as archives (campaign findings F1, F10).
    if mime == "application/zip" {
        return classify_zip_container(bytes);
    }
    // Emails are RFC 822 text — tree_magic_mini detects them as text/plain.
    // Sniff for email headers so .eml files route to the Email extractor.
    if mime == "text/plain" && looks_like_email(bytes) {
        return "message/rfc822";
    }
    mime
}

/// Refine a detected `application/zip` container by peeking at its entry names.
///
/// `tree_magic_mini` reports OOXML office documents (.docx/.xlsx/.pptx) and Java
/// archives (.jar) as plain `application/zip` — they are all ZIP containers. This
/// scans a **bounded** prefix of the bytes (read-only substring match, no
/// allocation, no ZIP parsing) for the marker entry names that identify them:
///
/// - `[Content_Types].xml` (the Open Packaging Convention root part) → the
///   matching OOXML MIME (disambiguated by the `word/` / `xl/` / `ppt/` part
///   directory), which is allow-listed and routes to the Tika document extractor.
/// - `META-INF/MANIFEST.MF` (a Java manifest) → `application/java-archive`, which
///   is in [`DENIED_MIMES`] and is therefore rejected as an executable.
///
/// An unmarked ZIP stays `application/zip`. Detection is by content only, so a
/// renamed payload cannot bypass the deny path.
fn classify_zip_container(bytes: &[u8]) -> &'static str {
    // OPC/JAR markers live in the local file headers near the start of the
    // archive (the central directory trails at the end, but `[Content_Types].xml`
    // is the first OPC part and `META-INF/` is written early). A 64 KiB window is
    // generous and keeps this O(1) on untrusted input.
    const SCAN_LIMIT: usize = 64 * 1024;
    let window = &bytes[..bytes.len().min(SCAN_LIMIT)];

    // OOXML first: the OPC requires a `[Content_Types].xml` part. (OOXML never
    // carries a Java manifest, so this ordering is unambiguous.)
    if contains_subslice(window, b"[Content_Types].xml") {
        if contains_subslice(window, b"word/") {
            return "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
        }
        if contains_subslice(window, b"xl/") {
            return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
        }
        if contains_subslice(window, b"ppt/") {
            return "application/vnd.openxmlformats-officedocument.presentationml.presentation";
        }
        // An OPC package without a recognised flavour directory — Tika can still
        // extract it; default to the wordprocessing MIME (Document kind).
        return "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
    }

    // JAR: a ZIP carrying a Java manifest is an executable, not a data archive.
    if contains_subslice(window, b"META-INF/MANIFEST.MF") {
        return "application/java-archive";
    }

    "application/zip"
}

/// Return `true` when the first ~1 KB of `bytes` looks like an RFC 822 email.
///
/// Scans for at least 2 known email header names at line starts (e.g. `From:`,
/// `To:`, `Subject:`, `Date:`, `MIME-Version:`, `Message-ID:`, `Received:`,
/// `Return-Path:`, `Delivered-To:`). A `.eml` file is just text/plain to
/// tree\_magic\_mini, so this content-based sniffing is needed to route it to
/// the Email extractor rather than the Document/Text extractor.
fn looks_like_email(bytes: &[u8]) -> bool {
    const SCAN_LIMIT: usize = 1024;
    let window = &bytes[..bytes.len().min(SCAN_LIMIT)];
    let text = match std::str::from_utf8(window) {
        Ok(t) => t,
        Err(_) => return false,
    };
    let email_headers = [
        "From:",
        "To:",
        "Cc:",
        "Bcc:",
        "Subject:",
        "Date:",
        "MIME-Version:",
        "Message-ID:",
        "Message-Id:",
        "Received:",
        "Return-Path:",
        "Delivered-To:",
        "Reply-To:",
        "Content-Type:",
        "Content-Transfer-Encoding:",
        "In-Reply-To:",
    ];
    let mut found = 0usize;
    for header in &email_headers {
        if text.lines().any(|line| line.starts_with(header)) {
            found += 1;
            if found >= 2 {
                return true;
            }
        }
    }
    false
}

/// Return `true` when `haystack` contains `needle` as a contiguous subslice.
///
/// A bounded, allocation-free scan used by [`classify_zip_container`]. An empty
/// `needle`, or one longer than `haystack`, yields `false`.
fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || needle.len() > haystack.len() {
        return false;
    }
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// Recover a usable MIME type for container formats that `tree_magic_mini` 3.2.2
/// misclassifies as `application/octet-stream` despite a clear magic signature.
///
/// Detection is by **magic bytes only**, never by filename/declared type — a
/// renamed payload (e.g. an ELF named `.docx`) still carries its own magic and
/// is rejected, so this widens *detection*, not the allow-list policy. Each
/// returned type is already allow-listed and routes the file to the extractor
/// that can read it (BUG-INGEST-05 OOXML + audio/video ingestion); the bytes
/// then cross the sandboxed extractor boundary (Tika / ffmpeg+whisper).
/// Genuinely-unknown bytes stay `application/octet-stream` and are rejected.
fn normalize_octet_stream_magic(bytes: &[u8]) -> &'static str {
    // ZIP container (incl. OOXML .docx/.xlsx): `PK\x03\x04` → Tika.
    if bytes.starts_with(b"PK\x03\x04") {
        return "application/zip";
    }
    // RIFF/WAVE audio: `RIFF` then `WAVE` at offset 8 → whisper.
    if bytes.len() >= 12 && &bytes[0..4] == b"RIFF" && &bytes[8..12] == b"WAVE" {
        return "audio/wav";
    }
    // RIFF/WEBP image: `RIFF` then `WEBP` at offset 8. Newer shared-mime DBs
    // stopped classifying minimal WebP, dropping it to octet-stream
    // (BUG-INGEST-17 detection drift); the container magic is unambiguous.
    if bytes.len() >= 12 && &bytes[0..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        return "image/webp";
    }
    // ISO base media family: the `ftyp` box type at offset 4 is shared by MP4,
    // QuickTime, 3GP, HEIC/HEIF and AVIF — the *major brand* at offset 8
    // disambiguates. MOV→quicktime keeps the ffmpeg path with the right label;
    // still-image brands (HEIC/AVIF) return their image MIME, which is NOT
    // allow-listed, so they get a clean 400 instead of a silently-broken "video".
    if bytes.len() >= 12 && &bytes[4..8] == b"ftyp" {
        let brand = [bytes[8], bytes[9], bytes[10], bytes[11]];
        return match &brand {
            b"qt  " => "video/quicktime",
            b"heic" | b"heix" | b"heim" | b"heis" | b"heif" => "image/heic",
            b"mif1" | b"msf1" | b"avif" => "image/avif",
            b if b.starts_with(b"3gp") => "video/3gpp",
            _ => "video/mp4",
        };
    }
    "application/octet-stream"
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    #![allow(clippy::assertions_on_constants)]

    use std::net::Ipv4Addr;

    use super::*;

    // ── is_safe_path ───────────────────────────────────────────────────────────

    #[test]
    fn safe_paths_pass() {
        assert!(is_safe_path("document.pdf"));
        assert!(is_safe_path("subdir/file.txt"));
        assert!(is_safe_path("a/b/c/d.txt"));
        assert!(is_safe_path("file with spaces.txt"));
        assert!(is_safe_path("file-with-dashes.txt"));
        assert!(is_safe_path("file_with_underscores.txt"));
        assert!(is_safe_path("UPPERCASE.TXT"));
        assert!(is_safe_path(""));
    }

    #[test]
    fn dotdot_rejected() {
        assert!(!is_safe_path("../etc/passwd"));
        assert!(!is_safe_path("a/../../b"));
        assert!(!is_safe_path(".."));
        assert!(!is_safe_path("./../hidden"));
    }

    #[test]
    fn dotdotdot_rejected() {
        // "..." can resolve to ".." in some shells (e.g. PowerShell).
        assert!(!is_safe_path("..."));
        assert!(!is_safe_path("a/.../b"));
    }

    #[test]
    fn absolute_paths_rejected() {
        assert!(!is_safe_path("/etc/passwd"));
        assert!(!is_safe_path("/root/.ssh/id_rsa"));
        assert!(!is_safe_path("\\Windows\\System32"));
    }

    #[test]
    fn null_byte_rejected() {
        assert!(!is_safe_path("file\0hidden.txt"));
        assert!(!is_safe_path("good\0bad"));
    }

    #[test]
    fn backslash_rejected() {
        assert!(!is_safe_path("dir\\file.txt"));
        assert!(!is_safe_path("C:\\Users\\test"));
    }

    #[test]
    fn dots_only_long_sequences_rejected() {
        // Any sequence of 2+ dots as a component is blocked.
        assert!(!is_safe_path("dir/..../file"));
        assert!(!is_safe_path("....."));
    }

    #[test]
    fn single_dot_allowed() {
        // "." alone is a valid path component (current directory).
        assert!(is_safe_path("./file.txt"));
        assert!(is_safe_path("dir/./file.txt"));
    }

    // ── validate_file_path ─────────────────────────────────────────────────────

    #[test]
    fn validate_safe_paths_pass() {
        validate_file_path("doc.pdf").unwrap();
        validate_file_path("sub/dir/file.txt").unwrap();
        validate_file_path("").unwrap();
    }

    #[test]
    fn validate_unsafe_path_errors() {
        let err = validate_file_path("../bad.txt").unwrap_err();
        assert!(err.to_string().contains("path-traversal"));
        assert!(err.to_string().contains("../bad.txt"));
    }

    // ── validate_file_size ─────────────────────────────────────────────────────

    #[test]
    fn size_under_limit_passes() {
        validate_file_size(100, 1024).unwrap();
        validate_file_size(0, 1024).unwrap();
        validate_file_size(1024, 1024).unwrap();
    }

    #[test]
    fn size_over_limit_errors() {
        let err = validate_file_size(1025, 1024).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("exceeds limit"));
        assert!(msg.contains("1025"));
        assert!(msg.contains("1024"));
    }

    // ── is_allowed_mime ────────────────────────────────────────────────────────

    #[test]
    fn common_mimes_allowed() {
        assert!(is_allowed_mime("text/plain", ALLOWED_MIMES));
        assert!(is_allowed_mime("image/jpeg", ALLOWED_MIMES));
        assert!(is_allowed_mime("application/pdf", ALLOWED_MIMES));
        assert!(is_allowed_mime("audio/mpeg", ALLOWED_MIMES));
        assert!(is_allowed_mime("video/mp4", ALLOWED_MIMES));
        assert!(is_allowed_mime("application/zip", ALLOWED_MIMES));
    }

    #[test]
    fn tree_magic_mini_mime_variants_allowed() {
        // tree_magic_mini returns unpredictable text/x-* variants for source
        // code files (x-csrc, x-python3, x-modelica, x-matlab, …). The
        // is_allowed_mime function now accepts ANY text/x-* prefix so we
        // don't play whack-a-mole.
        assert!(is_allowed_mime("text/x-csrc", ALLOWED_MIMES));
        assert!(is_allowed_mime("text/x-python3", ALLOWED_MIMES));
        assert!(is_allowed_mime("text/x-modelica", ALLOWED_MIMES));
        assert!(is_allowed_mime("text/x-matlab", ALLOWED_MIMES));
        assert!(is_allowed_mime("text/x-fortran", ALLOWED_MIMES));
        assert!(is_allowed_mime(
            "text/x-unknown-future-variant",
            ALLOWED_MIMES
        ));
    }

    #[test]
    fn denied_mimes_blocked_even_if_in_allowed() {
        // DENIED_MIMES entries are blocked regardless.
        assert!(!is_allowed_mime("application/x-msdownload", ALLOWED_MIMES));
        assert!(!is_allowed_mime("application/x-dosexec", ALLOWED_MIMES));
    }

    #[test]
    fn unknown_mime_rejected() {
        assert!(!is_allowed_mime("application/x-unknown", ALLOWED_MIMES));
        assert!(!is_allowed_mime("evil/type", ALLOWED_MIMES));
        assert!(!is_allowed_mime("", ALLOWED_MIMES));
    }

    // ── validate_mime ──────────────────────────────────────────────────────────

    #[test]
    fn validate_allowed_mime_passes() {
        validate_mime("text/plain").unwrap();
        validate_mime("image/png").unwrap();
    }

    #[test]
    fn validate_denied_mime_errors() {
        let err = validate_mime("application/octet-stream").unwrap_err();
        assert!(err.to_string().contains("unsupported MIME type"));
    }

    // ── extract_url_host ───────────────────────────────────────────────────────

    #[test]
    fn extract_host_from_http_url() {
        assert_eq!(
            extract_url_host("http://tika:9998/tika").as_deref(),
            Some("tika")
        );
    }

    #[test]
    fn extract_host_from_https_url() {
        assert_eq!(
            extract_url_host("https://example.com/path").as_deref(),
            Some("example.com")
        );
    }

    #[test]
    fn extract_host_no_scheme() {
        assert_eq!(extract_url_host("tika:9998").as_deref(), Some("tika"));
    }

    #[test]
    fn extract_host_with_userinfo() {
        assert_eq!(
            extract_url_host("http://user:pass@host:8080/path").as_deref(),
            Some("host")
        );
    }

    #[test]
    fn extract_host_ipv6() {
        assert_eq!(
            extract_url_host("http://[::1]:8080/api").as_deref(),
            Some("::1")
        );
    }

    #[test]
    fn extract_host_empty_string() {
        assert_eq!(extract_url_host(""), None);
    }

    // ── is_safe_target_url ─────────────────────────────────────────────────────

    #[test]
    fn compose_service_names_are_safe() {
        assert!(is_safe_target_url("http://tika:9998/tika"));
        assert!(is_safe_target_url(
            "http://whisper-server:8080/v1/audio/transcriptions"
        ));
        assert!(is_safe_target_url("http://postgres:5432"));
    }

    #[test]
    fn localhost_rejected() {
        assert!(!is_safe_target_url("http://localhost:9998"));
        assert!(!is_safe_target_url("http://localhost:8080"));
        // Internal hostname suffixes.
        assert!(!is_safe_target_url("http://db.internal:5432"));
        assert!(!is_safe_target_url("http://service.local:8080"));
    }

    #[test]
    fn loopback_ip_rejected() {
        assert!(!is_safe_target_url("http://127.0.0.1:8080"));
        assert!(!is_safe_target_url("http://127.0.0.2:8080"));
        assert!(!is_safe_target_url("http://[::1]:8080"));
    }

    #[test]
    fn private_ip_rejected() {
        assert!(!is_safe_target_url("http://10.0.0.1:8080"));
        assert!(!is_safe_target_url("http://172.16.0.1:8080"));
        assert!(!is_safe_target_url("http://192.168.1.1:8080"));
    }

    #[test]
    fn link_local_rejected() {
        assert!(!is_safe_target_url("http://169.254.1.1:8080"));
        assert!(!is_safe_target_url("http://[fe80::1]:8080"));
    }

    #[test]
    fn unspecified_rejected() {
        assert!(!is_safe_target_url("http://0.0.0.0:8080"));
        assert!(!is_safe_target_url("http://[::]:8080"));
    }

    #[test]
    fn dot_local_rejected() {
        assert!(!is_safe_target_url("http://service.local:8080"));
        assert!(!is_safe_target_url("http://db.internal:5432"));
    }

    #[test]
    fn public_hosts_accepted() {
        assert!(is_safe_target_url("http://example.com:8080"));
        assert!(is_safe_target_url("https://api.openai.com/v1"));
        assert!(is_safe_target_url("http://1.2.3.4:8080"));
    }

    #[test]
    fn malformed_url_rejected() {
        // Empty string cannot be parsed to a host.
        assert!(!is_safe_target_url(""));
    }

    // ── is_internal_ip ─────────────────────────────────────────────────────────

    #[test]
    fn internal_ipv4_loopback() {
        assert!(is_internal_ip(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))));
    }

    #[test]
    fn internal_ipv4_private() {
        assert!(is_internal_ip(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))));
        assert!(is_internal_ip(IpAddr::V4(Ipv4Addr::new(172, 16, 0, 1))));
        assert!(is_internal_ip(IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1))));
    }

    #[test]
    fn internal_ipv4_link_local() {
        assert!(is_internal_ip(IpAddr::V4(Ipv4Addr::new(169, 254, 1, 1))));
    }

    #[test]
    fn internal_ipv4_unspecified() {
        assert!(is_internal_ip(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0))));
    }

    #[test]
    fn internal_ipv4_broadcast() {
        assert!(is_internal_ip(IpAddr::V4(Ipv4Addr::new(
            255, 255, 255, 255
        ))));
    }

    #[test]
    fn public_ipv4_not_internal() {
        assert!(!is_internal_ip(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))));
        assert!(!is_internal_ip(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1))));
    }

    #[test]
    fn internal_ipv6_loopback() {
        assert!(is_internal_ip(IpAddr::V6(Ipv6Addr::LOCALHOST)));
    }

    #[test]
    fn internal_ipv6_unspecified() {
        assert!(is_internal_ip(IpAddr::V6(Ipv6Addr::UNSPECIFIED)));
    }

    #[test]
    fn ipv6_unique_local() {
        assert!(is_ipv6_unique_local(Ipv6Addr::new(
            0xfd00, 0, 0, 0, 0, 0, 0, 1
        )));
        assert!(!is_ipv6_unique_local(Ipv6Addr::new(
            0x2001, 0, 0, 0, 0, 0, 0, 1
        )));
    }

    #[test]
    fn ipv6_link_local() {
        assert!(is_ipv6_link_local(Ipv6Addr::new(
            0xfe80, 0, 0, 0, 0, 0, 0, 1
        )));
        assert!(!is_ipv6_link_local(Ipv6Addr::new(
            0x2001, 0, 0, 0, 0, 0, 0, 1
        )));
    }

    // ── validate_upload (composable) ──────────────────────────────────────────

    #[test]
    fn valid_text_file_passes_all_guards() {
        let bytes = b"Hello, world! This is a text file.\n";
        validate_upload(bytes, Some("readme.txt"), MAX_INDIVIDUAL_FILE_BYTES).unwrap();
    }

    #[test]
    fn valid_image_passes() {
        // Minimal JPEG header
        let jpeg = [
            0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46, 0x00, 0x01,
        ];
        validate_upload(&jpeg, Some("photo.jpg"), MAX_INDIVIDUAL_FILE_BYTES).unwrap();
    }

    #[test]
    fn oversize_file_rejected() {
        let bytes = vec![0u8; 1024];
        let err = validate_upload(&bytes, Some("big.dat"), 512).unwrap_err();
        assert!(err.to_string().contains("exceeds limit"));
    }

    #[test]
    fn traverse_path_rejected() {
        let bytes = b"harmless content";
        let err = validate_upload(
            bytes,
            Some("../../../etc/passwd"),
            MAX_INDIVIDUAL_FILE_BYTES,
        )
        .unwrap_err();
        assert!(err.to_string().contains("path-traversal"));
    }

    #[test]
    fn null_byte_path_rejected() {
        let bytes = b"content";
        let err =
            validate_upload(bytes, Some("file\0.exe"), MAX_INDIVIDUAL_FILE_BYTES).unwrap_err();
        assert!(err.to_string().contains("path-traversal"));
    }

    #[test]
    fn executable_mime_rejected() {
        // ELF magic bytes → detected as executable.
        let elf = [0x7f, 0x45, 0x4c, 0x46, 0x02, 0x01, 0x01, 0x00];
        let err = validate_upload(&elf, Some("program"), MAX_INDIVIDUAL_FILE_BYTES).unwrap_err();
        assert!(err.to_string().contains("unsupported MIME type"));
    }

    #[test]
    fn rejections_downcast_to_upload_rejected() {
        // BUG-INGEST-06/07: every validate_upload rejection must be downcastable
        // to a typed UploadRejected so the API layer can answer 400 (not 500).

        // Zero-byte upload → application/x-empty → DisallowedMime (BUG-INGEST-06).
        let err = validate_upload(b"", None, 1024).unwrap_err();
        match err.downcast_ref::<UploadRejected>() {
            Some(UploadRejected::DisallowedMime(m)) => assert_eq!(m, "application/x-empty"),
            other => panic!("expected DisallowedMime, got {other:?}"),
        }

        // MIME/magic mismatch (ELF executable) → DisallowedMime (BUG-INGEST-07).
        let elf = [0x7f, 0x45, 0x4c, 0x46, 0x02, 0x01, 0x01, 0x00];
        let err = validate_upload(&elf, Some("program"), MAX_INDIVIDUAL_FILE_BYTES).unwrap_err();
        assert!(matches!(
            err.downcast_ref::<UploadRejected>(),
            Some(UploadRejected::DisallowedMime(_))
        ));

        // Oversized → TooLarge.
        let big = vec![b'a'; 2048];
        let err = validate_upload(&big, Some("big.dat"), 512).unwrap_err();
        assert!(matches!(
            err.downcast_ref::<UploadRejected>(),
            Some(UploadRejected::TooLarge { .. })
        ));

        // Unsafe filename → UnsafePath.
        let err = validate_upload(b"hello", Some("../escape.txt"), 1024).unwrap_err();
        assert!(matches!(
            err.downcast_ref::<UploadRejected>(),
            Some(UploadRejected::UnsafePath(_))
        ));
    }

    #[test]
    fn normalize_octet_stream_magic_recovers_container_types() {
        // tree_magic_mini mislabels these containers as octet-stream; recover
        // them by magic so they reach the right extractor (BUG-INGEST-05 OOXML +
        // audio/video). Test the pure helper directly (independent of how
        // tree_magic happens to classify a given sample).
        let mut zip = b"PK\x03\x04".to_vec();
        zip.extend_from_slice(&[0u8; 8]);
        assert_eq!(normalize_octet_stream_magic(&zip), "application/zip");

        let mut wav = b"RIFF".to_vec();
        wav.extend_from_slice(&[0u8; 4]);
        wav.extend_from_slice(b"WAVE");
        wav.extend_from_slice(&[0u8; 8]);
        assert_eq!(normalize_octet_stream_magic(&wav), "audio/wav");

        // RIFF/WEBP (newer shared-mime DBs drop minimal WebP to octet-stream —
        // BUG-INGEST-17 detection drift).
        let mut webp = b"RIFF".to_vec();
        webp.extend_from_slice(&[0u8; 4]);
        webp.extend_from_slice(b"WEBP");
        webp.extend_from_slice(&[0u8; 8]);
        assert_eq!(normalize_octet_stream_magic(&webp), "image/webp");
        assert!(is_allowed_mime("image/webp", ALLOWED_MIMES));
        // And the matroska naming variant is allow-listed.
        assert!(is_allowed_mime("application/x-matroska", ALLOWED_MIMES));

        let mut mp4 = vec![0u8, 0, 0, 0x14];
        mp4.extend_from_slice(b"ftypisom");
        mp4.extend_from_slice(&[0u8; 8]);
        assert_eq!(normalize_octet_stream_magic(&mp4), "video/mp4");

        // Genuinely-unknown bytes stay octet-stream (still rejected).
        assert_eq!(
            normalize_octet_stream_magic(b"\x01\x02\x03 not a known container"),
            "application/octet-stream"
        );

        // Every recovered type is allow-listed (so a normalised file passes the
        // allow-list and routes to its extractor).
        assert!(is_allowed_mime("application/zip", ALLOWED_MIMES));
        assert!(is_allowed_mime("audio/wav", ALLOWED_MIMES));
        assert!(is_allowed_mime("video/mp4", ALLOWED_MIMES));
    }

    #[test]
    fn ftyp_brand_disambiguates_iso_base_media() {
        // The `ftyp` box marker is shared across the ISO base-media family; the
        // major brand at offset 8 is what tells MP4 / MOV / 3GP / HEIC / AVIF
        // apart. Building `\0\0\0\x14ftyp<brand>` + padding for each.
        let ftyp = |brand: &[u8]| {
            let mut v = vec![0u8, 0, 0, 0x14];
            v.extend_from_slice(b"ftyp");
            v.extend_from_slice(brand);
            v.extend_from_slice(&[0u8; 8]);
            v
        };

        // QuickTime .mov → video/quicktime (allow-listed → same ffmpeg path, right label).
        assert_eq!(
            normalize_octet_stream_magic(&ftyp(b"qt  ")),
            "video/quicktime"
        );
        assert!(is_allowed_mime("video/quicktime", ALLOWED_MIMES));

        // OGG container (detected as video/ogg by tree_magic for both
        // audio-only and video OGG files — must be allowed so audio OGG
        // uploads are not rejected).
        assert!(is_allowed_mime("video/ogg", ALLOWED_MIMES));

        // MP4 brands stay video/mp4.
        assert_eq!(normalize_octet_stream_magic(&ftyp(b"mp42")), "video/mp4");
        assert_eq!(normalize_octet_stream_magic(&ftyp(b"isom")), "video/mp4");

        // Still-image brands → image MIME, which is NOT allow-listed, so they get
        // a clean 400 rather than being mislabelled as a broken "video".
        assert_eq!(normalize_octet_stream_magic(&ftyp(b"heic")), "image/heic");
        assert_eq!(normalize_octet_stream_magic(&ftyp(b"heif")), "image/heic");
        assert_eq!(normalize_octet_stream_magic(&ftyp(b"avif")), "image/avif");
        assert_eq!(normalize_octet_stream_magic(&ftyp(b"mif1")), "image/avif");
        assert!(!is_allowed_mime("image/heic", ALLOWED_MIMES));
        assert!(!is_allowed_mime("image/avif", ALLOWED_MIMES));

        // 3GP video → video/3gpp (also not allow-listed → clean 400).
        assert_eq!(normalize_octet_stream_magic(&ftyp(b"3gp4")), "video/3gpp");
        assert!(!is_allowed_mime("video/3gpp", ALLOWED_MIMES));

        // A truncated ftyp box (no full brand) can't be classified → octet-stream.
        let mut truncated = vec![0u8, 0, 0, 0x14];
        truncated.extend_from_slice(b"ftyp");
        assert_eq!(
            normalize_octet_stream_magic(&truncated),
            "application/octet-stream"
        );
    }

    #[test]
    fn empty_bytes_validated() {
        // Empty bytes → application/x-empty → not in allow-list.
        let err = validate_upload(b"", Some("empty.txt"), MAX_INDIVIDUAL_FILE_BYTES).unwrap_err();
        assert!(err.to_string().contains("unsupported MIME type"));
    }

    #[test]
    fn no_path_passes_when_other_checks_ok() {
        let bytes = b"Some text content here.";
        validate_upload(bytes, None, MAX_INDIVIDUAL_FILE_BYTES).unwrap();
    }

    // ── detect_upload_mime ─────────────────────────────────────────────────────

    #[test]
    fn detect_png_magic() {
        let png = [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];
        let mime = detect_upload_mime(&png);
        assert_eq!(mime, "image/png");
    }

    #[test]
    fn detect_jpeg_magic() {
        let jpeg = [0xFF, 0xD8, 0xFF, 0xE0];
        let mime = detect_upload_mime(&jpeg);
        // tree_magic_mini reports this as image/jpeg.
        assert!(mime.starts_with("image/"));
    }

    #[test]
    fn detect_empty_returns_x_empty() {
        let mime = detect_upload_mime(b"");
        assert_eq!(mime, "application/x-empty");
    }

    #[test]
    fn detect_unknown_falls_to_text_plain() {
        let mime = detect_upload_mime(b"random bytes not matching any magic");
        assert_eq!(mime, "text/plain");
    }

    // ── classify_zip_container (F1 OOXML routing, F10 JAR deny) ──────────────

    /// Build a fake ZIP-ish blob: PK local-file-header magic + the given marker
    /// byte strings (each followed by a NUL), enough for the content scan.
    fn zip_with(markers: &[&[u8]]) -> Vec<u8> {
        let mut v = b"PK\x03\x04".to_vec();
        for m in markers {
            v.extend_from_slice(m);
            v.push(0);
        }
        v
    }

    #[test]
    fn classify_zip_ooxml_flavours() {
        let docx = zip_with(&[b"[Content_Types].xml", b"word/document.xml"]);
        assert_eq!(
            classify_zip_container(&docx),
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        );
        let xlsx = zip_with(&[b"[Content_Types].xml", b"xl/workbook.xml"]);
        assert_eq!(
            classify_zip_container(&xlsx),
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        );
        let pptx = zip_with(&[b"[Content_Types].xml", b"ppt/presentation.xml"]);
        assert_eq!(
            classify_zip_container(&pptx),
            "application/vnd.openxmlformats-officedocument.presentationml.presentation"
        );
    }

    #[test]
    fn classify_zip_opc_without_flavour_defaults_to_docx() {
        let opc = zip_with(&[b"[Content_Types].xml"]);
        assert_eq!(
            classify_zip_container(&opc),
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        );
    }

    #[test]
    fn classify_zip_jar_is_java_archive() {
        let jar = zip_with(&[b"META-INF/MANIFEST.MF", b"com/x/Main.class"]);
        assert_eq!(classify_zip_container(&jar), "application/java-archive");
    }

    #[test]
    fn classify_zip_plain_stays_zip() {
        let plain = zip_with(&[b"data/file.bin", b"readme.txt"]);
        assert_eq!(classify_zip_container(&plain), "application/zip");
    }

    #[test]
    fn classify_zip_ooxml_takes_precedence_over_manifest() {
        // A pathological archive carrying both markers is OOXML (a real OOXML
        // never carries a Java manifest; the documented order resolves it).
        let both = zip_with(&[
            b"[Content_Types].xml",
            b"word/document.xml",
            b"META-INF/MANIFEST.MF",
        ]);
        assert_eq!(
            classify_zip_container(&both),
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        );
    }

    #[test]
    fn contains_subslice_basics() {
        assert!(contains_subslice(b"hello world", b"world"));
        assert!(contains_subslice(b"abc", b"abc"));
        assert!(!contains_subslice(b"abc", b"abcd"));
        assert!(!contains_subslice(b"abc", b""));
        assert!(!contains_subslice(b"", b"x"));
    }

    /// F10: a JAR (PK magic + Java manifest) is detected as java-archive and
    /// rejected by `validate_upload` as an executable, not accepted as an archive.
    #[test]
    fn jar_marked_zip_is_denied() {
        let jar = zip_with(&[b"META-INF/MANIFEST.MF", b"com/x/Main.class"]);
        let err = validate_upload(&jar, Some("app.jar"), MAX_INDIVIDUAL_FILE_BYTES).unwrap_err();
        match err.downcast_ref::<UploadRejected>() {
            Some(UploadRejected::DisallowedMime(m)) => {
                assert_eq!(m, "application/java-archive")
            }
            other => panic!("expected DisallowedMime(java-archive), got {other:?}"),
        }
    }

    /// F1: an OOXML doc (PK magic + OPC markers) is detected as its office MIME
    /// (Document/Tika path) and passes `validate_upload`.
    #[test]
    fn ooxml_marked_zip_is_allowed_as_document() {
        let docx = zip_with(&[b"[Content_Types].xml", b"word/document.xml"]);
        assert_eq!(
            detect_upload_mime(&docx),
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        );
        validate_upload(&docx, Some("report.docx"), MAX_INDIVIDUAL_FILE_BYTES).unwrap();
    }

    // ── DENIED_MIMES list integrity ────────────────────────────────────────────

    #[test]
    fn denied_mimes_are_not_in_allowed() {
        for &denied in DENIED_MIMES {
            assert!(
                !ALLOWED_MIMES.contains(&denied),
                "DENIED_MIME '{denied}' should not appear in ALLOWED_MIMES"
            );
        }
    }

    // ── Constant sanity ────────────────────────────────────────────────────────

    #[test]
    fn max_individual_exceeds_total() {
        // A single large file can be uploaded alone even though the
        // total upload cap is lower.
        assert!(
            MAX_INDIVIDUAL_FILE_BYTES > MAX_TOTAL_UPLOAD_BYTES,
            "single-file cap should exceed total multipart cap"
        );
    }

    #[test]
    fn allowed_mimes_is_non_empty() {
        assert!(!ALLOWED_MIMES.is_empty());
    }
}
