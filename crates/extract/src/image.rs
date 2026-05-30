//! Image extractor: validates magic bytes for JPEG/PNG/GIF/WebP/TIFF and harvests
//! EXIF metadata via `kamadak-exif` into namespaced `meta` JSON.
//!
//! Produces one [`PageImage`] carrying the original encoded bytes + MIME type for
//! later VLM captioning (the actual VLM call lands in P3). Does **not** decode
//! pixels or resize — raw bytes pass through (plan §2, §7).
//!
//! No EXIF means an empty `meta` object, not an error — many images lack EXIF headers
//! (screenshots, generated graphics, cleaned files).

use async_trait::async_trait;
use kb_core::extractor::{Extracted, Extractor, PageImage, RawFile};

// ── magic byte helpers ──────────────────────────────────────────────────────

/// Detect an image format from magic bytes. Returns the MIME type on success,
/// or `None` if the bytes don't match any known image header.
///
/// Recognised formats (and their MIME type):
/// - JPEG — `image/jpeg`
/// - PNG  — `image/png`
/// - GIF  — `image/gif`
/// - WebP — `image/webp`
/// - TIFF — `image/tiff`
fn detect_image_mime(bytes: &[u8]) -> Option<&'static str> {
    // JPEG: FF D8 FF
    if bytes.len() >= 3 && bytes[0] == 0xFF && bytes[1] == 0xD8 && bytes[2] == 0xFF {
        return Some("image/jpeg");
    }
    // PNG: 89 50 4E 47 0D 0A 1A 0A
    if bytes.len() >= 8 && bytes[0..8] == [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A] {
        return Some("image/png");
    }
    // GIF: 47 49 46 38 (GIF8)
    if bytes.len() >= 4 && bytes[0..4] == [0x47, 0x49, 0x46, 0x38] {
        return Some("image/gif");
    }
    // WebP: RIFF....WEBP
    if bytes.len() >= 12
        && bytes[0..4] == [0x52, 0x49, 0x46, 0x46] // "RIFF"
        && bytes[8..12] == [0x57, 0x45, 0x42, 0x50]
    // "WEBP"
    {
        return Some("image/webp");
    }
    // TIFF: II (little-endian) or MM (big-endian)
    if bytes.len() >= 4
        && ((bytes[0..4] == [0x49, 0x49, 0x2A, 0x00]) || (bytes[0..4] == [0x4D, 0x4D, 0x00, 0x2A]))
    {
        return Some("image/tiff");
    }
    None
}

/// Verify that the given bytes start with magic bytes for a known image format.
///
/// Returns the MIME type on success, or an error describing the mismatch.
fn validate_image(bytes: &[u8], path: Option<&str>) -> anyhow::Result<&'static str> {
    detect_image_mime(bytes).ok_or_else(|| {
        let name = path.unwrap_or("<unknown>");
        let end = bytes.len().min(16);
        let preview = if end > 0 {
            bytes[..end]
                .iter()
                .map(|x| format!("{x:02X}"))
                .collect::<Vec<_>>()
                .join(" ")
        } else {
            "(empty)".into()
        };
        anyhow::anyhow!(
            "ImageExtractor: file '{name}' is not a recognised image format \
             (first 16 bytes: [{preview}])"
        )
    })
}

// ── EXIF harvest ────────────────────────────────────────────────────────────

/// Harvest EXIF metadata from the raw bytes and return it as a namespaced JSON
/// object (`"exif:<tag>"` → `"<value>"`).
///
/// Returns an empty JSON object on parse failure or when no EXIF data is present —
/// it is never an error because many valid images lack EXIF headers (screenshots,
/// generated graphics, etc.).
fn harvest_exif(bytes: &[u8]) -> serde_json::Value {
    let mut cursor = std::io::Cursor::new(bytes);
    let reader = exif::Reader::new();
    let exif_data = match reader.read_from_container(&mut cursor) {
        Ok(data) => data,
        Err(_) => return serde_json::Value::Object(Default::default()),
    };

    let mut map = serde_json::Map::new();
    for field in exif_data.fields() {
        let key = format!("exif:{}", field.tag);
        let value = exif_value_to_string(&exif_data, field);
        map.insert(key, serde_json::Value::String(value));
    }

    serde_json::Value::Object(map)
}

/// Convert an EXIF field value to a clean string representation.
///
/// For ASCII strings we extract the raw bytes (trailing NULs already stripped by
/// `kamadak-exif`) so the result does not include the display-layer double-quotes.
/// All other types use the library's `display_value().with_unit()` formatter.
fn exif_value_to_string(exif: &exif::Exif, field: &exif::Field) -> String {
    if let exif::Value::Ascii(ref strings) = field.value {
        // Each inner Vec<u8> is one null-terminated string (NUL already stripped).
        strings
            .iter()
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .collect::<Vec<_>>()
            .join(", ")
    } else if let exif::Value::Undefined(ref data, _) = field.value {
        // Undefined blobs: display as hex to keep meta deterministic and readable.
        let hex = data
            .iter()
            .take(32)
            .map(|b| format!("{b:02X}"))
            .collect::<Vec<_>>()
            .join("");
        if data.len() > 32 {
            format!("{hex}… ({len}B)", len = data.len())
        } else {
            hex
        }
    } else {
        field.display_value().with_unit(exif).to_string()
    }
}

// ── ImageExtractor ──────────────────────────────────────────────────────────

/// Extracts metadata and a [`PageImage`] from image-kind files.
///
/// - **Magic byte validation:** rejects non-image bytes outright.
/// - **EXIF harvest:** reads EXIF if present → namespaced `"exif:<tag>"` keys in
///   [`Extracted::meta`]; empty object when EXIF is absent (not an error).
/// - **Page image:** stores the original encoded bytes + MIME in
///   [`Extracted::page_images`] so the VLM captioner (P3) can consume them.
///
/// No pixel decoding, resizing, or VLM call happens here — raw bytes pass through.
///
/// # Examples
///
/// ```rust
/// use bytes::Bytes;
/// use kb_core::extractor::{Extracted, Extractor, RawFile};
/// use kb_core::kind::DocKind;
/// use kb_extract::image::ImageExtractor;
///
/// # async fn example() -> anyhow::Result<()> {
/// let ex = ImageExtractor;
/// // Minimal valid JPEG: SOI marker + EOI marker, no EXIF.
/// let jpeg: &[u8] = &[0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46, 0x00,
///                      0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0xFF, 0xDB,
///                      0x00, 0x43, 0x00, 0x08, 0x06, 0x06, 0x07, 0x06, 0x05, 0x08, 0x07,
///                      0x07, 0x07, 0x09, 0x09, 0x08, 0x0A, 0x0C, 0x14, 0x0D, 0x0C, 0x0B,
///                      0x0B, 0x0C, 0x19, 0x12, 0x13, 0x0F, 0x14, 0x1D, 0x1A, 0x1F, 0x1E,
///                      0x1D, 0x1A, 0x1C, 0x1C, 0x20, 0x24, 0x2E, 0x27, 0x20, 0x22, 0x2C,
///                      0x23, 0x1C, 0x1C, 0x28, 0x37, 0x29, 0x2C, 0x30, 0x31, 0x34, 0x34,
///                      0x34, 0x1F, 0x27, 0x39, 0x3D, 0x38, 0x32, 0x3C, 0x2E, 0x33, 0x34,
///                      0x32, 0xFF, 0xC0, 0x00, 0x0B, 0x08, 0x00, 0x01, 0x00, 0x01, 0x01,
///                      0x01, 0x11, 0x00, 0xFF, 0xC4, 0x00, 0x1F, 0x00, 0x00, 0x01, 0x05,
///                      0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
///                      0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
///                      0x09, 0x0A, 0x0B, 0xFF, 0xDA, 0x00, 0x08, 0x01, 0x01, 0x00, 0x00,
///                      0x3F, 0x00, 0x7B, 0x48, 0x3A, 0x55, 0x00, 0xFF, 0xD9];
/// let raw = RawFile {
///     bytes: Bytes::copy_from_slice(jpeg),
///     mime: Some("image/jpeg".into()),
///     kind: DocKind::Image,
///     path: Some("photo.jpg".into()),
/// };
/// let out = ex.extract(&raw).await?;
/// assert_eq!(out.text, "");
/// assert_eq!(out.page_images.len(), 1);
/// assert_eq!(out.page_images[0].mime, "image/jpeg");
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct ImageExtractor;

#[async_trait]
impl Extractor for ImageExtractor {
    async fn extract(&self, file: &RawFile) -> anyhow::Result<Extracted> {
        let mime = validate_image(&file.bytes, file.path.as_deref())?;

        let meta = harvest_exif(&file.bytes);

        let page_image = PageImage {
            data: file.bytes.clone(),
            mime: mime.into(),
        };

        Ok(Extracted {
            text: String::new(),
            meta,
            page_images: vec![page_image],
        })
    }
}

// ── unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use bytes::Bytes;
    use kb_core::extractor::RawFile;
    use kb_core::kind::DocKind;

    // ── helpers ─────────────────────────────────────────────────────────────

    /// Build a [`RawFile`] for an image with the given bytes, MIME, and path.
    fn image_raw(bytes: &[u8], mime: &str, path: &str) -> RawFile {
        RawFile {
            bytes: Bytes::copy_from_slice(bytes),
            mime: Some(mime.into()),
            kind: DocKind::Image,
            path: Some(path.into()),
        }
    }

    /// Build a minimal valid JPEG: SOI, APP0 (JFIF), DQT, SOF0, DHT, SOS, scan data, EOI.
    ///
    /// This is exactly 1×1 pixel, grayscale, no EXIF. It is valid enough to pass magic
    /// byte detection and not confuse `kamadak-exif` (which will simply find no EXIF).
    fn minimal_jpeg() -> Vec<u8> {
        vec![
            0xFF, 0xD8, // SOI
            0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46, 0x00, 0x01, 0x01, 0x00, 0x00, 0x01,
            0x00, 0x01, 0x00, 0x00, // APP0
            0xFF, 0xDB, 0x00, 0x43, 0x00, // DQT
            0x08, 0x06, 0x06, 0x07, 0x06, 0x05, 0x08, 0x07, 0x07, 0x07, 0x09, 0x09, 0x08, 0x0A,
            0x0C, 0x14, 0x0D, 0x0C, 0x0B, 0x0B, 0x0C, 0x19, 0x12, 0x13, 0x0F, 0x14, 0x1D, 0x1A,
            0x1F, 0x1E, 0x1D, 0x1A, 0x1C, 0x1C, 0x20, 0x24, 0x2E, 0x27, 0x20, 0x22, 0x2C, 0x23,
            0x1C, 0x1C, 0x28, 0x37, 0x29, 0x2C, 0x30, 0x31, 0x34, 0x34, 0x34, 0x1F, 0x27, 0x39,
            0x3D, 0x38, 0x32, 0x3C, 0x2E, 0x33, 0x34, 0x32, // quant table
            0xFF, 0xC0, 0x00, 0x0B, 0x08, 0x00, 0x01, 0x00, 0x01, 0x01, 0x01, 0x11,
            0x00, // SOF0
            0xFF, 0xC4, 0x00, 0x1F, 0x00, 0x00, 0x01, 0x05, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
            0x07, 0x08, 0x09, 0x0A, 0x0B, // DHT
            0xFF, 0xDA, 0x00, 0x08, 0x01, 0x01, 0x00, 0x00, 0x3F, 0x00, // SOS
            0x7B, // scan data (1 encoded MCU for 1x1 gray)
            0xFF, 0xD9, // EOI
        ]
    }

    /// Build a minimal valid PNG (IHDR+IDAT+IEND, 1×1 gray pixel, no EXIF).
    fn minimal_png() -> Vec<u8> {
        let mut buf = Vec::new();
        // PNG signature
        buf.extend_from_slice(&[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]);
        // IHDR chunk
        let ihdr_data: Vec<u8> = {
            let mut d = Vec::new();
            d.extend_from_slice(&1u32.to_be_bytes()); // width
            d.extend_from_slice(&1u32.to_be_bytes()); // height
            d.extend_from_slice(&[8, 0, 0, 0, 0]); // bit depth 8, gray, no filter/interlace
            d
        };
        png_chunk(&mut buf, b"IHDR", &ihdr_data);
        // IDAT chunk (filter byte 0 + 1 gray pixel)
        let idat = deflate_encode(&[0, 128]);
        png_chunk(&mut buf, b"IDAT", &idat);
        // IEND chunk
        png_chunk(&mut buf, b"IEND", &[]);
        buf
    }

    /// Append a single PNG chunk (big-endian length, type, data, CRC placeholder of zeros).
    fn png_chunk(buf: &mut Vec<u8>, ctype: &[u8; 4], data: &[u8]) {
        buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
        buf.extend_from_slice(ctype);
        buf.extend_from_slice(data);
        // Use zeros as a simplified CRC — kamadak-exif doesn't validate CRC.
        buf.extend_from_slice(&[0u8; 4]);
    }

    /// Minimal deflate (zlib-wrapped) encoder using a stored (uncompressed) block.
    fn deflate_encode(data: &[u8]) -> Vec<u8> {
        // zlib header: CMF=0x78 FLG=0x01 (no dict, compression level 0)
        let mut out = vec![0x78, 0x01];
        // Deflate stored block: BFINAL=1, BTYPE=0 (no compression)
        let block_header = 0x01u8; // final block, stored
        out.push(block_header);
        let len = data.len() as u16;
        let nlen = !len;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&nlen.to_le_bytes());
        out.extend_from_slice(data);
        // Adler-32 checksum (placeholder — enough that the header is RFC-1950 compliant).
        out.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
        out
    }

    // ── detect_image_mime ───────────────────────────────────────────────────

    #[test]
    fn detects_jpeg() {
        let bytes = &[0xFF, 0xD8, 0xFF, 0x00];
        assert_eq!(detect_image_mime(bytes), Some("image/jpeg"));
    }

    #[test]
    fn detects_png() {
        let bytes = &[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00];
        assert_eq!(detect_image_mime(bytes), Some("image/png"));
    }

    #[test]
    fn detects_gif() {
        let bytes = &[0x47, 0x49, 0x46, 0x38, 0x39, 0x61]; // GIF89a
        assert_eq!(detect_image_mime(bytes), Some("image/gif"));
    }

    #[test]
    fn detects_gif87a() {
        let bytes = &[0x47, 0x49, 0x46, 0x38, 0x37, 0x61]; // GIF87a
        assert_eq!(detect_image_mime(bytes), Some("image/gif"));
    }

    #[test]
    fn detects_webp() {
        // RIFF (4) + size (4) + WEBP (4)
        let mut bytes = vec![0x52, 0x49, 0x46, 0x46]; // "RIFF"
        bytes.extend_from_slice(&0u32.to_le_bytes()); // file size - 8
        bytes.extend_from_slice(b"WEBP"); // "WEBP"
        assert_eq!(detect_image_mime(&bytes), Some("image/webp"));
    }

    #[test]
    fn detects_tiff_le() {
        let bytes = &[0x49, 0x49, 0x2A, 0x00]; // little-endian TIFF
        assert_eq!(detect_image_mime(bytes), Some("image/tiff"));
    }

    #[test]
    fn detects_tiff_be() {
        let bytes = &[0x4D, 0x4D, 0x00, 0x2A]; // big-endian TIFF
        assert_eq!(detect_image_mime(bytes), Some("image/tiff"));
    }

    #[test]
    fn returns_none_for_unknown_bytes() {
        assert_eq!(detect_image_mime(b"hello world"), None);
        assert_eq!(detect_image_mime(b""), None);
    }

    #[test]
    fn returns_none_for_too_short_input() {
        assert_eq!(detect_image_mime(&[0xFF]), None);
        assert_eq!(detect_image_mime(&[0x89]), None);
    }

    // ── validate_image ──────────────────────────────────────────────────────

    #[test]
    fn validates_known_image() {
        let jpeg = minimal_jpeg();
        let mime = validate_image(&jpeg, Some("photo.jpg")).unwrap();
        assert_eq!(mime, "image/jpeg");
    }

    #[test]
    fn validates_png() {
        let png = minimal_png();
        let mime = validate_image(&png, Some("icon.png")).unwrap();
        assert_eq!(mime, "image/png");
    }

    #[test]
    fn validate_rejects_non_image_bytes() {
        let err = validate_image(b"not an image", Some("readme.txt")).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("not a recognised image"),
            "error should mention unrecognised image, got: {msg}"
        );
        assert!(
            msg.contains("readme.txt"),
            "error should mention filename, got: {msg}"
        );
    }

    #[test]
    fn rejection_includes_hex_preview() {
        let err = validate_image(b"\x00\x01\x02\x03", Some("bad.bin")).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("00 01 02 03"),
            "error should include hex preview, got: {msg}"
        );
    }

    #[test]
    fn validate_handles_missing_path() {
        let err = validate_image(b"garbage", None).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("<unknown>"), "got: {msg}");
    }

    #[test]
    fn validate_handles_empty_bytes() {
        let err = validate_image(b"", Some("empty.jpg")).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("not a recognised image"), "got: {msg}");
    }

    // ── harvest_exif ────────────────────────────────────────────────────────

    #[test]
    fn returns_empty_object_for_no_exif() {
        let jpeg = minimal_jpeg();
        let meta = harvest_exif(&jpeg);
        assert_eq!(meta, serde_json::json!({}));
    }

    #[test]
    fn returns_empty_object_for_non_image() {
        let meta = harvest_exif(b"plain text");
        assert_eq!(meta, serde_json::json!({}));
    }

    #[test]
    fn returns_empty_object_for_empty_input() {
        let meta = harvest_exif(b"");
        assert_eq!(meta, serde_json::json!({}));
    }

    /// JPEG with an EXIF APP1 segment containing Make and Model tags.
    /// This builds a synthetic EXIF IFD manually inside an APP1 marker.
    ///
    /// Layout (offsets within the exif data block, which starts after the
    /// 2-byte APP1 length field in the JPEG stream):
    ///
    /// ```text
    /// Exif data block ("Exif\0\0" + TIFF header + IFD + data area):
    ///   0..6   "Exif\0\0"
    ///   6..8   Byte order (II = 0x4949)
    ///   8..10  TIFF magic 0x002A
    ///  10..14  Offset to IFD0 (= 8, → absolute exif-block position 6+8 = 14)
    ///  14..16  IFD0 entry count (= 2)
    ///  16..28  IFD entry 0 (12 bytes: tag, type, count, value/offset)
    ///  28..40  IFD entry 1 (12 bytes)
    ///  40..44  Next-IFD offset (= 0)
    ///  44..    String data area (Make\0 + Model\0)
    /// ```
    ///
    /// Entries reference the string data with a TIFF-relative offset (i.e.
    /// relative to byte 6 of the exif data block). That means:
    ///
    /// - Data-area start (exif-block position 44) → TIFF offset = 44 − 6 = **38**.
    /// - Model = Make offset + len(Make) + 1.
    fn exif_jpeg(make: &str, model: &str) -> Vec<u8> {
        // Build the TIFF header + IFD inside the APP1 segment.
        let exif_data = {
            let mut t = Vec::new();
            // EXIF identifier
            t.extend_from_slice(b"Exif\x00\x00"); // 0..6
            // TIFF header: II (little-endian), magic 0x002A, IFD0 offset = 8
            t.extend_from_slice(&[0x49, 0x49, 0x2A, 0x00, 0x08, 0x00, 0x00, 0x00]); // 6..14

            // IFD0 at exif-block position 14:
            //   count = 2 entries
            t.extend_from_slice(&2u16.to_le_bytes()); // 14..16

            // Entry 0: Make (tag 0x010F, type ASCII=2)
            let make_tag: u16 = 0x010F;
            let make_type: u16 = 2;
            let make_count: u32 = (make.len() + 1) as u32; // include NUL
            // String data starts at exif-block position 44 → TIFF offset = 38
            let make_offset: u32 = 38;
            t.extend_from_slice(&make_tag.to_le_bytes()); // 16..18
            t.extend_from_slice(&make_type.to_le_bytes()); // 18..20
            t.extend_from_slice(&make_count.to_le_bytes()); // 20..24
            t.extend_from_slice(&make_offset.to_le_bytes()); // 24..28

            // Entry 1: Model (tag 0x0110, type ASCII=2)
            let model_tag: u16 = 0x0110;
            let model_type: u16 = 2;
            let model_count: u32 = (model.len() + 1) as u32;
            // Model data right after Make (which occupies make_count bytes)
            let model_offset: u32 = make_offset + make_count;
            t.extend_from_slice(&model_tag.to_le_bytes()); // 28..30
            t.extend_from_slice(&model_type.to_le_bytes()); // 30..32
            t.extend_from_slice(&model_count.to_le_bytes()); // 32..36
            t.extend_from_slice(&model_offset.to_le_bytes()); // 36..40

            // Next IFD offset: 0 (no more IFDs)
            t.extend_from_slice(&0u32.to_le_bytes()); // 40..44

            // String data area (exif-block position 44 onward)
            t.extend_from_slice(make.as_bytes());
            t.push(0); // NUL terminator for Make
            t.extend_from_slice(model.as_bytes());
            t.push(0); // NUL terminator for Model

            t
        };

        // Build the APP1 segment: marker (0xFF 0xE1), length (2 bytes), data.
        let app1_len = (exif_data.len() + 2) as u16; // +2 for the length field itself
        let mut jpeg = vec![0xFF, 0xD8]; // SOI
        jpeg.push(0xFF);
        jpeg.push(0xE1); // APP1
        jpeg.extend_from_slice(&app1_len.to_be_bytes());
        jpeg.extend_from_slice(&exif_data);

        // Append the rest of a minimal JPEG (skipping its own SOI).
        let rest = minimal_jpeg();
        jpeg.extend_from_slice(&rest[2..]);
        jpeg
    }

    #[test]
    fn harvests_exif_make_and_model() {
        let jpeg = exif_jpeg("Canon", "Canon EOS R5");
        let meta = harvest_exif(&jpeg);

        // The meta keys are namespaced: "exif:Make" and "exif:Model"
        let make = meta.get("exif:Make").and_then(|v| v.as_str());
        let model = meta.get("exif:Model").and_then(|v| v.as_str());
        assert_eq!(make, Some("Canon"));
        assert_eq!(model, Some("Canon EOS R5"));
    }

    #[test]
    fn harvests_exif_fields_with_single_tag() {
        let jpeg = exif_jpeg("Nikon", "D850");
        let meta = harvest_exif(&jpeg);

        assert_eq!(
            meta.get("exif:Make").and_then(|v| v.as_str()),
            Some("Nikon")
        );
        assert_eq!(
            meta.get("exif:Model").and_then(|v| v.as_str()),
            Some("D850")
        );
        // Should only have the two tags we injected.
        let obj = meta.as_object().unwrap();
        assert_eq!(obj.len(), 2, "expected 2 EXIF fields, got: {obj:?}");
    }

    // ── ImageExtractor::extract ─────────────────────────────────────────────

    #[tokio::test]
    async fn extracts_jpeg_with_no_exif() {
        let ex = ImageExtractor;
        let raw = image_raw(&minimal_jpeg(), "image/jpeg", "photo.jpg");
        let out = ex.extract(&raw).await.unwrap();

        assert_eq!(out.text, "");
        assert_eq!(out.meta, serde_json::json!({}));
        assert_eq!(out.page_images.len(), 1);
        assert_eq!(out.page_images[0].mime, "image/jpeg");
        assert_eq!(out.page_images[0].data, raw.bytes);
    }

    #[tokio::test]
    async fn extracts_png() {
        let ex = ImageExtractor;
        let raw = image_raw(&minimal_png(), "image/png", "icon.png");
        let out = ex.extract(&raw).await.unwrap();

        assert_eq!(out.text, "");
        assert!(out.meta.as_object().map(|o| o.is_empty()).unwrap_or(false));
        assert_eq!(out.page_images.len(), 1);
        assert_eq!(out.page_images[0].mime, "image/png");
        assert_eq!(out.page_images[0].data.len(), raw.bytes.len());
    }

    #[tokio::test]
    async fn extracts_gif() {
        let ex = ImageExtractor;
        // Minimal valid GIF (GIF89a, 1×1 image, no LCT, single block)
        let gif: &[u8] = &[
            0x47, 0x49, 0x46, 0x38, 0x39, 0x61, // header
            0x01, 0x00, // width 1
            0x01, 0x00, // height 1
            0x00, // packed: no GCT, 1 bit color res
            0x00, // background color index
            0x00, // pixel aspect ratio
            0x2C, // image separator
            0x00, 0x00, // left
            0x00, 0x00, // top
            0x01, 0x00, // width 1
            0x01, 0x00, // height 1
            0x00, // packed
            0x02, // LZW min code size
            0x02, // block size
            0x4C, 0x01, // LZW encoded data
            0x00, // block terminator
            0x3B, // trailer
        ];
        let raw = image_raw(gif, "image/gif", "anim.gif");
        let out = ex.extract(&raw).await.unwrap();

        assert_eq!(out.text, "");
        assert_eq!(out.page_images.len(), 1);
        assert_eq!(out.page_images[0].mime, "image/gif");
    }

    #[tokio::test]
    async fn extracts_webp() {
        let ex = ImageExtractor;
        // Minimal RIFF/WEBP container (VP8X chunk is empty — just header).
        let mut webp = Vec::new();
        webp.extend_from_slice(b"RIFF");
        let riff_size: u32 = 4 + (4 + 4 + 10); // "WEBP" + "VP8X" chunk (4B tag + 4B size + 10B vp8x)
        webp.extend_from_slice(&(riff_size).to_le_bytes());
        webp.extend_from_slice(b"WEBP");
        // VP8X chunk (required for lossless/lossy; minimal valid header)
        webp.extend_from_slice(b"VP8X");
        webp.extend_from_slice(&10u32.to_le_bytes()); // chunk size
        webp.extend_from_slice(&[
            0x00, // flags (no alpha, no animation, no exif, no xmp, no icc)
            0x00, 0x00, 0x00, // reserved
            0x01, 0x00, 0x00, // canvas width 1 (24-bit LE + 1)
            0x01, 0x00, 0x00, // canvas height 1 (24-bit LE + 1)
        ]);
        let raw = image_raw(&webp, "image/webp", "frame.webp");
        let out = ex.extract(&raw).await.unwrap();

        assert_eq!(out.text, "");
        assert_eq!(out.page_images.len(), 1);
        assert_eq!(out.page_images[0].mime, "image/webp");
    }

    #[tokio::test]
    async fn extracts_tiff() {
        let ex = ImageExtractor;
        // Minimal TIFF (little-endian, one IFD entry, 1 strip, 1×1 gray)
        let mut tiff = Vec::new();
        tiff.extend_from_slice(&[0x49, 0x49, 0x2A, 0x00]); // II, magic 42
        tiff.extend_from_slice(&(8u32).to_le_bytes()); // offset to IFD0
        // IFD0: 1 entry
        tiff.extend_from_slice(&(1u16).to_le_bytes());
        // Tag 256 (ImageWidth) = SHORT, count 1, value 1
        tiff.extend_from_slice(&(256u16).to_le_bytes()); // ImageWidth
        tiff.extend_from_slice(&(3u16).to_le_bytes()); // SHORT
        tiff.extend_from_slice(&(1u32).to_le_bytes()); // count
        tiff.extend_from_slice(&(1u32).to_le_bytes()); // value
        // Next IFD: 0
        tiff.extend_from_slice(&(0u32).to_le_bytes());
        let raw = image_raw(&tiff, "image/tiff", "scan.tiff");
        let out = ex.extract(&raw).await.unwrap();

        assert_eq!(out.text, "");
        assert_eq!(out.page_images.len(), 1);
        assert_eq!(out.page_images[0].mime, "image/tiff");
    }

    #[tokio::test]
    async fn extracts_jpeg_with_exif() {
        let ex = ImageExtractor;
        let jpeg = exif_jpeg("Sony", "ILCE-7M4");
        let raw = image_raw(&jpeg, "image/jpeg", "sony.jpg");
        let out = ex.extract(&raw).await.unwrap();

        assert_eq!(out.text, "");
        assert_eq!(
            out.meta.get("exif:Make").and_then(|v| v.as_str()),
            Some("Sony")
        );
        assert_eq!(
            out.meta.get("exif:Model").and_then(|v| v.as_str()),
            Some("ILCE-7M4")
        );
        assert_eq!(out.page_images.len(), 1);
        assert_eq!(out.page_images[0].mime, "image/jpeg");
        assert_eq!(out.page_images[0].data.len(), raw.bytes.len());
    }

    #[tokio::test]
    async fn page_image_carries_original_bytes() {
        let ex = ImageExtractor;
        let jpeg = minimal_jpeg();
        let raw = image_raw(&jpeg, "image/jpeg", "photo.jpg");
        let out = ex.extract(&raw).await.unwrap();

        let pi = &out.page_images[0];
        // The page image bytes should be identical to the original input.
        assert_eq!(pi.data, raw.bytes);
        assert_eq!(pi.mime, "image/jpeg");
    }

    #[tokio::test]
    async fn text_is_always_empty() {
        let ex = ImageExtractor;
        let jpeg = exif_jpeg("Fuji", "X-T5");
        let raw = image_raw(&jpeg, "image/jpeg", "fuji.jpg");
        let out = ex.extract(&raw).await.unwrap();
        assert_eq!(out.text, "");
    }

    #[tokio::test]
    async fn rejects_non_image_bytes() {
        let ex = ImageExtractor;
        let raw = image_raw(
            b"this is a text file, not a jpeg",
            "text/plain",
            "readme.txt",
        );
        let err = ex.extract(&raw).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("not a recognised image"),
            "error should say not recognised, got: {msg}"
        );
        assert!(
            msg.contains("readme.txt"),
            "error should mention filename, got: {msg}"
        );
    }

    #[tokio::test]
    async fn rejects_empty_bytes() {
        let ex = ImageExtractor;
        let raw = image_raw(b"", "image/jpeg", "empty.jpg");
        let err = ex.extract(&raw).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("not a recognised image"),
            "error should reject empty input, got: {msg}"
        );
    }

    #[tokio::test]
    async fn error_includes_hex_preview_of_non_image() {
        let ex = ImageExtractor;
        let raw = image_raw(
            &[0xDE, 0xAD, 0xBE, 0xEF],
            "application/octet-stream",
            "evil.bin",
        );
        let err = ex.extract(&raw).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("DE AD BE EF"),
            "error should include hex preview, got: {msg}"
        );
    }

    #[tokio::test]
    async fn error_handles_missing_path() {
        let ex = ImageExtractor;
        let raw = RawFile {
            bytes: Bytes::from_static(b"not an image"),
            mime: None,
            kind: DocKind::Image,
            path: None,
        };
        let err = ex.extract(&raw).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("<unknown>"), "got: {msg}");
    }

    #[tokio::test]
    async fn handles_jpeg_mime_from_filekind_not_input_mime() {
        // Even if the input mime is wrong or missing, magic bytes determine the real MIME.
        let ex = ImageExtractor;
        let raw = RawFile {
            bytes: Bytes::copy_from_slice(&minimal_jpeg()),
            mime: None, // caller didn't set it
            kind: DocKind::Image,
            path: Some("unknown.file".into()),
        };
        let out = ex.extract(&raw).await.unwrap();
        // The page image carries the MIME from magic byte detection, not from the input.
        assert_eq!(out.page_images[0].mime, "image/jpeg");
    }

    // ── Integration-like: round-trip through Extractor trait ────────────────

    #[tokio::test]
    async fn dyn_extractor_trait_object() {
        let ex: Box<dyn Extractor> = Box::new(ImageExtractor);
        let raw = image_raw(&minimal_jpeg(), "image/jpeg", "photo.jpg");
        let out = ex.extract(&raw).await.unwrap();
        assert_eq!(out.page_images.len(), 1);
        assert_eq!(out.page_images[0].mime, "image/jpeg");
    }
}
