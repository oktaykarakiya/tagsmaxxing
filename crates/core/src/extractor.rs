// SPDX-License-Identifier: AGPL-3.0-or-later

//! The `Extractor` capability and its I/O types (plan §4, §7).

use async_trait::async_trait;
use bytes::Bytes;

use crate::kind::DocKind;

/// A raw input file handed to an [`Extractor`]: the bytes plus what detection already knows.
#[derive(Debug, Clone)]
pub struct RawFile {
    /// The file's raw bytes.
    pub bytes: Bytes,
    /// Detected MIME type, if any.
    pub mime: Option<String>,
    /// Detected kind (drives which extractor is selected).
    pub kind: DocKind,
    /// Original path/filename, if known.
    pub path: Option<String>,
}

/// A rendered page image as **encoded** bytes (e.g. PNG/JPEG) plus its MIME type.
///
/// This deliberately diverges from the plan §4 sketch (`Vec<image::DynamicImage>`): `kb-core`
/// is the I/O- and dependency-light contract, so it carries already-encoded image bytes —
/// exactly what a VLM HTTP API consumes (base64) — and leaves pixel decoding to `kb-extract`.
/// (Recorded in `BUILD_LEDGER.toml`, task `P0-T4`.)
#[derive(Debug, Clone, PartialEq)]
pub struct PageImage {
    /// Encoded image bytes.
    pub data: Bytes,
    /// MIME type of the encoded bytes (e.g. `image/png`).
    pub mime: String,
}

/// The product of extraction: text, deterministic metadata, and any page images.
#[derive(Debug, Clone, Default)]
pub struct Extracted {
    /// Extracted plain text (may be empty for pure-image inputs).
    pub text: String,
    /// Raw, namespaced metadata harvested deterministically (exif/ffprobe/doc props).
    pub meta: serde_json::Value,
    /// Page images to caption with the VLM (e.g. scanned PDF pages, photos).
    pub page_images: Vec<PageImage>,
}

/// Turns raw bytes of a given kind into text + metadata + page images.
///
/// Implementations live in `kb-extract` (native docs, Tika, ffmpeg/whisper, vision).
#[async_trait]
pub trait Extractor: Send + Sync {
    /// Extract text, metadata, and page images from a raw file.
    ///
    /// # Errors
    /// Returns an error if the bytes cannot be parsed/decoded for this kind.
    async fn extract(&self, file: &RawFile) -> anyhow::Result<Extracted>;
}
