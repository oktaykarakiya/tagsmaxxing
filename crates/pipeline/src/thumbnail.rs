// SPDX-License-Identifier: AGPL-3.0-or-later

//! Ingest-time thumbnail generation (plan §20, P8-T13).
//!
//! Generates small derived preview/thumbnail blobs from file bytes at ingest
//! time so the browse/grid/search UI serves tiny cached thumbnails and never
//! pulls the full original from B2.
//!
//! - **Images** (JPEG/PNG/GIF/WebP/TIFF): decoded, downscaled so the longest
//!   side is at most `max_dimension` pixels, and re-encoded as JPEG.
//! - **Video**: a single keyframe is extracted via ffmpeg (piped).
//! - **PDF / documents**: not yet supported (needs an external renderer).
//!
//! Thumbnails are stored as separate content-addressed blobs
//! (`{tenant_id}/thumb/{sha256_hex}`) and inherit the same encryption (P8-T5)
//! and local-cache (P8-T2) layers as the original blob.

use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use bytes::Bytes;
use sha2::{Digest, Sha256};

// ── ThumbnailConfig ─────────────────────────────────────────────────────────────

/// Configuration for thumbnail generation (from [`kb_config::Thumbnail`]).
///
/// This is a lightweight snapshot suitable for passing into the pipeline; the
/// upstream config is hot-swappable.
#[derive(Debug, Clone)]
pub struct ThumbnailConfig {
    /// Whether thumbnail generation is enabled.
    pub enabled: bool,
    /// Maximum dimension (width or height) in pixels.
    pub max_dimension: u32,
    /// JPEG quality 1–100.
    pub jpeg_quality: u8,
}

impl Default for ThumbnailConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_dimension: 256,
            jpeg_quality: 70,
        }
    }
}

impl From<&kb_config::Thumbnail> for ThumbnailConfig {
    fn from(cfg: &kb_config::Thumbnail) -> Self {
        Self {
            enabled: cfg.enabled,
            max_dimension: cfg.max_dimension,
            jpeg_quality: cfg.jpeg_quality,
        }
    }
}

// ── ThumbnailOutput ────────────────────────────────────────────────────────────

/// Result of generating a thumbnail for a single file.
#[derive(Debug, Clone)]
pub struct ThumbnailOutput {
    /// The thumbnail image bytes (JPEG).
    pub bytes: Bytes,
    /// MIME type — always `"image/jpeg"`.
    pub mime: String,
    /// Width in pixels.
    pub width: u32,
    /// Height in pixels.
    pub height: u32,
    /// Content-addressed blob key: `{tenant_id}/thumb/{sha256_hex}`.
    pub blob_key: String,
}

impl ThumbnailOutput {
    /// Compute the SHA-256 of the thumbnail bytes and build the blob key.
    fn from_bytes(bytes: Bytes, width: u32, height: u32, tenant_id: i64) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let digest = hasher.finalize();
        let blob_key = format!("{}/thumb/{}", tenant_id, hex::encode(digest));
        Self {
            bytes,
            mime: "image/jpeg".to_string(),
            width,
            height,
            blob_key,
        }
    }
}

// ── VideoThumbnailer trait ─────────────────────────────────────────────────────

/// Abstraction for extracting a keyframe from video bytes.
///
/// The real implementation shells out to ffmpeg; tests use a mock.
#[async_trait]
pub trait VideoThumbnailer: Send + Sync {
    /// Extract a single keyframe JPEG from the given video bytes.
    ///
    /// Returns `None` if no frame could be extracted (e.g. video is too short
    /// or ffmpeg is not available).
    ///
    /// # Errors
    /// Returns an error if the subprocess fails unexpectedly.
    async fn extract_keyframe(&self, video_bytes: &[u8]) -> anyhow::Result<Option<Bytes>>;
}

/// Real ffmpeg-based video thumbnailer.
///
/// Shells out to `ffmpeg`, piping the input bytes via stdin and capturing a
/// single JPEG keyframe from stdout. A short timeout prevents stalled
/// subprocesses from blocking ingest.
pub struct FfmpegThumbnailer {
    /// Maximum duration to wait for ffmpeg (seconds).
    timeout_secs: u64,
}

impl FfmpegThumbnailer {
    /// Create a new ffmpeg thumbnailer with the given timeout.
    #[must_use]
    pub fn new(timeout_secs: u64) -> Self {
        Self { timeout_secs }
    }
}

#[async_trait]
impl VideoThumbnailer for FfmpegThumbnailer {
    async fn extract_keyframe(&self, video_bytes: &[u8]) -> anyhow::Result<Option<Bytes>> {
        use tokio::io::AsyncWriteExt;
        use tokio::process::Command;

        let mut child = Command::new("ffmpeg")
            .args([
                "-i",
                "pipe:0", // read from stdin
                "-ss",
                "00:00:05", // seek to 5 seconds (skip titles)
                "-vframes",
                "1", // one frame only
                "-f",
                "image2pipe", // output raw image to stdout
                "-vcodec",
                "mjpeg", // JPEG format
                "-loglevel",
                "error", // suppress ffmpeg output
                "-",     // write to stdout
            ])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .context("failed to spawn ffmpeg for video thumbnail")?;

        // Write input bytes to ffmpeg's stdin, then close it.
        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(video_bytes)
                .await
                .context("failed to write video bytes to ffmpeg stdin")?;
            // Dropping stdin closes the pipe so ffmpeg knows input is complete.
        }

        // Read stdout with timeout.
        let output = tokio::time::timeout(
            Duration::from_secs(self.timeout_secs),
            child.wait_with_output(),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "ffmpeg thumbnail extraction timed out after {} seconds",
                self.timeout_secs
            )
        })?
        .context("ffmpeg failed during thumbnail extraction")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // If ffmpeg couldn't process the file (e.g. not a video), return
            // None rather than failing — thumbnail generation is best-effort.
            if stderr.contains("Invalid data") || stderr.contains("could not find codec") {
                return Ok(None);
            }
            anyhow::bail!("ffmpeg exited with {}: {}", output.status, stderr);
        }

        if output.stdout.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Bytes::from(output.stdout)))
        }
    }
}

// ── ThumbnailGenerator ─────────────────────────────────────────────────────────

/// Generates thumbnails from raw file bytes at ingest time.
///
/// Uses the `image` crate for image resizing and a [`VideoThumbnailer`] for
/// video keyframe extraction. PDF/document thumbnails are not yet supported.
pub struct ThumbnailGenerator {
    config: ThumbnailConfig,
    video_thumbnailer: Option<Box<dyn VideoThumbnailer>>,
}

impl ThumbnailGenerator {
    /// Create a new thumbnail generator.
    ///
    /// Pass `None` for `video_thumbnailer` to skip video thumbnail generation
    /// (e.g. in dev or when ffmpeg is not available).
    #[must_use]
    pub fn new(
        config: ThumbnailConfig,
        video_thumbnailer: Option<Box<dyn VideoThumbnailer>>,
    ) -> Self {
        Self {
            config,
            video_thumbnailer,
        }
    }

    /// Generate a thumbnail from raw file bytes, if applicable.
    ///
    /// Returns `None` when:
    /// - Thumbnail generation is disabled via config.
    /// - The MIME type is not an image or video format we can handle.
    /// - Processing fails (best-effort — failure is logged but does not fail
    ///   the ingest).
    ///
    /// # Errors
    /// Only returns an error for truly unexpected failures (e.g. ffmpeg crash).
    /// Image decoding/encoding failures return `Ok(None)`.
    pub async fn generate(
        &self,
        bytes: &[u8],
        mime: &str,
        tenant_id: i64,
    ) -> anyhow::Result<Option<ThumbnailOutput>> {
        if !self.config.enabled {
            return Ok(None);
        }

        if is_image_mime(mime) {
            Ok(generate_image_thumbnail(bytes, &self.config, tenant_id))
        } else if is_video_mime(mime) {
            self.generate_video_thumbnail(bytes, tenant_id).await
        } else {
            // PDFs, documents, audio, etc. — no thumbnail for now.
            Ok(None)
        }
    }

    async fn generate_video_thumbnail(
        &self,
        bytes: &[u8],
        tenant_id: i64,
    ) -> anyhow::Result<Option<ThumbnailOutput>> {
        let thumbnailer = match &self.video_thumbnailer {
            Some(t) => t,
            None => return Ok(None),
        };

        let frame = match thumbnailer.extract_keyframe(bytes).await? {
            Some(f) => f,
            None => return Ok(None),
        };

        // The extracted keyframe may already be a small JPEG, but resize it
        // anyway to enforce the max dimension.
        Ok(generate_image_thumbnail(&frame, &self.config, tenant_id))
    }
}

// ── MIME detection helpers ─────────────────────────────────────────────────────

/// Returns `true` if `mime` is a browser-viewable image format we can thumbnail.
fn is_image_mime(mime: &str) -> bool {
    matches!(
        mime,
        "image/jpeg" | "image/png" | "image/gif" | "image/webp" | "image/tiff"
    )
}

/// Returns `true` if `mime` is a video format we can extract a keyframe from.
fn is_video_mime(mime: &str) -> bool {
    matches!(
        mime,
        "video/mp4"
            | "video/webm"
            | "video/quicktime"
            | "video/x-msvideo"
            | "video/x-matroska"
            | "video/mpeg"
            | "video/ogg"
    )
}

// ── Image thumbnail generation (pure function) ─────────────────────────────────

/// Generate a downscaled JPEG thumbnail from raw image bytes.
///
/// Pure function — decodes, resizes (maintaining aspect ratio, longest side ≤
/// `max_dimension`), and re-encodes as JPEG at the configured quality.
/// Returns `None` on decode/encode failures (best-effort).
fn generate_image_thumbnail(
    bytes: &[u8],
    config: &ThumbnailConfig,
    tenant_id: i64,
) -> Option<ThumbnailOutput> {
    use image::ImageReader;
    use image::imageops::FilterType;
    use std::io::Cursor;

    let img = ImageReader::new(Cursor::new(bytes))
        .with_guessed_format()
        .ok()?
        .decode()
        .ok()?;

    let (orig_w, orig_h) = (img.width(), img.height());
    let max_dim = config.max_dimension;

    // If the image is already within the max dimension, don't upscale.
    if orig_w <= max_dim && orig_h <= max_dim {
        let mut out = Cursor::new(Vec::new());
        img.write_to(&mut out, image::ImageFormat::Jpeg).ok()?;
        return Some(ThumbnailOutput::from_bytes(
            Bytes::from(out.into_inner()),
            orig_w,
            orig_h,
            tenant_id,
        ));
    }

    // Compute new dimensions preserving aspect ratio.
    let (new_w, new_h) = if orig_w >= orig_h {
        let w = max_dim;
        #[allow(clippy::cast_possible_truncation)]
        let h = ((orig_h as f64) * (max_dim as f64) / (orig_w as f64)).round() as u32;
        (w, h.max(1))
    } else {
        let h = max_dim;
        #[allow(clippy::cast_possible_truncation)]
        let w = ((orig_w as f64) * (max_dim as f64) / (orig_h as f64)).round() as u32;
        (w.max(1), h)
    };

    let resized = img.resize_exact(new_w, new_h, FilterType::Lanczos3);

    let mut out = Cursor::new(Vec::new());
    // JPEG does not support alpha — convert to RGB8 first.
    let rgb = resized.to_rgb8();
    let mut encoder =
        image::codecs::jpeg::JpegEncoder::new_with_quality(&mut out, config.jpeg_quality);
    encoder
        .encode(rgb.as_raw(), new_w, new_h, image::ExtendedColorType::Rgb8)
        .ok()?;

    Some(ThumbnailOutput::from_bytes(
        Bytes::from(out.into_inner()),
        new_w,
        new_h,
        tenant_id,
    ))
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // ── is_image_mime ─────────────────────────────────────────────────────

    #[test]
    fn image_mime_jpeg() {
        assert!(is_image_mime("image/jpeg"));
    }

    #[test]
    fn image_mime_png() {
        assert!(is_image_mime("image/png"));
    }

    #[test]
    fn image_mime_gif() {
        assert!(is_image_mime("image/gif"));
    }

    #[test]
    fn image_mime_webp() {
        assert!(is_image_mime("image/webp"));
    }

    #[test]
    fn image_mime_tiff() {
        assert!(is_image_mime("image/tiff"));
    }

    #[test]
    fn image_mime_rejects_non_image() {
        assert!(!is_image_mime("text/plain"));
        assert!(!is_image_mime("video/mp4"));
        assert!(!is_image_mime("application/pdf"));
        assert!(!is_image_mime("audio/mp3"));
    }

    // ── is_video_mime ─────────────────────────────────────────────────────

    #[test]
    fn video_mime_mp4() {
        assert!(is_video_mime("video/mp4"));
    }

    #[test]
    fn video_mime_webm() {
        assert!(is_video_mime("video/webm"));
    }

    #[test]
    fn video_mime_quicktime() {
        assert!(is_video_mime("video/quicktime"));
    }

    #[test]
    fn video_mime_rejects_non_video() {
        assert!(!is_video_mime("image/png"));
        assert!(!is_video_mime("text/html"));
        assert!(!is_video_mime("application/zip"));
    }

    // ── ThumbnailConfig ───────────────────────────────────────────────────

    #[test]
    fn thumbnail_config_defaults() {
        let cfg = ThumbnailConfig::default();
        assert!(cfg.enabled);
        assert_eq!(cfg.max_dimension, 256);
        assert_eq!(cfg.jpeg_quality, 70);
    }

    #[test]
    fn thumbnail_config_from_kb_config() {
        let kb_cfg = kb_config::Thumbnail::default();
        let cfg = ThumbnailConfig::from(&kb_cfg);
        assert!(cfg.enabled);
        assert_eq!(cfg.max_dimension, 256);
        assert_eq!(cfg.jpeg_quality, 70);
    }

    #[test]
    fn thumbnail_config_from_kb_config_disabled() {
        let kb_cfg = kb_config::Thumbnail {
            enabled: false,
            max_dimension: 128,
            jpeg_quality: 50,
        };
        let cfg = ThumbnailConfig::from(&kb_cfg);
        assert!(!cfg.enabled);
        assert_eq!(cfg.max_dimension, 128);
        assert_eq!(cfg.jpeg_quality, 50);
    }

    // ── ThumbnailOutput ───────────────────────────────────────────────────

    #[test]
    fn thumbnail_output_computes_key() {
        let bytes = Bytes::from_static(b"test thumbnail data");
        let output = ThumbnailOutput::from_bytes(bytes.clone(), 100, 50, 42);
        assert_eq!(output.mime, "image/jpeg");
        assert_eq!(output.width, 100);
        assert_eq!(output.height, 50);
        // Key: "42/thumb/" (9 chars) + 64 hex chars (SHA-256) = 73.
        assert!(output.blob_key.starts_with("42/thumb/"));
        assert_eq!(output.blob_key.len(), 73);
    }

    #[test]
    fn thumbnail_output_key_is_deterministic() {
        let bytes = Bytes::from_static(b"same data, same key");
        let a = ThumbnailOutput::from_bytes(bytes.clone(), 10, 10, 1);
        let b = ThumbnailOutput::from_bytes(bytes, 10, 10, 1);
        assert_eq!(a.blob_key, b.blob_key);
    }

    #[test]
    fn thumbnail_output_key_differs_by_tenant() {
        let bytes = Bytes::from_static(b"data");
        let a = ThumbnailOutput::from_bytes(bytes.clone(), 10, 10, 1);
        let b = ThumbnailOutput::from_bytes(bytes.clone(), 10, 10, 2);
        assert_ne!(a.blob_key, b.blob_key);
    }

    // ── generate_image_thumbnail ──────────────────────────────────────────

    #[test]
    fn generate_thumbnail_small_image_passthrough() {
        // Create a tiny 10x10 red PNG that fits within max_dimension=256.
        let mut png_bytes = Vec::new();
        {
            let img = image::DynamicImage::new_rgba8(10, 10);
            img.write_to(
                &mut std::io::Cursor::new(&mut png_bytes),
                image::ImageFormat::Png,
            )
            .unwrap();
        }
        let cfg = ThumbnailConfig::default();
        let output =
            generate_image_thumbnail(&png_bytes, &cfg, 1).expect("should produce thumbnail");
        assert_eq!(output.width, 10);
        assert_eq!(output.height, 10);
        assert_eq!(output.mime, "image/jpeg");
    }

    #[test]
    fn generate_thumbnail_large_image_downscales() {
        // Create a 512x256 green PNG — wider than max_dimension=256.
        let mut png_bytes = Vec::new();
        {
            let img = image::DynamicImage::new_rgba8(512, 256);
            img.write_to(
                &mut std::io::Cursor::new(&mut png_bytes),
                image::ImageFormat::Png,
            )
            .unwrap();
        }
        let cfg = ThumbnailConfig::default();
        let output =
            generate_image_thumbnail(&png_bytes, &cfg, 1).expect("should produce thumbnail");
        // With max_dimension=256, width should be 256 and height should be
        // proportional: 256 * 256/512 = 128.
        assert_eq!(output.width, 256);
        assert_eq!(output.height, 128);
        assert!(!output.bytes.is_empty());
    }

    #[test]
    fn generate_thumbnail_tall_image_downscales() {
        // 256x512 tall PNG.
        let mut png_bytes = Vec::new();
        {
            let img = image::DynamicImage::new_rgba8(256, 512);
            img.write_to(
                &mut std::io::Cursor::new(&mut png_bytes),
                image::ImageFormat::Png,
            )
            .unwrap();
        }
        let cfg = ThumbnailConfig::default();
        let output =
            generate_image_thumbnail(&png_bytes, &cfg, 1).expect("should produce thumbnail");
        assert_eq!(output.width, 128);
        assert_eq!(output.height, 256);
    }

    #[test]
    fn generate_thumbnail_square_image() {
        let mut png_bytes = Vec::new();
        {
            let img = image::DynamicImage::new_rgba8(1024, 1024);
            img.write_to(
                &mut std::io::Cursor::new(&mut png_bytes),
                image::ImageFormat::Png,
            )
            .unwrap();
        }
        let cfg = ThumbnailConfig::default();
        let output =
            generate_image_thumbnail(&png_bytes, &cfg, 1).expect("should produce thumbnail");
        assert_eq!(output.width, 256);
        assert_eq!(output.height, 256);
    }

    #[test]
    fn generate_thumbnail_invalid_bytes_returns_none() {
        let cfg = ThumbnailConfig::default();
        let result = generate_image_thumbnail(b"not an image at all", &cfg, 1);
        assert!(result.is_none());
    }

    #[test]
    fn generate_thumbnail_custom_max_dimension() {
        let mut png_bytes = Vec::new();
        {
            let img = image::DynamicImage::new_rgba8(800, 400);
            img.write_to(
                &mut std::io::Cursor::new(&mut png_bytes),
                image::ImageFormat::Png,
            )
            .unwrap();
        }
        let cfg = ThumbnailConfig {
            max_dimension: 128,
            ..Default::default()
        };
        let output =
            generate_image_thumbnail(&png_bytes, &cfg, 1).expect("should produce thumbnail");
        assert_eq!(output.width, 128);
        assert_eq!(output.height, 64);
    }

    #[test]
    fn generate_thumbnail_jpeg_input() {
        // Generate a JPEG and verify it can be re-thumbnailed.
        let mut jpg_bytes = Vec::new();
        {
            let img = image::DynamicImage::new_rgba8(300, 200);
            img.write_to(
                &mut std::io::Cursor::new(&mut jpg_bytes),
                image::ImageFormat::Jpeg,
            )
            .unwrap();
        }
        let cfg = ThumbnailConfig::default();
        let output =
            generate_image_thumbnail(&jpg_bytes, &cfg, 1).expect("should produce thumbnail");
        assert_eq!(output.width, 256);
        // 256 * 200/300 ≈ 170.666 → 171 rounded.
        assert_eq!(output.height, 171);
    }

    #[test]
    fn generate_thumbnail_very_small_max_dimension() {
        let mut png_bytes = Vec::new();
        {
            let img = image::DynamicImage::new_rgba8(100, 100);
            img.write_to(
                &mut std::io::Cursor::new(&mut png_bytes),
                image::ImageFormat::Png,
            )
            .unwrap();
        }
        let cfg = ThumbnailConfig {
            max_dimension: 50,
            ..Default::default()
        };
        let output =
            generate_image_thumbnail(&png_bytes, &cfg, 1).expect("should produce thumbnail");
        assert_eq!(output.width, 50);
        assert_eq!(output.height, 50);
    }

    // ── ThumbnailGenerator (disabled / mime routing) ────────────────────────

    #[tokio::test]
    async fn generator_disabled_returns_none() {
        let cfg = ThumbnailConfig {
            enabled: false,
            ..Default::default()
        };
        let tg = ThumbnailGenerator::new(cfg, None);
        let result = tg.generate(b"fake-image", "image/png", 1).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn generator_non_image_mime_returns_none() {
        let cfg = ThumbnailConfig::default();
        let tg = ThumbnailGenerator::new(cfg, None);
        let result = tg.generate(b"hello", "text/plain", 1).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn generator_video_no_thumbnailer_returns_none() {
        let cfg = ThumbnailConfig::default();
        let tg = ThumbnailGenerator::new(cfg, None);
        let result = tg.generate(b"fake-video", "video/mp4", 1).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn generator_image_produces_thumbnail() {
        let mut png_bytes = Vec::new();
        {
            let img = image::DynamicImage::new_rgba8(500, 300);
            img.write_to(
                &mut std::io::Cursor::new(&mut png_bytes),
                image::ImageFormat::Png,
            )
            .unwrap();
        }
        let cfg = ThumbnailConfig::default();
        let tg = ThumbnailGenerator::new(cfg, None);
        let output = tg
            .generate(&png_bytes, "image/png", 1)
            .await
            .unwrap()
            .expect("should produce thumbnail");
        assert_eq!(output.width, 256);
        assert_eq!(output.mime, "image/jpeg");
        assert!(output.blob_key.starts_with("1/thumb/"));
    }

    #[tokio::test]
    async fn generator_invalid_image_returns_none() {
        let cfg = ThumbnailConfig::default();
        let tg = ThumbnailGenerator::new(cfg, None);
        let result = tg.generate(b"garbage bytes", "image/png", 1).await.unwrap();
        assert!(result.is_none());
    }

    // ── Mock VideoThumbnailer ─────────────────────────────────────────────

    struct MockVideoThumbnailer {
        frame: Option<Bytes>,
    }

    #[async_trait]
    impl VideoThumbnailer for MockVideoThumbnailer {
        async fn extract_keyframe(&self, _video_bytes: &[u8]) -> anyhow::Result<Option<Bytes>> {
            Ok(self.frame.clone())
        }
    }

    #[tokio::test]
    async fn generator_video_with_mock_produces_thumbnail() {
        // Create a small frame image for the mock to return.
        let mut frame_bytes = Vec::new();
        {
            let img = image::DynamicImage::new_rgba8(1920, 1080);
            img.write_to(
                &mut std::io::Cursor::new(&mut frame_bytes),
                image::ImageFormat::Jpeg,
            )
            .unwrap();
        }

        let mock = MockVideoThumbnailer {
            frame: Some(Bytes::from(frame_bytes)),
        };
        let cfg = ThumbnailConfig::default();
        let tg = ThumbnailGenerator::new(cfg, Some(Box::new(mock)));

        let output = tg
            .generate(b"fake-video-bytes", "video/mp4", 1)
            .await
            .unwrap()
            .expect("should produce thumbnail");
        // 1920x1080 resized to max 256 → 256x144.
        assert_eq!(output.width, 256);
        assert_eq!(output.height, 144);
        assert!(output.blob_key.starts_with("1/thumb/"));
    }

    #[tokio::test]
    async fn generator_video_mock_returns_none_frame() {
        let mock = MockVideoThumbnailer { frame: None };
        let cfg = ThumbnailConfig::default();
        let tg = ThumbnailGenerator::new(cfg, Some(Box::new(mock)));

        let result = tg.generate(b"fake-video", "video/mp4", 1).await.unwrap();
        assert!(result.is_none());
    }

    // ── ThumbnailOutput blob_key identity ─────────────────────────────────

    #[test]
    fn thumbnail_output_same_bytes_same_key() {
        let data = Bytes::from_static(b"identical content");
        let a = ThumbnailOutput::from_bytes(data.clone(), 10, 10, 5);
        let b = ThumbnailOutput::from_bytes(data, 10, 10, 5);
        assert_eq!(a.blob_key, b.blob_key);
        assert_eq!(a.bytes, b.bytes);
    }

    #[test]
    fn thumbnail_output_different_bytes_different_key() {
        let a = ThumbnailOutput::from_bytes(Bytes::from_static(b"aaa"), 5, 5, 1);
        let b = ThumbnailOutput::from_bytes(Bytes::from_static(b"bbb"), 5, 5, 1);
        assert_ne!(a.blob_key, b.blob_key);
    }

    // ── FfmpegThumbnailer construction ────────────────────────────────────

    #[test]
    fn ffmpeg_thumbnailer_has_timeout() {
        let t = FfmpegThumbnailer::new(30);
        assert_eq!(t.timeout_secs, 30);
    }
}
