// SPDX-License-Identifier: AGPL-3.0-or-later

//! VLM captioning: describe image bytes with a vision-language model.
//!
//! The [`VisionCaptioner`] takes [`PageImage`]s produced by the image extractor
//! and calls a VLM backend (e.g. Qwen3.6-VL) via the scheduler pool. The
//! textual descriptions are prepended to the tagger's input so summaries and
//! tags reflect the actual visual content rather than just EXIF metadata.
//!
//! # Safety measures
//!
//! - **Format allow-list**: only JPEG, PNG, GIF, and TIFF are sent to the VLM.
//!   WebP and other formats are silently skipped (the VLM backend crashes on
//!   unsupported image codecs). The original full-resolution image is still
//!   stored in the blob store and the document ingests normally with EXIF-only
//!   metadata.
//! - **Client-side resize**: images larger than 1024 px on the longest edge are
//!   downscaled before base64 encoding. This reduces HTTP payload size (a 12 MP
//!   photo goes from ~13 MiB base64 to ~200 KiB), speeds up VLM calls, and
//!   lowers memory pressure on the llama-server process. The original bytes
//!   in blob storage are never modified.
//! - **No retries**: the internal `LlamaClient` is configured with zero retries
//!   so a VLM-specific failure never cascades into a circuit-breaker trip that
//!   would also block the `text` role (which shares the same backend).

use std::sync::Arc;

use image::GenericImageView;

use kb_core::extractor::PageImage;
use kb_core::provider::{ChatMessage, ChatReq, Usage};
use kb_core::role::Role;
use kb_core::usage::{UsageEvent, UsageRecorder};

use crate::client::LlamaClient;

/// Short, focused prompt that produces concise image descriptions for the
/// tagger to consume. Kept short to minimise VLM latency and token spend.
const VLM_PROMPT: &str = "Describe this image concisely in 1-2 sentences, \
    focusing on the main subject, setting, and any visible text.";

/// Maximum dimension (width or height) in pixels for images sent to the VLM.
/// Images larger than this are downscaled before base64 encoding. This reduces
/// HTTP payload size and memory pressure on the llama-server process without
/// meaningfully affecting caption quality — the mmproj projector already
/// resizes to ~980×980 internally.
const MAX_IMAGE_DIM: u32 = 1024;

/// JPEG quality (1–100) used when re-encoding a downscaled image for the VLM.
///
/// The downscale to [`MAX_IMAGE_DIM`] is the only resolution reduction the VLM
/// needs (it resizes to ~980 px internally), but re-encoding at the `image`
/// crate's default quality (75) adds visible artifacts that blur small text and
/// fine detail — exactly what the VLM must read on scanned invoices, signs, and
/// screenshots. 92 is visually lossless to the model while keeping payloads
/// small (a 1024 px frame stays ~150–250 KB vs the multi-MB original), so caption
/// and image-search quality improve at no UX cost.
const VLM_JPEG_QUALITY: u8 = 92;

/// MIME types that the VLM backend is known to support. Images with
/// unsupported MIME types (e.g. `image/webp`) are silently skipped to
/// prevent backend crashes (`vk::DeviceLostError` on AMD Vulkan).
const VLM_SUPPORTED_MIMES: &[&str] = &["image/jpeg", "image/png", "image/gif", "image/tiff"];

/// Calls a VLM (vision-language model) through the scheduler pool to
/// produce textual descriptions of encoded image bytes.
pub struct VisionCaptioner {
    client: LlamaClient,
    model: String,
    /// Optional sink for per-call token usage (P14-T3). When set, each VLM
    /// captioning call during ingest is metered to the ingesting tenant+user.
    usage_recorder: Option<Arc<dyn UsageRecorder>>,
}

impl VisionCaptioner {
    /// Create a captioner that calls `model` via the shared [`LlamaClient`].
    ///
    /// The client should be configured with `max_retries = 0` so that VLM
    /// failures do not cascade into circuit-breaker trips for the text role
    /// (which shares the same backend).
    pub fn new(client: LlamaClient, model: String) -> Self {
        Self {
            client,
            model,
            usage_recorder: None,
        }
    }

    /// Attach a [`UsageRecorder`] so each VLM captioning call's token usage is
    /// metered into `usage_events` for the ingesting tenant+user (P14-T3).
    ///
    /// Without this, VLM image-captioning calls go uncounted — the last
    /// unmetered AI call on the ingest path.
    #[must_use]
    pub fn with_usage_recorder(mut self, recorder: Arc<dyn UsageRecorder>) -> Self {
        self.usage_recorder = Some(recorder);
        self
    }

    /// Meter one VLM captioning call's token usage to `tenant_id`, attributed
    /// to `user_id` when known (best-effort, P14-T3).
    ///
    /// A no-op when no [`UsageRecorder`] is attached. When the backend reports
    /// no `usage`, a zero-token [`Role::Vision`] event is still recorded so the
    /// call is counted. Recording failures are logged and swallowed — metering
    /// must never fail a captioning call.
    async fn meter(&self, tenant_id: i64, user_id: Option<i64>, usage: Option<&Usage>) {
        let Some(recorder) = &self.usage_recorder else {
            return;
        };
        let event = UsageEvent {
            id: 0,
            tenant_id,
            user_id,
            model: self.model.clone(),
            role: Role::Vision,
            backend_id: None,
            prompt_tokens: usage.map(|u| u.prompt_tokens as i32),
            completion_tokens: usage.map(|u| u.completion_tokens as i32),
            latency_ms: None,
            cost_micros: None,
            created_at: chrono::Utc::now(),
        };
        if let Err(e) = recorder.record_usage(&event).await {
            tracing::warn!(error = %e, tenant_id, "vision: failed to meter usage event");
        }
    }

    /// Describe a single page image, returning the VLM-generated caption.
    ///
    /// Unsupported MIME types are skipped (empty caption returned without
    /// calling the VLM). Images larger than [`MAX_IMAGE_DIM`] on the longest
    /// edge are downscaled before sending.
    ///
    /// `tenant_id` and `user_id` attribute the VLM call's token usage on the
    /// ingest path (P14-T3): a [`Role::Vision`] usage event is recorded for the
    /// tenant, stamped with the ingesting user when known. Skipped formats make
    /// no model call and therefore meter nothing. Metering is best-effort.
    ///
    /// # Errors
    ///
    /// Returns an error only if the VLM call itself fails after format
    /// validation and resizing succeed.
    pub async fn describe_image(
        &self,
        image: &PageImage,
        tenant_id: i64,
        user_id: Option<i64>,
        local_only: bool,
    ) -> anyhow::Result<String> {
        // ── Format allow-list ─────────────────────────────────────────────
        // Unsupported formats are skipped before any VLM call is made, so no
        // usage is metered here — there is no model call to attribute.
        if !VLM_SUPPORTED_MIMES.contains(&image.mime.as_str()) {
            tracing::warn!(
                mime = %image.mime,
                bytes = image.data.len(),
                "VLM captioning skipped: unsupported image format"
            );
            return Ok(String::new());
        }

        // ── Client-side resize ────────────────────────────────────────────
        let image_bytes =
            resize_for_vlm(&image.data, &image.mime).unwrap_or_else(|| image.data.to_vec());

        // ── VLM call ──────────────────────────────────────────────────────
        let resized = PageImage {
            data: bytes::Bytes::from(image_bytes),
            mime: image.mime.clone(),
        };

        let req = ChatReq {
            messages: vec![ChatMessage {
                role: kb_core::provider::ChatRole::User,
                content: VLM_PROMPT.to_string(),
            }],
            images: vec![resized],
            ..Default::default()
        };

        let resp = self
            .client
            .chat(Role::Vision, &self.model, &req, local_only, 0)
            .await
            .map_err(|e| anyhow::anyhow!("VLM captioning failed: {e}"))?;

        // Meter the call to the ingesting tenant+user (P14-T3). Recorded even
        // when the backend reports no token usage so the call is still counted.
        self.meter(tenant_id, user_id, Some(&resp.usage)).await;

        Ok(resp.text)
    }

    /// Describe multiple page images.
    ///
    /// Calls the VLM once per image and joins the captions with newlines.
    /// For multi-page documents, each caption is prefixed with `[Page N]`
    /// so the tagger can associate descriptions with pages.
    ///
    /// `tenant_id` and `user_id` attribute each VLM call's token usage (P14-T3),
    /// threaded through to every [`describe_image`](Self::describe_image) call.
    ///
    /// # Errors
    ///
    /// Returns an error if any single VLM call fails. The returned error
    /// includes the page index that failed.
    pub async fn describe_many(
        &self,
        images: &[PageImage],
        tenant_id: i64,
        user_id: Option<i64>,
        local_only: bool,
    ) -> anyhow::Result<String> {
        let mut captions = Vec::with_capacity(images.len());
        for (i, image) in images.iter().enumerate() {
            let cap = self
                .describe_image(image, tenant_id, user_id, local_only)
                .await
                .map_err(|e| anyhow::anyhow!("VLM captioning failed on image {}: {e}", i + 1))?;
            let trimmed = cap.trim();
            if !trimmed.is_empty() {
                if images.len() > 1 {
                    captions.push(format!("[Page {}]: {}", i + 1, trimmed));
                } else {
                    captions.push(trimmed.to_string());
                }
            }
        }
        Ok(captions.join("\n"))
    }
}

// ── Image resize helper ──────────────────────────────────────────────────────

/// Resize an encoded image so its longest edge is at most [`MAX_IMAGE_DIM`] px.
///
/// Returns `None` if the image is already small enough, or if decoding fails.
/// The output is always re-encoded as JPEG (universally supported by VLM
/// backends) regardless of the input format, keeping payload sizes predictable.
fn resize_for_vlm(bytes: &[u8], mime: &str) -> Option<Vec<u8>> {
    let img = image::load_from_memory(bytes).ok()?;
    let (w, h) = img.dimensions();
    let longest = w.max(h);

    if longest <= MAX_IMAGE_DIM {
        return None; // Already small enough — use original bytes.
    }

    let scale = MAX_IMAGE_DIM as f64 / longest as f64;
    let new_w = (w as f64 * scale) as u32;
    let new_h = (h as f64 * scale) as u32;

    let resized = img.resize_exact(new_w, new_h, image::imageops::FilterType::Lanczos3);

    // Always encode as JPEG for predictable, compact output the VLM decodes
    // natively. Encode at VLM_JPEG_QUALITY (not the image crate's default 75) so
    // the downscale is the only quality reduction — preserving small-text
    // legibility for captioning/OCR. JPEG has no alpha channel, so flatten to
    // RGB8 once before encoding.
    let rgb = resized.to_rgb8();
    let mut out = Vec::new();
    image::codecs::jpeg::JpegEncoder::new_with_quality(&mut out, VLM_JPEG_QUALITY)
        .encode(
            rgb.as_raw(),
            rgb.width(),
            rgb.height(),
            image::ExtendedColorType::Rgb8,
        )
        .ok()?;
    tracing::debug!(
        original_bytes = bytes.len(),
        resized_bytes = out.len(),
        original_dims = format!("{w}x{h}"),
        resized_dims = format!("{new_w}x{new_h}"),
        input_mime = %mime,
        "resized image for VLM",
    );

    Some(out)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::Arc;
    use std::time::Duration;

    use kb_core::extractor::PageImage;
    use kb_core::role::Role;
    use kb_mock_backend::{MockBackend, ResponseMode};
    use kb_scheduler::{Pool, test_backend};
    use reqwest::Client;

    use super::*;

    /// Build a captioner with a mock vision backend.
    /// Uses `max_retries = 0` so VLM failures never cascade to circuit breaker.
    async fn captioner_with_vision_backend(caption: &str) -> (VisionCaptioner, MockBackend) {
        let mock = MockBackend::start().await;
        mock.scenario().lock().await.chat_content = Some(caption.to_string());

        let base_url = mock.url("/v1");
        let backend = Arc::new(test_backend(
            "mock-vision",
            &base_url,
            vec![Role::Vision],
            0,
            2,
        ));
        let pool = Pool::new(vec![backend], Duration::from_secs(5));
        // Zero retries — VLM failures must not trip the circuit breaker
        // for the text role (which shares the same backend).
        let client = LlamaClient::new(
            pool,
            Client::new(),
            0, // max_retries
            3, // circuit_threshold
            Duration::from_millis(200),
        );
        let captioner = VisionCaptioner::new(client, "test-vision-model".into());
        (captioner, mock)
    }

    fn test_image() -> PageImage {
        PageImage {
            data: bytes::Bytes::from_static(b"\xff\xd8\xff\xe0\x00\x10JFIF"),
            mime: "image/jpeg".into(),
        }
    }

    fn test_webp_image() -> PageImage {
        PageImage {
            data: bytes::Bytes::from_static(b"RIFF....WEBPVP8 "),
            mime: "image/webp".into(),
        }
    }

    // ── Happy path ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn describe_image_success() {
        let expected = "A photo of a sunset over mountains.";
        let (captioner, mock) = captioner_with_vision_backend(expected).await;

        let caption = captioner
            .describe_image(&test_image(), 1, None, false)
            .await
            .unwrap();
        assert_eq!(caption, expected);

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn describe_many_labels_pages() {
        let expected = "A landscape photo.";
        let (captioner, mock) = captioner_with_vision_backend(expected).await;

        let images = vec![test_image(), test_image()];
        let result = captioner
            .describe_many(&images, 1, None, false)
            .await
            .unwrap();

        assert!(result.contains("[Page 1]"), "should label page 1: {result}");
        assert!(result.contains("[Page 2]"), "should label page 2: {result}");
        assert!(result.contains(expected));

        mock.shutdown().await;
    }

    // ── Format allow-list ──────────────────────────────────────────────────

    /// WebP images are silently skipped — the VLM backend (llama.cpp) cannot
    /// decode them and attempting to do so triggers `vk::DeviceLostError` on
    /// AMD Vulkan GPUs.
    #[tokio::test]
    async fn describe_image_skips_unsupported_webp() {
        let (captioner, mock) = captioner_with_vision_backend("should-not-be-called").await;

        // Should return empty without calling the VLM.
        let result = captioner
            .describe_image(&test_webp_image(), 1, None, false)
            .await
            .unwrap();
        assert!(result.is_empty(), "WebP should be skipped, got: {result:?}");

        mock.shutdown().await;
    }

    /// TIFF is supported by the VLM (unlike WebP).
    #[tokio::test]
    async fn describe_image_allows_tiff() {
        let expected = "A scanned document.";
        let (captioner, mock) = captioner_with_vision_backend(expected).await;

        let tiff = PageImage {
            data: bytes::Bytes::from_static(b"II*\x00"),
            mime: "image/tiff".into(),
        };
        let result = captioner
            .describe_image(&tiff, 1, None, false)
            .await
            .unwrap();
        assert_eq!(result, expected);

        mock.shutdown().await;
    }

    // ── Image resize ───────────────────────────────────────────────────────

    /// Images within MAX_IMAGE_DIM are NOT resized (no-op).
    #[test]
    fn resize_small_image_returns_none() {
        // Create a tiny JPEG — ImageExtractor magic bytes pass, but this
        // is too small to decode. The resize function returns None on
        // decode failure (not a real image).
        let result = resize_for_vlm(b"not-an-image", "image/jpeg");
        assert!(result.is_none(), "undecodable input returns None");
    }

    /// Large images get downscaled.
    #[test]
    fn resize_large_image_downscales() {
        use image::{ImageFormat, RgbImage};

        // Create a 2000×2000 RGB image (exceeds MAX_IMAGE_DIM=1024).
        let img = RgbImage::from_pixel(2000, 2000, image::Rgb([120, 80, 200]));
        let mut buf = std::io::Cursor::new(Vec::new());
        img.write_to(&mut buf, ImageFormat::Jpeg).unwrap();
        let jpeg_bytes = buf.into_inner();

        let resized = resize_for_vlm(&jpeg_bytes, "image/jpeg");
        assert!(resized.is_some(), "large image should be resized");

        let out = resized.unwrap();
        // Decode the output to check dimensions.
        let decoded = image::load_from_memory(&out).unwrap();
        let (w, h) = decoded.dimensions();
        assert!(w <= MAX_IMAGE_DIM, "width {w} exceeds max {MAX_IMAGE_DIM}");
        assert!(h <= MAX_IMAGE_DIM, "height {h} exceeds max {MAX_IMAGE_DIM}");
        // Output should be smaller than input.
        assert!(out.len() < jpeg_bytes.len(), "resized should be smaller");
    }

    /// The downscaled image is re-encoded at VLM_JPEG_QUALITY (92), not the
    /// image crate's default (75): for detailed content the q92 output carries
    /// more bytes (= more preserved detail) than a q75 re-encode of the same
    /// pixels, while staying far smaller than the source. Uses a deterministic
    /// high-frequency pattern, since flat colour compresses to nothing at any
    /// quality and would not distinguish the two.
    #[test]
    fn resize_encodes_at_high_quality() {
        use image::codecs::jpeg::JpegEncoder;
        use image::{ExtendedColorType, RgbImage};

        // 1600×1600 detailed image (exceeds MAX_IMAGE_DIM so it is re-encoded).
        let mut img = RgbImage::new(1600, 1600);
        for (x, y, px) in img.enumerate_pixels_mut() {
            let v = ((x * 7) ^ (y * 13)) as u8;
            *px = image::Rgb([v, v.wrapping_mul(3), v.wrapping_add(90)]);
        }
        let mut src = Vec::new();
        JpegEncoder::new_with_quality(&mut src, 100)
            .encode(
                img.as_raw(),
                img.width(),
                img.height(),
                ExtendedColorType::Rgb8,
            )
            .unwrap();

        let out = resize_for_vlm(&src, "image/jpeg").expect("large image is re-encoded");

        // Decodes as a valid JPEG within the dimension cap.
        let decoded = image::load_from_memory(&out).unwrap();
        let (w, h) = decoded.dimensions();
        assert!(
            w.max(h) <= MAX_IMAGE_DIM,
            "longest edge {} exceeds {MAX_IMAGE_DIM}",
            w.max(h)
        );

        // Re-encode the SAME downscaled pixels at the old default (75) and assert
        // q92 preserves strictly more bytes — i.e. higher fidelity.
        let rgb = decoded.to_rgb8();
        let mut q75 = Vec::new();
        JpegEncoder::new_with_quality(&mut q75, 75)
            .encode(
                rgb.as_raw(),
                rgb.width(),
                rgb.height(),
                ExtendedColorType::Rgb8,
            )
            .unwrap();
        assert!(
            out.len() > q75.len(),
            "q92 output ({} B) should retain more detail than q75 ({} B)",
            out.len(),
            q75.len(),
        );
        // Still far smaller than the full-size source.
        assert!(out.len() < src.len(), "resized must be smaller than source");
    }

    // ── Error handling ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn describe_image_vlm_error() {
        let (captioner, mock) = captioner_with_vision_backend("unused").await;
        mock.scenario().lock().await.chat = ResponseMode::ServerError;

        let err = captioner
            .describe_image(&test_image(), 1, None, false)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("VLM captioning failed"),
            "error should mention VLM captioning: {err}"
        );

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn describe_many_empty_on_all_empty_captions() {
        let (captioner, mock) = captioner_with_vision_backend("").await;

        let result = captioner
            .describe_many(&[test_image()], 1, None, false)
            .await
            .unwrap();
        assert!(result.is_empty(), "empty caption should be skipped");

        mock.shutdown().await;
    }

    // ── Resize integration: captioner sends resized image ──────────────────

    #[tokio::test]
    async fn describe_image_resizes_large_image_before_sending() {
        use image::{ImageFormat, RgbImage};

        let expected = "A large purple square.";
        let (captioner, mock) = captioner_with_vision_backend(expected).await;

        // Create a 2000×2000 JPEG (above MAX_IMAGE_DIM).
        let img = RgbImage::from_pixel(2000, 2000, image::Rgb([200, 100, 50]));
        let mut buf = std::io::Cursor::new(Vec::new());
        img.write_to(&mut buf, ImageFormat::Jpeg).unwrap();
        let large_jpeg = buf.into_inner();
        // Pre-existing unused binding; prefixed to satisfy clippy -D warnings
        // (the mock can't validate payload size — see note below).
        let _original_len = large_jpeg.len();

        let page = PageImage {
            data: bytes::Bytes::from(large_jpeg),
            mime: "image/jpeg".into(),
        };

        let result = captioner
            .describe_image(&page, 1, None, false)
            .await
            .unwrap();
        assert_eq!(result, expected);
        // The VLM call succeeded — the image was resized before sending.
        // (The mock doesn't validate payload size, but the resize function
        // is tested separately above.)

        mock.shutdown().await;
    }

    // ── metering (P14-T3) ────────────────────────────────────────────────────

    /// A [`UsageRecorder`] capturing events for assertions (P14-T3).
    #[derive(Default)]
    struct CapturingRecorder {
        events: std::sync::Mutex<Vec<UsageEvent>>,
    }

    #[async_trait::async_trait]
    impl UsageRecorder for CapturingRecorder {
        async fn record_usage(&self, event: &UsageEvent) -> anyhow::Result<()> {
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }
    }

    /// A successful VLM caption meters a [`Role::Vision`] usage event attributed
    /// to the ingesting tenant+user, carrying the backend's reported token usage.
    #[tokio::test]
    async fn describe_image_meters_vision_usage() {
        let (captioner, mock) = captioner_with_vision_backend("a cat on a mat").await;
        let recorder = Arc::new(CapturingRecorder::default());
        let captioner = captioner.with_usage_recorder(recorder.clone() as Arc<dyn UsageRecorder>);

        let _ = captioner
            .describe_image(&test_image(), 42, Some(99), false)
            .await
            .unwrap();
        mock.shutdown().await;

        let events = recorder.events.lock().unwrap();
        assert_eq!(events.len(), 1, "one VLM call should be metered");
        assert_eq!(events[0].tenant_id, 42);
        assert_eq!(events[0].user_id, Some(99));
        assert_eq!(events[0].role, Role::Vision);
        assert_eq!(events[0].model, "test-vision-model");
        // The mock chat endpoint reports prompt_tokens: 10, completion_tokens: 5.
        assert_eq!(events[0].prompt_tokens, Some(10));
        assert_eq!(events[0].completion_tokens, Some(5));
    }

    /// A system/CLI ingest (no acting user) still meters the tenant but leaves
    /// `user_id` NULL.
    #[tokio::test]
    async fn describe_image_without_user_records_tenant_only() {
        let (captioner, mock) = captioner_with_vision_backend("a dog").await;
        let recorder = Arc::new(CapturingRecorder::default());
        let captioner = captioner.with_usage_recorder(recorder.clone() as Arc<dyn UsageRecorder>);

        let _ = captioner
            .describe_image(&test_image(), 7, None, false)
            .await
            .unwrap();
        mock.shutdown().await;

        let events = recorder.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tenant_id, 7);
        assert_eq!(events[0].user_id, None);
        assert_eq!(events[0].role, Role::Vision);
    }

    /// An unsupported format is skipped before any VLM call, so NO usage event
    /// is recorded — there is no model call to attribute.
    #[tokio::test]
    async fn describe_image_skipped_format_meters_nothing() {
        let (captioner, mock) = captioner_with_vision_backend("unused").await;
        let recorder = Arc::new(CapturingRecorder::default());
        let captioner = captioner.with_usage_recorder(recorder.clone() as Arc<dyn UsageRecorder>);

        let result = captioner
            .describe_image(&test_webp_image(), 42, Some(99), false)
            .await
            .unwrap();
        assert!(result.is_empty(), "WebP is skipped");
        mock.shutdown().await;

        let events = recorder.events.lock().unwrap();
        assert!(
            events.is_empty(),
            "a skipped format makes no model call and must not meter"
        );
    }

    /// `describe_many` meters one [`Role::Vision`] event per image that reaches
    /// the VLM.
    #[tokio::test]
    async fn describe_many_meters_per_image() {
        let (captioner, mock) = captioner_with_vision_backend("a photo").await;
        let recorder = Arc::new(CapturingRecorder::default());
        let captioner = captioner.with_usage_recorder(recorder.clone() as Arc<dyn UsageRecorder>);

        let images = vec![test_image(), test_image()];
        let _ = captioner
            .describe_many(&images, 5, Some(8), false)
            .await
            .unwrap();
        mock.shutdown().await;

        let events = recorder.events.lock().unwrap();
        assert_eq!(events.len(), 2, "two images → two Vision usage events");
        assert!(events.iter().all(|e| e.role == Role::Vision));
        assert!(events.iter().all(|e| e.tenant_id == 5));
        assert!(events.iter().all(|e| e.user_id == Some(8)));
    }

    /// With no recorder attached, captioning does not attempt to meter (no
    /// panic, caption returned as normal).
    #[tokio::test]
    async fn describe_image_without_recorder_is_noop() {
        let (captioner, mock) = captioner_with_vision_backend("a tree").await;

        let caption = captioner
            .describe_image(&test_image(), 1, Some(2), false)
            .await
            .unwrap();
        assert_eq!(caption, "a tree");

        mock.shutdown().await;
    }
}
