//! VLM captioning: describe image bytes with a vision-language model.
//!
//! The [`VisionCaptioner`] takes [`PageImage`]s produced by the image extractor
//! and calls a VLM backend (e.g. Qwen3.6-VL) via the scheduler pool. The
//! textual descriptions are prepended to the tagger's input so summaries and
//! tags reflect the actual visual content rather than just EXIF metadata.

use kb_core::extractor::PageImage;
use kb_core::provider::{ChatMessage, ChatReq};
use kb_core::role::Role;

use crate::client::LlamaClient;

/// Short, focused prompt that produces concise image descriptions for the
/// tagger to consume. Kept short to minimise VLM latency and token spend.
const VLM_PROMPT: &str = "Describe this image concisely in 1-2 sentences, \
    focusing on the main subject, setting, and any visible text.";

/// Calls a VLM (vision-language model) through the scheduler pool to
/// produce textual descriptions of encoded image bytes.
pub struct VisionCaptioner {
    client: LlamaClient,
    model: String,
}

impl VisionCaptioner {
    /// Create a captioner that calls `model` via the shared [`LlamaClient`].
    pub fn new(client: LlamaClient, model: String) -> Self {
        Self { client, model }
    }

    /// Describe a single page image, returning the VLM-generated caption.
    ///
    /// The image bytes are base64-encoded and sent as a multimodal content
    /// part alongside a fixed text prompt.
    ///
    /// # Errors
    ///
    /// Returns an error if the VLM call fails (network, overload, model
    /// error). Callers should treat this as best-effort and continue with
    /// whatever text content is already available.
    pub async fn describe_image(
        &self,
        image: &PageImage,
        local_only: bool,
    ) -> anyhow::Result<String> {
        let req = ChatReq {
            messages: vec![ChatMessage {
                role: kb_core::provider::ChatRole::User,
                content: VLM_PROMPT.to_string(),
            }],
            images: vec![image.clone()],
            ..Default::default()
        };

        let resp = self
            .client
            .chat(Role::Vision, &self.model, &req, local_only, 0)
            .await
            .map_err(|e| anyhow::anyhow!("VLM captioning failed: {e}"))?;

        Ok(resp.text)
    }

    /// Describe multiple page images.
    ///
    /// Calls the VLM once per image and joins the captions with newlines.
    /// For multi-page documents, each caption is prefixed with `[Page N]`
    /// so the tagger can associate descriptions with pages.
    ///
    /// # Errors
    ///
    /// Returns an error if any single VLM call fails. The returned error
    /// includes the page index that failed.
    pub async fn describe_many(
        &self,
        images: &[PageImage],
        local_only: bool,
    ) -> anyhow::Result<String> {
        let mut captions = Vec::with_capacity(images.len());
        for (i, image) in images.iter().enumerate() {
            let cap = self
                .describe_image(image, local_only)
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

    async fn captioner_with_vision_backend(
        caption: &str,
    ) -> (VisionCaptioner, MockBackend) {
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
        let client = LlamaClient::new(
            pool,
            Client::new(),
            2,
            3,
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

    #[tokio::test]
    async fn describe_image_success() {
        let expected = "A photo of a sunset over mountains.";
        let (captioner, mock) = captioner_with_vision_backend(expected).await;

        let caption = captioner.describe_image(&test_image(), false).await.unwrap();
        assert_eq!(caption, expected);

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn describe_many_labels_pages() {
        let expected = "A landscape photo.";
        let (captioner, mock) = captioner_with_vision_backend(expected).await;

        let images = vec![test_image(), test_image()];
        let result = captioner.describe_many(&images, false).await.unwrap();

        assert!(result.contains("[Page 1]"), "should label page 1: {result}");
        assert!(result.contains("[Page 2]"), "should label page 2: {result}");
        assert!(result.contains(expected));

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn describe_image_vlm_error() {
        let (captioner, mock) = captioner_with_vision_backend("unused").await;
        mock.scenario().lock().await.chat = ResponseMode::ServerError;

        let err = captioner.describe_image(&test_image(), false).await.unwrap_err();
        assert!(
            err.to_string().contains("VLM captioning failed"),
            "error should mention VLM captioning: {err}"
        );

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn describe_many_empty_on_all_empty_captions() {
        // Mock returns empty string.
        let (captioner, mock) = captioner_with_vision_backend("").await;

        let result = captioner.describe_many(&[test_image()], false).await.unwrap();
        assert!(result.is_empty(), "empty caption should be skipped");

        mock.shutdown().await;
    }
}
