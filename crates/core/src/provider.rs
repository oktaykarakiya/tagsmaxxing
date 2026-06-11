// SPDX-License-Identifier: AGPL-3.0-or-later

//! The `ProviderAdapter` capability and its wire DTOs (plan §26.1).

use std::fmt::Debug;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::extractor::PageImage;
use crate::macros::str_enum;
use crate::role::Role;

/// How to reach a provider, independent of the scheduler's capacity/leasing machinery.
///
/// This diverges from the plan §26.1 sketch, where `ProviderAdapter` took `&Backend`.
/// `Backend` is a `kb-scheduler` type that owns the capacity guard; depending on it from
/// `kb-core` would invert the dependency direction. The adapter only needs *where* and *how*
/// to call, so it takes this descriptor; the scheduler keeps owning capacity.
/// (Recorded in `BUILD_LEDGER.toml`, task `P0-T4`.)
#[derive(Debug, Clone, Default)]
pub struct ProviderConn {
    /// Base URL for HTTP providers (`None` for native-SDK endpoints).
    pub endpoint: Option<String>,
    /// Decrypted API key, if the provider requires one (never logged; plan §24).
    pub api_key: Option<String>,
    /// Provider's model id / local alias to invoke.
    pub model_id: String,
    /// Extra headers to send (e.g. `anthropic-version`).
    pub headers: Vec<(String, String)>,
}

str_enum! {
    /// The author of a chat message.
    pub enum ChatRole {
        /// System/developer instruction.
        System = "system",
        /// End-user content.
        User = "user",
        /// Prior model output.
        Assistant = "assistant",
    }
}

/// One message in a chat request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatMessage {
    /// Who authored the message.
    pub role: ChatRole,
    /// The message text.
    pub content: String,
}

/// A normalized chat-completion request.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChatReq {
    /// Ordered conversation messages.
    pub messages: Vec<ChatMessage>,
    /// Optional JSON Schema for grammar-constrained structured output (plan §9).
    pub json_schema: Option<serde_json::Value>,
    /// Name for the `response_format.json_schema.name` field in the OpenAI wire format.
    /// Required by the API when `json_schema` is set; defaults to "output" if omitted.
    pub json_schema_name: Option<String>,
    /// Optional cap on generated tokens.
    pub max_tokens: Option<u32>,
    /// Optional sampling temperature.
    pub temperature: Option<f32>,
    /// Page images for multimodal VLM calls. Skipped in serde — wire serialization
    /// is handled by the client when images are present (multimodal content format).
    #[serde(skip)]
    pub images: Vec<PageImage>,
}

/// Token counts for one call, normalized across providers so `usage_events`/cost work
/// uniformly (plan §26.1).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    /// Prompt (input) tokens.
    pub prompt_tokens: u32,
    /// Completion (output) tokens.
    pub completion_tokens: u32,
}

impl Usage {
    /// Total tokens for the call, saturating instead of overflowing.
    #[must_use]
    pub const fn total_tokens(&self) -> u32 {
        self.prompt_tokens.saturating_add(self.completion_tokens)
    }
}

/// A normalized chat-completion response.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChatResp {
    /// The generated text.
    pub text: String,
    /// Token usage for the call.
    pub usage: Usage,
}

/// A normalized embedding request.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EmbedReq {
    /// Texts to embed.
    pub texts: Vec<String>,
}

/// A normalized embedding response.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EmbedResp {
    /// One vector per input text, order-aligned.
    pub vectors: Vec<Vec<f32>>,
    /// Token usage for the call.
    pub usage: Usage,
}

/// Normalizes calls across provider families (OpenAI-compatible, Anthropic, Gemini, …). The
/// scheduler's acquire/lease/failover machinery is reused unchanged around it (plan §26.1).
#[async_trait]
pub trait ProviderAdapter: Debug + Send + Sync {
    /// Perform a chat completion against `conn`.
    ///
    /// # Errors
    /// Returns an error on transport failure, a non-success status, or a malformed response.
    async fn chat(&self, conn: &ProviderConn, req: ChatReq) -> anyhow::Result<ChatResp>;

    /// Produce embeddings against `conn`.
    ///
    /// # Errors
    /// Returns an error on transport failure, a non-success status, or a malformed response.
    async fn embed(&self, conn: &ProviderConn, req: EmbedReq) -> anyhow::Result<EmbedResp>;

    /// Whether this adapter's provider can serve `role` (capability gating; plan §26.6).
    fn supports(&self, role: Role) -> bool;
}

/// A no-op [`ProviderAdapter`] returning errors for every call.
///
/// Used as a **bootstrap placeholder** while the scheduler is being constructed
/// from configuration (before the real adapters are wired — P9-T6). All methods
/// return an error explaining that the backend is not yet fully wired.
///
/// For tests that only need a backend to exist (pool acquire, health, etc. —
/// no actual API call), this adapter is sufficient. For tests that exercise
/// real API calls, use a real adapter like [`OpenAiCompat`](kb_llm::OpenAiCompat).
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopAdapter;

#[async_trait]
impl ProviderAdapter for NoopAdapter {
    async fn chat(&self, _conn: &ProviderConn, _req: ChatReq) -> anyhow::Result<ChatResp> {
        anyhow::bail!("NoopAdapter::chat called — this backend was not fully wired (P9-T6)")
    }

    async fn embed(&self, _conn: &ProviderConn, _req: EmbedReq) -> anyhow::Result<EmbedResp> {
        anyhow::bail!("NoopAdapter::embed called — this backend was not fully wired (P9-T6)")
    }

    fn supports(&self, _role: Role) -> bool {
        // Permissive: actual gating is done by the Backend's CapSet field.
        true
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use std::str::FromStr;

    use super::*;

    #[test]
    fn total_tokens_sums() {
        let u = Usage {
            prompt_tokens: 30,
            completion_tokens: 12,
        };
        assert_eq!(u.total_tokens(), 42);
    }

    #[test]
    fn total_tokens_saturates() {
        let u = Usage {
            prompt_tokens: u32::MAX,
            completion_tokens: 1,
        };
        assert_eq!(u.total_tokens(), u32::MAX);
    }

    #[test]
    fn chat_role_wire_strings_are_locked() {
        assert_eq!(ChatRole::System.as_str(), "system");
        assert_eq!(ChatRole::User.as_str(), "user");
        assert_eq!(ChatRole::Assistant.as_str(), "assistant");
        for r in ChatRole::all() {
            assert_eq!(ChatRole::from_str(r.as_str()).unwrap(), *r);
        }
    }

    #[test]
    fn chat_req_images_defaults_empty() {
        let req = ChatReq::default();
        assert!(req.images.is_empty());
    }

    #[test]
    fn chat_req_carries_images() {
        let img = PageImage {
            data: bytes::Bytes::from_static(b"\xff\xd8\xff"),
            mime: "image/jpeg".into(),
        };
        let req = ChatReq {
            images: vec![img.clone()],
            ..Default::default()
        };
        assert_eq!(req.images.len(), 1);
        assert_eq!(req.images[0].data, img.data);
        assert_eq!(req.images[0].mime, "image/jpeg");
    }
}
