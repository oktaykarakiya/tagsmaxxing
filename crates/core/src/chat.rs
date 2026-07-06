// SPDX-License-Identifier: AGPL-3.0-or-later

//! Domain types for multi-turn chat / conversations (P18).
//!
//! These mirror the `conversations` and `conversation_messages` tables.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A conversation — a persistent chat thread scoped to a tenant + user.
///
/// Each conversation has a title (auto-generated from the first message)
/// and a message count. Messages are stored in [`ChatMessage`] rows linked
/// via `conversation_id`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatConversation {
    /// Surrogate primary key (`conversations.id`).
    pub id: i64,
    /// Owning tenant.
    pub tenant_id: i64,
    /// The user who created this conversation.
    pub user_id: i64,
    /// Auto-generated title (truncated from the first user message).
    pub title: Option<String>,
    /// Model reference used for this conversation, e.g. `"local/qwen-35b"`.
    pub model_ref: String,
    /// Number of messages in this conversation.
    pub message_count: i32,
    /// Row creation time.
    pub created_at: DateTime<Utc>,
    /// Last activity time (updated on each new message).
    pub updated_at: DateTime<Utc>,
}

/// A single message in a conversation.
///
/// Each row is one turn: a user prompt, an assistant response, or an
/// invisible system prompt (for audit/debug). Messages are ordered by
/// `created_at` within a conversation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatMessage {
    /// Surrogate primary key (`conversation_messages.id`).
    pub id: i64,
    /// Owning tenant.
    pub tenant_id: i64,
    /// The conversation this message belongs to.
    pub conversation_id: i64,
    /// Message role: `"user"`, `"assistant"`, or `"system"`.
    pub role: String,
    /// The message content.
    pub content: String,
    /// Prompt tokens consumed for this turn (input side).
    pub tokens_in: Option<i32>,
    /// Completion tokens generated for this turn (output side).
    pub tokens_out: Option<i32>,
    /// RAG search results used as context for this message, stored as JSON
    /// for audit and future "show sources" features.
    pub search_results_json: Option<serde_json::Value>,
    /// Row creation time.
    pub created_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn conversation_serde_roundtrips() {
        let now = Utc::now();
        let conv = ChatConversation {
            id: 1,
            tenant_id: 7,
            user_id: 42,
            title: Some("Hello".into()),
            model_ref: "local/qwen-35b".into(),
            message_count: 3,
            created_at: now,
            updated_at: now,
        };
        let json = serde_json::to_string(&conv).unwrap();
        let back: ChatConversation = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, 1);
        assert_eq!(back.title.as_deref(), Some("Hello"));
        assert_eq!(back.model_ref, "local/qwen-35b");
    }

    #[test]
    fn message_serde_roundtrips() {
        let msg = ChatMessage {
            id: 10,
            tenant_id: 7,
            conversation_id: 1,
            role: "user".into(),
            content: "What is Rust?".into(),
            tokens_in: Some(50),
            tokens_out: None,
            search_results_json: Some(serde_json::json!([{"title": "Rust Guide"}])),
            created_at: Utc::now(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: ChatMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(back.role, "user");
        assert_eq!(back.content, "What is Rust?");
        assert!(back.search_results_json.is_some());
    }

    #[test]
    fn message_role_is_freeform() {
        // The CHECK constraint lives in the DB; the domain type allows any
        // string for flexibility in tests.
        let msg = ChatMessage {
            id: 1,
            tenant_id: 1,
            conversation_id: 1,
            role: "assistant".into(),
            content: "…".into(),
            tokens_in: None,
            tokens_out: Some(200),
            search_results_json: None,
            created_at: Utc::now(),
        };
        assert_eq!(msg.role, "assistant");
    }
}
