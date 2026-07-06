// SPDX-License-Identifier: AGPL-3.0-or-later

//! Chat engine — LLM + RAG + history assembly for multi-turn chat (P18).
//!
//! Wraps the `RetrievalPipeline` for RAG context and the scheduler `Pool`
//! for LLM calls, assembling the full system prompt + history + context
//! into a single `ChatReq` for each turn.

use std::sync::Arc;

use kb_core::provider::{ChatMessage, ChatReq, ChatRole, Usage};
use kb_core::query::{Hit, Query, QueryFilters};
use kb_llm::LlamaClient;
use kb_store::PgStore;

use crate::retrieval::RetrievalPipeline;

/// The result of one chat turn.
#[derive(Debug, Clone)]
pub struct ChatResponse {
    /// The assistant's response text.
    pub text: String,
    /// RAG search results used as context.
    pub search_results: Vec<Hit>,
    /// Token usage for this turn.
    pub usage: Usage,
}

/// System prompt for the knowledge-base assistant.
const SYSTEM_PROMPT: &str = "You are a helpful knowledge-base assistant. You have access \
to the user's document library stored in their personal knowledge base. When answering, \
use the provided context if relevant. If the context does not contain relevant information, \
say so and answer based on your general knowledge. Be concise and accurate.";

/// Assembles and executes one chat turn: RAG retrieval + history + LLM call.
pub struct ChatEngine {
    client: Arc<LlamaClient>,
    retrieval: Arc<RetrievalPipeline>,
    pg: Arc<PgStore>,
    model: String,
    max_rag_docs: usize,
    max_history_messages: usize,
}

impl ChatEngine {
    /// Create a new chat engine.
    pub fn new(
        client: Arc<LlamaClient>,
        retrieval: Arc<RetrievalPipeline>,
        pg: Arc<PgStore>,
        model: String,
        max_rag_docs: usize,
        max_history_messages: usize,
    ) -> Self {
        Self {
            client,
            retrieval,
            pg,
            model,
            max_rag_docs,
            max_history_messages,
        }
    }

    /// Process one user message and return the assistant's response.
    ///
    /// 1. Embeds the user query and runs hybrid search for RAG context.
    /// 2. Loads recent conversation history from the DB.
    /// 3. Assembles the ChatReq with system prompt + RAG docs + history.
    /// 4. Calls the LLM via the LlamaClient.
    /// 5. Returns the response text, search results, and token usage.
    pub async fn respond(
        &self,
        tenant_id: i64,
        user_id: i64,
        conv_id: i64,
        user_message: &str,
        local_only: bool,
    ) -> anyhow::Result<ChatResponse> {
        // 1. RAG retrieval: embed user query → hybrid search → rerank.
        let query = Query {
            text: user_message.to_string(),
            filters: QueryFilters {
                kinds: vec![],
                tags: vec![],
                created_after: None,
                created_before: None,
            },
            top_k: self.max_rag_docs,
        };
        let search_results: Vec<Hit> = self
            .retrieval
            .retrieve(tenant_id, None, &query, local_only, false)
            .await
            .map(|(hits, _mode)| hits)
            .unwrap_or_default();

        // 2. Recent history.
        let history = self
            .pg
            .get_recent_messages(
                tenant_id,
                user_id,
                conv_id,
                self.max_history_messages as i64,
            )
            .await
            .unwrap_or_default();

        // 3. Build messages.
        let mut messages = Vec::new();

        // System prompt + RAG context.
        let system_content = if search_results.is_empty() {
            SYSTEM_PROMPT.to_string()
        } else {
            let docs: Vec<String> = search_results
                .iter()
                .map(|h| {
                    let title = h.title.as_deref().unwrap_or("Untitled");
                    format!("- {title}: {}", h.snippet)
                })
                .collect();
            format!(
                "{}\n\nRELEVANT DOCUMENTS:\n{}",
                SYSTEM_PROMPT,
                docs.join("\n")
            )
        };
        messages.push(ChatMessage {
            role: ChatRole::System,
            content: system_content,
            tool_calls: None,
            tool_call_id: None,
        });

        // History.
        for msg in &history {
            let role = match msg.role.as_str() {
                "user" => ChatRole::User,
                "assistant" => ChatRole::Assistant,
                _ => continue, // skip system messages in history
            };
            messages.push(ChatMessage {
                role,
                content: msg.content.clone(),
                tool_calls: None,
                tool_call_id: None,
            });
        }

        // Current user message.
        messages.push(ChatMessage {
            role: ChatRole::User,
            content: user_message.to_string(),
            tool_calls: None,
            tool_call_id: None,
        });

        let req = ChatReq {
            messages,
            max_tokens: Some(1024),
            ..Default::default()
        };

        // 4. LLM call via LlamaClient (failover, circuit-breaker, usage metering).
        let resp = self
            .client
            .chat(kb_core::role::Role::Text, &self.model, &req, local_only, 0)
            .await
            .map_err(|e| anyhow::anyhow!("chat LLM call failed: {e}"))?;

        Ok(ChatResponse {
            text: resp.text,
            search_results,
            usage: resp.usage,
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn system_prompt_is_non_empty() {
        assert!(!SYSTEM_PROMPT.is_empty());
    }

    #[test]
    fn chat_response_cloneable() {
        let resp = ChatResponse {
            text: "hello".into(),
            search_results: vec![],
            usage: Usage::default(),
        };
        let cloned = resp.clone();
        assert_eq!(cloned.text, "hello");
    }
}
