// SPDX-License-Identifier: AGPL-3.0-or-later

//! Tool execution loop — runs tool calls and feeds results back to the LLM (P20).

use kb_core::provider::{ChatMessage, ChatReq, ChatRole};
use kb_core::role::Role;
use kb_llm::LlamaClient;

use super::registry::{ToolContext, ToolRegistry};

/// Run a chat turn with tool support.
///
/// If the model returns `tool_calls`, executes them via the registry,
/// feeds results back to the conversation, and continues until the model
/// returns a text response or `max_rounds` is exhausted.
///
/// Returns the final text response and any tool calls made.
pub async fn run_with_tools(
    client: &LlamaClient,
    registry: &ToolRegistry,
    ctx: &ToolContext,
    model: &str,
    messages: &mut Vec<ChatMessage>,
    local_only: bool,
    max_rounds: usize,
) -> anyhow::Result<RunWithToolsResult> {
    let mut all_tool_calls: Vec<kb_core::provider::ToolCall> = Vec::new();
    let mut total_usage = kb_core::provider::Usage::default();

    for _round in 0..max_rounds {
        let req = ChatReq {
            messages: messages.clone(),
            tools: Some(registry.definitions()),
            tool_choice: Some(kb_core::provider::ToolChoice::Auto),
            max_tokens: Some(1024),
            ..Default::default()
        };

        let resp = client.chat(Role::Text, model, &req, local_only, 0).await?;
        total_usage.prompt_tokens += resp.usage.prompt_tokens;
        total_usage.completion_tokens += resp.usage.completion_tokens;

        if resp.tool_calls.is_empty() {
            return Ok(RunWithToolsResult {
                text: resp.text,
                tool_calls: all_tool_calls,
                usage: total_usage,
            });
        }

        // Execute each tool call.
        for tc in &resp.tool_calls {
            all_tool_calls.push(tc.clone());

            // Add assistant message with tool calls.
            messages.push(ChatMessage {
                role: ChatRole::Assistant,
                content: String::new(),
                tool_calls: Some(vec![tc.clone()]),
                tool_call_id: None,
            });

            let result = match registry.find(&tc.function.name) {
                Some(tool) => tool
                    .execute(&tc.function.arguments, ctx)
                    .await
                    .unwrap_or_else(|e| {
                        format!(r#"{{"error":"{}","tool":"{}"}}"#, e, tc.function.name)
                    }),
                None => format!(
                    r#"{{"error":"unknown tool","tool":"{}"}}"#,
                    tc.function.name
                ),
            };

            // Add tool result message.
            messages.push(ChatMessage {
                role: ChatRole::Tool,
                content: result,
                tool_calls: None,
                tool_call_id: Some(tc.id.clone()),
            });
        }
    }

    // Max rounds reached without a text response.
    Ok(RunWithToolsResult {
        text: "I ran out of steps while processing your request. Please try again.".into(),
        tool_calls: all_tool_calls,
        usage: total_usage,
    })
}

/// The result of a tool-augmented chat turn.
#[derive(Debug, Clone)]
pub struct RunWithToolsResult {
    /// The final text response from the model.
    pub text: String,
    /// All tool calls made during the turn.
    pub tool_calls: Vec<kb_core::provider::ToolCall>,
    /// Total token usage across all rounds.
    pub usage: kb_core::provider::Usage,
}
