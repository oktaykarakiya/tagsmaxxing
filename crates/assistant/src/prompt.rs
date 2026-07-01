// SPDX-License-Identifier: AGPL-3.0-or-later

//! Prompt builder — enriches user input with context from the knowledge base.
//!
//! Before each agent prompt, we search the KB for relevant past documents,
//! fetch pending action items, and inject thread continuity from the prior
//! session transcript. The enriched prompt is capped to the configured
//! token budget.

/// Builds augmented prompts by searching the knowledge base for context.
#[derive(Clone)]
pub struct PromptBuilder {
    /// Maximum fraction of the model context window to use (percent, 0-100).
    context_budget_pct: u8,
}

impl PromptBuilder {
    /// Create a new prompt builder.
    #[must_use]
    pub fn new(context_budget_pct: u8) -> Self {
        Self {
            context_budget_pct: context_budget_pct.clamp(1, 100),
        }
    }

    /// Estimate tokens in a string using a simple character-count heuristic.
    #[must_use]
    pub fn estimate_tokens(text: &str) -> usize {
        // Rough heuristic: ~4 chars per token for English text
        text.chars().count().div_ceil(4)
    }

    /// Build an enriched prompt for a given user input.
    ///
    /// The returned string is the augmented prompt ready for the agent.
    #[must_use]
    pub fn build(
        &self,
        user_prompt: &str,
        relevant_context: &str,
        pending_actions: &str,
        recent_decisions: &str,
        thread_continuity: &str,
        max_tokens: usize,
    ) -> String {
        let mut sections: Vec<(&str, &str)> = Vec::new();

        if !pending_actions.is_empty() {
            sections.push(("USER TASK LIST", pending_actions));
        }
        if !recent_decisions.is_empty() {
            sections.push(("USER DECISIONS", recent_decisions));
        }
        if !relevant_context.is_empty() {
            sections.push(("RELEVANT PAST CONTEXT", relevant_context));
        }
        if !thread_continuity.is_empty() {
            sections.push(("THREAD CONTINUITY", thread_continuity));
        }

        let mut prompt = String::new();

        // Build context block
        if !sections.is_empty() {
            prompt.push_str("---\n");
            for (label, content) in &sections {
                prompt.push_str(&format!("# [{label}]\n{content}\n\n"));
            }
            prompt.push_str("---\n\n");
        }

        // User prompt
        prompt.push_str(user_prompt);

        // Cap to budget if needed
        let budget_tokens =
            (max_tokens as f64 * (f64::from(self.context_budget_pct) / 100.0)) as usize;
        let prompt_tokens = Self::estimate_tokens(&prompt);
        if prompt_tokens > budget_tokens {
            // Simple truncation: keep the user prompt, truncate context
            let user_section = format!("---\n\n{user_prompt}");
            let user_tokens = Self::estimate_tokens(&user_section);
            if user_tokens >= budget_tokens {
                return user_prompt.to_string();
            }
            let remaining = budget_tokens - user_tokens;
            let context_chars = remaining * 4;
            let mut truncated = String::with_capacity(context_chars + user_section.len());
            for (label, content) in &sections {
                let header = format!("# [{label}]\n");
                truncated.push_str(&header);
                let snippet: String = content
                    .chars()
                    .take(context_chars / sections.len())
                    .collect();
                truncated.push_str(&snippet);
                truncated.push_str("\n\n");
            }
            truncated.push_str("---\n\n");
            truncated.push_str(user_prompt);
            return truncated;
        }

        prompt
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_estimate_sanity() {
        assert!(PromptBuilder::estimate_tokens("hello world") > 0);
        assert_eq!(PromptBuilder::estimate_tokens(""), 0);
    }

    #[test]
    fn build_enriches_with_sections() {
        let pb = PromptBuilder::new(85);
        let result = pb.build(
            "find my invoice",
            "## Past Context\ninvoice #42 from June",
            "- Remind me to pay invoice #42 by Friday",
            "",
            "",
            4096,
        );
        assert!(result.contains("[RELEVANT PAST CONTEXT]"));
        assert!(result.contains("[USER TASK LIST]"));
        assert!(result.contains("find my invoice"));
    }

    #[test]
    fn build_with_empty_context_still_includes_prompt() {
        let pb = PromptBuilder::new(85);
        let result = pb.build("hello", "", "", "", "", 4096);
        assert_eq!(result, "hello");
    }

    #[test]
    fn budget_cap_truncates_large_context() {
        let pb = PromptBuilder::new(50);
        let huge_context = "x".repeat(10_000);
        let result = pb.build("simple prompt", &huge_context, "", "", "", 100);
        // Should be truncated
        assert!(result.len() < huge_context.len() + 200);
        // Must still contain the user prompt
        assert!(result.contains("simple prompt"));
    }

    #[test]
    fn budget_clamped_to_valid_range() {
        let pb = PromptBuilder::new(200);
        assert_eq!(pb.context_budget_pct, 100);
        let pb = PromptBuilder::new(0);
        assert_eq!(pb.context_budget_pct, 1);
    }

    #[test]
    fn prompt_thread_continuity_section() {
        let pb = PromptBuilder::new(85);
        let result = pb.build(
            "prompt",
            "",
            "",
            "",
            "Previous: we discussed invoices",
            4096,
        );
        assert!(result.contains("[THREAD CONTINUITY]"));
        assert!(result.contains("invoices"));
    }

    #[test]
    fn prompt_decisions_section() {
        let pb = PromptBuilder::new(85);
        let result = pb.build("prompt", "", "", "Decided: use Rust", "", 4096);
        assert!(result.contains("[USER DECISIONS]"));
        assert!(result.contains("Rust"));
    }

    #[test]
    fn prompt_exact_budget_boundary() {
        let pb = PromptBuilder::new(85);
        // budget = 400 * 0.85 = 340 tokens = 1360 chars
        // format overhead with one section + "x": 38 chars
        // content = 1360 - 38 = 1322 chars — exactly at boundary
        let content = "a".repeat(1322);
        let result = pb.build("x", &content, "", "", "", 400);
        assert!(result.contains("[RELEVANT PAST CONTEXT]"));
        assert!(result.contains("x"));
        assert!(
            result.contains(&content),
            "full content should be present at exact boundary"
        );
    }

    #[test]
    fn prompt_user_prompt_exceeds_budget_alone() {
        let pb = PromptBuilder::new(85);
        let user_prompt = "x".repeat(500);
        // budget = 100 * 0.85 = 85 tokens; user_section = ~127 tokens >= 85 → bare prompt
        let result = pb.build(&user_prompt, &"y".repeat(500), "", "", "", 100);
        assert_eq!(result, user_prompt);
    }

    #[test]
    fn prompt_unicode_tokens() {
        let tokens = PromptBuilder::estimate_tokens("🚀 hello 世界");
        assert!(tokens > 0);
    }

    #[test]
    fn prompt_all_sections_empty() {
        let pb = PromptBuilder::new(85);
        let result = pb.build("just a prompt", "", "", "", "", 4096);
        assert_eq!(result, "just a prompt");
    }

    #[test]
    fn prompt_section_priority_order() {
        let pb = PromptBuilder::new(85);
        let result = pb.build(
            "go",
            "context-data",
            "task-data",
            "decision-data",
            "thread-data",
            20000,
        );
        let task_pos = result.find("[USER TASK LIST]").unwrap();
        let decision_pos = result.find("[USER DECISIONS]").unwrap();
        let context_pos = result.find("[RELEVANT PAST CONTEXT]").unwrap();
        let thread_pos = result.find("[THREAD CONTINUITY]").unwrap();
        assert!(task_pos < decision_pos);
        assert!(decision_pos < context_pos);
        assert!(context_pos < thread_pos);
    }
}
