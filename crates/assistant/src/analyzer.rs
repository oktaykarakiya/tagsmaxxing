// SPDX-License-Identifier: AGPL-3.0-or-later

//! Post-prompt output analyzer.
//!
//! Scans the agent's response for quality signals: missed memory-block updates,
//! naming violations, abnormally short responses, and potential action items
//! that should have been created.

use crate::taxonomy::Taxonomy;

/// An improvement opportunity found by the analyzer.
#[derive(Debug, Clone)]
pub struct Improvement {
    /// Human-readable suggestion.
    pub message: String,
    /// Severity level.
    pub severity: Severity,
}

/// Severity of an improvement suggestion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    /// Informational — nice to have.
    Info,
    /// Warning — should be addressed.
    Warning,
    /// Error — must be fixed.
    Error,
}

/// Analyzes agent output for quality and completeness.
pub struct Analyzer {
    /// Pre-compiled taxonomy for filename validation.
    taxonomy: Taxonomy,
    /// Minimum response tokens — below this is flagged as potentially incomplete.
    min_response_tokens: usize,
}

impl Analyzer {
    /// Create a new analyzer.
    #[must_use]
    pub fn new(taxonomy: Taxonomy, min_response_tokens: usize) -> Self {
        Self {
            taxonomy,
            min_response_tokens,
        }
    }

    /// Analyze the agent's output and return improvement suggestions.
    #[must_use]
    pub fn analyze(
        &self,
        response_text: &str,
        changed_files: &[String],
        prompt_tokens: usize,
        response_tokens: usize,
    ) -> Vec<Improvement> {
        let mut improvements = Vec::new();

        // Check for missing memory block updates
        self.check_memory_blocks(response_text, &mut improvements);

        // Check for abnormally short responses
        if response_tokens < self.min_response_tokens && prompt_tokens > 20 {
            improvements.push(Improvement {
                message: format!(
                    "Short response ({} tokens) for a {} token prompt — may be incomplete",
                    response_tokens, prompt_tokens
                ),
                severity: Severity::Warning,
            });
        }

        // Validate changed file names
        for file in changed_files {
            let name = std::path::Path::new(file)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(file);
            let violations = self.taxonomy.validate_filename(name);
            for v in violations {
                improvements.push(Improvement {
                    message: v,
                    severity: Severity::Warning,
                });
            }
        }

        // Check for empty tool-call output
        if response_text.trim().is_empty() {
            improvements.push(Improvement {
                message: "Agent produced no output — possible crash or timeout".into(),
                severity: Severity::Error,
            });
        }

        improvements
    }

    fn check_memory_blocks(&self, text: &str, improvements: &mut Vec<Improvement>) {
        let has_open = text.contains("AGENT_MEMORY_START");
        let has_close = text.contains("AGENT_MEMORY_END");

        if has_open && !has_close {
            improvements.push(Improvement {
                message:
                    "AGENT_MEMORY_START without matching AGENT_MEMORY_END — block may be malformed"
                        .into(),
                severity: Severity::Warning,
            });
        }
        if has_close && !has_open {
            improvements.push(Improvement {
                message: "AGENT_MEMORY_END without AGENT_MEMORY_START — block may be malformed"
                    .into(),
                severity: Severity::Warning,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use crate::config_ext::AssistantConfig;

    use super::*;

    fn test_analyzer() -> Analyzer {
        let taxonomy = Taxonomy::from_config(&AssistantConfig::default()).unwrap();
        Analyzer::new(taxonomy, 10)
    }

    #[test]
    fn flags_short_responses() {
        let a = test_analyzer();
        let improvements = a.analyze("ok", &[], 100, 1);
        assert!(
            improvements
                .iter()
                .any(|i| i.message.contains("Short response"))
        );
    }

    #[test]
    fn flags_empty_output() {
        let a = test_analyzer();
        let improvements = a.analyze("   \n", &[], 50, 2);
        assert!(improvements.iter().any(|i| i.severity == Severity::Error));
    }

    #[test]
    fn flags_unbalanced_memory_blocks() {
        let a = test_analyzer();
        let improvements = a.analyze("some text <!-- AGENT_MEMORY_START --> stuff", &[], 10, 100);
        assert!(
            improvements
                .iter()
                .any(|i| i.message.contains("AGENT_MEMORY"))
        );
    }

    #[test]
    fn flags_naming_violations() {
        let a = test_analyzer();
        let improvements = a.analyze(
            "all good here, processed the file successfully",
            &["report.pdf".into()],
            10,
            50,
        );
        assert!(improvements.iter().any(|i| i.message.contains("missing")));
    }

    #[test]
    fn clean_output_no_flags() {
        let a = test_analyzer();
        let improvements = a.analyze(
            "The document has been updated with the Q2 figures. All files passed validation.",
            &["2026-06-30_q2-report.csv".into()],
            10,
            100,
        );
        // No violations for date-prefixed files, adequate length, no memory issues
        assert!(improvements.is_empty());
    }

    #[test]
    fn analyzer_balanced_memory_blocks() {
        let a = test_analyzer();
        let improvements = a.analyze(
            "<!-- AGENT_MEMORY_START -->\n- rule\n<!-- AGENT_MEMORY_END -->",
            &["2026-06-30_report.csv".into()],
            10,
            100,
        );
        assert!(improvements.is_empty());
    }

    #[test]
    fn analyzer_multiple_violations() {
        let a = test_analyzer();
        let improvements = a.analyze("ok", &["report.pdf".into()], 100, 1);
        assert!(improvements.len() >= 2);
        let has_short = improvements
            .iter()
            .any(|i| i.message.contains("Short response"));
        let has_naming = improvements.iter().any(|i| i.message.contains("missing"));
        assert!(has_short);
        assert!(has_naming);
    }

    #[test]
    fn analyzer_no_changed_files() {
        let a = test_analyzer();
        let improvements = a.analyze(
            "A detailed response with enough tokens to avoid the short-response check.",
            &[],
            10,
            50,
        );
        assert!(improvements.iter().all(|i| !i.message.contains("missing")));
    }

    #[test]
    fn analyzer_custom_low_min_tokens() {
        let taxonomy = Taxonomy::from_config(&AssistantConfig::default()).unwrap();
        let a = Analyzer::new(taxonomy, 5);
        let improvements = a.analyze(
            "This response has enough tokens to be above the custom low minimum.",
            &[],
            100,
            20,
        );
        assert!(
            improvements
                .iter()
                .all(|i| !i.message.contains("Short response"))
        );
    }

    #[test]
    fn analyzer_custom_high_min_tokens() {
        let taxonomy = Taxonomy::from_config(&AssistantConfig::default()).unwrap();
        let a = Analyzer::new(taxonomy, 50);
        let improvements = a.analyze("Short reply.", &[], 100, 20);
        assert!(
            improvements
                .iter()
                .any(|i| i.message.contains("Short response"))
        );
    }

    #[test]
    fn analyzer_html_in_response_does_not_false_trigger() {
        let a = test_analyzer();
        let improvements = a.analyze(
            "click <!-- AGENT_MEMORY_START --> here <!-- AGENT_MEMORY_END -->",
            &[],
            10,
            50,
        );
        assert!(
            improvements
                .iter()
                .all(|i| !i.message.contains("AGENT_MEMORY"))
        );
    }
}
