// SPDX-License-Identifier: AGPL-3.0-or-later

//! Extension methods on [`kb_config::Config`] for assistant settings.

/// Assistant-specific configuration section.
///
/// When `opencode_bin` is [`None`], the assistant module is dormant — all routes
/// return 404 and no background jobs start.
#[derive(Debug, Clone)]
pub struct AssistantConfig {
    /// Path to the `opencode` CLI binary. `None` disables the assistant.
    pub opencode_bin: Option<String>,

    /// Maximum subprocess runtime per prompt (seconds). Default 300.
    pub prompt_timeout_secs: u64,

    /// Model reference for agent tasks, e.g. `"local/qwen-35b"`.
    pub model_ref: String,

    /// Conference budget: max fraction of context window for augmented prompts.
    /// Default 85 (percent).
    pub context_budget_pct: u8,

    /// Sensitive department directory names — skipped for commercial API sandboxes.
    pub sensitive_departments: Vec<String>,

    /// Glob patterns for files never copied to the sandbox.
    pub never_copy_patterns: Vec<String>,

    /// Regex for enforced date-prefix file naming.
    pub date_prefix_regex: String,
}

impl Default for AssistantConfig {
    fn default() -> Self {
        // Read the TOML-based config, then overlay env vars (hot-swap rule).
        let toml = kb_config::Assistant::default();
        Self::from_toml_with_env(&toml)
    }
}

impl AssistantConfig {
    /// Build from a TOML config section, with env-var overrides.
    pub fn from_toml_with_env(toml: &kb_config::Assistant) -> Self {
        let opencode_bin = toml
            .opencode_bin
            .clone()
            .or_else(|| std::env::var("OPENCODE_BIN").ok().filter(|v| !v.is_empty()));
        let model_ref = toml
            .model_ref
            .clone()
            .filter(|v| !v.is_empty())
            .or_else(|| {
                std::env::var("ASSISTANT_MODEL_REF")
                    .ok()
                    .filter(|v| !v.is_empty())
            })
            .unwrap_or_default();
        Self {
            opencode_bin,
            prompt_timeout_secs: toml.prompt_timeout_secs.unwrap_or(300),
            model_ref,
            context_budget_pct: toml.context_budget_pct.unwrap_or(85),
            ..Self::default_base()
        }
    }

    /// Base default values shared by all constructors.
    fn default_base() -> Self {
        Self {
            opencode_bin: None,
            prompt_timeout_secs: 300,
            model_ref: String::new(),
            context_budget_pct: 85,
            sensitive_departments: vec![
                "accounting".into(),
                "finance".into(),
                "hr".into(),
                "legal".into(),
                "compliance-risk".into(),
                "security".into(),
                "sales".into(),
            ],
            never_copy_patterns: vec![
                "*.pem".into(),
                "token.json".into(),
                "*secret*".into(),
                "*credential*".into(),
                ".env".into(),
                "*.key".into(),
                "auth.json".into(),
            ],
            date_prefix_regex:
                r"^\d{4}-\d{2}-\d{2}_[a-z0-9](?:[a-z0-9\-]*[a-z0-9])?\.[a-z0-9]+(\.[a-z0-9]+)?$"
                    .into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults_have_expected_values() {
        let cfg = AssistantConfig::default();
        assert!(cfg.opencode_bin.is_none());
        assert_eq!(cfg.prompt_timeout_secs, 300);
        assert_eq!(cfg.context_budget_pct, 85);
        assert!(
            cfg.sensitive_departments
                .contains(&"accounting".to_string())
        );
        assert!(cfg.sensitive_departments.contains(&"legal".to_string()));
        assert!(cfg.never_copy_patterns.contains(&"*.pem".to_string()));
        assert!(cfg.never_copy_patterns.contains(&"token.json".to_string()));
        assert!(cfg.date_prefix_regex.contains(r"\d{4}-\d{2}-\d{2}"));
    }

    #[test]
    fn config_date_prefix_regex_compiles() {
        let cfg = AssistantConfig::default();
        let result = regex::Regex::new(&cfg.date_prefix_regex);
        assert!(result.is_ok());
    }
}
