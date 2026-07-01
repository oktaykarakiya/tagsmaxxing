// SPDX-License-Identifier: AGPL-3.0-or-later

//! File taxonomy — naming conventions, safety classification, and validation.
//!
//! Ported from `ai-assistant/plugins/business/taxonomy.py`. Configurable via
//! [`AssistantConfig`], with sensible defaults for a knowledge-base workspace.

use std::path::Path;

use regex::Regex;

use crate::config_ext::AssistantConfig;

/// Pre-compiled taxonomy rules loaded from configuration.
#[derive(Debug)]
pub struct Taxonomy {
    /// Compiled regex for date-prefixed kebab-case filenames.
    date_prefix: Regex,
    /// File extensions exempt from date-prefix enforcement (code/config files).
    /// Matching Python `ai-assistant` SAFE_PATTERNS.
    safe_extensions: Vec<String>,
    /// Simple glob patterns for NEVER_COPY — stored as compiled Regex via
    /// fnmatch-to-regex conversion.
    never_copy: Vec<Regex>,
    /// Directory names that are sensitive and skip sandbox copies when
    /// using a commercial API model.
    sensitive_depts: Vec<String>,
}

impl Taxonomy {
    /// Build from configuration, compiling regexes at startup.
    ///
    /// # Errors
    ///
    /// Returns an error if any regex pattern is invalid.
    pub fn from_config(cfg: &AssistantConfig) -> Result<Self, regex::Error> {
        let date_prefix = Regex::new(&cfg.date_prefix_regex)?;
        let safe_extensions = vec![
            ".py",
            ".js",
            ".ts",
            ".tsx",
            ".jsx",
            ".html",
            ".css",
            ".json",
            ".md",
            ".mdx",
            ".txt",
            ".csv",
            ".yaml",
            ".yml",
            ".toml",
            ".cfg",
            ".ini",
            ".sh",
            ".bash",
            ".conf",
            ".xml",
            ".properties",
            ".service",
            ".sql",
            ".svg",
            ".log",
            ".env.example",
            // Rust + compiled languages
            ".rs",
            ".go",
            ".java",
            ".rb",
            ".c",
            ".h",
            ".cpp",
            ".hpp",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect();
        let never_copy = cfg
            .never_copy_patterns
            .iter()
            .filter_map(|pat| fnmatch_to_regex(pat).and_then(|r| Regex::new(&r).ok()))
            .collect();
        Ok(Self {
            date_prefix,
            safe_extensions,
            never_copy,
            sensitive_depts: cfg.sensitive_departments.clone(),
        })
    }

    /// Returns `true` if the filename matches the date-prefix enforcement rule.
    #[must_use]
    pub fn is_date_prefixed(&self, name: &str) -> bool {
        self.date_prefix.is_match(name)
    }

    /// Returns `true` if the filename is a code/config file exempt from
    /// date-prefix enforcement (matches a known safe extension).
    #[must_use]
    pub fn is_safe_extension(&self, name: &str) -> bool {
        self.safe_extensions
            .iter()
            .any(|ext| name.ends_with(ext.as_str()))
    }

    /// Returns `true` if the filename should NEVER be copied to a sandbox.
    #[must_use]
    pub fn is_never_copy(&self, name: &str) -> bool {
        self.never_copy.iter().any(|re| re.is_match(name))
    }

    /// Returns `true` if the path contains a sensitive department directory.
    #[must_use]
    pub fn is_sensitive_path(&self, path: &Path) -> bool {
        path.components().any(|c| {
            if let Some(name) = c.as_os_str().to_str() {
                self.sensitive_depts.iter().any(|d| d == name)
            } else {
                false
            }
        })
    }

    /// Classify a path as `confidential`, `internal`, or `public`.
    #[must_use]
    pub fn classify_path(&self, path: &Path) -> &'static str {
        if self.is_sensitive_path(path) {
            return "confidential";
        }
        // Root-level files with no subdirectory are "internal"
        if path.parent().is_none_or(|p| p.as_os_str().is_empty()) {
            return "internal";
        }
        "public"
    }

    /// Validate a file name. Returns a list of violation messages.
    ///
    /// Code/config files (known safe extensions: `.py`, `.js`, `.json`, `.md`, etc.)
    /// are exempt from date-prefix enforcement. Content files (`.pdf`, `.docx`, etc.)
    /// must have `YYYY-MM-DD_` prefix.
    #[must_use]
    pub fn validate_filename(&self, name: &str) -> Vec<String> {
        let mut violations = Vec::new();

        // Code/config exemption — files with known safe extensions
        if self.is_safe_extension(name) {
            return violations;
        }

        // Content files need a date prefix
        if !self.is_date_prefixed(name) {
            violations.push(format!(
                "file '{name}' is missing required YYYY-MM-DD_ prefix"
            ));
        }

        violations
    }
}

// ── fnmatch-to-regex conversion (handles the simple globs used in NEVER_COPY) ──

/// Convert a simple fnmatch-style glob to a regex pattern.
/// Supports: `*` (any chars), `?` (single char). No `[...]` or `{...}`.
fn fnmatch_to_regex(pattern: &str) -> Option<String> {
    let mut re = String::from("^");
    for ch in pattern.chars() {
        match ch {
            '*' => re.push_str(".*"),
            '?' => re.push('.'),
            '.' | '+' | '(' | ')' | '|' | '^' | '$' | '{' | '}' | '[' | ']' | '\\' => {
                re.push('\\');
                re.push(ch);
            }
            _ => re.push(ch),
        }
    }
    re.push('$');
    Some(re)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_taxonomy() -> Taxonomy {
        let cfg = AssistantConfig::default();
        Taxonomy::from_config(&cfg).expect("valid config")
    }

    #[test]
    fn date_prefixed_names_pass() {
        let t = test_taxonomy();
        assert!(t.is_date_prefixed("2026-01-15_quarterly-report.pdf"));
        assert!(t.is_date_prefixed("2024-12-01_invoice-003.csv"));
    }

    #[test]
    fn non_prefixed_names_fail() {
        let t = test_taxonomy();
        assert!(!t.is_date_prefixed("report.pdf"));
        assert!(!t.is_date_prefixed("invoice.csv"));
    }

    #[test]
    fn kebab_case_code_files_pass() {
        let t = test_taxonomy();
        assert!(t.is_safe_extension("web-server.py"));
        assert!(t.is_safe_extension("my-utils.js"));
        assert!(t.is_safe_extension("config.toml"));
        assert!(!t.is_safe_extension("report.pdf"));
        assert!(!t.is_safe_extension("invoice.docx"));
    }

    #[test]
    fn never_copy_matches_patterns() {
        let t = test_taxonomy();
        assert!(t.is_never_copy("token.json"));
        assert!(t.is_never_copy("some-secret-file.txt"));
        assert!(t.is_never_copy("server.pem"));
        assert!(t.is_never_copy("my.key"));
        assert!(t.is_never_copy("auth.json"));
        assert!(t.is_never_copy(".env"));
        assert!(t.is_never_copy("secret-credentials.yml"));
        assert!(!t.is_never_copy("readme.md"));
        assert!(!t.is_never_copy("report.pdf"));
    }

    #[test]
    fn validate_code_files_no_violations() {
        let t = test_taxonomy();
        assert!(t.validate_filename("main.rs").is_empty());
        assert!(t.validate_filename("app.js").is_empty());
        assert!(t.validate_filename("config.yaml").is_empty());
    }

    #[test]
    fn validate_content_files_without_prefix_produces_violation() {
        let t = test_taxonomy();
        let violations = t.validate_filename("report.pdf");
        assert!(!violations.is_empty());
    }

    #[test]
    fn fnmatch_basic_patterns() {
        let re = fnmatch_to_regex("*.pem").unwrap();
        assert!(Regex::new(&re).unwrap().is_match("server.pem"));
        assert!(!Regex::new(&re).unwrap().is_match("pem.txt"));

        let re = fnmatch_to_regex("*secret*").unwrap();
        assert!(Regex::new(&re).unwrap().is_match("my-secret-file.txt"));
        assert!(Regex::new(&re).unwrap().is_match("secret.json"));
        assert!(!Regex::new(&re).unwrap().is_match("public.txt"));
    }

    #[test]
    fn classify_path_confidential() {
        let t = test_taxonomy();
        let classification = t.classify_path(Path::new("accounting/report.pdf"));
        assert_eq!(classification, "confidential");
    }

    #[test]
    fn classify_path_internal() {
        let t = test_taxonomy();
        let classification = t.classify_path(Path::new("readme.md"));
        assert_eq!(classification, "internal");
    }

    #[test]
    fn classify_path_public() {
        let t = test_taxonomy();
        let classification = t.classify_path(Path::new("marketing/brochure.pdf"));
        assert_eq!(classification, "public");
    }

    #[test]
    fn is_sensitive_nested() {
        let t = test_taxonomy();
        assert!(t.is_sensitive_path(Path::new("company/accounting/reports/x.pdf")));
    }

    #[test]
    fn is_sensitive_non_sensitive() {
        let t = test_taxonomy();
        assert!(!t.is_sensitive_path(Path::new("marketing/ads/campaign.pdf")));
    }

    #[test]
    fn double_extension_date_prefixed() {
        let t = test_taxonomy();
        assert!(t.is_date_prefixed("2026-01-15_backup.tar.gz"));
    }

    #[test]
    fn hidden_file_dot_gitignore_safe() {
        let t = test_taxonomy();
        assert!(!t.is_safe_extension(".gitignore"));
        assert!(t.is_safe_extension(".json"));
        assert!(t.is_safe_extension(".md"));
        assert!(t.is_safe_extension(".toml"));
        assert!(t.is_safe_extension(".rs"));
    }
}
