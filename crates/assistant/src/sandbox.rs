// SPDX-License-Identifier: AGPL-3.0-or-later

//! Sandboxed workspace for agent execution.
//!
//! Creates an isolated directory under the system temp path, copies safe files
//! from the workspace root, and atomically syncs validated output back.
//! The container's existing security boundaries (`cap_drop: ALL`,
//! `no-new-privileges`) provide process isolation — this module handles file
//! isolation only.

use std::path::{Path, PathBuf};

use crate::taxonomy::Taxonomy;

/// Operational artifact directory names excluded from sandbox copies.
const EXCLUDED_DIRS: &[&str] = &[
    "logs", ".git", "__pycache__", "node_modules",
    "sessions", "memory", ".backup", ".opencode",
];

/// System files that are blocked from sandbox when using a commercial API model.
const COMMERCIAL_BLOCKED_FILES: &[&str] = &["company-profile.json", "AGENTS.md"];

/// A sandboxed workspace for one agent session.
pub struct Sandbox {
    /// Root of the sandbox filesystem.
    root: PathBuf,
    /// The user's document root (source for safe files).
    company_root: PathBuf,
    /// Whether a commercial (cloud) API model is in use.
    commercial_model: bool,
    /// Pre-compiled taxonomy rules.
    taxonomy: Taxonomy,
}

impl Sandbox {
    /// Create a new sandbox in `base_dir/<session_key>/`.
    pub fn new(
        base_dir: &Path,
        session_key: &str,
        company_root: &Path,
        taxonomy: Taxonomy,
        commercial_model: bool,
    ) -> Self {
        let root = base_dir.join(sanitize_session_key(session_key));
        Self {
            root,
            company_root: company_root.to_path_buf(),
            commercial_model,
            taxonomy,
        }
    }

    /// Returns the root path of the sandbox.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Create the sandbox directory and copy safe files from the workspace.
    ///
    /// # Errors
    ///
    /// Returns an error if directory creation or file copying fails.
    pub fn setup(&self) -> Result<(), std::io::Error> {
        if self.root.exists() {
            std::fs::remove_dir_all(&self.root)?;
        }
        std::fs::create_dir_all(&self.root)?;

        // Copy safe root-level files
        for entry in std::fs::read_dir(&self.company_root)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if self.should_skip_file(name) {
                continue;
            }
            let dest = self.root.join(name);
            std::fs::copy(&path, &dest)?;
        }

        // Copy department subdirectories (skipping sensitive when commercial)
        for entry in std::fs::read_dir(&self.company_root)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if EXCLUDED_DIRS.contains(&name) {
                continue;
            }
            if self.commercial_model && self.taxonomy.is_sensitive_path(&path) {
                continue;
            }
            copy_dir_all(&path, &self.root.join(name))?;
        }

        Ok(())
    }

    /// Clean up the sandbox directory.
    pub fn cleanup(&self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }

    fn should_skip_file(&self, name: &str) -> bool {
        if self.taxonomy.is_never_copy(name) {
            return true;
        }
        if self.commercial_model && COMMERCIAL_BLOCKED_FILES.contains(&name) {
            return true;
        }
        false
    }
}

/// Recursively copy a directory tree.
fn copy_dir_all(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if ty.is_dir() {
            copy_dir_all(&src_path, &dst_path)?;
        } else if ty.is_file() {
            std::fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
}

/// Sanitize a session key for use as a directory name.
fn sanitize_session_key(key: &str) -> String {
    key.chars()
        .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_ext::AssistantConfig;

    #[test]
    fn sanitize_replaces_slashes() {
        let result = sanitize_session_key("abc/../def");
        assert!(!result.contains('/'));
        assert!(!result.contains('\\'));
        assert!(!result.contains(".."));
    }

    #[test]
    fn sandbox_setup_creates_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let company = tempfile::tempdir().unwrap();
        // Create a safe file in the company root
        std::fs::write(company.path().join("readme.md"), "# Test").unwrap();

        let taxonomy = Taxonomy::from_config(&AssistantConfig::default()).unwrap();
        let sandbox = Sandbox::new(
            tmp.path(),
            "test-session",
            company.path(),
            taxonomy,
            false,
        );
        sandbox.setup().unwrap();
        assert!(sandbox.root().exists());
        assert!(sandbox.root().join("readme.md").exists());

        sandbox.cleanup();
        assert!(!sandbox.root().exists());
    }

    #[test]
    fn never_copy_files_are_skipped() {
        let tmp = tempfile::tempdir().unwrap();
        let company = tempfile::tempdir().unwrap();
        std::fs::write(company.path().join("readme.md"), "# Test").unwrap();
        std::fs::write(company.path().join("token.json"), "secret").unwrap();
        std::fs::write(company.path().join(".env"), "SECRET=1").unwrap();

        let taxonomy = Taxonomy::from_config(&AssistantConfig::default()).unwrap();
        let sandbox = Sandbox::new(
            tmp.path(),
            "test-never-copy",
            company.path(),
            taxonomy,
            false,
        );
        sandbox.setup().unwrap();

        assert!(sandbox.root().join("readme.md").exists());
        assert!(!sandbox.root().join("token.json").exists());
        assert!(!sandbox.root().join(".env").exists());

        sandbox.cleanup();
    }
}
