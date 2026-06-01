// pgbackrest_validation.rs — structural validation of pgBackRest config, backup
// scripts, and DR runbook (P8-T6, plan §21).
//
// Pure file-structure checks. Does NOT require pgBackRest, Postgres, Podman, or B2
// credentials at test time.
//
// Validates:
//   - ops/pgbackrest.conf: INI-like structure, required sections+keys
//   - scripts/*.sh: existence, shebangs, non-empty
//   - docs/DR_RUNBOOK.md: required sections present
//   - .env.example: backup-related env vars documented

#![allow(missing_docs)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;

/// Workspace root, resolved from the kb-api crate manifest directory.
fn workspace_root() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest.join("../..")
}

/// Read a file relative to the workspace root.
fn read_workspace_file(relative_path: &str) -> String {
    let path = workspace_root().join(relative_path);
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

/// Return the set of keys found in `[section]` within an INI-like file.
fn ini_section_keys(content: &str, section: &str) -> HashSet<String> {
    let marker = format!("[{section}]");
    let mut inside = false;
    let mut keys = HashSet::new();
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed == marker {
            inside = true;
            continue;
        }
        if inside {
            // A new section starts → we left ours.
            if trimmed.starts_with('[') && trimmed.ends_with(']') {
                inside = false;
                continue;
            }
            // Skip comments and blanks.
            if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with(';') {
                continue;
            }
            // Grab the key (before the first `=`).
            if let Some(key) = trimmed.split('=').next() {
                let key = key.trim().to_string();
                if !key.is_empty() {
                    keys.insert(key);
                }
            }
        }
    }
    keys
}

/// Return true if the content has a line starting with `#!`.
fn has_shebang(content: &str) -> bool {
    content.lines().next().is_some_and(|l| l.starts_with("#!"))
}

/// Return the set of markdown heading titles (lines starting with `## ` or `### `).
fn markdown_headings(content: &str) -> Vec<String> {
    content
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if trimmed.starts_with("## ") {
                Some(trimmed.strip_prefix("## ").unwrap().to_string())
            } else {
                None
            }
        })
        .collect()
}

// ── pgbackrest.conf ──────────────────────────────────────────────────────

const PGBACKREST_CONF: &str = "ops/pgbackrest.conf";

#[test]
fn pgbackrest_conf_exists() {
    let path = workspace_root().join(PGBACKREST_CONF);
    assert!(path.is_file(), "missing file: {}", path.display());
}

#[test]
fn pgbackrest_conf_has_stanza_section() {
    let content = read_workspace_file(PGBACKREST_CONF);
    assert!(
        content.contains("[kb]"),
        "pgbackrest.conf missing [kb] stanza section"
    );
}

#[test]
fn pgbackrest_conf_has_global_section() {
    let content = read_workspace_file(PGBACKREST_CONF);
    assert!(
        content.contains("[global]"),
        "pgbackrest.conf missing [global] section"
    );
}

#[test]
fn pgbackrest_conf_stanza_has_pg1_path() {
    let content = read_workspace_file(PGBACKREST_CONF);
    let keys = ini_section_keys(&content, "kb");
    assert!(
        keys.contains("pg1-path"),
        "[kb] section missing pg1-path (Postgres data directory)"
    );
}

#[test]
fn pgbackrest_conf_global_has_repo_type_s3() {
    let content = read_workspace_file(PGBACKREST_CONF);
    let keys = ini_section_keys(&content, "global");
    assert!(
        keys.contains("repo1-type"),
        "[global] section missing repo1-type"
    );
    // Verify it's set to S3 (B2-compatible).
    assert!(
        content.contains("repo1-type=s3"),
        "repo1-type must be 's3' for B2-compatible backups"
    );
}

#[test]
fn pgbackrest_conf_global_has_b2_endpoint() {
    let content = read_workspace_file(PGBACKREST_CONF);
    assert!(
        content.contains("backblazeb2.com"),
        "pgbackrest.conf should target the Backblaze B2 S3 endpoint"
    );
    let keys = ini_section_keys(&content, "global");
    assert!(
        keys.contains("repo1-s3-bucket"),
        "[global] missing repo1-s3-bucket"
    );
    assert!(
        keys.contains("repo1-s3-endpoint"),
        "[global] missing repo1-s3-endpoint"
    );
}

#[test]
fn pgbackrest_conf_has_repo_encryption() {
    let content = read_workspace_file(PGBACKREST_CONF);
    let keys = ini_section_keys(&content, "global");
    assert!(
        keys.contains("repo1-cipher-type"),
        "[global] missing repo1-cipher-type (§21 requires repo encryption)"
    );
    assert!(
        content.contains("aes-256-cbc"),
        "repo1-cipher-type must be aes-256-cbc"
    );
}

#[test]
fn pgbackrest_conf_has_retention_policy() {
    let content = read_workspace_file(PGBACKREST_CONF);
    let keys = ini_section_keys(&content, "global");
    assert!(
        keys.contains("repo1-retention-full"),
        "[global] missing repo1-retention-full (retention policy)"
    );
    assert!(
        keys.contains("repo1-retention-diff"),
        "[global] missing repo1-retention-diff (retention policy)"
    );
}

#[test]
fn pgbackrest_conf_has_compression() {
    let content = read_workspace_file(PGBACKREST_CONF);
    let keys = ini_section_keys(&content, "global");
    assert!(
        keys.contains("compress-type"),
        "[global] missing compress-type"
    );
}

#[test]
fn pgbackrest_conf_has_archive_async() {
    let content = read_workspace_file(PGBACKREST_CONF);
    let keys = ini_section_keys(&content, "global");
    assert!(
        keys.contains("archive-async"),
        "[global] missing archive-async (WAL archiving mode)"
    );
}

#[test]
fn pgbackrest_conf_has_archive_push_section() {
    let content = read_workspace_file(PGBACKREST_CONF);
    assert!(
        content.contains("[global:archive-push]"),
        "pgbackrest.conf missing [global:archive-push] section"
    );
}

#[test]
fn pgbackrest_conf_no_plaintext_secrets() {
    let content = read_workspace_file(PGBACKREST_CONF);
    // The config must NOT contain literal secrets — keys must be supplied via
    // the environment (B2_BACKUP_KEY_ID, B2_BACKUP_KEY, etc.).
    // Check that s3-key values are NOT hardcoded in the config.
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("repo1-s3-key=") && !trimmed.starts_with('#') {
            // If the key is set, it must reference an env var or be commented out.
            assert!(
                trimmed.contains("${") || trimmed == "repo1-s3-key=",
                "pgbackrest.conf must not contain literal B2 keys (§24): {trimmed}"
            );
        }
    }
}

// ── Backup scripts ────────────────────────────────────────────────────────

const BACKUP_SCRIPTS: &[&str] = &[
    "scripts/pgbackrest-stanza-create.sh",
    "scripts/pgbackrest-backup.sh",
    "scripts/pgbackrest-restore.sh",
];

#[test]
fn backup_scripts_exist() {
    for script in BACKUP_SCRIPTS {
        let path = workspace_root().join(script);
        assert!(path.is_file(), "missing script: {}", path.display());
    }
}

#[test]
fn backup_scripts_have_shebang() {
    for script in BACKUP_SCRIPTS {
        let content = read_workspace_file(script);
        assert!(
            has_shebang(&content),
            "{script} missing shebang line (#!/usr/bin/env bash)"
        );
    }
}

#[test]
fn backup_scripts_are_non_empty() {
    for script in BACKUP_SCRIPTS {
        let content = read_workspace_file(script);
        let non_blank_non_comment: Vec<&str> = content
            .lines()
            .filter(|l| {
                let trimmed = l.trim();
                !trimmed.is_empty() && !trimmed.starts_with('#')
            })
            .collect();
        assert!(
            non_blank_non_comment.len() >= 3,
            "{script} is too short ({len} non-comment lines) — likely a stub",
            len = non_blank_non_comment.len()
        );
    }
}

#[test]
fn backup_scripts_use_set_euo_pipefail() {
    for script in BACKUP_SCRIPTS {
        let content = read_workspace_file(script);
        // The `set -euo pipefail` pattern (or `set -eu` + separate pipefail).
        let has_set = content.contains("set -euo pipefail") || content.contains("set -eu");
        assert!(
            has_set,
            "{script} must use 'set -euo pipefail' (or -eu) for strict error handling"
        );
    }
}

#[test]
fn backup_scripts_reference_pgbackrest_config() {
    for script in BACKUP_SCRIPTS {
        let content = read_workspace_file(script);
        assert!(
            content.contains("pgbackrest.conf") || content.contains("PGBACKREST_CONFIG"),
            "{script} does not reference pgbackrest.conf"
        );
    }
}

#[test]
fn backup_script_supports_type_flag() {
    let content = read_workspace_file("scripts/pgbackrest-backup.sh");
    assert!(
        content.contains("--type"),
        "pgbackrest-backup.sh must support --type flag (full, diff, incr)"
    );
}

#[test]
fn restore_script_supports_target_flag() {
    let content = read_workspace_file("scripts/pgbackrest-restore.sh");
    assert!(
        content.contains("--target"),
        "pgbackrest-restore.sh must support --target flag for PITR"
    );
}

#[test]
fn stanza_create_script_is_idempotent_documented() {
    let content = read_workspace_file("scripts/pgbackrest-stanza-create.sh");
    let lower = content.to_lowercase();
    assert!(
        lower.contains("idempotent") || lower.contains("no-op") || lower.contains("already exists"),
        "pgbackrest-stanza-create.sh must document idempotent behaviour"
    );
}

#[test]
fn restore_script_has_confirmation_prompt() {
    let content = read_workspace_file("scripts/pgbackrest-restore.sh");
    assert!(
        content.contains("WARNING") || content.contains("Proceed") || content.contains("confirm"),
        "pgbackrest-restore.sh must warn before destructive restore"
    );
    assert!(
        content.contains("--force"),
        "pgbackrest-restore.sh must support --force to skip confirmation"
    );
}

// ── DR_RUNBOOK.md ─────────────────────────────────────────────────────────

const DR_RUNBOOK: &str = "docs/DR_RUNBOOK.md";

#[test]
fn dr_runbook_exists() {
    let path = workspace_root().join(DR_RUNBOOK);
    assert!(path.is_file(), "missing file: {}", path.display());
}

#[test]
fn dr_runbook_has_rto_rpo_section() {
    let content = read_workspace_file(DR_RUNBOOK);
    assert!(
        content.contains("RTO") || content.contains("Recovery Time"),
        "DR_RUNBOOK.md must define RTO (Recovery Time Objective)"
    );
    assert!(
        content.contains("RPO") || content.contains("Recovery Point"),
        "DR_RUNBOOK.md must define RPO (Recovery Point Objective)"
    );
}

#[test]
fn dr_runbook_has_prerequisites_section() {
    let content = read_workspace_file(DR_RUNBOOK);
    assert!(
        content.contains("Prerequisite") || content.contains("prerequisite"),
        "DR_RUNBOOK.md must list prerequisites for recovery"
    );
}

#[test]
fn dr_runbook_has_restore_steps() {
    let content = read_workspace_file(DR_RUNBOOK);
    // The runbook must document numbered/sectioned restore steps.
    let headings = markdown_headings(&content);
    let has_restore_heading = headings.iter().any(|h| {
        let lower = h.to_lowercase();
        lower.contains("restore") || lower.contains("recovery") || lower.contains("procedure")
    });
    assert!(
        has_restore_heading,
        "DR_RUNBOOK.md must have a restore procedure section. Headings found: {headings:?}"
    );
}

#[test]
fn dr_runbook_references_object_lock() {
    let content = read_workspace_file(DR_RUNBOOK);
    assert!(
        content.contains("Object Lock"),
        "DR_RUNBOOK.md must note that the backup bucket has Object Lock enabled (§21)"
    );
}

#[test]
fn dr_runbook_references_encryption() {
    let content = read_workspace_file(DR_RUNBOOK);
    assert!(
        content.contains("AES-256-CBC")
            || content.contains("aes-256-cbc")
            || content.contains("encryption"),
        "DR_RUNBOOK.md must document repo encryption (§21)"
    );
}

#[test]
fn dr_runbook_has_backup_scheduling_section() {
    let content = read_workspace_file(DR_RUNBOOK);
    let lower = content.to_lowercase();
    assert!(
        lower.contains("schedule") || lower.contains("cron") || lower.contains("timer"),
        "DR_RUNBOOK.md must document backup scheduling (cron or systemd timer)"
    );
}

#[test]
fn dr_runbook_has_wal_archiving_section() {
    let content = read_workspace_file(DR_RUNBOOK);
    assert!(
        content.contains("archive_mode")
            || content.contains("archive_command")
            || content.contains("WAL archiving"),
        "DR_RUNBOOK.md must document WAL archiving configuration"
    );
}

// ── .env.example ──────────────────────────────────────────────────────────

const DOTENV_EXAMPLE: &str = ".env.example";

#[test]
fn dotenv_example_has_backup_vars() {
    let content = read_workspace_file(DOTENV_EXAMPLE);
    for var in &[
        "B2_BACKUP_KEY_ID",
        "B2_BACKUP_KEY",
        "B2_BACKUP_BUCKET",
        "PGBACKREST_REPO1_CIPHER_PASS",
    ] {
        assert!(
            content.contains(var),
            ".env.example must document {var} for pgBackRest backups (§21)"
        );
    }
}

#[test]
fn dotenv_example_backup_vars_are_commented() {
    let content = read_workspace_file(DOTENV_EXAMPLE);
    // The backup vars should have descriptive comments.
    assert!(
        content.contains("separate bucket")
            || content.contains("Object Lock")
            || content.contains("Object Lock"),
        ".env.example should explain that the backup bucket is separate with Object Lock"
    );
}

// ── Idempotency documentation ────────────────────────────────────────────

#[test]
fn dr_runbook_documents_idempotent_operations() {
    let content = read_workspace_file(DR_RUNBOOK);
    let lower = content.to_lowercase();
    // The runbook should note somewhere that restore is repeatable.
    assert!(
        lower.contains("idempotent") || lower.contains("repeatable") || lower.contains("re-run"),
        "DR_RUNBOOK.md should document that backup/restore operations are idempotent"
    );
}
