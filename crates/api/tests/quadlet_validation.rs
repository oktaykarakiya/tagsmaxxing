// quadlet_validation.rs — structural validation of the quadlet deployment files.
//
// §14, P7-T3: proves that every required unit file exists, carries its
// mandatory section, and has no contradictory settings.  Does NOT require
// podman or quadlet at test time — pure file-structure checks.
//
// The `quadlet -dryrun` acceptance gate was verified manually during
// development (see commit message).

#![allow(missing_docs)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;

/// All expected quadlet files (relative to the workspace root `quadlet/` dir).
const EXPECTED_FILES: &[&str] = &[
    "kb.pod",
    "kb-app.container",
    "kb-postgres.container",
    "kb-tika.container",
    "kb-llama.container",
    "kb-whisper.container",
    "kb-pgdata.volume",
    "kb-app-data.volume",
];

fn quadlet_dir() -> PathBuf {
    // CARGO_MANIFEST_DIR points at crates/api when running from kb-api crate.
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let quadlet = manifest.join("../../quadlet");
    assert!(
        quadlet.is_dir(),
        "quadlet directory missing: {}",
        quadlet.display()
    );
    quadlet
}

/// Return the content of a quadlet file as a string.
fn read_quadlet_file(name: &str) -> String {
    let path = quadlet_dir().join(name);
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

/// Return the set of keys present in `[section]` across all lines.
fn section_keys(content: &str, section: &str) -> HashSet<String> {
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
            // A new `[section]` starts → we left our section.
            if trimmed.starts_with('[') && trimmed.ends_with(']') {
                inside = false;
                continue;
            }
            // Skip comments and blanks.
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            // Grab the key (before the first `=`).
            if let Some(key) = trimmed.split('=').next() {
                keys.insert(key.to_string());
            }
        }
    }
    keys
}

// ── File existence ───────────────────────────────────────────────────────

#[test]
fn all_expected_files_present() {
    let dir = quadlet_dir();
    for name in EXPECTED_FILES {
        let p = dir.join(name);
        assert!(p.is_file(), "missing quadlet file: {}", p.display());
    }
}

#[test]
fn no_unexpected_files() {
    let dir = quadlet_dir();
    let expected: HashSet<&str> = EXPECTED_FILES.iter().copied().collect();
    for entry in fs::read_dir(&dir).unwrap() {
        let fname = entry.unwrap().file_name();
        let name = fname.to_str().unwrap();
        assert!(
            expected.contains(name),
            "unexpected file in quadlet/: {name}"
        );
    }
}

// ── Pod ──────────────────────────────────────────────────────────────────

#[test]
fn pod_has_required_sections() {
    let content = read_quadlet_file("kb.pod");
    assert!(content.contains("[Pod]"), "kb.pod missing [Pod] section");
    assert!(
        content.contains("[Install]"),
        "kb.pod missing [Install] section"
    );
}

#[test]
fn pod_has_pod_name() {
    let content = read_quadlet_file("kb.pod");
    let keys = section_keys(&content, "Pod");
    assert!(keys.contains("PodName"), "kb.pod missing PodName=");
}

#[test]
fn pod_publishes_core_ports() {
    let content = read_quadlet_file("kb.pod");
    // At minimum: the API port 9999 + Postgres 5432 + Tika 9998.
    for port in &["9999", "5432", "9998"] {
        assert!(content.contains(port), "kb.pod should publish port {port}");
    }
}

// ── Volume files ─────────────────────────────────────────────────────────

#[test]
fn volume_files_have_volume_section() {
    for name in &["kb-pgdata.volume", "kb-app-data.volume"] {
        let content = read_quadlet_file(name);
        assert!(
            content.contains("[Volume]"),
            "{name} missing [Volume] section"
        );
    }
}

// ── Container files — common checks ──────────────────────────────────────

#[test]
fn container_files_have_required_sections() {
    for name in EXPECTED_FILES.iter().filter(|n| n.ends_with(".container")) {
        let content = read_quadlet_file(name);
        assert!(
            content.contains("[Container]"),
            "{name} missing [Container] section"
        );
        assert!(
            content.contains("[Service]"),
            "{name} missing [Service] section"
        );
        assert!(
            content.contains("[Install]"),
            "{name} missing [Install] section"
        );
    }
}

#[test]
fn container_files_have_image_key() {
    for name in EXPECTED_FILES.iter().filter(|n| n.ends_with(".container")) {
        let content = read_quadlet_file(name);
        let keys = section_keys(&content, "Container");
        assert!(
            keys.contains("Image"),
            "{name} missing Image= in [Container]"
        );
    }
}

#[test]
fn container_files_have_restart_policy() {
    for name in EXPECTED_FILES.iter().filter(|n| n.ends_with(".container")) {
        let content = read_quadlet_file(name);
        let keys = section_keys(&content, "Service");
        assert!(
            keys.contains("Restart"),
            "{name} missing Restart= in [Service]"
        );
    }
}

// ── Postgres-specific ────────────────────────────────────────────────────

#[test]
fn postgres_has_data_checksums() {
    let content = read_quadlet_file("kb-postgres.container");
    assert!(
        content.contains("data-checksums"),
        "kb-postgres.container should enable data-checksums (§23)"
    );
}

#[test]
fn postgres_has_shm_size() {
    let content = read_quadlet_file("kb-postgres.container");
    assert!(
        content.contains("ShmSize"),
        "kb-postgres.container should set ShmSize for HNSW index builds"
    );
}

#[test]
fn postgres_has_health_check() {
    let content = read_quadlet_file("kb-postgres.container");
    assert!(
        content.contains("HealthCmd"),
        "kb-postgres.container missing HealthCmd"
    );
}

// ── Tika-specific ────────────────────────────────────────────────────────

#[test]
fn tika_has_health_check() {
    let content = read_quadlet_file("kb-tika.container");
    assert!(
        content.contains("HealthCmd"),
        "kb-tika.container missing HealthCmd"
    );
}

// ── App-specific ─────────────────────────────────────────────────────────

#[test]
fn app_has_env_vars() {
    let content = read_quadlet_file("kb-app.container");
    let keys = section_keys(&content, "Container");
    assert!(
        keys.contains("Environment"),
        "kb-app.container missing Environment="
    );
}

#[test]
fn app_env_includes_postgres_url() {
    let content = read_quadlet_file("kb-app.container");
    assert!(
        content.contains("POSTGRES_URL="),
        "kb-app.container should set POSTGRES_URL"
    );
    assert!(
        content.contains("APP_POSTGRES_URL="),
        "kb-app.container should set APP_POSTGRES_URL"
    );
}

#[test]
fn app_env_includes_session_secret() {
    let content = read_quadlet_file("kb-app.container");
    assert!(
        content.contains("SESSION_SECRET="),
        "kb-app.container should set SESSION_SECRET"
    );
}

#[test]
fn app_has_volumes() {
    let content = read_quadlet_file("kb-app.container");
    assert!(
        content.contains("kb-app-data.volume"),
        "kb-app.container should mount kb-app-data.volume"
    );
    // Quadlet mounts the config read-only so the app can read the deployment config.
    assert!(
        content.contains("config.toml"),
        "kb-app.container should mount config.toml"
    );
}

#[test]
fn app_has_health_check() {
    let content = read_quadlet_file("kb-app.container");
    assert!(
        content.contains("HealthCmd"),
        "kb-app.container missing HealthCmd"
    );
}

// ── GPU services ─────────────────────────────────────────────────────────

#[test]
fn llama_has_gpu_device() {
    let content = read_quadlet_file("kb-llama.container");
    assert!(
        content.contains("AddDevice=nvidia.com/gpu=all"),
        "kb-llama.container should include AddDevice for GPU access"
    );
    assert!(
        content.contains("LLAMA_ARG_N_GPU_LAYERS=99"),
        "kb-llama.container should set LLAMA_ARG_N_GPU_LAYERS"
    );
}

#[test]
fn whisper_has_gpu_device() {
    let content = read_quadlet_file("kb-whisper.container");
    assert!(
        content.contains("AddDevice=nvidia.com/gpu=all"),
        "kb-whisper.container should include AddDevice for GPU access"
    );
}

#[test]
fn gpu_services_use_fully_qualified_images() {
    for name in &["kb-llama.container", "kb-whisper.container"] {
        let content = read_quadlet_file(name);
        // Registry images should be fully qualified (docker.io/…).
        assert!(
            content.contains("Image=docker.io/"),
            "{name} should use a fully qualified image name"
        );
    }
}

#[test]
fn core_services_use_fully_qualified_images() {
    for name in &["kb-postgres.container", "kb-tika.container"] {
        let content = read_quadlet_file(name);
        assert!(
            content.contains("Image=docker.io/"),
            "{name} should use a fully qualified image name"
        );
    }
}

// ── Pod membership ───────────────────────────────────────────────────────

#[test]
fn all_containers_belong_to_kb_pod() {
    for name in EXPECTED_FILES.iter().filter(|n| n.ends_with(".container")) {
        let content = read_quadlet_file(name);
        let keys = section_keys(&content, "Container");
        assert!(
            keys.contains("Pod"),
            "{name} missing Pod= in [Container] — it should join kb.pod"
        );
    }
}
