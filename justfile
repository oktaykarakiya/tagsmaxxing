# Quality gates and dev tasks for the Local File Knowledge Base.
# `just` to list recipes. The Definition-of-Done gate suite is `just ci` (plan §31.2/§31.3).

set shell := ["bash", "-euo", "pipefail", "-c"]

# Minimum line coverage enforced by `just cov` (plan §31.2/§31.3 target: >=85, higher on store/auth).
# RE-ARMED (review/p6-readiness pass): the gate had been lowered 85->84->80->79 AND neutered with
# a trailing `|| true` so it could never fail — a §31.2 violation ("never weaken a gate"). It is
# now a REAL enforced floor again, set to the current honest LINE level ~77% (NB: llvm-cov's first
# two summary %s are region/function coverage; LINES are the 3rd column — the `|| true` had been
# masking that real line coverage was below even the 79 it claimed). Measured 2026-05-31.
# DB-heavy crates (pg_store, session_store, B2) are largely covered by #[ignore]
# integration tests that don't run in the fast `just ci`; task P6-T0 adds a Podman-backed
# `just ci-integration` lane that runs them and restores the spec's >=85 with per-crate floors on
# kb-store + auth. Do NOT lower this floor to make a red gate pass — cover the code or run the
# integration lane.
cov_min := "76"

# List available recipes.
default:
    @just --list

# ── individual gates ─────────────────────────────────────────────────────────

# Format the whole workspace in place.
fmt:
    cargo fmt --all

# Fail if any file is unformatted.
fmt-check:
    cargo fmt --all --check

# Build everything, including tests/examples.
build:
    cargo build --workspace --all-targets

# Lint with zero tolerance for warnings.
clippy:
    cargo clippy --workspace --all-targets --all-features -- -D warnings

# Run the whole test suite (unit + integration + doctests).
test:
    cargo test --workspace --all-features

# RustSec security advisories.
# `cargo audit` scans the whole Cargo.lock literally, including OPTIONAL dependencies that are
# never enabled or compiled. RUSTSEC-2023-0071 (rsa, Marvin timing sidechannel — no fix
# available) is reachable only through `sqlx-mysql`, the MySQL driver we never enable (we are
# Postgres-only). `cargo deny check` (graph- and feature-aware) confirms it is absent from the
# real build graph, so this is a lockfile-superset false positive, not a shipped vulnerability.
# Scope: this single id only; `cargo deny`'s advisory gate stays fully strict.
audit:
    cargo audit --ignore RUSTSEC-2023-0071

# Licenses / bans / sources / advisories policy.
deny:
    cargo deny check

# Line coverage with an enforced floor (REAL gate — no `|| true` masking; review/p6-readiness).
cov: cov-run
    cargo llvm-cov --workspace --all-features --fail-under-lines {{cov_min}} --summary-only

# Run instrumented tests to produce coverage data.
cov-run:
    @cargo llvm-cov --workspace --all-features --no-report --fail-under-lines 0 2>/dev/null

# HTML coverage report for local inspection.
cov-html:
    cargo llvm-cov --workspace --all-features --html
    @echo "open target/llvm-cov/html/index.html"

# ── composite ────────────────────────────────────────────────────────────────

# Full Definition-of-Done suite. Must be green to mark a ledger task `done` (plan §31.2).
# Mirrors CI exactly, so "green locally" == "green in CI".
ci: fmt-check build clippy test deny audit cov
    @echo "✓ all gates passed"

# ── autonomous build loop (plan §31.1) ───────────────────────────────────────

# Print the next eligible ledger task id (or PHASE_COMPLETE / CHECKPOINT:<id>).
next:
    @python3 scripts/ledger.py next

# Show ledger status summary.
status:
    @python3 scripts/ledger.py status

# Run the autonomous build loop until a checkpoint, a blocked task, or phase completion.
# Pass approved checkpoints with: just loop --approve P1-T1
loop *ARGS:
    bash scripts/build-loop.sh {{ARGS}}

# Install the gate tools into CARGO_HOME/bin (idempotent; needed after a ramdisk reset).
bootstrap-tools:
    bash scripts/bootstrap-dev.sh
