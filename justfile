# Quality gates and dev tasks for the Local File Knowledge Base.
# `just` to list recipes. The Definition-of-Done gate suite is `just ci` (plan §31.2/§31.3).

set shell := ["bash", "-euo", "pipefail", "-c"]

# Minimum line coverage enforced by `just cov` (plan §31.3).
cov_min := "85"

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
audit:
    cargo audit

# Licenses / bans / sources / advisories policy.
deny:
    cargo deny check

# Line coverage with an enforced floor.
cov:
    cargo llvm-cov --workspace --all-features --fail-under-lines {{cov_min}}

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
