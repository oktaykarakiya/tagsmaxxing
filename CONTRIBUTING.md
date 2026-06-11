# Contributing to the Local File Knowledge Base

## Development environment

### Prerequisites

- **Rust** 1.92.0 (pinned via `rust-toolchain.toml` — `rustup` picks this up automatically)
- **Podman** (rootless) for the sidecar stack and integration tests
- The gate tools installed in `CARGO_HOME/bin`

```bash
# Install / restore the gate tools (just, cargo-deny, cargo-audit, cargo-llvm-cov)
just bootstrap-tools        # or: bash scripts/bootstrap-dev.sh
```

> **Note:** `CARGO_HOME` may live on a ramdisk (`/mnt/ramdisk/cargo`). If tools vanish
> after a reboot, re-run `just bootstrap-tools`. The toolchain is pinned to 1.92.0 in
> `rust-toolchain.toml`.

### Start the sidecar stack

Postgres (with pgvector) and Apache Tika run in Podman containers:

```bash
podman compose up -d         # start sidecars (healthchecks pass in ~9s)
podman compose ps            # verify both are "healthy"
# App runs on the host against these sidecars for development
podman compose down -v       # tear down and drop data volume
```

The app (once built) will serve on **port 9999** by default. GPU inference (llama.cpp,
whisper.cpp) is a separate profile — see `compose.gpu.yaml`.

## Quality gates — `just ci`

The Definition of Done is `just ci` — all gates must pass before a change is committed
(plan §31.2). It mirrors CI exactly:

```bash
just ci   # fmt-check → build → clippy -D warnings → test → deny → audit → cov (≥76% lines)
```

Individual gates:

| Command | What it checks |
|---------|---------------|
| `just fmt` / `just fmt-check` | Rustfmt formatting |
| `just build` | `cargo build --workspace --all-targets` |
| `just clippy` | `cargo clippy --workspace --all-targets -D warnings` — **zero warnings** |
| `just test` | `cargo test --workspace --all-features` (fast suite, no testcontainers) |
| `just deny` | `cargo deny check` — license allow-list, unknown sources, advisories |
| `just audit` | `cargo audit` — RustSec advisory database |
| `just cov` | `cargo llvm-cov` with enforced floor (currently 76% lines) |
| `just cov-html` | HTML coverage report for local inspection |

### Integration lane — `just ci-integration`

The fast `just test` skips `#[ignore]` tests (testcontainers suites that need a real
Postgres). Run the full suite with:

```bash
just ci-integration   # requires a running rootless Podman socket
```

This enforces higher coverage floors: ≥85% lines overall, with ≥85% on the
security-critical `kb-store` crate and auth paths.

## Test patterns

### Unit tests — pure logic

Colocated with the implementation, no I/O: `#[cfg(test)] mod tests { … }` in each source
file. Every function carrying logic gets ≥1 test (happy path + edge/error). One-line
delegators may rely on integration coverage.

### Mock-backend pattern (plan §31.3)

The `kb-mock-backend` crate provides an in-process axum server on `127.0.0.1:0` with a
programmable `Scenario` handle. Tests mutate the scenario between calls to simulate
healthy, unhealthy, slow, 5xx, and 429 responses. This makes scheduler and LLM client
tests **deterministic** — no real network, no flakiness.

```rust
use kb_mock_backend::{MockBackend, Scenario, ResponseMode};

let backend = MockBackend::start().await;
backend.scenario().set_health(ResponseMode::Unhealthy);  // simulate a dead host
// … test acquire() behaviour …
```

### Integration tests — testcontainers + Podman

Tests that need a real Postgres are placed in `tests/` directories, marked `#[ignore]`,
and run via `just ci-integration`. The `kb-testsupport` crate provides a shared harness:
one pgvector container per test binary, fresh database per test, admin + app role URLs.

```rust
use kb_testsupport::TestDb;

let db = TestDb::new().await;        // one container per binary
let admin_pool = db.admin_pool().await;  // for seeding
let app_pool = db.app_pool().await;      // for RLS-enforced assertions
```

### Proptest — wide inputs

For hard pure logic (scheduler acquisition, RRF fusion, tag-canonicalization threshold,
quota math), use `proptest` to cover a wide input space.

### Determinism

All tests inject clocks/timeouts so they are deterministic. No real sleeps, no network
flakiness. The mock-backend pattern eliminates the only non-deterministic external
dependency (the LLM server).

## Autonomous build loop

Development is **ledger-driven** (plan §31.1): one small task at a time, each fully
verified before the next. Progress is tracked in [`BUILD_LEDGER.toml`](./BUILD_LEDGER.toml).

```bash
just status                 # ledger summary + next eligible task
just next                   # just the next task id
just loop                   # run the loop until checkpoint / blocked / phase complete
just loop --max 1           # do exactly one task
just loop --approve P1-T1   # clear a human-review checkpoint
just loop --dry-run         # show next task without invoking anything
```

The loop (a bash script at `scripts/build-loop.sh`) works per-task:
1. Pick the next `status = "todo"` task whose dependencies are all `done`.
2. Invoke Claude Code with the task's `plan_sections` + existing code.
3. Independently re-run `just ci` and confirm it's green.
4. Set `status = "done"`, append a one-line note, and commit.
5. If `checkpoint = true`, stop for human review.

**Never implement multiple tasks in one pass.** See [`CLAUDE.md`](./CLAUDE.md) for the
full protocol — it governs how this project is built.

### Checkpoints (§31.5)

Certain tasks carry `checkpoint = true` — these are security-critical (tenant isolation,
encryption, auth, Stripe billing, extractor untrusted-bytes/prompt-injection boundary).
The loop stops after implementing them and **requires human sign-off** before continuing.
Do not self-merge a checkpoint task.

## Project standards

### Modularity (plan §31.4)

- One responsibility per module; module tree mirrors the workspace crates.
- File soft cap **~300 lines**, hard cap **~500**. No god-files.
- `lib.rs`/`mod.rs` stay thin (re-exports + wiring).
- Depend on `kb-core` traits, not concretions.
- Add new capabilities as a **new module** implementing a trait — never enlarge an existing file.

### Code quality

- Every public item has a doc comment (`#![warn(missing_docs)]`).
- **No stubs**: `todo!()`, `unimplemented!()`, `unreachable!()` in reachable paths, or
  hardcoded fake returns are denied by lint (`#![deny(clippy::todo, clippy::unimplemented)]`).
- **No naked unwraps**: `#![deny(clippy::unwrap_used, clippy::expect_used)]`. In test
  modules, `#![allow(clippy::unwrap_used, clippy::expect_used)]` is the accepted exception.
- Errors via `thiserror` per crate; capability traits return `anyhow::Result`.
- `#![deny(unsafe_code)]` workspace-wide.

### Hot-swappable configuration

Runtime parameters (model endpoints, API keys, model IDs, rate limits, quotas) must be
hot-swappable — changeable without a restart — wherever it makes sense and is genuinely
useful. Hold mutable config behind `arc_swap::ArcSwap` and read it **per call** via
`config.current()`. See the standing design rule in `CLAUDE.md`.

### Commit messages

One task = one commit. Format: `P7-T5: short description of what changed`. End with:
```
Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
```

## Contributor License Agreement

All contributions are accepted under the terms of the [Contributor License Agreement
(CLA)](./CLA.md). Before your first PR is merged, you must sign the CLA — this grants
the Project a copyright and patent license for your contributions and enables the
dual-licensing model (AGPLv3 open source + commercial license).

**How to sign:**
- Comment **`/cla-signed`** on your first PR, or
- Email a signed copy of [`CLA.md`](./CLA.md) to the Maintainer.

By signing, you confirm that You have read and agree to the CLA terms. The CLA is based
on the Apache Software Foundation Individual CLA v2.0 — it is perpetual, non-exclusive,
and royalty-free. Crucially, it allows the Project to relicense Your Contribution under
different terms (enabling the commercial license option for enterprises).

If you submit a PR without signing the CLA, your contribution is accepted under the
project's default inbound license (**AGPL-3.0-or-later only**) — this means it can be
used in the open-source edition but **cannot** be incorporated into the commercial
license offering. Please sign the CLA so your contribution benefits both tracks.

## PR process

1. **Sign the CLA** — comment `/cla-signed` on your first PR (see above).
2. **Create a branch** from `main`.
3. **Implement** following `CLAUDE.md` and the test patterns above.
4. **Run `just ci`** — all gates must be green.
5. If your change adds or modifies DB code, also run **`just ci-integration`** against
   a running Podman socket to ensure the integration suites pass.
6. **Update `BUILD_LEDGER.toml`** — set the task `status = "done"` and append a one-line
   `notes` entry.
7. **Push and open a PR.** CI runs the same gates on push/PR. A reviewer checks for
   correctness, test coverage, and adherence to the architectural patterns.
8. **Merge** once approved and CI is green. Prefer squash-merge to keep `main` linear.

## Generating an SBOM

A Software Bill of Materials can be generated via `cargo cyclonedx`:

```bash
# Install once
cargo install cargo-cyclonedx

# Generate a CycloneDX SBOM (JSON or XML)
cargo cyclonedx --format json --output-file sbom.cdx.json
```

This produces a complete inventory of every Rust dependency with version, licence, and
provenance metadata. SBOM generation is also available as `just sbom` and is intended as
a CI artifact.
