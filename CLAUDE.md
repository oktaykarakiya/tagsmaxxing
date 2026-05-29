# CLAUDE.md — operating rules for this repository

You are building the **Local File Knowledge Base**. The complete, binding specification is
[`local-kb-plan.md`](./local-kb-plan.md). **Read [`§31`](./local-kb-plan.md) before doing
anything** — it defines *how* to build (this file summarizes it; the plan governs).

## The prime directive
Build strictly **one small ledger task at a time**, each fully verified before the next.
Code quality comes from the **compile → test → fix** loop, **never** from emitting many files
at once. Do **not** implement multiple tasks or a whole phase in one pass.

## The loop (plan §31.1)
The single source of progress is [`BUILD_LEDGER.toml`](./BUILD_LEDGER.toml). Each task:
1. Select the next `status = "todo"` task whose `depends_on` are all `done`
   (`just next` / `python3 scripts/ledger.py next`).
2. Read its `plan_sections` in `local-kb-plan.md` and the existing code it touches.
3. Implement it **with unit tests for every function carrying logic**.
4. Run **`just ci`** — ALL gates must pass.
5. Set the task's `status = "done"`, append a one-line `notes`, and **commit in one commit**
   whose message begins with the task id (e.g. `P0-T5: …`).
6. If the task has `checkpoint = true`, **STOP and request human review** — do not self-merge.

`scripts/build-loop.sh` (run via `just loop`) orchestrates this and independently re-runs the
gates. It is user-launched. It never writes code — you do.

## Definition of Done — `just ci` must be green (plan §31.2)
`just ci` runs, and all must pass on a clean checkout:
- `cargo build` (workspace) • `cargo fmt --check` • `cargo clippy --all-targets -- -D warnings`
  (zero warnings) • `cargo test` (whole workspace) • `cargo deny check` • `cargo audit`
  • `cargo llvm-cov` ≥ 85% lines (higher on crypto/auth/store).
- Every public item has a doc comment. **No stubs**: `todo!()`, `unimplemented!()`,
  `unreachable!()` in reachable paths, or hardcoded fake returns mean the task is **not done** —
  implement it or set the task `blocked` with a reason.

The workspace lints already enforce much of this (`missing_docs`, `unsafe_code`,
`clippy::unwrap_used`/`expect_used`/`todo`/`unimplemented` are deny). Don't relax them. In test
modules, `#![allow(clippy::unwrap_used, clippy::expect_used)]` is the accepted exception.

## Testing (plan §31.3)
- Every function with logic gets ≥1 colocated unit test (happy path + edge/error). Pure
  one-line delegators may rely on integration coverage.
- **TDD the hard pure logic**: scheduler acquire/spill/cooldown, token buckets, RRF fusion,
  tag-canonicalization threshold, quota math, envelope wrap/unwrap. Use `proptest` for wide
  inputs.
- **Mock-backend pattern** for the scheduler/llm (in-process HTTP server). **Integration
  tests** per phase (Postgres via testcontainers, MinIO for blobs).
- Deterministic only — inject clocks/timeouts; no real sleeps or network flakiness.

## Modularity (plan §31.4)
- One responsibility per module; module tree mirrors the workspace crates.
- File size: soft cap **~300 lines**, hard cap **~500**. No god-files. `lib.rs`/`mod.rs` stay
  thin (re-exports + wiring). Small functions, single level of abstraction.
- Depend on the **`kb-core` traits**, not concretions. Add each new capability
  (extractor, provider adapter, store, blob backend) as a **new module** implementing the
  relevant trait — never by enlarging an existing file.
- Errors via `thiserror` per crate; capability traits return `anyhow::Result`. No
  `unwrap()`/`expect()` outside tests.

## Mandatory human-review checkpoints (plan §31.5) — implement+test, but do NOT self-merge
Tenant isolation / RLS (ship a cross-tenant negative-test suite) • encryption & key handling •
auth & sessions • Stripe billing webhooks • the extractor untrusted-bytes/prompt-injection
boundary. Mark these tasks `checkpoint = true`; the loop stops for sign-off.

## Sequencing & commits (plan §31.6)
- **Walking skeleton first**: finish the P0–P4 vertical slice (ingest one text file → tag →
  embed → store → hybrid search → result) before breadth.
- One task = one commit; conventional message referencing the ledger id. Update the ledger in
  the same commit. End commit messages with:
  `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`
- **Verify crate versions/APIs against current sources before use** — the plan's names have
  drifted (e.g. thiserror 2.x, reqwest 0.13, sqlx 0.9, axum 0.8). Confirm before adding deps.
- Treat the plan as living: if implementation reveals a gap, note it in the ledger and surface
  it — don't silently diverge.

## Workspace facts
- Crates live in `crates/` and are named `kb-*` (avoids clashing with std `core`). Edition
  2024, resolver 3. Shared dep versions + lints are in the root `Cargo.toml`
  (`[workspace.dependencies]`, `[workspace.lints]`); add shared deps there and reference with
  `dep = { workspace = true }`.
- **The HTTP API serves on port `9999`** (default in config / `.env`).
- Recorded `kb-core` divergences from the plan sketches (intentional — do not "fix"):
  `TagOutput` has no `category` (§1/§6.5/§7.4); `PageImage` carries encoded bytes, not
  `image::DynamicImage` (keeps core codec-free); `ProviderAdapter` takes a core `ProviderConn`,
  not the scheduler's `Backend` (avoids a dependency cycle).

## Environment
- `CARGO_HOME` is on a **ramdisk** (`/mnt/ramdisk/cargo`); its `bin/` must be on `PATH` to run
  `just`. If gate tools vanish after a reboot, run `just bootstrap-tools` (or
  `bash scripts/bootstrap-dev.sh`). Toolchain is pinned to 1.92.0 in `rust-toolchain.toml`.
