# Code Quality Audit

**State as of 2026-05-31** (point-in-time; gates re-run on this date). Authoritative ledger:
40/44 tasks done, parked at the P5-T5 auth checkpoint.

## Verdict
High-quality and disciplined. `just ci` is green on a clean checkout. The architecture is
genuinely trait-based (not cosmetic), there are zero stubs, and pure-logic modules are
exhaustively tested. The one substantive concern is that the **security-critical tests
(cross-tenant RLS, auth) exist but do not run in the default CI gate** — they are `#[ignore]`d
behind a real Postgres/Podman.

## Gate-by-gate (all re-run 2026-05-31)
| Gate | Result |
|---|---|
| `cargo fmt --check` | ✅ clean |
| `cargo build` / `clippy --all-targets -- -D warnings` | ✅ **0 warnings** |
| `cargo test --workspace` | ✅ **709 passed, 0 failed**, 72 ignored |
| `cargo deny check` | ✅ advisories / bans / licenses / sources ok |
| `cargo audit` (scoped ignore) | ✅ — see note below |
| `cargo llvm-cov` | ✅ **86.29% lines** (86.50% fn, 80.38% region) |

Toolchain pinned to Rust 1.92.0 (`rust-toolchain.toml`).

### The `cargo audit` finding is a non-issue, handled well
Raw `cargo audit` flags **RUSTSEC-2023-0071** (`rsa` crate — Marvin timing side-channel,
*no fix available*). `rsa` is only reachable through the `sqlx-mysql` driver, which this project
never enables. The gate scopes an ignore for exactly that ID with a written rationale in the
`justfile`, and graph-aware `cargo deny` confirms `rsa` is absent from the real build graph.
This is correct engineering judgment, not a shortcut. (`sqlx` is deliberately pinned to 0.8.6
because 0.9 raises MSRV to rustc 1.94 > the pinned 1.92 — another example of disciplined,
documented dependency decisions.)

## Architecture
- **9 crates**, clean dependency inversion. Eight capability traits live in `kb-core`:
  `Extractor, Tagger, Embedder, Reranker, Store, Blob, ProviderAdapter, SessionStore`.
  Concretions depend on traits, matching the plan's modularity rules (§31.4).
- **26.3k lines** of Rust across 76 files.
- Module tree mirrors the workspace crates; `lib.rs`/`mod.rs` stay thin.

## Strengths
- **No rot:** 0 `todo!`/`unimplemented!`/`unreachable!`, **0 `unwrap`/`expect` outside tests**,
  0 `TODO`/`FIXME` comments. `panic!` appears only in test assertions and one mock-fixture bind.
- **Docs complete** on public items (`missing_docs` + `-D warnings` makes it a hard gate).
- **Test density is excellent where it counts** — pure logic is 96–100% covered:
  `metadata_merge` 100%, `chunker` 99.7%, `document_builder` 99.5%, `retrieval` 98.5%,
  `tag_canonicalizer` 96.1%, `scheduler/pool` 97.3%, `mock-backend` 97.2%.
- **Process discipline holds:** one task = one commit with a conventional `P#-T#:` prefix;
  the ledger is updated in the same commit.
- **File-size "violations" are mostly false alarms** — files appear large only because unit
  tests are colocated. By *code* lines (excluding `#[cfg(test)]`), nothing exceeds ~481 and only
  4 files mildly pass the 300-line soft cap. No god-files.

## Weaknesses / risks (prioritized)
1. **The security surface is the least-covered, and its tests are excluded from CI.**
   `pg_store.rs` 35.9%, `hybrid_search.rs` 41.0%, `job_queue.rs` 35.0%, `session_store.rs`
   57.3% — all the Postgres/RLS/auth/SQL paths. Their tests (including the **9-test
   cross-tenant isolation suite**, a mandated checkpoint deliverable per §31.5) are `#[ignore]`d
   and only run against real Postgres + Podman. Net effect: **tenant isolation and auth are
   validated only on demand, never automatically.** For a multi-tenant boundary this is the #1
   risk. → Add a Podman-backed CI lane that actually executes the ignored suites.
2. **The coverage gate contradicts the spec where it matters most.** The Definition of Done
   (§31.2) says "≥85% lines, *higher on crypto/auth/store*," but the `justfile` sets
   `cov_min := "80"`, and store/auth are the *lowest*-covered files. Today's 86.29% clears even
   the 85 bar, but the gate is set 5 points soft, inverting the stated priority.
   → Raise `cov_min` to 85; consider per-crate floors on `store`/auth.
3. **Minor:** `extract/src/video.rs` (~481 code lines) and `pipeline/src/ingest.rs` (~418) are
   creeping toward the cap — future split candidates.

## Bottom line
Well above typical agent-generated quality: honest gates, sound architecture, visible discipline
in every commit. The next thing to fix before trusting it in production is operational, not
stylistic — make the multi-tenant RLS and auth tests **run automatically**, and tighten the
coverage gate to match the project's own 85% rule. Fittingly, the build is parked exactly at the
**P5-T5 auth checkpoint** — the surface this audit flags as highest-risk.
