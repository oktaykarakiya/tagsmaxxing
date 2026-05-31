# 06 — Integration-suite findings (exposed by the P6-T0 Podman lane)

**Status:** open — needs triage. **Discovered:** 2026-05-31, building ledger task **P6-T0**
(the re-armed CI + Podman integration lane). **Severity:** high (touches the §31.5 RLS
checkpoint and the walking-skeleton E2E).

## TL;DR

`just ci-integration` is the **first time the `#[ignore]` testcontainers suites have been run
as a whole** in this repo. They were authored across P3–P5 and gated `#[ignore]` so the fast
`just ci` stays green without a container runtime — but, as the memory note *"CI masks
security-test gap"* anticipated, **nothing actually ran them**. Running them now reveals two
separate facts:

1. **The coverage mechanism works and the security-critical code is well covered.** With the
   `#[ignore]` suites included, the P6-T0 floors all **pass** (measured against a real
   pgvector Postgres via Podman, `--test-threads=1`):

   | group    | lines | hit   | cover  | floor | status |
   |----------|-------|-------|--------|-------|--------|
   | overall  | 16220 | 15321 | 94.46% | 85.00 | PASS   |
   | kb-store | 4543  | 4201  | 92.47% | 85.00 | PASS   |
   | auth     | 560   | 533   | 95.18% | 85.00 | PASS   |

2. **The suite does not pass cleanly: ~38 of ~190 `#[ignore]` tests fail.** These are
   **pre-existing** — none are in P6-T0's scope (it only builds the lane that runs them). They
   split into genuine bugs and test-infrastructure flakiness (see below). Because the lane is
   `--no-fail-fast`, every failing test still executed enough code that the coverage above is
   representative; in the gate (`set -euo pipefail`) the failing test run aborts the lane
   **before** the coverage step, which is the correct behaviour — fix the tests, then the
   floors are enforced.

## Failure taxonomy (serial run, `--test-threads=1`, so not parallel contention)

### A. Confirmed real bugs (deterministic — fix in their own tasks)

| # | Symptom | Where | Likely cause |
|---|---------|-------|--------------|
| 1 | `assertion left == right failed: left: Queued, right: Running` | `pipeline/tests/job_queue_pg.rs:199` (`claim_returns_next_eligible_job`) | `JobQueue::claim()` (`job_queue.rs:160`) `SELECT … FOR UPDATE SKIP LOCKED`, then a separate `UPDATE … status='running'`, but returns `row_to_job(&r)` from the **pre-update** row → the returned `Job.status` is stale `Queued` while the DB row is correctly `running`. Fix: set the field on the returned struct (or `UPDATE … RETURNING`). |
| 2 | `mismatched types; Rust type i64 (INT8) is not compatible with SQL type NUMERIC` → `failed to query storage usage` | `pg_store` quota path (`pg_store::tests::quota_integration::*`) | A `SUM(bytes)`/quota column is `NUMERIC` but decoded as `i64`. Decode as `i64` via `::BIGINT` cast or use a `Decimal`/`i64` that matches the column type. |
| 3 | `expected 1024 dimensions, not 2` | `pg_store::tests::doc_ingest_integration::*` | A test/fixture inserts a 2-dim embedding into a `vector(1024)` column. Fixture or schema mismatch. |
| 4 | `column "job_type" of relation "jobs" does not exist` | jobs schema/query drift | A query references `job_type`; the column is `kind`. Migration/query drift. |
| 5 | `duplicate key value violates unique constraint "files_tenant_id_sha256_key"` | `transactional_ingest_*` | A test re-inserts the same `(tenant_id, sha256)`; either a test-isolation issue (reused fixture sha within a shared setup) or a real idempotency gap in transactional ingest. |

### B. Test-infrastructure flakiness (lane reliability — not code bugs)

- `pool timed out while waiting for an open connection` (×7) and `container is not ready:
  container startup timeout` (×3). Each `#[ignore]` test spins **its own** pgvector container
  via `setup()`; running ~150 of them — even serially — churns the Podman daemon hard enough
  that some container starts / first connections time out (~7% of the suite).
- `--test-threads=1` (added to the lane in P6-T0) removes the *parallel* exhaustion (the
  parallel run failed ~35; serial fails on the same real bugs plus residual churn timeouts),
  but does not eliminate churn-induced flakiness. The durable fix is a **test-infra** change
  (out of P6-T0 scope): share one container per test binary (a `OnceCell`/static pool), or add
  startup ret/backoff. Track separately.

### C. Cross-tenant RLS isolation — **must re-verify (§31.5)**

The P5-T2 cross-tenant negative tests (`tenant_b_cannot_read_tenant_a_*`, `*_are_tenant_scoped`,
`each_tenant_only_sees_own_data_in_search`, `hybrid_search_is_tenant_scoped`) and the P5 E2E
isolation test are **among the failures**. The errors that reach them are dominated by the
category-A/B causes above (e.g. `failed to set app.current_tenant for RLS`, connection/pool
timeouts, the `NUMERIC`/dim/`job_type` errors in shared setup), i.e. the tests appear to fall
over in **setup/ingest** before exercising the isolation assertion — *not* an observed
cross-tenant read. **However**, isolation is a §31.5 security checkpoint: it cannot be assumed.
Once A/B are fixed, this suite **must be made green and signed off** to re-affirm that RLS
prevents cross-tenant access. Treat as a checkpoint.

## Recommended follow-up

Triage task added to `BUILD_LEDGER.toml` (see the task right after P6-T0). Suggested split:
fix the discrete real bugs (1–5) as small commits; make the testcontainers harness reliable
(B); then re-run `just ci-integration` until green and obtain §31.5 sign-off on the
cross-tenant isolation suite (C). Until then the integration lane is correctly **red** — that
is the lane doing its job.

> **Update (P6-T13, 2026-05-31):** triaging **part C** found that RLS is **not actually
> enforced** — the connection role `kb` is a superuser with `BYPASSRLS`, and even under a
> non-superuser the store sets the tenant GUC on a *separate* round-trip from the query so it
> reverts (deny-all); the job queue is intrinsically cross-tenant. Part C therefore cannot be
> made *genuinely* green without a connection-role redesign — a §31.5 human decision. P6-T13
> is **blocked** on it. Discrete bugs **1–4** are fixed + verified; bug **5** and harness
> reliability (B) fold into that rework. Full proof + design options:
> [`07-rls-enforcement-blocker.md`](./07-rls-enforcement-blocker.md).
