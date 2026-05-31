# 07 — RLS is not actually enforced (P6-T13 §31.5 blocker)

**Status:** ✅ RESOLVED in **P6-T14** (the connection-role rework). The two-role model below was
implemented and the cross-tenant isolation suite now passes **under the non-superuser `kb_app`
role** — see the *Resolution* section at the end. The original blocker write-up is kept intact
for the record. **Discovered:** 2026-05-31, building ledger task **P6-T13** (triage the
`#[ignore]` integration-suite failures the P6-T0 Podman lane exposed, incl. the part-C
cross-tenant RLS re-verification). **Severity:** critical (tenant isolation / §13 / §31.5).

## TL;DR

P6-T13 was scoped as "≈38 failing tests / 5 small bugs + harness reliability, then make the
cross-tenant RLS isolation suite green and obtain §31.5 sign-off." Triaging part C revealed
that **Row-Level Security is not actually enforced anywhere in the current design**, and the
isolation suite therefore *cannot be made genuinely green* without a security-critical,
cross-cutting change to how the application connects to Postgres. Concretely, three facts —
each verified empirically below against a real pgvector container — combine so that RLS is a
no-op today:

1. **The connection role `kb` is a Postgres superuser with `BYPASSRLS`.** Superusers bypass
   RLS unconditionally; `FORCE ROW LEVEL SECURITY` (migration `0003_rls.sql`) only reaches the
   table *owner*, not a superuser. Every `#[ignore]` suite connects as `kb`, so RLS is off and
   the isolation assertions (`tenant_b ... sees 0 rows`) would **fail**, not pass, if they
   reached the assertion.
2. **The tenant GUC does not survive to the query that needs it.** `set_tenant(&pool)` runs
   `SELECT app_set_current_tenant($1)` (which calls `set_config('app.current_tenant', …,
   true)` — *transaction-local*) on the pool in autocommit mode, then the actual query runs as
   a **separate** round-trip. The local setting reverts when the `set_config` statement's
   implicit transaction commits, so the subsequent query sees `app.current_tenant = NULL` →
   deny-by-default → **zero rows**. Under a non-superuser this makes the store's own legitimate
   queries return nothing.
3. **The job queue is intrinsically cross-tenant and cannot run under per-tenant RLS.**
   `JobQueue::claim()` scans `jobs` across *all* tenants and sets no tenant GUC at all; under a
   non-superuser RLS it would be denied every row and the worker pool could never claim a job.
   So the queue/admin paths *need* a privileged (BYPASSRLS) connection while tenant-data paths
   *need* a non-superuser RLS-enforced connection — a two-role model.

Making the §31.5 isolation suite truly green requires deciding and implementing the DB
**connection-role model** and reworking the store's per-connection tenant-GUC handling. That
is a foundational, security-critical change spanning `kb-store` (≈20 methods), `kb-pipeline`
(queue), the API/config, `compose.yaml`, and the migrations — well beyond "5 small bugs," and
exactly the kind of change §31.5 reserves for human review. **It must not be auto-signed-off
or made *falsely* green** (e.g. by keeping the superuser connection or weakening the isolation
assertions — that would ship a cross-tenant data-leak path behind a green checkpoint). Hence
P6-T13 is set `blocked`.

## Evidence (real pgvector `pg17` via Podman; migrations `0001`–`0005` applied)

Role of the `kb` connection user (the one every suite uses):

```text
$ psql -U kb -d kb -tAc "SELECT rolname,rolsuper,rolbypassrls FROM pg_roles WHERE rolname='kb';"
kb|t|t                       # rolsuper = t, rolbypassrls = t  → RLS bypassed
```

**Probe 1 — superuser bypasses RLS (cross-tenant read succeeds):** insert a document as
tenant 1, then as tenant 2 count documents.

```text
SELECT app_set_current_tenant(1); INSERT INTO documents(tenant_id,title,kind,status)
    VALUES (1,'SecretA','document','ready');
SELECT app_set_current_tenant(2); SELECT count(*) FROM documents;
=> 1                         # tenant 2 SEES tenant 1's row → isolation NOT enforced
```

**Probe 2 — `set_config(…, true)` does not persist across separate round-trips:**

```text
# two separate statements/sessions:
SELECT app_set_current_tenant(1);
SELECT current_setting('app.current_tenant', true);   => <NULL>   # reverted
```

**Probe 3 — under a non-superuser role `app` (NOSUPERUSER NOBYPASSRLS), RLS *does* work, and
the store's split pattern breaks legitimate access:**

```text
# 3a: same transaction, tenant 2 → correctly isolated
BEGIN; SELECT app_set_current_tenant(2); SELECT count(*) FROM documents; COMMIT;  => 0
# 3b: same transaction, tenant 1 → sees its own row
BEGIN; SELECT app_set_current_tenant(1); SELECT count(*) FROM documents; COMMIT;  => 1
# 3c: BROKEN pattern — set tenant, then query in a SEPARATE round-trip (GUC reverted)
SELECT app_set_current_tenant(1);  (round-trip 1)
SELECT count(*) FROM documents;    (round-trip 2)                                 => 0
```

Probe 3 is the crux: RLS is correct *only* when the tenant GUC and the query share one
connection+transaction. The store sets the GUC on the pool and queries separately (probe 3c),
so under the role that actually enforces RLS, every store read/write would be denied.

## Why the listed "5 bugs" don't resolve part C

Bugs 1–4 (below) are genuine and fixed in this commit, but none of them makes the isolation
suite *meaningful*: the isolation tests (`tenant_b_cannot_read_tenant_a_*`,
`jobs_are_tenant_scoped`, `each_tenant_only_sees_own_data_in_search`,
`hybrid_search_is_tenant_scoped`, and the P5 E2E) assert that the *other* tenant sees **0**
rows. As long as the suite connects as the `kb` superuser, those assertions fail because RLS
is bypassed (probe 1). Switching the suite to a non-superuser role then exposes probe 3c: the
store (and the raw-SQL `switch_tenant` helper) must be reworked to keep the tenant GUC on the
query's own connection/transaction, or every legitimate query returns nothing.

## Required design decision (for the human reviewer)

1. **Connection-role model.** Recommended: two roles.
   - `kb_app` — `LOGIN NOSUPERUSER NOBYPASSRLS`, owns nothing, granted DML on the tenant
     tables; used for all tenant-scoped data operations so RLS is enforced (defense-in-depth
     even if a query forgets its `WHERE tenant_id`).
   - `kb_admin`/`kb_worker` — privileged (superuser or `BYPASSRLS`); used for migrations, the
     cross-tenant job queue, and admin/usage roll-ups.
   This must be wired through config (the DB URL/role is operator-set and should be
   hot-swappable per CLAUDE.md), `compose.yaml`, and the testcontainers harness (which today
   relies on `kb` being a superuser to insert seed/tenant rows).
2. **Per-connection tenant GUC.** Rework `set_tenant`/the store so the GUC and the query run
   on the *same* connection within the *same* transaction (e.g. acquire a connection, `BEGIN`,
   `app_set_current_tenant`, run queries, `COMMIT`; or a session-level GUC on a held connection
   with a reset-on-release guard so a pooled connection never leaks one tenant's GUC to the
   next checkout — itself a cross-tenant hazard).
3. **Queue/admin exemption.** Confirm the queue/admin paths use the privileged role (or
   dedicated GUC handling) so cross-tenant scans keep working.

This is foundational and security-critical (it also affects the production deployment's DB
auth), so it is surfaced for a human decision rather than chosen unilaterally in an autonomous
overnight commit. Recommended follow-up: the reviewer decides the role model, then a dedicated
task implements it (store + queue + config + compose + harness) and re-runs the isolation
suite to green *under the non-superuser role* — the only way the §31.5 sign-off is real.

## What P6-T13 delivered vs. what remains

**Fixed + verified in this commit (discrete real bugs, role-independent):**

- **Bug 1** — `JobQueue::claim()` returned the *pre-update* row (stale `Queued`). Rewritten as
  a single atomic `UPDATE jobs SET status='running' WHERE id = (SELECT … FOR UPDATE SKIP
  LOCKED) RETURNING …` so the returned `Job.status` is the authoritative `Running`.
  *Verified:* `claim_returns_next_eligible_job` green against real Postgres.
- **Bug 2** — `get_storage_usage` decoded `SUM(size_bytes)` (Postgres types `SUM(bigint)` as
  `NUMERIC`) into `i64` and failed. Cast the aggregate `::BIGINT`. *Verified:* `quota_integration`
  green.
- **Bug 3** — tag fixtures inserted 2–3-dim embeddings into `VECTOR(1024)` (`expected 1024
  dimensions, not N`). Added a unit-tested `emb1024()` helper that pads seeds to 1024 and routed
  the `#[ignore]` `upsert_tag` fixtures through it. *Verified:* `tag_integration` green.
- **Bug 4** — `jobs_are_tenant_scoped` inserted into non-existent `jobs.job_type` / `payload`
  columns. Fixed to `(tenant_id, kind, status)`. (The query is now valid; its *isolation*
  assertion still depends on the RLS rework above.)

**Remaining (blocked):**

- **Bug 5** — `p4_integration::create_test_document` sets `files.sha256 = title.as_bytes()` and
  inserts with **no `ON CONFLICT`**, so two files sharing a (short) title collide on
  `files_tenant_id_sha256_key`. Fix is a fixture-uniqueness change (derive sha per file, or add
  `ON CONFLICT`) — folded into the harness rework since those test files are rewritten for the
  non-superuser role model anyway.
- **Test-infra reliability (finding 06-B)** — sharing one container per binary (fresh DB per
  test) vs. per-test containers should be decided together with the role model, since the
  harness seeding currently depends on the superuser connection.
- **The RLS connection-role redesign + §31.5 re-verification** — the core blocker above.

See also [`06-integration-suite-findings.md`](./06-integration-suite-findings.md) (the P6-T0
lane that first ran these suites).

## Resolution (P6-T14, 2026-05-31)

The recommended two-role model was implemented and **verified on real pgvector via Podman**.
RLS is now genuinely enforced for the application's own queries.

**1. Two roles (migration `0006_app_role.sql`).** `kb_app` is created `LOGIN NOSUPERUSER
NOBYPASSRLS`, owns nothing, and is granted only DML on the nine tenant-scoped tables (+ sequence
usage + EXECUTE on `app_set_current_tenant`) — explicitly **not** on `tenants` / `sessions` /
`settings`. The bootstrap superuser stays the privileged role for migrations, the job queue, and
admin/usage roll-ups.

**2. Two pools, per-transaction GUC (`kb-store`).** `PgStore` now holds an `admin_pool`
(privileged) and an `app_pool` (`kb_app`). Every tenant-scoped method (~20 of them) and
`hybrid_search` run through `begin_tenant_tx(app_pool, tenant)`, which `BEGIN`s a transaction and
sets `app.current_tenant` **transaction-locally on that same connection**, so the GUC and the
query share one connection and the setting is discarded on commit (no leak across pooled
checkouts — the Probe 3c failure mode is gone). Cross-tenant paths (`create_tenant`,
`tenant_count`, quotas' tenant-limit lookup, the queue) use the admin pool. Both URLs are wired
through `kb-config` (`storage.app_postgres_url` / `APP_POSTGRES_URL`, hot-swappable) and
`compose.yaml`/`.env.example`.

**3. Verified.** The cross-tenant suite was rewritten to seed via the privileged pool and run
every tenant-data assertion as `kb_app` inside a tenant transaction. **doc-07 Probe 1 now returns
0** (`tenant_b_cannot_read_tenant_a_document`), and all nine isolation tests + the P5 E2E
isolation test pass under `kb_app`. `migrations_pg` additionally asserts `kb_app` is
`NOSUPERUSER`/`NOBYPASSRLS`.

**4. Bug 5 + harness (finding 06-B).** Fixed by deriving a unique `sha256` per seeded file
(p4 + the hybrid_search fixture). Harness reliability solved by the new `kb-testsupport` crate:
**one pgvector container per test binary** (a `OnceCell`) with a **fresh database per test** and
the container's `max_connections` raised — eliminating per-test container churn. (Subtlety found
+ fixed: a sqlx pool cannot be shared across the separate tokio runtimes that `#[tokio::test]`
creates, so each `fresh_db()` opens its own short-lived maintenance connection.)

The §31.5 sign-off is therefore based on the isolation suite passing **under the role that
actually enforces RLS**, not under the superuser.
