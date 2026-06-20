# Migration Squash

## Decision

On 2026-06-20, the 27 forward-only migrations (0001–0027) were squashed into a single
`0001_base_schema.sql` that creates the full schema at its final state.

**Rationale:**
- Zero production deployments — no upgrade path to preserve.
- 27-migration bootstrap slower: 27 checksum verifications, 27 `_sqlx_migrations` inserts,
  no value from intermediate states.
- Migration 0023 had a `TRUNCATE TABLE chunks` with no idempotency guard — a re-run would
  destroy data. Eliminated by baking the final `VECTOR(2560)` columns and `halfvec`cast
  HNSW index directly into the base schema.
- 5 consecutive `DROP/ADD CONSTRAINT` cycles for `jobs.kind` CHECK collapsed into one.

## What changed

| Before | After |
|--------|-------|
| 27 `.sql` files, versions 1–27 | 1 `.sql` file, version 1 |
| `chunks.embedding` started as `VECTOR(1024)`, ALTERed to 2560 | Created as `VECTOR(2560)` |
| `tags.embedding` started as `VECTOR(1024)`, ALTERed to 2560 | Created as `VECTOR(2560)` |
| HNSW index started as `vector_cosine_ops`, rebuilt as `halfvec_cosine_ops` | Created with `halfvec_cosine_ops` from the start |
| `jobs.priority` started as `DEFAULT 100`, ALTERed to 0 | Created as `DEFAULT 0` |
| `jobs.priority` index started as ASC, rebuilt as DESC | Created as DESC |
| `jobs.kind` CHECK expanded across 5 migrations | Single CHECK with all 9 values |
| `settings` embedder row started as bge-m3/1024, UPDATEd to qwen3-4b/2560 | Seeded as qwen3-4b/2560 |
| 17 ALTER TABLE ADD COLUMNs | Folded into initial CREATE TABLE |
| Per-migration tests (13) | One comprehensive DDL lock-in test |

## Schema contents

A single file at `crates/store/migrations/0001_base_schema.sql` creates, in 7 ordered phases:

1. **Extension** — `CREATE EXTENSION IF NOT EXISTS vector`
2. **Helper function** — `app_set_current_tenant(BIGINT)`
3. **Root tables** (no dependent FKs) — `plans`, `tenants`, `providers`, `settings`,
   `stripe_events`, `audit_events`, `decrypt_audit`, `tenant_tombstones`
4. **Tables with FKs** — `users`, `documents`, `files`, `tags`, `tag_aliases`,
   `document_tags`, `chunks`, `jobs`, `usage_events`, `models`, `routes`, `sessions`,
   `tenant_data_keys`, `api_tokens`, `tenant_monthly_usage`
5. **RLS** — ENABLE + FORCE + `tenant_isolation` policy on 10 tenant-scoped tables
   (including the `EXISTS`-subquery variant for `document_tags`)
6. **kb_app role + grants** — cluster-global role guarded by `IF NOT EXISTS`;
   DML GRANT on 10 tenant tables; sequence + function grants
7. **Seed data** — embedder lock-in row (`qwen3-embed-4b`, 2560-dim);
   billing plans free/pro/team (idempotent via `ON CONFLICT DO NOTHING`)

Total: **23 tables**, **19 indexes**, **10 RLS policies**, **12 CHECK constraints**,
**4 grants**, **2 seed data categories**.

## Verification

**Compile-time**: `crates/store/src/migrations.rs` has 3 tests:
- `embeds_the_schema_migrations` — asserts exactly 1 migration at version 1
- `migrations_are_forward_only_and_strictly_increasing` — no down/reversible scripts,
  non-empty SQL with checksums
- `schema_locks_critical_ddl` — comprehensive test asserting all tables, CHECK values,
  indexes, RLS, kb_app role properties, FK relationships, seed data, and generated columns

**Runtime**: `crates/store/tests/migrations_pg.rs` verifies against a real
pgvector Postgres container (Podman):
- pgvector extension installed
- `VECTOR(2560)` on both `chunks.embedding` and `tags.embedding`
- RLS enabled + forced on 7 tables (the test skips some)
- `kb_app` role: `NOSUPERUSER NOBYPASSRLS`, can LOGIN
- Embedder settings row: `qwen3-embed-4b`, dim 2560
- Idempotent re-run

## Future migrations

When this project has deployments, new migrations start at version 2 following the
same forward-only convention:

```
crates/store/migrations/
  0001_base_schema.sql       <-- this file (do not modify)
  0002_new_feature.sql
  0003_another_change.sql
```

The `MIGRATOR` and `_sqlx_migrations` ledger work identically — they track applied
versions, skip already-applied ones.

## References updated

85 comment/doc-string references to specific migration numbers were updated across
~27 files to use descriptive names instead of numeric identifiers.
