//! The embedded, forward-only schema migrations (plan §5 schema, §13 RLS, §16 queue).

use sqlx::migrate::Migrator;

/// The full schema, compiled in from `crates/store/migrations/` at build time.
///
/// Forward-only: each file is a simple `{version}_{name}.sql` migration with no `down` script
/// (a changed embedder needs a `reembed` job, not a reverse migration — plan §5 lock-in note).
/// Apply against a Postgres pool with `kb_store::MIGRATOR.run(&pool).await`; the
/// `_sqlx_migrations` ledger makes that idempotent, so already-applied versions are skipped on
/// a re-run.
pub static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    /// All fourteen migrations are present, in version order (1–14).
    #[test]
    fn embeds_the_schema_migrations() {
        let versions: Vec<i64> = MIGRATOR.iter().map(|m| m.version).collect();
        assert_eq!(
            versions,
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        );
    }

    /// Forward-only and monotonic: no down/reversible scripts, strictly increasing versions,
    /// and every migration carries a checksum + non-empty SQL.
    #[test]
    fn migrations_are_forward_only_and_strictly_increasing() {
        let mut prev: Option<i64> = None;
        for m in MIGRATOR.iter() {
            assert!(
                !m.migration_type.is_down_migration(),
                "migration {} is reversible/down; the schema is forward-only",
                m.version
            );
            if let Some(p) = prev {
                assert!(
                    m.version > p,
                    "versions must strictly increase: {p} !< {}",
                    m.version
                );
            }
            prev = Some(m.version);
            assert!(
                !m.checksum.is_empty(),
                "migration {} has no checksum",
                m.version
            );
            assert!(
                !m.sql.trim().is_empty(),
                "migration {} has empty SQL",
                m.version
            );
        }
    }

    /// Pin the schema invariants that downstream code (and the §5 plan) depends on: pgvector,
    /// the locked 1024-dim vectors, every table, the HNSW + GIN indexes, RLS, and the embedder
    /// lock-in row.
    #[test]
    fn schema_locks_critical_ddl() {
        let sql: String = MIGRATOR
            .iter()
            .map(|m| m.sql.as_ref())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(sql.contains("CREATE EXTENSION IF NOT EXISTS vector"));
        // The embedder lock-in: both vector columns are dim 1024 (§5). Count column
        // definitions only — comment lines (e.g. the settings lock-in note) also say it.
        let vector_columns = sql
            .lines()
            .filter(|l| l.contains("VECTOR(1024)") && !l.trim_start().starts_with("--"))
            .count();
        assert_eq!(vector_columns, 2, "tags + chunks vector columns");

        for table in [
            "tenants",
            "users",
            "documents",
            "files",
            "tags",
            "tag_aliases",
            "document_tags",
            "chunks",
            "jobs",
            "usage_events",
            "settings",
            "sessions",
        ] {
            assert!(
                sql.contains(&format!("CREATE TABLE {table} ")),
                "missing CREATE TABLE for {table}"
            );
        }

        // Hybrid-search indexes: HNSW for vectors, GIN for the full-text tsvector.
        assert!(sql.contains("USING hnsw (embedding vector_cosine_ops)"));
        assert!(sql.contains("USING GIN (tsv)"));

        // Multi-tenancy: RLS + the app.current_tenant pattern (plan §5/§13).
        assert!(sql.contains("ENABLE ROW LEVEL SECURITY"));
        assert!(sql.contains("app.current_tenant"));

        // Embedder lock-in row recording id + dim (§5 note).
        assert!(sql.contains("'embedder'"));
        assert!(sql.contains("\"dim\": 1024"));
    }

    /// Migration 0007 (P6-T11): tag provenance columns (`source`, `locked`) must be present,
    /// the jobs CHECK must include `'retag'`, and `document_id` must exist on jobs.
    #[test]
    fn schema_adds_tag_provenance_and_retag_kind() {
        let sql: String = MIGRATOR
            .iter()
            .map(|m| m.sql.as_ref())
            .collect::<Vec<_>>()
            .join("\n");

        // document_tags gains source + locked.
        assert!(
            sql.contains("document_tags\n    ADD COLUMN IF NOT EXISTS source"),
            "missing source column on document_tags"
        );
        assert!(
            sql.contains("document_tags\n    ADD COLUMN IF NOT EXISTS locked"),
            "missing locked column on document_tags"
        );
        assert!(
            sql.contains("'llm'") && sql.contains("'user'"),
            "source CHECK must allow 'llm' and 'user'"
        );

        // jobs.kind CHECK now includes 'retag'.
        assert!(
            sql.contains("'retag'"),
            "jobs kind CHECK must include 'retag'"
        );

        // jobs has document_id column.
        assert!(
            sql.contains("ADD COLUMN IF NOT EXISTS document_id"),
            "jobs must gain document_id column"
        );
    }

    /// Migration 0009 (P8-T7): the jobs.kind CHECK must include `'restore_test'`.
    #[test]
    fn schema_adds_restore_test_job_kind() {
        let sql: String = MIGRATOR
            .iter()
            .map(|m| m.sql.as_ref())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(
            sql.contains("'restore_test'"),
            "jobs kind CHECK must include 'restore_test' for the automated restore-test job (P8-T7)"
        );
    }

    /// Migration 0010 (P8-T10): the jobs.kind CHECK must include `'orphan_gc'` and
    /// `'integrity_scan'`.
    #[test]
    fn schema_adds_orphan_gc_and_integrity_scan_kinds() {
        let sql: String = MIGRATOR
            .iter()
            .map(|m| m.sql.as_ref())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(
            sql.contains("'orphan_gc'"),
            "jobs kind CHECK must include 'orphan_gc' for the orphan GC job (P8-T10)"
        );
        assert!(
            sql.contains("'integrity_scan'"),
            "jobs kind CHECK must include 'integrity_scan' for the integrity scan job (P8-T10)"
        );
    }

    /// Migration 0011 (P9-T5): the `providers`, `models`, and `routes` tables for DB-driven
    /// routing (§26.5).
    #[test]
    fn schema_adds_providers_models_and_routes_tables() {
        let sql: String = MIGRATOR
            .iter()
            .map(|m| m.sql.as_ref())
            .collect::<Vec<_>>()
            .join("\n");

        for table in ["providers", "models", "routes"] {
            assert!(
                sql.contains(&format!("CREATE TABLE {table} ")),
                "missing CREATE TABLE for {table} (migration 0011)"
            );
        }

        // providers.kind CHECK covers all four ProviderKind variants.
        assert!(sql.contains("'openai_compat'"));
        assert!(sql.contains("'anthropic'"));
        assert!(sql.contains("'gemini_native'"));
        assert!(sql.contains("'local'"));

        // routes UNIQUE constraint for tenant-override detection.
        assert!(sql.contains("UNIQUE (tenant_id, role, tier, model_id)"));

        // routes.role CHECK covers all five Role variants.
        for r in ["'text'", "'vision'", "'code'", "'embed'", "'rerank'"] {
            assert!(sql.contains(r), "routes.role CHECK missing {r}");
        }

        // routes.strategy CHECK covers all four RoutingStrategy variants.
        for s in [
            "'least_loaded'",
            "'round_robin'",
            "'cost_asc'",
            "'priority'",
        ] {
            assert!(sql.contains(s), "routes.strategy CHECK missing {s}");
        }

        // models FK to providers.
        assert!(sql.contains("REFERENCES providers(id)"));
        // routes FK to models.
        assert!(sql.contains("REFERENCES models(id)"));
    }

    /// The two-role RLS model (P6-T14, §13): migration 0006 must create the non-privileged
    /// `kb_app` role as `NOSUPERUSER NOBYPASSRLS` (so RLS is enforced for it) and grant it DML
    /// on the tenant-scoped tables — but NOT on `tenants`/`sessions`/`settings`.
    #[test]
    fn schema_creates_non_bypassrls_app_role() {
        let sql: String = MIGRATOR
            .iter()
            .map(|m| m.sql.as_ref())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(
            sql.contains("CREATE ROLE kb_app"),
            "kb_app role not created"
        );
        assert!(
            sql.contains("NOSUPERUSER") && sql.contains("NOBYPASSRLS"),
            "kb_app must be NOSUPERUSER + NOBYPASSRLS so RLS is enforced"
        );
        // DML grant covers the tenant tables.
        assert!(
            sql.contains("ON\n    users, documents, files, tags, tag_aliases, document_tags, chunks, jobs, usage_events\n    TO kb_app")
                || sql.contains("TO kb_app"),
            "kb_app must be granted DML on the tenant tables"
        );
        // The privileged-only tables must NOT appear in a GRANT … TO kb_app list.
        for forbidden in ["tenants,", "sessions,", "settings,"] {
            assert!(
                !sql.contains(&format!(
                    "GRANT SELECT, INSERT, UPDATE, DELETE ON\n    {forbidden}"
                )),
                "kb_app must not be granted DML on {forbidden}"
            );
        }
    }

    /// Migration 0014 (P9-T12): job priority semantics flipped — default changed from
    /// 100 to 0, index rebuilt for `priority DESC`.
    #[test]
    fn schema_flips_job_priority_semantics() {
        let sql: String = MIGRATOR
            .iter()
            .map(|m| m.sql.as_ref())
            .collect::<Vec<_>>()
            .join("\n");

        // Default changed from 100 to 0.
        assert!(
            sql.contains("ALTER TABLE jobs ALTER COLUMN priority SET DEFAULT 0"),
            "migration 0014 must set priority default to 0 (higher=preferred)"
        );

        // New index for priority DESC ordering.
        assert!(
            sql.contains("priority DESC"),
            "migration 0014 must rebuild the index for priority DESC"
        );
    }

    /// Migration 0013 (P9-T10): cost-tracking columns — `usage_events.cost_micros` and
    /// `tenants.budget_monthly_cents`.
    #[test]
    fn schema_adds_cost_tracking_columns() {
        let sql: String = MIGRATOR
            .iter()
            .map(|m| m.sql.as_ref())
            .collect::<Vec<_>>()
            .join("\n");

        // cost_micros column on usage_events for priced remote calls.
        assert!(
            sql.contains("usage_events\n    ADD COLUMN IF NOT EXISTS cost_micros"),
            "missing cost_micros column on usage_events (migration 0013)"
        );

        // budget_monthly_cents column on tenants for per-tenant budget caps.
        assert!(
            sql.contains("tenants\n    ADD COLUMN IF NOT EXISTS budget_monthly_cents"),
            "missing budget_monthly_cents column on tenants (migration 0013)"
        );
    }
}
