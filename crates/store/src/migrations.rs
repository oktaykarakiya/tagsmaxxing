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

    /// All six migrations are present, in version order (1, 2, 3, 4, 5, 6).
    #[test]
    fn embeds_the_schema_migrations() {
        let versions: Vec<i64> = MIGRATOR.iter().map(|m| m.version).collect();
        assert_eq!(versions, vec![1, 2, 3, 4, 5, 6]);
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
}
