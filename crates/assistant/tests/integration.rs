// SPDX-License-Identifier: AGPL-3.0-or-later

//! Integration tests for kb-assistant migrations against a real Postgres
//! instance as well as SQL-schema validation tests.
//!
//! Tests that need a running container runtime (Podman) are marked `#[ignore]`.
//! Schema-validation tests run in `cargo test` without any infrastructure.
//!
//! ```text
//! cargo test -p kb-assistant --test integration
//! cargo test -p kb-assistant --test integration -- --ignored
//! ```

#[cfg(test)]
mod integration {
    use kb_store::MIGRATOR;
    use sqlx::PgPool;

    const MIGRATION_SQL: &str =
        include_str!("../../store/migrations/0003_assistant_schema.sql");

    // ── Test 1: migration applies clean against real Postgres ──────────────

    /// Provision a fresh Postgres container, run all kb-store migrations, and
    /// assert that migration 0003 (`assistant_*` tables) exists and that every
    /// table it creates is present in `information_schema.tables`.
    #[tokio::test]
    #[ignore = "needs container runtime (Podman) and network"]
    async fn migration_0003_applies_clean() {
        let db = kb_testsupport::fresh_db().await.expect("fresh_db");
        let pool = PgPool::connect(&db.admin_url)
            .await
            .expect("admin pool connect");

        MIGRATOR.run(&pool).await.expect("migrations run");

        // ── Migration record exists ──────────────────────────────────
        let version: i64 = sqlx::query_scalar(
            "SELECT version FROM _sqlx_migrations WHERE version = 3",
        )
        .fetch_one(&pool)
        .await
        .expect("migration 0003 record");

        assert_eq!(version, 3, "migration version should be 3");

        // ── Tables exist ─────────────────────────────────────────────
        let tables = &[
            "assistant_sessions",
            "assistant_action_items",
            "assistant_decisions",
            "assistant_stale_watches",
            "assistant_transcripts",
        ];

        for table in tables {
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = $1)",
            )
            .bind(table)
            .fetch_one(&pool)
            .await
            .unwrap_or(false);

            assert!(exists, "table '{table}' should exist after migration 0003");
        }
    }

    // ── Test 2: pure-SQL validation — all table DDL present ───────────────

    #[test]
    fn migration_contains_all_tables() {
        assert!(
            MIGRATION_SQL.contains("CREATE TABLE assistant_sessions"),
            "missing CREATE TABLE assistant_sessions"
        );
        assert!(
            MIGRATION_SQL.contains("CREATE TABLE assistant_action_items"),
            "missing CREATE TABLE assistant_action_items"
        );
        assert!(
            MIGRATION_SQL.contains("CREATE TABLE assistant_decisions"),
            "missing CREATE TABLE assistant_decisions"
        );
        assert!(
            MIGRATION_SQL.contains("CREATE TABLE assistant_stale_watches"),
            "missing CREATE TABLE assistant_stale_watches"
        );
        assert!(
            MIGRATION_SQL.contains("CREATE TABLE assistant_transcripts"),
            "missing CREATE TABLE assistant_transcripts"
        );
    }

    // ── Test 3: pure-SQL validation — RLS enabled on every table ──────────

    #[test]
    fn migration_has_rls_on_all_tables() {
        let rls_tables = &[
            "assistant_sessions",
            "assistant_action_items",
            "assistant_decisions",
            "assistant_stale_watches",
            "assistant_transcripts",
        ];

        for table in rls_tables {
            let needle = format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY");
            assert!(
                MIGRATION_SQL.contains(&needle),
                "missing RLS for {table}"
            );
        }
    }

    // ── Test 4: pure-SQL validation — expected indexes present ────────────

    #[test]
    fn migration_has_expected_indexes() {
        let indexes = &[
            "idx_asst_sessions_tenant",
            "idx_asst_actions_tenant",
            "idx_asst_stale_watch_unique",
            "idx_asst_transcripts_session",
        ];

        for idx in indexes {
            assert!(
                MIGRATION_SQL.contains(idx),
                "missing index {idx}"
            );
        }

        // Extra sanity: the UNIQUE constraint on stale_watches is a UNIQUE INDEX
        assert!(
            MIGRATION_SQL.contains("UNIQUE INDEX idx_asst_stale_watch_unique"),
            "idx_asst_stale_watch_unique should be a UNIQUE index"
        );

        // Verify CHECK constraints for key tables
        assert!(
            MIGRATION_SQL.contains("CHECK (status IN ('idle', 'running', 'done', 'killed'))"),
            "missing CHECK constraint on assistant_sessions.status"
        );
        assert!(
            MIGRATION_SQL.contains("CHECK (status IN ('pending', 'done', 'cancelled'))"),
            "missing CHECK constraint on assistant_action_items.status"
        );
        assert!(
            MIGRATION_SQL.contains("CHECK (schedule IN ('daily', 'weekly', 'monthly'))"),
            "missing CHECK constraint on assistant_stale_watches.schedule"
        );
        assert!(
            MIGRATION_SQL.contains("CHECK (status IN ('active', 'paused'))"),
            "missing CHECK constraint on assistant_stale_watches.status"
        );
    }
}
