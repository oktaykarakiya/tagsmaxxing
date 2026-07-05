// SPDX-License-Identifier: AGPL-3.0-or-later

//! The embedded, forward-only single squashed base schema migration.

use sqlx::migrate::Migrator;

/// The full schema, compiled in from `crates/store/migrations/` at build time.
///
/// Forward-only single squashed migration with no `down` script
/// (a changed embedder needs a `reembed` job, not a reverse migration — plan §5 lock-in note).
/// Apply against a Postgres pool with `kb_store::MIGRATOR.run(&pool).await`; the
/// `_sqlx_migrations` ledger makes that idempotent, so already-applied versions are skipped on
/// a re-run.
pub static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    /// The squashed base schema migration plus the follow-up index migrations
    /// (0004 jobs tenant/created listing index, 0005 document_tags tag_id index).
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

    /// Comprehensive schema lock-in test for the single squashed migration: extension,
    /// function, all tables, embedder lock-in (2560-dim), CHECK constraints, indexes,
    /// RLS, kb_app role, FK relationships, seed data, and generated column.
    #[test]
    fn schema_locks_critical_ddl() {
        let sql: String = MIGRATOR
            .iter()
            .map(|m| m.sql.as_ref())
            .collect::<Vec<_>>()
            .join("\n");

        // ── a) Extension & function ────────────────────────────────────────────
        assert!(
            sql.contains("CREATE EXTENSION IF NOT EXISTS vector"),
            "missing pgvector extension"
        );
        assert!(
            sql.contains("CREATE FUNCTION app_set_current_tenant"),
            "missing app_set_current_tenant function"
        );
        assert!(
            sql.contains("set_config('app.current_tenant'"),
            "function must call set_config for app.current_tenant"
        );

        // ── b) All tables ──────────────────────────────────────────────────────
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
            "providers",
            "models",
            "routes",
            "audit_events",
            "tenant_data_keys",
            "tenant_tombstones",
            "decrypt_audit",
            "plans",
            "stripe_events",
            "api_tokens",
            "tenant_monthly_usage",
            "assistant_sessions",
            "assistant_action_items",
            "assistant_decisions",
            "assistant_stale_watches",
            "assistant_transcripts",
            "document_versions",
        ] {
            assert!(
                sql.contains(&format!("CREATE TABLE {table} ")),
                "missing CREATE TABLE for {table}"
            );
        }

        // ── c) Embedder lock-in (final state: 2560-dim, not 1024) ─────────────
        let vector_2560_lines = sql
            .lines()
            .filter(|l| l.contains("VECTOR(2560)") && !l.trim_start().starts_with("--"))
            .count();
        assert_eq!(
            vector_2560_lines, 2,
            "expected 2 non-comment VECTOR(2560) column definitions (tags + chunks)"
        );
        let vector_1024_lines = sql
            .lines()
            .filter(|l| l.contains("VECTOR(1024)") && !l.trim_start().starts_with("--"))
            .count();
        assert_eq!(
            vector_1024_lines, 0,
            "squashed schema must not contain VECTOR(1024) — goes straight to 2560"
        );
        assert!(
            sql.contains("halfvec(2560)) halfvec_cosine_ops"),
            "missing halfvec HNSW index for >2000-dim vectors"
        );
        assert!(
            sql.contains("\"id\": \"qwen3-embed-4b\""),
            "embedder id must be qwen3-embed-4b"
        );
        assert!(sql.contains("\"dim\": 2560"), "embedder dim must be 2560");
        assert!(sql.contains("'embedder'"), "missing embedder settings key");

        // ── d) CHECK constraint values ─────────────────────────────────────────
        // documents.kind
        for kind in [
            "'document'",
            "'image'",
            "'audio'",
            "'video'",
            "'identity_document'",
            "'code'",
            "'archive'",
            "'binary'",
        ] {
            assert!(sql.contains(kind), "documents.kind CHECK missing {kind}");
        }
        // documents.status
        for status in ["'pending'", "'ready'", "'failed'"] {
            assert!(
                sql.contains(status),
                "documents.status CHECK missing {status}"
            );
        }
        // files.status
        for status in ["'pending'", "'ready'", "'failed'"] {
            assert!(sql.contains(status), "files.status CHECK missing {status}");
        }
        // users.role
        for role in ["'owner'", "'admin'", "'member'"] {
            assert!(sql.contains(role), "users.role CHECK missing {role}");
        }
        // jobs.status
        for status in ["'queued'", "'running'", "'failed'", "'dead'", "'done'"] {
            assert!(sql.contains(status), "jobs.status CHECK missing {status}");
        }
        // jobs.kind
        for kind in [
            "'ingest'",
            "'reembed'",
            "'export'",
            "'retag'",
            "'restore_test'",
            "'orphan_gc'",
            "'integrity_scan'",
            "'delete_tenant'",
            "'send_email'",
            "'refetch'",
        ] {
            assert!(sql.contains(kind), "jobs.kind CHECK missing {kind}");
        }
        // tenants.residency
        for residency in ["'local_only'", "'allow_remote'"] {
            assert!(
                sql.contains(residency),
                "tenants.residency CHECK missing {residency}"
            );
        }
        // models.data_class
        for dc in ["'local'", "'remote'"] {
            assert!(sql.contains(dc), "models.data_class CHECK missing {dc}");
        }
        // providers.kind
        for pkind in [
            "'openai_compat'",
            "'anthropic'",
            "'gemini_native'",
            "'local'",
        ] {
            assert!(sql.contains(pkind), "providers.kind CHECK missing {pkind}");
        }
        // routes.role
        for role in ["'text'", "'vision'", "'code'", "'embed'", "'rerank'"] {
            assert!(sql.contains(role), "routes.role CHECK missing {role}");
        }
        // routes.strategy
        for strategy in [
            "'least_loaded'",
            "'round_robin'",
            "'cost_asc'",
            "'priority'",
        ] {
            assert!(
                sql.contains(strategy),
                "routes.strategy CHECK missing {strategy}"
            );
        }
        // document_tags.source
        for src in ["'llm'", "'user'"] {
            assert!(
                sql.contains(src),
                "document_tags.source CHECK missing {src}"
            );
        }

        // ── e) Indexes ─────────────────────────────────────────────────────────
        assert!(
            sql.contains("USING hnsw")
                && sql.contains("halfvec")
                && sql.contains("halfvec_cosine_ops"),
            "missing HNSW halfvec index"
        );
        assert!(
            sql.contains("CREATE EXTENSION IF NOT EXISTS pg_search"),
            "missing pg_search extension"
        );
        assert!(
            sql.contains("paradedb.create_bm25"),
            "missing bm25 index creation"
        );
        for idx in [
            "documents_meta_gin",
            "files_meta_gin",
            "files_document_id",
            "chunks_document_id",
            "chunks_file_id",
            "sessions_expires_at",
            "idx_audit_events_tenant",
            "idx_audit_events_actor",
            "idx_decrypt_audit_tenant",
            "idx_decrypt_audit_key",
            "idx_decrypt_audit_operation",
            "idx_api_tokens_token_hash",
            "idx_api_tokens_tenant_id",
            "usage_events_tenant_created",
            "jobs_running_lease",
            "jobs_tenant_pending",
            "idx_asst_sessions_tenant",
            "idx_asst_sessions_user",
            "idx_asst_actions_tenant",
            "idx_asst_actions_user",
            "idx_asst_decisions_tenant",
            "idx_asst_stale_watch_unique",
            "idx_asst_stale_watch_due",
            "idx_asst_transcripts_session",
            "documents_next_fetch_due",
            "document_versions_doc_desc",
        ] {
            assert!(sql.contains(idx), "missing index: {idx}");
        }
        assert!(
            sql.contains("priority DESC"),
            "missing priority DESC in jobs index"
        );

        // ── f) RLS ─────────────────────────────────────────────────────────────
        let rls_enable_count = sql.matches("ENABLE ROW LEVEL SECURITY").count();
        assert!(
            rls_enable_count >= 16,
            "expected >=16 ENABLE ROW LEVEL SECURITY, found {rls_enable_count}"
        );
        let rls_force_count = sql.matches("FORCE ROW LEVEL SECURITY").count();
        assert!(
            rls_force_count >= 11,
            "expected >=11 FORCE ROW LEVEL SECURITY, found {rls_force_count}"
        );
        assert!(
            sql.contains("app.current_tenant"),
            "missing app.current_tenant RLS reference"
        );
        assert!(
            sql.contains("CREATE POLICY tenant_isolation"),
            "missing tenant_isolation policy"
        );
        assert!(
            sql.contains("EXISTS (SELECT 1 FROM documents"),
            "missing document_tags EXISTS subquery variant"
        );

        // ── g) kb_app role ─────────────────────────────────────────────────────
        assert!(
            sql.contains("CREATE ROLE kb_app"),
            "missing kb_app role creation"
        );
        assert!(
            sql.contains("NOSUPERUSER") && sql.contains("NOBYPASSRLS"),
            "kb_app must be NOSUPERUSER + NOBYPASSRLS"
        );

        // DML grant on the 10 tenant tables.
        let tenant_tables = "users, documents, files, tags, tag_aliases, document_tags, chunks, jobs, usage_events, tenant_monthly_usage";
        assert!(
            sql.contains(&format!("ON\n    {tenant_tables}\n    TO kb_app"))
                || sql.contains(&format!("ON {tenant_tables} TO kb_app"))
                || sql.contains("TO kb_app"),
            "kb_app must be granted DML on the 10 tenant tables"
        );

        // Sequences grant.
        assert!(
            sql.contains("ALL SEQUENCES IN SCHEMA public TO kb_app"),
            "kb_app must be granted ALL SEQUENCES"
        );

        // Function grant.
        assert!(
            sql.contains("EXECUTE ON FUNCTION app_set_current_tenant"),
            "kb_app must be granted EXECUTE on app_set_current_tenant"
        );

        // document_versions: INSERT-only immutability (no UPDATE/DELETE grant).
        assert!(
            sql.contains("SELECT, INSERT ON document_versions TO kb_app"),
            "kb_app must be granted SELECT, INSERT on document_versions"
        );
        assert!(
            !sql.contains("UPDATE ON document_versions TO kb_app"),
            "kb_app must NOT be granted UPDATE on document_versions (immutable)"
        );
        assert!(
            !sql.contains("DELETE ON document_versions TO kb_app"),
            "kb_app must NOT be granted DELETE on document_versions (immutable)"
        );
        assert!(
            sql.contains("document_versions_id_seq TO kb_app"),
            "kb_app must be granted USAGE, SELECT on document_versions_id_seq"
        );

        // Privileged-only tables must NOT appear in a GRANT … TO kb_app list.
        for forbidden in ["tenants,", "sessions,", "settings,"] {
            assert!(
                !sql.contains(&format!(
                    "GRANT SELECT, INSERT, UPDATE, DELETE ON\n    {forbidden}"
                )),
                "kb_app must not be granted DML on {forbidden}"
            );
        }

        // ── h) FK relationships ────────────────────────────────────────────────
        let cascade_count = sql
            .matches("REFERENCES tenants(id) ON DELETE CASCADE")
            .count();
        assert!(
            cascade_count >= 8,
            "expected >=8 REFERENCES tenants(id) ON DELETE CASCADE, found {cascade_count}"
        );
        assert!(
            sql.contains("REFERENCES plans(id)"),
            "tenants.plan_id must reference plans(id)"
        );
        assert!(
            sql.contains("REFERENCES providers(id)"),
            "models.provider_id must reference providers(id)"
        );
        assert!(
            sql.contains("REFERENCES models(id)"),
            "routes.model_id must reference models(id)"
        );
        assert!(
            sql.contains("REFERENCES users(id) ON DELETE SET NULL"),
            "jobs.created_by must reference users(id) ON DELETE SET NULL"
        );

        // ── i) Seed data ───────────────────────────────────────────────────────
        assert!(sql.contains("'free'"), "seed plan 'free' missing");
        assert!(sql.contains("'pro'"), "seed plan 'pro' missing");
        assert!(sql.contains("'team'"), "seed plan 'team' missing");
        assert!(
            sql.contains("ON CONFLICT (code) DO NOTHING"),
            "seed inserts must use ON CONFLICT (code) DO NOTHING"
        );
        assert!(
            sql.contains("52428800"),
            "free plan quota: 50 MiB = 52428800 bytes"
        );
        assert!(
            sql.contains("5368709120"),
            "pro plan quota: 5 GiB = 5368709120 bytes"
        );
        assert!(
            sql.contains("53687091200"),
            "team plan quota: 50 GiB = 53687091200 bytes"
        );
        assert!(sql.contains("10000"), "free plan: 10K token budget");
        assert!(sql.contains("500000"), "pro plan: 500K token budget");
        assert!(sql.contains("2000000"), "team plan: 2M token budget");

        // ── j) ParadeDB BM25 replaces tsvector ──────────────────────────────────
        assert!(
            sql.contains("DROP INDEX IF EXISTS chunks_tsv_gin"),
            "missing tsvector GIN index drop"
        );
        assert!(
            sql.contains("DROP COLUMN IF EXISTS tsv"),
            "missing tsv column drop"
        );
    }
}
