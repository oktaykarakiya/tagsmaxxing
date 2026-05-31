//! [`TagStore`] trait — the database operations needed by
//! [`TagCanonicalizer`](super::tag_canonicalizer::TagCanonicalizer).
//!
//! Extracted from [`PgStore`](kb_store::PgStore) so the canonicalization algorithm can
//! be tested with an in-memory mock instead of requiring a live Postgres instance.

use async_trait::async_trait;

/// The set of Postgres tag operations required by the canonicalization algorithm
/// (plan §6.5). Implementations include the production [`PgStore`] and an in-memory
/// mock for deterministic tests.
#[async_trait]
pub trait TagStore: Send + Sync {
    /// Look up a canonical tag id from a raw tag name via an existing alias row.
    ///
    /// Returns `Ok(Some(tag_id))` if an alias exists, `Ok(None)` otherwise.
    async fn lookup_alias(&self, tenant_id: i64, alias: &str) -> anyhow::Result<Option<i64>>;

    /// Fetch every canonical tag id and its embedding for the given tenant, used for
    /// cosine matching.
    async fn find_similar_tags(&self, tenant_id: i64) -> anyhow::Result<Vec<(i64, Vec<f32>)>>;

    /// Insert a new alias row linking `alias` to an existing canonical `tag_id`.
    async fn insert_tag_alias(
        &self,
        tenant_id: i64,
        alias: &str,
        tag_id: i64,
    ) -> anyhow::Result<()>;

    /// Upsert a canonical tag — insert (with embedding) on first occurrence, or
    /// return the existing id when the (tenant, name) pair already exists.
    ///
    /// Returns the canonical `tag_id`.
    async fn upsert_tag(
        &self,
        tenant_id: i64,
        name: &str,
        embedding: &[f32],
    ) -> anyhow::Result<i64>;
}

// ── PgStore impl (delegates to the real Postgres methods) ────────────────────

#[async_trait]
impl TagStore for kb_store::PgStore {
    async fn lookup_alias(&self, tenant_id: i64, alias: &str) -> anyhow::Result<Option<i64>> {
        self.lookup_alias(tenant_id, alias).await
    }

    async fn find_similar_tags(&self, tenant_id: i64) -> anyhow::Result<Vec<(i64, Vec<f32>)>> {
        self.find_similar_tags(tenant_id).await
    }

    async fn insert_tag_alias(
        &self,
        tenant_id: i64,
        alias: &str,
        tag_id: i64,
    ) -> anyhow::Result<()> {
        self.insert_tag_alias(tenant_id, alias, tag_id).await
    }

    async fn upsert_tag(
        &self,
        tenant_id: i64,
        name: &str,
        embedding: &[f32],
    ) -> anyhow::Result<i64> {
        self.upsert_tag(tenant_id, name, embedding).await
    }
}

// ── In-memory mock for tests ─────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::type_complexity)]
pub(crate) mod mock {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use super::*;

    /// An in-memory [`TagStore`] for deterministic canonicalization tests.
    ///
    /// Tags and aliases are stored in `HashMap`s behind a `Mutex` so the mock can
    /// be shared across `Arc`.
    pub(crate) struct MockTagStore {
        /// (tenant_id, name) → (tag_id, embedding)
        tags: Mutex<HashMap<(i64, String), (i64, Vec<f32>)>>,
        /// (tenant_id, alias) → canonical tag_id
        aliases: Mutex<HashMap<(i64, String), i64>>,
        /// Next id counter.
        next_id: Mutex<i64>,
    }

    impl MockTagStore {
        /// Create an empty tag store.
        pub(crate) fn new() -> Self {
            Self {
                tags: Mutex::new(HashMap::new()),
                aliases: Mutex::new(HashMap::new()),
                next_id: Mutex::new(1),
            }
        }

        /// Seed the store with a pre-existing tag (useful for merge tests).
        pub(crate) fn seed_tag(
            &self,
            tenant_id: i64,
            name: &str,
            tag_id: i64,
            embedding: Vec<f32>,
        ) {
            self.tags
                .lock()
                .unwrap()
                .insert((tenant_id, name.to_string()), (tag_id, embedding));
            // Ensure next_id is beyond the seeded id.
            let mut next = self.next_id.lock().unwrap();
            if tag_id >= *next {
                *next = tag_id + 1;
            }
        }

        /// Seed the store with a pre-existing alias.
        pub(crate) fn seed_alias(&self, tenant_id: i64, alias: &str, tag_id: i64) {
            self.aliases
                .lock()
                .unwrap()
                .insert((tenant_id, alias.to_string()), tag_id);
        }
    }

    impl Default for MockTagStore {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait]
    impl TagStore for MockTagStore {
        async fn lookup_alias(&self, tenant_id: i64, alias: &str) -> anyhow::Result<Option<i64>> {
            Ok(self
                .aliases
                .lock()
                .unwrap()
                .get(&(tenant_id, alias.to_string()))
                .copied())
        }

        async fn find_similar_tags(&self, tenant_id: i64) -> anyhow::Result<Vec<(i64, Vec<f32>)>> {
            Ok(self
                .tags
                .lock()
                .unwrap()
                .iter()
                .filter(|((t, _), _)| *t == tenant_id)
                .map(|(_, (id, emb))| (*id, emb.clone()))
                .collect())
        }

        async fn insert_tag_alias(
            &self,
            tenant_id: i64,
            alias: &str,
            tag_id: i64,
        ) -> anyhow::Result<()> {
            self.aliases
                .lock()
                .unwrap()
                .insert((tenant_id, alias.to_string()), tag_id);
            Ok(())
        }

        async fn upsert_tag(
            &self,
            tenant_id: i64,
            name: &str,
            embedding: &[f32],
        ) -> anyhow::Result<i64> {
            let key = (tenant_id, name.to_string());
            let mut tags = self.tags.lock().unwrap();
            if let Some((id, _)) = tags.get(&key) {
                Ok(*id)
            } else {
                let id = {
                    let mut next = self.next_id.lock().unwrap();
                    let id = *next;
                    *next += 1;
                    id
                };
                tags.insert(key, (id, embedding.to_vec()));
                Ok(id)
            }
        }
    }
}

// ── Tests for PgStore delegation (no DB needed — error-before-connect paths) ──

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod delegation_tests {
    use std::sync::Arc;

    use kb_store::PgStore;

    use super::*;

    fn store() -> Arc<dyn TagStore> {
        Arc::new(PgStore::new("postgres://localhost/test"))
    }

    #[tokio::test]
    async fn lookup_alias_errors_before_connect() {
        let s = store();
        let err = s.lookup_alias(1, "test").await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn find_similar_tags_errors_before_connect() {
        let s = store();
        let err = s.find_similar_tags(1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn insert_tag_alias_errors_before_connect() {
        let s = store();
        let err = s.insert_tag_alias(1, "x", 42).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn upsert_tag_errors_before_connect() {
        let s = store();
        let err = s.upsert_tag(1, "x", &[0.1, 0.2]).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }
}
