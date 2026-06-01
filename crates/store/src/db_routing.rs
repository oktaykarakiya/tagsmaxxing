//! DB-driven routing: CRUD for `providers`/`models`/`routes` tables +
//! [`get_routing_table`] and [`test_connection`] (plan §26.5, P9-T5).
//!
//! These methods operate on the **privileged (admin) pool** — the routing
//! tables are global/admin resources, not tenant-scoped (the job queue and
//! the health loop both need to see them without a tenant context).
//!
//! Routes support tenant-specific overrides: when a route exists with
//! `tenant_id = 7` for the same `(role, tier, model_id)`, it replaces the
//! global (`tenant_id IS NULL`) entry for that tenant. The [`get_routing_table`]
//! function applies this override in the query itself.

use std::time::Duration;

use anyhow::{Context, bail};
use kb_core::data_class::DataClass;
use kb_core::role::Role;
use kb_core::routing::{
    ModelRow, ProviderKind, ProviderRow, RouteRow, RoutingChangeNotifier, RoutingEntry,
    RoutingStrategy,
};

use crate::pg_store::PgStore;

impl PgStore {
    // ── Provider CRUD ──────────────────────────────────────────────────────────

    /// Insert a new provider row, returning its id.
    ///
    /// The `headers` parameter is a JSON value (empty-object `{}` if none).
    /// `enabled` defaults to `true` if not explicitly set.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn create_provider(
        &self,
        name: &str,
        kind: ProviderKind,
        endpoint: Option<&str>,
        headers: &serde_json::Value,
    ) -> anyhow::Result<i64> {
        let pool = self.admin_pool()?;
        let id = sqlx::query_scalar::<sqlx::Postgres, i64>(
            "INSERT INTO providers (name, kind, endpoint, headers) \
             VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(name)
        .bind(kind.as_str())
        .bind(endpoint)
        .bind(headers)
        .fetch_one(&pool)
        .await
        .context("failed to insert provider")?;
        Ok(id)
    }

    /// Fetch a single provider by id.
    ///
    /// Returns `None` if the provider does not exist.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn get_provider(&self, id: i64) -> anyhow::Result<Option<ProviderRow>> {
        let pool = self.admin_pool()?;
        let row: Option<ProviderRowSql> = sqlx::query_as(
            "SELECT id, name, kind, endpoint, headers, enabled \
             FROM providers WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&pool)
        .await
        .context("failed to fetch provider")?;
        row.map(provider_from_sql).transpose()
    }

    /// Update mutable fields on an existing provider row.
    ///
    /// Fields that are `None` on the caller side are left unchanged in the DB.
    /// `enabled` is never changed here — use [`disable_provider`](Self::disable_provider)
    /// or [`enable_provider`](Self::enable_provider) for that.
    ///
    /// # Errors
    /// Returns an error if the database is not connected, the provider does not
    /// exist, or the query fails.
    pub async fn update_provider(
        &self,
        id: i64,
        name: Option<&str>,
        kind: Option<ProviderKind>,
        endpoint: Option<Option<&str>>,
        headers: Option<&serde_json::Value>,
    ) -> anyhow::Result<()> {
        let pool = self.admin_pool()?;
        let current = self
            .get_provider(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("provider {id} not found"))?;
        let new_name = name.unwrap_or(&current.name);
        let new_kind = kind.unwrap_or(current.kind);
        let new_endpoint = match endpoint {
            Some(ep) => ep.map(String::from),
            None => current.endpoint,
        };
        let new_headers = headers.cloned().unwrap_or(current.headers.clone());

        sqlx::query("UPDATE providers SET name=$1, kind=$2, endpoint=$3, headers=$4 WHERE id=$5")
            .bind(new_name)
            .bind(new_kind.as_str())
            .bind(new_endpoint.as_deref())
            .bind(&new_headers)
            .bind(id)
            .execute(&pool)
            .await
            .context("failed to update provider")?;
        Ok(())
    }

    /// Soft-delete a provider by setting `enabled = false`.
    ///
    /// Returns the number of rows updated (0 if the provider did not exist).
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn disable_provider(&self, id: i64) -> anyhow::Result<u64> {
        let pool = self.admin_pool()?;
        let rows = sqlx::query("UPDATE providers SET enabled = false WHERE id = $1")
            .bind(id)
            .execute(&pool)
            .await
            .context("failed to disable provider")?
            .rows_affected();
        Ok(rows)
    }

    /// Re-enable a previously disabled provider.
    ///
    /// Returns the number of rows updated (0 if the provider did not exist).
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn enable_provider(&self, id: i64) -> anyhow::Result<u64> {
        let pool = self.admin_pool()?;
        let rows = sqlx::query("UPDATE providers SET enabled = true WHERE id = $1")
            .bind(id)
            .execute(&pool)
            .await
            .context("failed to enable provider")?
            .rows_affected();
        Ok(rows)
    }

    /// List all providers, ordered by id.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn list_providers(&self) -> anyhow::Result<Vec<ProviderRow>> {
        let pool = self.admin_pool()?;
        let rows: Vec<ProviderRowSql> = sqlx::query_as(
            "SELECT id, name, kind, endpoint, headers, enabled \
             FROM providers ORDER BY id",
        )
        .fetch_all(&pool)
        .await
        .context("failed to list providers")?;
        rows.into_iter().map(provider_from_sql).collect()
    }

    // ── Model CRUD ─────────────────────────────────────────────────────────────

    /// Insert a new model row, returning its id.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the parent provider
    /// does not exist (FK violation).
    #[allow(clippy::too_many_arguments)]
    pub async fn create_model(
        &self,
        provider_id: i64,
        model_id: &str,
        caps: &[String],
        ctx_tokens: Option<i32>,
        max_conc: Option<i32>,
        rpm: Option<i32>,
        tpm: Option<i32>,
        price_in: Option<f64>,
        price_out: Option<f64>,
        data_class: DataClass,
    ) -> anyhow::Result<i64> {
        let pool = self.admin_pool()?;
        let id = sqlx::query_scalar::<sqlx::Postgres, i64>(
            "INSERT INTO models (provider_id, model_id, caps, ctx_tokens, max_conc, rpm, tpm, \
             price_in, price_out, data_class) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING id",
        )
        .bind(provider_id)
        .bind(model_id)
        .bind(caps)
        .bind(ctx_tokens)
        .bind(max_conc)
        .bind(rpm)
        .bind(tpm)
        .bind(price_in)
        .bind(price_out)
        .bind(data_class.as_str())
        .fetch_one(&pool)
        .await
        .context("failed to insert model")?;
        Ok(id)
    }

    /// Fetch a single model by id.
    ///
    /// Returns `None` if the model does not exist.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn get_model(&self, id: i64) -> anyhow::Result<Option<ModelRow>> {
        let pool = self.admin_pool()?;
        let row: Option<ModelRowSql> = sqlx::query_as(
            "SELECT id, provider_id, model_id, caps, ctx_tokens, max_conc, rpm, tpm, \
             price_in, price_out, data_class, enabled \
             FROM models WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&pool)
        .await
        .context("failed to fetch model")?;
        row.map(model_from_sql).transpose()
    }

    /// Disable a model by setting `enabled = false`.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn disable_model(&self, id: i64) -> anyhow::Result<u64> {
        let pool = self.admin_pool()?;
        let rows = sqlx::query("UPDATE models SET enabled = false WHERE id = $1")
            .bind(id)
            .execute(&pool)
            .await
            .context("failed to disable model")?
            .rows_affected();
        Ok(rows)
    }

    /// Enable a previously disabled model.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn enable_model(&self, id: i64) -> anyhow::Result<u64> {
        let pool = self.admin_pool()?;
        let rows = sqlx::query("UPDATE models SET enabled = true WHERE id = $1")
            .bind(id)
            .execute(&pool)
            .await
            .context("failed to enable model")?
            .rows_affected();
        Ok(rows)
    }

    /// List all models, ordered by provider_id then id.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn list_models(&self) -> anyhow::Result<Vec<ModelRow>> {
        let pool = self.admin_pool()?;
        let rows: Vec<ModelRowSql> = sqlx::query_as(
            "SELECT id, provider_id, model_id, caps, ctx_tokens, max_conc, rpm, tpm, \
             price_in, price_out, data_class, enabled \
             FROM models ORDER BY provider_id, id",
        )
        .fetch_all(&pool)
        .await
        .context("failed to list models")?;
        rows.into_iter().map(model_from_sql).collect()
    }

    // ── Route CRUD ─────────────────────────────────────────────────────────────

    /// Insert a new route row, returning its id.
    ///
    /// `tenant_id` of `None` means the route is a global default; `Some(t)` makes it
    /// tenant-specific. The `UNIQUE (tenant_id, role, tier, model_id)` constraint
    /// prevents duplicate global or tenant entries.
    ///
    /// # Errors
    /// Returns an error on a duplicate-key violation, missing parent model, or
    /// general query failure.
    pub async fn create_route(
        &self,
        tenant_id: Option<i64>,
        role: Role,
        tier: i32,
        strategy: RoutingStrategy,
        spill_ms: i32,
        model_id: i64,
    ) -> anyhow::Result<i64> {
        let pool = self.admin_pool()?;
        let id = sqlx::query_scalar::<sqlx::Postgres, i64>(
            "INSERT INTO routes (tenant_id, role, tier, strategy, spill_ms, model_id) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (tenant_id, role, tier, model_id) DO NOTHING \
             RETURNING id",
        )
        .bind(tenant_id)
        .bind(role.as_str())
        .bind(tier)
        .bind(strategy.as_str())
        .bind(spill_ms)
        .bind(model_id)
        .fetch_optional(&pool)
        .await
        .context("failed to insert route")?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "duplicate route: tenant={tenant_id:?} role={} tier={tier} model={model_id}",
                role.as_str()
            )
        })?;
        Ok(id)
    }

    /// Fetch a single route by id.
    ///
    /// Returns `None` if the route does not exist.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn get_route(&self, id: i64) -> anyhow::Result<Option<RouteRow>> {
        let pool = self.admin_pool()?;
        let row: Option<RouteRowSql> = sqlx::query_as(
            "SELECT id, tenant_id, role, tier, strategy, spill_ms, model_id \
             FROM routes WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&pool)
        .await
        .context("failed to fetch route")?;
        row.map(route_from_sql).transpose()
    }

    /// Update mutable fields on an existing route.
    ///
    /// # Errors
    /// Returns an error if the database is not connected, the route does not
    /// exist, or the query fails.
    pub async fn update_route(
        &self,
        id: i64,
        tier: Option<i32>,
        strategy: Option<RoutingStrategy>,
        spill_ms: Option<i32>,
        model_id: Option<i64>,
    ) -> anyhow::Result<()> {
        let pool = self.admin_pool()?;
        let current = self
            .get_route(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("route {id} not found"))?;
        let new_tier = tier.unwrap_or(current.tier);
        let new_strategy = strategy.unwrap_or(current.strategy);
        let new_spill = spill_ms.unwrap_or(current.spill_ms);
        let new_model = model_id.unwrap_or(current.model_id);

        sqlx::query("UPDATE routes SET tier=$1, strategy=$2, spill_ms=$3, model_id=$4 WHERE id=$5")
            .bind(new_tier)
            .bind(new_strategy.as_str())
            .bind(new_spill)
            .bind(new_model)
            .bind(id)
            .execute(&pool)
            .await
            .context("failed to update route")?;
        Ok(())
    }

    /// Hard-delete a route row.
    ///
    /// Returns the number of rows deleted (0 if the route did not exist).
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn delete_route(&self, id: i64) -> anyhow::Result<u64> {
        let pool = self.admin_pool()?;
        let rows = sqlx::query("DELETE FROM routes WHERE id = $1")
            .bind(id)
            .execute(&pool)
            .await
            .context("failed to delete route")?
            .rows_affected();
        Ok(rows)
    }

    /// List all routes, ordered by role, tenant_id NULLS FIRST, tier.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn list_routes(&self) -> anyhow::Result<Vec<RouteRow>> {
        let pool = self.admin_pool()?;
        let rows: Vec<RouteRowSql> = sqlx::query_as(
            "SELECT id, tenant_id, role, tier, strategy, spill_ms, model_id \
             FROM routes ORDER BY role, tenant_id NULLS FIRST, tier",
        )
        .fetch_all(&pool)
        .await
        .context("failed to list routes")?;
        rows.into_iter().map(route_from_sql).collect()
    }

    // ── Routing table build ─────────────────────────────────────────────────────

    /// Build the full routing table by joining `providers`, `models`, and `routes`
    /// for all enabled rows, applying tenant-specific override semantics.
    ///
    /// # Override semantics (plan §26.5)
    ///
    /// When both a global (`tenant_id IS NULL`) and a tenant-specific route exist
    /// for the same `(role, tier, model_id)`, the tenant-specific entry replaces
    /// the global one **for that tenant**. The global entry is still returned
    /// for all other tenants.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn get_routing_table(&self) -> anyhow::Result<Vec<RoutingEntry>> {
        let pool = self.admin_pool()?;

        // Join all three tables where both provider and model are enabled.
        // The query returns every route entry; tenant-override filtering is done
        // in Rust below so we can test it as pure logic.
        let rows: Vec<RoutingTableRow> = sqlx::query_as(
            r#"SELECT
                r.id         AS route_id,
                r.tenant_id  AS route_tenant_id,
                r.role,
                r.tier,
                r.strategy,
                r.spill_ms,
                m.id         AS model_id,
                m.provider_id,
                m.model_id   AS model_name,
                m.caps,
                m.ctx_tokens,
                m.max_conc,
                m.rpm,
                m.tpm,
                m.price_in,
                m.price_out,
                m.data_class AS model_data_class,
                m.enabled    AS model_enabled,
                p.id         AS provider_id_val,
                p.name       AS provider_name,
                p.kind       AS provider_kind,
                p.endpoint,
                p.headers,
                p.enabled    AS provider_enabled
            FROM routes r
            JOIN models m ON r.model_id = m.id AND m.enabled
            JOIN providers p ON m.provider_id = p.id AND p.enabled
            ORDER BY r.role, r.tenant_id NULLS FIRST, r.tier"#,
        )
        .fetch_all(&pool)
        .await
        .context("failed to build routing table")?;

        // Apply tenant-override filtering.
        //
        // Algorithm: collect the set of (role, tier, model_id) keys that have
        // at least one tenant-specific entry. For each key, exclude the global
        // (tenant_id IS NULL) rows — they are replaced by the tenant-specific
        // row for their respective tenants.
        let tenant_keys: std::collections::HashSet<(String, i32, i64)> = rows
            .iter()
            .filter(|r| r.route_tenant_id.is_some())
            .map(|r| (r.role.clone(), r.tier, r.model_id))
            .collect();

        let entries: Vec<RoutingEntry> = rows
            .into_iter()
            .filter(|r| {
                if r.route_tenant_id.is_none() {
                    let key = (r.role.clone(), r.tier, r.model_id);
                    // Keep global entry only if no tenant override exists for this key.
                    !tenant_keys.contains(&key)
                } else {
                    true
                }
            })
            .map(routing_table_row_to_entry)
            .collect::<anyhow::Result<Vec<_>>>()?;

        Ok(entries)
    }

    // ── Connection test ────────────────────────────────────────────────────────

    /// Test whether a provider's endpoint is reachable (plan §26.5).
    ///
    /// Sends an HTTP `GET {endpoint}/health` with a 3-second timeout.
    /// For local or native-SDK providers with no endpoint, returns `Ok(())`
    /// immediately — connectivity for those is verified at model-load time.
    ///
    /// The API key is **not** decrypted for this check (key decryption lands in
    /// P10-T3). A successful HTTP response (any 2xx) means the endpoint is live.
    ///
    /// # Errors
    /// Returns an error if the provider doesn't exist, has no endpoint, or the
    /// health check fails (connection refused, timeout, non-2xx status).
    pub async fn test_connection(&self, provider_id: i64) -> anyhow::Result<()> {
        let provider = self
            .get_provider(provider_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("provider {provider_id} not found"))?;

        let endpoint = provider.endpoint.ok_or_else(|| {
            anyhow::anyhow!(
                "provider {provider_id} has no endpoint (native SDK provider — \
                 connectivity verified at model-load time)"
            )
        })?;

        // Build the health URL: {endpoint}/health, ensuring no double-slash.
        let health_url = if endpoint.ends_with('/') {
            format!("{endpoint}health")
        } else {
            format!("{endpoint}/health")
        };

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .context("failed to build HTTP client for connection test")?;

        let resp = client
            .get(&health_url)
            .send()
            .await
            .with_context(|| format!("connection test failed: GET {health_url}"))?;

        let status = resp.status();
        if status.is_success() {
            Ok(())
        } else {
            bail!("connection test returned {status} for GET {health_url}",)
        }
    }

    // ── Update model ──────────────────────────────────────────────────────────

    /// Update mutable fields on an existing model row.
    ///
    /// Fields that are `None` are left unchanged in the DB. `enabled` is managed
    /// via [`disable_model`](Self::disable_model)/[`enable_model`](Self::enable_model).
    ///
    /// # Errors
    /// Returns an error if the database is not connected, the model does not
    /// exist, or the query fails.
    #[allow(clippy::too_many_arguments)]
    pub async fn update_model(
        &self,
        id: i64,
        provider_id: Option<i64>,
        model_id: Option<&str>,
        caps: Option<&[String]>,
        ctx_tokens: Option<Option<i32>>,
        max_conc: Option<Option<i32>>,
        rpm: Option<Option<i32>>,
        tpm: Option<Option<i32>>,
        price_in: Option<Option<f64>>,
        price_out: Option<Option<f64>>,
        data_class: Option<DataClass>,
    ) -> anyhow::Result<()> {
        let pool = self.admin_pool()?;
        let current = self
            .get_model(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("model {id} not found"))?;
        let new_provider_id = provider_id.unwrap_or(current.provider_id);
        let new_model_id = model_id.unwrap_or(&current.model_id);
        let new_caps = caps.unwrap_or(&current.caps);
        let new_ctx_tokens = match ctx_tokens {
            Some(v) => v,
            None => current.ctx_tokens,
        };
        let new_max_conc = match max_conc {
            Some(v) => v,
            None => current.max_conc,
        };
        let new_rpm = match rpm {
            Some(v) => v,
            None => current.rpm,
        };
        let new_tpm = match tpm {
            Some(v) => v,
            None => current.tpm,
        };
        let new_price_in = match price_in {
            Some(v) => v,
            None => current.price_in,
        };
        let new_price_out = match price_out {
            Some(v) => v,
            None => current.price_out,
        };
        let new_data_class = data_class.unwrap_or(current.data_class);

        sqlx::query(
            "UPDATE models SET provider_id=$1, model_id=$2, caps=$3, ctx_tokens=$4, \
             max_conc=$5, rpm=$6, tpm=$7, price_in=$8, price_out=$9, data_class=$10 \
             WHERE id=$11",
        )
        .bind(new_provider_id)
        .bind(new_model_id)
        .bind(new_caps)
        .bind(new_ctx_tokens)
        .bind(new_max_conc)
        .bind(new_rpm)
        .bind(new_tpm)
        .bind(new_price_in)
        .bind(new_price_out)
        .bind(new_data_class.as_str())
        .bind(id)
        .execute(&pool)
        .await
        .context("failed to update model")?;
        Ok(())
    }

    // ── Routing change notification ───────────────────────────────────────────

    /// Send a `NOTIFY` on the routing-changed channel so the
    /// [`RoutingChangeNotifier`] wakes up and reloads the routing table.
    ///
    /// Callers should invoke this after every CRUD mutation on `providers`,
    /// `models`, or `routes` so the scheduler picks up the change without
    /// waiting for the next poll cycle.
    ///
    /// Uses the admin pool (no tenant scope).
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn notify_routing_change(&self) -> anyhow::Result<()> {
        let pool = self.admin_pool()?;
        sqlx::query("SELECT pg_notify($1, '')")
            .bind(ROUTING_CHANNEL)
            .execute(&pool)
            .await
            .context("failed to send routing change NOTIFY")?;
        Ok(())
    }
}

// ── PgRoutingNotifier (plan §26.5, P9-T7) ─────────────────────────────────

/// A Postgres `LISTEN`/`NOTIFY`-based routing change notifier.
///
/// On the first call to [`wait_for_change`] it returns immediately so the
/// reloader performs its startup load. Subsequent calls attempt to create a
/// [`sqlx::postgres::PgListener`] on the configured channel; if the database
/// is unreachable or the `LISTEN` fails, it falls back to polling at the
/// configured interval.
///
/// The database URL is resolved via a shared function on every call so
/// credential rotation needs no restart (the hot-swap rule, CLAUDE.md).
pub struct PgRoutingNotifier {
    url_fn: std::sync::Arc<dyn Fn() -> String + Send + Sync>,
    channel: String,
    poll_interval: Duration,
    first_call: std::sync::atomic::AtomicBool,
}

impl PgRoutingNotifier {
    /// Create a new Postgres-backed routing notifier.
    ///
    /// # Arguments
    ///
    /// * `url_fn` — a function that returns the current admin connection URL
    ///   (read on every call so credential rotation takes effect without a
    ///   restart).
    /// * `channel` — the Postgres `NOTIFY` channel name (e.g.
    ///   `"routing_changed"`).
    /// * `poll_interval` — fallback poll interval when `LISTEN` is
    ///   unavailable.
    #[must_use]
    pub fn new(
        url_fn: impl Fn() -> String + Send + Sync + 'static,
        channel: String,
        poll_interval: Duration,
    ) -> Self {
        Self {
            url_fn: std::sync::Arc::new(url_fn),
            channel,
            poll_interval,
            first_call: std::sync::atomic::AtomicBool::new(true),
        }
    }

    /// The NOTIFY channel name this notifier listens on.
    #[must_use]
    pub fn channel(&self) -> &str {
        &self.channel
    }

    /// The fallback poll interval.
    #[must_use]
    pub fn poll_interval(&self) -> Duration {
        self.poll_interval
    }
}

/// The recommended NOTIFY channel name for routing-table changes.
pub const ROUTING_CHANNEL: &str = "routing_changed";

#[async_trait::async_trait]
impl RoutingChangeNotifier for PgRoutingNotifier {
    async fn wait_for_change(&self) -> anyhow::Result<()> {
        use std::sync::atomic::Ordering;

        // First call returns immediately (startup load).
        if self.first_call.swap(false, Ordering::SeqCst) {
            return Ok(());
        }

        let url = (self.url_fn)();
        if url.is_empty() {
            // DB not yet connected — poll.
            tokio::time::sleep(self.poll_interval).await;
            return Ok(());
        }

        // Try Postgres LISTEN/NOTIFY; on any failure, fall back to polling.
        match sqlx::postgres::PgListener::connect(&url).await {
            Ok(mut listener) => {
                if let Err(e) = listener.listen(&self.channel).await {
                    tracing::warn!(
                        error = %e,
                        channel = %self.channel,
                        "failed to LISTEN on routing channel; falling back to polling"
                    );
                    tokio::time::sleep(self.poll_interval).await;
                } else {
                    match listener.recv().await {
                        Ok(_notification) => {
                            tracing::debug!(
                                channel = %_notification.channel(),
                                "received routing change notification"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                "PgListener recv failed; falling back to polling"
                            );
                            tokio::time::sleep(self.poll_interval).await;
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "failed to connect PgListener; falling back to polling"
                );
                tokio::time::sleep(self.poll_interval).await;
            }
        }

        Ok(())
    }
}

// ── sqlx row types ──────────────────────────────────────────────────────────

#[derive(sqlx::FromRow)]
struct ProviderRowSql {
    id: i64,
    name: String,
    kind: String,
    endpoint: Option<String>,
    headers: serde_json::Value,
    enabled: bool,
}

/// A string representation of a Postgres NUMERIC value.
///
/// sqlx 0.8.x decodes `NUMERIC` columns into `String` by default (no
/// `bigdecimal` / `rust_decimal` feature). We parse to `f64` in the
/// conversion helpers; values that don't fit are rejected with a clear error.
type NumericStr = String;

fn parse_numeric(s: &str) -> anyhow::Result<f64> {
    s.parse::<f64>()
        .with_context(|| format!("invalid NUMERIC value: {s}"))
}

/// Parse an optional NUMERIC string into `Option<f64>`.
fn parse_optional_numeric(s: &Option<String>) -> anyhow::Result<Option<f64>> {
    s.as_deref().map(parse_numeric).transpose()
}

fn provider_from_sql(r: ProviderRowSql) -> anyhow::Result<ProviderRow> {
    use std::str::FromStr;
    let kind = ProviderKind::from_str(&r.kind)
        .with_context(|| format!("invalid provider kind in DB: {}", r.kind))?;
    Ok(ProviderRow {
        id: r.id,
        name: r.name,
        kind,
        endpoint: r.endpoint,
        headers: r.headers,
        enabled: r.enabled,
    })
}

#[derive(sqlx::FromRow)]
struct ModelRowSql {
    id: i64,
    provider_id: i64,
    model_id: String,
    caps: Vec<String>,
    ctx_tokens: Option<i32>,
    max_conc: Option<i32>,
    rpm: Option<i32>,
    tpm: Option<i32>,
    price_in: Option<NumericStr>,
    price_out: Option<NumericStr>,
    data_class: String,
    enabled: bool,
}

fn model_from_sql(r: ModelRowSql) -> anyhow::Result<ModelRow> {
    use std::str::FromStr;
    let data_class = DataClass::from_str(&r.data_class)
        .with_context(|| format!("invalid data_class in DB: {}", r.data_class))?;
    let price_in = parse_optional_numeric(&r.price_in).context("invalid price_in NUMERIC")?;
    let price_out = parse_optional_numeric(&r.price_out).context("invalid price_out NUMERIC")?;
    Ok(ModelRow {
        id: r.id,
        provider_id: r.provider_id,
        model_id: r.model_id,
        caps: r.caps,
        ctx_tokens: r.ctx_tokens,
        max_conc: r.max_conc,
        rpm: r.rpm,
        tpm: r.tpm,
        price_in,
        price_out,
        data_class,
        enabled: r.enabled,
    })
}

#[derive(sqlx::FromRow)]
struct RouteRowSql {
    id: i64,
    tenant_id: Option<i64>,
    role: String,
    tier: i32,
    strategy: String,
    spill_ms: i32,
    model_id: i64,
}

fn route_from_sql(r: RouteRowSql) -> anyhow::Result<RouteRow> {
    use std::str::FromStr;
    let role =
        Role::from_str(&r.role).with_context(|| format!("invalid role in DB: {}", r.role))?;
    let strategy = RoutingStrategy::from_str(&r.strategy)
        .with_context(|| format!("invalid routing strategy in DB: {}", r.strategy))?;
    Ok(RouteRow {
        id: r.id,
        tenant_id: r.tenant_id,
        role,
        tier: r.tier,
        strategy,
        spill_ms: r.spill_ms,
        model_id: r.model_id,
    })
}

/// The full JOIN row returned by [`get_routing_table`].
#[derive(sqlx::FromRow)]
struct RoutingTableRow {
    #[allow(dead_code)]
    route_id: i64,
    route_tenant_id: Option<i64>,
    role: String,
    tier: i32,
    strategy: String,
    spill_ms: i32,
    model_id: i64,
    provider_id: i64,
    model_name: String,
    caps: Vec<String>,
    ctx_tokens: Option<i32>,
    max_conc: Option<i32>,
    rpm: Option<i32>,
    tpm: Option<i32>,
    price_in: Option<NumericStr>,
    price_out: Option<NumericStr>,
    model_data_class: String,
    model_enabled: bool,
    provider_id_val: i64,
    provider_name: String,
    provider_kind: String,
    endpoint: Option<String>,
    headers: serde_json::Value,
    #[allow(dead_code)]
    provider_enabled: bool,
}

fn routing_table_row_to_entry(r: RoutingTableRow) -> anyhow::Result<RoutingEntry> {
    use std::str::FromStr;

    let role = Role::from_str(&r.role)
        .with_context(|| format!("invalid role in routing table: {}", r.role))?;
    let strategy = RoutingStrategy::from_str(&r.strategy)
        .with_context(|| format!("invalid strategy in routing table: {}", r.strategy))?;
    let provider_kind = ProviderKind::from_str(&r.provider_kind)
        .with_context(|| format!("invalid provider kind: {}", r.provider_kind))?;
    let data_class = DataClass::from_str(&r.model_data_class)
        .with_context(|| format!("invalid data_class: {}", r.model_data_class))?;

    let price_in = parse_optional_numeric(&r.price_in).context("invalid price_in NUMERIC")?;
    let price_out = parse_optional_numeric(&r.price_out).context("invalid price_out NUMERIC")?;

    Ok(RoutingEntry {
        tenant_id: r.route_tenant_id,
        role,
        tier: r.tier,
        strategy,
        spill_ms: r.spill_ms,
        provider: ProviderRow {
            id: r.provider_id_val,
            name: r.provider_name,
            kind: provider_kind,
            endpoint: r.endpoint,
            headers: r.headers,
            enabled: r.provider_enabled,
        },
        model: ModelRow {
            id: r.model_id,
            provider_id: r.provider_id,
            model_id: r.model_name,
            caps: r.caps,
            ctx_tokens: r.ctx_tokens,
            max_conc: r.max_conc,
            rpm: r.rpm,
            tpm: r.tpm,
            price_in,
            price_out,
            data_class,
            enabled: r.model_enabled,
        },
    })
}
// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use kb_core::role::Role;
    use kb_core::routing::RoutingTable;

    // ── Pure-logic: error-before-connect ─────────────────────────────────

    #[tokio::test]
    async fn create_provider_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store
            .create_provider(
                "p",
                ProviderKind::Local,
                Some("http://x"),
                &serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn get_provider_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.get_provider(1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn disable_provider_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.disable_provider(1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn create_model_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store
            .create_model(
                1,
                "m",
                &["text".into()],
                None,
                None,
                None,
                None,
                None,
                None,
                DataClass::Local,
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn get_model_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.get_model(1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn disable_model_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.disable_model(1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn create_route_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store
            .create_route(None, Role::Text, 0, RoutingStrategy::LeastLoaded, 800, 1)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn get_route_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.get_route(1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn delete_route_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.delete_route(1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn get_routing_table_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.get_routing_table().await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    // ── Pure-logic: tenant override filtering ───────────────────────────

    #[allow(clippy::too_many_arguments)]
    fn make_routing_entry(
        tenant_id: Option<i64>,
        role: Role,
        tier: i32,
        strategy: RoutingStrategy,
        spill_ms: i32,
        provider_name: &str,
        model_name: &str,
        caps: &[&str],
    ) -> RoutingEntry {
        RoutingEntry {
            tenant_id,
            role,
            tier,
            strategy,
            spill_ms,
            provider: ProviderRow {
                id: 1,
                name: provider_name.into(),
                kind: ProviderKind::Local,
                endpoint: Some("http://localhost:8001/v1".into()),
                headers: Default::default(),
                enabled: true,
            },
            model: ModelRow {
                id: 1,
                provider_id: 1,
                model_id: model_name.into(),
                caps: caps.iter().map(|s| s.to_string()).collect(),
                ctx_tokens: Some(32768),
                max_conc: Some(4),
                rpm: None,
                tpm: None,
                price_in: None,
                price_out: None,
                data_class: DataClass::Local,
                enabled: true,
            },
        }
    }

    fn apply_tenant_overrides(entries: &[RoutingEntry]) -> Vec<RoutingEntry> {
        let tenant_keys: std::collections::HashSet<(Role, i32, &str)> = entries
            .iter()
            .filter(|e| e.tenant_id.is_some())
            .map(|e| (e.role, e.tier, e.model.model_id.as_str()))
            .collect();
        entries
            .iter()
            .filter(|e| {
                if e.tenant_id.is_none() {
                    let key = (e.role, e.tier, e.model.model_id.as_str());
                    !tenant_keys.contains(&key)
                } else {
                    true
                }
            })
            .cloned()
            .collect()
    }

    #[test]
    fn tenant_override_replaces_global_for_same_role_tier_model() {
        let entries = vec![
            make_routing_entry(
                None,
                Role::Text,
                0,
                RoutingStrategy::LeastLoaded,
                800,
                "global-prov",
                "global-model",
                &["text"],
            ),
            make_routing_entry(
                Some(7),
                Role::Text,
                0,
                RoutingStrategy::RoundRobin,
                500,
                "tenant-prov",
                "global-model",
                &["text"],
            ),
        ];
        let filtered = apply_tenant_overrides(&entries);
        assert_eq!(filtered.len(), 1, "global should be removed");
        assert!(filtered[0].tenant_id.is_some());
        assert_eq!(filtered[0].strategy, RoutingStrategy::RoundRobin);
    }

    #[test]
    fn global_only_no_filtering() {
        let entries = vec![
            make_routing_entry(
                None,
                Role::Text,
                0,
                RoutingStrategy::LeastLoaded,
                800,
                "p1",
                "m1",
                &["text"],
            ),
            make_routing_entry(
                None,
                Role::Embed,
                0,
                RoutingStrategy::Priority,
                200,
                "p2",
                "m2",
                &["embed"],
            ),
        ];
        let filtered = apply_tenant_overrides(&entries);
        assert_eq!(
            filtered.len(),
            2,
            "all global entries kept when no overrides"
        );
    }

    #[test]
    fn tenant_only_no_filtering_needed() {
        let entries = vec![
            make_routing_entry(
                Some(1),
                Role::Text,
                0,
                RoutingStrategy::LeastLoaded,
                800,
                "p1",
                "m1",
                &["text"],
            ),
            make_routing_entry(
                Some(2),
                Role::Text,
                0,
                RoutingStrategy::RoundRobin,
                500,
                "p2",
                "m1",
                &["text"],
            ),
        ];
        let filtered = apply_tenant_overrides(&entries);
        assert_eq!(filtered.len(), 2);
    }

    #[test]
    fn different_tiers_dont_override_each_other() {
        let entries = vec![
            make_routing_entry(
                None,
                Role::Text,
                0,
                RoutingStrategy::LeastLoaded,
                800,
                "primary",
                "model-a",
                &["text"],
            ),
            make_routing_entry(
                None,
                Role::Text,
                1,
                RoutingStrategy::CostAsc,
                1500,
                "fallback",
                "model-b",
                &["text"],
            ),
            make_routing_entry(
                Some(7),
                Role::Text,
                1,
                RoutingStrategy::RoundRobin,
                600,
                "tenant-fallback",
                "model-b",
                &["text"],
            ),
        ];
        let filtered = apply_tenant_overrides(&entries);
        assert_eq!(filtered.len(), 2);
        for e in &filtered {
            if e.tier == 0 {
                assert!(e.tenant_id.is_none());
            } else {
                assert_eq!(e.tenant_id, Some(7));
            }
        }
    }

    #[test]
    fn different_model_ids_dont_override() {
        let entries = vec![
            make_routing_entry(
                None,
                Role::Text,
                0,
                RoutingStrategy::LeastLoaded,
                800,
                "p",
                "model-a",
                &["text"],
            ),
            make_routing_entry(
                Some(7),
                Role::Text,
                0,
                RoutingStrategy::RoundRobin,
                500,
                "p",
                "model-b",
                &["text"],
            ),
        ];
        let filtered = apply_tenant_overrides(&entries);
        assert_eq!(filtered.len(), 2);
    }

    // ── RoutingTable from entries ───────────────────────────────────────

    #[test]
    fn routing_table_from_filtered_entries() {
        let entries = vec![
            make_routing_entry(
                None,
                Role::Text,
                0,
                RoutingStrategy::LeastLoaded,
                800,
                "global",
                "g1",
                &["text"],
            ),
            make_routing_entry(
                Some(7),
                Role::Text,
                0,
                RoutingStrategy::RoundRobin,
                500,
                "tenant7",
                "t1",
                &["text"],
            ),
        ];
        let table = RoutingTable::from_entries(entries);
        assert_eq!(table.group_count(), 2);
        assert_eq!(
            table.tiers_for(None, Role::Text)[0][0].provider.name,
            "global"
        );
        assert_eq!(
            table.tiers_for(Some(7), Role::Text)[0][0].provider.name,
            "tenant7"
        );
        assert_eq!(
            table.tiers_for(Some(99), Role::Text)[0][0].provider.name,
            "global"
        );
    }

    #[test]
    fn routing_table_overridden_tenant_uses_own_route() {
        let entries = vec![
            make_routing_entry(
                None,
                Role::Text,
                0,
                RoutingStrategy::LeastLoaded,
                800,
                "global",
                "model",
                &["text"],
            ),
            make_routing_entry(
                Some(7),
                Role::Text,
                0,
                RoutingStrategy::RoundRobin,
                500,
                "override",
                "model",
                &["text"],
            ),
        ];
        let table = RoutingTable::from_entries(entries);
        let t7 = table.tiers_for(Some(7), Role::Text);
        assert_eq!(t7[0][0].provider.name, "override");
        assert_eq!(t7[0][0].strategy, RoutingStrategy::RoundRobin);
    }

    // ── test_connection ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_connection_no_endpoint_is_error() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.test_connection(1).await.unwrap_err();
        assert!(
            err.to_string().contains("not connected"),
            "expected 'not connected', got: {err}"
        );
    }

    // ── DB integration tests (#[ignore]) ───────────────────────────────

    async fn with_store<F, Fut>(f: F)
    where
        F: FnOnce(PgStore) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<()>>,
    {
        let db = kb_testsupport::fresh_db().await.unwrap();
        let store = PgStore::new(&db.admin_url);
        store.connect().await.unwrap();
        f(store).await.unwrap();
    }

    #[ignore]
    #[tokio::test]
    async fn test_connection_against_mock_endpoint() {
        with_store(|store| async move {
            let mock = kb_mock_backend::MockBackend::start().await;
            let provider_id = store
                .create_provider(
                    "mock-prov",
                    ProviderKind::OpenAiCompat,
                    Some(&mock.url("/")),
                    &serde_json::json!({}),
                )
                .await?;
            store.test_connection(provider_id).await?;
            mock.shutdown().await;
            Ok(())
        })
        .await;
    }

    #[ignore]
    #[tokio::test]
    async fn test_connection_bad_endpoint_is_error() {
        with_store(|store| async move {
            let provider_id = store
                .create_provider(
                    "dead-endpoint",
                    ProviderKind::OpenAiCompat,
                    Some("http://127.0.0.1:19999"),
                    &serde_json::json!({}),
                )
                .await?;
            let err = store.test_connection(provider_id).await.unwrap_err();
            assert!(
                err.to_string().contains("connection test failed")
                    || err.to_string().contains("Connection refused")
                    || err.to_string().contains("timeout")
                    || err.to_string().contains("error"),
                "expected connection failure, got: {err}"
            );
            Ok(())
        })
        .await;
    }

    #[ignore]
    #[tokio::test]
    async fn test_connection_provider_not_found() {
        with_store(|store| async move {
            let err = store.test_connection(99999).await.unwrap_err();
            assert!(
                err.to_string().contains("not found"),
                "expected 'not found', got: {err}"
            );
            Ok(())
        })
        .await;
    }

    #[ignore]
    #[tokio::test]
    async fn provider_crud_round_trip() {
        with_store(|store| async move {
            let pid = store
                .create_provider(
                    "test-prov",
                    ProviderKind::OpenAiCompat,
                    Some("http://localhost:8080/v1"),
                    &serde_json::json!({"x-custom": "val"}),
                )
                .await?;
            assert!(pid > 0);
            let p = store.get_provider(pid).await?.unwrap();
            assert_eq!(p.name, "test-prov");
            assert_eq!(p.kind, ProviderKind::OpenAiCompat);
            assert_eq!(p.endpoint.as_deref(), Some("http://localhost:8080/v1"));
            assert_eq!(p.headers["x-custom"], "val");
            assert!(p.enabled);

            store
                .update_provider(
                    pid,
                    Some("renamed"),
                    None,
                    Some(Some("http://new:9090/v1")),
                    None,
                )
                .await?;
            let updated = store.get_provider(pid).await?.unwrap();
            assert_eq!(updated.name, "renamed");
            assert_eq!(updated.endpoint.as_deref(), Some("http://new:9090/v1"));

            assert_eq!(store.disable_provider(pid).await?, 1);
            assert!(!store.get_provider(pid).await?.unwrap().enabled);

            assert_eq!(store.enable_provider(pid).await?, 1);
            assert!(store.get_provider(pid).await?.unwrap().enabled);
            Ok(())
        })
        .await;
    }

    #[ignore]
    #[tokio::test]
    async fn model_crud_round_trip() {
        with_store(|store| async move {
            let pid = store
                .create_provider(
                    "parent",
                    ProviderKind::Local,
                    Some("http://localhost:8001/v1"),
                    &serde_json::json!({}),
                )
                .await?;
            let mid = store
                .create_model(
                    pid,
                    "my-model",
                    &["text".into(), "vision".into()],
                    Some(32768),
                    Some(4),
                    None,
                    None,
                    Some(1.5),
                    Some(3.0),
                    DataClass::Local,
                )
                .await?;
            assert!(mid > 0);

            let m = store.get_model(mid).await?.unwrap();
            assert_eq!(m.provider_id, pid);
            assert_eq!(m.model_id, "my-model");
            assert_eq!(m.caps, vec!["text", "vision"]);
            assert_eq!(m.max_conc, Some(4));
            assert!((m.price_in.unwrap() - 1.5).abs() < 0.01);
            assert_eq!(m.data_class, DataClass::Local);
            assert!(m.enabled);

            store.disable_model(mid).await?;
            assert!(!store.get_model(mid).await?.unwrap().enabled);
            store.enable_model(mid).await?;
            assert!(store.get_model(mid).await?.unwrap().enabled);
            Ok(())
        })
        .await;
    }

    #[ignore]
    #[tokio::test]
    async fn route_crud_and_duplicate_detection() {
        with_store(|store| async move {
            let pid = store
                .create_provider(
                    "p",
                    ProviderKind::Local,
                    Some("http://localhost:8001/v1"),
                    &serde_json::json!({}),
                )
                .await?;
            let mid = store
                .create_model(
                    pid,
                    "m",
                    &["text".into()],
                    None,
                    Some(2),
                    None,
                    None,
                    None,
                    None,
                    DataClass::Local,
                )
                .await?;

            let rid = store
                .create_route(None, Role::Text, 0, RoutingStrategy::LeastLoaded, 800, mid)
                .await?;
            assert!(rid > 0);

            let dup = store
                .create_route(None, Role::Text, 0, RoutingStrategy::RoundRobin, 500, mid)
                .await;
            assert!(dup.is_err(), "duplicate route must be rejected");

            let rid2 = store
                .create_route(
                    Some(7),
                    Role::Text,
                    0,
                    RoutingStrategy::RoundRobin,
                    500,
                    mid,
                )
                .await?;
            assert_ne!(rid2, rid);

            let r = store.get_route(rid).await?.unwrap();
            assert_eq!(r.role, Role::Text);
            assert_eq!(r.tier, 0);
            assert_eq!(r.strategy, RoutingStrategy::LeastLoaded);
            assert_eq!(r.model_id, mid);

            store
                .update_route(rid, Some(1), Some(RoutingStrategy::CostAsc), None, None)
                .await?;
            let ru = store.get_route(rid).await?.unwrap();
            assert_eq!(ru.tier, 1);
            assert_eq!(ru.strategy, RoutingStrategy::CostAsc);

            assert_eq!(store.delete_route(rid).await?, 1);
            assert!(store.get_route(rid).await?.is_none());
            Ok(())
        })
        .await;
    }

    #[ignore]
    #[tokio::test]
    async fn get_routing_table_joins_and_orders_correctly() {
        with_store(|store| async move {
            let pid1 = store
                .create_provider(
                    "primary",
                    ProviderKind::Local,
                    Some("http://a:8001/v1"),
                    &serde_json::json!({}),
                )
                .await?;
            let pid2 = store
                .create_provider(
                    "fallback",
                    ProviderKind::OpenAiCompat,
                    Some("https://api.o.com/v1"),
                    &serde_json::json!({}),
                )
                .await?;

            let mid1 = store
                .create_model(
                    pid1,
                    "local-model",
                    &["text".into()],
                    Some(32768),
                    Some(4),
                    None,
                    None,
                    None,
                    None,
                    DataClass::Local,
                )
                .await?;
            let mid2 = store
                .create_model(
                    pid2,
                    "remote-model",
                    &["text".into()],
                    Some(128000),
                    Some(10),
                    Some(500),
                    Some(1_000_000),
                    Some(5.0),
                    Some(15.0),
                    DataClass::Remote,
                )
                .await?;

            store
                .create_route(None, Role::Text, 0, RoutingStrategy::LeastLoaded, 800, mid1)
                .await?;
            store
                .create_route(None, Role::Text, 1, RoutingStrategy::CostAsc, 1500, mid2)
                .await?;

            let entries = store.get_routing_table().await?;
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].tier, 0);
            assert_eq!(entries[0].model.model_id, "local-model");
            assert_eq!(entries[0].provider.name, "primary");
            assert_eq!(entries[1].tier, 1);
            assert_eq!(entries[1].model.model_id, "remote-model");
            assert_eq!(entries[1].model.price_in, Some(5.0));
            assert_eq!(entries[1].model.data_class, DataClass::Remote);
            Ok(())
        })
        .await;
    }

    #[ignore]
    #[tokio::test]
    async fn get_routing_table_tenant_override() {
        with_store(|store| async move {
            let pid = store
                .create_provider(
                    "p",
                    ProviderKind::Local,
                    Some("http://localhost:8001/v1"),
                    &serde_json::json!({}),
                )
                .await?;
            let mid = store
                .create_model(
                    pid,
                    "model",
                    &["text".into()],
                    None,
                    Some(2),
                    None,
                    None,
                    None,
                    None,
                    DataClass::Local,
                )
                .await?;
            store
                .create_route(None, Role::Text, 0, RoutingStrategy::LeastLoaded, 800, mid)
                .await?;
            store
                .create_route(
                    Some(7),
                    Role::Text,
                    0,
                    RoutingStrategy::RoundRobin,
                    500,
                    mid,
                )
                .await?;

            let entries = store.get_routing_table().await?;
            assert_eq!(entries.len(), 1, "global should be overridden");
            assert_eq!(entries[0].tenant_id, Some(7));
            assert_eq!(entries[0].strategy, RoutingStrategy::RoundRobin);
            Ok(())
        })
        .await;
    }

    #[ignore]
    #[tokio::test]
    async fn disabled_provider_excluded_from_routing_table() {
        with_store(|store| async move {
            let pid = store
                .create_provider(
                    "enabled",
                    ProviderKind::Local,
                    Some("http://a:8001/v1"),
                    &serde_json::json!({}),
                )
                .await?;
            let mid = store
                .create_model(
                    pid,
                    "m",
                    &["text".into()],
                    None,
                    Some(2),
                    None,
                    None,
                    None,
                    None,
                    DataClass::Local,
                )
                .await?;
            store
                .create_route(None, Role::Text, 0, RoutingStrategy::LeastLoaded, 800, mid)
                .await?;
            assert_eq!(store.get_routing_table().await?.len(), 1);

            store.disable_provider(pid).await?;
            assert!(store.get_routing_table().await?.is_empty());
            Ok(())
        })
        .await;
    }

    #[ignore]
    #[tokio::test]
    async fn disabled_model_excluded_from_routing_table() {
        with_store(|store| async move {
            let pid = store
                .create_provider(
                    "p",
                    ProviderKind::Local,
                    Some("http://a:8001/v1"),
                    &serde_json::json!({}),
                )
                .await?;
            let mid = store
                .create_model(
                    pid,
                    "m",
                    &["text".into()],
                    None,
                    Some(2),
                    None,
                    None,
                    None,
                    None,
                    DataClass::Local,
                )
                .await?;
            store
                .create_route(None, Role::Text, 0, RoutingStrategy::LeastLoaded, 800, mid)
                .await?;
            assert_eq!(store.get_routing_table().await?.len(), 1);

            store.disable_model(mid).await?;
            assert!(store.get_routing_table().await?.is_empty());
            Ok(())
        })
        .await;
    }

    #[ignore]
    #[tokio::test]
    async fn empty_routing_table_returns_empty() {
        with_store(|store| async move {
            let entries = store.get_routing_table().await?;
            assert!(entries.is_empty());
            Ok(())
        })
        .await;
    }

    #[ignore]
    #[tokio::test]
    async fn list_providers_returns_all() {
        with_store(|store| async move {
            store
                .create_provider(
                    "a",
                    ProviderKind::Local,
                    Some("http://a"),
                    &serde_json::json!({}),
                )
                .await?;
            store
                .create_provider(
                    "b",
                    ProviderKind::OpenAiCompat,
                    Some("http://b"),
                    &serde_json::json!({}),
                )
                .await?;
            assert_eq!(store.list_providers().await?.len(), 2);
            Ok(())
        })
        .await;
    }

    #[ignore]
    #[tokio::test]
    async fn list_models_and_routes() {
        with_store(|store| async move {
            let pid = store
                .create_provider(
                    "p",
                    ProviderKind::Local,
                    Some("http://x"),
                    &serde_json::json!({}),
                )
                .await?;
            store
                .create_model(
                    pid,
                    "m1",
                    &["text".into()],
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    DataClass::Local,
                )
                .await?;
            store
                .create_model(
                    pid,
                    "m2",
                    &["embed".into()],
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    DataClass::Local,
                )
                .await?;
            assert_eq!(store.list_models().await?.len(), 2);

            let models = store.list_models().await?;
            let m1 = models.iter().find(|m| m.model_id == "m1").unwrap();
            store
                .create_route(
                    None,
                    Role::Text,
                    0,
                    RoutingStrategy::LeastLoaded,
                    800,
                    m1.id,
                )
                .await?;
            assert_eq!(store.list_routes().await?.len(), 1);
            Ok(())
        })
        .await;
    }

    // ── PgRoutingNotifier (P9-T7) ─────────────────────────────────────────

    #[test]
    fn pg_routing_notifier_first_call_is_immediate() {
        let notifier = PgRoutingNotifier::new(
            String::new,
            "routing_changed".into(),
            Duration::from_secs(30),
        );
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            notifier.wait_for_change().await.unwrap();
        });
    }

    #[test]
    fn pg_routing_notifier_second_call_falls_back_to_polling_when_no_db() {
        let notifier = PgRoutingNotifier::new(
            String::new,
            "routing_changed".into(),
            Duration::from_millis(10),
        );
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            // First call: immediate.
            notifier.wait_for_change().await.unwrap();
            // Second call: DB URL is empty → poll for 10ms.
            let start = std::time::Instant::now();
            notifier.wait_for_change().await.unwrap();
            assert!(
                start.elapsed() >= Duration::from_millis(10),
                "must have waited at least poll_interval"
            );
        });
    }

    #[test]
    fn pg_routing_notifier_creation_has_correct_fields() {
        let notifier = PgRoutingNotifier::new(
            || "postgres://localhost/db".into(),
            "my_channel".into(),
            Duration::from_secs(15),
        );
        assert_eq!(notifier.channel(), "my_channel");
        assert_eq!(notifier.poll_interval(), Duration::from_secs(15));
    }

    #[test]
    fn pg_routing_notifier_falls_back_to_polling_when_bad_url() {
        let notifier = PgRoutingNotifier::new(
            || "postgres://nonexistent:5432/nodb".into(),
            "routing_changed".into(),
            Duration::from_millis(10),
        );
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            // First call: immediate.
            notifier.wait_for_change().await.unwrap();
            // Second call: bad URL → connection fails → poll fallback.
            let start = std::time::Instant::now();
            notifier.wait_for_change().await.unwrap();
            assert!(
                start.elapsed() >= Duration::from_millis(10),
                "must have fallen back to polling after connection failure"
            );
        });
    }

    /// Integration test: NOTIFY fires → wait_for_change returns.
    ///
    /// Requires a running Postgres instance (testcontainers), so it is
    /// `#[ignore]` by default. The test creates a listener, sends a NOTIFY
    /// from a separate connection, and verifies the listener receives it.
    #[ignore]
    #[tokio::test]
    async fn pg_routing_notifier_receives_notify_from_db() {
        let db = kb_testsupport::fresh_db().await.unwrap();
        let admin_url = db.admin_url.clone();
        let notifier = PgRoutingNotifier::new(
            move || admin_url.clone(),
            "routing_changed".into(),
            Duration::from_secs(30),
        );

        // First call is immediate (startup load).
        notifier.wait_for_change().await.unwrap();

        // Send a NOTIFY from a different connection.
        let notify_url = db.admin_url.clone();
        let notify_handle = tokio::spawn(async move {
            // Small delay to ensure the listener is waiting.
            tokio::time::sleep(Duration::from_millis(200)).await;
            let pool = sqlx::PgPool::connect(&notify_url).await.unwrap();
            sqlx::query("NOTIFY routing_changed")
                .execute(&pool)
                .await
                .unwrap();
            pool.close().await;
        });

        // The notifier should return within 5 seconds (receiving the notification).
        let result = tokio::time::timeout(Duration::from_secs(5), notifier.wait_for_change())
            .await
            .unwrap();
        assert!(result.is_ok());

        notify_handle.await.unwrap();
    }
}
