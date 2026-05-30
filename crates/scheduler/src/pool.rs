//! [`Pool`]: the role-indexed registry of backends (plan §6.2, §6.6).

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use kb_config::Config;
use kb_core::role::Role;

use crate::backend::Backend;

/// The scheduler's registry of backends, indexed by the role they serve.
///
/// A backend that serves several roles is registered under each — by a shared
/// `Arc`, so all roles see one slot pool. The pool owns live semaphores and is a
/// long-lived component: build it once at startup with [`Pool::from_config`].
///
/// Hot-reload note (CLAUDE.md): because the semaphores hold in-flight
/// [`crate::Lease`]s, a config change should *reconcile* the registry and timeout
/// in place rather than rebuild the pool wholesale. The acquire path therefore
/// reads the timeout per call via [`Pool::acquire_timeout`] (the live seam); the
/// reconcile step lands with the acquisition algorithm in a later P1 task
/// (plan §6.3, §6.6).
#[derive(Clone)]
pub struct Pool {
    by_role: Arc<DashMap<Role, Vec<Arc<Backend>>>>,
    acquire_timeout: Duration,
}

impl Pool {
    /// Build a pool from explicit backends and an acquire timeout.
    ///
    /// Each backend is registered (by shared `Arc`) under every role it serves.
    pub fn new(backends: Vec<Arc<Backend>>, acquire_timeout: Duration) -> Self {
        let by_role: DashMap<Role, Vec<Arc<Backend>>> = DashMap::new();
        for backend in backends {
            for role in &backend.roles {
                by_role.entry(*role).or_default().push(Arc::clone(&backend));
            }
        }
        Self {
            by_role: Arc::new(by_role),
            acquire_timeout,
        }
    }

    /// Build a pool from the typed configuration (plan §6.6).
    ///
    /// Every `[[backend]]` becomes a [`Backend`] registered under each role it
    /// serves; `acquire_timeout` comes from `scheduler.acquire_timeout_secs`.
    pub fn from_config(cfg: &Config) -> Self {
        let backends = cfg
            .backends
            .iter()
            .map(|b| Arc::new(Backend::from_config(b)))
            .collect();
        Self::new(
            backends,
            Duration::from_secs(cfg.scheduler.acquire_timeout_secs),
        )
    }

    /// The timeout bounding a single acquire wait — read per call (plan §6.3).
    pub fn acquire_timeout(&self) -> Duration {
        self.acquire_timeout
    }

    /// Candidate backends registered for `role`.
    ///
    /// Returns a snapshot clone (cheap `Arc` clones); an unknown role yields an
    /// empty vec. Health filtering and priority ordering happen in the acquire
    /// path, not here (plan §6.3).
    pub fn backends_for(&self, role: Role) -> Vec<Arc<Backend>> {
        self.by_role
            .get(&role)
            .map(|entry| entry.value().clone())
            .unwrap_or_default()
    }

    /// Distinct roles that at least one backend serves.
    pub fn roles(&self) -> Vec<Role> {
        self.by_role.iter().map(|entry| *entry.key()).collect()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use kb_config::{Backend as BackendCfg, Config, Scheduler};

    fn backend_cfg(
        id: &str,
        base_url: &str,
        roles: Vec<Role>,
        slots: u32,
        priority: u8,
    ) -> BackendCfg {
        BackendCfg {
            id: id.into(),
            base_url: base_url.into(),
            roles,
            slots,
            priority,
        }
    }

    /// gpu-a serves text+embed (4 slots); gpu-b serves embed only (2 slots).
    fn two_backend_config() -> Config {
        Config {
            scheduler: Scheduler {
                acquire_timeout_secs: 7,
                ..Default::default()
            },
            backends: vec![
                backend_cfg(
                    "gpu-a",
                    "http://localhost:8001",
                    vec![Role::Text, Role::Embed],
                    4,
                    10,
                ),
                backend_cfg("gpu-b", "http://localhost:8002", vec![Role::Embed], 2, 20),
            ],
            ..Default::default()
        }
    }

    #[test]
    fn from_config_indexes_backends_by_role() {
        let pool = Pool::from_config(&two_backend_config());
        assert_eq!(pool.acquire_timeout(), Duration::from_secs(7));

        let text = pool.backends_for(Role::Text);
        assert_eq!(text.len(), 1);
        assert_eq!(text[0].id, "gpu-a");
        assert_eq!(text[0].free(), 4);

        let embed = pool.backends_for(Role::Embed);
        assert_eq!(embed.len(), 2);

        // A role no backend serves yields an empty candidate set, not a panic.
        assert!(pool.backends_for(Role::Rerank).is_empty());
    }

    #[test]
    fn multi_role_backend_shares_one_slot_pool() {
        let pool = Pool::from_config(&two_backend_config());
        let text = pool.backends_for(Role::Text);
        let text_a = &text[0];
        let embed_a = pool
            .backends_for(Role::Embed)
            .into_iter()
            .find(|b| b.id == "gpu-a")
            .unwrap();

        // gpu-a under `text` and under `embed` is the very same Arc.
        assert!(Arc::ptr_eq(text_a, &embed_a));

        let _permit = text_a.slots.clone().try_acquire_owned().unwrap();
        assert_eq!(embed_a.free(), 3, "one shared semaphore across roles");
    }

    #[test]
    fn roles_lists_every_served_role() {
        // `Role` has no `Ord`, so compare as an unordered set.
        let roles = Pool::from_config(&two_backend_config()).roles();
        assert_eq!(roles.len(), 2);
        assert!(roles.contains(&Role::Text));
        assert!(roles.contains(&Role::Embed));
    }

    #[test]
    fn new_with_no_backends_is_empty() {
        let pool = Pool::new(vec![], Duration::from_secs(1));
        assert!(pool.backends_for(Role::Text).is_empty());
        assert!(pool.roles().is_empty());
        assert_eq!(pool.acquire_timeout(), Duration::from_secs(1));
    }
}
