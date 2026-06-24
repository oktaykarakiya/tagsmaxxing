// SPDX-License-Identifier: AGPL-3.0-or-later

//! [`Lease`]: an RAII guard holding acquired capacity on a backend
//! (plan §6.1–§6.2, §26.2).

use std::sync::Arc;

use kb_core::role::Role;

use crate::capacity::CapacityPermit;
use crate::priority_wait::PriorityWaiterQueue;

/// An RAII handle to acquired capacity on a backend.
///
/// Holding a `Lease` keeps a [`CapacityPermit`] checked out of the backend's
/// capacity guard; dropping the lease releases the permit, freeing the slot
/// (and refunding token-bucket credits for `Rated` capacity). Carry it for the
/// lifetime of an in-flight request and drop it — explicitly or by leaving
/// scope — when the call completes or fails over to another backend.
///
/// # Drop notification (P9-T12)
///
/// When the lease is dropped, it notifies the associated wait queue (if any),
/// waking the highest-priority waiter for the role that this lease belonged to.
///
/// # `endpoint`
///
/// The `endpoint` field is the backend's base URL, resolved at acquire time.
/// It is always `Some` when a lease is granted (backends without an endpoint
/// are not eligible for capacity-based acquisition — they use native SDKs
/// with their own connection pools).
#[derive(Debug)]
pub struct Lease {
    /// Id of the backend this capacity was taken from.
    pub backend_id: String,
    /// Base URL the request should be sent to (resolved at acquire time).
    pub endpoint: String,
    // The held capacity guard; its `Drop` frees the slot and refunds
    // token-bucket credits. Never read directly — the leading underscore
    // documents that and exempts it from the dead-code lint.
    _guard: CapacityPermit,
    /// Optional wait queue to notify when the lease is dropped (P9-T12).
    /// When `Some`, the `(role, queue)` pair is used to wake the
    /// highest-priority waiter for this role.
    _on_drop: Option<LeaseDropNotify>,
}

/// Data needed to notify a wait queue when a lease is dropped.
#[derive(Debug, Clone)]
struct LeaseDropNotify {
    role: Role,
    queue: Arc<PriorityWaiterQueue>,
}

impl Lease {
    /// Wrap an acquired capacity permit together with the backend it was taken from.
    ///
    /// No drop notification is set — use [`Lease::with_notify`] if P9-T12
    /// priority-aware waiter waking is needed.
    pub fn new(
        backend_id: impl Into<String>,
        endpoint: impl Into<String>,
        guard: CapacityPermit,
    ) -> Self {
        Self {
            backend_id: backend_id.into(),
            endpoint: endpoint.into(),
            _guard: guard,
            _on_drop: None,
        }
    }

    /// Attach a wait-queue notification to this lease (P9-T12).
    ///
    /// When the lease is dropped, the queue will be notified for `role`,
    /// waking the highest-priority waiter.
    #[must_use]
    pub fn with_notify(mut self, role: Role, queue: Arc<PriorityWaiterQueue>) -> Self {
        self._on_drop = Some(LeaseDropNotify { role, queue });
        self
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        tracing::debug!(backend_id = %self.backend_id, "released slot");
        if let Some(ref notify) = self._on_drop {
            // The capacity permit's drop already ran (Rust drops fields in
            // declaration order, so `_guard` drops before `_on_drop`). Now
            // notify the highest-priority waiter that a slot is free.
            let queue = Arc::clone(&notify.queue);
            let role = notify.role;
            tokio::spawn(async move {
                queue.notify(role).await;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use std::sync::Arc;

    use super::*;

    use crate::capacity::Capacity;

    #[test]
    fn lease_holds_a_slot_from_slots_capacity_then_frees_on_drop() {
        let cap = Capacity::new_slots(2);
        assert_eq!(cap.free(), 2);

        let guard = cap.try_acquire().unwrap();
        let lease = Lease::new("b1", "http://x", guard);

        assert_eq!(lease.backend_id, "b1");
        assert_eq!(lease.endpoint, "http://x");
        assert_eq!(cap.free(), 1, "slot is held while leased");

        drop(lease);
        assert_eq!(cap.free(), 2, "slot returns on drop");
    }

    #[tokio::test]
    async fn lease_with_notify_triggers_wait_queue_on_drop() {
        let cap = Capacity::new_slots(1);
        let guard = cap.try_acquire().unwrap();
        let q = Arc::new(PriorityWaiterQueue::default());

        // Register a waiter before dropping the lease.
        let mut ticket = q.register(Role::Text, 10).await;

        let lease = Lease::new("b1", "http://x", guard).with_notify(Role::Text, Arc::clone(&q));

        // Dropping the lease should fire the notification, waking the waiter.
        drop(lease);

        // Give the async notification a moment to fire.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // The ticket should have been resolved.
        assert!(ticket.wait().await.is_ok());
    }
}
