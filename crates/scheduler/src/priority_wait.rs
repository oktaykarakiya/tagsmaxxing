//! Priority-ordered waiter queue for the scheduler (P9-T12).
//!
//! When multiple callers are waiting for a backend slot, the highest-priority
//! waiter should get the newly-freed slot first. This module provides
//! [`PriorityWaiterQueue`] — a shared, role-indexed registry that dispatches
//! slot-free notifications to the highest-priority waiting caller.
//!
//! # How it works
//!
//! 1. A caller that cannot acquire immediately registers in the queue with
//!    its [`Role`] and [`priority`](PriorityWaiterQueue::register), receiving
//!    a oneshot [`Receiver`](tokio::sync::oneshot::Receiver).
//! 2. The caller races the receiver against its global acquire timeout.
//! 3. When a lease is dropped (slot freed), [`PriorityWaiterQueue::notify`]
//!    fires the highest-priority waiter's oneshot for that role.
//! 4. The woken caller retries the fast-path — which should now succeed
//!    because a slot was just freed.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use kb_core::role::Role;
use tokio::sync::{Mutex, oneshot};

/// A priority-ordered waiter registry, indexed by [`Role`].
///
/// Thread-safe and cheap-to-clone: the internal state is behind a
/// `tokio::sync::Mutex` wrapped in an `Arc`.
#[derive(Clone)]
pub struct PriorityWaiterQueue {
    inner: Arc<Mutex<Inner>>,
}

impl std::fmt::Debug for PriorityWaiterQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PriorityWaiterQueue")
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
struct Inner {
    /// Monotonically increasing ticket counter for stable ordering when two
    /// waiters have the same priority (earlier waiter wins).
    next_id: u64,
    /// Waiters grouped by role. For each priority level (high to low), a list
    /// of `(ticket_id, sender)` pairs in FIFO order.
    by_role: HashMap<Role, RoleWaiters>,
}

/// Waiters for a single role, keyed by priority (descending — highest first).
///
/// Each priority bucket holds a `Vec` of `(ticket_id, oneshot::Sender)`. When
/// a slot is freed for this role, the entry with the highest priority and
/// lowest ticket_id is popped and its sender fired.
type RoleWaiters = BTreeMap<i32, Vec<(u64, oneshot::Sender<()>)>>;

impl PriorityWaiterQueue {
    /// Create a new empty priority waiter queue.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::default())),
        }
    }

    /// Register a waiter for `role` with the given `priority` (higher = more
    /// urgent).
    ///
    /// Returns a [`Ticket`] that the caller can `await` on to be woken when a
    /// slot is freed and this waiter is the highest-priority one waiting.
    pub async fn register(&self, role: Role, priority: i32) -> Ticket {
        let (tx, rx) = oneshot::channel();
        let mut inner = self.inner.lock().await;
        let ticket_id = inner.next_id;
        inner.next_id = inner.next_id.wrapping_add(1);

        let role_waiters = inner.by_role.entry(role).or_default();
        role_waiters
            .entry(priority)
            .or_default()
            .push((ticket_id, tx));

        Ticket { rx }
    }

    /// Notify the highest-priority waiter for `role` that a slot was freed.
    ///
    /// If the highest-priority waiter's receiver has been dropped (e.g. caller
    /// timed out), the stale entry is removed and the next-highest waiter is
    /// tried. This continues until a live waiter is found or the queue is empty.
    ///
    /// If no waiter is registered for the role, this is a no-op. The caller
    /// does not need to check whether anyone was woken — the waiter will
    /// retry the fast-path which may or may not succeed.
    pub async fn notify(&self, role: Role) {
        let mut inner = self.inner.lock().await;
        if let Some(role_waiters) = inner.by_role.get_mut(&role) {
            // Loop: try the highest-priority waiter. If its receiver is already
            // dropped (Err on send), remove it and try the next one.
            loop {
                let highest_key = {
                    let keys: Vec<i32> = role_waiters.keys().copied().collect();
                    keys.into_iter().max()
                };

                let prio = match highest_key {
                    Some(k) => k,
                    None => break, // No waiters left.
                };

                let entry = role_waiters.get_mut(&prio);
                let Some(waiters) = entry else { break };
                if waiters.is_empty() {
                    role_waiters.remove(&prio);
                    continue;
                }

                let (_, tx) = waiters.remove(0);
                if waiters.is_empty() {
                    role_waiters.remove(&prio);
                }

                // Try to fire the sender. If the receiver was dropped (Err),
                // the waiter timed out — skip to the next one.
                if tx.send(()).is_ok() {
                    break; // Successfully woken a live waiter.
                }
                // Receiver dropped — continue to next waiter.
            }
        }
    }
}

impl Default for PriorityWaiterQueue {
    fn default() -> Self {
        Self::new()
    }
}

/// A oneshot-based ticket that a caller awaits to be woken when a slot frees.
///
/// Created by [`PriorityWaiterQueue::register`]. The caller should race this
/// receiver against its global acquire timeout.
///
/// When dropped without being consumed, the ticket's oneshot channel is simply
/// abandoned. The sender side will fire into the void on the next notify (a
/// no-op), and the entry was already popped from the queue by notify. This is
/// acceptable because the waiter is only unregistered by notify (not by drop).
pub struct Ticket {
    rx: oneshot::Receiver<()>,
}

impl Ticket {
    /// Await the notification. Returns `Ok(())` when woken, or
    /// `Err(oneshot::error::RecvError)` if the sender was dropped before
    /// sending (which should not happen in normal operation).
    pub async fn wait(&mut self) -> Result<(), oneshot::error::RecvError> {
        (&mut self.rx).await
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn register_and_notify_wakes_waiter() {
        let q = Arc::new(PriorityWaiterQueue::new());
        let mut ticket = q.register(Role::Text, 5).await;

        // Notify for a different role should NOT wake our waiter.
        q.notify(Role::Embed).await;

        // After a brief delay, the waiter should still be pending.
        tokio::select! {
            _ = ticket.wait() => panic!("should not have been woken yet"),
            _ = tokio::time::sleep(Duration::from_millis(10)) => {},
        }

        // Notify for the correct role should wake our waiter.
        q.notify(Role::Text).await;
        tokio::select! {
            res = ticket.wait() => assert!(res.is_ok(), "should have been woken"),
            _ = tokio::time::sleep(Duration::from_millis(50)) => panic!("timed out waiting for wake"),
        }
    }

    #[tokio::test]
    async fn highest_priority_woken_first() {
        let q = Arc::new(PriorityWaiterQueue::new());

        // Register three waiters with different priorities.
        let mut low = q.register(Role::Text, 0).await;
        let mut high = q.register(Role::Text, 100).await;
        let mut mid = q.register(Role::Text, 50).await;

        // First notify → highest priority (100) gets woken.
        q.notify(Role::Text).await;

        let mut woken = String::new();
        tokio::select! {
            res = high.wait() => {
                assert!(res.is_ok());
                woken.push_str("high ");
            }
            _ = tokio::time::sleep(Duration::from_millis(50)) => panic!("high should wake first"),
        }

        // Second notify → next highest (50) gets woken.
        q.notify(Role::Text).await;
        tokio::select! {
            res = mid.wait() => {
                assert!(res.is_ok());
                woken.push_str("mid ");
            }
            _ = tokio::time::sleep(Duration::from_millis(50)) => panic!("mid should wake second"),
        }

        // Third notify → lowest (0) gets woken.
        q.notify(Role::Text).await;
        tokio::select! {
            res = low.wait() => {
                assert!(res.is_ok());
                woken.push_str("low");
            }
            _ = tokio::time::sleep(Duration::from_millis(50)) => panic!("low should wake third"),
        }

        assert_eq!(woken, "high mid low");
    }

    #[tokio::test]
    async fn same_priority_fifo_order() {
        let q = Arc::new(PriorityWaiterQueue::new());

        let mut first = q.register(Role::Text, 10).await;
        let mut second = q.register(Role::Text, 10).await;

        // First notify → earliest registered (first) is woken.
        q.notify(Role::Text).await;
        tokio::select! {
            res = first.wait() => assert!(res.is_ok()),
            _ = tokio::time::sleep(Duration::from_millis(50)) => panic!("first should wake"),
        }

        // Second notify → next (second) is woken.
        q.notify(Role::Text).await;
        tokio::select! {
            res = second.wait() => assert!(res.is_ok()),
            _ = tokio::time::sleep(Duration::from_millis(50)) => panic!("second should wake"),
        }
    }

    #[tokio::test]
    async fn notify_no_waiters_is_noop() {
        let q = PriorityWaiterQueue::new();
        // Should not panic or hang when no one is waiting.
        q.notify(Role::Text).await;
        q.notify(Role::Rerank).await;
    }

    #[tokio::test]
    async fn dropped_ticket_does_not_panic() {
        let q = Arc::new(PriorityWaiterQueue::new());

        // Register a high-priority waiter and immediately drop it.
        {
            let _ticket = q.register(Role::Text, 100).await;
            // Dropped here.
        }

        // Give async cleanup a moment.
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Register a low-priority waiter.
        let mut low = q.register(Role::Text, 5).await;

        // Notify should wake the low-priority waiter (the dropped one is gone).
        q.notify(Role::Text).await;
        tokio::select! {
            res = low.wait() => assert!(res.is_ok()),
            _ = tokio::time::sleep(Duration::from_millis(50)) => panic!("low should be woken after high was dropped"),
        }
    }
}
