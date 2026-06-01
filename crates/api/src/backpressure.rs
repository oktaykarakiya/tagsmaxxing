//! In-flight request limiter for backpressure (plan §22, P8-T9).
//!
//! Caps the number of concurrent ingest requests to prevent resource
//! exhaustion. When the limit is reached the caller receives `None` from
//! [`try_acquire`](InflightLimiter::try_acquire) — the API handler can then
//! return `429 Too Many Requests` with a `Retry-After` header.

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Caps in-flight ingest requests with a [`Semaphore`].
///
/// # Cloning
///
/// The struct is cheap to clone — the underlying [`Semaphore`] is
/// [`Arc`]-wrapped and shared across clones.
#[derive(Clone, Debug)]
pub struct InflightLimiter {
    semaphore: Arc<Semaphore>,
    max: usize,
}

impl InflightLimiter {
    /// Create a new limiter with `max` concurrent permits.
    ///
    /// When `max` is `0` the limiter is disabled — [`try_acquire`](Self::try_acquire)
    /// always succeeds regardless of concurrency.
    #[must_use]
    pub fn new(max: usize) -> Self {
        // When max is 0 (disabled), use the maximum possible permits so
        // try_acquire never blocks. tokio::sync::Semaphore::MAX_PERMITS is
        // the hard upper bound. We subtract a safety margin so in_flight()
        // arithmetic doesn't overflow.
        let permits = if max == 0 {
            Semaphore::MAX_PERMITS / 2
        } else {
            max
        };
        Self {
            semaphore: Arc::new(Semaphore::new(permits)),
            max,
        }
    }

    /// Try to acquire a permit without waiting.
    ///
    /// Returns `Some` with a RAII permit when capacity is available.
    /// Returns `None` when the limit is reached — the caller should
    /// return `429 Too Many Requests`.
    pub fn try_acquire(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.semaphore).try_acquire_owned().ok()
    }

    /// Number of currently in-flight requests (approximate — read without
    /// locking, so concurrent acquires/releases may make it slightly stale).
    /// Returns `0` when disabled.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        if self.max == 0 {
            return 0;
        }
        self.max.saturating_sub(self.semaphore.available_permits())
    }

    /// Maximum concurrent requests this limiter allows.
    /// Returns `0` when disabled (unlimited).
    #[must_use]
    pub fn max(&self) -> usize {
        self.max
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::Arc;

    use super::*;

    #[test]
    fn acquire_below_limit() {
        let limiter = InflightLimiter::new(2);
        let p1 = limiter.try_acquire();
        assert!(p1.is_some());
        let p2 = limiter.try_acquire();
        assert!(p2.is_some());
    }

    #[test]
    fn none_when_saturated() {
        let limiter = InflightLimiter::new(1);
        let p1 = limiter.try_acquire();
        assert!(p1.is_some());
        let p2 = limiter.try_acquire();
        assert!(p2.is_none());
    }

    #[test]
    fn release_frees_capacity() {
        let limiter = InflightLimiter::new(1);
        let p = limiter.try_acquire().unwrap();
        drop(p);
        // After dropping, we can acquire again.
        assert!(limiter.try_acquire().is_some());
    }

    #[test]
    fn in_flight_counts_correctly() {
        let limiter = InflightLimiter::new(3);
        assert_eq!(limiter.in_flight(), 0);
        let p1 = limiter.try_acquire().unwrap();
        assert_eq!(limiter.in_flight(), 1);
        let p2 = limiter.try_acquire().unwrap();
        assert_eq!(limiter.in_flight(), 2);
        drop(p1);
        assert_eq!(limiter.in_flight(), 1);
        drop(p2);
        assert_eq!(limiter.in_flight(), 0);
    }

    #[test]
    fn disabled_always_acquires() {
        let limiter = InflightLimiter::new(0);
        assert_eq!(limiter.max(), 0);
        assert_eq!(limiter.in_flight(), 0);
        // With a very large permit pool, many concurrent acquires succeed.
        let permits: Vec<_> = (0..100).map(|_| limiter.try_acquire().unwrap()).collect();
        assert_eq!(permits.len(), 100);
        drop(permits);
    }

    #[test]
    fn arc_shared_across_clones() {
        let lim1 = Arc::new(InflightLimiter::new(1));
        let lim2 = Arc::clone(&lim1);
        let p = lim1.try_acquire().unwrap();
        assert!(lim2.try_acquire().is_none());
        drop(p);
        assert!(lim2.try_acquire().is_some());
    }

    #[test]
    fn in_flight_zero_when_disabled() {
        let limiter = InflightLimiter::new(0);
        // When disabled (max=0), in_flight always returns 0.
        assert_eq!(limiter.in_flight(), 0);
        let _permits: Vec<_> = (0..5).map(|_| limiter.try_acquire().unwrap()).collect();
        // in_flight is still 0 (disabled path).
        assert_eq!(limiter.in_flight(), 0);
    }
}
