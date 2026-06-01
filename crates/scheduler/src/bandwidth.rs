//! Per-backend bandwidth throttle (plan §26.2, P9-T13).
//!
//! A [`BandwidthLimiter`] bounds throughput to a configured MB/s using the
//! same [`TokenBucket`](crate::capacity::TokenBucket) + [`Clock`](crate::capacity::Clock)
//! pattern as the RPM/TPM rate limiters in [`Rated`](crate::capacity::Capacity::Rated).
//!
//! When attached to a [`Backend`](crate::backend::Backend), it gates every capacity
//! acquisition: if the bandwidth bucket is exhausted, the backend is excluded from
//! candidate lists, throttling total in-flight bytes to the configured limit.

use std::sync::Arc;

use crate::capacity::{Clock, TokenBucket};

/// Current multiplier for converting MB/s to bytes/sec.
const BYTES_PER_MB: f64 = 1_000_000.0;

/// A per-backend bandwidth rate limiter that bounds byte throughput to `rate_mbps`.
///
/// Internally delegates to a [`TokenBucket`] whose rate is `rate_mbps * 1_000_000`
/// bytes/second. The bucket starts **full** (burst capacity = one second's worth).
///
/// Each capacity acquisition should call [`try_consume_bytes`](Self::try_consume_bytes)
/// with an estimated byte count; if the bucket can't provide that many tokens, the
/// backend is treated as exhausted for bandwidth purposes.
///
/// # Hot-swappable configuration
///
/// To change the bandwidth limit at runtime, replace the entire limiter via
/// [`Backend::set_bandwidth_limit_mbps`](crate::backend::Backend::set_bandwidth_limit_mbps),
/// which stores a new [`BandwidthLimiter`] behind an
/// [`ArcSwap`](https://docs.rs/arc-swap).  The old limiter (and its accumulated
/// depletion) is dropped; the new one starts at full capacity.
#[derive(Debug, Clone)]
pub struct BandwidthLimiter {
    bucket: TokenBucket,
    /// The configured rate in MB/s, stored for observability.
    rate_mbps: f64,
}

impl BandwidthLimiter {
    /// Create a bandwidth limiter enforcing at most `rate_mbps` megabytes per second.
    ///
    /// # Panics
    ///
    /// Panics if `rate_mbps` is negative (checked in debug via debug_assert).
    pub fn new(rate_mbps: f64, clock: Arc<dyn Clock>) -> Self {
        debug_assert!(
            rate_mbps >= 0.0,
            "bandwidth rate must be non-negative, got {rate_mbps}"
        );
        let bytes_per_sec = rate_mbps * BYTES_PER_MB;
        // Bucket starts full: one second of burst capacity.
        // Rate = bytes_per_sec, so it refills fully in 1 second.
        Self {
            bucket: TokenBucket::new(bytes_per_sec, bytes_per_sec, clock),
            rate_mbps,
        }
    }

    /// Attempt to consume `bytes` from the bandwidth budget.
    ///
    /// Returns `true` if at least `bytes` tokens were available after internal refill.
    pub fn try_consume_bytes(&self, bytes: f64) -> bool {
        self.bucket.try_consume(bytes)
    }

    /// Current available bytes (after refill).
    #[must_use]
    pub fn available_bytes(&self) -> f64 {
        self.bucket.available()
    }

    /// The configured bandwidth limit in MB/s.
    #[must_use]
    pub fn rate_mbps(&self) -> f64 {
        self.rate_mbps
    }

    /// Whether the limiter has at least `threshold` bytes available.
    ///
    /// A backend with `available_bytes() < threshold` should be excluded from
    /// candidate lists.
    #[must_use]
    pub fn is_usable(&self, threshold: f64) -> bool {
        self.bucket.available() >= threshold
    }
}

/// The default minimum bytes threshold for checking bandwidth availability.
///
/// If the bandwidth bucket has fewer than this many bytes, the backend is
/// considered bandwidth-exhausted for filtering purposes.  At 1 MB, this
/// means the backend is still usable as long as its bucket has at least
/// ~1 MB of headroom.
const MIN_BYTES_THRESHOLD: f64 = 1_000_000.0;

impl BandwidthLimiter {
    /// Whether the bandwidth bucket has the default minimum tokens available.
    ///
    /// Convenience wrapper around [`is_usable`](Self::is_usable) with
    /// [`MIN_BYTES_THRESHOLD`].
    #[must_use]
    pub fn bandwidth_available(&self) -> bool {
        self.is_usable(MIN_BYTES_THRESHOLD)
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use std::time::{Duration, Instant};

    use crate::capacity::FakeClock;

    use super::*;

    fn test_clock() -> Arc<FakeClock> {
        Arc::new(FakeClock::new(Instant::now()))
    }

    /// A fresh bucket starts full.
    #[test]
    fn fresh_bucket_starts_full() {
        let clock = test_clock();
        let limiter = BandwidthLimiter::new(10.0, Arc::clone(&clock) as Arc<dyn Clock>); // 10 MB/s
        // 10 MB/s → max burst = 10_000_000 bytes.
        assert_eq!(limiter.rate_mbps(), 10.0);
        assert!((limiter.available_bytes() - 10_000_000.0).abs() < 1.0);
        assert!(limiter.bandwidth_available());
    }

    /// Consuming bytes drains the bucket.
    #[test]
    fn consume_bytes_drains_bucket() {
        let clock = test_clock();
        let limiter = BandwidthLimiter::new(5.0, clock); // 5 MB/s
        let initial = limiter.available_bytes();

        assert!(limiter.try_consume_bytes(2_000_000.0));
        assert!((limiter.available_bytes() - (initial - 2_000_000.0)).abs() < 1.0);
    }

    /// Trying to consume more than available fails without changing state.
    #[test]
    fn consume_too_much_fails() {
        let clock = test_clock();
        let limiter = BandwidthLimiter::new(1.0, clock); // 1 MB/s → 1_000_000 burst
        assert!(limiter.try_consume_bytes(900_000.0));
        assert!(!limiter.try_consume_bytes(200_000.0));
        // State unchanged: still ~100_000 bytes available (1M - 900K = 100K).
        assert!(limiter.available_bytes() < 101_000.0);
        assert!(limiter.available_bytes() > 99_000.0);
    }

    /// The bucket refills over time at the correct rate.
    #[test]
    fn refill_over_time() {
        let clock = Arc::new(FakeClock::new(Instant::now()));
        let limiter = BandwidthLimiter::new(2.0, Arc::clone(&clock) as Arc<dyn Clock>); // 2 MB/s
        let initial = limiter.available_bytes(); // 2_000_000

        // Drain the bucket.
        assert!(limiter.try_consume_bytes(initial));
        assert!(limiter.available_bytes() < 1.0);

        // Advance 0.4 seconds → 800_000 bytes refilled (still < 1 MB threshold).
        clock.advance(Duration::from_millis(400));
        assert!(limiter.available_bytes() > 790_000.0);
        assert!(!limiter.bandwidth_available(), "800K < 1 MB threshold");

        // Advance another 0.2 seconds → 1_200_000 bytes total refilled (now ≥ 1 MB).
        clock.advance(Duration::from_millis(200));
        assert!(limiter.available_bytes() >= 1_000_000.0);
        assert!(limiter.bandwidth_available(), "1.2M ≥ 1 MB threshold");
    }

    /// is_usable with a custom threshold works correctly.
    #[test]
    fn is_usable_custom_threshold() {
        let clock = test_clock();
        let limiter = BandwidthLimiter::new(5.0, clock); // 5_000_000 byte burst
        assert!(limiter.is_usable(4_000_000.0)); // 5M >= 4M
        assert!(!limiter.is_usable(6_000_000.0)); // 5M < 6M
    }

    /// bandwidth_available uses the default 1 MB threshold.
    #[test]
    fn bandwidth_available_default_threshold() {
        let clock = test_clock();
        let limiter = BandwidthLimiter::new(0.5, clock); // 0.5 MB/s → 500K burst
        assert!(
            !limiter.bandwidth_available(),
            "500K burst < 1 MB threshold"
        );
    }

    /// Zero rate means no bandwidth — bucket is empty from the start.
    #[test]
    fn zero_rate_bucket_immediately_exhausted() {
        let clock = test_clock();
        let limiter = BandwidthLimiter::new(0.0, clock);
        assert!(limiter.available_bytes() < 1.0);
        assert!(!limiter.bandwidth_available());
        assert!(!limiter.try_consume_bytes(1.0));
    }

    /// Clone shares the bucket state.
    #[test]
    fn clone_shares_state() {
        let clock = test_clock();
        let lim1 = BandwidthLimiter::new(3.0, clock);
        let lim2 = lim1.clone();

        assert!(lim1.try_consume_bytes(1_000_000.0));
        // lim2 sees the same reduced capacity.
        assert!((lim2.available_bytes() - 2_000_000.0).abs() < 1.0); // 3M - 1M = 2M
    }

    /// A high rate allows many concurrent byte consumptions.
    #[test]
    fn high_rate_allows_many_concurrent() {
        let clock = test_clock();
        let limiter = BandwidthLimiter::new(100.0, clock); // 100 MB/s → 100M burst
        for _ in 0..50 {
            assert!(limiter.try_consume_bytes(1_000_000.0)); // 1 MB each
        }
        // Still ~50_000_000 bytes remain (100M - 50 × 1M).
        assert!(limiter.available_bytes() > 49_000_000.0);
    }

    /// Exhaustion: continuous consumption at exactly the rate limit.
    #[test]
    fn exhaustion_at_rate_limit() {
        let clock = Arc::new(FakeClock::new(Instant::now()));
        let limiter = BandwidthLimiter::new(1.0, Arc::clone(&clock) as Arc<dyn Clock>); // 1 MB/s

        // Consume all 1M immediately (the full burst).
        assert!(limiter.try_consume_bytes(1_000_000.0));
        assert!(!limiter.bandwidth_available());

        // Advance 1s → 1M refilled.
        clock.advance(Duration::from_secs(1));
        assert!(limiter.try_consume_bytes(1_000_000.0));
        assert!(limiter.available_bytes() < 1.0);
    }

    /// Refill bounded by max capacity (1-second burst window).
    #[test]
    fn refill_bounded_by_burst_capacity() {
        let clock = Arc::new(FakeClock::new(Instant::now()));
        let limiter = BandwidthLimiter::new(5.0, Arc::clone(&clock) as Arc<dyn Clock>); // 5 MB/s
        let burst = limiter.available_bytes(); // 5_000_000

        // Consume 1 MB, leaving 4 MB.
        assert!(limiter.try_consume_bytes(1_000_000.0));

        // Advance far more than needed (10 seconds).
        clock.advance(Duration::from_secs(10));
        // Should be capped at the burst capacity (5 MB), not 10s × 5 MB/s = 50 MB.
        assert!((limiter.available_bytes() - burst).abs() < 1.0);
    }

    /// rate_mbps() returns the configured rate.
    #[test]
    fn rate_mbps_returns_configured_value() {
        let clock = test_clock();
        let limiter = BandwidthLimiter::new(42.5, clock);
        assert_eq!(limiter.rate_mbps(), 42.5);
    }
}
