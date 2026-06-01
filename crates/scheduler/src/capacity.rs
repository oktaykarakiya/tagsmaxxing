//! Pluggable capacity guards (plan §26.2).
//!
//! The [`Capacity`] enum replaces the hardcoded [`tokio::sync::Semaphore`] in
//! [`crate::Backend`] with three variants:
//!
//! - `Slots(Arc<Semaphore>)` — local inference server with `--parallel N` slots.
//! - `Concurrency(Arc<Semaphore>)` — remote API with operator-chosen max in-flight.
//! - `Rated { conc, rpm, tpm }` — remote API with documented RPM/TPM limits
//!   enforced by [`TokenBucket`]s.
//!
//! A [`TokenBucket`] uses an injected [`Clock`] so rate-limit math is
//! deterministic in tests.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

// ── Clock abstraction ──────────────────────────────────────────────────────

/// A clock abstraction so [`TokenBucket`] refill math is deterministic in tests.
///
/// In production, use [`WallClock`]; in tests, inject a [`FakeClock`].
pub trait Clock: Send + Sync + std::fmt::Debug {
    /// The current instant according to this clock.
    fn now(&self) -> Instant;
}

/// The real system clock.
#[derive(Debug, Clone, Copy)]
pub struct WallClock;

impl Clock for WallClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// A fake clock whose time advances only when [`FakeClock::advance`] is called.
///
/// Used in tests to deterministically control token-bucket refill timing.
// Constructed only in tests; silence dead-code warning outside test builds.
#[allow(dead_code)]
#[derive(Debug)]
pub struct FakeClock {
    now: Mutex<Instant>,
}

#[allow(dead_code)]
impl FakeClock {
    /// Create a fake clock starting at `start`.
    pub fn new(start: Instant) -> Self {
        Self {
            now: Mutex::new(start),
        }
    }

    /// Advance the clock by `d`.
    pub fn advance(&self, d: std::time::Duration) {
        let mut n = self.now.lock().unwrap_or_else(|e| e.into_inner());
        *n += d;
    }
}

impl Clock for FakeClock {
    fn now(&self) -> Instant {
        *self.now.lock().unwrap_or_else(|e| e.into_inner())
    }
}

// ── Token bucket ───────────────────────────────────────────────────────────

/// Inner state of a [`TokenBucket`], protected by a [`Mutex`].
///
/// All public operations on [`TokenBucket`] acquire this lock briefly (microseconds),
/// never across an `.await` point, so `std::sync::Mutex` is fine here.
#[derive(Debug)]
struct TokenBucketInner {
    clock: Arc<dyn Clock>,
    /// Tokens refilled per second (fractional rates supported, e.g. 0.5 = every 2 s).
    rate: f64,
    /// Maximum burst capacity (bucket size).
    max_tokens: f64,
    /// Current token count; always in [0, max_tokens].
    tokens: f64,
    /// The instant of the last refill operation.
    last_refill: Instant,
}

/// A token-bucket rate limiter (plan §26.2).
///
/// Refills continuously at `rate` tokens/second, bounded by `max_tokens`.
/// Every operation that inspects the bucket first refills it based on
/// elapsed wall-clock (or fake-clock) time, so the caller never needs to
/// call `refill` manually.
///
/// Cheap to [`Clone`] — the inner state is shared via [`Arc`].
#[derive(Debug, Clone)]
pub struct TokenBucket {
    inner: Arc<Mutex<TokenBucketInner>>,
}

impl TokenBucket {
    /// Create a bucket full to `max_tokens`, refilling at `rate` tokens/second.
    ///
    /// The `clock` is used to measure elapsed time; inject a [`FakeClock`]
    /// for deterministic tests.
    pub fn new(rate: f64, max_tokens: f64, clock: Arc<dyn Clock>) -> Self {
        let now = clock.now();
        Self {
            inner: Arc::new(Mutex::new(TokenBucketInner {
                clock,
                rate,
                max_tokens,
                tokens: max_tokens,
                last_refill: now,
            })),
        }
    }

    /// Refill the bucket based on elapsed time, then attempt to consume `n` tokens.
    ///
    /// Returns `true` if at least `n` tokens were available after refill.
    pub fn try_consume(&self, n: f64) -> bool {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.refill();
        if inner.tokens >= n {
            inner.tokens -= n;
            true
        } else {
            false
        }
    }

    /// Return `n` tokens to the bucket, capped at [`max`](Self::max_tokens).
    ///
    /// This does *not* trigger a refill — the tokens are simply added back
    /// to the current balance. Used when a reserved request is cancelled
    /// (plan: "release() returns … token bucket credit").
    pub fn refund(&self, n: f64) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.tokens = (inner.tokens + n).min(inner.max_tokens);
    }

    /// Current token count after a refill, useful for observability (`free`).
    pub fn available(&self) -> f64 {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.refill();
        inner.tokens
    }

    /// The configured refill rate in tokens/second.
    pub fn rate(&self) -> f64 {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).rate
    }

    /// The configured maximum burst capacity.
    pub fn max_tokens(&self) -> f64 {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .max_tokens
    }
}

impl TokenBucketInner {
    /// Refill tokens based on elapsed time since `last_refill`.
    fn refill(&mut self) {
        let now = self.clock.now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        if elapsed > 0.0 && self.tokens < self.max_tokens {
            self.tokens = (self.tokens + elapsed * self.rate).min(self.max_tokens);
        }
        self.last_refill = now;
    }
}

// ── Capacity enum ──────────────────────────────────────────────────────────

/// A pluggable capacity guard for a [`crate::Backend`] (plan §26.2).
///
/// Replaces the single hardcoded `Arc<Semaphore>` with three variants that
/// share a common interface: `try_acquire`, `acquire` (async), `free`, and
/// `is_usable`.
#[derive(Debug, Clone)]
pub enum Capacity {
    /// Local inference (llama.cpp `--parallel N`): semaphore permits == concurrent slots.
    Slots {
        /// The concurrency semaphore; one permit == one in-flight request.
        sem: Arc<Semaphore>,
    },
    /// Remote API with an operator-chosen max in-flight limit.
    Concurrency {
        /// The concurrency semaphore.
        sem: Arc<Semaphore>,
    },
    /// Remote API with documented RPM + TPM limits in addition to a concurrency cap.
    Rated {
        /// Concurrency semaphore.
        sem: Arc<Semaphore>,
        /// Requests-per-minute token bucket.
        rpm: TokenBucket,
        /// Tokens-per-minute token bucket.
        tpm: TokenBucket,
    },
}

impl Capacity {
    /// Create a `Slots` variant with `n` concurrent permits.
    pub fn new_slots(n: usize) -> Self {
        Capacity::Slots {
            sem: Arc::new(Semaphore::new(n)),
        }
    }

    /// Create a `Concurrency` variant with `n` max in-flight requests.
    pub fn new_concurrency(n: usize) -> Self {
        Capacity::Concurrency {
            sem: Arc::new(Semaphore::new(n)),
        }
    }

    /// Create a `Rated` variant.
    ///
    /// `conc` — max concurrent in-flight (semaphore permits).
    /// `rpm_rate` — requests-per-minute limit (tokens refilled per second = rpm_rate / 60).
    /// `tpm_rate` — tokens-per-minute limit (tokens refilled per second = tpm_rate / 60).
    /// `clock` — injected clock; use `Arc::new(WallClock)` in production.
    pub fn new_rated(conc: usize, rpm_rate: f64, tpm_rate: f64, clock: Arc<dyn Clock>) -> Self {
        // RPM bucket: each request consumes 1 token; refill at rpm_rate/60 tokens/sec.
        // TPM bucket: each request consumes 1 token (a placeholder; real token counts
        // are tracked in a later phase).

        Capacity::Rated {
            sem: Arc::new(Semaphore::new(conc)),
            rpm: TokenBucket::new(rpm_rate / 60.0, rpm_rate, Arc::clone(&clock)),
            tpm: TokenBucket::new(tpm_rate / 60.0, tpm_rate, clock),
        }
    }

    /// Synchronously try to acquire capacity.
    ///
    /// Returns [`Some(CapacityPermit)`] on success, or [`None`] if no capacity
    /// is available right now.  The returned permit frees the semaphore slot
    /// (and refunds token-bucket credits for `Rated`) on drop.
    pub fn try_acquire(&self) -> Option<CapacityPermit> {
        match self {
            Capacity::Slots { sem } | Capacity::Concurrency { sem } => sem
                .clone()
                .try_acquire_owned()
                .ok()
                .map(CapacityPermit::Semaphore),
            Capacity::Rated { sem, rpm, tpm } => {
                // Check token buckets first — if they're drained, don't
                // bother with the semaphore.
                if !rpm.try_consume(1.0) {
                    return None;
                }
                if !tpm.try_consume(0.0) {
                    // TPM placeholder: consume 0 tokens per request in P9-T3.
                    // Real token-count tracking arrives in P9-T10.
                }
                match sem.clone().try_acquire_owned() {
                    Ok(permit) => Some(CapacityPermit::Rated {
                        _permit: permit,
                        rpm: rpm.clone(),
                        tpm: tpm.clone(),
                    }),
                    Err(_) => {
                        // Refund the RPM token we already consumed.
                        rpm.refund(1.0);
                        None
                    }
                }
            }
        }
    }

    /// Asynchronously wait for capacity.
    ///
    /// For `Slots`/`Concurrency`, waits on the semaphore.
    /// For `Rated`, waits on the semaphore, then checks token buckets;
    /// if tokens are exhausted, the permit is released and `None` is returned.
    pub async fn acquire(&self) -> Option<CapacityPermit> {
        match self {
            Capacity::Slots { sem } | Capacity::Concurrency { sem } => {
                let sem = Arc::clone(sem);
                sem.acquire_owned()
                    .await
                    .ok()
                    .map(CapacityPermit::Semaphore)
            }
            Capacity::Rated { sem, rpm, tpm } => {
                let sem = Arc::clone(sem);
                let permit = sem.acquire_owned().await.ok()?;
                // After waiting for the semaphore, check token buckets.
                if !rpm.try_consume(1.0) {
                    drop(permit);
                    return None;
                }
                // TPM placeholder (see try_acquire).
                let _ = tpm.try_consume(0.0);
                Some(CapacityPermit::Rated {
                    _permit: permit,
                    rpm: rpm.clone(),
                    tpm: tpm.clone(),
                })
            }
        }
    }

    /// Free concurrency slots right now.
    ///
    /// For `Slots`/`Concurrency`: returns `semaphore.available_permits()`.
    /// For `Rated`: returns the concurrency headroom (semaphore permits);
    /// token-bucket headroom is available via [`is_usable`](Self::is_usable).
    pub fn free(&self) -> usize {
        match self {
            Capacity::Slots { sem }
            | Capacity::Concurrency { sem }
            | Capacity::Rated { sem, .. } => sem.available_permits(),
        }
    }

    /// Whether this capacity guard can serve a new request.
    ///
    /// For `Slots`/`Concurrency` this is always `true` (the caller can wait
    /// even if all permits are held).
    /// For `Rated` this is `false` when the RPM token bucket is exhausted,
    /// because waiting for the semaphore would then waste time — the bucket
    /// refills on its own schedule, not by releasing held permits.
    pub fn is_usable(&self) -> bool {
        match self {
            Capacity::Slots { .. } | Capacity::Concurrency { .. } => true,
            Capacity::Rated { rpm, .. } => rpm.available() >= 1.0,
        }
    }

    /// Total permits initially configured (for observability / metrics).
    pub fn max_concurrency(&self) -> usize {
        match self {
            Capacity::Slots { sem }
            | Capacity::Concurrency { sem }
            | Capacity::Rated { sem, .. } => {
                // `Semaphore` does not expose its max permits, so we derive it
                // from `available_permits + number_of_held_permits` if needed.
                // For now, this is best-effort via the constructor contract.
                sem.available_permits()
            }
        }
    }
}

// ── Capacity permit (returns capacity on drop) ─────────────────────────────

/// An RAII guard holding acquired capacity.
///
/// Dropping the guard returns the semaphore permit (for all variants) and
/// refunds RPM/TPM tokens (for `Rated`).  This is the return type of
/// [`Capacity::try_acquire`] and [`Capacity::acquire`].
#[derive(Debug)]
pub enum CapacityPermit {
    /// A semaphore permit for `Slots` or `Concurrency` variants.
    Semaphore(OwnedSemaphorePermit),
    /// Holds a semaphore permit plus references to the token buckets
    /// for refund on drop.
    Rated {
        /// The semaphore permit; returned to the pool on drop.
        _permit: OwnedSemaphorePermit,
        /// RPM token bucket to refund 1 token on drop.
        rpm: TokenBucket,
        /// TPM token bucket (placeholder; 0 tokens consumed per request).
        tpm: TokenBucket,
    },
}

impl Drop for CapacityPermit {
    fn drop(&mut self) {
        if let CapacityPermit::Rated { rpm, .. } = self {
            rpm.refund(1.0);
        }
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use std::time::Duration;

    // ── Clock tests ─────────────────────────────────────────────────────

    #[test]
    fn wall_clock_returns_now() {
        let before = Instant::now();
        let got = WallClock.now();
        let after = Instant::now();
        assert!(got >= before);
        assert!(got <= after);
    }

    #[test]
    fn fake_clock_starts_at_given_time() {
        let t0 = Instant::now();
        let clock = FakeClock::new(t0);
        assert_eq!(clock.now(), t0);
    }

    #[test]
    fn fake_clock_advances() {
        let t0 = Instant::now();
        let clock = FakeClock::new(t0);
        clock.advance(Duration::from_secs(5));
        assert_eq!(clock.now(), t0 + Duration::from_secs(5));
        clock.advance(Duration::from_millis(100));
        assert_eq!(clock.now(), t0 + Duration::from_millis(5_100));
    }

    // ── Token bucket tests (plan §26.2 TDD) ────────────────────────────

    fn test_clock() -> Arc<FakeClock> {
        Arc::new(FakeClock::new(Instant::now()))
    }

    /// A fresh bucket starts full.
    #[test]
    fn fresh_bucket_is_full() {
        let clock = test_clock();
        let bucket = TokenBucket::new(10.0, 100.0, Arc::clone(&clock) as Arc<dyn Clock>);
        assert_eq!(bucket.available(), 100.0);
    }

    /// Consuming tokens drains the bucket.
    #[test]
    fn consume_drains_bucket() {
        let clock = test_clock();
        let bucket = TokenBucket::new(10.0, 100.0, clock);
        assert!(bucket.try_consume(30.0));
        assert_eq!(bucket.available(), 70.0);
        assert!(bucket.try_consume(50.0));
        assert_eq!(bucket.available(), 20.0);
    }

    /// Trying to consume more than available fails without changing state.
    #[test]
    fn consume_more_than_available_fails() {
        let clock = test_clock();
        let bucket = TokenBucket::new(10.0, 50.0, clock);
        assert!(!bucket.try_consume(60.0));
        assert_eq!(
            bucket.available(),
            50.0,
            "state unchanged after failed consume"
        );
    }

    /// Consuming exactly the available amount drains to zero.
    #[test]
    fn consume_exact_empties_bucket() {
        let clock = test_clock();
        let bucket = TokenBucket::new(10.0, 50.0, clock);
        assert!(bucket.try_consume(50.0));
        assert_eq!(bucket.available(), 0.0);
    }

    /// Zero-consumption always succeeds.
    #[test]
    fn consume_zero_always_succeeds() {
        let clock = test_clock();
        let bucket = TokenBucket::new(10.0, 50.0, clock);
        // Drain then try zero.
        assert!(bucket.try_consume(50.0));
        assert!(bucket.try_consume(0.0));
        assert_eq!(bucket.available(), 0.0);
    }

    /// The bucket refills over time at the configured rate.
    #[test]
    fn refill_over_time() {
        let clock = Arc::new(FakeClock::new(Instant::now()));
        let bucket = TokenBucket::new(10.0, 100.0, Arc::clone(&clock) as Arc<dyn Clock>);

        // Drain half.
        assert!(bucket.try_consume(100.0));
        assert_eq!(bucket.available(), 0.0);

        // Advance 5 seconds at 10 tokens/sec → 50 tokens refilled.
        clock.advance(Duration::from_secs(5));
        // available() refills before returning.
        assert_eq!(bucket.available(), 50.0);
    }

    /// The bucket never exceeds max_tokens.
    #[test]
    fn refill_bounded_by_max() {
        let clock = Arc::new(FakeClock::new(Instant::now()));
        let bucket = TokenBucket::new(100.0, 50.0, Arc::clone(&clock) as Arc<dyn Clock>);

        // Consume 10, leaving 40.
        assert!(bucket.try_consume(10.0));
        assert_eq!(bucket.available(), 40.0);

        // Advance far enough that refill would produce > max.
        clock.advance(Duration::from_secs(10)); // 10s × 100 = 1000 tokens would refill
        assert_eq!(bucket.available(), 50.0, "capped at max_tokens");
    }

    /// Multiple small consumes and refills work correctly.
    #[test]
    fn fractional_rate_tokens_accumulate() {
        // 0.5 tokens/sec means 1 token every 2 seconds.
        let clock = Arc::new(FakeClock::new(Instant::now()));
        let bucket = TokenBucket::new(0.5, 10.0, Arc::clone(&clock) as Arc<dyn Clock>);

        // Drain completely.
        assert!(bucket.try_consume(10.0));
        assert_eq!(bucket.available(), 0.0);

        // After 3s at 0.5/s → 1.5 tokens (floor at consume = 1 whole token consumed).
        clock.advance(Duration::from_secs(3));
        assert!(bucket.available() >= 1.0);
    }

    /// `refund` returns tokens back to the bucket, capped at max.
    #[test]
    fn refund_returns_tokens() {
        let clock = test_clock();
        let bucket = TokenBucket::new(10.0, 100.0, clock);

        assert!(bucket.try_consume(60.0));
        assert_eq!(bucket.available(), 40.0);

        bucket.refund(10.0);
        // available() refills after refund — but elapsed is ~0, so no extra refill.
        assert_eq!(bucket.available(), 50.0);
    }

    /// Refund beyond max_tokens is capped.
    #[test]
    fn refund_capped_at_max() {
        let clock = test_clock();
        let bucket = TokenBucket::new(10.0, 100.0, clock);

        assert!(bucket.try_consume(10.0));
        assert_eq!(bucket.available(), 90.0);

        bucket.refund(50.0);
        assert_eq!(bucket.available(), 100.0, "refund capped at max_tokens");
    }

    /// `rate` and `max_tokens` getters return configured values.
    #[test]
    fn getters_return_configured_values() {
        let clock = test_clock();
        let bucket = TokenBucket::new(7.5, 123.0, clock);
        assert_eq!(bucket.rate(), 7.5);
        assert_eq!(bucket.max_tokens(), 123.0);
    }

    /// Refill is idempotent: calling `available()` multiple times in quick
    /// succession without advancing the clock returns the same value.
    #[test]
    fn refill_idempotent_without_time_passage() {
        let clock = Arc::new(FakeClock::new(Instant::now()));
        let bucket = TokenBucket::new(10.0, 100.0, Arc::clone(&clock) as Arc<dyn Clock>);
        assert!(bucket.try_consume(20.0));
        let a = bucket.available();
        let b = bucket.available();
        assert_eq!(a, b, "same time → same refill result");
    }

    /// Clone shares the same bucket state.
    #[test]
    fn clone_shares_state() {
        let clock = test_clock();
        let b1 = TokenBucket::new(10.0, 100.0, clock);
        let b2 = b1.clone();
        assert!(b1.try_consume(30.0));
        assert_eq!(b2.available(), 70.0);
        b2.refund(10.0);
        assert_eq!(b1.available(), 80.0);
    }

    /// Burst: a bucket just under max can consume immediately after a
    /// tiny refill (testing edge of available tokens).
    #[test]
    fn burst_just_under_max() {
        let clock = Arc::new(FakeClock::new(Instant::now()));
        let bucket = TokenBucket::new(10.0, 10.0, Arc::clone(&clock) as Arc<dyn Clock>);

        // Consume 9.9, leaving 0.1.
        assert!(bucket.try_consume(9.9));
        assert!(!bucket.try_consume(1.0), "0.1 < 1.0");

        // Advance 0.1s → 1.0 tokens refilled, total = 1.1.
        clock.advance(Duration::from_millis(100));
        assert!(bucket.try_consume(1.0), "1.1 >= 1.0");
        // ~0.1 remains (floating point).
        assert!(bucket.available() < 0.2);
    }

    /// Exhaustion: continuous consumption at the rate limit should
    /// deplete the bucket to near zero.
    #[test]
    fn exhaustion_at_rate_limit() {
        let clock = Arc::new(FakeClock::new(Instant::now()));
        // 5 tokens/sec, max burst 10.
        let bucket = TokenBucket::new(5.0, 10.0, Arc::clone(&clock) as Arc<dyn Clock>);

        // Consume all 10 immediately (burst).
        assert!(bucket.try_consume(10.0));
        assert_eq!(bucket.available(), 0.0);

        // Advance 1s — 5 tokens refilled.
        clock.advance(Duration::from_secs(1));
        assert!(bucket.try_consume(5.0));
        // After consuming exactly the refill amount, near zero.
        assert!(bucket.available() < 1.0);
    }

    // ── Capacity variant tests ──────────────────────────────────────────

    /// Slots: N concurrent → N successes, N+1th fails `try_acquire`.
    #[test]
    fn slots_n_concurrent_n_succeed_n_plus_one_fails() {
        let cap = Capacity::new_slots(3);
        assert_eq!(cap.free(), 3);

        let p1 = cap.try_acquire().unwrap();
        let _p2 = cap.try_acquire().unwrap();
        let _p3 = cap.try_acquire().unwrap();
        assert_eq!(cap.free(), 0);
        assert!(cap.try_acquire().is_none(), "N+1th must fail");

        drop(p1);
        assert_eq!(cap.free(), 1);
        assert!(cap.try_acquire().is_some());
        assert!(cap.try_acquire().is_some());
    }

    /// Concurrency: same behavior as Slots.
    #[test]
    fn concurrency_behaves_like_slots() {
        let cap = Capacity::new_concurrency(2);
        assert_eq!(cap.free(), 2);

        let _p1 = cap.try_acquire().unwrap();
        let p2 = cap.try_acquire().unwrap();
        assert!(cap.try_acquire().is_none());

        drop(p2);
        assert!(cap.try_acquire().is_some());
        drop(_p1);
    }

    /// Rated: RPM bucket drains with each request.
    #[test]
    fn rated_rpm_bucket_drains() {
        let clock: Arc<dyn Clock> = Arc::new(FakeClock::new(Instant::now()));
        // 30 concurrent (enough to hold all RPM tokens), 30 RPM, 1000 TPM.
        let cap = Capacity::new_rated(30, 30.0, 1000.0, clock);

        // Consume 30 permits via try_acquire.
        let mut permits = Vec::new();
        for _ in 0..30 {
            let p = cap.try_acquire().expect("should succeed up to RPM limit");
            permits.push(p);
        }
        // The 31st should fail because RPM bucket is exhausted.
        assert!(
            cap.try_acquire().is_none(),
            "31st must fail (RPM exhausted)"
        );

        // Release one → refunds 1 RPM token, making it available again.
        drop(permits.pop().unwrap());
        assert!(
            cap.try_acquire().is_some(),
            "after refund, one more available"
        );
    }

    /// Rated: RPM bucket refills over time.
    #[test]
    fn rated_rpm_refills_over_time() {
        let fake = Arc::new(FakeClock::new(Instant::now()));
        let clock: Arc<dyn Clock> = Arc::clone(&fake) as Arc<dyn Clock>;
        // 30 concurrent (enough to hold all RPM tokens), 30 RPM = 0.5 tokens/sec.
        let cap = Capacity::new_rated(30, 30.0, 1000.0, clock);

        // Drain all 30 RPM tokens.
        let permits: Vec<_> = (0..30).map(|_| cap.try_acquire().unwrap()).collect();
        assert!(cap.try_acquire().is_none());

        // Advance 10 seconds → 5 RPM tokens refilled (10s × 0.5 t/s = 5 tokens).
        fake.advance(Duration::from_secs(10));
        // Release one permit → frees a concurrency slot + refunds 1 RPM token.
        drop(permits.into_iter().next().unwrap());
        // Now: 5 (refill) + 1 (refund) = 6 RPM tokens. Concurrency slot freed.
        assert!(
            cap.try_acquire().is_some(),
            "refilled RPM tokens + concurrency slot available"
        );
    }

    /// is_usable: Slots/Concurrency always true; Rated false when RPM drained.
    #[test]
    fn is_usable() {
        let cap_slots = Capacity::new_slots(0);
        assert!(cap_slots.is_usable(), "Slots always usable (can wait)");

        let cap_conc = Capacity::new_concurrency(0);
        assert!(cap_conc.is_usable(), "Concurrency always usable (can wait)");

        let clock: Arc<dyn Clock> = Arc::new(FakeClock::new(Instant::now()));
        let cap_rated = Capacity::new_rated(10, 1.0, 1000.0, clock);
        assert!(cap_rated.is_usable());
        let _p = cap_rated.try_acquire().unwrap();
        assert!(
            !cap_rated.is_usable(),
            "Rated not usable when RPM exhausted"
        );
    }

    /// free() returns concurrency headroom for all variants.
    #[test]
    fn free_returns_concurrency_headroom() {
        let cap_slots = Capacity::new_slots(5);
        assert_eq!(cap_slots.free(), 5);
        let _p = cap_slots.try_acquire().unwrap();
        assert_eq!(cap_slots.free(), 4);

        let clock: Arc<dyn Clock> = Arc::new(FakeClock::new(Instant::now()));
        let cap_rated = Capacity::new_rated(3, 100.0, 1000.0, clock);
        assert_eq!(cap_rated.free(), 3);
        let _p2 = cap_rated.try_acquire().unwrap();
        assert_eq!(cap_rated.free(), 2);
    }

    /// Rated with exhausted concurrency but available RPM: try_acquire fails
    /// due to semaphore, not RPM.
    #[test]
    fn rated_semaphore_exhausted_rpm_available() {
        let clock: Arc<dyn Clock> = Arc::new(FakeClock::new(Instant::now()));
        let cap = Capacity::new_rated(2, 100.0, 1000.0, clock);

        let _p1 = cap.try_acquire().unwrap();
        let _p2 = cap.try_acquire().unwrap();
        // Semaphore full, RPM untouched.
        assert!(cap.try_acquire().is_none());
        // RPM is still usable.
        assert!(cap.is_usable());
    }

    /// CapacityPermit drop refunds RPM for Rated variant.
    #[test]
    fn rated_permit_drop_refunds_rpm() {
        let clock: Arc<dyn Clock> = Arc::new(FakeClock::new(Instant::now()));
        let cap = Capacity::new_rated(10, 1.0, 1000.0, Arc::clone(&clock));

        // The only RPM token.
        let permit = cap.try_acquire().unwrap();
        assert!(!cap.is_usable(), "RPM exhausted");

        drop(permit);
        assert!(cap.is_usable(), "RPM refunded on drop");
    }

    /// Permit for Slots variant is a simple Semaphore permit.
    #[test]
    fn slots_permit_is_semaphore() {
        let cap = Capacity::new_slots(2);
        let permit = cap.try_acquire().unwrap();
        assert!(matches!(permit, CapacityPermit::Semaphore(_)));
        drop(permit);
    }

    // ── Async acquire tests ─────────────────────────────────────────────

    /// async acquire on Slots: waits when all permits held, succeeds on release.
    #[tokio::test]
    async fn slots_acquire_async_waits() {
        let cap = Capacity::new_slots(1);
        let hold = cap.try_acquire().unwrap();

        // Spawn a task that releases after a short delay.
        let cap2 = cap.clone();
        let release = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            drop(hold);
        });

        let permit = cap2.acquire().await.unwrap();
        assert!(matches!(permit, CapacityPermit::Semaphore(_)));
        release.await.unwrap();
    }

    /// async acquire on Concurrency: same behavior as Slots.
    #[tokio::test]
    async fn concurrency_acquire_async_waits() {
        let cap = Capacity::new_concurrency(1);
        let hold = cap.try_acquire().unwrap();

        let cap2 = cap.clone();
        let release = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            drop(hold);
        });

        let permit = cap2.acquire().await.unwrap();
        assert!(matches!(permit, CapacityPermit::Semaphore(_)));
        release.await.unwrap();
    }

    /// async acquire on Rated: succeeds when both semaphore and RPM are available.
    #[tokio::test]
    async fn rated_acquire_async_succeeds() {
        let clock: Arc<dyn Clock> = Arc::new(WallClock);
        let cap = Capacity::new_rated(2, 100.0, 1000.0, clock);

        let permit = cap.acquire().await.unwrap();
        assert!(matches!(permit, CapacityPermit::Rated { .. }));
    }

    /// async acquire on Rated: returns None when RPM is exhausted (even if
    /// semaphore frees up).
    #[tokio::test]
    async fn rated_acquire_async_fails_when_rpm_exhausted() {
        let clock: Arc<dyn Clock> = Arc::new(FakeClock::new(Instant::now()));
        let cap = Capacity::new_rated(2, 1.0, 1000.0, clock);

        // Exhaust the single RPM token.
        let hold = cap.try_acquire().unwrap();

        let cap2 = cap.clone();
        // Spawn a task that tries to acquire — it will wait on the semaphore,
        // get the permit, but then find RPM exhausted.
        let waiter = tokio::spawn(async move { cap2.acquire().await });

        // Give the waiter time to start waiting on the semaphore.
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Release the holder → semaphore frees up, waiter gets permit,
        // but RPM is still exhausted.
        drop(hold);

        let result = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .unwrap()
            .unwrap();
        assert!(result.is_none(), "should fail after finding RPM exhausted");
    }

    // ── Token bucket: fractional rate edge cases ───────────────────────

    /// Very low rate (0.001 tokens/sec) produces no tokens over short duration.
    #[test]
    fn very_low_rate_produces_nothing_over_short_duration() {
        let clock = Arc::new(FakeClock::new(Instant::now()));
        let bucket = TokenBucket::new(0.001, 10.0, Arc::clone(&clock) as Arc<dyn Clock>);

        assert!(bucket.try_consume(10.0));
        assert_eq!(bucket.available(), 0.0);

        // 5 seconds at 0.001/s = 0.005 tokens refilled (not enough for 1 whole).
        clock.advance(Duration::from_secs(5));
        assert!(bucket.available() < 1.0);
        assert!(!bucket.try_consume(1.0));
    }

    /// Very high rate refills near-instantly.
    #[test]
    fn very_high_rate_refills_quickly() {
        let clock = Arc::new(FakeClock::new(Instant::now()));
        // 10_000 tokens/sec.
        let bucket = TokenBucket::new(10_000.0, 100.0, Arc::clone(&clock) as Arc<dyn Clock>);

        assert!(bucket.try_consume(100.0));
        assert_eq!(bucket.available(), 0.0);

        // 0.01s → 100 tokens refilled (capped at max=100).
        clock.advance(Duration::from_millis(10));
        assert!((bucket.available() - 100.0).abs() < 0.01);
    }
}
