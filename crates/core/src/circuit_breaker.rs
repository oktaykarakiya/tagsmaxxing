// SPDX-License-Identifier: AGPL-3.0-or-later

//! Reusable circuit breaker for external dependencies (plan §6.4, §22).
//!
//! After `threshold` consecutive failures the circuit opens (fast-fail).
//! After `cooldown`, a half-open probe is allowed. On the first success
//! after cooldown, the counter resets and the circuit closes.
//!
//! A `threshold` of `0` disables the circuit breaker — `is_open()` always
//! returns `false` and failures are not tracked.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Per-dependency circuit-breaker state.
///
/// After `threshold` consecutive failures the backend enters a cooldown
/// period. While in cooldown the caller should fast-fail rather than
/// attempt the operation. On the first success after cooldown the counter
/// resets to zero.
#[derive(Debug, Clone, Default)]
struct CircuitState {
    /// Consecutive failures since the last success or cooldown expiry.
    failures: u32,
    /// If `Some`, the circuit is open until this instant.
    cooldown_until: Option<Instant>,
}

/// A collection of circuit breakers keyed by dependency name.
///
/// Each dependency (e.g. `"b2"`, `"embed-backend"`) gets its own
/// independent circuit-breaker state.  All public methods accept a
/// `key: &str` to identify the target dependency.
///
/// # Thread safety
///
/// Interior [`Mutex`] protects the [`HashMap`] of per-dependency states.
/// Circuit-breaker checks happen infrequently (once per external call),
/// so the contention cost is negligible.
#[derive(Debug)]
pub struct CircuitBreaker {
    threshold: u32,
    cooldown: Duration,
    states: Mutex<HashMap<String, CircuitState>>,
}

impl CircuitBreaker {
    /// Create a new circuit-breaker collection.
    ///
    /// - `threshold`: consecutive failures before the circuit opens.
    ///   Use `0` to disable (circuit never opens).
    /// - `cooldown`: how long the circuit stays open before allowing a
    ///   half-open probe.
    #[must_use]
    pub fn new(threshold: u32, cooldown: Duration) -> Self {
        Self {
            threshold,
            cooldown,
            states: Mutex::new(HashMap::new()),
        }
    }

    /// Returns `true` when the circuit for `key` is open and callers
    /// should fast-fail (reject the operation immediately rather than
    /// attempting it and burning resources).
    ///
    /// Returns `false` when the circuit is closed or when the cooldown
    /// has elapsed (allowing a half-open probe). When `threshold` is
    /// `0`, always returns `false`.
    pub fn is_open(&self, key: &str) -> bool {
        if self.threshold == 0 {
            return false;
        }
        let mut guard = self.states.lock().unwrap_or_else(|e| e.into_inner());
        let entry = guard.entry(key.to_owned()).or_default();
        match entry.cooldown_until {
            Some(until) => {
                if Instant::now() < until {
                    true // still in cooldown
                } else {
                    // Cooldown expired — clear it to allow half-open probe.
                    entry.cooldown_until = None;
                    false
                }
            }
            None => false,
        }
    }

    /// Record a successful call for `key`. Resets the failure counter
    /// and closes the circuit.
    pub fn record_success(&self, key: &str) {
        if self.threshold == 0 {
            return;
        }
        self.states
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(key);
    }

    /// Record a failed call for `key`. If failures reach `threshold`,
    /// the circuit opens (trips).
    pub fn record_failure(&self, key: &str) {
        if self.threshold == 0 {
            return;
        }
        let mut guard = self.states.lock().unwrap_or_else(|e| e.into_inner());
        let entry = guard.entry(key.to_owned()).or_default();
        entry.failures = entry.failures.saturating_add(1);
        if entry.failures >= self.threshold {
            entry.cooldown_until = Some(Instant::now() + self.cooldown);
        }
    }

    /// Number of consecutive failures recorded for `key`.
    /// Returns `0` for unknown keys.
    #[must_use]
    pub fn failures(&self, key: &str) -> u32 {
        self.states
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
            .map_or(0, |s| s.failures)
    }

    /// The configured threshold.
    #[must_use]
    pub fn threshold(&self) -> u32 {
        self.threshold
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::thread;
    use std::time::Duration;

    use super::*;

    // ── Construction ──────────────────────────────────────────────────────

    #[test]
    fn new_with_positive_threshold() {
        let cb = CircuitBreaker::new(3, Duration::from_secs(30));
        assert_eq!(cb.threshold(), 3);
        assert!(!cb.is_open("dep"));
    }

    #[test]
    fn threshold_zero_disables() {
        let cb = CircuitBreaker::new(0, Duration::from_secs(30));
        assert!(!cb.is_open("dep"));
        cb.record_failure("dep");
        cb.record_failure("dep");
        cb.record_failure("dep");
        assert!(!cb.is_open("dep"));
        assert_eq!(cb.failures("dep"), 0);
    }

    // ── Basic open / close cycle ──────────────────────────────────────────

    #[test]
    fn opens_after_threshold_failures() {
        let cb = CircuitBreaker::new(3, Duration::from_secs(30));
        cb.record_failure("dep");
        assert!(!cb.is_open("dep"));
        cb.record_failure("dep");
        assert!(!cb.is_open("dep"));
        cb.record_failure("dep");
        assert!(cb.is_open("dep"));
    }

    #[test]
    fn record_success_resets_counter() {
        let cb = CircuitBreaker::new(3, Duration::from_secs(30));
        cb.record_failure("dep");
        cb.record_failure("dep");
        cb.record_success("dep");
        // After reset, need 3 more failures to open.
        assert!(!cb.is_open("dep"));
        cb.record_failure("dep");
        assert!(!cb.is_open("dep"));
        cb.record_failure("dep");
        assert!(!cb.is_open("dep"));
        cb.record_failure("dep");
        assert!(cb.is_open("dep"));
    }

    #[test]
    fn record_success_closes_open_circuit() {
        let cb = CircuitBreaker::new(3, Duration::from_secs(300));
        cb.record_failure("dep");
        cb.record_failure("dep");
        cb.record_failure("dep");
        assert!(cb.is_open("dep"));
        cb.record_success("dep");
        assert!(!cb.is_open("dep"));
        assert_eq!(cb.failures("dep"), 0);
    }

    // ── Half-open probe ──────────────────────────────────────────────────

    #[test]
    fn half_open_after_cooldown() {
        let cb = CircuitBreaker::new(3, Duration::from_millis(50));
        cb.record_failure("dep");
        cb.record_failure("dep");
        cb.record_failure("dep");
        assert!(cb.is_open("dep"));

        // Wait for cooldown to expire.
        thread::sleep(Duration::from_millis(100));

        // Now the circuit should be half-open — is_open returns false.
        assert!(!cb.is_open("dep"));
    }

    #[test]
    fn reopens_after_half_open_failure() {
        let cb = CircuitBreaker::new(3, Duration::from_millis(50));
        cb.record_failure("dep");
        cb.record_failure("dep");
        cb.record_failure("dep");
        assert!(cb.is_open("dep"));

        thread::sleep(Duration::from_millis(100));
        assert!(!cb.is_open("dep")); // half-open

        // Fail the half-open probe — should re-trip immediately.
        cb.record_failure("dep");
        assert!(cb.is_open("dep"));
    }

    #[test]
    fn success_during_half_open_closes() {
        let cb = CircuitBreaker::new(3, Duration::from_millis(50));
        cb.record_failure("dep");
        cb.record_failure("dep");
        cb.record_failure("dep");
        assert!(cb.is_open("dep"));

        thread::sleep(Duration::from_millis(100));
        assert!(!cb.is_open("dep")); // half-open

        // Success during half-open should fully reset.
        cb.record_success("dep");
        assert!(!cb.is_open("dep"));
        assert_eq!(cb.failures("dep"), 0);
    }

    // ── Multiple dependencies ────────────────────────────────────────────

    #[test]
    fn independent_per_dependency() {
        let cb = CircuitBreaker::new(2, Duration::from_secs(300));
        cb.record_failure("b2");
        cb.record_failure("b2");
        assert!(cb.is_open("b2"));

        // A different dependency is unaffected.
        assert!(!cb.is_open("embed"));
        cb.record_failure("embed");
        assert!(!cb.is_open("embed"));
    }

    #[test]
    fn failures_counter() {
        let cb = CircuitBreaker::new(5, Duration::from_secs(30));
        assert_eq!(cb.failures("x"), 0);
        cb.record_failure("x");
        assert_eq!(cb.failures("x"), 1);
        cb.record_failure("x");
        assert_eq!(cb.failures("x"), 2);
        cb.record_success("x");
        assert_eq!(cb.failures("x"), 0);
    }

    #[test]
    fn failures_does_not_create_entry() {
        let cb = CircuitBreaker::new(3, Duration::from_secs(30));
        // Querying failures for an unknown key returns 0 but does
        // not insert an entry in the map.
        assert_eq!(cb.failures("no-such-key"), 0);
        // The key still doesn't exist in the map; is_open is false.
        assert!(!cb.is_open("no-such-key"));
    }
}
