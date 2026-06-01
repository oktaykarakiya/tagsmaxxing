//! Per-tenant spend-budget enforcement — pure logic (plan §26.6, P9-T10).
//!
//! These functions compare a tenant's running monthly cost (sum of
//! `usage_events.cost_micros` for the current month) against its
//! `tenants.budget_monthly_cents` limit. They are I/O-free: callers fetch the
//! totals from the database and pass them in.
//!
//! Budget checks are **best-effort** — concurrent calls may briefly exceed the
//! cap. The hard-cap mode (`TENANT_HARD_BUDGET_CAP=true`) rejects calls with a
//! structured error; the default soft-cap mode only logs a warning and sets a
//! Prometheus gauge.

/// Status returned by [`check_budget`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BudgetStatus {
    /// Spending is within the monthly budget (or the tenant has no budget cap).
    WithinBudget,
    /// Spending exceeds the monthly budget. Carries details for logging,
    /// Prometheus gauge, and — when hard-cap mode is enabled — rejection.
    OverBudget {
        /// Running monthly cost in micro-dollars.
        current_micros: u64,
        /// Budget cap in micro-dollars (`budget_monthly_cents * 10000`).
        budget_micros: u64,
        /// Percentage of budget consumed (0–100+). Rounded down.
        usage_pct: u32,
    },
}

/// Check whether `monthly_cost_micros` exceeds the tenant's monthly spend
/// budget.
///
/// # Arguments
///
/// * `monthly_cost_micros` — sum of `cost_micros` for the current calendar
///   month (skip rows where `cost_micros IS NULL`, which are free/local).
/// * `budget_monthly_cents` — the `tenants.budget_monthly_cents` column;
///   `None` or ≤ 0 means unlimited.
///
/// # Returns
///
/// * [`BudgetStatus::WithinBudget`] when spending is at or below the cap, or
///   when the budget is `None` / ≤ 0 (unlimited).
/// * [`BudgetStatus::OverBudget`] with the current spend, the cap, and the
///   rounded-down usage percentage.
///
/// # Examples
///
/// ```
/// use kb_core::budget::{check_budget, BudgetStatus};
///
/// // $5.00 spend, $10.00 budget → OK (50%).
/// assert_eq!(check_budget(5_000_000, Some(1000)), BudgetStatus::WithinBudget);
///
/// // $12.00 spend, $10.00 budget → over (120%).
/// let s = check_budget(12_000_000, Some(1000));
/// assert!(matches!(s, BudgetStatus::OverBudget { .. }));
///
/// // None budget → always OK.
/// assert_eq!(check_budget(u64::MAX, None), BudgetStatus::WithinBudget);
/// ```
#[inline]
pub fn check_budget(monthly_cost_micros: u64, budget_monthly_cents: Option<i64>) -> BudgetStatus {
    let Some(budget_cents) = budget_monthly_cents else {
        return BudgetStatus::WithinBudget;
    };
    // A zero or negative budget is treated as unlimited (guard against bad
    // data).
    if budget_cents <= 0 {
        return BudgetStatus::WithinBudget;
    }
    // 1 cent = 10_000 micro-dollars.
    let budget_micros = budget_cents as u64 * 10_000;
    if monthly_cost_micros <= budget_micros {
        return BudgetStatus::WithinBudget;
    }
    // Integer percentage, rounding down.
    let usage_pct = ((monthly_cost_micros / (budget_micros / 100)).min(u32::MAX as u64)) as u32;
    BudgetStatus::OverBudget {
        current_micros: monthly_cost_micros,
        budget_micros,
        usage_pct,
    }
}

/// Error returned when a hard budget cap is enforced (plan §26.6).
///
/// Callers in the API layer should map this to HTTP 429 with a `Retry-After`
/// header pointing to the first day of the next month.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "monthly spend budget exceeded: {current_micros} micro-dollars spent vs {budget_micros} micro-dollars ({usage_pct}% of budget)"
)]
pub struct BudgetExceeded {
    /// Running monthly cost in micro-dollars.
    pub current_micros: u64,
    /// Budget cap in micro-dollars.
    pub budget_micros: u64,
    /// Percentage of budget consumed.
    pub usage_pct: u32,
}

impl BudgetExceeded {
    /// Create a [`BudgetExceeded`] from an [`OverBudget`](BudgetStatus::OverBudget) status.
    #[must_use]
    pub fn from_status(s: &BudgetStatus) -> Option<Self> {
        match s {
            BudgetStatus::WithinBudget => None,
            BudgetStatus::OverBudget {
                current_micros,
                budget_micros,
                usage_pct,
            } => Some(Self {
                current_micros: *current_micros,
                budget_micros: *budget_micros,
                usage_pct: *usage_pct,
            }),
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // ── check_budget ────────────────────────────────────────────────────────

    #[test]
    fn unlimited_none_always_within() {
        assert_eq!(check_budget(u64::MAX, None), BudgetStatus::WithinBudget);
    }

    #[test]
    fn zero_budget_is_unlimited() {
        assert_eq!(check_budget(1_000_000, Some(0)), BudgetStatus::WithinBudget);
    }

    #[test]
    fn negative_budget_is_unlimited() {
        assert_eq!(
            check_budget(1_000_000, Some(-5)),
            BudgetStatus::WithinBudget
        );
    }

    #[test]
    fn under_budget_is_within() {
        // $5.00 spend, $10.00 budget (1000 cents).
        assert_eq!(
            check_budget(5_000_000, Some(1000)),
            BudgetStatus::WithinBudget
        );
    }

    #[test]
    fn exactly_at_budget_is_within() {
        // $10.00 spend, $10.00 budget.
        assert_eq!(
            check_budget(10_000_000, Some(1000)),
            BudgetStatus::WithinBudget
        );
    }

    #[test]
    fn over_budget_returns_details() {
        // $15.00 spend, $10.00 budget → 150%.
        let status = check_budget(15_000_000, Some(1000));
        match status {
            BudgetStatus::OverBudget {
                current_micros,
                budget_micros,
                usage_pct,
            } => {
                assert_eq!(current_micros, 15_000_000);
                assert_eq!(budget_micros, 10_000_000);
                assert_eq!(usage_pct, 150);
            }
            BudgetStatus::WithinBudget => panic!("expected OverBudget, got WithinBudget"),
        }
    }

    #[test]
    fn fractional_percentage_rounds_down() {
        // $1.09 spend, $1.00 budget → 109%.
        let status = check_budget(1_090_000, Some(100));
        match status {
            BudgetStatus::OverBudget { usage_pct, .. } => {
                assert_eq!(usage_pct, 109);
            }
            BudgetStatus::WithinBudget => panic!("expected OverBudget"),
        }
    }

    #[test]
    fn zero_spend_is_within() {
        assert_eq!(check_budget(0, Some(1000)), BudgetStatus::WithinBudget);
    }

    #[test]
    fn free_calls_zero_cost_is_within_budget() {
        // Only free calls this month, so spend is 0.
        assert_eq!(
            check_budget(0, Some(1)), // $0.01 budget
            BudgetStatus::WithinBudget
        );
    }

    // ── BudgetExceeded ──────────────────────────────────────────────────────

    #[test]
    fn from_within_budget_is_none() {
        assert!(BudgetExceeded::from_status(&BudgetStatus::WithinBudget).is_none());
    }

    #[test]
    fn from_over_budget_builds_error() {
        let status = BudgetStatus::OverBudget {
            current_micros: 20_000_000,
            budget_micros: 10_000_000,
            usage_pct: 200,
        };
        let err = BudgetExceeded::from_status(&status).unwrap();
        assert_eq!(err.current_micros, 20_000_000);
        assert_eq!(err.budget_micros, 10_000_000);
        assert_eq!(err.usage_pct, 200);
    }

    #[test]
    fn exceeded_error_is_descriptive() {
        let err = BudgetExceeded {
            current_micros: 15_000_000,
            budget_micros: 10_000_000,
            usage_pct: 150,
        };
        let msg = err.to_string();
        assert!(msg.contains("15000000"));
        assert!(msg.contains("10000000"));
        assert!(msg.contains("150%"));
    }

    // ── Edge cases ──────────────────────────────────────────────────────────

    #[test]
    fn large_budget_no_overflow() {
        // $10000 budget, $5000 spend — well within u64 range.
        let status = check_budget(5_000_000_000, Some(1_000_000));
        assert!(matches!(status, BudgetStatus::WithinBudget));
    }

    #[test]
    fn huge_usage_pct_is_capped() {
        // 100x over budget. usage_pct computation must not panic/overflow.
        let status = check_budget(100_000_000_000, Some(100));
        match status {
            BudgetStatus::OverBudget { usage_pct, .. } => {
                // 100_000_000_000 / 1_000_000 * 100 = 10_000_000% — capped.
                assert!(usage_pct > 0);
            }
            BudgetStatus::WithinBudget => panic!("massive overspend should be OverBudget"),
        }
    }

    // ── BudgetStatus is cloneable and comparable ────────────────────────────

    #[test]
    fn budget_status_is_cloneable() {
        let s1 = BudgetStatus::WithinBudget;
        let s2 = s1.clone();
        assert_eq!(s1, s2);

        let over = BudgetStatus::OverBudget {
            current_micros: 1,
            budget_micros: 2,
            usage_pct: 50,
        };
        let over2 = over.clone();
        assert_eq!(over, over2);
    }

    #[test]
    fn budget_status_is_comparable() {
        assert_eq!(BudgetStatus::WithinBudget, BudgetStatus::WithinBudget);
        let a = BudgetStatus::OverBudget {
            current_micros: 10,
            budget_micros: 5,
            usage_pct: 200,
        };
        let b = BudgetStatus::OverBudget {
            current_micros: 10,
            budget_micros: 5,
            usage_pct: 200,
        };
        let c = BudgetStatus::OverBudget {
            current_micros: 20,
            budget_micros: 10,
            usage_pct: 200,
        };
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
