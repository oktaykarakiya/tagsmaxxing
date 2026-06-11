// SPDX-License-Identifier: AGPL-3.0-or-later

//! Per-model pricing metadata (plan §26.3, §26.6).
//!
//! [`Pricing`] drives the cost-aware routing strategy ([`CostAsc`]) and
//! per-tenant budget enforcement. `None` means free or local-only.

/// Per-million-token pricing for a model (plan §26.3).
///
/// All values are in **USD per million tokens**. When `pricing` is [`None`]
/// on a [`Backend`](kb_scheduler::Backend), the model is treated as free
/// (local inference).
///
/// # Cost calculation
///
/// ```text
/// cost_micros = ceil((prompt_tokens × input_per_mtok / 1_000_000 + completion_tokens × output_per_mtok / 1_000_000) × 1_000_000)
/// ```
///
/// The result is stored in `usage_events.cost_micros` (plan §26.6).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Pricing {
    /// USD per million input (prompt) tokens.
    pub input_per_mtok: f64,
    /// USD per million output (completion) tokens.
    pub output_per_mtok: f64,
}

impl Pricing {
    /// Create a new pricing record. Both values must be non-negative.
    ///
    /// # Panics
    ///
    /// Panics in debug if a price is negative (a logic error, not a runtime condition).
    pub fn new(input_per_mtok: f64, output_per_mtok: f64) -> Self {
        debug_assert!(input_per_mtok >= 0.0, "input_per_mtok must be >= 0");
        debug_assert!(output_per_mtok >= 0.0, "output_per_mtok must be >= 0");
        Self {
            input_per_mtok,
            output_per_mtok,
        }
    }

    /// Compute cost in micro-dollars (millionths of a USD) for the given token counts.
    ///
    /// # Examples
    ///
    /// ```
    /// use kb_core::pricing::Pricing;
    /// let p = Pricing::new(1.0, 2.0); // $1/Mtok in, $2/Mtok out
    /// // 1M input tokens = $1.00
    /// assert_eq!(p.cost_micros(1_000_000, 0), 1_000_000);
    /// // 1M output tokens = $2.00
    /// assert_eq!(p.cost_micros(0, 1_000_000), 2_000_000);
    /// // Mixed
    /// assert_eq!(p.cost_micros(500_000, 500_000), 1_500_000);
    /// ```
    pub fn cost_micros(&self, prompt_tokens: u32, completion_tokens: u32) -> u64 {
        let prompt_cost = (prompt_tokens as f64 / 1_000_000.0) * self.input_per_mtok;
        let completion_cost = (completion_tokens as f64 / 1_000_000.0) * self.output_per_mtok;
        let total = (prompt_cost + completion_cost) * 1_000_000.0;
        // Ceil to avoid charging zero for tiny amounts.
        total.ceil() as u64
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn free_pricing_is_zero_cost() {
        let p = Pricing::new(0.0, 0.0);
        assert_eq!(p.cost_micros(1_000_000, 1_000_000), 0);
        assert_eq!(p.cost_micros(0, 0), 0);
    }

    #[test]
    fn input_only_cost() {
        let p = Pricing::new(3.0, 0.0);
        // 1M input = $3.00 = 3_000_000 micro-dollars
        assert_eq!(p.cost_micros(1_000_000, 0), 3_000_000);
    }

    #[test]
    fn output_only_cost() {
        let p = Pricing::new(0.0, 15.0);
        assert_eq!(p.cost_micros(0, 1_000_000), 15_000_000);
    }

    #[test]
    fn mixed_cost() {
        let p = Pricing::new(2.0, 8.0);
        // 500k in ($1) + 250k out ($2) = $3.00
        assert_eq!(p.cost_micros(500_000, 250_000), 3_000_000);
    }

    #[test]
    fn sub_penny_cost_rounds_up() {
        let p = Pricing::new(0.15, 0.15); // $0.15 / Mtok
        // 100 tokens at $0.15/Mtok = 0.000015 = 15 micro-dollars
        assert_eq!(p.cost_micros(100, 0), 15);
        // 1 token at same rate = 0.15 micro-dollars → ceil to 1
        let c = p.cost_micros(1, 0);
        assert!(c <= 1, "ceil(0.15) should be 1, got {c}");
    }

    #[test]
    fn zero_tokens_is_free() {
        let p = Pricing::new(100.0, 100.0);
        assert_eq!(p.cost_micros(0, 0), 0);
    }

    #[test]
    fn large_input_does_not_overflow() {
        let p = Pricing::new(200.0, 200.0);
        // u32::MAX prompt tokens at $200/Mtok.
        // 4_294_967_295 / 1_000_000 * 200 = ~858_993 USD = ~859_000_000_000 micro-dollars
        // Fits easily in u64 (max ~1.8e19 micro).
        let c = p.cost_micros(u32::MAX, 0);
        assert!(c > 0);
        assert!(c < u64::MAX / 2, "no overflow");
    }

    #[test]
    fn new_stores_fields() {
        let p = Pricing::new(1.5, 3.0);
        assert!((p.input_per_mtok - 1.5).abs() < f64::EPSILON);
        assert!((p.output_per_mtok - 3.0).abs() < f64::EPSILON);
    }
}
