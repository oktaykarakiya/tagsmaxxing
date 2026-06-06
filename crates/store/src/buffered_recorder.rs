//! Fail-open, asynchronous usage recorder (plan §15, §29; ledger P14-T1).
//!
//! [`BufferedUsageRecorder`] wraps an inner [`UsageRecorder`] (typically the
//! [`PgStore`](crate::pg_store::PgStore)) behind a bounded channel so that
//! metering never blocks — and never fails — a request path.
//!
//! ## Why fail-open
//!
//! Recording usage is best-effort accounting; it must not be able to abort an
//! ingest or a search. So [`record_usage`](BufferedUsageRecorder::record_usage)
//! does a **non-blocking** `try_send` onto the channel and returns `Ok(())`
//! even when the channel is full or the drain task has stopped. A dropped event
//! is counted (see [`dropped_count`](BufferedUsageRecorder::dropped_count)) and
//! logged at `warn`, so operators can detect sustained back-pressure, but the
//! caller is never affected.
//!
//! The actual persistence happens on a background task spawned by
//! [`BufferedUsageRecorder::start`], which drains the channel and forwards each
//! event to the inner recorder, logging (and continuing) on error.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use kb_core::usage::{UsageEvent, UsageRecorder};
use tokio::sync::mpsc;

/// Default channel capacity for the buffered recorder.
///
/// Sized so a healthy backend keeps the queue near-empty; the bound exists only
/// to cap memory under a stalled or overwhelmed sink (events are dropped, not
/// queued unboundedly).
pub const DEFAULT_CAPACITY: usize = 4096;

/// A fail-open, non-blocking [`UsageRecorder`] front-end.
///
/// Construct it with [`BufferedUsageRecorder::start`], which returns the
/// recorder together with the [`JoinHandle`](tokio::task::JoinHandle) of the
/// background drain task. Clone-free sharing is via `Arc<dyn UsageRecorder>`;
/// the recorder is cheap to hold (a channel sender + an atomic counter).
pub struct BufferedUsageRecorder {
    /// Non-blocking producer side of the bounded buffer.
    tx: mpsc::Sender<UsageEvent>,
    /// Count of events dropped because the buffer was full or the drain task
    /// had stopped (fail-open). Monotonic; exposed for observability.
    dropped: Arc<AtomicU64>,
}

impl BufferedUsageRecorder {
    /// Build a buffered recorder around `inner` and spawn its drain task.
    ///
    /// `capacity` bounds the in-memory buffer (pass [`DEFAULT_CAPACITY`] for the
    /// standard size; a capacity of 0 is treated as 1, the smallest bound
    /// `tokio::sync::mpsc` permits). The returned
    /// [`JoinHandle`](tokio::task::JoinHandle) completes when the recorder (and
    /// every clone of its sender) is dropped and the buffer has been fully
    /// drained — making graceful shutdown observable.
    #[must_use]
    pub fn start(
        inner: Arc<dyn UsageRecorder>,
        capacity: usize,
    ) -> (Self, tokio::task::JoinHandle<()>) {
        let (tx, mut rx) = mpsc::channel::<UsageEvent>(capacity.max(1));
        let handle = tokio::spawn(async move {
            // Drain until all senders are dropped and the buffer is empty.
            while let Some(event) = rx.recv().await {
                if let Err(e) = inner.record_usage(&event).await {
                    // The durable write actually failed — this accounting write
                    // is lost. Count it alongside the fail-open drops (P14-T10).
                    kb_metrics::record_metering_write_failure();
                    tracing::warn!(
                        error = %e,
                        tenant_id = event.tenant_id,
                        "buffered usage recorder: inner sink failed to persist event"
                    );
                }
            }
        });
        (
            Self {
                tx,
                dropped: Arc::new(AtomicU64::new(0)),
            },
            handle,
        )
    }

    /// Number of events dropped so far because the buffer was full or the drain
    /// task had stopped. Useful as a health/observability signal.
    #[must_use]
    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl UsageRecorder for BufferedUsageRecorder {
    /// Enqueue `event` for asynchronous persistence, never blocking and never
    /// erroring (fail-open, P14-T1).
    ///
    /// On a full or closed channel the event is dropped: the dropped-counter is
    /// incremented and a `warn` is logged, but `Ok(())` is still returned so the
    /// calling request path is unaffected.
    async fn record_usage(&self, event: &UsageEvent) -> anyhow::Result<()> {
        // Count tokens at this single chokepoint as the event is accepted, so
        // every metered AI call (ingest, search, vision) is counted regardless
        // of whether the durable write later drops (P14-T10). The metric tracks
        // tokens *used*, which is true the moment the call happened.
        let tokens = event.prompt_tokens.unwrap_or(0).max(0) as u64
            + event.completion_tokens.unwrap_or(0).max(0) as u64;
        kb_metrics::record_tokens(event.role.as_str(), &event.model, tokens);

        match self.tx.try_send(event.clone()) {
            Ok(()) => Ok(()),
            Err(err) => {
                let prior = self.dropped.fetch_add(1, Ordering::Relaxed);
                // A full or closed channel means this accounting write is lost
                // (fail-open). Record it so operators can alert on lost writes.
                kb_metrics::record_metering_write_failure();
                let reason = match err {
                    mpsc::error::TrySendError::Full(_) => "buffer full",
                    mpsc::error::TrySendError::Closed(_) => "drain task stopped",
                };
                tracing::warn!(
                    tenant_id = event.tenant_id,
                    reason,
                    dropped_total = prior + 1,
                    "buffered usage recorder: dropped usage event (fail-open)"
                );
                // Fail-open: metering must never abort the request.
                Ok(())
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use kb_core::role::Role;

    use super::*;

    /// An inner recorder that records every event it receives.
    #[derive(Default)]
    struct CapturingRecorder {
        events: Mutex<Vec<UsageEvent>>,
    }

    #[async_trait]
    impl UsageRecorder for CapturingRecorder {
        async fn record_usage(&self, event: &UsageEvent) -> anyhow::Result<()> {
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }
    }

    /// An inner recorder that blocks on a release gate, so the channel can be
    /// driven to full in a deterministic test (no sleeps).
    struct BlockingRecorder {
        release: Arc<tokio::sync::Notify>,
        seen: Arc<AtomicU64>,
    }

    #[async_trait]
    impl UsageRecorder for BlockingRecorder {
        async fn record_usage(&self, _event: &UsageEvent) -> anyhow::Result<()> {
            // Block the drain task on the very first event until released.
            self.release.notified().await;
            self.seen.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    /// An inner recorder that always fails, to prove the drain task logs and
    /// keeps draining rather than aborting.
    struct FailingRecorder {
        seen: Arc<AtomicU64>,
    }

    #[async_trait]
    impl UsageRecorder for FailingRecorder {
        async fn record_usage(&self, _event: &UsageEvent) -> anyhow::Result<()> {
            self.seen.fetch_add(1, Ordering::Relaxed);
            anyhow::bail!("inner sink boom")
        }
    }

    fn event(tenant_id: i64, prompt: i32) -> UsageEvent {
        event_for_model(tenant_id, prompt, 0, "test-model")
    }

    /// Build a usage event with an explicit model id and prompt/completion split,
    /// so token-counter tests can isolate a unique `{role,model}` series in the
    /// process-global metrics recorder.
    fn event_for_model(tenant_id: i64, prompt: i32, completion: i32, model: &str) -> UsageEvent {
        UsageEvent {
            id: 0,
            tenant_id,
            user_id: Some(9),
            model: model.into(),
            role: Role::Embed,
            backend_id: None,
            prompt_tokens: Some(prompt),
            completion_tokens: Some(completion),
            latency_ms: None,
            cost_micros: None,
            created_at: chrono::Utc::now(),
        }
    }

    /// Parse the integer value of a single unlabelled counter family from a
    /// Prometheus exposition document, returning 0 when the family has no data
    /// line yet. Used to take before/after deltas of the global, label-free
    /// `kb_metering_write_failures_total` counter without racing parallel tests.
    fn counter_value(text: &str, name: &str) -> u64 {
        let prefix = format!("{name} ");
        text.lines()
            .map(str::trim_start)
            .find_map(|l| l.strip_prefix(&prefix))
            .and_then(|v| v.trim().parse::<f64>().ok())
            .map(|v| v as u64)
            .unwrap_or(0)
    }

    #[tokio::test]
    async fn forwards_events_to_inner() {
        let inner = Arc::new(CapturingRecorder::default());
        let (rec, handle) = BufferedUsageRecorder::start(inner.clone(), 16);

        rec.record_usage(&event(1, 10)).await.unwrap();
        rec.record_usage(&event(2, 20)).await.unwrap();

        // Dropping the recorder closes the channel; the drain task then finishes
        // after persisting everything buffered.
        drop(rec);
        handle.await.unwrap();

        let events = inner.events.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].tenant_id, 1);
        assert_eq!(events[0].prompt_tokens, Some(10));
        assert_eq!(events[1].tenant_id, 2);
    }

    #[tokio::test]
    async fn record_usage_is_ok_and_counts_zero_when_healthy() {
        let inner = Arc::new(CapturingRecorder::default());
        let (rec, handle) = BufferedUsageRecorder::start(inner, DEFAULT_CAPACITY);
        rec.record_usage(&event(7, 1)).await.unwrap();
        assert_eq!(rec.dropped_count(), 0, "no drops on a healthy buffer");
        drop(rec);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn drops_on_full_buffer_without_erroring() {
        // Capacity 1, and the drain task is wedged on the first event, so once
        // the in-flight slot + the single buffer slot are taken every further
        // send is dropped — but each still returns Ok(()).
        let release = Arc::new(tokio::sync::Notify::new());
        let seen = Arc::new(AtomicU64::new(0));
        let inner = Arc::new(BlockingRecorder {
            release: release.clone(),
            seen: seen.clone(),
        });
        let (rec, handle) = BufferedUsageRecorder::start(inner, 1);

        // Push events until at least one is observed as dropped. The first is
        // pulled by the (now-blocked) drain task; the next fills the buffer;
        // subsequent ones are dropped. Bounded loop — never hangs.
        let mut sent = 0;
        for i in 0..100 {
            rec.record_usage(&event(1, i)).await.unwrap();
            sent += 1;
            if rec.dropped_count() > 0 {
                break;
            }
            // Yield so the drain task can pick up the first event and block.
            tokio::task::yield_now().await;
        }

        assert!(
            rec.dropped_count() >= 1,
            "a full buffer must drop at least one event (sent {sent})"
        );

        // Release the gate and drain; no panic, inner saw at least one event.
        release.notify_waiters();
        drop(rec);
        let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        assert!(seen.load(Ordering::Relaxed) >= 1);
    }

    #[tokio::test]
    async fn record_usage_ok_after_drain_task_stopped() {
        // If the drain task (and thus the receiver) is gone, sends fail-open.
        let inner = Arc::new(CapturingRecorder::default());
        let (rec, handle) = BufferedUsageRecorder::start(inner, 4);
        handle.abort();
        let _ = handle.await; // join the aborted task so the receiver is dropped

        // The receiver is dropped → try_send returns Closed → still Ok(()).
        let result = rec.record_usage(&event(1, 5)).await;
        assert!(result.is_ok(), "closed channel must fail open");
        assert_eq!(rec.dropped_count(), 1);
    }

    #[tokio::test]
    async fn drain_continues_after_inner_error() {
        // The inner sink always errors; the drain task must log and keep going,
        // processing every buffered event rather than aborting on the first.
        let seen = Arc::new(AtomicU64::new(0));
        let inner = Arc::new(FailingRecorder { seen: seen.clone() });
        let (rec, handle) = BufferedUsageRecorder::start(inner, 16);

        for i in 0..5 {
            rec.record_usage(&event(1, i)).await.unwrap();
        }
        drop(rec);
        handle.await.unwrap();

        assert_eq!(
            seen.load(Ordering::Relaxed),
            5,
            "drain task must attempt every event despite inner errors"
        );
    }

    // ── Metering-seam metric tests (P14-T10) ──────────────────────────────

    #[tokio::test]
    async fn record_usage_counts_tokens_at_the_seam() {
        let _ = kb_metrics::init_metrics();
        let inner = Arc::new(CapturingRecorder::default());
        let (rec, handle) = BufferedUsageRecorder::start(inner, 16);

        // A model id nobody else uses so the rendered series is exactly ours.
        // role=embed (event_for_model), prompt 7 + completion 5 = 12 per call.
        let model = "p14t10-seam-model";
        rec.record_usage(&event_for_model(1, 7, 5, model))
            .await
            .unwrap();
        rec.record_usage(&event_for_model(1, 3, 0, model))
            .await
            .unwrap();

        drop(rec);
        handle.await.unwrap();

        let text = kb_metrics::render();
        // Tokens are counted on accept, summed across both calls: 12 + 3 = 15.
        assert!(
            text.contains(&format!(
                "kb_tokens_total{{role=\"embed\",model=\"{model}\"}} 15"
            )),
            "expected kb_tokens_total of 15 for {model} in: {text}"
        );
    }

    #[tokio::test]
    async fn dropped_event_increments_metering_write_failures() {
        let _ = kb_metrics::init_metrics();
        let before = counter_value(&kb_metrics::render(), "kb_metering_write_failures_total");

        // Drive the buffer to full with a wedged drain task, exactly like
        // `drops_on_full_buffer_without_erroring`, then assert the failure
        // counter advanced by at least the number of drops observed.
        let release = Arc::new(tokio::sync::Notify::new());
        let seen = Arc::new(AtomicU64::new(0));
        let inner = Arc::new(BlockingRecorder {
            release: release.clone(),
            seen: seen.clone(),
        });
        let (rec, handle) = BufferedUsageRecorder::start(inner, 1);

        for i in 0..100 {
            rec.record_usage(&event(1, i)).await.unwrap();
            if rec.dropped_count() > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        let drops = rec.dropped_count();
        assert!(drops >= 1, "test must observe at least one drop");

        release.notify_waiters();
        drop(rec);
        let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

        let after = counter_value(&kb_metrics::render(), "kb_metering_write_failures_total");
        assert!(
            after >= before + drops,
            "metering-write-failures must rise by >= the {drops} drop(s): {before} -> {after}"
        );
    }

    #[tokio::test]
    async fn inner_failure_increments_metering_write_failures() {
        let _ = kb_metrics::init_metrics();
        let before = counter_value(&kb_metrics::render(), "kb_metering_write_failures_total");

        // Every durable write fails; the drain task must count each lost write.
        let seen = Arc::new(AtomicU64::new(0));
        let inner = Arc::new(FailingRecorder { seen: seen.clone() });
        let (rec, handle) = BufferedUsageRecorder::start(inner, 16);

        for i in 0..3 {
            rec.record_usage(&event(1, i)).await.unwrap();
        }
        drop(rec);
        handle.await.unwrap();
        assert_eq!(seen.load(Ordering::Relaxed), 3, "all 3 must be attempted");

        let after = counter_value(&kb_metrics::render(), "kb_metering_write_failures_total");
        assert!(
            after >= before + 3,
            "metering-write-failures must rise by >= 3 inner failures: {before} -> {after}"
        );
    }
}
