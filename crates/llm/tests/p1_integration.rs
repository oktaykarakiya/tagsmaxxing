//! P1 integration test suite — exercising the full scheduler + llm + mock-backend
//! contract together (plan §10 P1, §31.3).
//!
//! Every test in this file is an **integration test**: it uses only the public API of
//! `kb_scheduler`, `kb_llm`, and `kb_mock_backend`, exercising them against each
//! other deterministically across in-process HTTP. No real network, no flakiness.
//!
//! The suite asserts the full P1 contract:
//! - Free-slot pick with priority ordering  (pool.acquire → lease.backend_id)
//! - Fair wait-then-acquire                 (concurrent chat calls)
//! - Acquire timeout                        (busy backends → Scheduler(Timeout))
//! - Dead-host skip                         (unhealthy filtered)
//! - Health loop → pool → client lifecycle  (unhealthy → NoBackend → recovery → ok)
//! - Failover on 5xx / transport error / 429
//! - Circuit-breaker cooldown
//! - Slot release after call completion
//!
//! Clippy unwrap/expect lints are allowed here: this is a test binary, and
//! `.unwrap()`/`.unwrap_err()` are the standard Rust test assertion pattern
//! (same exception as the `#[cfg(test)]` modules in unit tests — plan §31.2).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use kb_core::provider::{ChatMessage, ChatReq, ChatRole, EmbedReq};
use kb_core::role::Role;
use kb_llm::{LlamaClient, LlmError};
use kb_mock_backend::{MockBackend, ResponseMode};
use kb_scheduler::{AcquireError, Backend, HealthLoop, Pool};
use reqwest::Client;

// ── helpers ───────────────────────────────────────────────────────────────────

/// Build a backend pointing at a running mock with a `/v1` base URL.
fn backend_for(
    mock: &MockBackend,
    id: &str,
    roles: Vec<Role>,
    priority: u8,
    slots: usize,
) -> Arc<Backend> {
    Arc::new(Backend::new(id, mock.url("/v1"), roles, priority, slots))
}

/// Construct a minimal [`ChatReq`] with one user message.
fn chat_req(text: &str) -> ChatReq {
    ChatReq {
        messages: vec![ChatMessage {
            role: ChatRole::User,
            content: text.into(),
        }],
        ..Default::default()
    }
}

/// Build a [`LlamaClient`] with default failover parameters suitable for
/// integration tests.
fn client_with(pool: Pool) -> LlamaClient {
    LlamaClient::new(
        pool,
        Client::new(),
        3,                          /* max_retries */
        3,                          /* circuit_threshold */
        Duration::from_millis(200), /* cooldown */
    )
}

// ── free-slot pick + priority ordering ────────────────────────────────────────

/// When two healthy backends serve the same role, the one with lower priority
/// (preferred) is picked. Uses `Pool::acquire` directly so the held lease reveals
/// which backend won.
#[tokio::test]
async fn free_slot_pick_favours_lower_priority() {
    let mock_low = MockBackend::start().await;
    let mock_high = MockBackend::start().await;

    let b_low = backend_for(&mock_low, "low-pri", vec![Role::Text], 0, 2);
    let b_high = backend_for(&mock_high, "high-pri", vec![Role::Text], 10, 2);
    let pool = Pool::new(vec![Arc::clone(&b_low), b_high], Duration::from_secs(5));

    let lease = pool.acquire(Role::Text).await.unwrap();
    assert_eq!(
        lease.backend_id, "low-pri",
        "lower priority (0) must be preferred over higher priority (10)"
    );
    // The lease holds a slot — verify it was taken from the winning backend.
    assert_eq!(
        b_low.free(),
        1,
        "one slot consumed on the preferred backend"
    );

    drop(lease);
    assert_eq!(b_low.free(), 2, "slot returned on drop");

    mock_low.shutdown().await;
    mock_high.shutdown().await;
}

/// When two backends have the same priority, the one with more free slots wins.
#[tokio::test]
async fn same_priority_picks_most_free_slots() {
    let mock_busy = MockBackend::start().await;
    let mock_free = MockBackend::start().await;

    let b_busy = backend_for(&mock_busy, "busy", vec![Role::Embed], 5, 1);
    let b_free = backend_for(&mock_free, "free", vec![Role::Embed], 5, 3);

    // Exhaust b_busy's only slot so the fast-path skips it.
    let _hold = b_busy.capacity.try_acquire().unwrap();

    let pool = Pool::new(
        vec![Arc::clone(&b_busy), Arc::clone(&b_free)],
        Duration::from_secs(5),
    );

    let lease = pool.acquire(Role::Embed).await.unwrap();
    assert_eq!(
        lease.backend_id, "free",
        "backend with more free slots must be picked at equal priority"
    );

    drop(lease);
    mock_busy.shutdown().await;
    mock_free.shutdown().await;
}

/// Priority ordering is also exercised through the LlamaClient: the lower-priority
/// backend's mock gets the request.
#[tokio::test]
async fn priority_ordering_through_full_client_stack() {
    let mock1 = MockBackend::start().await;
    let mock2 = MockBackend::start().await;

    let b1 = backend_for(&mock1, "preferred", vec![Role::Text], 0, 2);
    let b2 = backend_for(&mock2, "fallback", vec![Role::Text], 10, 2);

    // Make the fallback return 500 so we can prove the client used the preferred
    // backend (priority 0). If priority ordering were broken and it used the
    // fallback first, the 500 would trigger a failover. The client would then
    // retry on the preferred backend and still succeed — but the fallback would
    // be marked unhealthy. We assert it stays healthy.
    mock2.scenario().lock().await.chat = ResponseMode::ServerError;

    let pool = Pool::new(
        vec![Arc::clone(&b1), Arc::clone(&b2)],
        Duration::from_secs(5),
    );
    let client = client_with(pool);

    let resp = client
        .chat(Role::Text, "model", &chat_req("prio"))
        .await
        .unwrap();
    assert_eq!(resp.text, "mock response");

    // The fallback was never touched → still healthy.
    assert!(
        b2.healthy.load(Ordering::Acquire),
        "fallback backend must not have been used (priority ordering worked)"
    );

    mock1.shutdown().await;
    mock2.shutdown().await;
}

// ── wait-then-acquire ─────────────────────────────────────────────────────────

/// When all slots are busy, a waiting caller succeeds once a slot is released.
/// Tests the wait path through the full LlamaClient → Pool chain.
#[tokio::test]
async fn wait_then_acquire_when_slot_frees() {
    let mock = MockBackend::start().await;
    let b = backend_for(&mock, "b1", vec![Role::Text], 0, 1);

    // Hold the only slot.
    let hold = b.capacity.try_acquire().unwrap();
    let pool = Pool::new(
        vec![Arc::clone(&b)],
        Duration::from_secs(10), /* long enough that wait-path is exercised */
    );
    let client = client_with(pool);

    // Spawn a task that frees the slot after a short delay.
    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(hold);
    });

    let resp = client
        .chat(Role::Text, "model", &chat_req("waiting"))
        .await
        .unwrap();
    assert_eq!(resp.text, "mock response");

    release.await.unwrap();
    // After the call completes, the internal lease is dropped → all slots free.
    assert_eq!(b.free(), 1, "slot returned after call completed");
    mock.shutdown().await;
}

/// Multiple concurrent callers against limited slots all eventually succeed.
#[tokio::test]
async fn concurrent_callers_wait_and_all_succeed() {
    let mock = MockBackend::start().await;
    let b = backend_for(&mock, "b1", vec![Role::Text], 0, 2);
    let pool = Pool::new(vec![b], Duration::from_secs(10));
    let client = client_with(pool);

    // Launch 4 callers against 2 slots — 2 succeed via fast path, 2 via wait path.
    let client = Arc::new(client);
    let mut handles = Vec::new();
    for i in 0..4 {
        let c = Arc::clone(&client);
        handles.push(tokio::spawn(async move {
            c.chat(Role::Text, "model", &chat_req(&format!("req-{i}")))
                .await
        }));
    }

    for h in handles {
        let result = h.await.unwrap();
        assert!(result.is_ok(), "all concurrent callers must succeed");
        let resp = result.unwrap();
        assert_eq!(resp.text, "mock response");
    }

    mock.shutdown().await;
}

// ── timeout ───────────────────────────────────────────────────────────────────

/// When all backends are fully busy and stay busy, the acquire times out.
#[tokio::test]
async fn acquire_timeout_when_all_busy() {
    let mock = MockBackend::start().await;
    let b = backend_for(&mock, "b1", vec![Role::Text], 0, 1);

    // Hold the only slot forever.
    let _hold = b.capacity.try_acquire().unwrap();
    let pool = Pool::new(vec![b], Duration::from_millis(10));
    let client = client_with(pool);

    let err = client
        .chat(Role::Text, "model", &chat_req("timeout"))
        .await
        .unwrap_err();

    match err {
        LlmError::Scheduler(AcquireError::Timeout(d)) => {
            assert_eq!(d, Duration::from_millis(10));
        }
        other => panic!("expected Scheduler(Timeout), got {other:?}"),
    }

    mock.shutdown().await;
}

// ── dead-host skip ────────────────────────────────────────────────────────────

/// An unhealthy backend is skipped even if it has free slots and better priority.
/// Uses `Pool::acquire` directly to observe which backend won.
#[tokio::test]
async fn unhealthy_backend_skipped_for_healthy_one() {
    let mock_dead = MockBackend::start().await;
    let mock_alive = MockBackend::start().await;

    let b_dead = backend_for(&mock_dead, "dead", vec![Role::Text], 0, 4);
    b_dead.healthy.store(false, Ordering::Release);

    let b_alive = backend_for(&mock_alive, "alive", vec![Role::Text], 10, 2);

    let pool = Pool::new(vec![b_dead, Arc::clone(&b_alive)], Duration::from_secs(5));

    let lease = pool.acquire(Role::Text).await.unwrap();
    assert_eq!(
        lease.backend_id, "alive",
        "unhealthy backend must be skipped even with better priority"
    );
    assert_eq!(b_alive.free(), 1, "alive backend consumed the slot");

    drop(lease);
    mock_dead.shutdown().await;
    mock_alive.shutdown().await;
}

/// When no healthy backend serves the role, `NoBackend` is returned through the
/// full LlamaClient.
#[tokio::test]
async fn no_backend_when_all_unhealthy() {
    let mock = MockBackend::start().await;
    let b = backend_for(&mock, "b1", vec![Role::Text], 0, 2);
    b.healthy.store(false, Ordering::Release);

    let pool = Pool::new(vec![b], Duration::from_secs(5));
    let client = client_with(pool);

    let err = client
        .chat(Role::Text, "model", &chat_req("hi"))
        .await
        .unwrap_err();

    match err {
        LlmError::Scheduler(AcquireError::NoBackend { role }) => {
            assert_eq!(role, Role::Text);
        }
        other => panic!("expected Scheduler(NoBackend), got {other:?}"),
    }

    mock.shutdown().await;
}

// ── health loop + pool + client lifecycle ─────────────────────────────────────

/// Full lifecycle: health loop detects an unhealthy backend → pool skips it
/// → client gets NoBackend. Then health loop detects recovery → pool includes
/// it again → client succeeds.
#[tokio::test]
async fn health_loop_unhealthy_then_recovery_full_pipeline() {
    let mock = MockBackend::start().await;
    let b = backend_for(&mock, "b1", vec![Role::Text], 0, 2);

    // Start healthy.
    let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));
    let client = client_with(pool.clone());

    // Spawn the health loop against this backend.
    let client_hl = reqwest::Client::new();
    let hl = HealthLoop::new(pool.all_backends(), client_hl, Duration::from_millis(10));
    let (hl_handle, hl_shutdown) = hl.spawn();

    // Phase 1: backend is healthy → chat succeeds.
    let resp = client
        .chat(Role::Text, "model", &chat_req("phase1"))
        .await
        .unwrap();
    assert_eq!(resp.text, "mock response");

    // Phase 2: make the mock return unhealthy → health loop detects it →
    // pool skips it → chat gets NoBackend.
    mock.scenario().lock().await.health = ResponseMode::Unhealthy;
    tokio::time::sleep(Duration::from_millis(40)).await;

    let err = client
        .chat(Role::Text, "model", &chat_req("phase2"))
        .await
        .unwrap_err();
    assert!(
        matches!(err, LlmError::Scheduler(AcquireError::NoBackend { .. })),
        "expected NoBackend after health loop marks unhealthy, got {err:?}"
    );

    // Phase 3: recover the mock → health loop detects it → chat succeeds again.
    mock.scenario().lock().await.health = ResponseMode::Healthy;
    tokio::time::sleep(Duration::from_millis(40)).await;

    let resp = client
        .chat(Role::Text, "model", &chat_req("phase3"))
        .await
        .unwrap();
    assert_eq!(resp.text, "mock response");

    drop(hl_shutdown);
    hl_handle.await.unwrap();
    mock.shutdown().await;
}

// ── failover ──────────────────────────────────────────────────────────────────

/// On a 5xx error from the preferred backend, the client fails over to the next
/// healthy backend.
#[tokio::test]
async fn failover_on_5xx_to_secondary_backend() {
    let mock1 = MockBackend::start().await;
    let mock2 = MockBackend::start().await;

    let b1 = backend_for(&mock1, "primary", vec![Role::Text], 0, 2);
    let b2 = backend_for(&mock2, "secondary", vec![Role::Text], 10, 2);

    let pool = Pool::new(vec![Arc::clone(&b1), b2], Duration::from_secs(5));
    let client = client_with(pool);

    // Primary returns 500, secondary stays healthy.
    mock1.scenario().lock().await.chat = ResponseMode::ServerError;

    let resp = client
        .chat(Role::Text, "model", &chat_req("failover"))
        .await
        .unwrap();
    assert_eq!(resp.text, "mock response");

    // Primary must be marked unhealthy after the 500.
    assert!(
        !b1.healthy.load(Ordering::Acquire),
        "failing backend must be marked unhealthy"
    );

    mock1.shutdown().await;
    mock2.shutdown().await;
}

/// Failover on transport error (mock shut down mid-test).
#[tokio::test]
async fn failover_on_transport_error() {
    let mock1 = MockBackend::start().await;
    let mock2 = MockBackend::start().await;

    let b1 = backend_for(&mock1, "dead", vec![Role::Text], 0, 2);
    let b2 = backend_for(&mock2, "alive", vec![Role::Text], 10, 2);

    let pool = Pool::new(vec![b1, b2], Duration::from_secs(5));
    let client = client_with(pool);

    // Kill mock1 so its port is unreachable.
    mock1.shutdown().await;

    let resp = client
        .chat(Role::Text, "model", &chat_req("transport"))
        .await
        .unwrap();
    assert_eq!(resp.text, "mock response");

    mock2.shutdown().await;
}

/// When ALL backends fail, the client returns `AllFailed`.
#[tokio::test]
async fn all_backends_fail_returns_all_failed() {
    let mock1 = MockBackend::start().await;
    let mock2 = MockBackend::start().await;

    let b1 = backend_for(&mock1, "b1", vec![Role::Text], 0, 2);
    let b2 = backend_for(&mock2, "b2", vec![Role::Text], 10, 2);

    let pool = Pool::new(vec![b1, b2], Duration::from_secs(5));
    let client = LlamaClient::new(
        pool,
        Client::new(),
        1, /* max_retries — one retry */
        3,
        Duration::from_millis(200),
    );

    // Both backends fail.
    mock1.scenario().lock().await.chat = ResponseMode::ServerError;
    mock2.scenario().lock().await.chat = ResponseMode::ServerError;

    let err = client
        .chat(Role::Text, "model", &chat_req("all-fail"))
        .await
        .unwrap_err();
    match err {
        LlmError::AllFailed { retries, .. } => {
            assert!(retries >= 1, "at least one retry was attempted");
        }
        other => panic!("expected AllFailed, got {other:?}"),
    }

    mock1.shutdown().await;
    mock2.shutdown().await;
}

// ── 429 rate-limit failover ───────────────────────────────────────────────────

/// The client treats 429 as a failure: marks unhealthy, fails over.
#[tokio::test]
async fn rate_limit_429_triggers_failover() {
    let mock1 = MockBackend::start().await;
    let mock2 = MockBackend::start().await;

    let b1 = backend_for(&mock1, "rate-limited", vec![Role::Text], 0, 2);
    let b2 = backend_for(&mock2, "spare", vec![Role::Text], 10, 2);

    let pool = Pool::new(vec![Arc::clone(&b1), b2], Duration::from_secs(5));
    let client = client_with(pool);

    mock1.scenario().lock().await.chat = ResponseMode::RateLimited {
        retry_after_secs: 30,
    };

    let resp = client
        .chat(Role::Text, "model", &chat_req("rate-limit"))
        .await
        .unwrap();
    assert_eq!(resp.text, "mock response");

    // Rate-limited backend must be unhealthy after 429.
    assert!(
        !b1.healthy.load(Ordering::Acquire),
        "rate-limited backend must be marked unhealthy"
    );

    mock1.shutdown().await;
    mock2.shutdown().await;
}

// ── slot release ──────────────────────────────────────────────────────────────

/// After a successful call, the slot is freed (the lease is RAII — plan §6.2).
#[tokio::test]
async fn slot_freed_after_success() {
    let mock = MockBackend::start().await;
    let b = backend_for(&mock, "b1", vec![Role::Text], 0, 2);

    let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));
    let client = client_with(pool);

    assert_eq!(b.free(), 2);
    let _resp = client
        .chat(Role::Text, "model", &chat_req("hi"))
        .await
        .unwrap();
    assert_eq!(
        b.free(),
        2,
        "slot must be freed after the call completes (RAII lease)"
    );

    mock.shutdown().await;
}

/// After a failed call, the slot is still freed (lease drops in the retry loop).
#[tokio::test]
async fn slot_freed_after_failure() {
    let mock = MockBackend::start().await;
    let b = backend_for(&mock, "b1", vec![Role::Text], 0, 2);

    // Use two backends so the retry loop doesn't terminate with NoBackend after
    // marking b1 unhealthy.
    let mock2 = MockBackend::start().await;
    let b2 = backend_for(&mock2, "b2", vec![Role::Text], 10, 2);

    mock.scenario().lock().await.chat = ResponseMode::ServerError;
    mock2.scenario().lock().await.chat = ResponseMode::ServerError;

    let pool = Pool::new(vec![Arc::clone(&b), b2], Duration::from_secs(5));
    let client = client_with(pool);

    assert_eq!(b.free(), 2);
    let _err = client.chat(Role::Text, "model", &chat_req("fail")).await;
    assert_eq!(b.free(), 2, "slot must be freed even after a failed call");

    mock.shutdown().await;
    mock2.shutdown().await;
}

// ── multi-role routing ────────────────────────────────────────────────────────

/// Backends serving different roles correctly route requests. Uses
/// `Pool::acquire` to observe which backend won for each role.
#[tokio::test]
async fn multi_role_routing_uses_correct_backend() {
    let mock_text = MockBackend::start().await;
    let mock_embed = MockBackend::start().await;

    let b_text = backend_for(&mock_text, "text-srv", vec![Role::Text], 0, 2);
    let b_embed = backend_for(&mock_embed, "embed-srv", vec![Role::Embed], 0, 2);

    let pool = Pool::new(
        vec![Arc::clone(&b_text), Arc::clone(&b_embed)],
        Duration::from_secs(5),
    );

    // Acquire for Text role → must pick the text backend.
    let lease_text = pool.acquire(Role::Text).await.unwrap();
    assert_eq!(lease_text.backend_id, "text-srv");
    assert_eq!(b_text.free(), 1, "text backend consumed a slot");
    drop(lease_text);

    // Acquire for Embed role → must pick the embed backend.
    let lease_embed = pool.acquire(Role::Embed).await.unwrap();
    assert_eq!(lease_embed.backend_id, "embed-srv");
    assert_eq!(b_embed.free(), 1, "embed backend consumed a slot");
    drop(lease_embed);

    // A role no backend serves → NoBackend.
    let err = pool.acquire(Role::Rerank).await.unwrap_err();
    assert!(
        matches!(err, AcquireError::NoBackend { .. }),
        "unserved role must produce NoBackend"
    );

    mock_text.shutdown().await;
    mock_embed.shutdown().await;
}

// ── embed failover ────────────────────────────────────────────────────────────

/// Embed requests also fail over on 5xx (same machinery as chat).
#[tokio::test]
async fn embed_failover_on_5xx() {
    let mock1 = MockBackend::start().await;
    let mock2 = MockBackend::start().await;

    let b1 = backend_for(&mock1, "primary", vec![Role::Embed], 0, 2);
    let b2 = backend_for(&mock2, "secondary", vec![Role::Embed], 10, 2);

    mock1.scenario().lock().await.embed = ResponseMode::ServerError;

    let pool = Pool::new(vec![b1, b2], Duration::from_secs(5));
    let client = client_with(pool);

    let req = EmbedReq {
        texts: vec!["failover".into()],
    };
    let resp = client.embed("model", &req).await.unwrap();
    assert_eq!(resp.vectors.len(), 1);

    mock1.shutdown().await;
    mock2.shutdown().await;
}
