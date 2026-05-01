//! Observability hooks for engine internals.
//!
//! The engine emits structured numeric events through the [`MetricsSink`] trait
//! and ships only a no-op default plus an in-memory sink for tests. Concrete
//! Prometheus / OTel adapters live outside this crate so we don't pin downstream
//! users to a specific metrics ecosystem.
//!
//! Today only the [`Promoter`](crate::promoter::Promoter) emits events. Other
//! components will grow their own callbacks on this same trait as the
//! observability surface expands.

use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
pub struct PromoterTick {
    /// Number of jobs the promote script moved from the delayed ZSET to the
    /// stream on this tick. `0` when the lock was held but nothing was due.
    pub promoted: u64,
    /// `ZCARD` of the delayed ZSET *after* promotion. Operators graph this
    /// to spot growing backlogs.
    pub depth: u64,
    /// Lag of the oldest entry **still pending after this tick's promotion
    /// completed**, in milliseconds: `now_ms - min_score_in_zset` when the
    /// remaining oldest entry is past-due, otherwise `0`.
    ///
    /// Saturation rules:
    /// - ZSET empty: `0` (nothing to be late about).
    /// - ZSET non-empty but oldest is future-dated: `0` (we're keeping up).
    /// - ZSET non-empty and oldest is past-due: positive ms-overdue.
    ///
    /// In a healthy steady state where the promoter keeps up, this is `0`
    /// most ticks — the metric becomes positive only when a backlog forms
    /// and an entry that should already have run is still sitting in the
    /// ZSET. Alert thresholds should be tuned around "how late is too
    /// late," not "did the metric move."
    pub oldest_pending_lag_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockOutcome {
    /// We hold the leader lock for this tick.
    Acquired,
    /// Another holder owns the lock; we'll sleep and retry.
    Held,
}

/// Receiver of engine-internal observability events.
///
/// **Implementations must be cheap and non-blocking.** Events fire on the
/// promoter's hot loop, and any latency added here directly raises
/// end-to-end scheduling lag. Specifically:
///
/// - Do **not** call `block_on` or anything else that parks the runtime.
/// - Do **not** acquire long-held locks; per-call work should be O(1).
/// - **Safe**: incrementing a Prometheus `Counter` / setting a `Gauge`,
///   pushing into an unbounded `crossbeam::channel`, atomic increments.
/// - **Unsafe**: synchronous network I/O, `tokio::sync::Mutex::lock().await`
///   via `block_on`, anything that allocates per-event in the millions.
///
/// The default [`NoopSink`] is zero-cost; use it (the configs default to it)
/// unless you've wired a real metrics backend.
pub trait MetricsSink: Send + Sync + 'static {
    fn promoter_tick(&self, _tick: PromoterTick) {}
    fn promoter_lock_outcome(&self, _outcome: LockOutcome) {}
}

/// Default sink — drops every event.
pub struct NoopSink;

impl MetricsSink for NoopSink {}

pub fn noop_sink() -> Arc<dyn MetricsSink> {
    Arc::new(NoopSink)
}

/// Dispatch a sink call, swallowing panics so a misbehaving user sink can
/// never take down the engine. The first panic per process is logged via
/// `tracing::error!`; subsequent panics fire a `tracing::warn!` with a
/// counter so a chronically-broken sink is visible without flooding logs.
///
/// This is the only correct way to invoke `MetricsSink` methods from
/// engine code. Direct calls bypass panic safety.
///
/// `AssertUnwindSafe` is fine here because the closure only borrows
/// `&dyn MetricsSink` and `Copy` payloads (`PromoterTick`, `LockOutcome`)
/// — there's no internal mutable state that could be observed in a
/// half-mutated form across a panic boundary.
pub(crate) fn dispatch<F: FnOnce()>(label: &'static str, f: F) {
    use std::panic::AssertUnwindSafe;
    use std::sync::atomic::{AtomicU64, Ordering};
    static PANIC_COUNT: AtomicU64 = AtomicU64::new(0);
    if let Err(payload) = std::panic::catch_unwind(AssertUnwindSafe(f)) {
        let n = PANIC_COUNT.fetch_add(1, Ordering::Relaxed);
        let msg = panic_message(&payload);
        if n == 0 {
            tracing::error!(
                sink_call = label,
                panic = msg.as_deref().unwrap_or("<non-string panic>"),
                "MetricsSink implementation panicked; engine continues but \
                 metrics for this call were dropped. Subsequent panics from \
                 the same sink will be logged at WARN with a counter."
            );
        } else {
            tracing::warn!(
                sink_call = label,
                total_panics = n + 1,
                panic = msg.as_deref().unwrap_or("<non-string panic>"),
                "MetricsSink panicked again"
            );
        }
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> Option<String> {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        return Some((*s).to_string());
    }
    if let Some(s) = payload.downcast_ref::<String>() {
        return Some(s.clone());
    }
    None
}

/// In-memory sink that accumulates every event for inspection. Useful in
/// integration tests and ad-hoc diagnostics; not intended for production —
/// the locking + unbounded `Vec` growth is deliberately unsophisticated.
pub mod testing {
    use super::{LockOutcome, MetricsSink, PromoterTick};
    use std::sync::Mutex;

    /// Tick history goes through a `Mutex<Vec<_>>` because callers want the
    /// full history (`ticks()`); lock counters use `AtomicU64` so assertions
    /// don't have to take a lock the test will then drop a millisecond later.
    /// The split is deliberate, not a refactor target.
    #[derive(Default)]
    pub struct InMemorySink {
        ticks: Mutex<Vec<PromoterTick>>,
        acquired: std::sync::atomic::AtomicU64,
        held: std::sync::atomic::AtomicU64,
    }

    impl InMemorySink {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn ticks(&self) -> Vec<PromoterTick> {
            self.ticks.lock().expect("poisoned").clone()
        }

        pub fn promoted_total(&self) -> u64 {
            self.ticks
                .lock()
                .expect("poisoned")
                .iter()
                .map(|t| t.promoted)
                .sum()
        }

        pub fn last_depth(&self) -> Option<u64> {
            self.ticks.lock().expect("poisoned").last().map(|t| t.depth)
        }

        pub fn max_oldest_pending_lag_ms(&self) -> u64 {
            self.ticks
                .lock()
                .expect("poisoned")
                .iter()
                .map(|t| t.oldest_pending_lag_ms)
                .max()
                .unwrap_or(0)
        }

        pub fn acquired_count(&self) -> u64 {
            self.acquired.load(std::sync::atomic::Ordering::Relaxed)
        }

        pub fn held_count(&self) -> u64 {
            self.held.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    impl MetricsSink for InMemorySink {
        fn promoter_tick(&self, tick: PromoterTick) {
            self.ticks.lock().expect("poisoned").push(tick);
        }

        fn promoter_lock_outcome(&self, outcome: LockOutcome) {
            use std::sync::atomic::Ordering;
            match outcome {
                LockOutcome::Acquired => {
                    self.acquired.fetch_add(1, Ordering::Relaxed);
                }
                LockOutcome::Held => {
                    self.held.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct PanickingSink;
    impl MetricsSink for PanickingSink {
        fn promoter_tick(&self, _tick: PromoterTick) {
            panic!("intentional sink panic for test");
        }
        fn promoter_lock_outcome(&self, _outcome: LockOutcome) {
            panic!("intentional sink lock-outcome panic");
        }
    }

    #[test]
    fn dispatch_swallows_panic_and_returns() {
        // Confirm calling code resumes normally after a panicking sink.
        let sink = PanickingSink;
        dispatch("promoter_tick", || {
            sink.promoter_tick(PromoterTick {
                promoted: 1,
                depth: 0,
                oldest_pending_lag_ms: 0,
            });
        });
        // Reaching this line proves catch_unwind captured the panic.
        // (If it hadn't, the `dispatch` call above would have unwound
        // through the test, failing it via the panic itself.)

        // Second call also doesn't propagate.
        dispatch("promoter_lock_outcome", || {
            sink.promoter_lock_outcome(LockOutcome::Acquired);
        });
    }

    #[test]
    fn dispatch_passes_through_non_panicking_call() {
        use std::cell::Cell;
        let counter = Cell::new(0u32);
        dispatch("test", || counter.set(42));
        assert_eq!(counter.get(), 42);
    }
}
