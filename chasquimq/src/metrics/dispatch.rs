//! Panic-safe sink invocation.
//!
//! Engine code never calls `MetricsSink` methods directly — every call goes
//! through [`dispatch`] so that a buggy user sink can't take down the
//! engine. The first panic per process logs at `error`; subsequent panics
//! log at `warn` with a counter so a chronically-broken sink stays visible
//! without flooding logs.

use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};

/// Dispatch a sink call, swallowing panics. See module docs.
///
/// `AssertUnwindSafe` is fine here because the closure only borrows
/// `&dyn MetricsSink` and moves owned plain-data payloads (no shared
/// interior-mutable state) — nothing can be observed in a half-mutated
/// form across a panic boundary.
pub(crate) fn dispatch<F: FnOnce()>(label: &'static str, f: F) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{LockOutcome, MetricsSink, PromoterTick};

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
