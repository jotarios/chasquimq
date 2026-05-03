//! Observability hooks for engine internals.
//!
//! The engine emits structured numeric events through the [`MetricsSink`] trait
//! and ships only a no-op default plus an in-memory sink for tests. Concrete
//! Prometheus / OTel adapters live outside this crate so we don't pin downstream
//! users to a specific metrics ecosystem.
//!
//! # Module layout
//!
//! - This module defines the public event types and the [`MetricsSink`] trait.
//! - [`dispatch`] is the engine-internal panic-safe sink invocation helper —
//!   not part of the public surface, but documented here because all engine
//!   call sites must use it.
//! - [`testing::InMemorySink`] accumulates every event for assertions in
//!   integration tests; not intended for production.
//!
//! # Event surface
//!
//! - [`PromoterTick`] / [`LockOutcome`]: delayed-job promoter telemetry.
//! - [`ReaderBatch`]: per-`XREADGROUP` batch shape (size, reclaimed count).
//! - [`JobOutcome`]: per-handler-invocation outcome (Ok / Err / Panic) +
//!   wall-clock duration.
//! - [`RetryScheduled`]: emitted only when a retry was actually rescheduled
//!   (the atomic gate in `RETRY_RESCHEDULE_SCRIPT` returned `1`).
//! - [`DlqRouted`]: emitted after a DLQ relocation succeeds, carrying the
//!   reason and the attempt count that just gave up.
//!
//! # What counts as "the job"
//!
//! Operators reading dashboards: the metrics map onto handler invocations,
//! not raw inbound jobs. Specifically:
//!
//! - **Reader-side DLQ paths** (malformed entry, oversize payload, decode
//!   failure, retries-exhausted-on-arrival): emit [`DlqRouted`] only.
//!   No [`JobOutcome`] — the handler never ran. These events carry
//!   `attempt: 0`.
//! - **Worker handler runs**: emit exactly one [`JobOutcome`].
//! - **Worker-side DLQ** (handler failed enough times): emit
//!   [`JobOutcome::Err`] (or [`JobOutcomeKind::Panic`]) AND a downstream
//!   [`DlqRouted`].
//!
//! So `chasquimq_jobs_completed_total + chasquimq_jobs_failed_total` is the
//! count of handler invocations; total inbound jobs = handler invocations +
//! `chasquimq_dlq_routed_total{reason!="retries_exhausted"}` (roughly —
//! `retries_exhausted` shows up on both worker-side and reader-side paths).

use std::sync::Arc;

mod dispatch;
pub mod testing;

pub(crate) use dispatch::dispatch;

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

/// Shape of the most recent non-empty `XREADGROUP` response. Empty responses
/// are not emitted — at low traffic with `block_ms` polling, that would
/// produce tens of events per second per consumer with no useful signal.
/// Track Redis-side metrics if you need raw poll rate.
#[derive(Debug, Clone, Copy)]
pub struct ReaderBatch {
    /// Number of stream entries Redis returned. This is the **raw** count
    /// before dispatch-time filtering — entries that are about to be
    /// DLQ-routed for being malformed / oversize / undecodable still count
    /// here. Subtract the corresponding `chasquimq_dlq_routed_total`
    /// counters to get "actually dispatched to a handler."
    pub size: u64,
    /// Subset of `size` whose `delivery_count > 1` — entries that were
    /// reclaimed via the `XREADGROUP ... CLAIM <idle_ms>` safety net rather
    /// than being seen for the first time. A persistently non-zero value
    /// means consumers are crashing mid-handler often enough that the
    /// CLAIM path is doing real work.
    pub reclaimed: u64,
}

/// Outcome of a single handler invocation.
#[derive(Debug, Clone, Copy)]
pub struct JobOutcome {
    pub kind: JobOutcomeKind,
    /// 1-indexed attempt number that just ran. The first invocation is `1`,
    /// the first retry is `2`, etc. Operators slice the failure counter by
    /// this to see whether retries are succeeding (`Ok` events at attempt
    /// > 1) or jobs are stuck failing on the same attempt forever.
    pub attempt: u32,
    /// Wall-clock time the handler future spent before resolving (or
    /// panicking), in **microseconds**. Measured with `Instant::now()`
    /// immediately before and after the `await`; includes time waiting on
    /// I/O inside the handler. Microseconds (not milliseconds) because
    /// most handlers in queue workloads complete well under 1ms — recording
    /// in ms would pile every sub-millisecond handler at zero. Adapters
    /// converting to a Prometheus histogram should render as seconds
    /// (`microseconds * 1e-6`) to match Prometheus convention.
    pub handler_duration_us: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobOutcomeKind {
    Ok,
    Err,
    Panic,
}

/// Emitted after the retry relocator has atomically removed the in-flight
/// stream entry and re-added it to the delayed ZSET (the
/// `RETRY_RESCHEDULE_SCRIPT` returned `1`). When the script returns `0`
/// (XACKDEL race lost — entry was already removed by a concurrent path),
/// no event fires.
#[derive(Debug, Clone, Copy)]
pub struct RetryScheduled {
    /// 1-indexed attempt number the rescheduled job will run as. Matches
    /// the `attempt` operators see on the eventual [`JobOutcome`] for the
    /// same job, so a `RetryScheduled{attempt: 3}` tells you "the next
    /// `JobOutcome` for this job will be at `attempt: 3`."
    pub attempt: u32,
    /// Backoff in ms that was applied (`run_at - now_at_enqueue`).
    /// Useful as a histogram to confirm exponential backoff is doing
    /// what `RetryConfig` says it should.
    pub backoff_ms: u64,
}

/// Emitted after the DLQ relocator has atomically moved an entry to the
/// DLQ stream and acked it from the main group.
#[derive(Debug, Clone, Copy)]
pub struct DlqRouted {
    pub reason: DlqReason,
    /// For worker-side routes (handler exhausted retries), the attempt
    /// that just gave up. For reader-side routes (malformed / oversize /
    /// decode-fail / retries-exhausted-on-arrival), `0` for the first
    /// three, and the carried attempt count for the last.
    pub attempt: u32,
}

/// Why an entry ended up in the DLQ. Lives in the metrics module because it
/// is part of the public observability surface; the consumer module
/// re-exports it for internal use at the routing call sites.
///
/// `Malformed { reason }` carries `&'static str` (not `String`) so the whole
/// enum is `Copy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DlqReason {
    /// Handler ran `max_attempts` times without succeeding (worker side),
    /// or arrived already past `max_attempts` after a CLAIM-recovery cycle
    /// (reader side).
    RetriesExhausted,
    /// `rmp_serde` could not decode the stream entry's payload bytes into
    /// the consumer's `Job<T>`.
    DecodeFailed,
    /// The stream entry was structurally invalid — wrong fields, missing
    /// id, etc. Carries a short static reason (e.g. `"missing payload"`).
    Malformed { reason: &'static str },
    /// Payload size exceeded `ConsumerConfig::max_payload_bytes`.
    OversizePayload,
    /// Handler returned [`crate::error::HandlerError::unrecoverable`],
    /// signalling the failure is terminal. The consumer skips the retry
    /// path and routes the job straight to the DLQ regardless of the
    /// remaining attempt budget.
    Unrecoverable,
}

impl DlqReason {
    /// Stable string key for metric `reason` labels. Snake_case to match
    /// Prometheus convention.
    pub fn as_str(&self) -> &'static str {
        match self {
            DlqReason::RetriesExhausted => "retries_exhausted",
            DlqReason::DecodeFailed => "decode_failed",
            DlqReason::Malformed { .. } => "malformed",
            DlqReason::OversizePayload => "oversize_payload",
            DlqReason::Unrecoverable => "unrecoverable",
        }
    }

    /// Sub-reason detail when available (currently only the `Malformed`
    /// variant carries one). Adapter sinks may surface this as a secondary
    /// label or drop it.
    pub fn detail(&self) -> Option<&'static str> {
        if let DlqReason::Malformed { reason } = self {
            Some(reason)
        } else {
            None
        }
    }
}

/// Receiver of engine-internal observability events.
///
/// **Implementations must be cheap and non-blocking.** Events fire on hot
/// loops — promoter ticks, every handler invocation, every retry/DLQ
/// transition — and any latency added here directly raises end-to-end
/// scheduling lag and lowers throughput. Specifically:
///
/// - Do **not** call `block_on` or anything else that parks the runtime.
/// - Do **not** acquire long-held locks; per-call work should be O(1).
/// - **Safe**: incrementing a Prometheus `Counter` / setting a `Gauge`,
///   recording into a `Histogram`, pushing into an unbounded
///   `crossbeam::channel`, atomic increments.
/// - **Unsafe**: synchronous network I/O, `tokio::sync::Mutex::lock().await`
///   via `block_on`, anything that allocates per-event in the millions.
///
/// The default [`NoopSink`] is zero-cost; use it (the configs default to it)
/// unless you've wired a real metrics backend.
///
/// All trait methods have default no-op bodies so adding new events here
/// in future does not break downstream `MetricsSink` implementations.
pub trait MetricsSink: Send + Sync + 'static {
    fn promoter_tick(&self, _tick: PromoterTick) {}
    fn promoter_lock_outcome(&self, _outcome: LockOutcome) {}
    fn reader_batch(&self, _batch: ReaderBatch) {}
    fn job_outcome(&self, _outcome: JobOutcome) {}
    fn retry_scheduled(&self, _retry: RetryScheduled) {}
    fn dlq_routed(&self, _dlq: DlqRouted) {}
}

/// Default sink — drops every event.
pub struct NoopSink;

impl MetricsSink for NoopSink {}

pub fn noop_sink() -> Arc<dyn MetricsSink> {
    Arc::new(NoopSink)
}
