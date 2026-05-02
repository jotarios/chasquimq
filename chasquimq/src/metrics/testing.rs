//! In-memory sink for integration tests.
//!
//! Accumulates every event with both raw history (`Vec` accessors) and
//! derived rollups (counts, sums, last-values). Not intended for production —
//! locking + unbounded `Vec` growth is deliberately unsophisticated.
//!
//! History `Vec`s go through `Mutex` because tests want full sequences
//! (`job_outcomes()`, `dlq_events()`); aggregate counters use `AtomicU64`
//! so `jobs_completed()` / `dlq_count()` etc. don't take a lock just to
//! read a number. The split is deliberate, not a refactor target.

use super::{
    DlqReason, DlqRouted, JobOutcome, JobOutcomeKind, LockOutcome, MetricsSink, PromoterTick,
    ReaderBatch, RetryScheduled,
};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct InMemorySink {
    ticks: Mutex<Vec<PromoterTick>>,
    reader_batches: Mutex<Vec<ReaderBatch>>,
    job_outcomes: Mutex<Vec<JobOutcome>>,
    retry_events: Mutex<Vec<RetryScheduled>>,
    dlq_events: Mutex<Vec<DlqRouted>>,
    acquired: AtomicU64,
    held: AtomicU64,
    jobs_completed: AtomicU64,
    jobs_failed_err: AtomicU64,
    jobs_failed_panic: AtomicU64,
    retries_total: AtomicU64,
}

impl InMemorySink {
    pub fn new() -> Self {
        Self::default()
    }

    // -------- promoter --------

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
        self.acquired.load(Ordering::Relaxed)
    }

    pub fn held_count(&self) -> u64 {
        self.held.load(Ordering::Relaxed)
    }

    // -------- reader --------

    pub fn reader_batches(&self) -> Vec<ReaderBatch> {
        self.reader_batches.lock().expect("poisoned").clone()
    }

    // -------- worker --------

    pub fn job_outcomes(&self) -> Vec<JobOutcome> {
        self.job_outcomes.lock().expect("poisoned").clone()
    }

    pub fn jobs_completed(&self) -> u64 {
        self.jobs_completed.load(Ordering::Relaxed)
    }

    pub fn jobs_failed(&self) -> u64 {
        self.jobs_failed_err.load(Ordering::Relaxed)
            + self.jobs_failed_panic.load(Ordering::Relaxed)
    }

    pub fn panics(&self) -> u64 {
        self.jobs_failed_panic.load(Ordering::Relaxed)
    }

    pub fn last_handler_duration_us(&self) -> Option<u64> {
        self.job_outcomes
            .lock()
            .expect("poisoned")
            .last()
            .map(|o| o.handler_duration_us)
    }

    // -------- retry --------

    pub fn retry_events(&self) -> Vec<RetryScheduled> {
        self.retry_events.lock().expect("poisoned").clone()
    }

    pub fn total_retries(&self) -> u64 {
        self.retries_total.load(Ordering::Relaxed)
    }

    // -------- dlq --------

    pub fn dlq_events(&self) -> Vec<DlqRouted> {
        self.dlq_events.lock().expect("poisoned").clone()
    }

    /// Count of DLQ events whose reason matches the given variant.
    /// `Malformed` matches regardless of the inner `reason` string.
    pub fn dlq_count(&self, reason: DlqReason) -> u64 {
        let want_kind = reason.as_str();
        self.dlq_events
            .lock()
            .expect("poisoned")
            .iter()
            .filter(|e| e.reason.as_str() == want_kind)
            .count() as u64
    }
}

impl MetricsSink for InMemorySink {
    fn promoter_tick(&self, tick: PromoterTick) {
        self.ticks.lock().expect("poisoned").push(tick);
    }

    fn promoter_lock_outcome(&self, outcome: LockOutcome) {
        match outcome {
            LockOutcome::Acquired => {
                self.acquired.fetch_add(1, Ordering::Relaxed);
            }
            LockOutcome::Held => {
                self.held.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn reader_batch(&self, batch: ReaderBatch) {
        self.reader_batches.lock().expect("poisoned").push(batch);
    }

    fn job_outcome(&self, outcome: JobOutcome) {
        match outcome.kind {
            JobOutcomeKind::Ok => {
                self.jobs_completed.fetch_add(1, Ordering::Relaxed);
            }
            JobOutcomeKind::Err => {
                self.jobs_failed_err.fetch_add(1, Ordering::Relaxed);
            }
            JobOutcomeKind::Panic => {
                self.jobs_failed_panic.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.job_outcomes.lock().expect("poisoned").push(outcome);
    }

    fn retry_scheduled(&self, retry: RetryScheduled) {
        self.retries_total.fetch_add(1, Ordering::Relaxed);
        self.retry_events.lock().expect("poisoned").push(retry);
    }

    fn dlq_routed(&self, dlq: DlqRouted) {
        self.dlq_events.lock().expect("poisoned").push(dlq);
    }
}
