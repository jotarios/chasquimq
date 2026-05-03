use crate::config::RetryConfig;
use crate::consumer::dlq::{self, DlqReason, DlqRelocate};
use crate::consumer::retry::{self, RetryRelocate};
use crate::error::HandlerError;
use crate::events::EventsWriter;
use crate::job::{Job, now_ms};
use crate::metrics::{self, JobOutcome, JobOutcomeKind, MetricsSink};
use crate::redis::parse::StreamEntryId;
use bytes::Bytes;
use futures_util::FutureExt;
use serde::Serialize;
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

pub(crate) struct DispatchedJob<T> {
    pub entry_id: StreamEntryId,
    pub job: Job<T>,
}

pub(crate) struct WorkerPool {
    set: JoinSet<()>,
}

pub(crate) struct WorkerWiring {
    pub ack_tx: mpsc::Sender<StreamEntryId>,
    pub retry_tx: mpsc::Sender<RetryRelocate>,
    pub dlq_tx: mpsc::Sender<DlqRelocate>,
    pub max_attempts: u32,
    pub retry_cfg: RetryConfig,
    pub metrics: Arc<dyn MetricsSink>,
    pub events: EventsWriter,
}

pub(crate) fn spawn_workers<T, H, Fut>(
    concurrency: usize,
    handler: H,
    job_rx: async_channel::Receiver<DispatchedJob<T>>,
    wiring: WorkerWiring,
) -> WorkerPool
where
    T: Serialize + Clone + Send + 'static,
    H: Fn(Job<T>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = std::result::Result<(), HandlerError>> + Send + 'static,
{
    let handler = Arc::new(handler);
    let wiring = Arc::new(wiring);
    let mut set: JoinSet<()> = JoinSet::new();
    for _ in 0..concurrency {
        let handler = handler.clone();
        let rx = job_rx.clone();
        let wiring = wiring.clone();
        set.spawn(async move {
            while let Ok(dispatched) = rx.recv().await {
                let entry_id = dispatched.entry_id.clone();
                let job_id = dispatched.job.id.clone();
                // 1-indexed attempt number that's *about* to run. `Job::attempt`
                // is 0-indexed ("0 runs have happened yet"), so the run we're
                // about to execute is number `Job::attempt + 1`. Used in both
                // the JobOutcome event (where it's "the run that just executed"
                // by the time the event fires) and human-readable log lines.
                let attempt_index = dispatched.job.attempt.saturating_add(1);
                let job_for_retry = dispatched.job.clone();

                // Emit `active` *before* the handler runs so subscribers can
                // build a "currently running" view that's correct even for
                // handlers that take a long time. Best-effort by design — a
                // failing XADD must not delay the handler.
                wiring.events.emit_active(&job_id, attempt_index).await;

                let started = Instant::now();
                let outcome = std::panic::AssertUnwindSafe(handler(dispatched.job))
                    .catch_unwind()
                    .await;
                // Microseconds, not millis: most handlers complete well under
                // 1ms — recording in ms makes every fast handler look like 0
                // and the histogram is useless. u128 → u64 saturates at ~584
                // millennia, well past any sane handler duration.
                let duration_us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);

                let kind = match &outcome {
                    Ok(Ok(())) => JobOutcomeKind::Ok,
                    Ok(Err(_)) => JobOutcomeKind::Err,
                    Err(_) => JobOutcomeKind::Panic,
                };
                let event = JobOutcome {
                    kind,
                    attempt: attempt_index,
                    handler_duration_us: duration_us,
                };
                let sink = &*wiring.metrics;
                metrics::dispatch("job_outcome", || sink.job_outcome(event));

                match outcome {
                    Ok(Ok(())) => {
                        wiring
                            .events
                            .emit_completed(&job_id, attempt_index, duration_us)
                            .await;
                        if wiring.ack_tx.send(entry_id).await.is_err() {
                            break;
                        }
                    }
                    Ok(Err(e)) => {
                        let reason = format!("{e}");
                        tracing::warn!(job_id = %job_id, error = %e, attempt = attempt_index, "handler returned Err");
                        wiring
                            .events
                            .emit_failed(&job_id, attempt_index, &reason, Some(duration_us))
                            .await;
                        on_handler_failure(&wiring, entry_id, job_for_retry).await;
                    }
                    Err(_panic) => {
                        tracing::warn!(job_id = %job_id, attempt = attempt_index, "handler panicked");
                        wiring
                            .events
                            .emit_failed(
                                &job_id,
                                attempt_index,
                                "handler panicked",
                                Some(duration_us),
                            )
                            .await;
                        on_handler_failure(&wiring, entry_id, job_for_retry).await;
                    }
                }
            }
        });
    }
    WorkerPool { set }
}

async fn on_handler_failure<T: Serialize + Send + 'static>(
    wiring: &WorkerWiring,
    entry_id: StreamEntryId,
    mut job: Job<T>,
) {
    let next_attempt = job.attempt.saturating_add(1);
    let encoded = match rmp_serde::to_vec(&job) {
        Ok(b) => Bytes::from(b),
        Err(e) => {
            tracing::error!(job_id = %job.id, error = %e, "retry: re-encode failed; entry will reclaim via CLAIM");
            return;
        }
    };

    // 1-indexed run count for metric events. `Job::attempt` is 0-indexed
    // ("0 runs have happened yet"), so the run that just executed is
    // `job.attempt + 1`, and the next run will be `job.attempt + 2`.
    // `next_attempt` above happens to equal "current 1-indexed run count"
    // numerically, but that's a coincidence — derive metric values from
    // run-counts explicitly so the meaning is unambiguous at the call site.
    let just_ran = job.attempt.saturating_add(1);
    let will_run_next = just_ran.saturating_add(1);

    // Per-job override: `Job::retry.max_attempts` wins over the queue-wide
    // `WorkerWiring::max_attempts` when set. None → fall back.
    let max_attempts = job
        .retry
        .as_ref()
        .and_then(|r| r.max_attempts)
        .unwrap_or(wiring.max_attempts);

    if next_attempt >= max_attempts {
        dlq::enqueue(
            &wiring.dlq_tx,
            job.id.clone(),
            entry_id,
            encoded,
            DlqReason::RetriesExhausted,
            just_ran,
        )
        .await;
        return;
    }

    // Per-job override: `Job::retry.backoff` wins over the queue-wide
    // `WorkerWiring::retry_cfg` when set. None → fall back to the
    // existing exponential math driven by `RetryConfig`.
    let backoff = match job.retry.as_ref().and_then(|r| r.backoff.as_ref()) {
        Some(spec) => retry::backoff_ms_from_spec(next_attempt, spec, &wiring.retry_cfg),
        None => retry::backoff_ms(next_attempt, &wiring.retry_cfg),
    };

    job.attempt = next_attempt;
    let encoded_with_bumped = match rmp_serde::to_vec(&job) {
        Ok(b) => Bytes::from(b),
        Err(e) => {
            tracing::error!(job_id = %job.id, error = %e, "retry: re-encode failed; entry will reclaim via CLAIM");
            return;
        }
    };
    let run_at = now_ms().saturating_add(backoff);
    let run_at_i64 = i64::try_from(run_at).unwrap_or(i64::MAX);
    retry::enqueue(
        &wiring.retry_tx,
        job.id.clone(),
        entry_id,
        encoded_with_bumped,
        run_at_i64,
        will_run_next,
        backoff,
    )
    .await;
}

pub(crate) async fn drain_workers(mut pool: WorkerPool, deadline: std::time::Duration) {
    let drain = async { while pool.set.join_next().await.is_some() {} };
    if tokio::time::timeout(deadline, drain).await.is_err() {
        tracing::warn!(
            ?deadline,
            "worker drain hit deadline; aborting in-flight tasks"
        );
        pool.set.abort_all();
        while pool.set.join_next().await.is_some() {}
    }
}
