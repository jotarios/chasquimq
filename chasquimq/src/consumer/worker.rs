use crate::config::RetryConfig;
use crate::consumer::dlq::{self, DlqReason, DlqRelocate};
use crate::consumer::retry::{self, RetryRelocate};
use crate::error::HandlerError;
use crate::job::{Job, now_ms};
use crate::redis::parse::StreamEntryId;
use bytes::Bytes;
use futures_util::FutureExt;
use serde::Serialize;
use std::future::Future;
use std::sync::Arc;
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
                let attempt_so_far = dispatched.job.attempt;
                let job_for_retry = dispatched.job.clone();
                let outcome = std::panic::AssertUnwindSafe(handler(dispatched.job))
                    .catch_unwind()
                    .await;
                match outcome {
                    Ok(Ok(())) => {
                        if wiring.ack_tx.send(entry_id).await.is_err() {
                            break;
                        }
                    }
                    Ok(Err(e)) => {
                        tracing::warn!(job_id = %job_id, error = %e, attempt = attempt_so_far + 1, "handler returned Err");
                        on_handler_failure(&wiring, entry_id, job_for_retry).await;
                    }
                    Err(_panic) => {
                        tracing::warn!(job_id = %job_id, attempt = attempt_so_far + 1, "handler panicked");
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

    if next_attempt >= wiring.max_attempts {
        dlq::enqueue(
            &wiring.dlq_tx,
            entry_id,
            encoded,
            DlqReason::RetriesExhausted,
        )
        .await;
        return;
    }

    job.attempt = next_attempt;
    let encoded_with_bumped = match rmp_serde::to_vec(&job) {
        Ok(b) => Bytes::from(b),
        Err(e) => {
            tracing::error!(job_id = %job.id, error = %e, "retry: re-encode failed; entry will reclaim via CLAIM");
            return;
        }
    };
    let backoff = retry::backoff_ms(next_attempt, &wiring.retry_cfg);
    let run_at = now_ms().saturating_add(backoff);
    let run_at_i64 = i64::try_from(run_at).unwrap_or(i64::MAX);
    retry::enqueue(&wiring.retry_tx, entry_id, encoded_with_bumped, run_at_i64).await;
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
