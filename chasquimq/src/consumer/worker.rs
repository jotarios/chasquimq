use crate::error::HandlerError;
use crate::job::Job;
use crate::redis::parse::StreamEntryId;
use futures_util::FutureExt;
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

pub(crate) fn spawn_workers<T, H, Fut>(
    concurrency: usize,
    handler: H,
    job_rx: async_channel::Receiver<DispatchedJob<T>>,
    ack_tx: mpsc::Sender<StreamEntryId>,
) -> WorkerPool
where
    T: Send + 'static,
    H: Fn(Job<T>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = std::result::Result<(), HandlerError>> + Send + 'static,
{
    let handler = Arc::new(handler);
    let mut set: JoinSet<()> = JoinSet::new();
    for _ in 0..concurrency {
        let handler = handler.clone();
        let rx = job_rx.clone();
        let ack_tx = ack_tx.clone();
        set.spawn(async move {
            while let Ok(dispatched) = rx.recv().await {
                let entry_id = dispatched.entry_id;
                let job_id = dispatched.job.id.clone();
                let outcome = std::panic::AssertUnwindSafe(handler(dispatched.job))
                    .catch_unwind()
                    .await;
                match outcome {
                    Ok(Ok(())) => {
                        if ack_tx.send(entry_id).await.is_err() {
                            break;
                        }
                    }
                    Ok(Err(e)) => {
                        tracing::warn!(job_id = %job_id, error = %e, "handler returned Err");
                    }
                    Err(_panic) => {
                        tracing::warn!(job_id = %job_id, "handler panicked");
                    }
                }
            }
        });
    }
    WorkerPool { set }
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
