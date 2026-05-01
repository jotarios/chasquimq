mod dlq;
mod reader;
mod retry;
mod worker;

use crate::ack::{AckFlusherConfig, run_ack_flusher};
use crate::config::{ConsumerConfig, PromoterConfig};
use crate::error::{HandlerError, Result};
use crate::job::Job;
use crate::promoter::Promoter;
use crate::redis::conn::connect;
use crate::redis::group::ensure_group;
use crate::redis::keys::{delayed_key, dlq_key, stream_key};
use crate::redis::parse::StreamEntryId;
use dlq::{DlqRelocatorConfig, run_relocator};
use reader::{ReadState, reader_loop};
use retry::{RetryRelocatorConfig, run_retry_relocator};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use worker::{DispatchedJob, WorkerWiring, drain_workers, spawn_workers};

pub struct Consumer<T> {
    redis_url: String,
    cfg: ConsumerConfig,
    stream_key: String,
    delayed_key: String,
    dlq_key: String,
    _marker: PhantomData<fn() -> T>,
}

impl<T> Consumer<T>
where
    T: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    pub fn new(redis_url: impl Into<String>, cfg: ConsumerConfig) -> Self {
        Self {
            redis_url: redis_url.into(),
            stream_key: stream_key(&cfg.queue_name),
            delayed_key: delayed_key(&cfg.queue_name),
            dlq_key: dlq_key(&cfg.queue_name),
            cfg,
            _marker: PhantomData,
        }
    }

    pub async fn run<H, Fut>(self, handler: H, shutdown: CancellationToken) -> Result<()>
    where
        H: Fn(Job<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<(), HandlerError>> + Send + 'static,
    {
        let reader = connect(&self.redis_url).await?;
        let dlq_writer = connect(&self.redis_url).await?;
        let ack_client = connect(&self.redis_url).await?;
        let retry_client = connect(&self.redis_url).await?;

        ensure_group(&reader, &self.stream_key, &self.cfg.group).await?;

        let concurrency = self.cfg.concurrency.max(1);
        let (job_tx, job_rx) = async_channel::bounded::<DispatchedJob<T>>(concurrency * 2);
        let (ack_tx, ack_rx) = mpsc::channel::<StreamEntryId>(concurrency * 4);
        let (dlq_tx, dlq_rx) = mpsc::channel(self.cfg.dlq_inflight.max(1));
        let (retry_tx, retry_rx) = mpsc::channel(self.cfg.retry_inflight.max(1));

        let ack_handle = tokio::spawn(run_ack_flusher(
            ack_client,
            AckFlusherConfig {
                stream_key: self.stream_key.clone(),
                group: self.cfg.group.clone(),
                batch: self.cfg.ack_batch,
                idle: std::time::Duration::from_millis(self.cfg.ack_idle_ms),
            },
            ack_rx,
        ));

        let dlq_producer_id: Arc<str> = Arc::from(uuid::Uuid::new_v4().to_string());
        let dlq_handle = tokio::spawn(run_relocator(
            dlq_writer,
            DlqRelocatorConfig {
                stream_key: self.stream_key.clone(),
                dlq_key: self.dlq_key.clone(),
                group: self.cfg.group.clone(),
                producer_id: dlq_producer_id,
                max_stream_len: self.cfg.dlq_max_stream_len,
            },
            dlq_rx,
        ));

        let retry_handle = tokio::spawn(run_retry_relocator(
            retry_client,
            RetryRelocatorConfig {
                stream_key: self.stream_key.clone(),
                delayed_key: self.delayed_key.clone(),
                group: self.cfg.group.clone(),
            },
            retry_rx,
        ));

        let promoter_handle = self.spawn_promoter(shutdown.clone());

        let wiring = WorkerWiring {
            ack_tx: ack_tx.clone(),
            retry_tx: retry_tx.clone(),
            dlq_tx: dlq_tx.clone(),
            max_attempts: self.cfg.max_attempts,
            retry_cfg: self.cfg.retry.clone(),
        };
        let workers = spawn_workers(concurrency, handler, job_rx, wiring);
        drop(ack_tx);
        drop(retry_tx);

        let read_state = ReadState {
            reader,
            stream_key: Arc::<str>::from(self.stream_key.clone()),
            cfg: self.cfg.clone(),
            job_tx,
            dlq_tx,
            shutdown: shutdown.clone(),
        };
        let reader_outcome = reader_loop::<T>(read_state).await;

        let promoter_outcome = match promoter_handle {
            Some(h) => match h.await {
                Ok(res) => res,
                Err(e) => {
                    tracing::warn!(error = %e, "promoter join error");
                    Ok(())
                }
            },
            None => Ok(()),
        };

        drain_workers(
            workers,
            std::time::Duration::from_secs(self.cfg.shutdown_deadline_secs),
        )
        .await;

        if let Err(e) = ack_handle.await {
            tracing::warn!(error = %e, "ack flusher join error");
        }
        if let Err(e) = retry_handle.await {
            tracing::warn!(error = %e, "retry relocator join error");
        }
        if let Err(e) = dlq_handle.await {
            tracing::warn!(error = %e, "dlq relocator join error");
        }

        match (reader_outcome, promoter_outcome) {
            (Err(e), _) => Err(e),
            (Ok(()), Err(e)) => Err(e),
            (Ok(()), Ok(())) => Ok(()),
        }
    }

    fn spawn_promoter(
        &self,
        shutdown: CancellationToken,
    ) -> Option<tokio::task::JoinHandle<Result<()>>> {
        if !self.cfg.delayed_enabled {
            return None;
        }
        let promoter_cfg = PromoterConfig {
            queue_name: self.cfg.queue_name.clone(),
            poll_interval_ms: self.cfg.delayed_poll_interval_ms,
            promote_batch: self.cfg.delayed_promote_batch,
            max_stream_len: self.cfg.delayed_max_stream_len,
            lock_ttl_secs: self.cfg.delayed_lock_ttl_secs,
            holder_id: self.cfg.consumer_id.clone(),
        };
        let promoter = Promoter::new(self.redis_url.clone(), promoter_cfg);
        Some(tokio::spawn(promoter.run(shutdown)))
    }
}
