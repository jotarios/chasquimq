mod dlq;
mod pause;
mod reader;
mod retry;
mod worker;

pub use pause::PauseControl;

use crate::ack::{
    AckFlusherConfig, JobOk, OkResultWriterConfig, run_ack_flusher, run_ok_result_writer,
};
use crate::config::{ConsumerConfig, PromoterConfig, SchedulerConfig};
use crate::error::{HandlerError, Result};
use crate::events::EventsWriter;
use crate::job::Job;
use crate::promoter::Promoter;
use crate::redis::conn::connect;
use crate::redis::group::ensure_group;
use crate::redis::keys::{delayed_key, dlq_key, paused_key, stream_key};
use crate::redis::parse::StreamEntryId;
use crate::scheduler::Scheduler;
use bytes::Bytes;
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
    paused_key: String,
    pause: Arc<PauseControl>,
    _marker: PhantomData<fn() -> T>,
}

impl<T> Consumer<T>
where
    T: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    pub fn new(redis_url: impl Into<String>, cfg: ConsumerConfig) -> Self {
        Self::with_pause_control(redis_url, cfg, Arc::new(PauseControl::new()))
    }

    /// Like [`Consumer::new`] but with a caller-owned [`PauseControl`].
    ///
    /// The FFI bindings construct the `Arc<PauseControl>` at the wrapper's
    /// own `new()` (so `pause()` / `resume()` are callable before `run()`
    /// is awaited) and hand the same `Arc` to the engine here — exactly
    /// the pattern `run()` already uses for the external shutdown
    /// `CancellationToken`. Pure-Rust callers use [`Consumer::new`] +
    /// [`Consumer::pause_control`] instead.
    pub fn with_pause_control(
        redis_url: impl Into<String>,
        cfg: ConsumerConfig,
        pause: Arc<PauseControl>,
    ) -> Self {
        Self {
            redis_url: redis_url.into(),
            stream_key: stream_key(&cfg.queue_name),
            delayed_key: delayed_key(&cfg.queue_name),
            dlq_key: dlq_key(&cfg.queue_name),
            paused_key: paused_key(&cfg.queue_name),
            pause,
            cfg,
            _marker: PhantomData,
        }
    }

    /// Shared handle to this consumer's in-process pause switch. Clone it
    /// before `run()` (or from another task) to pause/resume the reader
    /// without tearing the consumer down. Mirrors the shutdown-token
    /// sharing pattern: the returned `Arc` stays valid for the consumer's
    /// lifetime and `pause()` / `resume()` are safe from any thread.
    ///
    /// This is the *process-local* control (`Worker.pause()`). The
    /// *cross-process* durable pause (`chasqui pause` / `Queue.pause()`)
    /// is the `{chasqui:<queue>}:paused` Redis key, which this same reader
    /// also honours independently.
    pub fn pause_control(&self) -> Arc<PauseControl> {
        self.pause.clone()
    }

    pub async fn run<H, Fut>(self, handler: H, shutdown: CancellationToken) -> Result<()>
    where
        H: Fn(Job<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<Bytes, HandlerError>> + Send + 'static,
    {
        tracing::debug!(
            queue = %self.cfg.queue_name,
            delayed_enabled = self.cfg.delayed_enabled,
            events_enabled = self.cfg.events_enabled,
            "consumer run entry"
        );
        let reader = connect(&self.redis_url, &self.cfg.connection).await?;
        let dlq_writer = connect(&self.redis_url, &self.cfg.connection).await?;
        let ack_client = connect(&self.redis_url, &self.cfg.connection).await?;
        let retry_client = connect(&self.redis_url, &self.cfg.connection).await?;

        ensure_group(&reader, &self.stream_key, &self.cfg.group).await?;

        // The events writer threads through all four hot-path subsystems
        // (reader / worker / retry-relocator / dlq-relocator) AND the
        // embedded promoter. When `events_enabled` is false we hand each one
        // a `Disabled` writer so every emit_* is a single branch + return
        // rather than an XADD.
        //
        // Wrapped in `Arc` so the embedded promoter can hold a refcounted
        // share without a second Redis connection. Hot-path subsystems get
        // a cheap `(*events).clone()` (the inner `EventsWriter` is itself
        // clone-by-Arc on its underlying fred `Client`), keeping their
        // `events: EventsWriter` field shape unchanged.
        let events = Arc::new(if self.cfg.events_enabled {
            let events_client = connect(&self.redis_url, &self.cfg.connection).await?;
            EventsWriter::new(
                events_client,
                &self.cfg.queue_name,
                self.cfg.events_max_stream_len,
            )
        } else {
            EventsWriter::disabled()
        });

        let concurrency = self.cfg.concurrency.max(1);
        let (job_tx, job_rx) = async_channel::bounded::<DispatchedJob<T>>(concurrency * 2);
        let (ack_tx, ack_rx) = mpsc::channel::<StreamEntryId>(concurrency * 4);
        let (dlq_tx, dlq_rx) = mpsc::channel(self.cfg.dlq_inflight.max(1));
        let (retry_tx, retry_rx) = mpsc::channel(self.cfg.retry_inflight.max(1));

        // Result-backend writer: opt-in. When `store_results` is false the
        // tx is `None` and the worker's match always takes the batched-ack
        // fast path — zero overhead for users who never call `get_result`.
        let (ok_result_tx, ok_result_handle) = if self.cfg.store_results {
            // Size so the writer always has ≥4 batches of headroom even
            // when batch >> concurrency (cloud-Redis with high
            // result_batch tuned to amortise per-RTT cost).
            let result_cap = (concurrency * 4).max(self.cfg.result_batch.max(1) * 4);
            let (tx, rx) = mpsc::channel::<JobOk>(result_cap);
            let ok_client = connect(&self.redis_url, &self.cfg.connection).await?;
            let handle = tokio::spawn(run_ok_result_writer(
                ok_client,
                OkResultWriterConfig {
                    stream_key: self.stream_key.clone(),
                    queue_name: self.cfg.queue_name.clone(),
                    group: self.cfg.group.clone(),
                    batch: self.cfg.result_batch.max(1),
                    idle: std::time::Duration::from_millis(self.cfg.result_idle_ms),
                },
                rx,
            ));
            (Some(tx), Some(handle))
        } else {
            (None, None)
        };

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
                metrics: self.cfg.metrics.clone(),
                events: (*events).clone(),
            },
            dlq_rx,
        ));

        let retry_handle = tokio::spawn(run_retry_relocator(
            retry_client,
            RetryRelocatorConfig {
                stream_key: self.stream_key.clone(),
                delayed_key: self.delayed_key.clone(),
                group: self.cfg.group.clone(),
                metrics: self.cfg.metrics.clone(),
                events: (*events).clone(),
            },
            retry_rx,
        ));

        let promoter_handle = self.spawn_promoter(shutdown.clone(), events.clone());
        let scheduler_handle = self.spawn_scheduler::<T>(shutdown.clone());

        let wiring = WorkerWiring {
            ack_tx: ack_tx.clone(),
            retry_tx: retry_tx.clone(),
            dlq_tx: dlq_tx.clone(),
            max_attempts: self.cfg.max_attempts,
            retry_cfg: self.cfg.retry.clone(),
            metrics: self.cfg.metrics.clone(),
            events: (*events).clone(),
            store_results: self.cfg.store_results,
            result_ttl_secs: self.cfg.result_ttl_secs,
            ok_result_tx: ok_result_tx.clone(),
        };
        let workers = spawn_workers(concurrency, handler, job_rx, wiring);
        drop(ack_tx);
        drop(retry_tx);
        drop(ok_result_tx);

        let read_state = ReadState {
            reader,
            stream_key: Arc::<str>::from(self.stream_key.clone()),
            paused_key: Arc::<str>::from(self.paused_key.clone()),
            cfg: self.cfg.clone(),
            job_tx,
            dlq_tx,
            shutdown: shutdown.clone(),
            pause_rx: self.pause.subscribe(),
            metrics: self.cfg.metrics.clone(),
            events: (*events).clone(),
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

        // Scheduler errors do not propagate up: a transient EVAL miss or a
        // misconfigured spec shouldn't bring down the consumer (which still
        // has the reader / workers / DLQ pipeline running). Log and move
        // on, mirroring how a panic in a single handler is contained.
        if let Some(h) = scheduler_handle {
            match h.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::error!(error = %e, "embedded scheduler stopped with error");
                }
                Err(e) => {
                    tracing::warn!(error = %e, "scheduler join error");
                }
            }
        }

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
        if let Some(h) = ok_result_handle
            && let Err(e) = h.await
        {
            tracing::warn!(error = %e, "ok-result writer join error");
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
        events: Arc<EventsWriter>,
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
            // The shared `Arc<EventsWriter>` carries the enable bit and the
            // stream-len cap already; these fields on `PromoterConfig` are
            // ignored on the `with_shared_events` path. They're still
            // forwarded here so the config struct stays well-formed for
            // anything that might inspect it (Debug, future probes).
            events_enabled: self.cfg.events_enabled,
            events_max_stream_len: self.cfg.events_max_stream_len,
            metrics: self.cfg.metrics.clone(),
            connection: self.cfg.connection.clone(),
        };
        // Hand the embedded promoter the same events writer the consumer's
        // hot-path subsystems use, so a worker process with
        // `events_enabled + delayed_enabled` opens one events connection
        // instead of two.
        let promoter = Promoter::with_shared_events(self.redis_url.clone(), promoter_cfg, events);
        tracing::debug!(queue = %self.cfg.queue_name, "consumer spawning embedded promoter");
        Some(tokio::spawn(promoter.run(shutdown)))
    }

    fn spawn_scheduler<U>(
        &self,
        shutdown: CancellationToken,
    ) -> Option<tokio::task::JoinHandle<Result<()>>>
    where
        U: Serialize + DeserializeOwned + Send + 'static,
    {
        if !self.cfg.run_scheduler {
            return None;
        }
        let scheduler_cfg = SchedulerConfig {
            queue_name: self.cfg.queue_name.clone(),
            metrics: self.cfg.metrics.clone(),
            connection: self.cfg.connection.clone(),
            ..self.cfg.scheduler.clone()
        };
        let scheduler = Scheduler::<U>::new(self.redis_url.clone(), scheduler_cfg);
        Some(tokio::spawn(scheduler.run(shutdown)))
    }
}
