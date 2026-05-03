//! `NativeScheduler` — N-API wrapper over `chasquimq::Scheduler<RawBytes>`.
//!
//! Standalone repeatable-job scheduler. Pinned to `RawBytes` so the binding
//! stays schema-agnostic — the JS shim msgpack-encodes user payloads at
//! `Queue.add({repeat})` time, and the scheduler hands those exact bytes
//! through to the stream / delayed ZSET on every fire (the engine's
//! `Job<RawBytes>` envelope wraps them with a fresh ULID + attempt=0 each
//! tick).
//!
//! Same `run` / `shutdown` shape as [`NativePromoter`]. The high-level
//! `Worker` shim auto-spawns one alongside the consumer so user code that
//! calls `Queue.add(name, data, { repeat: { ... } })` and runs a `Worker`
//! gets cron / interval fires without a separate scheduler process.

use crate::payload::RawBytes;
use crate::producer::map_engine_err;
use chasquimq::Scheduler;
use chasquimq::config::SchedulerConfig;
use napi_derive::napi;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[napi(object)]
pub struct NativeSchedulerOpts {
    pub queue_name: Option<String>,
    pub tick_interval_ms: Option<i64>,
    pub batch: Option<u32>,
    pub max_stream_len: Option<i64>,
    pub lock_ttl_secs: Option<i64>,
    pub holder_id: Option<String>,
}

#[napi]
pub struct NativeScheduler {
    redis_url: String,
    cfg: SchedulerConfig,
    shutdown: Arc<CancellationToken>,
}

#[napi]
impl NativeScheduler {
    #[napi(constructor)]
    pub fn new(redis_url: String, opts: Option<NativeSchedulerOpts>) -> napi::Result<Self> {
        let cfg = build_scheduler_config(opts);
        Ok(Self {
            redis_url,
            cfg,
            shutdown: Arc::new(CancellationToken::new()),
        })
    }

    /// Run the scheduler loop until `shutdown()` is called.
    #[napi]
    pub async fn run(&self) -> napi::Result<()> {
        let scheduler = Scheduler::<RawBytes>::new(self.redis_url.clone(), self.cfg.clone());
        let shutdown = (*self.shutdown).clone();
        scheduler.run(shutdown).await.map_err(map_engine_err)
    }

    /// Signal graceful shutdown. Idempotent.
    #[napi]
    pub fn shutdown(&self) -> napi::Result<()> {
        self.shutdown.cancel();
        Ok(())
    }
}

fn build_scheduler_config(opts: Option<NativeSchedulerOpts>) -> SchedulerConfig {
    let mut cfg = SchedulerConfig::default();
    if let Some(o) = opts {
        if let Some(v) = o.queue_name {
            cfg.queue_name = v;
        }
        if let Some(v) = o.tick_interval_ms {
            if v >= 0 {
                cfg.tick_interval_ms = v as u64;
            }
        }
        if let Some(v) = o.batch {
            cfg.batch = v as usize;
        }
        if let Some(v) = o.max_stream_len {
            if v >= 0 {
                cfg.max_stream_len = v as u64;
            }
        }
        if let Some(v) = o.lock_ttl_secs {
            if v >= 0 {
                cfg.lock_ttl_secs = v as u64;
            }
        }
        if let Some(v) = o.holder_id {
            cfg.holder_id = v;
        }
    }
    cfg
}
