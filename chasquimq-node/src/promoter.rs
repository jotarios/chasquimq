//! `NativePromoter` — N-API wrapper over `chasquimq::Promoter`.
//!
//! Standalone delayed-job promoter, used when the consumer is configured
//! with `delayedEnabled: false` and the deployment runs the promoter as a
//! separate process (or sidecar). Same `run` / `shutdown` shape as
//! `NativeConsumer`.

use crate::producer::map_engine_err;
use chasquimq::Promoter;
use chasquimq::config::PromoterConfig;
use napi_derive::napi;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[napi(object)]
pub struct NativePromoterOpts {
    pub queue_name: Option<String>,
    pub poll_interval_ms: Option<i64>,
    pub promote_batch: Option<u32>,
    pub max_stream_len: Option<i64>,
    pub lock_ttl_secs: Option<i64>,
    pub holder_id: Option<String>,
}

#[napi]
pub struct NativePromoter {
    redis_url: String,
    cfg: PromoterConfig,
    shutdown: Arc<CancellationToken>,
}

#[napi]
impl NativePromoter {
    #[napi(constructor)]
    pub fn new(redis_url: String, opts: Option<NativePromoterOpts>) -> napi::Result<Self> {
        let cfg = build_promoter_config(opts);
        Ok(Self {
            redis_url,
            cfg,
            shutdown: Arc::new(CancellationToken::new()),
        })
    }

    /// Run the promoter loop until `shutdown()` is called.
    #[napi]
    pub async fn run(&self) -> napi::Result<()> {
        let promoter = Promoter::new(self.redis_url.clone(), self.cfg.clone());
        let shutdown = (*self.shutdown).clone();
        promoter.run(shutdown).await.map_err(map_engine_err)
    }

    /// Signal graceful shutdown. Idempotent.
    #[napi]
    pub fn shutdown(&self) -> napi::Result<()> {
        self.shutdown.cancel();
        Ok(())
    }
}

fn build_promoter_config(opts: Option<NativePromoterOpts>) -> PromoterConfig {
    let mut cfg = PromoterConfig::default();
    if let Some(o) = opts {
        if let Some(v) = o.queue_name {
            cfg.queue_name = v;
        }
        if let Some(v) = o.poll_interval_ms {
            if v >= 0 {
                cfg.poll_interval_ms = v as u64;
            }
        }
        if let Some(v) = o.promote_batch {
            cfg.promote_batch = v as usize;
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
