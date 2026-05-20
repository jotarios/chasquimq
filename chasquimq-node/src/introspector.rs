//! `Introspector` — thin N-API wrapper over `chasquimq::Introspector`.
//!
//! Exposes the engine's bounded-scan introspection API to JS. Mirrors the
//! shape of `chasquimq-py::Introspector` so the high-level `Queue` shim
//! can plug introspection methods in symmetrically across both runtimes.

use crate::credential_provider::{CredentialProviderTsfn, build_js_credential_provider};
use crate::producer::map_engine_err;
use chasquimq::{ConnectionTuning, Introspector as EngineIntrospector, JobState as EngineJobState};
use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::sync::Arc;

/// Construction options for [`Introspector::connect`]. Mirrors
/// `ProducerOpts` where overlapping fields exist.
#[napi(object)]
pub struct IntrospectorOpts {
    pub queue_name: String,
    /// Consumer group whose PEL the inspector reads for "active" state.
    /// Defaults to `"default"` (the engine's `ConsumerConfig::group`
    /// default). Match whatever the workers run under.
    pub consumer_group: Option<String>,
    /// Cap on fred's exponential reconnect attempts. `0` (the engine
    /// default) = retry forever.
    pub reconnect_max_attempts: Option<u32>,
}

/// Aggregate per-queue job counts. Mirrors
/// `chasquimq::introspect::JobCounts` 1:1.
#[napi(object)]
pub struct JobCounts {
    pub waiting: u32,
    pub active: u32,
    pub delayed: u32,
    pub completed: u32,
    pub failed: u32,
    /// `0` (not paused) or `1` (durably paused). BullMQ-shaped count
    /// column so this dict drops straight into the high-level shim's
    /// `getJobCounts` return.
    pub paused: u32,
    /// `true` when the SCAN over `result:*` keys hit the per-call cap
    /// (`CHASQUIMQ_COMPLETED_SCAN_CAP`, default 10_000) without
    /// exhausting the keyspace.
    pub completed_is_capped: bool,
}

/// Per-job snapshot. Mirrors `chasquimq::introspect::JobInfo`. `payload`
/// is the opaque msgpack-encoded user data — the high-level shim decodes
/// it with `@msgpack/msgpack`.
#[napi(object)]
pub struct JobInfo {
    pub id: String,
    pub name: String,
    pub payload: Buffer,
    pub attempt: u32,
    /// One of `"waiting" | "active" | "delayed" | "completed" | "failed" | "unknown"`.
    pub state: String,
    pub created_at_ms: f64,
    pub processed_on_ms: Option<f64>,
    pub finished_on_ms: Option<f64>,
    pub failure_reason: Option<String>,
    pub failure_detail: Option<String>,
    pub decode_failed: bool,
}

/// One page of [`Introspector::getJobs`] results.
#[napi(object)]
pub struct JobsPage {
    pub jobs: Vec<JobInfo>,
    pub next_cursor: Option<String>,
}

#[napi]
pub struct Introspector {
    inner: Arc<EngineIntrospector>,
}

#[napi]
impl Introspector {
    /// Connect against `redisUrl` and bind to a (queue, consumerGroup)
    /// pair. The native pool is small (2 connections) — introspection is
    /// bursty, not sustained, and we don't want it stealing producer
    /// connections.
    #[napi(
        factory,
        ts_args_type = "redisUrl: string, opts: IntrospectorOpts, credentialProvider?: ((host: string | null) => Promise<CredentialResponseJs>) | undefined | null"
    )]
    pub async fn connect(
        redis_url: String,
        opts: IntrospectorOpts,
        credential_provider: Option<CredentialProviderTsfn>,
    ) -> napi::Result<Introspector> {
        let mut tuning = ConnectionTuning::default();
        if let Some(n) = opts.reconnect_max_attempts {
            tuning.reconnect_max_attempts = n;
        }
        if let Some(tsfn) = credential_provider {
            tuning.credential_provider = Some(build_js_credential_provider(tsfn));
        }
        let group = opts.consumer_group.as_deref();
        let inner = EngineIntrospector::connect(&redis_url, &opts.queue_name, &tuning, group)
            .await
            .map_err(map_engine_err)?;
        Ok(Introspector {
            inner: Arc::new(inner),
        })
    }

    #[napi]
    pub async fn shutdown(&self) -> napi::Result<()> {
        self.inner.shutdown().await.map_err(map_engine_err)
    }

    #[napi]
    pub async fn get_job_counts(&self) -> napi::Result<JobCounts> {
        let c = self.inner.get_job_counts().await.map_err(map_engine_err)?;
        Ok(JobCounts {
            waiting: clamp_u32(c.waiting),
            active: clamp_u32(c.active),
            delayed: clamp_u32(c.delayed),
            completed: clamp_u32(c.completed),
            failed: clamp_u32(c.failed),
            paused: clamp_u32(c.paused),
            completed_is_capped: c.completed_is_capped,
        })
    }

    #[napi]
    pub async fn get_job_state(&self, id: String) -> napi::Result<String> {
        let s = self
            .inner
            .get_job_state(&id)
            .await
            .map_err(map_engine_err)?;
        Ok(s.as_str().to_string())
    }

    #[napi]
    pub async fn get_job(&self, id: String) -> napi::Result<Option<JobInfo>> {
        let opt = self.inner.get_job(&id).await.map_err(map_engine_err)?;
        Ok(opt.map(engine_info_into_napi))
    }

    /// `state`: one of `"waiting" | "active" | "delayed" | "completed" | "failed"`.
    /// Pagination shape per state — see `chasquimq::Introspector::get_jobs`
    /// doc for the canonical semantics.
    #[napi]
    pub async fn get_jobs(
        &self,
        state: String,
        offset: Option<u32>,
        limit: Option<u32>,
        cursor: Option<String>,
    ) -> napi::Result<JobsPage> {
        let parsed = EngineJobState::parse(&state).ok_or_else(|| {
            napi::Error::from_reason(format!(
                "unknown state '{state}'; expected one of waiting | active | delayed | completed | failed"
            ))
        })?;
        let page = self
            .inner
            .get_jobs(
                parsed,
                offset.unwrap_or(0) as u64,
                limit.unwrap_or(100) as u64,
                cursor,
            )
            .await
            .map_err(map_engine_err)?;
        Ok(JobsPage {
            jobs: page.jobs.into_iter().map(engine_info_into_napi).collect(),
            next_cursor: page.next_cursor,
        })
    }
}

fn engine_info_into_napi(info: chasquimq::JobInfo) -> JobInfo {
    JobInfo {
        id: info.id,
        name: info.name,
        payload: Buffer::from(info.payload.to_vec()),
        attempt: info.attempt,
        state: info.state.as_str().to_string(),
        created_at_ms: info.created_at_ms as f64,
        processed_on_ms: info.processed_on_ms.map(|n| n as f64),
        finished_on_ms: info.finished_on_ms.map(|n| n as f64),
        failure_reason: info.failure_reason,
        failure_detail: info.failure_detail,
        decode_failed: info.decode_failed,
    }
}

fn clamp_u32(n: u64) -> u32 {
    n.min(u32::MAX as u64) as u32
}
