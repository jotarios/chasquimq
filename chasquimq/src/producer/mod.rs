mod dlq;

use crate::config::ProducerConfig;
use crate::error::{Error, Result};
use crate::job::{Job, JobId, now_ms};
use crate::redis::commands::{
    CANCEL_DELAYED_SCRIPT, SCHEDULE_DELAYED_IDEMPOTENT_SCRIPT, eval_cancel_delayed_args,
    eval_schedule_delayed_idempotent_args, evalsha_cancel_delayed_args,
    evalsha_schedule_delayed_idempotent_args, script_load_args, xadd_args, zadd_delayed_args,
};
use crate::redis::conn::connect_pool;
use bytes::Bytes;
use fred::clients::{Client, Pool};
use fred::interfaces::ClientLike;
use fred::types::{ClusterHash, CustomCommand, Value};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub use crate::redis::keys::{
    dedup_marker_key, delayed_index_key, delayed_key, dlq_key, promoter_lock_key, stream_key,
};

#[derive(Debug, Clone)]
pub struct DlqEntry {
    pub dlq_id: String,
    pub source_id: String,
    pub reason: String,
    pub detail: Option<String>,
    pub payload: Bytes,
}

pub struct Producer<T> {
    pool: Pool,
    producer_id: Arc<str>,
    queue_name: Arc<str>,
    stream_key: Arc<str>,
    delayed_key: Arc<str>,
    dlq_key: Arc<str>,
    max_stream_len: u64,
    max_delay_secs: u64,
    _marker: PhantomData<fn(T)>,
}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            producer_id: self.producer_id.clone(),
            queue_name: self.queue_name.clone(),
            stream_key: self.stream_key.clone(),
            delayed_key: self.delayed_key.clone(),
            dlq_key: self.dlq_key.clone(),
            max_stream_len: self.max_stream_len,
            max_delay_secs: self.max_delay_secs,
            _marker: PhantomData,
        }
    }
}

impl<T: Serialize> Producer<T> {
    pub async fn connect(redis_url: &str, config: ProducerConfig) -> Result<Self> {
        let pool = connect_pool(redis_url, config.pool_size).await?;
        let producer_id: Arc<str> = Arc::from(uuid::Uuid::new_v4().to_string());
        let queue_name: Arc<str> = Arc::from(config.queue_name.as_str());
        let stream_key: Arc<str> = Arc::from(stream_key(&config.queue_name));
        let delayed_key: Arc<str> = Arc::from(delayed_key(&config.queue_name));
        let dlq_key: Arc<str> = Arc::from(dlq_key(&config.queue_name));
        Ok(Self {
            pool,
            producer_id,
            queue_name,
            stream_key,
            delayed_key,
            dlq_key,
            max_stream_len: config.max_stream_len,
            max_delay_secs: config.max_delay_secs,
            _marker: PhantomData,
        })
    }

    pub fn producer_id(&self) -> &str {
        &self.producer_id
    }

    pub fn stream_key(&self) -> &str {
        &self.stream_key
    }

    pub fn delayed_key(&self) -> &str {
        &self.delayed_key
    }

    pub fn dlq_key(&self) -> &str {
        &self.dlq_key
    }

    /// Read up to `limit` DLQ entries without removing them. The returned bytes
    /// are the raw encoded `Job<T>` payload exactly as it landed in the DLQ —
    /// no `attempt` reset, no decode.
    pub async fn peek_dlq(&self, limit: usize) -> Result<Vec<DlqEntry>> {
        let entries = dlq::xrange_dlq(&self.pool, self.dlq_key.as_ref(), limit).await?;
        Ok(entries.into_iter().map(dlq::parse_dlq_entry).collect())
    }

    /// Cancel a previously-scheduled delayed job by its [`JobId`].
    ///
    /// Returns `true` if the entry was atomically removed from the delayed
    /// ZSET, `false` if there was nothing to cancel — either the id was never
    /// scheduled, the side-index already expired, or the promoter has already
    /// moved the entry into the main stream (cancel-vs-promote race lost).
    /// In all three "false" cases the only safe assumption is "the job may
    /// already be running or already ran".
    ///
    /// Cancel is itself a single Lua round trip: `GET` the side-index for the
    /// exact ZSET member, `ZREM` it, then `DEL` both the side-index and the
    /// dedup marker so the same id can be rescheduled. Both schedule and
    /// promote paths are also Lua under the same `{chasqui:<queue>}` hash
    /// tag, so the three operations serialize at Redis — the
    /// (delivered, cancel=true) outcome is impossible.
    pub async fn cancel_delayed(&self, id: &JobId) -> Result<bool> {
        let index_key = delayed_index_key(self.queue_name.as_ref(), id);
        let marker_key = dedup_marker_key(self.queue_name.as_ref(), id);
        let client = self.pool.next_connected();
        let removed =
            run_cancel_delayed(client, self.delayed_key.as_ref(), &index_key, &marker_key).await?;
        Ok(removed == 1)
    }

    /// Pipelined bulk variant of [`Producer::cancel_delayed`]. Issues one
    /// `EVALSHA` per id on a single connection. The returned `Vec<bool>` is
    /// in the same order as `ids`. An empty input returns an empty vec
    /// without touching Redis.
    pub async fn cancel_delayed_bulk(&self, ids: &[JobId]) -> Result<Vec<bool>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let client = self.pool.next_connected();
        let sha = load_cancel_script(client).await?;
        let pipeline = client.pipeline();
        let evalsha_cmd = CustomCommand::new_static("EVALSHA", ClusterHash::FirstKey, false);
        for id in ids {
            let index_key = delayed_index_key(self.queue_name.as_ref(), id);
            let marker_key = dedup_marker_key(self.queue_name.as_ref(), id);
            let args = evalsha_cancel_delayed_args(
                &sha,
                self.delayed_key.as_ref(),
                &index_key,
                &marker_key,
            );
            let _: () = pipeline
                .custom(evalsha_cmd.clone(), args)
                .await
                .map_err(Error::Redis)?;
        }
        let results: std::result::Result<Vec<Value>, _> = pipeline.all().await;
        let values = match results {
            Ok(v) => v,
            Err(e) if format!("{e}").contains("NOSCRIPT") => {
                // Script flushed mid-pipeline; replay each call as EVAL.
                let pipeline = client.pipeline();
                let eval_cmd = CustomCommand::new_static("EVAL", ClusterHash::FirstKey, false);
                for id in ids {
                    let index_key = delayed_index_key(self.queue_name.as_ref(), id);
                    let marker_key = dedup_marker_key(self.queue_name.as_ref(), id);
                    let args = eval_cancel_delayed_args(
                        CANCEL_DELAYED_SCRIPT,
                        self.delayed_key.as_ref(),
                        &index_key,
                        &marker_key,
                    );
                    let _: () = pipeline
                        .custom(eval_cmd.clone(), args)
                        .await
                        .map_err(Error::Redis)?;
                }
                pipeline.all().await.map_err(Error::Redis)?
            }
            Err(e) => return Err(Error::Redis(e)),
        };
        Ok(values.iter().map(|v| parse_lua_int(v) == 1).collect())
    }
}

impl<T: Serialize + DeserializeOwned> Producer<T> {
    /// Move up to `limit` entries from the DLQ back into the main stream. Each
    /// entry's `attempt` counter is reset to 0 before re-XADDing, so the replayed
    /// job gets a full retry budget. Returns the number of entries replayed.
    pub async fn replay_dlq(&self, limit: usize) -> Result<usize> {
        dlq::replay::<T>(
            &self.pool,
            self.dlq_key.as_ref(),
            self.stream_key.as_ref(),
            self.max_stream_len,
            limit,
        )
        .await
    }
}

impl<T: Serialize> Producer<T> {
    pub async fn add(&self, payload: T) -> Result<JobId> {
        let job = Job::new(payload);
        let id = job.id.clone();
        let bytes = Bytes::from(rmp_serde::to_vec(&job)?);
        self.xadd(&id, bytes).await?;
        Ok(id)
    }

    pub async fn add_with_id(&self, id: JobId, payload: T) -> Result<JobId> {
        let job = Job::with_id(id.clone(), payload);
        let bytes = Bytes::from(rmp_serde::to_vec(&job)?);
        self.xadd(&id, bytes).await?;
        Ok(id)
    }

    pub async fn add_bulk(&self, payloads: Vec<T>) -> Result<Vec<JobId>> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        let mut encoded: Vec<(JobId, Bytes)> = Vec::with_capacity(payloads.len());
        for payload in payloads {
            let job = Job::new(payload);
            let id = job.id.clone();
            let bytes = Bytes::from(rmp_serde::to_vec(&job)?);
            encoded.push((id, bytes));
        }

        let client = self.pool.next_connected();
        let pipeline = client.pipeline();
        let cmd = CustomCommand::new_static("XADD", ClusterHash::FirstKey, false);
        for (iid, bytes) in &encoded {
            let args = xadd_args(
                self.stream_key.as_ref(),
                self.producer_id.as_ref(),
                iid,
                self.max_stream_len,
                bytes.clone(),
            );
            let _: () = pipeline
                .custom(cmd.clone(), args)
                .await
                .map_err(Error::Redis)?;
        }
        let _: Vec<Value> = pipeline.all().await.map_err(Error::Redis)?;
        Ok(encoded.into_iter().map(|(id, _)| id).collect())
    }

    /// Schedule a job to run after `delay`.
    ///
    /// At-least-once under caller-driven retry: each call generates a fresh job id, so a
    /// retry after a network failure can land a duplicate scheduled job. Callers needing
    /// idempotent scheduling should retry only after confirming the previous call did not
    /// reach Redis. (`Producer::add` has Redis-side IDMP dedup; the delayed path does not.)
    pub async fn add_in(&self, delay: Duration, payload: T) -> Result<JobId> {
        self.check_delay_secs(delay.as_secs())?;
        if delay.is_zero() {
            return self.add(payload).await;
        }
        let now = now_ms();
        let run_at_ms = now.saturating_add(delay.as_millis() as u64);
        if run_at_ms <= now {
            return self.add(payload).await;
        }
        self.zadd_delayed(payload, run_at_ms).await
    }

    /// Schedule a job to run at `run_at` (absolute time).
    ///
    /// Same at-least-once-under-retry caveat as [`Producer::add_in`].
    pub async fn add_at(&self, run_at: SystemTime, payload: T) -> Result<JobId> {
        let run_at_ms = match run_at.duration_since(UNIX_EPOCH) {
            Ok(d) => u128_to_u64_or_err(d.as_millis())?,
            Err(_) => 0,
        };
        let now = now_ms();
        if run_at_ms <= now {
            return self.add(payload).await;
        }
        let delay_secs = (run_at_ms - now) / 1000;
        self.check_delay_secs(delay_secs)?;
        self.zadd_delayed(payload, run_at_ms).await
    }

    /// Schedule a batch of jobs to all run after `delay`. Same at-least-once-under-retry
    /// caveat as [`Producer::add_in`].
    pub async fn add_in_bulk(&self, delay: Duration, payloads: Vec<T>) -> Result<Vec<JobId>> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        self.check_delay_secs(delay.as_secs())?;
        if delay.is_zero() {
            return self.add_bulk(payloads).await;
        }
        let now = now_ms();
        let run_at_ms = now.saturating_add(delay.as_millis() as u64);
        if run_at_ms <= now {
            return self.add_bulk(payloads).await;
        }
        self.zadd_delayed_bulk(payloads, run_at_ms).await
    }

    /// Idempotent variant of [`Producer::add_in`]: the caller supplies a stable
    /// [`JobId`] so a network-driven retry of the same logical schedule does
    /// **not** end up double-scheduled. Implementation is one Lua round trip
    /// per job: a `SET NX EX` dedup marker gates the `ZADD`. If the marker
    /// already exists (a previous call already reached Redis), the second
    /// call is a no-op and returns the same `JobId` without an error.
    ///
    /// The marker TTL is `seconds_until_run + DEDUP_MARKER_GRACE_SECS` so the
    /// marker outlives the scheduled fire time long enough that a delayed
    /// retry of the producer cannot race a successful promotion.
    pub async fn add_in_with_id(&self, id: JobId, delay: Duration, payload: T) -> Result<JobId> {
        self.check_delay_secs(delay.as_secs())?;
        if delay.is_zero() {
            return self.add_with_id(id, payload).await;
        }
        let now = now_ms();
        let run_at_ms = now.saturating_add(delay.as_millis() as u64);
        if run_at_ms <= now {
            return self.add_with_id(id, payload).await;
        }
        let delay_secs = delay.as_secs().max(1);
        self.schedule_delayed_idempotent(id, payload, run_at_ms, delay_secs)
            .await
    }

    /// Idempotent variant of [`Producer::add_at`]: see [`Producer::add_in_with_id`]
    /// for the dedup model. If `run_at` has already passed, falls back to the
    /// IDMP-protected immediate path via [`Producer::add_with_id`].
    pub async fn add_at_with_id(&self, id: JobId, run_at: SystemTime, payload: T) -> Result<JobId> {
        let run_at_ms = match run_at.duration_since(UNIX_EPOCH) {
            Ok(d) => u128_to_u64_or_err(d.as_millis())?,
            Err(_) => 0,
        };
        let now = now_ms();
        if run_at_ms <= now {
            return self.add_with_id(id, payload).await;
        }
        let delay_ms = run_at_ms - now;
        let delay_secs = (delay_ms / 1000).max(1);
        self.check_delay_secs(delay_secs)?;
        self.schedule_delayed_idempotent(id, payload, run_at_ms, delay_secs)
            .await
    }

    /// Idempotent bulk variant: each `(JobId, T)` is scheduled under its own
    /// dedup marker so a partial network failure followed by a caller retry
    /// only re-schedules the entries that didn't make it. Pipelines one Lua
    /// `EVALSHA` per pair on a single connection.
    ///
    /// All entries share the same `delay`. Returns the supplied `JobId`s in
    /// the same order; duplicates suppressed by the marker still appear in
    /// the returned list (no error, no per-entry delivery report — peek the
    /// queue if you need that).
    pub async fn add_in_bulk_with_ids(
        &self,
        delay: Duration,
        items: Vec<(JobId, T)>,
    ) -> Result<Vec<JobId>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        self.check_delay_secs(delay.as_secs())?;
        if delay.is_zero() {
            return self.add_bulk_with_ids_immediate(items).await;
        }
        let now = now_ms();
        let run_at_ms = now.saturating_add(delay.as_millis() as u64);
        if run_at_ms <= now {
            return self.add_bulk_with_ids_immediate(items).await;
        }
        let delay_secs = delay.as_secs().max(1);
        self.schedule_delayed_idempotent_bulk(items, run_at_ms, delay_secs)
            .await
    }

    async fn add_bulk_with_ids_immediate(&self, items: Vec<(JobId, T)>) -> Result<Vec<JobId>> {
        // Encode all jobs first so a mid-batch encode failure leaves Redis
        // untouched (mirrors `add_bulk`'s atomic-encode posture).
        let mut encoded: Vec<(JobId, Bytes)> = Vec::with_capacity(items.len());
        for (id, payload) in items {
            let job = Job::with_id(id.clone(), payload);
            let bytes = Bytes::from(rmp_serde::to_vec(&job)?);
            encoded.push((id, bytes));
        }
        let client = self.pool.next_connected();
        let pipeline = client.pipeline();
        let cmd = CustomCommand::new_static("XADD", ClusterHash::FirstKey, false);
        for (iid, bytes) in &encoded {
            let args = xadd_args(
                self.stream_key.as_ref(),
                self.producer_id.as_ref(),
                iid,
                self.max_stream_len,
                bytes.clone(),
            );
            let _: () = pipeline
                .custom(cmd.clone(), args)
                .await
                .map_err(Error::Redis)?;
        }
        let _: Vec<Value> = pipeline.all().await.map_err(Error::Redis)?;
        Ok(encoded.into_iter().map(|(id, _)| id).collect())
    }

    fn check_delay_secs(&self, delay_secs: u64) -> Result<()> {
        if self.max_delay_secs > 0 && delay_secs > self.max_delay_secs {
            return Err(Error::Config(format!(
                "delay {}s exceeds max_delay_secs {}s",
                delay_secs, self.max_delay_secs
            )));
        }
        Ok(())
    }

    async fn zadd_delayed(&self, payload: T, run_at_ms: u64) -> Result<JobId> {
        let job = Job::new(payload);
        let id = job.id.clone();
        let bytes = Bytes::from(rmp_serde::to_vec(&job)?);
        let client = self.pool.next_connected();
        let cmd = CustomCommand::new_static("ZADD", ClusterHash::FirstKey, false);
        let args = zadd_delayed_args(
            self.delayed_key.as_ref(),
            run_at_ms_as_i64(run_at_ms)?,
            bytes,
        );
        let _: Value = client.custom(cmd, args).await.map_err(Error::Redis)?;
        Ok(id)
    }

    async fn zadd_delayed_bulk(&self, payloads: Vec<T>, run_at_ms: u64) -> Result<Vec<JobId>> {
        let mut encoded: Vec<(JobId, Bytes)> = Vec::with_capacity(payloads.len());
        for payload in payloads {
            let job = Job::new(payload);
            let id = job.id.clone();
            let bytes = Bytes::from(rmp_serde::to_vec(&job)?);
            encoded.push((id, bytes));
        }

        let score = run_at_ms_as_i64(run_at_ms)?;
        let client = self.pool.next_connected();
        let pipeline = client.pipeline();
        let cmd = CustomCommand::new_static("ZADD", ClusterHash::FirstKey, false);
        for (_, bytes) in &encoded {
            let args = zadd_delayed_args(self.delayed_key.as_ref(), score, bytes.clone());
            let _: () = pipeline
                .custom(cmd.clone(), args)
                .await
                .map_err(Error::Redis)?;
        }
        let _: Vec<Value> = pipeline.all().await.map_err(Error::Redis)?;
        Ok(encoded.into_iter().map(|(id, _)| id).collect())
    }

    async fn schedule_delayed_idempotent(
        &self,
        id: JobId,
        payload: T,
        run_at_ms: u64,
        delay_secs: u64,
    ) -> Result<JobId> {
        let job = Job::with_id(id.clone(), payload);
        let bytes = Bytes::from(rmp_serde::to_vec(&job)?);
        let marker_key = dedup_marker_key(self.queue_name.as_ref(), &id);
        let index_key = delayed_index_key(self.queue_name.as_ref(), &id);
        let marker_ttl = delay_secs.saturating_add(DEDUP_MARKER_GRACE_SECS);
        let score = run_at_ms_as_i64(run_at_ms)?;
        let client = self.pool.next_connected();
        run_schedule_delayed_idempotent(
            client,
            &marker_key,
            self.delayed_key.as_ref(),
            &index_key,
            marker_ttl,
            score,
            bytes,
        )
        .await?;
        // Whether the script returned 1 (newly scheduled) or 0 (duplicate
        // suppressed), the caller's logical schedule is now in flight under
        // the supplied `id`. Returning the same id in both cases is what
        // makes this a true idempotent operation from the API surface.
        Ok(id)
    }

    async fn schedule_delayed_idempotent_bulk(
        &self,
        items: Vec<(JobId, T)>,
        run_at_ms: u64,
        delay_secs: u64,
    ) -> Result<Vec<JobId>> {
        // Encode all jobs first so a mid-batch encode failure leaves Redis
        // untouched.
        let mut encoded: Vec<(JobId, Bytes)> = Vec::with_capacity(items.len());
        for (id, payload) in items {
            let job = Job::with_id(id.clone(), payload);
            let bytes = Bytes::from(rmp_serde::to_vec(&job)?);
            encoded.push((id, bytes));
        }
        let marker_ttl = delay_secs.saturating_add(DEDUP_MARKER_GRACE_SECS);
        let score = run_at_ms_as_i64(run_at_ms)?;

        let client = self.pool.next_connected();
        let sha = load_idempotent_script(client).await?;

        let pipeline = client.pipeline();
        let evalsha_cmd = CustomCommand::new_static("EVALSHA", ClusterHash::FirstKey, false);
        for (id, bytes) in &encoded {
            let marker_key = dedup_marker_key(self.queue_name.as_ref(), id);
            let index_key = delayed_index_key(self.queue_name.as_ref(), id);
            let args = evalsha_schedule_delayed_idempotent_args(
                &sha,
                &marker_key,
                self.delayed_key.as_ref(),
                &index_key,
                marker_ttl,
                score,
                bytes.clone(),
            );
            let _: () = pipeline
                .custom(evalsha_cmd.clone(), args)
                .await
                .map_err(Error::Redis)?;
        }
        let results: std::result::Result<Vec<Value>, _> = pipeline.all().await;
        match results {
            Ok(_) => {}
            Err(e) if format!("{e}").contains("NOSCRIPT") => {
                // Script was flushed mid-pipeline. Replay each call as EVAL
                // (sends the body, no SHA cache needed). Still pipelined.
                let pipeline = client.pipeline();
                let eval_cmd = CustomCommand::new_static("EVAL", ClusterHash::FirstKey, false);
                for (id, bytes) in &encoded {
                    let marker_key = dedup_marker_key(self.queue_name.as_ref(), id);
                    let index_key = delayed_index_key(self.queue_name.as_ref(), id);
                    let args = eval_schedule_delayed_idempotent_args(
                        SCHEDULE_DELAYED_IDEMPOTENT_SCRIPT,
                        &marker_key,
                        self.delayed_key.as_ref(),
                        &index_key,
                        marker_ttl,
                        score,
                        bytes.clone(),
                    );
                    let _: () = pipeline
                        .custom(eval_cmd.clone(), args)
                        .await
                        .map_err(Error::Redis)?;
                }
                let _: Vec<Value> = pipeline.all().await.map_err(Error::Redis)?;
            }
            Err(e) => return Err(Error::Redis(e)),
        }
        Ok(encoded.into_iter().map(|(id, _)| id).collect())
    }

    async fn xadd(&self, iid: &str, bytes: Bytes) -> Result<()> {
        let client = self.pool.next_connected();
        let args = xadd_args(
            self.stream_key.as_ref(),
            self.producer_id.as_ref(),
            iid,
            self.max_stream_len,
            bytes,
        );
        let cmd = CustomCommand::new_static("XADD", ClusterHash::FirstKey, false);
        let _: Value = client.custom(cmd, args).await.map_err(Error::Redis)?;
        Ok(())
    }
}

fn run_at_ms_as_i64(ms: u64) -> Result<i64> {
    i64::try_from(ms).map_err(|_| Error::Config(format!("run_at_ms {ms} overflows i64")))
}

fn u128_to_u64_or_err(ms: u128) -> Result<u64> {
    u64::try_from(ms).map_err(|_| Error::Config(format!("run_at_ms {ms} overflows u64")))
}

/// Grace period (in seconds) added to the idempotent-schedule dedup marker's
/// TTL on top of the time-until-fire. A producer that retries its `add_in_with_id`
/// call this long after the original call would otherwise race a successful
/// promotion (marker gone, ZSET entry already promoted to the stream → second
/// call would re-schedule). One hour is generous: producer-side network retries
/// almost always finish in seconds, and the marker is just a tiny string key.
pub(crate) const DEDUP_MARKER_GRACE_SECS: u64 = 3600;

async fn load_idempotent_script(client: &Client) -> Result<String> {
    load_script_sha(client, SCHEDULE_DELAYED_IDEMPOTENT_SCRIPT).await
}

async fn load_cancel_script(client: &Client) -> Result<String> {
    load_script_sha(client, CANCEL_DELAYED_SCRIPT).await
}

async fn load_script_sha(client: &Client, body: &str) -> Result<String> {
    let cmd = CustomCommand::new_static("SCRIPT", ClusterHash::FirstKey, false);
    let res: Value = client
        .custom(cmd, script_load_args(body))
        .await
        .map_err(Error::Redis)?;
    match res {
        Value::String(s) => Ok(s.to_string()),
        Value::Bytes(b) => std::str::from_utf8(&b)
            .map(|s| s.to_string())
            .map_err(|_| Error::Config("SCRIPT LOAD returned non-utf8 sha".into())),
        other => Err(Error::Config(format!(
            "SCRIPT LOAD returned unexpected: {other:?}"
        ))),
    }
}

async fn run_cancel_delayed(
    client: &Client,
    delayed_key: &str,
    index_key: &str,
    marker_key: &str,
) -> Result<i64> {
    let sha = load_cancel_script(client).await?;
    let evalsha_cmd = CustomCommand::new_static("EVALSHA", ClusterHash::FirstKey, false);
    let args = evalsha_cancel_delayed_args(&sha, delayed_key, index_key, marker_key);
    let res: std::result::Result<Value, _> = client.custom(evalsha_cmd, args).await;
    let v = match res {
        Ok(v) => v,
        Err(e) if format!("{e}").contains("NOSCRIPT") => {
            let cmd = CustomCommand::new_static("EVAL", ClusterHash::FirstKey, false);
            let args =
                eval_cancel_delayed_args(CANCEL_DELAYED_SCRIPT, delayed_key, index_key, marker_key);
            client.custom(cmd, args).await.map_err(Error::Redis)?
        }
        Err(e) => return Err(Error::Redis(e)),
    };
    Ok(parse_lua_int(&v))
}

async fn run_schedule_delayed_idempotent(
    client: &Client,
    marker_key: &str,
    delayed_key: &str,
    index_key: &str,
    marker_ttl_secs: u64,
    run_at_ms: i64,
    bytes: Bytes,
) -> Result<i64> {
    let sha = load_idempotent_script(client).await?;
    let evalsha_cmd = CustomCommand::new_static("EVALSHA", ClusterHash::FirstKey, false);
    let args = evalsha_schedule_delayed_idempotent_args(
        &sha,
        marker_key,
        delayed_key,
        index_key,
        marker_ttl_secs,
        run_at_ms,
        bytes.clone(),
    );
    let res: std::result::Result<Value, _> = client.custom(evalsha_cmd, args).await;
    let v = match res {
        Ok(v) => v,
        Err(e) if format!("{e}").contains("NOSCRIPT") => {
            let cmd = CustomCommand::new_static("EVAL", ClusterHash::FirstKey, false);
            let args = eval_schedule_delayed_idempotent_args(
                SCHEDULE_DELAYED_IDEMPOTENT_SCRIPT,
                marker_key,
                delayed_key,
                index_key,
                marker_ttl_secs,
                run_at_ms,
                bytes,
            );
            client.custom(cmd, args).await.map_err(Error::Redis)?
        }
        Err(e) => return Err(Error::Redis(e)),
    };
    // Defensive across `Value::Integer` / `Value::String` / `Value::Bytes`,
    // matching how `RETRY_RESCHEDULE_SCRIPT`'s return value is parsed elsewhere
    // in the engine.
    Ok(parse_lua_int(&v))
}

/// Parse a Lua script's integer reply across the three `fred::types::Value`
/// shapes Redis can hand back depending on transport (RESP2 vs RESP3) and
/// fred decode path. The previous version of this function read only the
/// **first byte** of the bulk-string variants — for `b"1"` it returned
/// `49` (ASCII code), not `1`, silently turning every cancel-success
/// reply into "false" on transports that surface integers as bulk
/// strings. Defensive parse via `str::parse::<i64>` matches the pattern
/// already established in `consumer/retry.rs::script_returned_one`.
fn parse_lua_int(v: &Value) -> i64 {
    match v {
        Value::Integer(n) => *n,
        Value::String(s) => s.parse::<i64>().unwrap_or(0),
        Value::Bytes(b) => std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0),
        _ => 0,
    }
}

#[cfg(test)]
mod parse_lua_int_tests {
    use super::parse_lua_int;
    use fred::types::Value;

    #[test]
    fn integer_passthrough() {
        assert_eq!(parse_lua_int(&Value::Integer(0)), 0);
        assert_eq!(parse_lua_int(&Value::Integer(1)), 1);
        assert_eq!(parse_lua_int(&Value::Integer(42)), 42);
    }

    #[test]
    fn string_is_parsed_as_decimal_not_first_byte() {
        // Regression: prior impl returned ASCII code 49 for "1".
        assert_eq!(parse_lua_int(&Value::String("0".into())), 0);
        assert_eq!(parse_lua_int(&Value::String("1".into())), 1);
        assert_eq!(parse_lua_int(&Value::String("10".into())), 10);
    }

    #[test]
    fn bytes_is_parsed_as_decimal_not_first_byte() {
        assert_eq!(
            parse_lua_int(&Value::Bytes(bytes::Bytes::from_static(b"0"))),
            0
        );
        assert_eq!(
            parse_lua_int(&Value::Bytes(bytes::Bytes::from_static(b"1"))),
            1
        );
        assert_eq!(
            parse_lua_int(&Value::Bytes(bytes::Bytes::from_static(b"10"))),
            10
        );
    }

    #[test]
    fn non_numeric_returns_zero() {
        assert_eq!(parse_lua_int(&Value::String("oops".into())), 0);
        assert_eq!(
            parse_lua_int(&Value::Bytes(bytes::Bytes::from_static(b"oops"))),
            0
        );
        assert_eq!(parse_lua_int(&Value::Null), 0);
    }
}
