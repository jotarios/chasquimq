mod dlq;

use crate::config::ProducerConfig;
use crate::error::{Error, Result};
use crate::job::{Job, JobId, JobRetryOverride, now_ms};
use crate::redis::commands::{
    CANCEL_DELAYED_SCRIPT, REMOVE_REPEATABLE_SCRIPT, SCHEDULE_DELAYED_IDEMPOTENT_SCRIPT,
    UPSERT_REPEATABLE_SCRIPT, eval_cancel_delayed_args, eval_remove_repeatable_args,
    eval_schedule_delayed_idempotent_args, eval_upsert_repeatable_args,
    evalsha_cancel_delayed_args, evalsha_remove_repeatable_args,
    evalsha_schedule_delayed_idempotent_args, evalsha_upsert_repeatable_args, script_load_args,
    xadd_args, zadd_delayed_args,
};
use crate::redis::conn::connect_pool;
use crate::repeat::{RepeatableMeta, RepeatableSpec, StoredSpec, next_fire_after};
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
    dedup_marker_key, delayed_index_key, delayed_key, dlq_key, events_key, promoter_lock_key,
    repeat_key, repeat_spec_key, scheduler_lock_key, stream_key,
};

#[derive(Debug, Clone)]
pub struct DlqEntry {
    pub dlq_id: String,
    pub source_id: String,
    pub reason: String,
    pub detail: Option<String>,
    pub payload: Bytes,
    /// Dispatch name preserved verbatim from the source stream entry's
    /// `n` field at DLQ-route time. Empty when the original entry had no
    /// `n` (pre-name-on-wire producers, or the `add` / `add_bulk` legacy
    /// path), and empty for reader-side DLQ routes (malformed / oversize /
    /// decode-fail) where the entry was never decoded in the first place.
    /// `Producer::replay_dlq` re-emits this back onto the main stream's
    /// `n` field, so a replayed job lands on the consumer with `Job::name`
    /// intact.
    pub name: String,
}

/// Per-job options for the `*_with_options` family of producer methods.
///
/// Kept intentionally minimal — only knobs already supported by the engine
/// hot path. BullMQ-shaped surface fields (priority, progress, lifo, etc.)
/// belong in higher SDK layers, not here.
///
/// `id`: stable [`JobId`] for at-most-once / idempotent scheduling. Mirrors
/// `add_with_id` / `add_in_with_id` semantics. `None` → engine generates a
/// fresh ULID.
///
/// `retry`: per-job overrides of [`crate::config::ConsumerConfig::max_attempts`]
/// and [`crate::config::ConsumerConfig::retry`]. Carried in the encoded
/// `Job<T>` payload and honored by the consumer's retry / DLQ gates. `None`
/// → consumer falls back to its queue-wide retry config.
///
/// `name`: optional dispatch name carried as a separate `n` field on the
/// stream entry (UTF-8, validated `<= 256` bytes at the producer boundary).
/// Empty string is the default and means "no name supplied" — equivalent
/// on the wire to omitting the field entirely.
///
/// **Slice-4 gap**: `name` is honored by [`Producer::add_with_options`] /
/// [`Producer::add_bulk_with_options`] (immediate XADD path) and survives
/// DLQ peek + DLQ replay. It is **not** yet supported on the delayed path —
/// [`Producer::add_in_with_options`] and [`Producer::add_at_with_options`]
/// reject a non-empty `name` with [`Error::Config`] until slice 4 plumbs
/// it through the delayed-ZSET encoder. Automatic retry-via-delayed-ZSET
/// (the consumer's retry path) and repeatable-spec fires also drop the
/// name on re-emit; the consumer sees `Job::name = ""` after a handler
/// error reschedules the job. See [`Job::name`] for the canonical list of
/// preserve / drop sites.
#[derive(Default, Clone, Debug)]
pub struct AddOptions {
    pub id: Option<JobId>,
    pub retry: Option<JobRetryOverride>,
    pub name: String,
}

impl AddOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_id(mut self, id: JobId) -> Self {
        self.id = Some(id);
        self
    }

    pub fn with_retry(mut self, retry: JobRetryOverride) -> Self {
        self.retry = Some(retry);
        self
    }

    /// Set the dispatch [`Self::name`] on these options. See [`AddOptions`]
    /// for the slice-4 gap on delayed / retry-reschedule / repeatable-fire
    /// paths — a name set here survives the immediate-XADD path and DLQ
    /// peek / replay, but **not** the delayed path (rejected up-front),
    /// automatic retry-via-delayed-ZSET, or scheduler-fire.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }
}

/// Maximum byte length of a job name on the wire. UTF-8 string, `<= 256` bytes.
/// Pragmatic upper bound: handler-dispatch keys, queue topic strings, and
/// BullMQ-style job names empirically fit in well under 100 bytes; the cap
/// keeps the per-entry stream framing small (one-byte length prefix in
/// Redis Streams' RESP frame) and gives operators a hard ceiling on a
/// field that's tagged onto every event-stream emit and metric label.
pub(crate) const MAX_NAME_LEN: usize = 256;

fn validate_name(name: &str) -> Result<()> {
    if name.len() > MAX_NAME_LEN {
        return Err(Error::Config(format!(
            "job name exceeds {MAX_NAME_LEN}-byte cap (got {} bytes)",
            name.len()
        )));
    }
    Ok(())
}

/// Reject `AddOptions::name` on producer entry points whose downstream path
/// re-encodes the job onto the delayed ZSET (or otherwise re-XADDs without
/// re-attaching the source entry's `n` field). Slice 1 only wired `name`
/// through the immediate `XADD`; the delayed-ZSET encoder, the promoter's
/// `ZRANGEBYSCORE → XADD`, and the scheduler-fire path will all silently
/// drop a producer-supplied name today. Until slice 4 closes that, an
/// explicit error is loudly preferable to a silent drop — callers find out
/// at compile-test time, not in production where the worker sees `name = ""`.
fn reject_name_on_delayed_path(name: &str, method: &str) -> Result<()> {
    if !name.is_empty() {
        return Err(Error::Config(format!(
            "{method}: delayed scheduling does not yet preserve `name` (slice 4); \
             got name={name:?}"
        )));
    }
    Ok(())
}

pub struct Producer<T> {
    pool: Pool,
    producer_id: Arc<str>,
    queue_name: Arc<str>,
    stream_key: Arc<str>,
    delayed_key: Arc<str>,
    dlq_key: Arc<str>,
    repeat_key: Arc<str>,
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
            repeat_key: self.repeat_key.clone(),
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
        let repeat_key: Arc<str> = Arc::from(repeat_key(&config.queue_name));
        Ok(Self {
            pool,
            producer_id,
            queue_name,
            stream_key,
            delayed_key,
            dlq_key,
            repeat_key,
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

impl<T> Producer<T> {
    /// Remove a repeatable spec by key. Returns `true` if a spec was
    /// removed, `false` if no spec with that key existed.
    ///
    /// Atomically `ZREM`s from the repeat ZSET and `DEL`s the spec hash in
    /// a single Lua round trip — no half-removed state visible to a
    /// concurrent scheduler tick.
    pub async fn remove_repeatable(&self, spec_key: &str) -> Result<bool> {
        let spec_hash_key = repeat_spec_key(self.queue_name.as_ref(), spec_key);
        let client = self.pool.next_connected();
        let sha = load_remove_repeatable_script(client).await?;
        let evalsha_cmd = CustomCommand::new_static("EVALSHA", ClusterHash::FirstKey, false);
        let args = evalsha_remove_repeatable_args(
            &sha,
            self.repeat_key.as_ref(),
            &spec_hash_key,
            spec_key,
        );
        let res: std::result::Result<Value, _> = client.custom(evalsha_cmd, args).await;
        let v = match res {
            Ok(v) => v,
            Err(e) if format!("{e}").contains("NOSCRIPT") => {
                let cmd = CustomCommand::new_static("EVAL", ClusterHash::FirstKey, false);
                let args = eval_remove_repeatable_args(
                    REMOVE_REPEATABLE_SCRIPT,
                    self.repeat_key.as_ref(),
                    &spec_hash_key,
                    spec_key,
                );
                client.custom(cmd, args).await.map_err(Error::Redis)?
            }
            Err(e) => return Err(Error::Redis(e)),
        };
        Ok(parse_lua_int(&v) >= 1)
    }

    /// List repeatable specs ordered by next fire time, ascending. Returns
    /// up to `limit` entries; pass a generous limit for full inventory
    /// (the wire size is small — payloads are not included).
    ///
    /// Two round trips: one `ZRANGE WITHSCORES` and one pipelined batch of
    /// `HGET` per spec. The payload bytes are intentionally **not** sent
    /// back from the spec hash — see [`RepeatableMeta`] for the projection.
    pub async fn list_repeatable(&self, limit: usize) -> Result<Vec<RepeatableMeta>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let client = self.pool.next_connected();
        let zrange_cmd = CustomCommand::new_static("ZRANGE", ClusterHash::FirstKey, false);
        let zrange_args: Vec<Value> = vec![
            Value::from(self.repeat_key.as_ref()),
            Value::from(0_i64),
            Value::from((limit as i64).saturating_sub(1)),
            Value::from("WITHSCORES"),
        ];
        let zrange_res: Value = client
            .custom(zrange_cmd, zrange_args)
            .await
            .map_err(Error::Redis)?;
        let pairs = parse_zrange_with_scores(&zrange_res);
        if pairs.is_empty() {
            return Ok(Vec::new());
        }

        // Pipeline one HGET per spec on a single connection.
        let pipeline = client.pipeline();
        let hget_cmd = CustomCommand::new_static("HGET", ClusterHash::FirstKey, false);
        for (spec_key, _) in &pairs {
            let spec_hash_key = repeat_spec_key(self.queue_name.as_ref(), spec_key);
            let args: Vec<Value> = vec![Value::from(spec_hash_key), Value::from("spec")];
            let _: () = pipeline
                .custom(hget_cmd.clone(), args)
                .await
                .map_err(Error::Redis)?;
        }
        let results: Vec<Value> = pipeline.all().await.map_err(Error::Redis)?;

        let mut out: Vec<RepeatableMeta> = Vec::with_capacity(pairs.len());
        for ((spec_key, next_fire_ms), spec_value) in pairs.into_iter().zip(results) {
            let bytes = match value_as_bytes(&spec_value) {
                Some(b) => b,
                None => continue, // dangling ZSET entry, skip
            };
            let stored: StoredSpec = match rmp_serde::from_slice(&bytes) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(error = %e, spec_key = %spec_key, "list_repeatable: spec decode failed; skipping");
                    continue;
                }
            };
            out.push(RepeatableMeta {
                key: spec_key,
                job_name: stored.job_name,
                pattern: stored.pattern,
                next_fire_ms,
                limit: stored.limit,
                start_after_ms: stored.start_after_ms,
                end_before_ms: stored.end_before_ms,
            });
        }
        Ok(out)
    }
}

impl<T: Serialize> Producer<T> {
    /// Upsert a repeatable spec. Returns the resolved spec key (auto-derived
    /// from `<job_name>::<pattern_signature>` when the caller leaves
    /// `spec.key` empty — see [`crate::repeat`] for the format).
    ///
    /// Re-upserting with the same key overwrites the spec and re-anchors
    /// the next fire time at "now + interval" (for `every`) or the next
    /// matching cron tick. Idempotent under caller retry: the same spec
    /// upserted twice in a row leaves Redis in the same state as one call.
    pub async fn upsert_repeatable(&self, spec: RepeatableSpec<T>) -> Result<String> {
        let resolved_key = spec.resolved_key();
        let spec_hash_key = repeat_spec_key(self.queue_name.as_ref(), &resolved_key);
        let now = now_ms();
        let next_fire =
            next_fire_after(&spec.pattern, now, spec.start_after_ms)?.ok_or_else(|| {
                Error::Config(format!(
                    "repeat pattern has no future fires: {:?}",
                    spec.pattern
                ))
            })?;
        // Reject patterns whose first fire would already be past the
        // configured end-bound — that would land in Redis only to be
        // immediately reaped on the first scheduler tick.
        if let Some(end) = spec.end_before_ms {
            if next_fire > end {
                return Err(Error::Config(
                    "repeat spec end_before_ms is before the first fire".into(),
                ));
            }
        }

        let payload_bytes = rmp_serde::to_vec(&spec.payload)?;
        let stored = StoredSpec {
            key: resolved_key.clone(),
            job_name: spec.job_name.clone(),
            pattern: spec.pattern.clone(),
            payload: payload_bytes,
            limit: spec.limit,
            start_after_ms: spec.start_after_ms,
            end_before_ms: spec.end_before_ms,
            fired: 0,
            missed_fires: spec.missed_fires,
        };
        let stored_bytes = Bytes::from(rmp_serde::to_vec(&stored)?);
        let next_fire_i64 = run_at_ms_as_i64(next_fire)?;

        let client = self.pool.next_connected();
        let sha = load_upsert_repeatable_script(client).await?;
        let evalsha_cmd = CustomCommand::new_static("EVALSHA", ClusterHash::FirstKey, false);
        let args = evalsha_upsert_repeatable_args(
            &sha,
            self.repeat_key.as_ref(),
            &spec_hash_key,
            next_fire_i64,
            &resolved_key,
            stored_bytes.clone(),
        );
        let res: std::result::Result<Value, _> = client.custom(evalsha_cmd, args).await;
        match res {
            Ok(_) => {}
            Err(e) if format!("{e}").contains("NOSCRIPT") => {
                let cmd = CustomCommand::new_static("EVAL", ClusterHash::FirstKey, false);
                let args = eval_upsert_repeatable_args(
                    UPSERT_REPEATABLE_SCRIPT,
                    self.repeat_key.as_ref(),
                    &spec_hash_key,
                    next_fire_i64,
                    &resolved_key,
                    stored_bytes,
                );
                let _: Value = client.custom(cmd, args).await.map_err(Error::Redis)?;
            }
            Err(e) => return Err(Error::Redis(e)),
        }
        Ok(resolved_key)
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
        self.xadd(&id, bytes, "").await?;
        Ok(id)
    }

    pub async fn add_with_id(&self, id: JobId, payload: T) -> Result<JobId> {
        let job = Job::with_id(id.clone(), payload);
        let bytes = Bytes::from(rmp_serde::to_vec(&job)?);
        self.xadd(&id, bytes, "").await?;
        Ok(id)
    }

    /// Like [`Producer::add`], but accepts an [`AddOptions`] carrying an
    /// optional stable [`JobId`], optional per-job [`JobRetryOverride`],
    /// and an optional dispatch `name`. The retry override rides inside the
    /// encoded `Job<T>` payload; the name rides as a separate `n` field on
    /// the stream entry, capped at 256 bytes (rejected with [`Error::Config`]
    /// when oversize).
    ///
    /// **`name` survives** this immediate `XADD` path end-to-end — the
    /// consumer reads it back as [`Job::name`]. It also survives a DLQ
    /// route + replay round trip (see [`Producer::peek_dlq`] /
    /// [`Producer::replay_dlq`]).
    ///
    /// **`name` does NOT survive** an automatic retry-via-delayed-ZSET
    /// reschedule (consumer-side retry path re-encodes `Job<T>` and
    /// `ZADD`s it without the `n` field — the promoter's later XADD has no
    /// name to attach). After the first handler error, the consumer sees
    /// `Job::name = ""` on the retry attempt. Slice 4 closes this gap.
    /// See [`Job::name`] for the canonical list of preserve / drop sites.
    pub async fn add_with_options(&self, payload: T, opts: AddOptions) -> Result<JobId> {
        validate_name(&opts.name)?;
        let mut job = match opts.id {
            Some(id) => Job::with_id(id, payload),
            None => Job::new(payload),
        };
        if let Some(retry) = opts.retry {
            job.retry = Some(retry);
        }
        let id = job.id.clone();
        let bytes = Bytes::from(rmp_serde::to_vec(&job)?);
        self.xadd(&id, bytes, &opts.name).await?;
        Ok(id)
    }

    /// Bulk variant of [`Producer::add_with_options`]. All entries share
    /// one [`AddOptions`] instance — the same retry override **and** the
    /// same `name` are applied to every job in the batch. Use
    /// [`Producer::add_bulk_named`] when you need per-job names.
    ///
    /// Setting `opts.id` with multiple payloads is rejected with
    /// `Error::Config`; a single id can't be reused across `n` jobs and
    /// silently using it for the first job only would be a footgun. Use
    /// [`Producer::add_in_bulk_with_ids`] when you need per-job stable IDs.
    /// `opts.id` set with exactly one payload is fine — it's the same as
    /// [`Producer::add_with_options`] one call deep.
    ///
    /// **`name`** has the same preserve / drop profile as
    /// [`Producer::add_with_options`]: survives the immediate XADD →
    /// consumer path and DLQ peek / replay; dropped on automatic
    /// retry-via-delayed-ZSET and repeatable-spec scheduler-fire until
    /// slice 4. See [`Job::name`] for the canonical list.
    pub async fn add_bulk_with_options(
        &self,
        payloads: Vec<T>,
        opts: AddOptions,
    ) -> Result<Vec<JobId>> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        validate_name(&opts.name)?;
        if opts.id.is_some() && payloads.len() > 1 {
            return Err(Error::Config(
                "add_bulk_with_options: opts.id can only be set when payloads.len() == 1; \
                 use add_in_bulk_with_ids for per-job stable IDs"
                    .into(),
            ));
        }
        let mut encoded: Vec<(JobId, Bytes)> = Vec::with_capacity(payloads.len());
        let mut consumed_id = opts.id;
        for payload in payloads {
            let mut job = match consumed_id.take() {
                Some(id) => Job::with_id(id, payload),
                None => Job::new(payload),
            };
            if let Some(retry) = opts.retry {
                job.retry = Some(retry);
            }
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
                &opts.name,
            );
            let _: () = pipeline
                .custom(cmd.clone(), args)
                .await
                .map_err(Error::Redis)?;
        }
        let _: Vec<Value> = pipeline.all().await.map_err(Error::Redis)?;
        Ok(encoded.into_iter().map(|(id, _)| id).collect())
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
                "",
            );
            let _: () = pipeline
                .custom(cmd.clone(), args)
                .await
                .map_err(Error::Redis)?;
        }
        let _: Vec<Value> = pipeline.all().await.map_err(Error::Redis)?;
        Ok(encoded.into_iter().map(|(id, _)| id).collect())
    }

    /// Bulk variant that accepts a per-job `(name, payload)` pair so each
    /// entry can carry its own dispatch name. Each name is validated against
    /// the 256-byte cap before any XADD is issued; an oversize name in the
    /// batch fails the whole call atomically (no partial writes to Redis,
    /// matching `add_bulk`'s atomic-encode posture).
    ///
    /// **Preserve / drop profile** matches [`Producer::add_with_options`]:
    /// each per-entry `name` survives the immediate XADD → consumer path
    /// and DLQ peek / replay. It does **not** survive automatic retry-via-
    /// delayed-ZSET re-encode or repeatable-spec scheduler-fire (slice 4
    /// closes this). See [`Job::name`] for the canonical list of preserve /
    /// drop sites.
    pub async fn add_bulk_named(&self, items: Vec<(String, T)>) -> Result<Vec<JobId>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        for (name, _) in &items {
            validate_name(name)?;
        }
        let mut encoded: Vec<(JobId, Bytes, String)> = Vec::with_capacity(items.len());
        for (name, payload) in items {
            let job = Job::new(payload);
            let id = job.id.clone();
            let bytes = Bytes::from(rmp_serde::to_vec(&job)?);
            encoded.push((id, bytes, name));
        }

        let client = self.pool.next_connected();
        let pipeline = client.pipeline();
        let cmd = CustomCommand::new_static("XADD", ClusterHash::FirstKey, false);
        for (iid, bytes, name) in &encoded {
            let args = xadd_args(
                self.stream_key.as_ref(),
                self.producer_id.as_ref(),
                iid,
                self.max_stream_len,
                bytes.clone(),
                name,
            );
            let _: () = pipeline
                .custom(cmd.clone(), args)
                .await
                .map_err(Error::Redis)?;
        }
        let _: Vec<Value> = pipeline.all().await.map_err(Error::Redis)?;
        Ok(encoded.into_iter().map(|(id, _, _)| id).collect())
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

    /// Like [`Producer::add_in`], but accepts an [`AddOptions`] carrying an
    /// optional stable [`JobId`] and optional per-job [`JobRetryOverride`].
    ///
    /// When `opts.id` is set, this routes through the idempotent
    /// `SCHEDULE_DELAYED_IDEMPOTENT_SCRIPT` path (same dedup-marker semantics
    /// as [`Producer::add_in_with_id`]). When `opts.id` is `None`, this
    /// uses the at-least-once `ZADD` path. Either way, the per-job retry
    /// override rides inside the encoded `Job<T>`.
    ///
    /// **`opts.name` is currently rejected with [`Error::Config`] when set
    /// on the delayed path** (slice 1 only plumbed `n` through the immediate
    /// `XADD`). Slice 4 will close this gap by carrying the name through the
    /// delayed-ZSET re-encode and the promoter's `ZRANGEBYSCORE → XADD` path.
    /// Until then, naming a delayed job would silently drop the name on
    /// promotion — an explicit error is loudly preferable.
    pub async fn add_in_with_options(
        &self,
        delay: Duration,
        payload: T,
        opts: AddOptions,
    ) -> Result<JobId> {
        reject_name_on_delayed_path(&opts.name, "add_in_with_options")?;
        self.check_delay_secs(delay.as_secs())?;
        if delay.is_zero() {
            return self.add_with_options(payload, opts).await;
        }
        let now = now_ms();
        let run_at_ms = now.saturating_add(delay.as_millis() as u64);
        if run_at_ms <= now {
            return self.add_with_options(payload, opts).await;
        }
        match opts.id {
            Some(id) => {
                let delay_secs = delay.as_secs().max(1);
                self.schedule_delayed_idempotent_with_retry(
                    id, payload, run_at_ms, delay_secs, opts.retry,
                )
                .await
            }
            None => {
                self.zadd_delayed_with_retry(payload, run_at_ms, opts.retry)
                    .await
            }
        }
    }

    /// Like [`Producer::add_at`], but accepts an [`AddOptions`]. See
    /// [`Producer::add_in_with_options`] for the dedup / retry-carry model
    /// and the slice-4 `opts.name` rejection.
    pub async fn add_at_with_options(
        &self,
        run_at: SystemTime,
        payload: T,
        opts: AddOptions,
    ) -> Result<JobId> {
        reject_name_on_delayed_path(&opts.name, "add_at_with_options")?;
        let run_at_ms = match run_at.duration_since(UNIX_EPOCH) {
            Ok(d) => u128_to_u64_or_err(d.as_millis())?,
            Err(_) => 0,
        };
        let now = now_ms();
        if run_at_ms <= now {
            return self.add_with_options(payload, opts).await;
        }
        let delay_secs = ((run_at_ms - now) / 1000).max(1);
        self.check_delay_secs(delay_secs)?;
        match opts.id {
            Some(id) => {
                self.schedule_delayed_idempotent_with_retry(
                    id, payload, run_at_ms, delay_secs, opts.retry,
                )
                .await
            }
            None => {
                self.zadd_delayed_with_retry(payload, run_at_ms, opts.retry)
                    .await
            }
        }
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
                "",
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
        self.zadd_delayed_with_retry(payload, run_at_ms, None).await
    }

    async fn zadd_delayed_with_retry(
        &self,
        payload: T,
        run_at_ms: u64,
        retry: Option<JobRetryOverride>,
    ) -> Result<JobId> {
        let mut job = Job::new(payload);
        if retry.is_some() {
            job.retry = retry;
        }
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
        self.schedule_delayed_idempotent_with_retry(id, payload, run_at_ms, delay_secs, None)
            .await
    }

    async fn schedule_delayed_idempotent_with_retry(
        &self,
        id: JobId,
        payload: T,
        run_at_ms: u64,
        delay_secs: u64,
        retry: Option<JobRetryOverride>,
    ) -> Result<JobId> {
        let mut job = Job::with_id(id.clone(), payload);
        if retry.is_some() {
            job.retry = retry;
        }
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

    async fn xadd(&self, iid: &str, bytes: Bytes, name: &str) -> Result<()> {
        let client = self.pool.next_connected();
        let args = xadd_args(
            self.stream_key.as_ref(),
            self.producer_id.as_ref(),
            iid,
            self.max_stream_len,
            bytes,
            name,
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

async fn load_upsert_repeatable_script(client: &Client) -> Result<String> {
    load_script_sha(client, UPSERT_REPEATABLE_SCRIPT).await
}

async fn load_remove_repeatable_script(client: &Client) -> Result<String> {
    load_script_sha(client, REMOVE_REPEATABLE_SCRIPT).await
}

fn value_as_bytes(v: &Value) -> Option<Bytes> {
    match v {
        Value::Bytes(b) => Some(b.clone()),
        Value::String(s) => Some(Bytes::copy_from_slice(s.as_bytes())),
        _ => None,
    }
}

/// Parse `ZRANGE ... WITHSCORES` reply into `(member, score)` pairs. The
/// reply is a flat array `[m1, s1, m2, s2, ...]` where members are bulk
/// strings and scores are doubles serialized as bulk strings.
fn parse_zrange_with_scores(v: &Value) -> Vec<(String, u64)> {
    let items = match v {
        Value::Array(items) => items,
        _ => return Vec::new(),
    };
    let mut out: Vec<(String, u64)> = Vec::with_capacity(items.len() / 2);
    let mut iter = items.iter();
    while let (Some(m), Some(s)) = (iter.next(), iter.next()) {
        let member = match m {
            Value::String(s) => s.to_string(),
            Value::Bytes(b) => match std::str::from_utf8(b) {
                Ok(s) => s.to_string(),
                Err(_) => continue,
            },
            _ => continue,
        };
        let score: u64 = match s {
            Value::Double(d) => (*d).max(0.0) as u64,
            Value::Integer(n) => (*n).max(0) as u64,
            Value::String(s) => s
                .parse::<f64>()
                .ok()
                .map(|f| f.max(0.0) as u64)
                .unwrap_or(0),
            Value::Bytes(b) => std::str::from_utf8(b)
                .ok()
                .and_then(|s| s.parse::<f64>().ok())
                .map(|f| f.max(0.0) as u64)
                .unwrap_or(0),
            _ => 0,
        };
        out.push((member, score));
    }
    out
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

#[cfg(test)]
mod name_validation_tests {
    use super::{MAX_NAME_LEN, validate_name};
    use crate::error::Error;

    #[test]
    fn empty_name_is_valid() {
        validate_name("").expect("empty name accepted");
    }

    #[test]
    fn short_name_is_valid() {
        validate_name("send-email").expect("short name accepted");
    }

    #[test]
    fn name_at_cap_is_valid() {
        let s = "x".repeat(MAX_NAME_LEN);
        validate_name(&s).expect("name at cap accepted");
    }

    #[test]
    fn oversize_name_is_rejected_with_config_error() {
        let s = "x".repeat(MAX_NAME_LEN + 1);
        let err = validate_name(&s).expect_err("oversize name rejected");
        match err {
            Error::Config(msg) => {
                assert!(
                    msg.contains("256") || msg.contains(&format!("{MAX_NAME_LEN}")),
                    "error message must mention the cap: {msg}"
                );
                assert!(
                    msg.contains(&format!("{}", MAX_NAME_LEN + 1)),
                    "error message must mention the actual byte length: {msg}"
                );
            }
            other => panic!("expected Error::Config, got {other:?}"),
        }
    }
}
