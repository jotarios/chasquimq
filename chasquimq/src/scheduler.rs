//! Repeatable-job scheduler. Sibling of the [`crate::Promoter`].
//!
//! The scheduler tails the per-queue repeat ZSET
//! (`{chasqui:<queue>}:repeat`) and, for each spec whose `next_fire_ms`
//! has elapsed:
//!
//! 1. `HGET` the spec hash and decode it as [`crate::repeat::StoredSpec`].
//! 2. Compute (a) the **fire time** (the score we just popped), (b) the
//!    next fire time after the fire time, and (c) the encoded `Job<T>`
//!    bytes for this iteration (a fresh ULID is minted per fire so jobs
//!    are idempotency-tracked individually under XADD IDMP).
//! 3. Hand all of the above to `SCHEDULE_REPEATABLE_SCRIPT` which atomically
//!    XADDs (or ZADDs to the delayed ZSET if `fire_at_ms > now_ms`),
//!    increments the spec's `fired` counter, and either updates the spec's
//!    score (next fire) or removes the spec entirely (limit hit /
//!    end_before_ms passed / pattern exhausted).
//!
//! Leader-elected via the same `SET NX EX` pattern as the [`crate::Promoter`]
//! but on its own lock key (`{chasqui:<queue>}:scheduler:lock`), so a
//! deployment can run scheduler and promoter on disjoint replicas.

use crate::config::SchedulerConfig;
use crate::error::{Error, Result};
use crate::job::{Job, now_ms};
use crate::metrics::{LockOutcome, dispatch};
use crate::redis::commands::{
    ACQUIRE_LOCK_SCRIPT, RELEASE_LOCK_SCRIPT, SCHEDULE_REPEATABLE_SCRIPT, eval_acquire_lock_args,
    eval_release_lock_args, eval_schedule_repeatable_args, evalsha_acquire_lock_args,
    evalsha_schedule_repeatable_args, script_load_args,
};
use crate::redis::conn::connect;
use crate::redis::keys::{
    delayed_key, repeat_key, repeat_spec_key, scheduler_lock_key, stream_key,
};
#[cfg(test)]
use crate::repeat::RepeatPattern;
use crate::repeat::{MissedFiresPolicy, StoredSpec, first_future_fire, next_fire_after};
use bytes::Bytes;
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::types::{ClusterHash, CustomCommand, Value};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const TRANSIENT_BACKOFF_MS: [u64; 4] = [50, 100, 200, 400];

/// Job scheduler for repeatable specs.
///
/// Generic over the queue's payload type `T` so each fire emits a normal
/// `Job<T>` body that the consumer can decode without any envelope
/// gymnastics. Pin `T` to the same type the matching `Producer<T>` /
/// `Consumer<T>` use for the queue.
pub struct Scheduler<T> {
    redis_url: String,
    cfg: SchedulerConfig,
    stream_key: String,
    delayed_key: String,
    repeat_key: String,
    lock_key: String,
    _marker: PhantomData<fn(T) -> T>,
}

impl<T> Scheduler<T>
where
    T: Serialize + DeserializeOwned + Send + 'static,
{
    pub fn new(redis_url: impl Into<String>, cfg: SchedulerConfig) -> Self {
        let stream_key = stream_key(&cfg.queue_name);
        let delayed_key = delayed_key(&cfg.queue_name);
        let repeat_key = repeat_key(&cfg.queue_name);
        let lock_key = scheduler_lock_key(&cfg.queue_name);
        Self {
            redis_url: redis_url.into(),
            cfg,
            stream_key,
            delayed_key,
            repeat_key,
            lock_key,
            _marker: PhantomData,
        }
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<()> {
        let client = connect(&self.redis_url).await?;
        let mut schedule_sha = load_script(&client, SCHEDULE_REPEATABLE_SCRIPT).await?;
        let mut lock_sha = load_script(&client, ACQUIRE_LOCK_SCRIPT).await?;
        let outcome = self
            .loop_until_shutdown(&client, &mut schedule_sha, &mut lock_sha, &shutdown)
            .await;
        self.release_lock_best_effort(&client).await;
        outcome
    }

    async fn loop_until_shutdown(
        &self,
        client: &Client,
        schedule_sha: &mut String,
        lock_sha: &mut String,
        shutdown: &CancellationToken,
    ) -> Result<()> {
        let tick = Duration::from_millis(self.cfg.tick_interval_ms);
        let mut backoff_idx: usize = 0;
        let mut last_outcome: Option<LockOutcome> = None;

        loop {
            if shutdown.is_cancelled() {
                return Ok(());
            }

            let acquired = match self.acquire_lock(client, lock_sha).await {
                Ok(v) => v,
                Err(LockError::NoScript) => {
                    *lock_sha = load_script(client, ACQUIRE_LOCK_SCRIPT).await?;
                    self.acquire_lock_via_eval(client).await?
                }
                Err(LockError::Transient(e)) => {
                    match backoff_after(backoff_idx, &e, "acquire_lock", shutdown).await {
                        Some(next) => backoff_idx = next,
                        None => return Ok(()),
                    }
                    continue;
                }
                Err(LockError::Permanent(e)) => return Err(Error::Redis(e)),
            };

            let outcome = if acquired {
                LockOutcome::Acquired
            } else {
                LockOutcome::Held
            };
            if last_outcome != Some(outcome) {
                let sink = &*self.cfg.metrics;
                dispatch("scheduler_lock_outcome", || {
                    sink.promoter_lock_outcome(outcome)
                });
                last_outcome = Some(outcome);
            }

            if !acquired {
                if !sleep_or_shutdown(tick, shutdown).await {
                    return Ok(());
                }
                continue;
            }

            match self.tick_once(client, schedule_sha).await {
                Ok(processed) => {
                    backoff_idx = 0;
                    // Drain immediately if we hit the batch ceiling — there
                    // are likely more due specs piled up; otherwise wait.
                    if processed >= self.cfg.batch {
                        continue;
                    }
                    if !sleep_or_shutdown(tick, shutdown).await {
                        return Ok(());
                    }
                }
                Err(TickError::NoScript) => {
                    *schedule_sha = load_script(client, SCHEDULE_REPEATABLE_SCRIPT).await?;
                    // Loop back; next iteration will retry with a fresh sha.
                    continue;
                }
                Err(TickError::Transient(e)) => {
                    match backoff_after(backoff_idx, &e, "scheduler_tick", shutdown).await {
                        Some(next) => backoff_idx = next,
                        None => return Ok(()),
                    }
                }
                Err(TickError::Permanent(e)) => return Err(Error::Redis(e)),
                Err(TickError::Engine(e)) => return Err(e),
            }
        }
    }

    /// One scheduler tick: pop due spec keys, hydrate each one, and feed
    /// them to the schedule script. Returns the number of specs successfully
    /// processed (so the loop can decide whether to drain or sleep).
    async fn tick_once(
        &self,
        client: &Client,
        schedule_sha: &str,
    ) -> std::result::Result<usize, TickError> {
        let now = now_ms();
        let spec_keys = match self.zrangebyscore_due(client, now).await {
            Ok(v) => v,
            Err(e) if format!("{e}").contains("NOSCRIPT") => return Err(TickError::NoScript),
            Err(e) => {
                return Err(if is_transient(&e) {
                    TickError::Transient(e)
                } else {
                    TickError::Permanent(e)
                });
            }
        };

        if spec_keys.is_empty() {
            return Ok(0);
        }

        let mut processed = 0_usize;
        for spec_key in &spec_keys {
            match self.schedule_one(client, schedule_sha, spec_key, now).await {
                Ok(()) => processed += 1,
                // A NOSCRIPT mid-batch is rare (would have to coincide with
                // a `SCRIPT FLUSH` between specs); bubble out so the loop
                // reloads the sha.
                Err(TickError::NoScript) => return Err(TickError::NoScript),
                Err(other) => return Err(other),
            }
        }

        Ok(processed)
    }

    /// `ZRANGEBYSCORE repeat -inf <now> LIMIT 0 batch`.
    async fn zrangebyscore_due(
        &self,
        client: &Client,
        now_ms: u64,
    ) -> std::result::Result<Vec<String>, fred::error::Error> {
        let cmd = CustomCommand::new_static("ZRANGEBYSCORE", ClusterHash::FirstKey, false);
        let args: Vec<Value> = vec![
            Value::from(self.repeat_key.as_str()),
            Value::from("-inf"),
            Value::from(now_ms as i64),
            Value::from("LIMIT"),
            Value::from(0_i64),
            Value::from(self.cfg.batch as i64),
        ];
        let res: Value = client.custom(cmd, args).await?;
        Ok(value_as_string_vec(&res))
    }

    /// Hydrate one spec, pick the fire time off its current ZSET score,
    /// compute the next fire time, encode the fired job, and dispatch via
    /// the schedule script.
    async fn schedule_one(
        &self,
        client: &Client,
        schedule_sha: &str,
        spec_key: &str,
        now_ms: u64,
    ) -> std::result::Result<(), TickError> {
        let spec_hash_key = repeat_spec_key(&self.cfg.queue_name, spec_key);

        // Fetch the encoded spec and the current ZSET score in one shot via
        // a small pipeline. (Two round trips would also work — this is not
        // a hot path.)
        let spec_bytes = match self.hget_spec(client, &spec_hash_key).await {
            Ok(Some(b)) => b,
            Ok(None) => {
                // Spec hash missing but ZSET still references it — clear the
                // dangling ZSET entry so we don't loop forever on a half-
                // upserted spec.
                self.zrem_spec(client, spec_key).await.ok();
                return Ok(());
            }
            Err(e) => {
                return Err(if is_transient(&e) {
                    TickError::Transient(e)
                } else {
                    TickError::Permanent(e)
                });
            }
        };

        let stored: StoredSpec = match rmp_serde::from_slice(&spec_bytes) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, spec_key = spec_key, "scheduler: spec decode failed; dropping");
                self.zrem_spec(client, spec_key).await.ok();
                return Ok(());
            }
        };

        let fire_at_ms = match self.zscore_spec(client, spec_key).await {
            Ok(Some(s)) => s,
            Ok(None) => {
                // ZSET entry vanished between ZRANGEBYSCORE and ZSCORE
                // (another scheduler? cancel?) — skip without error.
                return Ok(());
            }
            Err(e) => {
                return Err(if is_transient(&e) {
                    TickError::Transient(e)
                } else {
                    TickError::Permanent(e)
                });
            }
        };

        // Decode the stored payload bytes back into `T` once. We'll mint a
        // fresh `Job<T>` per fire below (new ULID, fresh `created_at_ms`,
        // zeroed `attempt`) so each fire is independently traceable. Decode
        // here, not per fire — `T` is `DeserializeOwned` and rmp-serde
        // cloning a deserialized `T` per fire would force `T: Clone` on
        // every consumer.
        let payload_bytes = stored.payload.clone();

        // Decide the fire list and the next_fire_ms based on the catch-up
        // policy. The "fast path" (no catch-up) is when the natural next
        // fire after `fire_at_ms` is already strictly greater than `now_ms`
        // — i.e. there's exactly one window due (the on-time one). The
        // policy logic only kicks in when at least one *additional* window
        // would have elapsed.
        let cadence_next = match next_fire_after(&stored.pattern, fire_at_ms, stored.start_after_ms)
        {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, spec_key = spec_key, "scheduler: next_fire_after failed; removing spec");
                self.zrem_spec(client, spec_key).await.ok();
                return Ok(());
            }
        };

        let policy = stored.missed_fires;
        let mut fires: Vec<(i64, Bytes)> = Vec::new();
        let next_fire_ms: u64 = match cadence_next {
            // Fast path: natural cadence-next is already in the future, so
            // there are no missed windows. One on-time fire, advance by
            // cadence — original behavior, regardless of policy.
            Some(next) if next > now_ms => {
                let bytes = encode_fire::<T>(&payload_bytes, spec_key)?;
                fires.push((fire_at_ms as i64, bytes));
                next
            }
            // Catch-up path: cadence-next is also <= now_ms (or pattern is
            // exhausted). Dispatch on policy.
            cadence_next => self.build_catchup_fires::<T>(
                &stored,
                spec_key,
                fire_at_ms,
                now_ms,
                cadence_next,
                policy,
                &mut fires,
            )?,
        };

        let limit = stored.limit.unwrap_or(0);
        let end_before_ms = stored.end_before_ms.unwrap_or(0);

        let cmd = CustomCommand::new_static("EVALSHA", ClusterHash::FirstKey, false);
        let args = evalsha_schedule_repeatable_args(
            schedule_sha,
            &self.stream_key,
            &self.delayed_key,
            &self.repeat_key,
            &spec_hash_key,
            now_ms as i64,
            next_fire_ms as i64,
            self.cfg.max_stream_len,
            spec_key,
            limit,
            end_before_ms,
            &fires,
        );

        let res: std::result::Result<Value, _> = client.custom(cmd, args).await;
        match res {
            Ok(_) => Ok(()),
            Err(e) if format!("{e}").contains("NOSCRIPT") => {
                // Replay via EVAL so the tick still completes, then bubble
                // up so the outer loop reloads the sha for next round.
                let cmd = CustomCommand::new_static("EVAL", ClusterHash::FirstKey, false);
                let args = eval_schedule_repeatable_args(
                    SCHEDULE_REPEATABLE_SCRIPT,
                    &self.stream_key,
                    &self.delayed_key,
                    &self.repeat_key,
                    &spec_hash_key,
                    now_ms as i64,
                    next_fire_ms as i64,
                    self.cfg.max_stream_len,
                    spec_key,
                    limit,
                    end_before_ms,
                    &fires,
                );
                let _: Value = client
                    .custom(cmd, args)
                    .await
                    .map_err(TickError::Permanent)?;
                Err(TickError::NoScript)
            }
            Err(e) => Err(if is_transient(&e) {
                TickError::Transient(e)
            } else {
                TickError::Permanent(e)
            }),
        }
    }

    /// Build the fire list and resolve `next_fire_ms` for a spec whose
    /// cadence-next is *also* in the past (i.e. at least one window has
    /// been missed beyond the on-time fire). Dispatches on the spec's
    /// [`MissedFiresPolicy`].
    #[allow(clippy::too_many_arguments)]
    fn build_catchup_fires<U>(
        &self,
        stored: &StoredSpec,
        spec_key: &str,
        fire_at_ms: u64,
        now_ms: u64,
        cadence_next: Option<u64>,
        policy: MissedFiresPolicy,
        fires: &mut Vec<(i64, Bytes)>,
    ) -> std::result::Result<u64, TickError>
    where
        U: Serialize + DeserializeOwned,
    {
        let payload_bytes = &stored.payload;
        match policy {
            MissedFiresPolicy::Skip => {
                // No fires this tick. Just jump next_fire_ms past `now_ms`.
                Ok(self
                    .resolve_first_future(stored, fire_at_ms, now_ms, cadence_next)
                    .unwrap_or(0))
            }
            MissedFiresPolicy::FireOnce => {
                // Emit exactly one job representing the missed window(s),
                // dated at the original `fire_at_ms`. Then advance past
                // `now_ms` so we don't immediately reprocess on the next
                // tick.
                let bytes = encode_fire::<U>(payload_bytes, spec_key)?;
                fires.push((fire_at_ms as i64, bytes));
                Ok(self
                    .resolve_first_future(stored, fire_at_ms, now_ms, cadence_next)
                    .unwrap_or(0))
            }
            MissedFiresPolicy::FireAll { max_catchup } => {
                let mut at = fire_at_ms;
                let mut count: u32 = 0;
                let mut next_after = cadence_next;
                loop {
                    if at > now_ms {
                        break;
                    }
                    if count >= max_catchup {
                        tracing::warn!(
                            spec_key,
                            max_catchup,
                            "scheduler: FireAll cap reached; advancing past missed fires",
                        );
                        break;
                    }
                    let bytes = encode_fire::<U>(payload_bytes, spec_key)?;
                    fires.push((at as i64, bytes));
                    count += 1;
                    match next_after {
                        Some(n) if n > at => {
                            at = n;
                            next_after = match next_fire_after(
                                &stored.pattern,
                                at,
                                stored.start_after_ms,
                            ) {
                                Ok(v) => v,
                                Err(e) => {
                                    tracing::warn!(error = %e, spec_key, "scheduler: next_fire_after failed mid-catchup; stopping replay");
                                    break;
                                }
                            };
                        }
                        _ => {
                            // Pattern exhausted (cron with no future
                            // match) or didn't advance (defensive). Stop.
                            break;
                        }
                    }
                }
                // After breaking, decide next_fire_ms: prefer the cursor
                // we landed on if it's now in the future; otherwise walk
                // first_future_fire from the current cursor.
                if at > now_ms {
                    Ok(at)
                } else {
                    Ok(
                        first_future_fire(&stored.pattern, now_ms, at, stored.start_after_ms)
                            .unwrap_or(None)
                            .unwrap_or(0),
                    )
                }
            }
        }
    }

    /// Helper used by `Skip` / `FireOnce`: from the cadence_next we already
    /// computed, walk forward until strictly greater than `now_ms`. Returns
    /// `None` if the pattern has no future fire.
    fn resolve_first_future(
        &self,
        stored: &StoredSpec,
        fire_at_ms: u64,
        now_ms: u64,
        cadence_next: Option<u64>,
    ) -> Option<u64> {
        // If cadence_next is itself already > now_ms, take it. Otherwise
        // re-walk from fire_at_ms (covers the case where the natural next
        // fire after `fire_at_ms` is also <= now_ms — common for `every:N`
        // when the outage is long).
        if let Some(n) = cadence_next {
            if n > now_ms {
                return Some(n);
            }
        }
        first_future_fire(&stored.pattern, now_ms, fire_at_ms, stored.start_after_ms)
            .unwrap_or(None)
    }

    async fn hget_spec(
        &self,
        client: &Client,
        spec_hash_key: &str,
    ) -> std::result::Result<Option<Bytes>, fred::error::Error> {
        let cmd = CustomCommand::new_static("HGET", ClusterHash::FirstKey, false);
        let args: Vec<Value> = vec![Value::from(spec_hash_key), Value::from("spec")];
        let v: Value = client.custom(cmd, args).await?;
        Ok(match v {
            Value::Bytes(b) => Some(b),
            Value::String(s) => Some(Bytes::copy_from_slice(s.as_bytes())),
            Value::Null => None,
            _ => None,
        })
    }

    async fn zscore_spec(
        &self,
        client: &Client,
        spec_key: &str,
    ) -> std::result::Result<Option<u64>, fred::error::Error> {
        let cmd = CustomCommand::new_static("ZSCORE", ClusterHash::FirstKey, false);
        let args: Vec<Value> = vec![Value::from(self.repeat_key.as_str()), Value::from(spec_key)];
        let v: Value = client.custom(cmd, args).await?;
        Ok(match v {
            Value::Double(d) => Some(d.max(0.0) as u64),
            Value::Integer(n) => Some(n.max(0) as u64),
            Value::String(s) => s.parse::<f64>().ok().map(|f| f.max(0.0) as u64),
            Value::Bytes(b) => std::str::from_utf8(&b)
                .ok()
                .and_then(|s| s.parse::<f64>().ok())
                .map(|f| f.max(0.0) as u64),
            Value::Null => None,
            _ => None,
        })
    }

    async fn zrem_spec(
        &self,
        client: &Client,
        spec_key: &str,
    ) -> std::result::Result<(), fred::error::Error> {
        let cmd = CustomCommand::new_static("ZREM", ClusterHash::FirstKey, false);
        let args: Vec<Value> = vec![Value::from(self.repeat_key.as_str()), Value::from(spec_key)];
        let _: Value = client.custom(cmd, args).await?;
        Ok(())
    }

    async fn acquire_lock(
        &self,
        client: &Client,
        sha: &str,
    ) -> std::result::Result<bool, LockError> {
        let cmd = CustomCommand::new_static("EVALSHA", ClusterHash::FirstKey, false);
        let args = evalsha_acquire_lock_args(
            sha,
            &self.lock_key,
            &self.cfg.holder_id,
            self.cfg.lock_ttl_secs,
        );
        let res: std::result::Result<Value, fred::error::Error> = client.custom(cmd, args).await;
        match res {
            Ok(v) => Ok(value_as_bool(&v)),
            Err(e) => Err(classify_lock_error(e)),
        }
    }

    async fn acquire_lock_via_eval(&self, client: &Client) -> Result<bool> {
        let cmd = CustomCommand::new_static("EVAL", ClusterHash::FirstKey, false);
        let args = eval_acquire_lock_args(
            ACQUIRE_LOCK_SCRIPT,
            &self.lock_key,
            &self.cfg.holder_id,
            self.cfg.lock_ttl_secs,
        );
        let v: Value = client.custom(cmd, args).await.map_err(Error::Redis)?;
        Ok(value_as_bool(&v))
    }

    async fn release_lock_best_effort(&self, client: &Client) {
        let cmd = CustomCommand::new_static("EVAL", ClusterHash::FirstKey, false);
        let args = eval_release_lock_args(RELEASE_LOCK_SCRIPT, &self.lock_key, &self.cfg.holder_id);
        let _: std::result::Result<Value, _> = client.custom(cmd, args).await;
    }
}

/// Decode the stored payload bytes into `T`, mint a fresh `Job<T>` (new
/// ULID, fresh `created_at_ms`, zeroed `attempt`), and re-encode for the
/// wire. Each catch-up fire gets its own ULID so consumers see distinct
/// jobs.
///
/// The decode-then-re-encode round trip keeps the wire format identical to
/// what `Producer::add` emits, so consumers can use a plain `Consumer<T>`
/// regardless of whether jobs come from one-shot or repeatable producers.
fn encode_fire<U>(payload_bytes: &[u8], spec_key: &str) -> std::result::Result<Bytes, TickError>
where
    U: Serialize + DeserializeOwned,
{
    let payload: U = match rmp_serde::from_slice(payload_bytes) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, spec_key = spec_key, "scheduler: payload decode failed");
            return Err(TickError::Engine(Error::Decode(e)));
        }
    };
    let job_id = ulid::Ulid::new().to_string();
    let job = Job::with_id(job_id, payload);
    rmp_serde::to_vec(&job)
        .map(Bytes::from)
        .map_err(|e| TickError::Engine(Error::Encode(e)))
}

enum TickError {
    NoScript,
    Transient(fred::error::Error),
    Permanent(fred::error::Error),
    Engine(Error),
}

enum LockError {
    NoScript,
    Transient(fred::error::Error),
    Permanent(fred::error::Error),
}

fn classify_lock_error(err: fred::error::Error) -> LockError {
    if format!("{err}").contains("NOSCRIPT") {
        return LockError::NoScript;
    }
    if is_transient(&err) {
        LockError::Transient(err)
    } else {
        LockError::Permanent(err)
    }
}

fn is_transient(err: &fred::error::Error) -> bool {
    use fred::error::ErrorKind;
    matches!(
        err.kind(),
        ErrorKind::Timeout | ErrorKind::IO | ErrorKind::Canceled
    )
}

fn value_as_bool(v: &Value) -> bool {
    match v {
        Value::Integer(n) => *n != 0,
        _ => false,
    }
}

fn value_as_string_vec(v: &Value) -> Vec<String> {
    match v {
        Value::Array(items) => items
            .iter()
            .filter_map(|item| match item {
                Value::String(s) => Some(s.to_string()),
                Value::Bytes(b) => std::str::from_utf8(b).ok().map(|s| s.to_string()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

async fn load_script(client: &Client, body: &str) -> Result<String> {
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

async fn sleep_or_shutdown(d: Duration, shutdown: &CancellationToken) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(d) => true,
        _ = shutdown.cancelled() => false,
    }
}

async fn backoff_after(
    idx: usize,
    err: &fred::error::Error,
    op: &str,
    shutdown: &CancellationToken,
) -> Option<usize> {
    let wait_ms = TRANSIENT_BACKOFF_MS[idx.min(TRANSIENT_BACKOFF_MS.len() - 1)];
    tracing::warn!(error = %err, op = op, backoff_ms = wait_ms, "scheduler transient error");
    if !sleep_or_shutdown(Duration::from_millis(wait_ms), shutdown).await {
        return None;
    }
    Some(idx.saturating_add(1))
}

/// Test-visible: confirm that on `every` patterns we always advance the
/// next fire time by exactly the interval — keeping cadence even when the
/// scheduler tick runs late.
#[cfg(test)]
#[allow(dead_code)]
fn advance_for_test(pattern: &RepeatPattern, fire_at_ms: u64) -> u64 {
    next_fire_after(pattern, fire_at_ms, None)
        .unwrap()
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_advances_by_interval_from_fire_time() {
        let pat = RepeatPattern::Every { interval_ms: 100 };
        // Even if "now" is well past fire_at_ms (scheduler ran late), the
        // next fire is `fire_at_ms + interval_ms`, not `now + interval_ms`
        // — that's what keeps cadence.
        let next = advance_for_test(&pat, 1000);
        assert_eq!(next, 1100);
    }

    #[test]
    fn cron_invalid_expression_is_caught() {
        let pat = RepeatPattern::Cron {
            expression: "absolutely not a cron".into(),
            tz: None,
        };
        let res = next_fire_after(&pat, 0, None);
        assert!(res.is_err());
    }

    #[test]
    fn value_as_string_vec_handles_bytes_and_strings() {
        let v = Value::Array(vec![
            Value::String("a".into()),
            Value::Bytes(Bytes::from_static(b"b")),
        ]);
        let out = value_as_string_vec(&v);
        assert_eq!(out, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn value_as_string_vec_non_array_returns_empty() {
        assert!(value_as_string_vec(&Value::Null).is_empty());
        assert!(value_as_string_vec(&Value::Integer(7)).is_empty());
    }
}
