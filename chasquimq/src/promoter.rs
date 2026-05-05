use crate::config::PromoterConfig;
use crate::error::{Error, Result};
use crate::events::EventsWriter;
use crate::job::Job;
use crate::metrics::{LockOutcome, PromoterTick, dispatch};
use crate::redis::commands::{
    ACQUIRE_LOCK_SCRIPT, PROMOTE_SCRIPT, RELEASE_LOCK_SCRIPT, eval_acquire_lock_args,
    eval_promote_args, eval_release_lock_args, evalsha_acquire_lock_args, evalsha_promote_args,
    script_load_args,
};
use crate::redis::conn::connect;
use crate::redis::keys::{delayed_index_key, delayed_key, promoter_lock_key, stream_key};
use bytes::Bytes;
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::types::{ClusterHash, CustomCommand, Value};
use serde::de::IgnoredAny;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const TRANSIENT_BACKOFF_MS: [u64; 4] = [50, 100, 200, 400];

pub struct Promoter {
    redis_url: String,
    cfg: PromoterConfig,
    stream_key: String,
    delayed_key: String,
    lock_key: String,
    /// Shared events writer handed in by the embedding `Consumer`. When
    /// `Some`, the promoter emits through this writer instead of opening a
    /// second Redis connection — collapsing the worker process from two
    /// events connections (one per role) to one. `None` for the standalone
    /// `Promoter::new` case, which keeps the original behavior of building
    /// its own writer based on `cfg.events_enabled`.
    shared_events: Option<Arc<EventsWriter>>,
}

impl Promoter {
    pub fn new(redis_url: impl Into<String>, cfg: PromoterConfig) -> Self {
        let stream_key = stream_key(&cfg.queue_name);
        let delayed_key = delayed_key(&cfg.queue_name);
        let lock_key = promoter_lock_key(&cfg.queue_name);
        Self {
            redis_url: redis_url.into(),
            cfg,
            stream_key,
            delayed_key,
            lock_key,
            shared_events: None,
        }
    }

    /// Construct a promoter that emits through the supplied `Arc<EventsWriter>`
    /// instead of opening its own Redis connection. Used by `Consumer::run`
    /// so the in-process consumer + embedded promoter share one events
    /// connection. The `cfg.events_enabled` / `cfg.events_max_stream_len`
    /// fields are ignored on this path — the shared writer carries that
    /// state already.
    pub(crate) fn with_shared_events(
        redis_url: impl Into<String>,
        cfg: PromoterConfig,
        events: Arc<EventsWriter>,
    ) -> Self {
        let stream_key = stream_key(&cfg.queue_name);
        let delayed_key = delayed_key(&cfg.queue_name);
        let lock_key = promoter_lock_key(&cfg.queue_name);
        Self {
            redis_url: redis_url.into(),
            cfg,
            stream_key,
            delayed_key,
            lock_key,
            shared_events: Some(events),
        }
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<()> {
        let client = connect(&self.redis_url).await?;
        // Three cases for the events writer:
        // 1. Shared writer supplied (embedded promoter): clone the `Arc`-shared
        //    writer; no extra Redis connection. This is the conn-share fast
        //    path.
        // 2. No shared writer, `events_enabled = true` (standalone promoter):
        //    open a separate connection so a slow XADD on the events stream
        //    cannot serialize with the promote-script EVAL on the main client.
        // 3. No shared writer, `events_enabled = false`: noop writer; no
        //    extra connection.
        let events = match &self.shared_events {
            Some(shared) => EventsWriter::clone(shared),
            None if self.cfg.events_enabled => {
                let events_client = connect(&self.redis_url).await?;
                EventsWriter::new(
                    events_client,
                    &self.cfg.queue_name,
                    self.cfg.events_max_stream_len,
                )
            }
            None => EventsWriter::disabled(),
        };
        let mut promote_sha = load_script(&client, PROMOTE_SCRIPT).await?;
        let mut lock_sha = load_script(&client, ACQUIRE_LOCK_SCRIPT).await?;
        let outcome = self
            .loop_until_shutdown(&client, &events, &mut promote_sha, &mut lock_sha, &shutdown)
            .await;
        self.release_lock_best_effort(&client).await;
        outcome
    }

    async fn loop_until_shutdown(
        &self,
        client: &Client,
        events: &EventsWriter,
        promote_sha: &mut String,
        lock_sha: &mut String,
        shutdown: &CancellationToken,
    ) -> Result<()> {
        let poll = Duration::from_millis(self.cfg.poll_interval_ms);
        let mut backoff_idx: usize = 0;
        // Track the previous lock outcome so we emit metrics on transitions
        // only — otherwise a non-leader replica would emit `Held` on every
        // poll forever, drowning the gauge.
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
                dispatch("promoter_lock_outcome", || {
                    sink.promoter_lock_outcome(outcome)
                });
                last_outcome = Some(outcome);
            }

            if !acquired {
                if !sleep_or_shutdown(poll, shutdown).await {
                    return Ok(());
                }
                continue;
            }

            match self.promote_once(client, promote_sha).await {
                Ok((tick, members)) => {
                    cleanup_promoted_indices(client, &self.cfg.queue_name, &members).await;
                    emit_waiting_for_promoted(events, &members).await;
                    match self
                        .handle_tick(tick, &mut backoff_idx, poll, shutdown)
                        .await
                    {
                        TickAction::Drain => continue,
                        TickAction::Shutdown => return Ok(()),
                        TickAction::Polled => {}
                    }
                }
                Err(PromoteError::NoScript) => {
                    *promote_sha = load_script(client, PROMOTE_SCRIPT).await?;
                    let (tick, members) =
                        run_promote_eval(client, &self.delayed_key, &self.stream_key, &self.cfg)
                            .await
                            .map_err(Error::Redis)?;
                    cleanup_promoted_indices(client, &self.cfg.queue_name, &members).await;
                    emit_waiting_for_promoted(events, &members).await;
                    match self
                        .handle_tick(tick, &mut backoff_idx, poll, shutdown)
                        .await
                    {
                        TickAction::Drain => continue,
                        TickAction::Shutdown => return Ok(()),
                        TickAction::Polled => {}
                    }
                }
                Err(PromoteError::Transient(e)) => {
                    match backoff_after(backoff_idx, &e, "promote", shutdown).await {
                        Some(next) => backoff_idx = next,
                        None => return Ok(()),
                    }
                }
                Err(PromoteError::Permanent(e)) => return Err(Error::Redis(e)),
            }
        }
    }

    /// Emit the tick metric, reset transient-error backoff, and decide what
    /// the loop should do next:
    /// - `Drain`: there are still due entries, iterate immediately without sleeping.
    /// - `Shutdown`: the shutdown token fired during the inter-tick sleep; exit cleanly.
    /// - `Polled`: a normal poll-interval has elapsed; iterate.
    ///
    /// Pulled out of the loop so the Ok and NoScript paths can't drift.
    async fn handle_tick(
        &self,
        tick: PromoterTick,
        backoff_idx: &mut usize,
        poll: Duration,
        shutdown: &CancellationToken,
    ) -> TickAction {
        let sink = &*self.cfg.metrics;
        dispatch("promoter_tick", || sink.promoter_tick(tick));
        *backoff_idx = 0;
        // Widen `promote_batch` to u64 instead of narrowing `tick.promoted`
        // to usize — on 32-bit targets the latter would silently truncate.
        if tick.promoted >= self.cfg.promote_batch as u64 {
            return TickAction::Drain;
        }
        if !sleep_or_shutdown(poll, shutdown).await {
            return TickAction::Shutdown;
        }
        TickAction::Polled
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

    async fn promote_once(
        &self,
        client: &Client,
        sha: &str,
    ) -> std::result::Result<(PromoterTick, Vec<Bytes>), PromoteError> {
        let cmd = CustomCommand::new_static("EVALSHA", ClusterHash::FirstKey, false);
        let args = evalsha_promote_args(
            sha,
            &self.delayed_key,
            &self.stream_key,
            self.cfg.promote_batch,
            self.cfg.max_stream_len,
        );
        let res: std::result::Result<Value, fred::error::Error> = client.custom(cmd, args).await;
        match res {
            Ok(v) => Ok((value_as_tick(&v), value_as_promoted_members(&v))),
            Err(e) => Err(classify_promote_error(e)),
        }
    }

    /// Best-effort: only deletes the lock if its current value still matches
    /// our holder_id. A paused promoter whose lease expired and was taken
    /// over by another holder must not delete the new holder's lock.
    async fn release_lock_best_effort(&self, client: &Client) {
        let cmd = CustomCommand::new_static("EVAL", ClusterHash::FirstKey, false);
        let args = eval_release_lock_args(RELEASE_LOCK_SCRIPT, &self.lock_key, &self.cfg.holder_id);
        let _: std::result::Result<Value, _> = client.custom(cmd, args).await;
    }
}

enum PromoteError {
    NoScript,
    Transient(fred::error::Error),
    Permanent(fred::error::Error),
}

/// Outcome of one `handle_tick`. The three variants mirror what the loop
/// must do next; using a named enum (instead of stdlib `ControlFlow` or a
/// bool) avoids confusion with `std::ops::ControlFlow` semantics.
enum TickAction {
    /// Promoted a full batch — likely more entries due. Iterate without sleeping.
    Drain,
    /// Shutdown token fired during the inter-tick sleep. Exit the loop cleanly.
    Shutdown,
    /// One full poll interval elapsed. Iterate normally.
    Polled,
}

enum LockError {
    NoScript,
    Transient(fred::error::Error),
    Permanent(fred::error::Error),
}

fn classify_promote_error(err: fred::error::Error) -> PromoteError {
    if format!("{err}").contains("NOSCRIPT") {
        return PromoteError::NoScript;
    }
    if is_transient(&err) {
        PromoteError::Transient(err)
    } else {
        PromoteError::Permanent(err)
    }
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

pub(crate) fn value_as_tick(v: &Value) -> PromoterTick {
    // PROMOTE_SCRIPT returns `{promoted, depth, oldest_pending_lag_ms,
    // promoted_members}` as a Lua table, which fred surfaces as
    // `Value::Array`. The 4th slot (promoted msgpack-only payload bytes —
    // the slice-3 length prefix is stripped server-side) is consumed by
    // `value_as_promoted_members` and intentionally ignored here so the
    // public `PromoterTick` stays a small `Copy` struct. Older deployments /
    // a future script regression returning a bare integer for `promoted` is
    // still accepted for forward-compatibility, with the rest zeroed.
    match v {
        Value::Array(items) => PromoterTick {
            promoted: items.first().map(value_as_u64).unwrap_or(0),
            depth: items.get(1).map(value_as_u64).unwrap_or(0),
            oldest_pending_lag_ms: items.get(2).map(value_as_u64).unwrap_or(0),
        },
        Value::Integer(n) => PromoterTick {
            promoted: (*n).max(0) as u64,
            depth: 0,
            oldest_pending_lag_ms: 0,
        },
        _ => PromoterTick {
            promoted: 0,
            depth: 0,
            oldest_pending_lag_ms: 0,
        },
    }
}

/// Pull the promoted-member byte strings (slot 4 of the script's reply) out
/// of the EVAL/EVALSHA result, for the side-index cleanup pass. Returns an
/// empty vec for the legacy bare-integer reply shape, for missing slot 4
/// (e.g. an older deployment of this script), or for any non-bytes element
/// — leaking a few `:didx:<id>` keys to TTL is preferable to crashing the
/// promoter on an unexpected reply shape.
pub(crate) fn value_as_promoted_members(v: &Value) -> Vec<Bytes> {
    let items = match v {
        Value::Array(items) => items,
        _ => return Vec::new(),
    };
    let raw = match items.get(3) {
        Some(Value::Array(raw)) => raw,
        _ => return Vec::new(),
    };
    let mut out = Vec::with_capacity(raw.len());
    for item in raw {
        match item {
            Value::Bytes(b) => out.push(b.clone()),
            Value::String(s) => out.push(Bytes::copy_from_slice(s.as_bytes())),
            _ => {}
        }
    }
    out
}

/// Decode a `JobId` from a msgpack-encoded `Job<T>` payload without
/// allocating for the (ignored) `payload` field. Returns `None` if the
/// bytes don't decode — a stale or malformed entry will simply skip the
/// side-index cleanup and let the key TTL out on its own.
fn extract_job_id(bytes: &[u8]) -> Option<String> {
    let job: Job<IgnoredAny> = rmp_serde::from_slice(bytes).ok()?;
    Some(job.id)
}

/// Pipeline `DEL :didx:<id>` for each promoted member, batched into one
/// round trip. The dedup marker `:dlid:<id>` is **deliberately not deleted**
/// here: its remaining TTL is the post-promote idempotence guard for slice
/// 6 (a delayed producer-retry of `add_in_with_id` that lands after
/// promotion must hit the marker and become a no-op rather than schedule
/// a second copy). The side-index, by contrast, is only useful to a future
/// `cancel_delayed` and is dead weight after promotion.
/// Emit one `waiting` event per promoted member to the per-queue events
/// stream. Best-effort: each XADD is awaited sequentially (the writer
/// shares a single connection — pipelining would buy us little here since
/// the promoter's hot path is already cap-bounded by `promote_batch`,
/// which defaults to 256). For batches large enough to make sequential
/// XADDs visible, an operator should raise `events_max_stream_len` and
/// drop the events sink, not pipeline.
///
/// A member whose payload bytes don't decode to a `Job<IgnoredAny>` is
/// silently skipped (matches the side-index cleanup posture: a stale or
/// malformed entry shouldn't take the promoter down).
async fn emit_waiting_for_promoted(events: &EventsWriter, members: &[Bytes]) {
    if !events.is_enabled() || members.is_empty() {
        return;
    }
    // Promoter knows only the encoded ZSET member bytes — `name` is not
    // preserved through the delayed path in v1 (slice 4 closes that gap),
    // so emit `waiting` with an empty `n`. Subscribers that need name on
    // the `waiting` transition can reconstruct it from the subsequent
    // `active` event for the same job id.
    for m in members {
        if let Some(id) = extract_job_id(m) {
            events.emit_waiting(&id, "").await;
        }
    }
}

async fn cleanup_promoted_indices(client: &Client, queue_name: &str, members: &[Bytes]) {
    if members.is_empty() {
        return;
    }
    let mut keys: Vec<Value> = Vec::with_capacity(members.len());
    for m in members {
        if let Some(id) = extract_job_id(m) {
            keys.push(Value::from(delayed_index_key(queue_name, &id)));
        }
    }
    if keys.is_empty() {
        return;
    }
    let cmd = CustomCommand::new_static("DEL", ClusterHash::FirstKey, false);
    let _: std::result::Result<Value, _> = client.custom(cmd, keys).await;
}

fn value_as_u64(v: &Value) -> u64 {
    match v {
        Value::Integer(n) => (*n).max(0) as u64,
        _ => 0,
    }
}

fn value_as_bool(v: &Value) -> bool {
    match v {
        Value::Integer(n) => *n != 0,
        _ => false,
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

async fn run_promote_eval(
    client: &Client,
    delayed_key: &str,
    stream_key: &str,
    cfg: &PromoterConfig,
) -> std::result::Result<(PromoterTick, Vec<Bytes>), fred::error::Error> {
    let cmd = CustomCommand::new_static("EVAL", ClusterHash::FirstKey, false);
    let args = eval_promote_args(
        PROMOTE_SCRIPT,
        delayed_key,
        stream_key,
        cfg.promote_batch,
        cfg.max_stream_len,
    );
    let res: Value = client.custom(cmd, args).await?;
    Ok((value_as_tick(&res), value_as_promoted_members(&res)))
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
    tracing::warn!(error = %err, op = op, backoff_ms = wait_ms, "promoter transient error");
    if !sleep_or_shutdown(Duration::from_millis(wait_ms), shutdown).await {
        return None;
    }
    Some(idx.saturating_add(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_as_tick_parses_three_element_array() {
        let v = Value::Array(vec![
            Value::Integer(7),
            Value::Integer(42),
            Value::Integer(1500),
        ]);
        let t = value_as_tick(&v);
        assert_eq!(t.promoted, 7);
        assert_eq!(t.depth, 42);
        assert_eq!(t.oldest_pending_lag_ms, 1500);
    }

    #[test]
    fn value_as_tick_short_array_zeros_missing_fields() {
        // Forward-compat: a future script returning fewer fields should not panic;
        // missing trailing fields default to 0.
        let v = Value::Array(vec![Value::Integer(3)]);
        let t = value_as_tick(&v);
        assert_eq!(t.promoted, 3);
        assert_eq!(t.depth, 0);
        assert_eq!(t.oldest_pending_lag_ms, 0);
    }

    #[test]
    fn value_as_tick_legacy_integer_treated_as_promoted_count() {
        // An older script that returned a bare integer for `promoted` is
        // still parseable; depth and lag default to 0.
        let v = Value::Integer(11);
        let t = value_as_tick(&v);
        assert_eq!(t.promoted, 11);
        assert_eq!(t.depth, 0);
        assert_eq!(t.oldest_pending_lag_ms, 0);
    }

    #[test]
    fn value_as_tick_negative_integer_clamps_to_zero() {
        // Lua should never return negative counts, but be defensive.
        let v = Value::Integer(-5);
        let t = value_as_tick(&v);
        assert_eq!(t.promoted, 0);
    }

    #[test]
    fn value_as_tick_unexpected_shape_returns_zero_tick() {
        let v = Value::Null;
        let t = value_as_tick(&v);
        assert_eq!(t.promoted, 0);
        assert_eq!(t.depth, 0);
        assert_eq!(t.oldest_pending_lag_ms, 0);
    }

    #[test]
    fn value_as_tick_array_with_non_integer_items_zeros_those_fields() {
        // If the script ever returns a string where we expected an integer,
        // that field should be 0, not panic.
        let v = Value::Array(vec![
            Value::Integer(5),
            Value::String("oops".into()),
            Value::Integer(99),
        ]);
        let t = value_as_tick(&v);
        assert_eq!(t.promoted, 5);
        assert_eq!(t.depth, 0);
        assert_eq!(t.oldest_pending_lag_ms, 99);
    }

    #[test]
    fn value_as_promoted_members_extracts_slot_4_bytes() {
        let v = Value::Array(vec![
            Value::Integer(2),
            Value::Integer(0),
            Value::Integer(0),
            Value::Array(vec![
                Value::Bytes(Bytes::from_static(b"member-a")),
                Value::Bytes(Bytes::from_static(b"member-b")),
            ]),
        ]);
        let m = value_as_promoted_members(&v);
        assert_eq!(
            m,
            vec![
                Bytes::from_static(b"member-a"),
                Bytes::from_static(b"member-b")
            ]
        );
    }

    #[test]
    fn value_as_promoted_members_missing_slot_returns_empty() {
        // 3-element legacy reply (no slot 4) → empty vec, not a panic.
        let v = Value::Array(vec![
            Value::Integer(0),
            Value::Integer(0),
            Value::Integer(0),
        ]);
        assert!(value_as_promoted_members(&v).is_empty());
    }

    #[test]
    fn value_as_promoted_members_legacy_integer_returns_empty() {
        assert!(value_as_promoted_members(&Value::Integer(5)).is_empty());
    }

    #[test]
    fn value_as_promoted_members_accepts_string_items() {
        // Some Redis client/transport combos surface RESP bulk strings as
        // `Value::String` rather than `Value::Bytes`. Both must work.
        let v = Value::Array(vec![
            Value::Integer(1),
            Value::Integer(0),
            Value::Integer(0),
            Value::Array(vec![Value::String("payload".into())]),
        ]);
        let m = value_as_promoted_members(&v);
        assert_eq!(m, vec![Bytes::from_static(b"payload")]);
    }

    #[test]
    fn extract_job_id_round_trips_through_msgpack() {
        let job = crate::job::Job::with_id("my-id".to_string(), 42_u32);
        let bytes = rmp_serde::to_vec(&job).expect("encode");
        assert_eq!(extract_job_id(&bytes).as_deref(), Some("my-id"));
    }

    #[test]
    fn extract_job_id_returns_none_for_garbage() {
        assert!(extract_job_id(b"not msgpack").is_none());
    }
}
