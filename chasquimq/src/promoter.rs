use crate::config::PromoterConfig;
use crate::error::{Error, Result};
use crate::metrics::{LockOutcome, PromoterTick, dispatch};
use crate::redis::commands::{
    ACQUIRE_LOCK_SCRIPT, PROMOTE_SCRIPT, RELEASE_LOCK_SCRIPT, eval_acquire_lock_args,
    eval_promote_args, eval_release_lock_args, evalsha_acquire_lock_args, evalsha_promote_args,
    script_load_args,
};
use crate::redis::conn::connect;
use crate::redis::keys::{delayed_key, promoter_lock_key, stream_key};
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::types::{ClusterHash, CustomCommand, Value};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const TRANSIENT_BACKOFF_MS: [u64; 4] = [50, 100, 200, 400];

pub struct Promoter {
    redis_url: String,
    cfg: PromoterConfig,
    stream_key: String,
    delayed_key: String,
    lock_key: String,
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
        }
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<()> {
        let client = connect(&self.redis_url).await?;
        let mut promote_sha = load_script(&client, PROMOTE_SCRIPT).await?;
        let mut lock_sha = load_script(&client, ACQUIRE_LOCK_SCRIPT).await?;
        let outcome = self
            .loop_until_shutdown(&client, &mut promote_sha, &mut lock_sha, &shutdown)
            .await;
        self.release_lock_best_effort(&client).await;
        outcome
    }

    async fn loop_until_shutdown(
        &self,
        client: &Client,
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
                Ok(tick) => match self
                    .handle_tick(tick, &mut backoff_idx, poll, shutdown)
                    .await
                {
                    TickAction::Drain => continue,
                    TickAction::Shutdown => return Ok(()),
                    TickAction::Polled => {}
                },
                Err(PromoteError::NoScript) => {
                    *promote_sha = load_script(client, PROMOTE_SCRIPT).await?;
                    let tick =
                        run_promote_eval(client, &self.delayed_key, &self.stream_key, &self.cfg)
                            .await
                            .map_err(Error::Redis)?;
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
    ) -> std::result::Result<PromoterTick, PromoteError> {
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
            Ok(v) => Ok(value_as_tick(&v)),
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
    // PROMOTE_SCRIPT returns `{promoted, depth, oldest_pending_lag_ms}` as a
    // Lua table, which fred surfaces as `Value::Array`. Older deployments /
    // a future script regression would return a bare integer for `promoted`
    // — we accept that shape for forward-compatibility and zero out the rest.
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
) -> std::result::Result<PromoterTick, fred::error::Error> {
    let cmd = CustomCommand::new_static("EVAL", ClusterHash::FirstKey, false);
    let args = eval_promote_args(
        PROMOTE_SCRIPT,
        delayed_key,
        stream_key,
        cfg.promote_batch,
        cfg.max_stream_len,
    );
    let res: Value = client.custom(cmd, args).await?;
    Ok(value_as_tick(&res))
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
}
