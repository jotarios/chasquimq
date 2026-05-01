use crate::config::PromoterConfig;
use crate::error::{Error, Result};
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

            if !acquired {
                if !sleep_or_shutdown(poll, shutdown).await {
                    return Ok(());
                }
                continue;
            }

            match self.promote_once(client, promote_sha).await {
                Ok(due_count) => {
                    backoff_idx = 0;
                    if due_count >= self.cfg.promote_batch {
                        continue;
                    }
                    if !sleep_or_shutdown(poll, shutdown).await {
                        return Ok(());
                    }
                }
                Err(PromoteError::NoScript) => {
                    *promote_sha = load_script(client, PROMOTE_SCRIPT).await?;
                    let count =
                        run_promote_eval(client, &self.delayed_key, &self.stream_key, &self.cfg)
                            .await
                            .map_err(Error::Redis)?;
                    backoff_idx = 0;
                    if count >= self.cfg.promote_batch {
                        continue;
                    }
                    if !sleep_or_shutdown(poll, shutdown).await {
                        return Ok(());
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
    ) -> std::result::Result<usize, PromoteError> {
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
            Ok(v) => Ok(value_as_count(&v)),
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

fn value_as_count(v: &Value) -> usize {
    match v {
        Value::Integer(n) => (*n).max(0) as usize,
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
) -> std::result::Result<usize, fred::error::Error> {
    let cmd = CustomCommand::new_static("EVAL", ClusterHash::FirstKey, false);
    let args = eval_promote_args(
        PROMOTE_SCRIPT,
        delayed_key,
        stream_key,
        cfg.promote_batch,
        cfg.max_stream_len,
    );
    let res: Value = client.custom(cmd, args).await?;
    Ok(value_as_count(&res))
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
