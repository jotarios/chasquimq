use crate::config::RetryConfig;
use crate::error::{Error, Result};
use crate::redis::commands::{
    RETRY_RESCHEDULE_SCRIPT, eval_retry_args, evalsha_retry_args, script_load_args,
};
use crate::redis::parse::StreamEntryId;
use bytes::Bytes;
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::types::{ClusterHash, CustomCommand, Value};
use std::time::Duration;
use tokio::sync::mpsc;

const RETRY_REDIS_ATTEMPTS: usize = 3;
const RETRY_REDIS_BASE_MS: u64 = 50;

#[derive(Debug)]
pub(crate) struct RetryRelocate {
    pub entry_id: StreamEntryId,
    pub job_bytes: Bytes,
    pub run_at_ms: i64,
}

pub(crate) struct RetryRelocatorConfig {
    pub stream_key: String,
    pub delayed_key: String,
    pub group: String,
}

pub(crate) async fn enqueue(
    tx: &mpsc::Sender<RetryRelocate>,
    entry_id: StreamEntryId,
    job_bytes: Bytes,
    run_at_ms: i64,
) {
    if tx
        .send(RetryRelocate {
            entry_id,
            job_bytes,
            run_at_ms,
        })
        .await
        .is_err()
    {
        tracing::error!("retry relocator channel closed; retry dropped");
    }
}

pub(crate) async fn run_retry_relocator(
    client: Client,
    cfg: RetryRelocatorConfig,
    mut rx: mpsc::Receiver<RetryRelocate>,
) {
    let mut sha = match load_script(&client).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "retry relocator: SCRIPT LOAD failed; entries will reclaim via CLAIM");
            return;
        }
    };
    while let Some(relocate) = rx.recv().await {
        if let Err(e) = reschedule_with_retry(&client, &cfg, &relocate, &mut sha).await {
            tracing::error!(entry_id = %relocate.entry_id, error = %e, "retry reschedule failed permanently; entry remains pending and will be retried on next CLAIM tick");
        }
    }
}

async fn reschedule_with_retry(
    client: &Client,
    cfg: &RetryRelocatorConfig,
    relocate: &RetryRelocate,
    sha: &mut String,
) -> Result<()> {
    let mut last_err: Option<Error> = None;
    for attempt in 0..RETRY_REDIS_ATTEMPTS {
        match reschedule_once(client, cfg, relocate, sha).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                let backoff = RETRY_REDIS_BASE_MS << attempt;
                tracing::warn!(entry_id = %relocate.entry_id, attempt = attempt + 1, error = %e, backoff_ms = backoff, "retry reschedule failed; retrying");
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(backoff)).await;
            }
        }
    }
    Err(last_err.unwrap_or_else(|| Error::Config("retry reschedule exhausted retries".into())))
}

async fn reschedule_once(
    client: &Client,
    cfg: &RetryRelocatorConfig,
    relocate: &RetryRelocate,
    sha: &mut String,
) -> Result<()> {
    let cmd = CustomCommand::new_static("EVALSHA", ClusterHash::FirstKey, false);
    let args = evalsha_retry_args(
        sha,
        &cfg.stream_key,
        &cfg.delayed_key,
        &cfg.group,
        relocate.entry_id.as_ref(),
        relocate.run_at_ms,
        relocate.job_bytes.clone(),
    );
    let res: std::result::Result<Value, fred::error::Error> = client.custom(cmd, args).await;
    match res {
        Ok(_) => Ok(()),
        Err(e) if format!("{e}").contains("NOSCRIPT") => {
            *sha = load_script(client).await?;
            let cmd = CustomCommand::new_static("EVAL", ClusterHash::FirstKey, false);
            let args = eval_retry_args(
                RETRY_RESCHEDULE_SCRIPT,
                &cfg.stream_key,
                &cfg.delayed_key,
                &cfg.group,
                relocate.entry_id.as_ref(),
                relocate.run_at_ms,
                relocate.job_bytes.clone(),
            );
            let _: Value = client.custom(cmd, args).await.map_err(Error::Redis)?;
            Ok(())
        }
        Err(e) => Err(Error::Redis(e)),
    }
}

async fn load_script(client: &Client) -> Result<String> {
    let cmd = CustomCommand::new_static("SCRIPT", ClusterHash::FirstKey, false);
    let res: Value = client
        .custom(cmd, script_load_args(RETRY_RESCHEDULE_SCRIPT))
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

/// Compute backoff for the upcoming Nth retry. `attempt` is 1-indexed:
/// 1 = the first retry (after the initial failure), 2 = the second, etc.
pub(crate) fn backoff_ms(attempt: u32, cfg: &RetryConfig) -> u64 {
    let exp = attempt.saturating_sub(1) as i32;
    let base = cfg.initial_backoff_ms as f64 * cfg.multiplier.powi(exp);
    let capped = base.min(cfg.max_backoff_ms as f64).max(0.0) as u64;
    let jitter = if cfg.jitter_ms == 0 {
        0
    } else {
        fastrand_jitter(cfg.jitter_ms)
    };
    capped.saturating_add(jitter)
}

fn fastrand_jitter(max: u64) -> u64 {
    use std::cell::Cell;
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEED: AtomicU64 = AtomicU64::new(0);
    thread_local! {
        static STATE: Cell<u64> = const { Cell::new(0) };
    }
    STATE.with(|s| {
        let mut x = s.get();
        if x == 0 {
            let salt = SEED.fetch_add(0x9E3779B97F4A7C15, Ordering::Relaxed);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(1);
            x = now ^ salt ^ 0xDEAD_BEEF_CAFE_F00D;
            if x == 0 {
                x = 1;
            }
        }
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        s.set(x);
        x % max
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_monotone_until_cap() {
        let cfg = RetryConfig {
            initial_backoff_ms: 100,
            max_backoff_ms: 1_000,
            multiplier: 2.0,
            jitter_ms: 0,
        };
        assert_eq!(backoff_ms(1, &cfg), 100);
        assert_eq!(backoff_ms(2, &cfg), 200);
        assert_eq!(backoff_ms(3, &cfg), 400);
        assert_eq!(backoff_ms(4, &cfg), 800);
        assert_eq!(backoff_ms(5, &cfg), 1_000);
        assert_eq!(backoff_ms(10, &cfg), 1_000);
    }

    #[test]
    fn backoff_jitter_within_bounds() {
        let cfg = RetryConfig {
            initial_backoff_ms: 100,
            max_backoff_ms: 1_000,
            multiplier: 2.0,
            jitter_ms: 50,
        };
        for _ in 0..200 {
            let v = backoff_ms(2, &cfg);
            assert!((200..200 + 50).contains(&v), "out of range: {v}");
        }
    }

    #[test]
    fn backoff_zero_attempt_floors_to_initial() {
        let cfg = RetryConfig {
            initial_backoff_ms: 100,
            max_backoff_ms: 1_000,
            multiplier: 2.0,
            jitter_ms: 0,
        };
        assert_eq!(backoff_ms(0, &cfg), 100);
    }
}
