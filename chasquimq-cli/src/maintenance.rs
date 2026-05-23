//! `chasqui clean` and `chasqui obliterate` — operator-facing wrappers
//! over the engine's job maintenance API.

use anyhow::{Context, Result};
use chasquimq::{JobState, Producer, ProducerConfig};
use std::io::{self, Write};

async fn connect(redis_url: &str, queue: &str) -> Result<Producer<rmpv::Value>> {
    let cfg = ProducerConfig {
        queue_name: queue.to_string(),
        ..Default::default()
    };
    Producer::<rmpv::Value>::connect(redis_url, cfg)
        .await
        .map_err(|e| anyhow::anyhow!("redis connect failed: {e}"))
}

fn confirm(prompt: &str) -> Result<bool> {
    eprint!("{prompt} [y/N]: ");
    io::stderr().flush().ok();
    let mut buf = String::new();
    io::stdin()
        .read_line(&mut buf)
        .context("failed to read confirmation from stdin")?;
    Ok(matches!(
        buf.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
}

/// `chasqui clean <queue> --state <s> --grace-ms <ms> --limit <n>` —
/// age- and state-filtered bulk delete. Prints the removed job ids.
pub async fn clean(
    redis_url: &str,
    queue: &str,
    group: &str,
    state: &str,
    grace_ms: u64,
    limit: u32,
    yes: bool,
) -> Result<()> {
    let parsed = JobState::parse(state).ok_or_else(|| {
        anyhow::anyhow!(
            "unknown state '{state}'; expected one of completed | failed | delayed | waiting"
        )
    })?;
    if !yes
        && !confirm(&format!(
            "Clean up to {limit} '{state}' jobs older than {grace_ms}ms on queue {queue}?"
        ))?
    {
        anyhow::bail!("aborted");
    }

    let producer = connect(redis_url, queue).await?;
    let removed = producer
        .clean(group, grace_ms, limit as usize, parsed)
        .await
        .context("clean failed")?;

    println!(
        "cleaned {} '{}' job(s) from queue {}",
        removed.len(),
        state,
        queue
    );
    for id in &removed {
        println!("  {id}");
    }
    Ok(())
}

/// `chasqui obliterate <queue>` — delete the entire `{chasqui:<queue>}`
/// keyspace. Destructive; gated behind an interactive confirm unless
/// `--yes` is passed.
pub async fn obliterate(redis_url: &str, queue: &str, group: &str, yes: bool) -> Result<()> {
    if !yes
        && !confirm(&format!(
            "Obliterate the ENTIRE queue {queue}? This deletes every job, the DLQ, \
             delayed jobs, repeatable specs, and results. This cannot be undone."
        ))?
    {
        anyhow::bail!("aborted");
    }

    let producer = connect(redis_url, queue).await?;
    let removed = producer
        .obliterate(group)
        .await
        .context("obliterate failed")?;

    println!("obliterated queue {queue}: removed {removed} Redis key(s)");
    Ok(())
}
