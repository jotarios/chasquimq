use crate::inspect::{self, Deltas, Snapshot};
use anyhow::{Context, Result};
use crossterm::{ExecutableCommand, cursor, terminal};
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::prelude::Config;
use std::io::{Write, stdout};
use std::time::Duration;
use tokio::signal;

pub async fn run(redis_url: &str, queue: &str, group: &str, interval_ms: u64) -> Result<()> {
    let cfg = Config::from_url(redis_url).context("invalid redis url")?;
    let client = Client::new(cfg, None, None, None);
    client.init().await.context("redis connect failed")?;

    let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut prev: Option<Snapshot> = None;
    let result: Result<()> = async {
        loop {
            tokio::select! {
                biased;
                _ = signal::ctrl_c() => break,
                _ = interval.tick() => {
                    let snap = inspect::collect(&client, queue, group)
                        .await
                        .context("inspect queries failed")?;
                    let deltas = prev.as_ref().map(|p| Deltas {
                        stream_depth: snap.stream_depth as i64 - p.stream_depth as i64,
                        dlq_depth: snap.dlq_depth as i64 - p.dlq_depth as i64,
                    });
                    redraw(queue, group, &snap, deltas, interval_ms)?;
                    prev = Some(snap);
                }
            }
        }
        Ok(())
    }
    .await;

    let _ = client.quit().await;
    result
}

fn redraw(
    queue: &str,
    group: &str,
    snap: &Snapshot,
    deltas: Option<Deltas>,
    interval_ms: u64,
) -> Result<()> {
    let mut out = stdout();
    out.execute(terminal::Clear(terminal::ClearType::All))
        .context("terminal clear failed")?;
    out.execute(cursor::MoveTo(0, 0))
        .context("cursor reset failed")?;
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    writeln!(
        out,
        "chasqui watch {queue}  refresh={interval_ms}ms  {now}  (Ctrl+C to exit)"
    )?;
    writeln!(
        out,
        "{}",
        inspect::render_with_deltas(queue, group, snap, deltas)
    )?;
    out.flush()?;
    Ok(())
}
