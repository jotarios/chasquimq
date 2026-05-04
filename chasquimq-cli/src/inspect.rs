use anyhow::{Context, Result};
use comfy_table::{Cell, ContentArrangement, Table, presets::UTF8_FULL};
use fred::clients::Client;
use fred::interfaces::{ClientLike, SortedSetsInterface, StreamsInterface};
use fred::prelude::Config;
use fred::types::Value;
use std::time::{SystemTime, UNIX_EPOCH};

pub async fn run(redis_url: &str, queue: &str, group: &str) -> Result<()> {
    let cfg = Config::from_url(redis_url).context("invalid redis url")?;
    let client = Client::new(cfg, None, None, None);
    client.init().await.context("redis connect failed")?;

    let snapshot = collect(&client, queue, group)
        .await
        .context("inspect queries failed")?;

    let _ = client.quit().await;

    println!("{}", render(queue, group, &snapshot));
    Ok(())
}

struct Snapshot {
    stream_depth: u64,
    pending: u64,
    pending_note: Option<&'static str>,
    dlq_depth: u64,
    delayed_depth: u64,
    oldest_delayed_lag_ms: Option<u64>,
    repeatable_count: u64,
}

async fn collect(client: &Client, queue: &str, group: &str) -> Result<Snapshot> {
    let stream_key = format!("{{chasqui:{queue}}}:stream");
    let dlq_key = format!("{{chasqui:{queue}}}:dlq");
    let delayed_key = format!("{{chasqui:{queue}}}:delayed");
    let repeat_key = format!("{{chasqui:{queue}}}:repeat");

    let pipeline = client.pipeline();
    let _: () = pipeline.xlen(&stream_key).await?;
    let _: () = pipeline.xpending(&stream_key, group, ()).await?;
    let _: () = pipeline.xlen(&dlq_key).await?;
    let _: () = pipeline.zcard(&delayed_key).await?;
    let _: () = pipeline
        .zrange(&delayed_key, 0_i64, 0_i64, None, false, None, true)
        .await?;
    let _: () = pipeline.zcard(&repeat_key).await?;

    let results = pipeline.try_all::<Value>().await;

    let stream_depth = results[0]
        .as_ref()
        .map_err(|e| anyhow::anyhow!("xlen stream failed: {e}"))?
        .clone()
        .convert::<u64>()
        .unwrap_or(0);

    let (pending, pending_note) = match results[1].as_ref() {
        Ok(v) => (parse_xpending_summary_count(v), None),
        Err(e) if format!("{e}").contains("NOGROUP") => (0, Some("group not created")),
        Err(e) => return Err(anyhow::anyhow!("xpending failed: {e}")),
    };

    let dlq_depth = results[2]
        .as_ref()
        .map_err(|e| anyhow::anyhow!("xlen dlq failed: {e}"))?
        .clone()
        .convert::<u64>()
        .unwrap_or(0);

    let delayed_depth = results[3]
        .as_ref()
        .map_err(|e| anyhow::anyhow!("zcard delayed failed: {e}"))?
        .clone()
        .convert::<u64>()
        .unwrap_or(0);

    let oldest_delayed_lag_ms = match results[4].as_ref() {
        Ok(v) => oldest_zrange_score(v).map(|score| {
            let now = now_ms();
            now.saturating_sub(score.max(0) as u64)
        }),
        Err(e) => return Err(anyhow::anyhow!("zrange delayed failed: {e}")),
    };

    let repeatable_count = results[5]
        .as_ref()
        .map_err(|e| anyhow::anyhow!("zcard repeat failed: {e}"))?
        .clone()
        .convert::<u64>()
        .unwrap_or(0);

    Ok(Snapshot {
        stream_depth,
        pending,
        pending_note,
        dlq_depth,
        delayed_depth,
        oldest_delayed_lag_ms,
        repeatable_count,
    })
}

fn parse_xpending_summary_count(v: &Value) -> u64 {
    if let Value::Array(items) = v {
        if let Some(first) = items.first() {
            return first.clone().convert::<u64>().unwrap_or(0);
        }
    }
    0
}

fn oldest_zrange_score(v: &Value) -> Option<i64> {
    if let Value::Array(items) = v {
        if items.len() >= 2 {
            return items[1].clone().convert::<i64>().ok();
        }
    }
    None
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn render(queue: &str, group: &str, s: &Snapshot) -> String {
    let pending_cell = match s.pending_note {
        Some(note) => format!("{} ({})", s.pending, note),
        None => s.pending.to_string(),
    };
    let lag_cell = match s.oldest_delayed_lag_ms {
        Some(lag) => lag.to_string(),
        None => "-".to_string(),
    };

    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new(format!("queue: {queue}")),
            Cell::new(format!("group: {group}")),
        ]);
    table.add_row(vec![Cell::new("stream depth"), Cell::new(s.stream_depth)]);
    table.add_row(vec![Cell::new("pending"), Cell::new(pending_cell)]);
    table.add_row(vec![Cell::new("DLQ depth"), Cell::new(s.dlq_depth)]);
    table.add_row(vec![Cell::new("delayed depth"), Cell::new(s.delayed_depth)]);
    table.add_row(vec![
        Cell::new("oldest delayed lag (ms)"),
        Cell::new(lag_cell),
    ]);
    table.add_row(vec![
        Cell::new("repeatable count"),
        Cell::new(s.repeatable_count),
    ]);
    table.to_string()
}
