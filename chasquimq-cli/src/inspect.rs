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

pub(crate) struct Snapshot {
    pub stream_depth: u64,
    pub pending: u64,
    pub pending_note: Option<&'static str>,
    pub dlq_depth: u64,
    pub delayed_depth: u64,
    pub oldest_delayed_lag_ms: Option<u64>,
    pub repeatable_count: u64,
}

pub(crate) async fn collect(client: &Client, queue: &str, group: &str) -> Result<Snapshot> {
    const PIPELINE_RESULT_COUNT: usize = 6;

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

    if results.len() < PIPELINE_RESULT_COUNT {
        return Err(anyhow::anyhow!(
            "expected {PIPELINE_RESULT_COUNT} pipeline results, got {}",
            results.len()
        ));
    }

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

pub(crate) fn render(queue: &str, group: &str, s: &Snapshot) -> String {
    render_with_deltas(queue, group, s, None)
}

pub(crate) fn render_with_deltas(
    queue: &str,
    group: &str,
    s: &Snapshot,
    deltas: Option<Deltas>,
) -> String {
    let pending_cell = match s.pending_note {
        Some(note) => format!("{} ({})", s.pending, note),
        None => s.pending.to_string(),
    };
    let lag_cell = match s.oldest_delayed_lag_ms {
        Some(lag) => lag.to_string(),
        None => "-".to_string(),
    };

    let mut table = Table::new();
    let mut header = vec![
        Cell::new(format!("queue: {queue}")),
        Cell::new(format!("group: {group}")),
    ];
    if deltas.is_some() {
        header.push(Cell::new("Δ"));
    }
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(header);

    let mut row = |label: &str, value: String, delta: Option<i64>| {
        let mut cells = vec![Cell::new(label), Cell::new(value)];
        if deltas.is_some() {
            cells.push(Cell::new(
                delta.map(format_delta).unwrap_or_else(|| "-".to_string()),
            ));
        }
        table.add_row(cells);
    };

    row(
        "stream depth",
        s.stream_depth.to_string(),
        deltas.map(|d| d.stream_depth),
    );
    row("pending", pending_cell, None);
    row(
        "DLQ depth",
        s.dlq_depth.to_string(),
        deltas.map(|d| d.dlq_depth),
    );
    row("delayed depth", s.delayed_depth.to_string(), None);
    row("oldest delayed lag (ms)", lag_cell, None);
    row("repeatable count", s.repeatable_count.to_string(), None);
    table.to_string()
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct Deltas {
    pub stream_depth: i64,
    pub dlq_depth: i64,
}

fn format_delta(d: i64) -> String {
    if d > 0 {
        format!("+{d}")
    } else {
        d.to_string()
    }
}
