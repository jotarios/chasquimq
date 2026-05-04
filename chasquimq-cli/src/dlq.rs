use anyhow::{Context, Result};
use chasquimq::{DlqEntry, Producer, ProducerConfig};
use comfy_table::{Cell, ContentArrangement, Table, presets::UTF8_FULL};
use std::collections::BTreeMap;
use std::io::{self, Write};

const DETAIL_TRUNC: usize = 60;

pub async fn peek(redis_url: &str, queue: &str, limit: u32) -> Result<()> {
    let producer = connect(redis_url, queue).await?;
    let entries = producer
        .peek_dlq(limit as usize)
        .await
        .context("peek_dlq failed")?;

    println!("{}", render_entries_table(queue, &entries));
    println!();
    println!("{}", render_histogram(&entries));
    Ok(())
}

pub async fn replay(redis_url: &str, queue: &str, limit: u32, yes: bool) -> Result<()> {
    if !yes && !confirm(queue, limit)? {
        anyhow::bail!("aborted");
    }

    let producer = connect(redis_url, queue).await?;
    let replayed = producer
        .replay_dlq(limit as usize)
        .await
        .context("replay_dlq failed")?;

    println!("replayed {} of {} entries", replayed, limit);
    Ok(())
}

async fn connect(redis_url: &str, queue: &str) -> Result<Producer<rmpv::Value>> {
    let cfg = ProducerConfig {
        queue_name: queue.to_string(),
        ..Default::default()
    };
    Producer::<rmpv::Value>::connect(redis_url, cfg)
        .await
        .map_err(|e| anyhow::anyhow!("redis connect failed: {e}"))
}

fn confirm(queue: &str, limit: u32) -> Result<bool> {
    eprint!("Replay up to {limit} DLQ entries on queue {queue}? [y/N]: ");
    io::stderr().flush().ok();
    let mut buf = String::new();
    io::stdin()
        .read_line(&mut buf)
        .context("failed to read confirmation from stdin")?;
    let trimmed = buf.trim().to_ascii_lowercase();
    Ok(matches!(trimmed.as_str(), "y" | "yes"))
}

fn render_entries_table(queue: &str, entries: &[DlqEntry]) -> String {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new(format!("dlq peek: {queue}")),
            Cell::new("source_id"),
            Cell::new("name"),
            Cell::new("reason"),
            Cell::new("detail"),
            Cell::new("payload bytes"),
        ]);
    if entries.is_empty() {
        table.add_row(vec![
            Cell::new("(empty)"),
            Cell::new("-"),
            Cell::new("-"),
            Cell::new("-"),
            Cell::new("-"),
            Cell::new("-"),
        ]);
        return table.to_string();
    }
    for e in entries {
        table.add_row(vec![
            Cell::new(&e.dlq_id),
            Cell::new(&e.source_id),
            Cell::new(if e.name.is_empty() { "-" } else { &e.name }),
            Cell::new(if e.reason.is_empty() { "-" } else { &e.reason }),
            Cell::new(format_detail(e.detail.as_deref())),
            Cell::new(e.payload.len()),
        ]);
    }
    table.to_string()
}

fn render_histogram(entries: &[DlqEntry]) -> String {
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for e in entries {
        let key = if e.reason.is_empty() {
            "(unknown)"
        } else {
            e.reason.as_str()
        };
        *counts.entry(key).or_default() += 1;
    }
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![Cell::new("reason"), Cell::new("count")]);
    if counts.is_empty() {
        table.add_row(vec![Cell::new("(no entries)"), Cell::new("0")]);
    } else {
        for (reason, count) in counts {
            table.add_row(vec![Cell::new(reason), Cell::new(count)]);
        }
    }
    table.to_string()
}

fn format_detail(detail: Option<&str>) -> String {
    match detail {
        None => "-".to_string(),
        Some(s) if s.len() <= DETAIL_TRUNC => s.to_string(),
        Some(s) => {
            let mut end = DETAIL_TRUNC;
            while !s.is_char_boundary(end) && end > 0 {
                end -= 1;
            }
            format!("{}…", &s[..end])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fred::bytes::Bytes;

    fn entry(reason: &str, detail: Option<&str>, payload_len: usize) -> DlqEntry {
        DlqEntry {
            dlq_id: "1-0".to_string(),
            source_id: "src".to_string(),
            reason: reason.to_string(),
            detail: detail.map(|s| s.to_string()),
            payload: Bytes::from(vec![0u8; payload_len]),
            name: String::new(),
        }
    }

    #[test]
    fn truncates_long_detail() {
        let s = "a".repeat(200);
        let out = format_detail(Some(&s));
        assert!(out.ends_with('…'));
        assert!(out.chars().count() <= DETAIL_TRUNC + 1);
    }

    #[test]
    fn empty_detail_renders_dash() {
        assert_eq!(format_detail(None), "-");
    }

    #[test]
    fn histogram_counts_by_reason() {
        let entries = vec![
            entry("retries_exhausted", None, 8),
            entry("retries_exhausted", None, 8),
            entry("unrecoverable", Some("kaboom"), 16),
        ];
        let out = render_histogram(&entries);
        assert!(out.contains("retries_exhausted"));
        assert!(out.contains("unrecoverable"));
    }
}
