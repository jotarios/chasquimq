use anyhow::{Context, Result};
use chasquimq::{Producer, ProducerConfig, RepeatPattern, RepeatableMeta};
use comfy_table::{Cell, ContentArrangement, Table, presets::UTF8_FULL};

pub async fn list(redis_url: &str, queue: &str, limit: u32) -> Result<()> {
    let producer = connect(redis_url, queue).await?;
    let metas = producer
        .list_repeatable(limit as usize)
        .await
        .context("list_repeatable failed")?;

    println!("{}", render(queue, &metas));
    Ok(())
}

pub async fn remove(redis_url: &str, queue: &str, key: &str) -> Result<()> {
    let producer = connect(redis_url, queue).await?;
    let removed = producer
        .remove_repeatable(key)
        .await
        .context("remove_repeatable failed")?;
    if removed {
        println!("removed");
    } else {
        println!("not found");
    }
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

fn render(queue: &str, metas: &[RepeatableMeta]) -> String {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new(format!("repeatable: {queue}")),
            Cell::new("job_name"),
            Cell::new("pattern"),
            Cell::new("tz"),
            Cell::new("next fire (ms)"),
            Cell::new("limit"),
        ]);
    if metas.is_empty() {
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
    for m in metas {
        let (pattern_str, tz_str) = describe_pattern(&m.pattern);
        table.add_row(vec![
            Cell::new(&m.key),
            Cell::new(&m.job_name),
            Cell::new(pattern_str),
            Cell::new(tz_str),
            Cell::new(m.next_fire_ms),
            Cell::new(
                m.limit
                    .map(|l| l.to_string())
                    .unwrap_or_else(|| "-".to_string()),
            ),
        ]);
    }
    table.to_string()
}

fn describe_pattern(pattern: &RepeatPattern) -> (String, String) {
    match pattern {
        RepeatPattern::Cron { expression, tz } => (
            expression.clone(),
            tz.clone().unwrap_or_else(|| "UTC".to_string()),
        ),
        RepeatPattern::Every { interval_ms } => (format!("every {interval_ms}ms"), "-".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn describe_cron_with_tz() {
        let p = RepeatPattern::Cron {
            expression: "0 9 * * *".to_string(),
            tz: Some("America/New_York".to_string()),
        };
        let (pat, tz) = describe_pattern(&p);
        assert_eq!(pat, "0 9 * * *");
        assert_eq!(tz, "America/New_York");
    }

    #[test]
    fn describe_cron_without_tz_defaults_utc() {
        let p = RepeatPattern::Cron {
            expression: "0 0 * * *".to_string(),
            tz: None,
        };
        let (_, tz) = describe_pattern(&p);
        assert_eq!(tz, "UTC");
    }

    #[test]
    fn describe_every_renders_interval() {
        let p = RepeatPattern::Every { interval_ms: 1000 };
        let (pat, tz) = describe_pattern(&p);
        assert_eq!(pat, "every 1000ms");
        assert_eq!(tz, "-");
    }
}
