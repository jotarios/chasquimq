use anyhow::{Context, Result};
use chrono::TimeZone;
use fred::clients::Client;
use fred::interfaces::{ClientLike, StreamsInterface};
use fred::prelude::Config;
use fred::types::Value;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::signal;

const BLOCKING_TIMEOUT_MS: u64 = 5_000;
const COUNT_PER_READ: u64 = 100;
const VALUE_TRUNC: usize = 80;

pub async fn run(redis_url: &str, queue: &str, from: &str) -> Result<()> {
    let cfg = Config::from_url(redis_url).context("invalid redis url")?;
    let client = Client::new(cfg, None, None, None);
    client.init().await.context("redis connect failed")?;

    let stream_key = format!("{{chasqui:{queue}}}:events");
    let mut last_id = from.to_string();

    let result: Result<()> = async {
        loop {
            tokio::select! {
                biased;
                _ = signal::ctrl_c() => break,
                res = client.xread::<Value, _, _>(
                    Some(COUNT_PER_READ),
                    Some(BLOCKING_TIMEOUT_MS),
                    stream_key.as_str(),
                    last_id.as_str(),
                ) => {
                    match res {
                        Ok(v) => {
                            for (id, fields) in extract_entries(&v, &stream_key) {
                                print_entry(&id, &fields);
                                last_id = id;
                            }
                        }
                        Err(e) => {
                            eprintln!("xread error: {e}; retrying in 200ms");
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                }
            }
        }
        Ok(())
    }
    .await;

    let _ = client.quit().await;
    result
}

fn extract_entries(value: &Value, stream_key: &str) -> Vec<(String, BTreeMap<String, String>)> {
    let outer = match value {
        Value::Array(items) => items,
        Value::Null => return Vec::new(),
        _ => return Vec::new(),
    };
    let mut out = Vec::new();
    for stream in outer {
        let pair = match stream {
            Value::Array(p) if p.len() >= 2 => p,
            _ => continue,
        };
        let key_matches = match &pair[0] {
            Value::String(s) => &**s == stream_key,
            Value::Bytes(b) => b.as_ref() == stream_key.as_bytes(),
            _ => false,
        };
        if !key_matches {
            continue;
        }
        let entries = match &pair[1] {
            Value::Array(items) => items,
            _ => continue,
        };
        for entry in entries {
            let entry_items = match entry {
                Value::Array(items) if items.len() >= 2 => items,
                _ => continue,
            };
            let id = match value_to_string(&entry_items[0]) {
                Some(s) => s,
                None => continue,
            };
            let fields_arr = match &entry_items[1] {
                Value::Array(items) => items,
                _ => continue,
            };
            let mut fields: BTreeMap<String, String> = BTreeMap::new();
            let mut i = 0;
            while i + 1 < fields_arr.len() {
                if let (Some(k), Some(v)) = (
                    value_to_string(&fields_arr[i]),
                    value_to_string(&fields_arr[i + 1]),
                ) {
                    fields.insert(k, v);
                }
                i += 2;
            }
            out.push((id, fields));
        }
    }
    out
}

fn value_to_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.to_string()),
        Value::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
        Value::Integer(n) => Some(n.to_string()),
        _ => None,
    }
}

fn print_entry(stream_id: &str, fields: &BTreeMap<String, String>) {
    let event_name = fields.get("e").map(String::as_str).unwrap_or("(unknown)");
    let job_id = fields.get("id").cloned().unwrap_or_default();
    let dispatch_name = fields.get("n").cloned().unwrap_or_default();
    let ts_iso = fields
        .get("ts")
        .and_then(|s| s.parse::<i64>().ok())
        .map(format_iso8601)
        .unwrap_or_else(|| "-".to_string());

    // Promote `name` (engine `n` field) to a fixed column right after
    // `id` so the per-event job kind is visually obvious — slice 5 of
    // name-on-the-wire makes this readable without msgpack-decoding.
    let mut extras: Vec<String> = Vec::new();
    for (k, v) in fields {
        if matches!(k.as_str(), "e" | "id" | "ts" | "n") {
            continue;
        }
        extras.push(format!("{k}={}", truncate(v)));
    }
    let job_id_cell = if job_id.is_empty() {
        "-".to_string()
    } else {
        truncate(&job_id)
    };
    let name_cell = if dispatch_name.is_empty() {
        "-".to_string()
    } else {
        truncate(&dispatch_name)
    };
    let suffix = if extras.is_empty() {
        String::new()
    } else {
        format!(" {}", extras.join(" "))
    };
    println!("{ts_iso} {stream_id} {event_name} id={job_id_cell} name={name_cell}{suffix}");
}

fn format_iso8601(unix_ms: i64) -> String {
    match chrono::Utc.timestamp_millis_opt(unix_ms) {
        chrono::LocalResult::Single(dt) => dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        _ => unix_ms.to_string(),
    }
}

fn truncate(s: &str) -> String {
    if s.chars().count() <= VALUE_TRUNC {
        return s.to_string();
    }
    let mut out = String::with_capacity(VALUE_TRUNC + 3);
    for (i, c) in s.chars().enumerate() {
        if i >= VALUE_TRUNC {
            break;
        }
        out.push(c);
    }
    out.push_str("...");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_short_unchanged() {
        assert_eq!(truncate("hi"), "hi");
    }

    #[test]
    fn truncate_long_appends_ellipsis() {
        let s: String = "a".repeat(200);
        let out = truncate(&s);
        assert!(out.ends_with("..."));
        assert_eq!(out.chars().count(), VALUE_TRUNC + 3);
    }

    #[test]
    fn iso8601_known_epoch() {
        let out = format_iso8601(0);
        assert!(out.starts_with("1970-01-01T00:00:00"));
    }
}
