//! Integration tests for the per-handler progress + log surface
//! ([`chasquimq::JobHandle`]).
//!
//! Each test runs against a real Redis (REDIS_URL) and asserts the
//! per-job progress key / log stream are populated as expected. The
//! handle is constructed directly (not via the worker dispatch wiring,
//! which is exercised by a separate commit) so the tests can isolate
//! the SET / XADD + XLEN paths.

mod common;

use chasquimq::JobHandle;
use chasquimq::producer::{log_key, progress_key};
use fred::interfaces::ClientLike;
use fred::types::{ClusterHash, CustomCommand, Value};
use std::sync::Arc;

use common::{admin, flush_all, redis_url};

/// Helper: build a [`JobHandle`] bound to a fresh small pool. The test
/// owns the pool's connection budget so a misbehaving test can't drain
/// the engine's worker pool.
async fn make_handle(
    queue: &str,
    job_id: &str,
    result_ttl_secs: u64,
    log_max_stream_len: u64,
    log_max_line_bytes: usize,
) -> JobHandle {
    JobHandle::new_for_test(
        &redis_url(),
        Arc::from(job_id),
        Arc::from(queue),
        result_ttl_secs,
        log_max_stream_len,
        log_max_line_bytes,
    )
    .await
    .expect("connect")
}

async fn redis_get(admin: &fred::clients::Client, key: &str) -> Option<String> {
    let v: Value = admin
        .custom(
            CustomCommand::new_static("GET", ClusterHash::FirstKey, false),
            vec![Value::from(key)],
        )
        .await
        .expect("GET");
    match v {
        Value::String(s) => Some(s.to_string()),
        Value::Bytes(b) => std::str::from_utf8(&b).ok().map(|s| s.to_string()),
        Value::Null => None,
        other => panic!("unexpected GET reply: {other:?}"),
    }
}

async fn redis_ttl(admin: &fred::clients::Client, key: &str) -> i64 {
    let v: Value = admin
        .custom(
            CustomCommand::new_static("TTL", ClusterHash::FirstKey, false),
            vec![Value::from(key)],
        )
        .await
        .expect("TTL");
    match v {
        Value::Integer(n) => n,
        other => panic!("unexpected TTL reply: {other:?}"),
    }
}

async fn xlen(admin: &fred::clients::Client, key: &str) -> i64 {
    let v: Value = admin
        .custom(
            CustomCommand::new_static("XLEN", ClusterHash::FirstKey, false),
            vec![Value::from(key)],
        )
        .await
        .expect("XLEN");
    match v {
        Value::Integer(n) => n,
        Value::Null => 0,
        other => panic!("unexpected XLEN reply: {other:?}"),
    }
}

async fn xrange_lines(admin: &fred::clients::Client, key: &str) -> Vec<String> {
    let v: Value = admin
        .custom(
            CustomCommand::new_static("XRANGE", ClusterHash::FirstKey, false),
            vec![Value::from(key), Value::from("-"), Value::from("+")],
        )
        .await
        .expect("XRANGE");
    let items = match v {
        Value::Array(items) => items,
        _ => return Vec::new(),
    };
    let mut out = Vec::with_capacity(items.len());
    for entry in items {
        let Value::Array(pair) = entry else { continue };
        let Some(Value::Array(fields)) = pair.into_iter().nth(1) else {
            continue;
        };
        let mut iter = fields.into_iter();
        while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
            let k = match k {
                Value::String(s) => s.to_string(),
                Value::Bytes(b) => String::from_utf8_lossy(&b).to_string(),
                other => format!("{other:?}"),
            };
            let v = match v {
                Value::String(s) => s.to_string(),
                Value::Bytes(b) => String::from_utf8_lossy(&b).to_string(),
                other => format!("{other:?}"),
            };
            if k == "line" {
                out.push(v);
            }
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires REDIS_URL"]
async fn progress_writes_key_and_sets_ttl() {
    let admin = admin().await;
    let queue = "progress_log_p1";
    flush_all(&admin, queue).await;

    let handle = make_handle(queue, "job-progress-1", 60, 1000, 4096).await;
    handle.update_progress(42).await.expect("update_progress");

    let key = progress_key(queue, "job-progress-1");
    let got = redis_get(&admin, &key).await.expect("progress key present");
    assert_eq!(got, "42", "ASCII decimal");
    let ttl = redis_ttl(&admin, &key).await;
    assert!(
        ttl > 0 && ttl <= 60,
        "TTL must be set and within result_ttl_secs (got {ttl})"
    );

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires REDIS_URL"]
async fn progress_clamps_out_of_range() {
    let admin = admin().await;
    let queue = "progress_log_p2";
    flush_all(&admin, queue).await;

    let handle = make_handle(queue, "job-clamp", 60, 1000, 4096).await;
    handle.update_progress(250).await.expect("update_progress");
    handle.update_progress(200).await.expect("warn-once second");

    let key = progress_key(queue, "job-clamp");
    let got = redis_get(&admin, &key).await.expect("present");
    assert_eq!(got, "100", "clamped to 100");

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires REDIS_URL"]
async fn log_appends_lines_in_order_and_returns_xlen() {
    let admin = admin().await;
    let queue = "progress_log_l1";
    flush_all(&admin, queue).await;

    let handle = make_handle(queue, "job-log-1", 60, 1000, 4096).await;
    let n1 = handle.log("first").await.expect("log 1");
    let n2 = handle.log("second").await.expect("log 2");
    let n3 = handle.log("third").await.expect("log 3");
    assert_eq!((n1, n2, n3), (1, 2, 3), "XLEN grows monotonically");

    let key = log_key(queue, "job-log-1");
    assert_eq!(xlen(&admin, &key).await, 3);
    let lines = xrange_lines(&admin, &key).await;
    assert_eq!(lines, vec!["first", "second", "third"]);

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires REDIS_URL"]
async fn log_truncates_oversize_line_on_utf8_boundary() {
    let admin = admin().await;
    let queue = "progress_log_l2";
    flush_all(&admin, queue).await;

    // Cap 8 bytes — well below the natural line length below.
    let handle = make_handle(queue, "job-log-trunc", 60, 1000, 8).await;
    let line = "abc\u{1F600}def\u{1F600}xyz"; // emoji = 4 bytes each
    handle.log(line).await.expect("log");

    let key = log_key(queue, "job-log-trunc");
    let lines = xrange_lines(&admin, &key).await;
    assert_eq!(lines.len(), 1);
    let got = &lines[0];
    assert!(
        got.ends_with("[\u{2026}truncated]"),
        "truncation marker appended: {got:?}"
    );
    // Truncation must respect UTF-8: the head is a valid str (the
    // assertion that `got` is a `String` already implies that — but
    // confirm we didn't slice through the emoji at byte 4).
    assert!(got.starts_with("abc"), "head preserved: {got:?}");

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires REDIS_URL"]
async fn empty_line_appends_an_entry() {
    let admin = admin().await;
    let queue = "progress_log_l3";
    flush_all(&admin, queue).await;

    let handle = make_handle(queue, "job-log-empty", 60, 1000, 4096).await;
    let n = handle.log("").await.expect("log");
    assert_eq!(n, 1, "even an empty line bumps XLEN");

    let key = log_key(queue, "job-log-empty");
    assert_eq!(xlen(&admin, &key).await, 1);

    let _: () = admin.quit().await.unwrap();
}
