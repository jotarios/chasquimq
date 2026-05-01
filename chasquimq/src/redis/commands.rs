use crate::redis::keys::PAYLOAD_FIELD;
use bytes::Bytes;
use fred::types::Value;

pub(crate) const PROMOTE_SCRIPT: &str = r#"
local time = redis.call('TIME')
local now_ms = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now_ms, 'LIMIT', 0, tonumber(ARGV[1]))
for _, bytes in ipairs(due) do
  redis.call('XADD', KEYS[2], 'MAXLEN', '~', tonumber(ARGV[2]), '*', 'd', bytes)
  redis.call('ZREM', KEYS[1], bytes)
end
return #due
"#;

pub(crate) const RETRY_RESCHEDULE_SCRIPT: &str = r#"
-- KEYS[1] = stream, KEYS[2] = delayed
-- ARGV[1] = group, ARGV[2] = entry_id, ARGV[3] = run_at_ms, ARGV[4] = encoded_bytes
redis.call('XACKDEL', KEYS[1], ARGV[1], 'IDS', 1, ARGV[2])
redis.call('ZADD', KEYS[2], tonumber(ARGV[3]), ARGV[4])
return 1
"#;

pub(crate) const ACQUIRE_LOCK_SCRIPT: &str = r#"
local cur = redis.call('GET', KEYS[1])
if cur == false then
  redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[2]))
  return 1
end
if cur == ARGV[1] then
  redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
  return 1
end
return 0
"#;

pub(crate) fn xadd_args(
    stream_key: &str,
    producer_id: &str,
    iid: &str,
    max_stream_len: u64,
    bytes: Bytes,
) -> Vec<Value> {
    vec![
        Value::from(stream_key),
        Value::from("IDMP"),
        Value::from(producer_id),
        Value::from(iid),
        Value::from("MAXLEN"),
        Value::from("~"),
        Value::from(max_stream_len as i64),
        Value::from("*"),
        Value::from(PAYLOAD_FIELD),
        Value::Bytes(bytes),
    ]
}

pub(crate) fn xreadgroup_args(
    group: &str,
    consumer: &str,
    batch: usize,
    block_ms: u64,
    claim_min_idle_ms: u64,
    stream_key: &str,
) -> Vec<Value> {
    vec![
        Value::from("GROUP"),
        Value::from(group),
        Value::from(consumer),
        Value::from("COUNT"),
        Value::from(batch as i64),
        Value::from("BLOCK"),
        Value::from(block_ms as i64),
        Value::from("CLAIM"),
        Value::from(claim_min_idle_ms as i64),
        Value::from("STREAMS"),
        Value::from(stream_key),
        Value::from(">"),
    ]
}

pub(crate) fn xackdel_args(stream_key: &str, group: &str, ids: &[impl AsRef<str>]) -> Vec<Value> {
    let mut args: Vec<Value> = Vec::with_capacity(4 + ids.len());
    args.push(Value::from(stream_key));
    args.push(Value::from(group));
    args.push(Value::from("IDS"));
    args.push(Value::from(ids.len() as i64));
    for id in ids {
        args.push(Value::from(id.as_ref()));
    }
    args
}

/// XADD args for relocating a stream entry into the DLQ.
/// Carries the original payload plus source_id/reason/optional detail metadata.
pub(crate) fn xadd_dlq_args(
    dlq_key: &str,
    producer_id: &str,
    source_id: &str,
    payload: Bytes,
    reason: &str,
    detail: Option<&str>,
) -> Vec<Value> {
    let mut args: Vec<Value> = Vec::with_capacity(13 + detail.is_some() as usize * 2);
    args.push(Value::from(dlq_key));
    args.push(Value::from("IDMP"));
    args.push(Value::from(producer_id));
    args.push(Value::from(source_id));
    args.push(Value::from("*"));
    args.push(Value::from(PAYLOAD_FIELD));
    args.push(Value::Bytes(payload));
    args.push(Value::from("source_id"));
    args.push(Value::from(source_id));
    args.push(Value::from("reason"));
    args.push(Value::from(reason));
    if let Some(d) = detail {
        args.push(Value::from("detail"));
        args.push(Value::from(d));
    }
    args
}

pub(crate) fn zadd_delayed_args(delayed_key: &str, run_at_ms: i64, bytes: Bytes) -> Vec<Value> {
    vec![
        Value::from(delayed_key),
        Value::from(run_at_ms),
        Value::Bytes(bytes),
    ]
}

pub(crate) fn evalsha_promote_args(
    sha: &str,
    delayed_key: &str,
    stream_key: &str,
    limit: usize,
    max_stream_len: u64,
) -> Vec<Value> {
    vec![
        Value::from(sha),
        Value::from(2_i64),
        Value::from(delayed_key),
        Value::from(stream_key),
        Value::from(limit as i64),
        Value::from(max_stream_len as i64),
    ]
}

pub(crate) fn eval_promote_args(
    script: &str,
    delayed_key: &str,
    stream_key: &str,
    limit: usize,
    max_stream_len: u64,
) -> Vec<Value> {
    vec![
        Value::from(script),
        Value::from(2_i64),
        Value::from(delayed_key),
        Value::from(stream_key),
        Value::from(limit as i64),
        Value::from(max_stream_len as i64),
    ]
}

pub(crate) fn script_load_args(script: &str) -> Vec<Value> {
    vec![Value::from("LOAD"), Value::from(script)]
}

pub(crate) fn evalsha_retry_args(
    sha: &str,
    stream_key: &str,
    delayed_key: &str,
    group: &str,
    entry_id: &str,
    run_at_ms: i64,
    bytes: Bytes,
) -> Vec<Value> {
    vec![
        Value::from(sha),
        Value::from(2_i64),
        Value::from(stream_key),
        Value::from(delayed_key),
        Value::from(group),
        Value::from(entry_id),
        Value::from(run_at_ms),
        Value::Bytes(bytes),
    ]
}

pub(crate) fn eval_retry_args(
    script: &str,
    stream_key: &str,
    delayed_key: &str,
    group: &str,
    entry_id: &str,
    run_at_ms: i64,
    bytes: Bytes,
) -> Vec<Value> {
    vec![
        Value::from(script),
        Value::from(2_i64),
        Value::from(stream_key),
        Value::from(delayed_key),
        Value::from(group),
        Value::from(entry_id),
        Value::from(run_at_ms),
        Value::Bytes(bytes),
    ]
}

pub(crate) fn eval_acquire_lock_args(
    script: &str,
    lock_key: &str,
    holder_id: &str,
    ttl_secs: u64,
) -> Vec<Value> {
    vec![
        Value::from(script),
        Value::from(1_i64),
        Value::from(lock_key),
        Value::from(holder_id),
        Value::from(ttl_secs as i64),
    ]
}

pub(crate) fn evalsha_acquire_lock_args(
    sha: &str,
    lock_key: &str,
    holder_id: &str,
    ttl_secs: u64,
) -> Vec<Value> {
    vec![
        Value::from(sha),
        Value::from(1_i64),
        Value::from(lock_key),
        Value::from(holder_id),
        Value::from(ttl_secs as i64),
    ]
}

pub(crate) fn del_args(key: &str) -> Vec<Value> {
    vec![Value::from(key)]
}
