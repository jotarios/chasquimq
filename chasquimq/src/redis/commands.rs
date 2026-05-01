use crate::redis::keys::PAYLOAD_FIELD;
use bytes::Bytes;
use fred::types::Value;

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
