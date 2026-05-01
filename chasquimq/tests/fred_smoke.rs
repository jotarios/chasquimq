// Lock-in reference for the raw command shapes we use throughout the engine:
//   XADD <stream> IDMP <pid> <iid> MAXLEN ~ <n> * d <bytes>
//   XGROUP CREATE <stream> <group> $ MKSTREAM
//   XREADGROUP GROUP <group> <consumer> COUNT <n> BLOCK <ms> CLAIM <min-idle-ms> STREAMS <stream> >
//   XACKDEL <stream> <group> <ids...>

use bytes::Bytes;
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::prelude::Config;
use fred::types::{ClusterHash, CustomCommand, Value};

fn redis_url() -> Option<String> {
    std::env::var("REDIS_URL").ok()
}

async fn connect() -> Client {
    let url = redis_url().expect("REDIS_URL must be set to run smoke tests");
    let cfg = Config::from_url(&url).expect("valid REDIS_URL");
    let client = Client::new(cfg, None, None, None);
    client.init().await.expect("connect");
    client
}

async fn raw(client: &Client, cmd: &'static str, args: Vec<Value>) -> Value {
    client
        .custom(
            CustomCommand::new_static(cmd, ClusterHash::FirstKey, false),
            args,
        )
        .await
        .unwrap_or_else(|e| panic!("{cmd}: {e}"))
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn xadd_idmp_xgroup_xreadgroup_claim_xackdel() {
    let client = connect().await;

    let key = "chasqui:smoke:stream";
    let group = "g1";
    let pid = "smoke-pid";
    let iid = "smoke-iid-1";
    let payload = Bytes::from_static(b"\x81\xa1k\xa1v"); // msgpack {"k":"v"}

    let _: Value = raw(&client, "DEL", vec![Value::from(key)]).await;

    let xadd_args = |iid_arg: &str| -> Vec<Value> {
        vec![
            Value::from(key),
            Value::from("IDMP"),
            Value::from(pid),
            Value::from(iid_arg),
            Value::from("MAXLEN"),
            Value::from("~"),
            Value::from(1000_i64),
            Value::from("*"),
            Value::from("d"),
            Value::Bytes(payload.clone()),
        ]
    };

    let first = raw(&client, "XADD", xadd_args(iid)).await;
    let second = raw(&client, "XADD", xadd_args(iid)).await;

    let entry_id_1 = match &first {
        Value::String(s) => s.to_string(),
        Value::Bytes(b) => String::from_utf8(b.to_vec()).unwrap(),
        other => panic!("unexpected XADD response: {other:?}"),
    };
    let entry_id_2 = match &second {
        Value::String(s) => s.to_string(),
        Value::Bytes(b) => String::from_utf8(b.to_vec()).unwrap(),
        other => panic!("unexpected XADD response: {other:?}"),
    };
    assert_eq!(
        entry_id_1, entry_id_2,
        "IDMP must return identical entry id for duplicate (pid, iid)"
    );

    let xlen = raw(&client, "XLEN", vec![Value::from(key)]).await;
    assert!(
        matches!(xlen, Value::Integer(1)),
        "XLEN should be 1 after dedup, got {xlen:?}"
    );

    let create = client
        .custom::<Value, Value>(
            CustomCommand::new_static("XGROUP", ClusterHash::FirstKey, false),
            vec![
                Value::from("CREATE"),
                Value::from(key),
                Value::from(group),
                Value::from("$"),
                Value::from("MKSTREAM"),
            ],
        )
        .await;
    if let Err(e) = &create {
        let msg = format!("{e}");
        if !msg.contains("BUSYGROUP") {
            panic!("XGROUP CREATE failed: {e}");
        }
    }

    let _: Value = raw(&client, "XADD", xadd_args("smoke-iid-2")).await;
    let xlen_before_consume = match raw(&client, "XLEN", vec![Value::from(key)]).await {
        Value::Integer(n) => n,
        other => panic!("XLEN: {other:?}"),
    };

    let read = raw(
        &client,
        "XREADGROUP",
        vec![
            Value::from("GROUP"),
            Value::from(group),
            Value::from("c1"),
            Value::from("COUNT"),
            Value::from(10_i64),
            Value::from("BLOCK"),
            Value::from(100_i64),
            Value::from("STREAMS"),
            Value::from(key),
            Value::from(">"),
        ],
    )
    .await;

    let entry_id = extract_first_entry_id(&read).expect("read returned at least one entry");

    let ackdel = raw(
        &client,
        "XACKDEL",
        vec![
            Value::from(key),
            Value::from(group),
            Value::from("IDS"),
            Value::from(1_i64),
            Value::from(entry_id.clone()),
        ],
    )
    .await;
    assert!(
        matches!(&ackdel, Value::Array(_) | Value::Integer(_)),
        "XACKDEL response shape: {ackdel:?}"
    );

    let xlen_after = match raw(&client, "XLEN", vec![Value::from(key)]).await {
        Value::Integer(n) => n,
        other => panic!("XLEN: {other:?}"),
    };
    assert_eq!(
        xlen_after,
        xlen_before_consume - 1,
        "XACKDEL should remove exactly the consumed entry"
    );

    let pending = raw(
        &client,
        "XPENDING",
        vec![Value::from(key), Value::from(group)],
    )
    .await;
    let pending_count = match &pending {
        Value::Array(items) => match items.first() {
            Some(Value::Integer(n)) => *n,
            other => panic!("unexpected XPENDING summary head: {other:?}"),
        },
        other => panic!("unexpected XPENDING response: {other:?}"),
    };
    assert_eq!(pending_count, 0, "no entries should remain pending");

    let _: Value = raw(&client, "XADD", xadd_args("smoke-iid-3")).await;
    let read_no_ack = raw(
        &client,
        "XREADGROUP",
        vec![
            Value::from("GROUP"),
            Value::from(group),
            Value::from("c1"),
            Value::from("COUNT"),
            Value::from(10_i64),
            Value::from("BLOCK"),
            Value::from(100_i64),
            Value::from("STREAMS"),
            Value::from(key),
            Value::from(">"),
        ],
    )
    .await;
    let pending_id = extract_first_entry_id(&read_no_ack).expect("read pending entry");

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    let read_with_claim = raw(
        &client,
        "XREADGROUP",
        vec![
            Value::from("GROUP"),
            Value::from(group),
            Value::from("c2"),
            Value::from("COUNT"),
            Value::from(10_i64),
            Value::from("BLOCK"),
            Value::from(100_i64),
            Value::from("CLAIM"),
            Value::from(50_i64),
            Value::from("STREAMS"),
            Value::from(key),
            Value::from(">"),
        ],
    )
    .await;
    let claimed_id = extract_first_entry_id(&read_with_claim)
        .expect("CLAIM should surface the idle pending entry to the second consumer");
    assert_eq!(
        claimed_id, pending_id,
        "CLAIM must redeliver the same entry that was idle in c1's PEL"
    );

    let _: Value = raw(
        &client,
        "XACKDEL",
        vec![
            Value::from(key),
            Value::from(group),
            Value::from("IDS"),
            Value::from(1_i64),
            Value::from(claimed_id),
        ],
    )
    .await;

    let _: () = client.quit().await.unwrap();
}

fn extract_first_entry_id(read_response: &Value) -> Option<String> {
    let outer = match read_response {
        Value::Array(items) => items,
        _ => return None,
    };
    let stream_block = outer.first()?;
    let stream_pair = match stream_block {
        Value::Array(items) => items,
        _ => return None,
    };
    let entries = stream_pair.get(1)?;
    let entries = match entries {
        Value::Array(items) => items,
        _ => return None,
    };
    let first_entry = entries.first()?;
    let entry_pair = match first_entry {
        Value::Array(items) => items,
        _ => return None,
    };
    match entry_pair.first()? {
        Value::String(s) => Some(s.to_string()),
        Value::Bytes(b) => String::from_utf8(b.to_vec()).ok(),
        _ => None,
    }
}
