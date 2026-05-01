use crate::ack::{AckFlusherConfig, run_ack_flusher};
use crate::config::ConsumerConfig;
use crate::error::{Error, HandlerError, Result};
use crate::job::Job;
use crate::producer::{dlq_key, stream_key};
use bytes::Bytes;
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::prelude::Config;
use fred::types::{ClusterHash, CustomCommand, Value};
use serde::de::DeserializeOwned;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub(crate) type StreamEntryId = String;

pub(crate) struct DispatchedJob<T> {
    pub entry_id: StreamEntryId,
    pub job: Job<T>,
}

const PAYLOAD_FIELD: &str = "d";

pub struct Consumer<T> {
    redis_url: String,
    cfg: ConsumerConfig,
    stream_key: String,
    dlq_key: String,
    _marker: PhantomData<fn() -> T>,
}

impl<T> Consumer<T>
where
    T: DeserializeOwned + Send + 'static,
{
    pub fn new(redis_url: impl Into<String>, cfg: ConsumerConfig) -> Self {
        let stream_key = stream_key(&cfg.queue_name);
        let dlq_key = dlq_key(&cfg.queue_name);
        Self {
            redis_url: redis_url.into(),
            cfg,
            stream_key,
            dlq_key,
            _marker: PhantomData,
        }
    }

    pub async fn run<H, Fut>(self, handler: H, shutdown: CancellationToken) -> Result<()>
    where
        H: Fn(Job<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<(), HandlerError>> + Send + 'static,
    {
        let reader = connect(&self.redis_url).await?;
        let dlq_writer = connect(&self.redis_url).await?;
        let ack_client = connect(&self.redis_url).await?;

        ensure_group(&reader, &self.stream_key, &self.cfg.group).await?;

        let (job_tx, job_rx) = mpsc::channel::<DispatchedJob<T>>(self.cfg.concurrency.max(1) * 2);
        let (ack_tx, ack_rx) = mpsc::channel::<StreamEntryId>(self.cfg.concurrency.max(1) * 4);

        let ack_handle = tokio::spawn(run_ack_flusher(
            ack_client,
            AckFlusherConfig {
                stream_key: self.stream_key.clone(),
                group: self.cfg.group.clone(),
                batch: self.cfg.ack_batch,
                idle: std::time::Duration::from_millis(self.cfg.ack_idle_ms),
            },
            ack_rx,
        ));

        let workers = spawn_workers(
            self.cfg.concurrency.max(1),
            handler,
            job_rx,
            ack_tx.clone(),
        );

        let read_state = ReadState {
            reader,
            dlq_writer,
            stream_key: self.stream_key.clone(),
            dlq_key: self.dlq_key.clone(),
            cfg: self.cfg.clone(),
            job_tx,
            ack_tx,
            shutdown: shutdown.clone(),
        };
        let reader_outcome = reader_loop::<T>(read_state).await;

        drain_workers(workers, std::time::Duration::from_secs(self.cfg.shutdown_deadline_secs)).await;

        // Closing ack_tx is implicit via drop in workers; flusher exits when channel closes.
        match ack_handle.await {
            Ok(()) => {}
            Err(e) => tracing::warn!(error = %e, "ack flusher join error"),
        }

        reader_outcome
    }
}

struct ReadState<T> {
    reader: Client,
    dlq_writer: Client,
    stream_key: String,
    dlq_key: String,
    cfg: ConsumerConfig,
    job_tx: mpsc::Sender<DispatchedJob<T>>,
    ack_tx: mpsc::Sender<StreamEntryId>,
    shutdown: CancellationToken,
}

async fn reader_loop<T>(state: ReadState<T>) -> Result<()>
where
    T: DeserializeOwned + Send + 'static,
{
    let ReadState {
        reader,
        dlq_writer,
        stream_key,
        dlq_key,
        cfg,
        job_tx,
        ack_tx,
        shutdown,
    } = state;

    let cmd = CustomCommand::new_static("XREADGROUP", ClusterHash::FirstKey, false);
    loop {
        if shutdown.is_cancelled() {
            break;
        }

        let args = build_xreadgroup_args(
            &cfg.group,
            &cfg.consumer_id,
            cfg.batch,
            cfg.block_ms,
            cfg.claim_min_idle_ms,
            &stream_key,
        );

        let response = tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            r = reader.custom::<Value, _>(cmd.clone(), args) => r,
        };

        let value = match response {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "XREADGROUP failed; backing off 200ms");
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                continue;
            }
        };

        let entries = parse_xreadgroup_response(&value);
        if entries.is_empty() {
            continue;
        }

        for entry in entries {
            let attempt = entry.delivery_count.saturating_add(1);
            if attempt as u32 > cfg.max_attempts {
                let _ = move_to_dlq(&dlq_writer, &dlq_key, &stream_key, &cfg.group, &entry).await;
                continue;
            }

            match rmp_serde::from_slice::<Job<T>>(&entry.payload) {
                Ok(job) => {
                    let dispatched = DispatchedJob {
                        entry_id: entry.id.clone(),
                        job,
                    };
                    if job_tx.send(dispatched).await.is_err() {
                        return Ok(());
                    }
                }
                Err(decode_err) => {
                    tracing::warn!(entry_id = %entry.id, error = %decode_err, "decode failed; routing to DLQ");
                    let _ = move_to_dlq(&dlq_writer, &dlq_key, &stream_key, &cfg.group, &entry).await;
                }
            }
        }

        let _ = ack_tx.capacity();
    }

    Ok(())
}

fn build_xreadgroup_args(
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

#[derive(Debug)]
struct ParsedEntry {
    id: StreamEntryId,
    payload: Bytes,
    delivery_count: i64,
}

fn parse_xreadgroup_response(value: &Value) -> Vec<ParsedEntry> {
    let outer = match value {
        Value::Array(items) => items,
        _ => return Vec::new(),
    };
    let stream_pair = match outer.first() {
        Some(Value::Array(items)) if items.len() >= 2 => items,
        _ => return Vec::new(),
    };
    let entries = match &stream_pair[1] {
        Value::Array(items) => items,
        _ => return Vec::new(),
    };
    entries.iter().filter_map(parse_entry).collect()
}

fn parse_entry(value: &Value) -> Option<ParsedEntry> {
    let items = match value {
        Value::Array(items) => items,
        _ => return None,
    };
    let id = match items.first()? {
        Value::String(s) => s.to_string(),
        Value::Bytes(b) => String::from_utf8(b.to_vec()).ok()?,
        _ => return None,
    };
    let fields = match items.get(1)? {
        Value::Array(items) => items,
        _ => return None,
    };
    let payload = extract_payload_field(fields)?;
    let delivery_count = items
        .get(3)
        .and_then(|v| match v {
            Value::Integer(n) => Some(*n),
            _ => None,
        })
        .unwrap_or(0);
    Some(ParsedEntry {
        id,
        payload,
        delivery_count,
    })
}

fn extract_payload_field(fields: &[Value]) -> Option<Bytes> {
    let mut iter = fields.iter();
    while let (Some(name), Some(val)) = (iter.next(), iter.next()) {
        let is_payload = match name {
            Value::String(s) => s.as_bytes() == PAYLOAD_FIELD.as_bytes(),
            Value::Bytes(b) => b.as_ref() == PAYLOAD_FIELD.as_bytes(),
            _ => false,
        };
        if is_payload {
            return match val {
                Value::Bytes(b) => Some(b.clone()),
                Value::String(s) => Some(Bytes::from(s.as_bytes().to_vec())),
                _ => None,
            };
        }
    }
    None
}

async fn move_to_dlq(
    dlq_writer: &Client,
    dlq_key: &str,
    stream_key: &str,
    group: &str,
    entry: &ParsedEntry,
) -> Result<()> {
    let pipeline = dlq_writer.pipeline();
    let xadd = CustomCommand::new_static("XADD", ClusterHash::FirstKey, false);
    let xackdel = CustomCommand::new_static("XACKDEL", ClusterHash::FirstKey, false);
    let xadd_args: Vec<Value> = vec![
        Value::from(dlq_key),
        Value::from("*"),
        Value::from(PAYLOAD_FIELD),
        Value::Bytes(entry.payload.clone()),
        Value::from("source_id"),
        Value::from(entry.id.as_str()),
    ];
    let xackdel_args: Vec<Value> = vec![
        Value::from(stream_key),
        Value::from(group),
        Value::from("IDS"),
        Value::from(1_i64),
        Value::from(entry.id.as_str()),
    ];
    let _: () = pipeline.custom(xadd, xadd_args).await.map_err(Error::Redis)?;
    let _: () = pipeline
        .custom(xackdel, xackdel_args)
        .await
        .map_err(Error::Redis)?;
    let _: Vec<Value> = pipeline.all().await.map_err(Error::Redis)?;
    Ok(())
}

struct WorkerPool {
    set: JoinSet<()>,
}

fn spawn_workers<T, H, Fut>(
    concurrency: usize,
    handler: H,
    job_rx: mpsc::Receiver<DispatchedJob<T>>,
    ack_tx: mpsc::Sender<StreamEntryId>,
) -> WorkerPool
where
    T: Send + 'static,
    H: Fn(Job<T>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = std::result::Result<(), HandlerError>> + Send + 'static,
{
    let handler = Arc::new(handler);
    let job_rx = Arc::new(tokio::sync::Mutex::new(job_rx));
    let mut set: JoinSet<()> = JoinSet::new();
    for _ in 0..concurrency {
        let handler = handler.clone();
        let rx = job_rx.clone();
        let ack_tx = ack_tx.clone();
        set.spawn(async move {
            loop {
                let dispatched = {
                    let mut guard = rx.lock().await;
                    guard.recv().await
                };
                let Some(dispatched) = dispatched else { break };
                let entry_id = dispatched.entry_id;
                let job_id = dispatched.job.id.clone();
                let fut = handler(dispatched.job);
                let outcome = tokio::spawn(fut).await;
                match outcome {
                    Ok(Ok(())) => {
                        if ack_tx.send(entry_id).await.is_err() {
                            break;
                        }
                    }
                    Ok(Err(e)) => {
                        tracing::warn!(job_id = %job_id, error = %e, "handler returned Err");
                    }
                    Err(join_err) => {
                        tracing::warn!(job_id = %job_id, error = %join_err, "handler panicked");
                    }
                }
            }
        });
    }
    WorkerPool { set }
}

async fn drain_workers(mut pool: WorkerPool, deadline: std::time::Duration) {
    let drain = async {
        while pool.set.join_next().await.is_some() {}
    };
    if tokio::time::timeout(deadline, drain).await.is_err() {
        tracing::warn!(?deadline, "worker drain hit deadline; aborting in-flight tasks");
        pool.set.abort_all();
        while pool.set.join_next().await.is_some() {}
    }
}

async fn ensure_group(client: &Client, stream_key: &str, group: &str) -> Result<()> {
    let cmd = CustomCommand::new_static("XGROUP", ClusterHash::FirstKey, false);
    let res = client
        .custom::<Value, _>(
            cmd,
            vec![
                Value::from("CREATE"),
                Value::from(stream_key),
                Value::from(group),
                Value::from("0"),
                Value::from("MKSTREAM"),
            ],
        )
        .await;
    match res {
        Ok(_) => Ok(()),
        Err(e) => {
            let msg = format!("{e}");
            if msg.contains("BUSYGROUP") {
                Ok(())
            } else {
                Err(Error::Redis(e))
            }
        }
    }
}

async fn connect(url: &str) -> Result<Client> {
    let cfg = Config::from_url(url).map_err(Error::Redis)?;
    let client = Client::new(cfg, None, None, None);
    client.init().await.map_err(Error::Redis)?;
    Ok(client)
}
