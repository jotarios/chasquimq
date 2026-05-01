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
use futures_util::FutureExt;
use serde::de::DeserializeOwned;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub(crate) type StreamEntryId = Arc<str>;

pub(crate) struct DispatchedJob<T> {
    pub entry_id: StreamEntryId,
    pub job: Job<T>,
}

const PAYLOAD_FIELD: &str = "d";
const DLQ_RETRY_ATTEMPTS: usize = 3;
const DLQ_RETRY_BASE_MS: u64 = 50;

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

        let concurrency = self.cfg.concurrency.max(1);
        let (job_tx, job_rx) = async_channel::bounded::<DispatchedJob<T>>(concurrency * 2);
        let (ack_tx, ack_rx) = mpsc::channel::<StreamEntryId>(concurrency * 4);
        let (dlq_tx, dlq_rx) =
            mpsc::channel::<DlqRelocate>(self.cfg.dlq_inflight.max(1));

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

        let dlq_producer_id: Arc<str> = Arc::from(uuid::Uuid::new_v4().to_string());
        let dlq_handle = tokio::spawn(run_dlq_relocator(
            dlq_writer,
            DlqRelocatorConfig {
                stream_key: self.stream_key.clone(),
                dlq_key: self.dlq_key.clone(),
                group: self.cfg.group.clone(),
                producer_id: dlq_producer_id,
            },
            dlq_rx,
        ));

        let workers = spawn_workers(concurrency, handler, job_rx, ack_tx.clone());
        drop(ack_tx);

        let read_state = ReadState {
            reader,
            stream_key: Arc::<str>::from(self.stream_key.clone()),
            cfg: self.cfg.clone(),
            job_tx,
            dlq_tx,
            shutdown: shutdown.clone(),
        };
        let reader_outcome = reader_loop::<T>(read_state).await;

        drain_workers(
            workers,
            std::time::Duration::from_secs(self.cfg.shutdown_deadline_secs),
        )
        .await;

        if let Err(e) = ack_handle.await {
            tracing::warn!(error = %e, "ack flusher join error");
        }
        if let Err(e) = dlq_handle.await {
            tracing::warn!(error = %e, "dlq relocator join error");
        }

        reader_outcome
    }
}

struct ReadState<T> {
    reader: Client,
    stream_key: Arc<str>,
    cfg: ConsumerConfig,
    job_tx: async_channel::Sender<DispatchedJob<T>>,
    dlq_tx: mpsc::Sender<DlqRelocate>,
    shutdown: CancellationToken,
}

async fn reader_loop<T>(state: ReadState<T>) -> Result<()>
where
    T: DeserializeOwned + Send + 'static,
{
    let ReadState {
        reader,
        stream_key,
        cfg,
        job_tx,
        dlq_tx,
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

        for shape in entries {
            let entry = match shape {
                EntryShape::Ok(e) => e,
                EntryShape::MalformedWithId { id, reason } => {
                    tracing::warn!(entry_id = %id, reason, "malformed stream entry; routing to DLQ");
                    enqueue_dlq(&dlq_tx, id, Bytes::new(), DlqReason::Malformed { reason }).await;
                    continue;
                }
                EntryShape::Unrecoverable => {
                    tracing::error!("XREADGROUP returned an entry with no recoverable id; cannot DLQ — skipping");
                    continue;
                }
            };

            let attempt: u32 = u32::try_from(entry.delivery_count.saturating_add(1))
                .unwrap_or(u32::MAX);
            if attempt > cfg.max_attempts {
                enqueue_dlq(&dlq_tx, entry.id, entry.payload, DlqReason::RetriesExhausted).await;
                continue;
            }

            if entry.payload.len() > cfg.max_payload_bytes {
                tracing::warn!(entry_id = %entry.id, size = entry.payload.len(), max = cfg.max_payload_bytes, "payload exceeds max_payload_bytes; routing to DLQ");
                enqueue_dlq(&dlq_tx, entry.id, entry.payload, DlqReason::OversizePayload).await;
                continue;
            }

            match rmp_serde::from_slice::<Job<T>>(&entry.payload) {
                Ok(job) => {
                    let dispatched = DispatchedJob {
                        entry_id: entry.id,
                        job,
                    };
                    if job_tx.send(dispatched).await.is_err() {
                        return Ok(());
                    }
                }
                Err(decode_err) => {
                    tracing::warn!(entry_id = %entry.id, error = %decode_err, "decode failed; routing to DLQ");
                    enqueue_dlq(&dlq_tx, entry.id, entry.payload, DlqReason::DecodeFailed).await;
                }
            }
        }
    }

    Ok(())
}

async fn enqueue_dlq(
    dlq_tx: &mpsc::Sender<DlqRelocate>,
    entry_id: StreamEntryId,
    payload: Bytes,
    reason: DlqReason,
) {
    if dlq_tx
        .send(DlqRelocate {
            entry_id,
            payload,
            reason,
        })
        .await
        .is_err()
    {
        tracing::error!("dlq relocator channel closed; relocation dropped");
    }
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

#[derive(Debug)]
enum EntryShape {
    Ok(ParsedEntry),
    MalformedWithId {
        id: StreamEntryId,
        reason: &'static str,
    },
    Unrecoverable,
}

fn parse_xreadgroup_response(value: &Value) -> Vec<EntryShape> {
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
    entries.iter().map(parse_entry).collect()
}

fn parse_entry(value: &Value) -> EntryShape {
    let items = match value {
        Value::Array(items) => items,
        _ => return EntryShape::Unrecoverable,
    };
    let id: StreamEntryId = match items.first() {
        Some(Value::String(s)) => Arc::from(&**s),
        Some(Value::Bytes(b)) => match std::str::from_utf8(b) {
            Ok(s) => Arc::from(s),
            Err(_) => return EntryShape::Unrecoverable,
        },
        _ => return EntryShape::Unrecoverable,
    };
    let fields = match items.get(1) {
        Some(Value::Array(items)) => items,
        _ => {
            return EntryShape::MalformedWithId {
                id,
                reason: "fields not an array",
            };
        }
    };
    let payload = match extract_payload_field(fields) {
        Some(b) => b,
        None => {
            return EntryShape::MalformedWithId {
                id,
                reason: "missing or non-bytes payload field",
            };
        }
    };
    let delivery_count = items
        .get(3)
        .and_then(|v| match v {
            Value::Integer(n) => Some(*n),
            _ => None,
        })
        .unwrap_or(0);
    EntryShape::Ok(ParsedEntry {
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

#[derive(Debug)]
enum DlqReason {
    RetriesExhausted,
    DecodeFailed,
    Malformed { reason: &'static str },
    OversizePayload,
}

impl DlqReason {
    fn as_str(&self) -> &'static str {
        match self {
            DlqReason::RetriesExhausted => "retries_exhausted",
            DlqReason::DecodeFailed => "decode_failed",
            DlqReason::Malformed { .. } => "malformed",
            DlqReason::OversizePayload => "oversize_payload",
        }
    }

    fn detail(&self) -> Option<&'static str> {
        if let DlqReason::Malformed { reason } = self {
            Some(reason)
        } else {
            None
        }
    }
}

#[derive(Debug)]
struct DlqRelocate {
    entry_id: StreamEntryId,
    payload: Bytes,
    reason: DlqReason,
}

struct DlqRelocatorConfig {
    stream_key: String,
    dlq_key: String,
    group: String,
    producer_id: Arc<str>,
}

async fn run_dlq_relocator(
    client: Client,
    cfg: DlqRelocatorConfig,
    mut rx: mpsc::Receiver<DlqRelocate>,
) {
    while let Some(relocate) = rx.recv().await {
        if let Err(e) = relocate_with_retry(&client, &cfg, &relocate).await {
            tracing::error!(entry_id = %relocate.entry_id, reason = %relocate.reason.as_str(), error = %e, "DLQ relocation failed permanently; entry remains pending and will be retried on next CLAIM tick");
        }
    }
}

async fn relocate_with_retry(
    client: &Client,
    cfg: &DlqRelocatorConfig,
    relocate: &DlqRelocate,
) -> Result<()> {
    let mut last_err: Option<Error> = None;
    for attempt in 0..DLQ_RETRY_ATTEMPTS {
        match relocate_once(client, cfg, relocate).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                let backoff = DLQ_RETRY_BASE_MS << attempt;
                tracing::warn!(entry_id = %relocate.entry_id, attempt = attempt + 1, error = %e, backoff_ms = backoff, "DLQ relocation failed; retrying");
                last_err = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
            }
        }
    }
    Err(last_err.unwrap_or_else(|| Error::Config("DLQ relocation exhausted retries".into())))
}

async fn relocate_once(
    client: &Client,
    cfg: &DlqRelocatorConfig,
    relocate: &DlqRelocate,
) -> Result<()> {
    let pipeline = client.pipeline();
    let xadd = CustomCommand::new_static("XADD", ClusterHash::FirstKey, false);
    let xackdel = CustomCommand::new_static("XACKDEL", ClusterHash::FirstKey, false);

    let mut xadd_args: Vec<Value> = Vec::with_capacity(12);
    xadd_args.push(Value::from(cfg.dlq_key.as_str()));
    xadd_args.push(Value::from("IDMP"));
    xadd_args.push(Value::from(cfg.producer_id.as_ref()));
    xadd_args.push(Value::from(relocate.entry_id.as_ref()));
    xadd_args.push(Value::from("*"));
    xadd_args.push(Value::from(PAYLOAD_FIELD));
    xadd_args.push(Value::Bytes(relocate.payload.clone()));
    xadd_args.push(Value::from("source_id"));
    xadd_args.push(Value::from(relocate.entry_id.as_ref()));
    xadd_args.push(Value::from("reason"));
    xadd_args.push(Value::from(relocate.reason.as_str()));
    if let Some(detail) = relocate.reason.detail() {
        xadd_args.push(Value::from("detail"));
        xadd_args.push(Value::from(detail));
    }

    let xackdel_args: Vec<Value> = vec![
        Value::from(cfg.stream_key.as_str()),
        Value::from(cfg.group.as_str()),
        Value::from("IDS"),
        Value::from(1_i64),
        Value::from(relocate.entry_id.as_ref()),
    ];
    let _: () = pipeline
        .custom(xadd, xadd_args)
        .await
        .map_err(Error::Redis)?;
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
    job_rx: async_channel::Receiver<DispatchedJob<T>>,
    ack_tx: mpsc::Sender<StreamEntryId>,
) -> WorkerPool
where
    T: Send + 'static,
    H: Fn(Job<T>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = std::result::Result<(), HandlerError>> + Send + 'static,
{
    let handler = Arc::new(handler);
    let mut set: JoinSet<()> = JoinSet::new();
    for _ in 0..concurrency {
        let handler = handler.clone();
        let rx = job_rx.clone();
        let ack_tx = ack_tx.clone();
        set.spawn(async move {
            while let Ok(dispatched) = rx.recv().await {
                let entry_id = dispatched.entry_id;
                let job_id = dispatched.job.id.clone();
                let outcome = std::panic::AssertUnwindSafe(handler(dispatched.job))
                    .catch_unwind()
                    .await;
                match outcome {
                    Ok(Ok(())) => {
                        if ack_tx.send(entry_id).await.is_err() {
                            break;
                        }
                    }
                    Ok(Err(e)) => {
                        tracing::warn!(job_id = %job_id, error = %e, "handler returned Err");
                    }
                    Err(_panic) => {
                        tracing::warn!(job_id = %job_id, "handler panicked");
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
