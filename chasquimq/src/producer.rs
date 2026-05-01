use crate::config::ProducerConfig;
use crate::error::{Error, Result};
use crate::job::{Job, JobId};
use bytes::Bytes;
use fred::clients::Pool;
use fred::interfaces::ClientLike;
use fred::prelude::Config;
use fred::types::{ClusterHash, ConnectHandle, CustomCommand, Value};
use serde::Serialize;
use std::marker::PhantomData;
use std::sync::Arc;

const PAYLOAD_FIELD: &str = "d";

pub fn stream_key(queue_name: &str) -> String {
    format!("chasqui:{queue_name}:stream")
}

pub fn dlq_key(queue_name: &str) -> String {
    format!("chasqui:{queue_name}:dlq")
}

pub struct Producer<T> {
    pool: Pool,
    producer_id: Arc<str>,
    stream_key: Arc<str>,
    max_stream_len: u64,
    _connect_handle: Arc<ConnectHandle>,
    _marker: PhantomData<fn(T)>,
}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            producer_id: self.producer_id.clone(),
            stream_key: self.stream_key.clone(),
            max_stream_len: self.max_stream_len,
            _connect_handle: self._connect_handle.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T> Producer<T>
where
    T: Serialize + Send + Sync + 'static,
{
    pub async fn connect(redis_url: &str, config: ProducerConfig) -> Result<Self> {
        if config.pool_size == 0 {
            return Err(Error::Config("pool_size must be > 0".into()));
        }
        let cfg = Config::from_url(redis_url).map_err(Error::Redis)?;
        let pool = Pool::new(cfg, None, None, None, config.pool_size).map_err(Error::Redis)?;
        let handle = pool.init().await.map_err(Error::Redis)?;
        let producer_id: Arc<str> = Arc::from(uuid::Uuid::new_v4().to_string());
        let stream_key: Arc<str> = Arc::from(stream_key(&config.queue_name));
        Ok(Self {
            pool,
            producer_id,
            stream_key,
            max_stream_len: config.max_stream_len,
            _connect_handle: Arc::new(handle),
            _marker: PhantomData,
        })
    }

    pub fn producer_id(&self) -> &str {
        &self.producer_id
    }

    pub fn stream_key(&self) -> &str {
        &self.stream_key
    }

    pub async fn add(&self, payload: T) -> Result<JobId> {
        let job = Job::new(payload);
        let id = job.id.clone();
        let bytes = Bytes::from(rmp_serde::to_vec(&job)?);
        self.xadd(&id, bytes).await?;
        Ok(id)
    }

    pub async fn add_with_id(&self, id: JobId, payload: T) -> Result<JobId> {
        let job = Job::with_id(id.clone(), payload);
        let bytes = Bytes::from(rmp_serde::to_vec(&job)?);
        self.xadd(&id, bytes).await?;
        Ok(id)
    }

    pub async fn add_bulk(&self, payloads: Vec<T>) -> Result<Vec<JobId>> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        let mut encoded: Vec<(JobId, Bytes)> = Vec::with_capacity(payloads.len());
        for payload in payloads {
            let job = Job::new(payload);
            let id = job.id.clone();
            let bytes = Bytes::from(rmp_serde::to_vec(&job)?);
            encoded.push((id, bytes));
        }

        let client = self.pool.next();
        let pipeline = client.pipeline();
        let cmd = CustomCommand::new_static("XADD", ClusterHash::FirstKey, false);
        for (iid, bytes) in &encoded {
            let args = build_xadd_args(
                self.stream_key.as_ref(),
                self.producer_id.as_ref(),
                iid,
                self.max_stream_len,
                bytes.clone(),
            );
            let _: () = pipeline.custom(cmd.clone(), args).await.map_err(Error::Redis)?;
        }
        let _: Vec<Value> = pipeline.all().await.map_err(Error::Redis)?;
        Ok(encoded.into_iter().map(|(id, _)| id).collect())
    }

    async fn xadd(&self, iid: &str, bytes: Bytes) -> Result<()> {
        let client = self.pool.next();
        let args = build_xadd_args(
            self.stream_key.as_ref(),
            self.producer_id.as_ref(),
            iid,
            self.max_stream_len,
            bytes,
        );
        let cmd = CustomCommand::new_static("XADD", ClusterHash::FirstKey, false);
        let _: Value = client.custom(cmd, args).await.map_err(Error::Redis)?;
        Ok(())
    }
}

fn build_xadd_args(
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
