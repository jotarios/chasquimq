use crate::config::ProducerConfig;
use crate::error::{Error, Result};
use crate::job::{Job, JobId};
use crate::redis::commands::xadd_args;
use crate::redis::conn::connect_pool;
use bytes::Bytes;
use fred::clients::Pool;
use fred::interfaces::ClientLike;
use fred::types::{ClusterHash, CustomCommand, Value};
use serde::Serialize;
use std::marker::PhantomData;
use std::sync::Arc;

pub use crate::redis::keys::{dlq_key, stream_key};

pub struct Producer<T> {
    pool: Pool,
    producer_id: Arc<str>,
    stream_key: Arc<str>,
    max_stream_len: u64,
    _marker: PhantomData<fn(T)>,
}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            producer_id: self.producer_id.clone(),
            stream_key: self.stream_key.clone(),
            max_stream_len: self.max_stream_len,
            _marker: PhantomData,
        }
    }
}

impl<T: Serialize> Producer<T> {
    pub async fn connect(redis_url: &str, config: ProducerConfig) -> Result<Self> {
        let pool = connect_pool(redis_url, config.pool_size).await?;
        let producer_id: Arc<str> = Arc::from(uuid::Uuid::new_v4().to_string());
        let stream_key: Arc<str> = Arc::from(stream_key(&config.queue_name));
        Ok(Self {
            pool,
            producer_id,
            stream_key,
            max_stream_len: config.max_stream_len,
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

        let client = self.pool.next_connected();
        let pipeline = client.pipeline();
        let cmd = CustomCommand::new_static("XADD", ClusterHash::FirstKey, false);
        for (iid, bytes) in &encoded {
            let args = xadd_args(
                self.stream_key.as_ref(),
                self.producer_id.as_ref(),
                iid,
                self.max_stream_len,
                bytes.clone(),
            );
            let _: () = pipeline
                .custom(cmd.clone(), args)
                .await
                .map_err(Error::Redis)?;
        }
        let _: Vec<Value> = pipeline.all().await.map_err(Error::Redis)?;
        Ok(encoded.into_iter().map(|(id, _)| id).collect())
    }

    async fn xadd(&self, iid: &str, bytes: Bytes) -> Result<()> {
        let client = self.pool.next_connected();
        let args = xadd_args(
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
