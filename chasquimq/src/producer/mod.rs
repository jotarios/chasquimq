mod dlq;

use crate::config::ProducerConfig;
use crate::error::{Error, Result};
use crate::job::{Job, JobId, now_ms};
use crate::redis::commands::{xadd_args, zadd_delayed_args};
use crate::redis::conn::connect_pool;
use bytes::Bytes;
use fred::clients::Pool;
use fred::interfaces::ClientLike;
use fred::types::{ClusterHash, CustomCommand, Value};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub use crate::redis::keys::{delayed_key, dlq_key, promoter_lock_key, stream_key};

#[derive(Debug, Clone)]
pub struct DlqEntry {
    pub dlq_id: String,
    pub source_id: String,
    pub reason: String,
    pub detail: Option<String>,
    pub payload: Bytes,
}

pub struct Producer<T> {
    pool: Pool,
    producer_id: Arc<str>,
    stream_key: Arc<str>,
    delayed_key: Arc<str>,
    dlq_key: Arc<str>,
    max_stream_len: u64,
    max_delay_secs: u64,
    _marker: PhantomData<fn(T)>,
}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            producer_id: self.producer_id.clone(),
            stream_key: self.stream_key.clone(),
            delayed_key: self.delayed_key.clone(),
            dlq_key: self.dlq_key.clone(),
            max_stream_len: self.max_stream_len,
            max_delay_secs: self.max_delay_secs,
            _marker: PhantomData,
        }
    }
}

impl<T: Serialize> Producer<T> {
    pub async fn connect(redis_url: &str, config: ProducerConfig) -> Result<Self> {
        let pool = connect_pool(redis_url, config.pool_size).await?;
        let producer_id: Arc<str> = Arc::from(uuid::Uuid::new_v4().to_string());
        let stream_key: Arc<str> = Arc::from(stream_key(&config.queue_name));
        let delayed_key: Arc<str> = Arc::from(delayed_key(&config.queue_name));
        let dlq_key: Arc<str> = Arc::from(dlq_key(&config.queue_name));
        Ok(Self {
            pool,
            producer_id,
            stream_key,
            delayed_key,
            dlq_key,
            max_stream_len: config.max_stream_len,
            max_delay_secs: config.max_delay_secs,
            _marker: PhantomData,
        })
    }

    pub fn producer_id(&self) -> &str {
        &self.producer_id
    }

    pub fn stream_key(&self) -> &str {
        &self.stream_key
    }

    pub fn delayed_key(&self) -> &str {
        &self.delayed_key
    }

    pub fn dlq_key(&self) -> &str {
        &self.dlq_key
    }

    /// Read up to `limit` DLQ entries without removing them. The returned bytes
    /// are the raw encoded `Job<T>` payload exactly as it landed in the DLQ —
    /// no `attempt` reset, no decode.
    pub async fn peek_dlq(&self, limit: usize) -> Result<Vec<DlqEntry>> {
        let entries = dlq::xrange_dlq(&self.pool, self.dlq_key.as_ref(), limit).await?;
        Ok(entries.into_iter().map(dlq::parse_dlq_entry).collect())
    }
}

impl<T: Serialize + DeserializeOwned> Producer<T> {
    /// Move up to `limit` entries from the DLQ back into the main stream. Each
    /// entry's `attempt` counter is reset to 0 before re-XADDing, so the replayed
    /// job gets a full retry budget. Returns the number of entries replayed.
    pub async fn replay_dlq(&self, limit: usize) -> Result<usize> {
        dlq::replay::<T>(
            &self.pool,
            self.dlq_key.as_ref(),
            self.stream_key.as_ref(),
            self.max_stream_len,
            limit,
        )
        .await
    }
}

impl<T: Serialize> Producer<T> {
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

    /// Schedule a job to run after `delay`.
    ///
    /// At-least-once under caller-driven retry: each call generates a fresh job id, so a
    /// retry after a network failure can land a duplicate scheduled job. Callers needing
    /// idempotent scheduling should retry only after confirming the previous call did not
    /// reach Redis. (`Producer::add` has Redis-side IDMP dedup; the delayed path does not.)
    pub async fn add_in(&self, delay: Duration, payload: T) -> Result<JobId> {
        self.check_delay_secs(delay.as_secs())?;
        if delay.is_zero() {
            return self.add(payload).await;
        }
        let now = now_ms();
        let run_at_ms = now.saturating_add(delay.as_millis() as u64);
        if run_at_ms <= now {
            return self.add(payload).await;
        }
        self.zadd_delayed(payload, run_at_ms).await
    }

    /// Schedule a job to run at `run_at` (absolute time).
    ///
    /// Same at-least-once-under-retry caveat as [`Producer::add_in`].
    pub async fn add_at(&self, run_at: SystemTime, payload: T) -> Result<JobId> {
        let run_at_ms = match run_at.duration_since(UNIX_EPOCH) {
            Ok(d) => u128_to_u64_or_err(d.as_millis())?,
            Err(_) => 0,
        };
        let now = now_ms();
        if run_at_ms <= now {
            return self.add(payload).await;
        }
        let delay_secs = (run_at_ms - now) / 1000;
        self.check_delay_secs(delay_secs)?;
        self.zadd_delayed(payload, run_at_ms).await
    }

    /// Schedule a batch of jobs to all run after `delay`. Same at-least-once-under-retry
    /// caveat as [`Producer::add_in`].
    pub async fn add_in_bulk(&self, delay: Duration, payloads: Vec<T>) -> Result<Vec<JobId>> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        self.check_delay_secs(delay.as_secs())?;
        if delay.is_zero() {
            return self.add_bulk(payloads).await;
        }
        let now = now_ms();
        let run_at_ms = now.saturating_add(delay.as_millis() as u64);
        if run_at_ms <= now {
            return self.add_bulk(payloads).await;
        }
        self.zadd_delayed_bulk(payloads, run_at_ms).await
    }

    fn check_delay_secs(&self, delay_secs: u64) -> Result<()> {
        if self.max_delay_secs > 0 && delay_secs > self.max_delay_secs {
            return Err(Error::Config(format!(
                "delay {}s exceeds max_delay_secs {}s",
                delay_secs, self.max_delay_secs
            )));
        }
        Ok(())
    }

    async fn zadd_delayed(&self, payload: T, run_at_ms: u64) -> Result<JobId> {
        let job = Job::new(payload);
        let id = job.id.clone();
        let bytes = Bytes::from(rmp_serde::to_vec(&job)?);
        let client = self.pool.next_connected();
        let cmd = CustomCommand::new_static("ZADD", ClusterHash::FirstKey, false);
        let args = zadd_delayed_args(
            self.delayed_key.as_ref(),
            run_at_ms_as_i64(run_at_ms)?,
            bytes,
        );
        let _: Value = client.custom(cmd, args).await.map_err(Error::Redis)?;
        Ok(id)
    }

    async fn zadd_delayed_bulk(&self, payloads: Vec<T>, run_at_ms: u64) -> Result<Vec<JobId>> {
        let mut encoded: Vec<(JobId, Bytes)> = Vec::with_capacity(payloads.len());
        for payload in payloads {
            let job = Job::new(payload);
            let id = job.id.clone();
            let bytes = Bytes::from(rmp_serde::to_vec(&job)?);
            encoded.push((id, bytes));
        }

        let score = run_at_ms_as_i64(run_at_ms)?;
        let client = self.pool.next_connected();
        let pipeline = client.pipeline();
        let cmd = CustomCommand::new_static("ZADD", ClusterHash::FirstKey, false);
        for (_, bytes) in &encoded {
            let args = zadd_delayed_args(self.delayed_key.as_ref(), score, bytes.clone());
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

fn run_at_ms_as_i64(ms: u64) -> Result<i64> {
    i64::try_from(ms).map_err(|_| Error::Config(format!("run_at_ms {ms} overflows i64")))
}

fn u128_to_u64_or_err(ms: u128) -> Result<u64> {
    u64::try_from(ms).map_err(|_| Error::Config(format!("run_at_ms {ms} overflows u64")))
}
