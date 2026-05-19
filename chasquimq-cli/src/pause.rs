use anyhow::{Context, Result};
use chasquimq::{Producer, ProducerConfig};

/// Durably pause every consumer of `queue` by setting the cross-process
/// `{chasqui:<queue>}:paused` key. Consumers park at their next batch
/// boundary; in-flight jobs drain; producers keep enqueueing. The pause
/// has no TTL — it persists (across consumer restarts) until `resume`.
pub async fn pause(redis_url: &str, queue: &str) -> Result<()> {
    let producer = connect(redis_url, queue).await?;
    producer.pause().await.context("pause failed")?;
    println!("queue {queue} paused; run `chasqui resume {queue}` to resume");
    Ok(())
}

/// Lift a durable pause set by `pause`. Each consumer resumes within its
/// configured pause-poll window. Idempotent (no-op if not paused).
pub async fn resume(redis_url: &str, queue: &str) -> Result<()> {
    let producer = connect(redis_url, queue).await?;
    producer.resume().await.context("resume failed")?;
    println!("queue {queue} resumed");
    Ok(())
}

async fn connect(redis_url: &str, queue: &str) -> Result<Producer<rmpv::Value>> {
    let cfg = ProducerConfig {
        queue_name: queue.to_string(),
        ..Default::default()
    };
    Producer::<rmpv::Value>::connect(redis_url, cfg)
        .await
        .map_err(|e| anyhow::anyhow!("redis connect failed: {e}"))
}
