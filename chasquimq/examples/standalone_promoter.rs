use chasquimq::{Promoter, PromoterConfig};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let queue_name = std::env::var("QUEUE_NAME").unwrap_or_else(|_| "default".into());

    let cfg = PromoterConfig {
        queue_name,
        ..Default::default()
    };

    let shutdown = CancellationToken::new();
    let shutdown_signal = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        shutdown_signal.cancel();
    });

    Promoter::new(redis_url, cfg).run(shutdown).await?;
    Ok(())
}
