use crate::config::ConnectionTuning;
use crate::error::{Error, Result};
use fred::clients::{Client, Pool};
use fred::interfaces::ClientLike;
use fred::prelude::Config;
use fred::socket2::TcpKeepalive;
use fred::types::config::{ConnectionConfig, ReconnectPolicy, TcpConfig};
use std::time::Duration;

pub(crate) async fn connect(url: &str, tuning: &ConnectionTuning) -> Result<Client> {
    let cfg = Config::from_url(url).map_err(Error::Redis)?;
    let (conn, policy) = build_fred_slots(tuning);
    let client = Client::new(cfg, None, Some(conn), Some(policy));
    client.init().await.map_err(Error::Redis)?;
    Ok(client)
}

pub(crate) async fn connect_pool(
    url: &str,
    pool_size: usize,
    tuning: &ConnectionTuning,
) -> Result<Pool> {
    if pool_size == 0 {
        return Err(Error::Config("pool_size must be > 0".into()));
    }
    let cfg = Config::from_url(url).map_err(Error::Redis)?;
    let (conn, policy) = build_fred_slots(tuning);
    let pool = Pool::new(cfg, None, Some(conn), Some(policy), pool_size).map_err(Error::Redis)?;
    std::mem::drop(pool.init().await.map_err(Error::Redis)?);
    Ok(pool)
}

pub(crate) fn build_fred_slots(tuning: &ConnectionTuning) -> (ConnectionConfig, ReconnectPolicy) {
    let mut conn = ConnectionConfig {
        connection_timeout: Duration::from_millis(tuning.connection_timeout_ms),
        reconnect_on_auth_error: true,
        ..ConnectionConfig::default()
    };
    let mut keepalive = TcpKeepalive::new();
    if tuning.tcp_keepalive_secs > 0 {
        keepalive = keepalive.with_time(Duration::from_secs(tuning.tcp_keepalive_secs));
        if tuning.tcp_keepalive_interval_secs > 0 {
            keepalive =
                keepalive.with_interval(Duration::from_secs(tuning.tcp_keepalive_interval_secs));
        }
        conn.tcp = TcpConfig {
            keepalive: Some(keepalive),
            ..TcpConfig::default()
        };
    }

    let mut policy = ReconnectPolicy::new_exponential(
        tuning.reconnect_max_attempts,
        tuning.reconnect_min_delay_ms,
        tuning.reconnect_max_delay_ms,
        tuning.reconnect_backoff_base,
    );
    policy.set_jitter(tuning.reconnect_jitter_ms);
    (conn, policy)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_yield_exponential_policy_with_unbounded_attempts() {
        let tuning = ConnectionTuning::default();
        let (_, policy) = build_fred_slots(&tuning);
        match policy {
            ReconnectPolicy::Exponential {
                max_attempts,
                min_delay,
                max_delay,
                base,
                jitter,
                ..
            } => {
                assert_eq!(max_attempts, 0);
                assert_eq!(min_delay, 100);
                assert_eq!(max_delay, 30_000);
                assert_eq!(base, 2);
                assert_eq!(jitter, 50);
            }
            other => panic!("expected Exponential, got {other:?}"),
        }
    }

    #[test]
    fn defaults_set_tcp_keepalive() {
        let tuning = ConnectionTuning::default();
        let (conn, _) = build_fred_slots(&tuning);
        assert!(conn.tcp.keepalive.is_some());
    }

    #[test]
    fn zero_keepalive_secs_disables_keepalive() {
        let tuning = ConnectionTuning {
            tcp_keepalive_secs: 0,
            ..ConnectionTuning::default()
        };
        let (conn, _) = build_fred_slots(&tuning);
        assert!(conn.tcp.keepalive.is_none());
    }

    #[test]
    fn override_propagates_through_to_connection_config() {
        let tuning = ConnectionTuning {
            connection_timeout_ms: 5_000,
            ..ConnectionTuning::default()
        };
        let (conn, _) = build_fred_slots(&tuning);
        assert_eq!(conn.connection_timeout, Duration::from_millis(5_000));
        assert!(conn.reconnect_on_auth_error);
    }
}
