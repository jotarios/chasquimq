use crate::error::{Error, Result};
use fred::clients::{Client, Pool};
use fred::interfaces::ClientLike;
use fred::prelude::Config;

pub(crate) async fn connect(url: &str) -> Result<Client> {
    let cfg = Config::from_url(url).map_err(Error::Redis)?;
    let client = Client::new(cfg, None, None, None);
    client.init().await.map_err(Error::Redis)?;
    Ok(client)
}

pub(crate) async fn connect_pool(url: &str, pool_size: usize) -> Result<Pool> {
    if pool_size == 0 {
        return Err(Error::Config("pool_size must be > 0".into()));
    }
    let cfg = Config::from_url(url).map_err(Error::Redis)?;
    let pool = Pool::new(cfg, None, None, None, pool_size).map_err(Error::Redis)?;
    std::mem::drop(pool.init().await.map_err(Error::Redis)?);
    Ok(pool)
}
