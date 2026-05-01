//! Engine error types.

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("redis: {0}")]
    Redis(#[from] fred::error::Error),
    #[error("encode: {0}")]
    Encode(#[from] rmp_serde::encode::Error),
    #[error("decode: {0}")]
    Decode(#[from] rmp_serde::decode::Error),
    #[error("config: {0}")]
    Config(String),
    #[error("shutdown")]
    Shutdown,
}

#[derive(thiserror::Error, Debug)]
#[error("handler: {0}")]
pub struct HandlerError(pub Box<dyn std::error::Error + Send + Sync>);
