#![deny(clippy::all)]
use napi_derive::napi;

/// Returns the version of the underlying chasquimq engine crate.
#[napi]
pub fn engine_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}
