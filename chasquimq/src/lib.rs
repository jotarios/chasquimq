//! ChasquiMQ — Redis-backed message broker / background job queue.
//!
//! Phase 1 surface: [`Producer`], [`Consumer`], [`Job`], [`Error`], [`HandlerError`].
//!
//! Delivery semantics: **at-least-once**. A handler MAY run more than once for
//! the same job — specifically when the handler succeeds but the ack does not
//! reach Redis (process crash, network drop). The unacked entry is reclaimed
//! via `XREADGROUP ... CLAIM` and the handler runs again. Handlers MUST be
//! idempotent.

pub mod ack;
pub mod config;
pub mod consumer;
pub mod error;
pub mod job;
pub mod producer;

pub use error::{Error, HandlerError, Result};
pub use job::{Job, JobId};
