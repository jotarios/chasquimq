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

/// Error type returned from a handler.
///
/// Handlers signal failure to the consumer by returning
/// `Err(HandlerError::new(err))`. By default the consumer treats every
/// returned error as recoverable: it bumps the attempt counter, applies
/// the configured backoff, and reschedules the job until the
/// queue-wide / per-job `max_attempts` budget is exhausted (after which
/// the job lands in the DLQ with [`crate::metrics::DlqReason::RetriesExhausted`]).
///
/// When a handler knows the failure is terminal — bad input that no
/// number of retries will fix, a permission error, a poison-pill —
/// it can short-circuit the retry loop by constructing the error with
/// [`HandlerError::unrecoverable`]. The consumer routes such errors
/// straight to the DLQ with [`crate::metrics::DlqReason::Unrecoverable`],
/// regardless of the remaining attempt budget. The handler still runs
/// exactly once for that delivery; subsequent CLAIM-recovered deliveries
/// of the same stream entry would re-invoke the handler in the normal way.
#[derive(thiserror::Error, Debug)]
#[error("handler: {inner}")]
pub struct HandlerError {
    inner: Box<dyn std::error::Error + Send + Sync>,
    unrecoverable: bool,
}

impl HandlerError {
    /// Construct a recoverable handler error. The consumer will bump
    /// the attempt counter and reschedule per the queue's retry config.
    pub fn new<E>(err: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self {
            inner: Box::new(err),
            unrecoverable: false,
        }
    }

    /// Construct an unrecoverable handler error. The consumer will skip
    /// the retry path and route the job directly to the DLQ with
    /// [`crate::metrics::DlqReason::Unrecoverable`] — no matter what
    /// the queue-wide or per-job `max_attempts` budget is.
    ///
    /// **When NOT to use**: reach for [`HandlerError::new`] for transient
    /// failures (network timeouts, rate limits, deadlocks, lock
    /// contention, stale-data conflicts) — those typically clear up on a
    /// fresh attempt. Reserve `unrecoverable()` for failures where
    /// retrying is provably wasteful: validation errors, missing
    /// dependencies, permission denials, poison-pill payloads, or
    /// programmer-guaranteed terminal conditions.
    pub fn unrecoverable<E>(err: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self {
            inner: Box::new(err),
            unrecoverable: true,
        }
    }

    /// Whether this error was constructed via [`HandlerError::unrecoverable`].
    pub fn is_unrecoverable(&self) -> bool {
        self.unrecoverable
    }

    /// Borrow the wrapped error for logging / inspection.
    pub fn source_err(&self) -> &(dyn std::error::Error + Send + Sync) {
        self.inner.as_ref()
    }

    /// Consume the wrapper and return the boxed source error.
    pub fn into_source(self) -> Box<dyn std::error::Error + Send + Sync> {
        self.inner
    }
}
