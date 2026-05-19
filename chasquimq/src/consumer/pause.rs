//! In-process pause control for the consumer's stream reader.
//!
//! `PauseControl` is the process-local half of pause/resume. It is shared
//! by `Arc` between the public `Consumer` handle and the reader loop,
//! mirroring exactly how the shutdown `CancellationToken` is shared.
//!
//! The cross-process half (the `{chasqui:<queue>}:paused` Redis key, set
//! by `chasqui pause` / `Queue.pause()`) is observed independently by the
//! reader at batch boundaries; see `consumer::reader`.
//!
//! ## Why `watch::channel(bool)` and not `AtomicBool` + `Notify`
//!
//! `watch` carries the state AND the wake in one edge-triggered channel.
//! That eliminates the lost-wakeup race a separate flag+notify pair has
//! (flip the flag, then signal — a waiter that read the old flag and is
//! about to await the signal can miss it). `watch::Receiver::changed()`
//! is cancellation-safe in a `select!`, and `borrow()` is an atomic read
//! of the current value. One source of truth, no two-field skew.
//!
//! ```text
//!  pause()  ──send(true)──►  watch ◄──changed()──  reader park loop
//!  resume() ──send(false)─►        │
//!  is_paused() ─borrow()──►        └── *borrow() == current pause state
//! ```

use tokio::sync::watch;

/// Process-local pause switch for one `Consumer`'s reader.
///
/// `pause()` / `resume()` are idempotent: pausing an already-paused
/// control (or resuming an already-running one) is a no-op and does not
/// spuriously wake the reader (`watch::Sender::send` only marks changed
/// when the value actually differs, via `send_if_modified`).
pub struct PauseControl {
    tx: watch::Sender<bool>,
}

impl PauseControl {
    /// Construct an unpaused control.
    pub fn new() -> Self {
        let (tx, _rx) = watch::channel(false);
        Self { tx }
    }

    /// Park the reader at its next batch boundary. In-flight jobs already
    /// dispatched still run to completion; producers are unaffected.
    /// Idempotent.
    pub fn pause(&self) {
        self.set(true);
    }

    /// Resume reading. The parked reader wakes immediately (edge-triggered;
    /// no `pause_poll_ms` latency for the in-process path). Idempotent.
    pub fn resume(&self) {
        self.set(false);
    }

    /// Current in-process pause state. Does not reflect the cross-process
    /// Redis key — a consumer can be running here yet parked because
    /// `chasqui pause` set the durable key.
    pub fn is_paused(&self) -> bool {
        *self.tx.borrow()
    }

    /// Subscribe a reader to state changes. The returned receiver's
    /// `changed()` fires only on real transitions.
    pub(crate) fn subscribe(&self) -> watch::Receiver<bool> {
        self.tx.subscribe()
    }

    fn set(&self, paused: bool) {
        // `send_if_modified` only marks the channel changed (and wakes
        // waiters) when the value actually flips, so double-pause /
        // double-resume never produces a spurious wake.
        self.tx.send_if_modified(|cur| {
            if *cur == paused {
                false
            } else {
                *cur = paused;
                true
            }
        });
    }
}

impl Default for PauseControl {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_unpaused() {
        let p = PauseControl::new();
        assert!(!p.is_paused());
    }

    #[test]
    fn pause_then_resume_round_trips() {
        let p = PauseControl::new();
        p.pause();
        assert!(p.is_paused());
        p.resume();
        assert!(!p.is_paused());
    }

    #[test]
    fn double_pause_and_double_resume_are_idempotent() {
        let p = PauseControl::new();
        p.pause();
        p.pause();
        assert!(p.is_paused());
        p.resume();
        p.resume();
        assert!(!p.is_paused());
    }

    #[tokio::test]
    async fn changed_fires_on_transition_only() {
        let p = PauseControl::new();
        let mut rx = p.subscribe();

        p.pause();
        rx.changed().await.unwrap();
        assert!(*rx.borrow_and_update());

        // A second pause is a no-op: no pending change.
        p.pause();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), rx.changed())
                .await
                .is_err(),
            "double-pause must not produce a spurious wake"
        );

        p.resume();
        rx.changed().await.unwrap();
        assert!(!*rx.borrow_and_update());
    }

    #[tokio::test]
    async fn changed_errs_when_all_senders_dropped() {
        let rx = {
            let p = PauseControl::new();
            p.subscribe()
        };
        let mut rx = rx;
        // Sender dropped with the PauseControl → changed() resolves Err.
        assert!(rx.changed().await.is_err());
    }
}
