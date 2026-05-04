// Error classes used by the high-level Queue / Job / Worker shim.
//
// Throwing a typed error (instead of a plain `Error`) lets application
// callers branch on `err.name === 'NotSupportedError'` to detect surface
// gaps that exist while the shim is being built out.

export class NotSupportedError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'NotSupportedError'
  }
}

/**
 * Thrown from a `Worker` processor to signal a *terminal* failure that
 * no number of retries will fix (bad input, permission denied, a
 * poison-pill payload, etc.). The native binding maps a rejection whose
 * `error.name === 'UnrecoverableError'` to `HandlerError::unrecoverable(...)`
 * on the Rust side, which causes the engine to route the job straight
 * to the DLQ with `DlqReason::Unrecoverable` regardless of the queue's
 * `maxAttempts` budget.
 *
 * **Contract:**
 * - `name` MUST be `'UnrecoverableError'` (set by the constructor; do not
 *   override on subclasses).
 * - The handler still runs exactly once for that delivery; the
 *   `failed` event fires with this error before it propagates.
 * - The job is **not** re-queued. CLAIM-recovery on a separate stream
 *   delivery (e.g. after a worker crash before XACK) would still call
 *   the handler again — `UnrecoverableError` is a per-handler-call
 *   signal, not a per-job idempotent latch.
 */
export class UnrecoverableError extends Error {
  constructor(message?: string) {
    super(message ?? 'Unrecoverable')
    this.name = 'UnrecoverableError'
  }
}

export class RateLimitError extends Error {
  constructor(message?: string) {
    super(message ?? 'Rate limited')
    this.name = 'RateLimitError'
  }
}
