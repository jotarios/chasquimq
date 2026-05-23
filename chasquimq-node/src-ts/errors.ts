// Error classes used by the high-level Queue / Job / Worker shim.
//
// Throwing a typed error (instead of a plain `Error`) lets application
// callers branch on `err.name === 'NotSupportedError'` to detect surface
// gaps that exist while the shim is being built out.

export class NotSupportedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "NotSupportedError";
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
    super(message ?? "Unrecoverable");
    this.name = "UnrecoverableError";
  }
}

export class RateLimitError extends Error {
  constructor(message?: string) {
    super(message ?? "Rate limited");
    this.name = "RateLimitError";
  }
}

/**
 * Thrown by {@link Job.waitForResult} when the polling loop's
 * `timeoutMs` elapses without the result key becoming visible.
 *
 * Distinct from a successful `undefined` resolution: a handler that
 * returned `undefined` (or a worker running with `storeResults=false`)
 * will *also* time out, because there is no way for the polling loop
 * to distinguish "result was never written" from "result not yet
 * written" — see {@link Job.waitForResult} for the full table.
 *
 * Detect via `err.name === 'WaitForResultTimeoutError'` rather than
 * `err instanceof WaitForResultTimeoutError` so subclasses across
 * realms (workers / vm contexts) still match.
 */
export class WaitForResultTimeoutError extends Error {
  constructor(message?: string) {
    super(message ?? "waitForResult timed out");
    this.name = "WaitForResultTimeoutError";
  }
}

/**
 * Thrown by {@link Job.waitUntilFinished} when neither a `completed` nor
 * a `failed` event for the watched job arrives within the supplied
 * `ttl` ms. Distinct from a failed job: a failed job rejects with a
 * regular `Error` carrying the engine-reported `failedReason`. This
 * error fires only when the events stream itself goes silent (the worker
 * died, the network blipped, or the `ttl` was too short for the
 * handler).
 *
 * Detect via `err.name === 'WaitUntilFinishedTimeoutError'` rather than
 * `err instanceof WaitUntilFinishedTimeoutError` so subclasses across
 * realms (workers / vm contexts) still match.
 */
export class WaitUntilFinishedTimeoutError extends Error {
  constructor(message?: string) {
    super(message ?? "waitUntilFinished timed out");
    this.name = "WaitUntilFinishedTimeoutError";
  }
}
