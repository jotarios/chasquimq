// High-level `BackoffSpec` builder — mirror of the Python shim's
// `BackoffSpec` dataclass. Returns a plain `BackoffOptions` object that
// `Queue.add(name, data, { backoff })` already accepts, so it's a thin
// ergonomic wrapper over the existing wire shape.
//
// Power users calling the unwrapped native producer want the wire-format
// `NativeBackoffSpec` (re-exported as a type from `chasquimq`); this
// class is the high-level equivalent for `Queue.add` callers who'd
// rather not hand-roll the `{ type, delay, ... }` literal.

import type { BackoffOptions } from "./types.js";

/**
 * Builder for {@link BackoffOptions}. Mirrors `chasquimq.BackoffSpec` on
 * the Python shim — same factory names, same field meanings.
 *
 * ```ts
 * import { Queue, BackoffSpec } from "chasquimq";
 *
 * await queue.add("send-email", payload, {
 *   attempts: 5,
 *   backoff: BackoffSpec.exponential(1_000, { maxDelayMs: 30_000 }),
 * });
 * ```
 *
 * The returned object is a {@link BackoffOptions} literal; you can also
 * hand-write that shape (`{ type: 'fixed', delay: 1000 }`) without going
 * through this builder. Provided for symmetry with Python.
 */
export class BackoffSpec {
  /**
   * Fixed delay between retries (no exponent). `delayMs` is the wait
   * time in milliseconds between every attempt; `jitterMs` (when set)
   * applies a symmetric ±jitter so retries don't synchronize across
   * the worker pool.
   */
  static fixed(delayMs: number, opts: { jitterMs?: number } = {}): BackoffOptions {
    const out: BackoffOptions = { type: "fixed", delay: delayMs };
    if (opts.jitterMs != null) out.jitterMs = opts.jitterMs;
    return out;
  }

  /**
   * Exponential backoff: `delayMs * multiplier^(attempt-1)` capped at
   * `maxDelayMs`. Default `multiplier = 2`. `jitterMs` (when set) is
   * applied symmetrically per attempt.
   */
  static exponential(
    delayMs: number,
    opts: { multiplier?: number; maxDelayMs?: number; jitterMs?: number } = {},
  ): BackoffOptions {
    const out: BackoffOptions = {
      type: "exponential",
      delay: delayMs,
      multiplier: opts.multiplier ?? 2,
    };
    if (opts.maxDelayMs != null) out.maxDelay = opts.maxDelayMs;
    if (opts.jitterMs != null) out.jitterMs = opts.jitterMs;
    return out;
  }
}
