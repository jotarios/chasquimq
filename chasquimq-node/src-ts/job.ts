// Lightweight `Job` value class for the high-level shim.
//
// v1 deliberately does NOT round-trip through Redis for any field on this
// object. The engine streams jobs via XREADGROUP / XACK and does not
// persist progress, return values, or per-job state metadata, so all the
// "lookup my job" methods are stubbed with `NotSupportedError` until a
// future slice exposes a stateful query path. Mutators that would require
// rewriting a stream entry (e.g. `update`) throw — Streams are append-only.

import type { JobsOptions, JobState, JobProgress } from "./types.js";
import { NotSupportedError, WaitForResultTimeoutError } from "./errors.js";
import type { Queue } from "./queue.js";

/**
 * Options for {@link Job.waitForResult}.
 */
export interface WaitForResultOptions {
  /**
   * Maximum total time (ms) to poll before throwing
   * {@link WaitForResultTimeoutError}. Default `30_000`.
   */
  timeoutMs?: number;
  /**
   * Polling interval (ms) between `Queue.getJobResult` calls.
   * Default `100`.
   */
  intervalMs?: number;
  /**
   * Optional `AbortSignal` to cancel the poll loop. If aborted before
   * the call begins the function throws synchronously via
   * `signal.throwIfAborted()`. Aborting mid-poll surfaces as the same
   * `AbortError` (typically `signal.reason`).
   */
  signal?: AbortSignal;
}

export class Job<
  DataType = unknown,
  ResultType = unknown,
  NameType extends string = string,
> {
  readonly id: string;
  readonly name: NameType;
  readonly data: DataType;
  readonly opts: JobsOptions;
  attemptsMade: number = 0;
  progress: JobProgress = 0;
  returnvalue?: ResultType;
  failedReason?: string;
  stacktrace: string[] = [];
  timestamp: number;
  delay: number;
  priority: number = 0;
  processedOn?: number;
  finishedOn?: number;

  /**
   * Backreference to the originating {@link Queue}, used by
   * {@link Job.waitForResult} to issue `Queue.getJobResult` polls.
   * Set automatically when the job is constructed via
   * {@link Queue.add}/{@link Queue.addBulk}; jobs constructed by the
   * worker shim (i.e. from inside a processor) leave this `undefined`,
   * and `waitForResult` throws a clear error in that case.
   */
  queue?: Queue<DataType, ResultType, NameType>;

  constructor(
    name: NameType,
    data: DataType,
    opts: JobsOptions,
    id: string,
    queue?: Queue<DataType, ResultType, NameType>,
  ) {
    this.id = id;
    this.name = name;
    this.data = data;
    this.opts = opts;
    this.timestamp = opts.timestamp ?? Date.now();
    this.delay = opts.delay ?? 0;
    this.queue = queue;
  }

  /**
   * In-memory progress update. The engine does not persist progress yet;
   * the Worker shim will surface this via its `progress` event when called
   * from inside a processor.
   */
  async updateProgress(progress: JobProgress): Promise<void> {
    this.progress = progress;
  }

  async log(_row: string): Promise<number> {
    throw new NotSupportedError("Job logs are not supported in v1");
  }

  async getState(): Promise<JobState | "unknown"> {
    return "unknown";
  }

  async remove(): Promise<void> {
    throw new NotSupportedError("Job.remove not supported in v1");
  }

  async retry(_state?: "completed" | "failed"): Promise<void> {
    throw new NotSupportedError(
      "Job.retry not supported in v1; use Queue-level replay",
    );
  }

  async discard(): Promise<void> {
    throw new NotSupportedError("Job.discard not supported in v1");
  }

  async update(_data: DataType): Promise<void> {
    throw new NotSupportedError(
      "Job.update not supported (Streams are append-only)",
    );
  }

  async updateData(d: DataType): Promise<void> {
    return this.update(d);
  }

  async isCompleted(): Promise<boolean> {
    return false;
  }
  async isFailed(): Promise<boolean> {
    return false;
  }
  async isDelayed(): Promise<boolean> {
    return this.delay > 0;
  }
  async isActive(): Promise<boolean> {
    return false;
  }
  async isWaiting(): Promise<boolean> {
    return false;
  }

  /**
   * Poll until the engine's stored result for this job becomes
   * readable, or until `timeoutMs` elapses, or until the supplied
   * `AbortSignal` fires. Returns the msgpack-decoded handler return
   * value on success, throws {@link WaitForResultTimeoutError} on
   * timeout, and re-throws the abort reason on cancel.
   *
   * **The void-handler trap.** If the producing worker resolved its
   * processor with `undefined` / `void`, *or* ran without
   * `WorkerOptions.storeResults = true`, no result key is ever
   * written. The polling loop has no way to distinguish that case from
   * "the job hasn't completed yet", so this method will time out.
   * Mirror the worker's `storeResults` config on the consumer side
   * before relying on `waitForResult`.
   *
   * **Polling cost.** Default `intervalMs = 100` is fine for one or two
   * concurrent waiters; `N` simultaneous `waitForResult` calls fan out
   * to `N` `GET` round trips per interval. For high-fanout workloads
   * (>10 concurrent waiters) subscribe to {@link QueueEvents} instead
   * — the engine emits `completed` events natively and you avoid the
   * polling tax against Redis.
   *
   * **TTL race.** A short `WorkerOptions.resultTtlMs` plus a long
   * `timeoutMs` here can race: the result expires mid-wait and this
   * method times out even though the handler succeeded. As a rule of
   * thumb keep `resultTtlMs >= timeoutMs * 2`.
   */
  async waitForResult(
    opts: WaitForResultOptions = {},
  ): Promise<ResultType | undefined> {
    if (!this.queue) {
      throw new Error(
        "Job.waitForResult requires a Queue reference; call from a Job returned by Queue.add / Queue.addBulk, not from a Worker handler",
      );
    }
    const timeoutMs = opts.timeoutMs ?? 30_000;
    const intervalMs = opts.intervalMs ?? 100;
    const signal = opts.signal;

    if (signal?.aborted) {
      // Surface the abort reason eagerly. throwIfAborted is the canonical
      // entry hook (Node 18+; matches the WHATWG spec).
      signal.throwIfAborted();
    }

    const start = Date.now();
    // Loop forever — break only on result, timeout, or abort.
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const value = await this.queue.getJobResult(this.id);
      if (value !== undefined) return value;

      const elapsed = Date.now() - start;
      if (elapsed >= timeoutMs) {
        throw new WaitForResultTimeoutError(
          `Job.waitForResult: no result for ${this.id} after ${timeoutMs}ms`,
        );
      }
      const remaining = timeoutMs - elapsed;
      const sleepMs = Math.min(intervalMs, remaining);
      await sleepWithSignal(sleepMs, signal);
    }
  }

  toJSON(): object {
    return {
      id: this.id,
      name: this.name,
      data: this.data,
      opts: this.opts,
      attemptsMade: this.attemptsMade,
      progress: this.progress,
      returnvalue: this.returnvalue,
      failedReason: this.failedReason,
      timestamp: this.timestamp,
      delay: this.delay,
      priority: this.priority,
      processedOn: this.processedOn,
      finishedOn: this.finishedOn,
    };
  }
}

/**
 * `setTimeout`-based sleep that wakes early when the supplied
 * `AbortSignal` fires. The rejection re-throws `signal.reason` so
 * callers see the standard `AbortError` shape.
 */
function sleepWithSignal(
  ms: number,
  signal: AbortSignal | undefined,
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    if (!signal) {
      setTimeout(resolve, ms);
      return;
    }
    if (signal.aborted) {
      reject(signal.reason ?? new DOMException("Aborted", "AbortError"));
      return;
    }
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", onAbort);
      reject(signal.reason ?? new DOMException("Aborted", "AbortError"));
    };
    signal.addEventListener("abort", onAbort, { once: true });
  });
}
