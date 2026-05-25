// Lightweight `Job` value class for the high-level shim.
//
// v1 deliberately does NOT round-trip through Redis for any field on this
// object. The engine streams jobs via XREADGROUP / XACK and does not
// persist progress, return values, or per-job state metadata, so all the
// "lookup my job" methods are stubbed with `NotSupportedError` until a
// future slice exposes a stateful query path. Mutators that would require
// rewriting a stream entry (e.g. `update`) throw — Streams are append-only.

import type { JobsOptions, JobState, JobProgress } from "./types.js";
import type { Job as NativeJob } from "../index.js";
import {
  NotSupportedError,
  WaitForResultTimeoutError,
  WaitUntilFinishedTimeoutError,
} from "./errors.js";
import type { Queue } from "./queue.js";
import type { QueueEvents } from "./queue-events.js";

/**
 * Read-only `Job` guard. Thrown by {@link Job.updateProgress} and
 * {@link Job.log} when the instance has no native handle backref —
 * `Queue.getJob()` / `Queue.getJobs()` return synthesized `Job`s
 * built from introspector data, with no per-handler connection. The
 * write path requires the engine's per-dispatch `JobHandle`, which
 * is attached only when the consumer hands a job to a `Worker`
 * processor. Catch via `err.name === 'ReadOnlyJobError'`.
 */
const READ_ONLY_PROGRESS_MSG =
  "Job.updateProgress() requires the Job be passed to your Worker handler; " +
  "Jobs returned by Queue.getJob() are read-only";
const READ_ONLY_LOG_MSG =
  "Job.log() requires the Job be passed to your Worker handler; " +
  "Jobs returned by Queue.getJob() are read-only";

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

  /**
   * Live native handle for in-handler progress + log writes. Set only
   * when the engine's worker dispatched this `Job` to a processor —
   * `Queue.getJob()` / `Queue.add()` paths leave this `undefined`, and
   * `updateProgress` / `log` throw a clear "read-only Job" error to
   * surface the contract violation early.
   */
  private _native?: NativeJob;

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
   * @internal — wires the native `Job` handle in before the user
   * processor sees this instance. The high-level `Worker` shim is the
   * only call site; application code should not touch this.
   */
  _attachNative(native: NativeJob): void {
    this._native = native;
  }

  /**
   * Persist a `0..=100` progress value for this job under the engine's
   * per-job progress key, mirror it on the local `progress` field, and
   * (when `WorkerOptions.eventsProgressEnabled !== false`) emit an
   * `e=progress` events-stream entry that `QueueEvents` re-fans onto
   * the broadcast `'progress'` channel and the per-id
   * `'progress:<jobId>'` channel.
   *
   * Values outside `0..=100` are clamped to `100` at the engine
   * boundary (no throw). Throws when called on a Job returned by
   * {@link Queue.getJob} / {@link Queue.getJobs} — those instances are
   * synthesized from introspector data and carry no per-handler
   * connection; only Jobs handed to a `Worker` processor have a live
   * backref.
   */
  async updateProgress(progress: JobProgress): Promise<void> {
    if (!this._native) {
      throw new Error(READ_ONLY_PROGRESS_MSG);
    }
    const n = Math.max(0, Math.floor(progress));
    await this._native.updateProgress(n);
    this.progress = progress;
  }

  /**
   * Append `line` to the per-job log stream
   * (`{chasqui:<queue>}:log:<id>`) and return the new XLEN. Oversize
   * lines are truncated on a UTF-8 char boundary with a
   * `[…truncated]` marker; the per-line cap is set by
   * `WorkerOptions.logMaxLineBytes` (default 4096). Read back via
   * {@link Queue.getJobLogs}.
   *
   * Same read-only Job guard as {@link Job.updateProgress}.
   */
  async log(line: string): Promise<number> {
    if (!this._native) {
      throw new Error(READ_ONLY_LOG_MSG);
    }
    return this._native.log(line);
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

  /**
   * Subscribe to the engine's events stream and resolve / reject when
   * the `completed` or `failed` event for **this** job fires.
   *
   * Unlike {@link Job.waitForResult}, this method is event-driven (no
   * polling, no Redis `GET` per interval) and does not require
   * `WorkerOptions.storeResults = true` to detect completion. It does,
   * however, require an attached {@link QueueEvents} subscriber so the
   * events-stream traffic actually reaches this process.
   *
   * Return value semantics:
   * - On `completed`, resolves with the handler's return value when
   *   `WorkerOptions.storeResults = true` was set on the worker. The
   *   value is fetched via `Queue.getJobResult(this.id)` after the
   *   event fires. If `storeResults` was not enabled (or the handler
   *   returned `undefined`), resolves with `undefined`. The events
   *   stream itself never carries the return value — keeping subscriber
   *   traffic small and predictable.
   * - On `failed`, rejects with `new Error(failedReason)` carrying the
   *   engine-reported reason (the same string surfaced on the
   *   `Worker`'s `failed` event).
   * - On `ttl` elapse, rejects with {@link WaitUntilFinishedTimeoutError}.
   *
   * **Race window.** If the job completed (or failed) *before* this
   * call wires up its listeners, the events-stream event has already
   * been dispatched and this method has nothing to subscribe to. The
   * `ttl` will fire normally. For producers that want to await a job
   * that may already have finished, pair this with a `getJobState`
   * check, or use {@link Job.waitForResult} (which can read a
   * persisted result key written before the wait started).
   *
   * @param queueEvents A live {@link QueueEvents} subscriber for the
   * same queue this job was added to. The caller owns the subscriber
   * (create one per process; share it across all `waitUntilFinished`
   * calls).
   * @param ttl Optional timeout in milliseconds. Omit for an unbounded
   * wait — practical only when you control the worker and trust it to
   * either complete or fail every job.
   */
  async waitUntilFinished(
    queueEvents: QueueEvents,
    ttl?: number,
  ): Promise<ResultType | undefined> {
    const jobId = this.id;
    // Surface a queue/queueEvents mismatch up-front rather than
    // letting the wait silently time out (the events stream is
    // per-queue, so a `QueueEvents` for a different queue will never
    // fire the per-id channel). Only checked when we have the queue
    // backref; jobs constructed without one (the worker-side path,
    // which has no queue handle) skip the guard — that's also the
    // path where calling `waitUntilFinished` is unusual.
    if (this.queue && queueEvents.name !== this.queue.name) {
      throw new Error(
        `Job.waitUntilFinished: queueEvents is for "${queueEvents.name}" ` +
          `but this job is on "${this.queue.name}" — pass a QueueEvents ` +
          `subscribed to the right queue`,
      );
    }
    const completedChannel = `completed:${jobId}`;
    const failedChannel = `failed:${jobId}`;

    return new Promise<ResultType | undefined>((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined;

      const onCompleted = (
        _args: { jobId: string; name: string; returnvalue: unknown },
      ): void => {
        cleanup();
        // Fetch the stored result on a best-effort basis. The engine
        // emits the `completed` event *before* the per-entry
        // JOB_OK_SCRIPT writes the result key (the events emit is
        // off the ack hot path, the result write is on it), so a
        // single `getJobResult` immediately after the event can lose
        // the race. Poll a few times with short backoff to give the
        // result writer a chance to land. If `storeResults` was
        // disabled on the worker, every poll returns `undefined` and
        // we fall through to resolve(undefined) — same shape as
        // jobs whose handler explicitly returned undefined.
        const queue = this.queue;
        if (!queue) {
          resolve(undefined);
          return;
        }
        void (async () => {
          for (let i = 0; i < 10; i++) {
            try {
              const v = await queue.getJobResult(jobId);
              if (v !== undefined) {
                resolve(v);
                return;
              }
            } catch {
              resolve(undefined);
              return;
            }
            await new Promise<void>((r) => setTimeout(r, 50));
          }
          resolve(undefined);
        })();
      };

      const onFailed = (args: { jobId: string; failedReason: string }): void => {
        cleanup();
        reject(new Error(args.failedReason || "job failed"));
      };

      const onTimeout = (): void => {
        cleanup();
        reject(
          new WaitUntilFinishedTimeoutError(
            `Job.waitUntilFinished: no terminal event for ${jobId} after ${ttl}ms`,
          ),
        );
      };

      const cleanup = (): void => {
        if (timer !== undefined) clearTimeout(timer);
        queueEvents.removeListener(completedChannel, onCompleted);
        queueEvents.removeListener(failedChannel, onFailed);
      };

      queueEvents.once(completedChannel, onCompleted);
      queueEvents.once(failedChannel, onFailed);
      if (ttl !== undefined && ttl > 0) {
        timer = setTimeout(onTimeout, ttl);
      }
    });
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
