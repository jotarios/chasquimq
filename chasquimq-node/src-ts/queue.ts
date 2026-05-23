// High-level Queue class — the JS-friendly entry point that turns
// `queue.add(name, data, opts)` into a single native producer call.
//
// The MVP wires `add` and `addBulk` (with delayed / idempotent jobId
// variants) through the native binding. Everything else throws
// `NotSupportedError` until a future slice exposes the matching engine
// surface; this keeps the public types stable while we iterate.

import "./_dispose-polyfill.js";
import {
  Producer as NativeProducer,
  Introspector as NativeIntrospector,
  type ProducerOpts as NativeProducerOpts,
  type AddOptions as NativeAddOptions,
  type BackoffSpec as NativeBackoffSpec,
  type DlqEntry as NativeDlqEntry,
  type IntrospectorOpts as NativeIntrospectorOpts,
  type JobCounts as NativeJobCounts,
  type JobInfo as NativeJobInfo,
  type JobsPage as NativeJobsPage,
  type JobRetryOverride as NativeJobRetryOverride,
  type RemovalReport,
} from "../index.js";
import type {
  BackoffOptions,
  ConnectionOptions,
  JobsOptions,
  BulkJobOptions,
  QueueOptions,
  JobState,
  JobType,
  MissedFiresOption,
  RepeatOptions,
  RepeatableJobMeta,
} from "./types.js";
import { NotSupportedError } from "./errors.js";
import { Job } from "./job.js";
import { decodePayload, encodePayload } from "./encoding.js";

/** Per-surface result of {@link Queue.removeReport}. Re-exported from the
 * native binding so callers can type the return value without reaching
 * into `../index.js`. */
export type { RemovalReport } from "../index.js";

let warnedPriority = false;
let warnedLifo = false;

export class Queue<
  DataType = unknown,
  ResultType = unknown,
  NameType extends string = string,
> {
  readonly name: string;
  readonly opts: QueueOptions;
  private producerPromise?: Promise<NativeProducer>;
  private introspectorPromise?: Promise<NativeIntrospector>;
  private closed = false;

  constructor(name: string, opts: QueueOptions) {
    this.name = name;
    this.opts = opts;
  }

  /**
   * `true` after the first {@link Queue.close} call. Mirrors
   * `Queue.is_closed` on the Python shim.
   */
  get isClosed(): boolean {
    return this.closed;
  }

  private async producer(): Promise<NativeProducer> {
    if (!this.producerPromise) {
      const url = buildRedisUrl(this.opts.connection);
      const native: NativeProducerOpts = {
        queueName: this.name,
        reconnectMaxAttempts: this.opts.connection.reconnectMaxAttempts,
      };
      // `connection.credentialProvider` is optional; passing `undefined`
      // through to the native binding routes to the
      // `Option<ThreadsafeFunction<...>>` -> `None` branch, leaving the
      // engine on its default auth-from-URL path. The native binding
      // ignores `null` vs `undefined`; both collapse to `None`.
      this.producerPromise = NativeProducer.connect(
        url,
        native,
        this.opts.connection.credentialProvider,
      );
    }
    return this.producerPromise;
  }

  private async introspector(): Promise<NativeIntrospector> {
    if (!this.introspectorPromise) {
      const url = buildRedisUrl(this.opts.connection);
      const native: NativeIntrospectorOpts = {
        queueName: this.name,
        consumerGroup: this.opts.consumerGroup,
        reconnectMaxAttempts: this.opts.connection.reconnectMaxAttempts,
      };
      this.introspectorPromise = NativeIntrospector.connect(
        url,
        native,
        this.opts.connection.credentialProvider,
      );
    }
    return this.introspectorPromise;
  }

  async add(
    name: NameType,
    data: DataType,
    opts: JobsOptions = {},
  ): Promise<Job<DataType, ResultType, NameType>> {
    const merged: JobsOptions = {
      ...(this.opts.defaultJobOptions ?? {}),
      ...opts,
    };

    if (merged.repeat) {
      return await this.upsertRepeatableJob(name, data, merged);
    }
    // Defense in depth: TS types nest `missedFires` under `RepeatOptions`,
    // but `Queue.add(name, data, { missedFires } as any)` would silently
    // pass through. Mirror Python's `ValueError("missed_fires is only
    // meaningful with repeat...")`.
    if ((merged as { missedFires?: unknown }).missedFires !== undefined) {
      throw new Error(
        "missedFires is only meaningful with `repeat`; pass it as { repeat: { missedFires } }",
      );
    }
    if (merged.parent) {
      throw new NotSupportedError("Parent/child flows are not supported");
    }
    if (merged.priority != null && merged.priority !== 0 && !warnedPriority) {
      console.warn(
        "[chasquimq] JobsOptions.priority is ignored (FIFO Streams). Set to 0 to silence this warning.",
      );
      warnedPriority = true;
    }
    if (merged.lifo === true && !warnedLifo) {
      console.warn("[chasquimq] JobsOptions.lifo is ignored (FIFO Streams).");
      warnedLifo = true;
    }

    if (merged.delay !== undefined) {
      if (!Number.isFinite(merged.delay)) {
        throw new RangeError(
          `delay must be a finite number, got ${merged.delay}`,
        );
      }
      if (merged.delay < 0) {
        throw new RangeError(`delay must be non-negative, got ${merged.delay}`);
      }
    }

    if (merged.jobId !== undefined) {
      if (
        typeof merged.jobId !== "string" ||
        merged.jobId.trim().length === 0
      ) {
        throw new TypeError(
          "Queue.add: opts.jobId must be a non-empty, non-whitespace string",
        );
      }
    }

    const isDelayed = !!(merged.delay && merged.delay > 0);
    const retryOverride = buildRetryOverride(merged);
    const nativeOpts = buildNativeAddOptions(
      merged.jobId,
      retryOverride,
      name as string,
    );

    const buf = encodePayload(data);
    const producer = await this.producer();
    let id: string;
    if (isDelayed) {
      if (nativeOpts) {
        id = await producer.addInWithOptions(merged.delay!, buf, nativeOpts);
      } else {
        id = await producer.addIn(merged.delay!, buf);
      }
    } else if (nativeOpts) {
      id = await producer.addWithOptions(buf, nativeOpts);
    } else {
      id = await producer.add(buf);
    }
    return new Job(name, data, merged, id, this);
  }

  /**
   * Idempotent variant of {@link Queue.add}. Requires `opts.jobId`; throws
   * `TypeError` when missing. Otherwise identical to `add(name, data, opts)`.
   *
   * Idempotency guarantees differ by path:
   * - **Delayed** (`delay > 0`) — strict and cross-process. Re-enqueueing
   *   the same `jobId` while the dedup marker is still alive is a no-op at
   *   Redis (Lua `SET NX EX` on `{chasqui:<queue>}:dlid:<jobId>` gates the
   *   `ZADD`). The marker TTL outlives the fire time by 1h so a
   *   producer-retry can't race a successful promotion. Two different
   *   `Queue` instances calling `addUnique` with the same id will only
   *   schedule once.
   * - **Immediate** (no `delay`) — strict within a single `Queue` instance,
   *   not across instances. Redis 8.6 `XADD IDMP <producer_id> <jobId>`
   *   dedups at the wire layer, but the IDMP scope is the producer id
   *   (one per `Queue`). For cross-process idempotency on the immediate
   *   path, give all callers the same `jobId` *and* use a `delay` so the
   *   delayed-path SET-NX-EX guard kicks in.
   *
   * Immediate-path dedup is also bounded by the stream's `IDMP-MAXSIZE`
   * LRU; high-cardinality `jobId` workloads may silently lose dedup for
   * the oldest entries.
   *
   * A `Producer` mints a new UUID on each construction (process restart,
   * `new Producer(...)`); immediate-path dedup is therefore not preserved
   * across producer instances even with the same `jobId`. For
   * cross-process / cross-restart strict dedup, use `delay > 0`
   * (delayed-path uses cross-process Lua dedup on the `:dlid:<job_id>`
   * key).
   */
  async addUnique(
    name: NameType,
    data: DataType,
    opts: JobsOptions = {},
  ): Promise<Job<DataType, ResultType, NameType>> {
    if (typeof opts.jobId !== "string" || opts.jobId.trim().length === 0) {
      throw new TypeError(
        "Queue.addUnique: opts.jobId must be a non-empty, non-whitespace string",
      );
    }
    return await this.add(name, data, opts);
  }

  async addBulk(
    jobs: Array<{ name: NameType; data: DataType; opts?: BulkJobOptions }>,
  ): Promise<Job<DataType, ResultType, NameType>[]> {
    if (jobs.length === 0) return [];
    if (jobs.some((j) => j.opts?.parent)) {
      throw new NotSupportedError("Parent options not supported in addBulk");
    }
    for (const j of jobs) {
      const d = j.opts?.delay;
      if (d !== undefined) {
        if (!Number.isFinite(d)) {
          throw new RangeError(`delay must be a finite number, got ${d}`);
        }
        if (d < 0) {
          throw new RangeError(`delay must be non-negative, got ${d}`);
        }
      }
    }
    // For v1: route through native add_bulk only when no per-job
    // delay / jobId / attempts / backoff. Anything else falls back to the
    // per-entry add() loop below — losing the bulk pipelining win.
    const allSimple = jobs.every((j) => {
      const o = j.opts ?? {};
      return !o.delay && !o.jobId && !o.attempts && !o.backoff;
    });
    if (allSimple) {
      const named = jobs.map((j) => ({
        name: j.name as string,
        payload: encodePayload(j.data),
      }));
      const producer = await this.producer();
      const ids = await producer.addBulkNamed(named);
      return jobs.map(
        (j, i) =>
          new Job(
            j.name,
            j.data,
            { ...(this.opts.defaultJobOptions ?? {}), ...(j.opts ?? {}) },
            ids[i]!,
            this,
          ),
      );
    }
    // Mixed path: per-entry add(). Loses bulk pipelining.
    const out: Job<DataType, ResultType, NameType>[] = [];
    for (const j of jobs) {
      out.push(await this.add(j.name, j.data, j.opts as JobsOptions));
    }
    return out;
  }

  private async upsertRepeatableJob(
    name: NameType,
    data: DataType,
    merged: JobsOptions,
  ): Promise<Job<DataType, ResultType, NameType>> {
    const repeat = merged.repeat as RepeatOptions;
    const pattern = translateRepeatPattern(repeat);
    const buf = encodePayload(data);
    const startAfterMs = coerceDateLike(repeat.startDate);
    const endBeforeMs = coerceDateLike(repeat.endDate);
    const producer = await this.producer();
    const resolvedKey = await producer.upsertRepeatable({
      key: merged.repeatJobKey ?? "",
      jobName: name,
      pattern,
      payload: buf,
      limit: repeat.limit,
      startAfterMs,
      endBeforeMs,
      missedFires: translateMissedFires(repeat.missedFires),
    });
    // The repeatable upsert is a *spec*, not a job invocation; the engine
    // mints a fresh ULID for each fire. Returning a Job here gives callers
    // a stable handle (the resolved spec key as `id`) to pair with
    // `Queue.removeRepeatableByKey(job.id)` for symmetry with the
    // single-add API. The `id` shape is therefore intentionally **not** a
    // ULID for repeatable upserts.
    return new Job(name, data, merged, resolvedKey, this);
  }

  // --- Repeatable / cron jobs (engine slice 10) ---

  /**
   * List repeatable specs ordered by next fire time, ascending.
   *
   * Returns up to `limit` entries (default 100). The wire size is small —
   * payloads are intentionally not included; call `Queue.add(name, data,
   * { repeat })` to inspect or modify a spec's payload.
   */
  async getRepeatableJobs(limit: number = 100): Promise<RepeatableJobMeta[]> {
    const producer = await this.producer();
    const metas = await producer.listRepeatable(limit);
    return metas.map((m): RepeatableJobMeta => {
      const base: RepeatableJobMeta = {
        key: m.key,
        jobName: m.jobName,
        patternKind: m.pattern.kind === "cron" ? "cron" : "every",
        nextFireMs: m.nextFireMs,
        limit: m.limit,
        startAfterMs: m.startAfterMs,
        endBeforeMs: m.endBeforeMs,
      };
      if (m.pattern.kind === "cron") {
        base.pattern = m.pattern.expression ?? undefined;
        base.tz = m.pattern.tz ?? undefined;
      } else {
        base.every = m.pattern.intervalMs ?? undefined;
      }
      if (m.missedFires) {
        if (m.missedFires.kind === "fire-once") {
          base.missedFires = { kind: "fire-once" };
        } else if (m.missedFires.kind === "fire-all") {
          base.missedFires = {
            kind: "fire-all",
            maxCatchup: m.missedFires.maxCatchup ?? 0,
          };
        }
      }
      return base;
    });
  }

  /**
   * Remove a repeatable spec by its resolved key. Returns `true` if a spec
   * was removed, `false` if no spec with that key existed.
   *
   * The resolved key is what {@link Queue.add} returns (via the upsert path)
   * and what {@link Queue.getRepeatableJobs} entries carry as `meta.key`.
   * If the caller did not supply an explicit `repeatJobKey`, the engine
   * derives one as `<jobName>::<patternSignature>`.
   */
  async removeRepeatableByKey(key: string): Promise<boolean> {
    const producer = await this.producer();
    return producer.removeRepeatable(key);
  }

  /**
   * Read a stored handler result by job id. Returns `undefined` for three
   * indistinguishable cases: the job has not yet completed, the result
   * key already expired (`Worker.resultTtlMs`), or no result was ever
   * written (job failed, was DLQ'd, or the worker ran without
   * `storeResults`).
   *
   * The bytes are msgpack-decoded with the same wire format the worker
   * shim used to encode them, so the typed `ResultType` of the Queue is
   * the natural return type.
   */
  async getJobResult(jobId: string): Promise<ResultType | undefined> {
    const producer = await this.producer();
    const buf = await producer.getResult(jobId);
    if (buf == null) return undefined;
    return decodePayload(buf) as ResultType;
  }

  // --- DLQ inspection / replay (engine slice 3) ---

  /**
   * Inspect up to `limit` DLQ entries, oldest first. Each entry carries
   * the relocated `dlqId`, the original `sourceId` it had on the main
   * stream, the routing `reason` (`retries_exhausted` / `unrecoverable`
   * / `malformed` / `oversize` / `decode_fail`) and an optional
   * `detail`, the dispatch `name`, plus the raw `payload` bytes. Mirrors
   * `Queue.peek_dlq` on the Python shim.
   */
  async peekDlq(limit: number = 20): Promise<NativeDlqEntry[]> {
    const producer = await this.producer();
    return producer.peekDlq(limit);
  }

  /**
   * Atomically move up to `limit` DLQ entries back into the main stream,
   * resetting their attempt counter so they get a fresh retry budget.
   * Returns the number of entries actually replayed. Mirrors
   * `Queue.replay_dlq` on the Python shim.
   */
  async replayDlq(limit: number = 100): Promise<number> {
    const producer = await this.producer();
    return producer.replayDlq(limit);
  }

  // --- Introspection -----------------------------------------------------

  /**
   * Look up a single job by id across the four queue surfaces (stream
   * PEL, delayed ZSET, main stream, DLQ, result key). Bounded scan;
   * returns `undefined` when the id isn't found in any surface.
   *
   * The returned {@link Job} surfaces engine state via `processedOn` /
   * `finishedOn` / `failedReason` where applicable. The `data` payload
   * is the msgpack-decoded user value.
   */
  async getJob(
    id: string,
  ): Promise<Job<DataType, ResultType, NameType> | undefined> {
    const insp = await this.introspector();
    const info = await insp.getJob(id);
    if (!info) return undefined;
    return this.nativeInfoToJob(info);
  }

  /**
   * Paginated listing within a single state. `state` is one of
   * `"waiting" | "active" | "delayed" | "completed" | "failed"`.
   * Pagination uses the `next_cursor` returned alongside the page;
   * pass it back via the optional last-arg cursor on the next call.
   *
   * BullMQ compat note: BullMQ's `getJobs` accepts `types: JobType[]`
   * and `start` / `end` indices. v1 takes a single state plus offset
   * / limit / cursor. Passing an array throws `NotSupportedError`.
   */
  async getJobs(
    types?: JobType | JobType[],
    start?: number,
    end?: number,
    _asc?: boolean,
  ): Promise<Job<DataType, ResultType, NameType>[]> {
    if (Array.isArray(types)) {
      if (types.length === 1) {
        types = types[0];
      } else {
        throw new NotSupportedError(
          "Queue.getJobs with multiple states is not supported; pass a single JobState",
        );
      }
    }
    const state: string =
      types === undefined || types === "paused" ? "waiting" : (types as string);
    const offset = start ?? 0;
    const limit = end !== undefined && end >= offset ? end - offset + 1 : 100;
    const insp = await this.introspector();
    const page = await insp.getJobs(state, offset, limit, undefined);
    return page.jobs.map((info) => this.nativeInfoToJob(info));
  }

  /**
   * One of `"waiting" | "active" | "delayed" | "completed" | "failed" |
   * "unknown"`. Live-state-first: a job that's been replayed from DLQ
   * resolves as `"waiting"` (not `"completed"`) during the race window.
   */
  async getJobState(id: string): Promise<JobState | "unknown"> {
    const insp = await this.introspector();
    const s = await insp.getJobState(id);
    return s as JobState | "unknown";
  }

  /**
   * Per-state counts: `{ waiting, active, delayed, completed, failed,
   * paused }`. Pass no args for the full count dict; pass one or more
   * `JobType`s to filter.
   *
   * `completed` is via bounded SCAN over `result:*` keys; large
   * keyspaces may return a lower-bound figure (configurable cap via
   * the `CHASQUIMQ_COMPLETED_SCAN_CAP` env var on the engine).
   */
  async getJobCounts(...types: JobType[]): Promise<Record<string, number>> {
    const insp = await this.introspector();
    const c = await insp.getJobCounts();
    const all: Record<string, number> = {
      waiting: c.waiting,
      active: c.active,
      delayed: c.delayed,
      completed: c.completed,
      failed: c.failed,
      paused: c.paused,
    };
    if (types.length === 0) return all;
    const out: Record<string, number> = {};
    for (const t of types) {
      if (t in all) out[t] = all[t];
    }
    return out;
  }

  async getWaitingCount(): Promise<number> {
    return (await this.getJobCounts("waiting")).waiting ?? 0;
  }
  async getActiveCount(): Promise<number> {
    return (await this.getJobCounts("active")).active ?? 0;
  }
  async getDelayedCount(): Promise<number> {
    return (await this.getJobCounts("delayed")).delayed ?? 0;
  }
  async getCompletedCount(): Promise<number> {
    return (await this.getJobCounts("completed")).completed ?? 0;
  }
  async getFailedCount(): Promise<number> {
    return (await this.getJobCounts("failed")).failed ?? 0;
  }
  async count(): Promise<number> {
    // BullMQ's `Queue.count` returns the number of waiting + active +
    // delayed jobs (i.e. everything that could still run). Mirror that
    // so swap-in BullMQ users don't get surprised.
    const c = await this.getJobCounts();
    return (c.waiting ?? 0) + (c.active ?? 0) + (c.delayed ?? 0);
  }

  private nativeInfoToJob(
    info: NativeJobInfo,
  ): Job<DataType, ResultType, NameType> {
    let data: DataType;
    try {
      data = decodePayload(info.payload) as DataType;
    } catch {
      // Surface poison payloads as the raw bytes; callers can branch
      // on `failedReason === "decode_failed"` if they want to.
      data = info.payload as unknown as DataType;
    }
    const opts: JobsOptions = {};
    const job = new Job<DataType, ResultType, NameType>(
      (info.name || "") as NameType,
      data,
      opts,
      info.id,
      this,
    );
    job.timestamp = info.createdAtMs ?? job.timestamp;
    job.attemptsMade = info.attempt ?? 0;
    if (info.processedOnMs !== undefined && info.processedOnMs !== null) {
      job.processedOn = info.processedOnMs;
    }
    if (info.finishedOnMs !== undefined && info.finishedOnMs !== null) {
      job.finishedOn = info.finishedOnMs;
    }
    if (info.failureReason !== undefined && info.failureReason !== null) {
      job.failedReason = info.failureReason;
    }
    if (info.decodeFailed) {
      job.failedReason = job.failedReason ?? "decode_failed";
    }
    return job;
  }

  /**
   * Durably pause every consumer of this queue. Sets a cross-process
   * Redis flag (`{chasqui:<queue>}:paused`); each worker stops dispatching
   * new jobs at its next batch boundary while in-flight jobs drain and
   * producers keep enqueueing. The pause survives worker restarts until
   * {@link Queue.resume}. Idempotent. This is the queue-wide control; for
   * a single in-process worker use {@link Worker.pause} instead.
   */
  async pause(): Promise<void> {
    const producer = await this.producer();
    await producer.pause();
  }

  /** Lift a durable pause set by {@link Queue.pause}. Idempotent. */
  async resume(): Promise<void> {
    const producer = await this.producer();
    await producer.resume();
  }

  /** Whether this queue is durably paused via the cross-process flag. */
  async isPaused(): Promise<boolean> {
    const producer = await this.producer();
    return producer.isPaused();
  }

  /**
   * Remove a single job by id from everywhere it could live — the delayed
   * stage, a waiting or in-flight stream entry, the dead-letter queue, and
   * the stored result. Idempotent: a job id that exists on no surface
   * resolves without error.
   *
   * Returns the number of distinct surfaces the job was removed from
   * (0 when the id was not found anywhere). For the per-surface breakdown
   * use {@link Queue.removeReport}.
   */
  async remove(jobId: string): Promise<number> {
    const report = await this.removeReport(jobId);
    return (
      (report.delayed ? 1 : 0) +
      (report.stream ? 1 : 0) +
      (report.dlq ? 1 : 0) +
      (report.result ? 1 : 0)
    );
  }

  /**
   * Like {@link Queue.remove}, but returns the full per-surface report so
   * a caller can tell "removed while delayed" from "removed from the DLQ".
   */
  async removeReport(jobId: string): Promise<RemovalReport> {
    const producer = await this.producer();
    return producer.remove(jobId, this.opts.consumerGroup ?? "default");
  }

  /**
   * Clear every waiting job from the queue. In-flight (active) jobs are
   * left running. By default the delayed stage is also emptied; pass
   * `delayed = false` to keep scheduled future jobs.
   *
   * Returns the count of jobs removed (stream + delayed).
   */
  async drain(delayed: boolean = true): Promise<number> {
    const producer = await this.producer();
    return producer.drain(this.opts.consumerGroup ?? "default", delayed);
  }

  /**
   * Tear the entire queue down — delete every Redis key backing it: the
   * stream and its consumer groups, the dead-letter queue, the delayed
   * stage, all per-job side-indexes and result keys, repeatable specs,
   * the pause flag, and the events stream.
   *
   * Returns the count of Redis keys removed. `opts` is accepted for
   * call-site compatibility; obliterate always tears the whole queue
   * down, so `force` / `count` have no effect.
   */
  async obliterate(_opts?: { force?: boolean; count?: number }): Promise<number> {
    const producer = await this.producer();
    return producer.obliterate(this.opts.consumerGroup ?? "default");
  }

  /**
   * Age- and state-filtered bulk delete. Removes up to `limit` jobs in
   * the given state that are older than `grace` milliseconds, and returns
   * the removed job ids.
   *
   * `type` is one of `"completed" | "failed" | "delayed" | "waiting"` and
   * defaults to `"completed"`. `"active"` is intentionally a no-op —
   * removing an in-flight job is a footgun; use {@link Queue.remove} for
   * the deliberate per-job case.
   */
  async clean(
    grace: number,
    limit: number,
    type: JobType = "completed",
  ): Promise<string[]> {
    const producer = await this.producer();
    return producer.clean(
      this.opts.consumerGroup ?? "default",
      grace,
      limit,
      type,
    );
  }

  async close(): Promise<void> {
    if (this.producerPromise) {
      const producer = await this.producerPromise;
      await producer.shutdown();
      this.producerPromise = undefined;
    }
    if (this.introspectorPromise) {
      const insp = await this.introspectorPromise;
      await insp.shutdown();
      this.introspectorPromise = undefined;
    }
    this.closed = true;
  }

  /**
   * `await using` integration (TypeScript 5.2+). Routes through
   * {@link Queue.close} so explicit-resource-management is symmetric
   * with manual close. Mirrors Python's `async with` / `__aexit__`.
   */
  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }
}

// Plain -> TLS scheme map. fred routes a clustered connection by the URL
// scheme, so the cluster schemes must keep their `-cluster` suffix when
// TLS is layered on: `redis-cluster://` becomes `rediss-cluster://`, never
// `rediss://redis-cluster://`. valkey schemes are fred aliases for the
// redis ones and get the same treatment.
const TLS_SCHEME: Record<string, string> = {
  redis: "rediss",
  "redis-cluster": "rediss-cluster",
  valkey: "valkeys",
  "valkey-cluster": "valkeys-cluster",
};
const ALREADY_TLS = [
  "rediss://",
  "rediss-cluster://",
  "valkeys://",
  "valkeys-cluster://",
];

function buildRedisUrl(c: ConnectionOptions): string {
  // A caller-supplied url wins over host/port + cluster, exactly as it
  // already wins over every other discrete connection field.
  if (c.url) return applyTls(c.url, c.tls === true);
  const host = c.host ?? "127.0.0.1";
  const port = c.port ?? 6379;
  const auth = c.password
    ? `${c.username ?? ""}:${encodeURIComponent(c.password)}@`
    : "";
  const db = c.db != null ? `/${c.db}` : "";
  const base = c.tls ? "rediss" : "redis";
  const scheme = c.cluster === true ? `${base}-cluster` : base;
  return `${scheme}://${auth}${host}:${port}${db}`;
}

function applyTls(url: string, tls: boolean): string {
  if (!tls) return url;
  const lower = url.toLowerCase();
  if (ALREADY_TLS.some((p) => lower.startsWith(p))) return url;
  const sep = url.indexOf("://");
  if (sep !== -1) {
    const tlsScheme = TLS_SCHEME[lower.slice(0, sep)];
    if (tlsScheme !== undefined) return tlsScheme + url.slice(sep);
  }
  return "rediss://" + url;
}

/**
 * Translate a {@link RepeatOptions} (the JS-shaped spec users pass to
 * `Queue.add`) into the NAPI-shaped pattern object the producer wants.
 *
 * Validates that exactly one of `pattern` / `every` is set: passing both
 * would silently take one and ignore the other, which is a footgun. The
 * thrown `Error` is plain (not `NotSupportedError`) — this is a config
 * mistake the caller can fix in their own code, not a missing feature.
 */
function translateRepeatPattern(repeat: RepeatOptions): {
  kind: "cron" | "every";
  expression?: string;
  tz?: string;
  intervalMs?: number;
} {
  const hasPattern =
    typeof repeat.pattern === "string" && repeat.pattern.length > 0;
  const hasEvery = typeof repeat.every === "number" && repeat.every >= 0;
  if (hasPattern && hasEvery) {
    throw new Error(
      "RepeatOptions: pass either `pattern` (cron) or `every` (ms), not both",
    );
  }
  if (!hasPattern && !hasEvery) {
    throw new Error(
      "RepeatOptions: one of `pattern` (cron) or `every` (ms) is required",
    );
  }
  if (hasPattern) {
    return {
      kind: "cron",
      expression: repeat.pattern,
      tz: repeat.tz,
    };
  }
  return {
    kind: "every",
    intervalMs: repeat.every,
  };
}

/**
 * Translate a {@link MissedFiresOption} (the JS-shaped policy users pass
 * via `RepeatOptions.missedFires`) into the NAPI-shaped policy object the
 * native binding accepts. `undefined` → `undefined` so the engine default
 * (`Skip`) applies. `fire-all` requires `maxCatchup` to be a finite
 * positive integer (`>= 1`); zero is rejected because the engine's
 * scheduler loop is `if count >= max_catchup { break }`, making
 * `max_catchup = 0` a wire-distinct equivalent of `Skip` that callers
 * almost certainly didn't mean.
 */
function translateMissedFires(
  policy: MissedFiresOption | undefined,
): { kind: string; maxCatchup?: number } | undefined {
  if (policy == null) return undefined;
  switch (policy.kind) {
    case "skip":
    case "fire-once":
      return { kind: policy.kind };
    case "fire-all": {
      const n = policy.maxCatchup;
      if (!Number.isFinite(n) || !Number.isInteger(n) || n < 1) {
        throw new RangeError(
          `missedFires.maxCatchup must be a positive integer (>= 1), got ${n}`,
        );
      }
      return { kind: "fire-all", maxCatchup: n };
    }
    default: {
      const _exhaustive: never = policy;
      throw new Error(
        `Unknown missedFires kind ${JSON.stringify(_exhaustive)}`,
      );
    }
  }
}

function coerceDateLike(
  d: Date | string | number | undefined,
): number | undefined {
  if (d == null) return undefined;
  if (d instanceof Date) return d.getTime();
  if (typeof d === "number") return d;
  const parsed = Date.parse(d);
  if (Number.isNaN(parsed)) {
    throw new Error(`RepeatOptions: invalid date string ${JSON.stringify(d)}`);
  }
  return parsed;
}

/**
 * Translate a `JobsOptions.backoff` (BullMQ-shaped: either a plain
 * `number` of ms, or `{ type, delay, maxDelay, multiplier, jitterMs }`)
 * into the native `NativeBackoffSpec` shape (`{ kind, delayMs,
 * maxDelayMs, multiplier, jitterMs }`). Field-name translation:
 * `type → kind`, `delay → delayMs`, `maxDelay → maxDelayMs`. The
 * `multiplier` and `jitterMs` fields keep their JS-side names.
 */
function translateBackoff(b: number | BackoffOptions): NativeBackoffSpec {
  if (typeof b === "number") {
    return { kind: "fixed", delayMs: b };
  }
  // Pass `kind` straight through; the engine's NAPI binding rejects
  // anything other than `'fixed'` / `'exponential'` so a typo here
  // surfaces as an Error rather than a silent fallthrough.
  const out: NativeBackoffSpec = {
    kind: b.type,
    delayMs: b.delay ?? 0,
  };
  if (b.maxDelay != null) out.maxDelayMs = b.maxDelay;
  if (b.multiplier != null) out.multiplier = b.multiplier;
  if (b.jitterMs != null) out.jitterMs = b.jitterMs;
  return out;
}

function buildRetryOverride(
  opts: JobsOptions,
): NativeJobRetryOverride | undefined {
  if (opts.attempts == null && opts.backoff == null) return undefined;
  const out: NativeJobRetryOverride = {};
  if (opts.attempts != null) out.maxAttempts = opts.attempts;
  if (opts.backoff != null) out.backoff = translateBackoff(opts.backoff);
  return out;
}

function buildNativeAddOptions(
  jobId: string | undefined,
  retry: NativeJobRetryOverride | undefined,
  name: string | undefined,
): NativeAddOptions | undefined {
  if (jobId == null && retry == null && !name) return undefined;
  const out: NativeAddOptions = {};
  if (jobId != null) out.id = jobId;
  if (retry != null) out.retry = retry;
  if (name) out.name = name;
  return out;
}

/**
 * @internal Test-only surface for the URL builders. Not re-exported from
 * `index.ts`, so it never reaches the public package API. Imported
 * directly from `dist/queue.js` by `__test__/cluster-url.test.ts` to
 * unit-test scheme handling without opening a Redis connection.
 */
export const __urlInternals = { buildRedisUrl, applyTls };
