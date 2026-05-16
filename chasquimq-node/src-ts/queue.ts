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
  type ProducerOpts as NativeProducerOpts,
  type AddOptions as NativeAddOptions,
  type BackoffSpec as NativeBackoffSpec,
  type DlqEntry as NativeDlqEntry,
  type JobRetryOverride as NativeJobRetryOverride,
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

  // --- Stubs (NotSupportedError) ---

  async getJob(_id: string): Promise<Job | undefined> {
    throw new NotSupportedError("Queue.getJob not implemented in v1");
  }

  async getJobs(
    _types?: JobType | JobType[],
    _start?: number,
    _end?: number,
    _asc?: boolean,
  ): Promise<Job[]> {
    throw new NotSupportedError("Queue.getJobs not implemented in v1");
  }

  async getJobState(_id: string): Promise<JobState | "unknown"> {
    return "unknown";
  }

  async getJobCounts(..._types: JobType[]): Promise<Record<string, number>> {
    throw new NotSupportedError("Queue.getJobCounts not implemented in v1");
  }

  async getWaitingCount(): Promise<number> {
    throw new NotSupportedError("not implemented");
  }
  async getActiveCount(): Promise<number> {
    throw new NotSupportedError("not implemented");
  }
  async getDelayedCount(): Promise<number> {
    throw new NotSupportedError("not implemented");
  }
  async getCompletedCount(): Promise<number> {
    return 0; // engine doesn't persist completions
  }
  async getFailedCount(): Promise<number> {
    throw new NotSupportedError("not implemented");
  }
  async count(): Promise<number> {
    throw new NotSupportedError("not implemented");
  }

  async pause(): Promise<void> {
    throw new NotSupportedError("Queue.pause not implemented in v1");
  }
  async resume(): Promise<void> {
    throw new NotSupportedError("Queue.resume not implemented in v1");
  }
  async isPaused(): Promise<boolean> {
    return false;
  }

  async remove(jobId: string): Promise<number> {
    // Best-effort: cancelDelayed for delayed jobs. Stream entries can't be
    // removed from a consumer group's PEL by id alone, so we throw instead
    // of silently returning 0.
    const producer = await this.producer();
    const removed = await producer.cancelDelayed(jobId);
    if (removed) return 1;
    throw new NotSupportedError(
      "Removing in-stream entries is not supported; only delayed-stage cancellation works in v1",
    );
  }

  async drain(_delayed?: boolean): Promise<void> {
    throw new NotSupportedError("Queue.drain not implemented in v1");
  }

  async obliterate(_opts?: { force?: boolean; count?: number }): Promise<void> {
    throw new NotSupportedError("Queue.obliterate not implemented in v1");
  }

  async clean(
    _grace: number,
    _limit: number,
    _type?: JobType,
  ): Promise<string[]> {
    throw new NotSupportedError("Queue.clean not implemented in v1");
  }

  async close(): Promise<void> {
    if (this.producerPromise) {
      const producer = await this.producerPromise;
      await producer.shutdown();
      this.producerPromise = undefined;
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

function buildRedisUrl(c: ConnectionOptions): string {
  if (c.url) return applyTls(c.url, c.tls === true);
  const host = c.host ?? "127.0.0.1";
  const port = c.port ?? 6379;
  const auth = c.password
    ? `${c.username ?? ""}:${encodeURIComponent(c.password)}@`
    : "";
  const db = c.db != null ? `/${c.db}` : "";
  const scheme = c.tls ? "rediss" : "redis";
  return `${scheme}://${auth}${host}:${port}${db}`;
}

function applyTls(url: string, tls: boolean): string {
  if (!tls) return url;
  const lower = url.toLowerCase();
  if (lower.startsWith("rediss://")) return url;
  if (lower.startsWith("redis://")) return "rediss://" + url.slice("redis://".length);
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
