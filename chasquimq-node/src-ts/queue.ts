// High-level Queue class — the JS-friendly entry point that turns
// `queue.add(name, data, opts)` into a single native producer call.
//
// The MVP wires `add` and `addBulk` (with delayed / idempotent jobId
// variants) through the native binding. Everything else throws
// `NotSupportedError` until a future slice exposes the matching engine
// surface; this keeps the public types stable while we iterate.

import {
  NativeProducer,
  type NativeProducerOpts,
  type NativeAddOptions,
  type NativeBackoffSpec,
  type NativeJobRetryOverride,
} from '../index.js'
import type {
  BackoffOptions,
  ConnectionOptions,
  JobsOptions,
  BulkJobOptions,
  QueueOptions,
  JobState,
  JobType,
  RepeatOptions,
  RepeatableJobMeta,
} from './types.js'
import { NotSupportedError } from './errors.js'
import { Job } from './job.js'
import { encodePayload } from './encoding.js'

let warnedPriority = false
let warnedLifo = false
let warnedNamePersist = false

export class Queue<
  DataType = unknown,
  ResultType = unknown,
  NameType extends string = string,
> {
  readonly name: string
  readonly opts: QueueOptions
  private producerPromise?: Promise<NativeProducer>

  constructor(name: string, opts: QueueOptions) {
    this.name = name
    this.opts = opts
  }

  private async producer(): Promise<NativeProducer> {
    if (!this.producerPromise) {
      const url = buildRedisUrl(this.opts.connection)
      const native: NativeProducerOpts = { queueName: this.name }
      this.producerPromise = NativeProducer.connect(url, native)
    }
    return this.producerPromise
  }

  async add(
    name: NameType,
    data: DataType,
    opts: JobsOptions = {},
  ): Promise<Job<DataType, ResultType, NameType>> {
    const merged: JobsOptions = {
      ...(this.opts.defaultJobOptions ?? {}),
      ...opts,
    }

    if (merged.repeat) {
      return await this.upsertRepeatableJob(name, data, merged)
    }
    if (merged.parent) {
      throw new NotSupportedError('Parent/child flows are not supported')
    }
    if (merged.priority != null && merged.priority !== 0 && !warnedPriority) {
      console.warn(
        '[chasquimq] JobsOptions.priority is ignored (FIFO Streams). Set to 0 to silence this warning.',
      )
      warnedPriority = true
    }
    if (merged.lifo === true && !warnedLifo) {
      console.warn('[chasquimq] JobsOptions.lifo is ignored (FIFO Streams).')
      warnedLifo = true
    }
    if (name && !warnedNamePersist) {
      console.warn(
        '[chasquimq] Job names are not persisted in v1. job.name is only available in the same process. The Worker callback receives an empty string.',
      )
      warnedNamePersist = true
    }

    const retryOverride = buildRetryOverride(merged)
    const nativeOpts = buildNativeAddOptions(merged.jobId, retryOverride)

    const buf = encodePayload(data)
    const producer = await this.producer()
    let id: string
    if (merged.delay && merged.delay > 0) {
      if (nativeOpts) {
        id = await producer.addInWithOptions(merged.delay, buf, nativeOpts)
      } else {
        id = await producer.addIn(merged.delay, buf)
      }
    } else if (nativeOpts) {
      id = await producer.addWithOptions(buf, nativeOpts)
    } else {
      id = await producer.add(buf)
    }
    return new Job(name, data, merged, id)
  }

  async addBulk(
    jobs: Array<{ name: NameType; data: DataType; opts?: BulkJobOptions }>,
  ): Promise<Job<DataType, ResultType, NameType>[]> {
    if (jobs.length === 0) return []
    if (jobs.some((j) => j.opts?.parent)) {
      throw new NotSupportedError('Parent options not supported in addBulk')
    }
    // For v1: route through native add_bulk only when no per-job
    // delay / jobId / attempts / backoff. Anything else falls back to the
    // per-entry add() loop below — losing the bulk pipelining win.
    const allSimple = jobs.every((j) => {
      const o = j.opts ?? {}
      return !o.delay && !o.jobId && !o.attempts && !o.backoff
    })
    if (allSimple) {
      const buffers = jobs.map((j) => encodePayload(j.data))
      const producer = await this.producer()
      const ids = await producer.addBulk(buffers)
      return jobs.map(
        (j, i) =>
          new Job(
            j.name,
            j.data,
            { ...(this.opts.defaultJobOptions ?? {}), ...(j.opts ?? {}) },
            ids[i]!,
          ),
      )
    }
    // Mixed path: per-entry add(). Loses bulk pipelining.
    const out: Job<DataType, ResultType, NameType>[] = []
    for (const j of jobs) {
      out.push(await this.add(j.name, j.data, j.opts as JobsOptions))
    }
    return out
  }

  private async upsertRepeatableJob(
    name: NameType,
    data: DataType,
    merged: JobsOptions,
  ): Promise<Job<DataType, ResultType, NameType>> {
    const repeat = merged.repeat as RepeatOptions
    const pattern = translateRepeatPattern(repeat)
    const buf = encodePayload(data)
    const startAfterMs = coerceDateLike(repeat.startDate)
    const endBeforeMs = coerceDateLike(repeat.endDate)
    const producer = await this.producer()
    const resolvedKey = await producer.upsertRepeatable({
      key: merged.repeatJobKey ?? '',
      jobName: name,
      pattern,
      payload: buf,
      limit: repeat.limit,
      startAfterMs,
      endBeforeMs,
    })
    // The repeatable upsert is a *spec*, not a job invocation; the engine
    // mints a fresh ULID for each fire. Returning a Job here gives callers
    // a stable handle (the resolved spec key as `id`) to pair with
    // `Queue.removeRepeatableByKey(job.id)` for symmetry with the
    // single-add API. The `id` shape is therefore intentionally **not** a
    // ULID for repeatable upserts.
    return new Job(name, data, merged, resolvedKey)
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
    const producer = await this.producer()
    const metas = await producer.listRepeatable(limit)
    return metas.map((m): RepeatableJobMeta => {
      const base: RepeatableJobMeta = {
        key: m.key,
        jobName: m.jobName,
        patternKind: m.pattern.kind === 'cron' ? 'cron' : 'every',
        nextFireMs: m.nextFireMs,
        limit: m.limit,
        startAfterMs: m.startAfterMs,
        endBeforeMs: m.endBeforeMs,
      }
      if (m.pattern.kind === 'cron') {
        base.pattern = m.pattern.expression ?? undefined
        base.tz = m.pattern.tz ?? undefined
      } else {
        base.every = m.pattern.intervalMs ?? undefined
      }
      return base
    })
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
    const producer = await this.producer()
    return producer.removeRepeatable(key)
  }

  // --- Stubs (NotSupportedError) ---

  async getJob(_id: string): Promise<Job | undefined> {
    throw new NotSupportedError('Queue.getJob not implemented in v1')
  }

  async getJobs(
    _types?: JobType | JobType[],
    _start?: number,
    _end?: number,
    _asc?: boolean,
  ): Promise<Job[]> {
    throw new NotSupportedError('Queue.getJobs not implemented in v1')
  }

  async getJobState(_id: string): Promise<JobState | 'unknown'> {
    return 'unknown'
  }

  async getJobCounts(..._types: JobType[]): Promise<Record<string, number>> {
    throw new NotSupportedError('Queue.getJobCounts not implemented in v1')
  }

  async getWaitingCount(): Promise<number> {
    throw new NotSupportedError('not implemented')
  }
  async getActiveCount(): Promise<number> {
    throw new NotSupportedError('not implemented')
  }
  async getDelayedCount(): Promise<number> {
    throw new NotSupportedError('not implemented')
  }
  async getCompletedCount(): Promise<number> {
    return 0 // engine doesn't persist completions
  }
  async getFailedCount(): Promise<number> {
    throw new NotSupportedError('not implemented')
  }
  async count(): Promise<number> {
    throw new NotSupportedError('not implemented')
  }

  async pause(): Promise<void> {
    throw new NotSupportedError('Queue.pause not implemented in v1')
  }
  async resume(): Promise<void> {
    throw new NotSupportedError('Queue.resume not implemented in v1')
  }
  async isPaused(): Promise<boolean> {
    return false
  }

  async remove(jobId: string): Promise<number> {
    // Best-effort: cancelDelayed for delayed jobs. Stream entries can't be
    // removed from a consumer group's PEL by id alone, so we throw instead
    // of silently returning 0.
    const producer = await this.producer()
    const removed = await producer.cancelDelayed(jobId)
    if (removed) return 1
    throw new NotSupportedError(
      'Removing in-stream entries is not supported; only delayed-stage cancellation works in v1',
    )
  }

  async drain(_delayed?: boolean): Promise<void> {
    throw new NotSupportedError('Queue.drain not implemented in v1')
  }

  async obliterate(_opts?: {
    force?: boolean
    count?: number
  }): Promise<void> {
    throw new NotSupportedError('Queue.obliterate not implemented in v1')
  }

  async clean(
    _grace: number,
    _limit: number,
    _type?: JobType,
  ): Promise<string[]> {
    throw new NotSupportedError('Queue.clean not implemented in v1')
  }

  async close(): Promise<void> {
    // The native producer manages its own pool lifetime; nothing to
    // explicitly close yet. Drop the cached promise so a future call
    // would lazily re-connect.
    this.producerPromise = undefined
  }
}

function buildRedisUrl(c: ConnectionOptions): string {
  const host = c.host ?? '127.0.0.1'
  const port = c.port ?? 6379
  const auth = c.password
    ? `${c.username ?? ''}:${encodeURIComponent(c.password)}@`
    : ''
  const db = c.db != null ? `/${c.db}` : ''
  return `redis://${auth}${host}:${port}${db}`
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
  kind: 'cron' | 'every'
  expression?: string
  tz?: string
  intervalMs?: number
} {
  const hasPattern = typeof repeat.pattern === 'string' && repeat.pattern.length > 0
  const hasEvery = typeof repeat.every === 'number' && repeat.every >= 0
  if (hasPattern && hasEvery) {
    throw new Error(
      'RepeatOptions: pass either `pattern` (cron) or `every` (ms), not both',
    )
  }
  if (!hasPattern && !hasEvery) {
    throw new Error(
      'RepeatOptions: one of `pattern` (cron) or `every` (ms) is required',
    )
  }
  if (hasPattern) {
    return {
      kind: 'cron',
      expression: repeat.pattern,
      tz: repeat.tz,
    }
  }
  return {
    kind: 'every',
    intervalMs: repeat.every,
  }
}

function coerceDateLike(d: Date | string | number | undefined): number | undefined {
  if (d == null) return undefined
  if (d instanceof Date) return d.getTime()
  if (typeof d === 'number') return d
  const parsed = Date.parse(d)
  if (Number.isNaN(parsed)) {
    throw new Error(`RepeatOptions: invalid date string ${JSON.stringify(d)}`)
  }
  return parsed
}

/**
 * Translate a `JobsOptions.backoff` (BullMQ-shaped: either a plain
 * `number` of ms, or `{ type, delay, ... }`) into the native
 * `NativeBackoffSpec` shape (`{ kind, delayMs, ... }`). Field-name
 * translation is the entire contract here — `type → kind`, `delay → delayMs`.
 */
function translateBackoff(b: number | BackoffOptions): NativeBackoffSpec {
  if (typeof b === 'number') {
    return { kind: 'fixed', delayMs: b }
  }
  // Pass `kind` straight through; the engine's NAPI binding rejects
  // anything other than `'fixed'` / `'exponential'` so a typo here
  // surfaces as an Error rather than a silent fallthrough.
  return {
    kind: b.type,
    delayMs: b.delay ?? 0,
  }
}

function buildRetryOverride(opts: JobsOptions): NativeJobRetryOverride | undefined {
  if (opts.attempts == null && opts.backoff == null) return undefined
  const out: NativeJobRetryOverride = {}
  if (opts.attempts != null) out.maxAttempts = opts.attempts
  if (opts.backoff != null) out.backoff = translateBackoff(opts.backoff)
  return out
}

function buildNativeAddOptions(
  jobId: string | undefined,
  retry: NativeJobRetryOverride | undefined,
): NativeAddOptions | undefined {
  if (jobId == null && retry == null) return undefined
  const out: NativeAddOptions = {}
  if (jobId != null) out.id = jobId
  if (retry != null) out.retry = retry
  return out
}
