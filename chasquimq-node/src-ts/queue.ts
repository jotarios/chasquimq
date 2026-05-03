// High-level Queue class — the JS-friendly entry point that turns
// `queue.add(name, data, opts)` into a single native producer call.
//
// The MVP wires `add` and `addBulk` (with delayed / idempotent jobId
// variants) through the native binding. Everything else throws
// `NotSupportedError` until a future slice exposes the matching engine
// surface; this keeps the public types stable while we iterate.

import { NativeProducer, type NativeProducerOpts } from '../index.js'
import type {
  ConnectionOptions,
  JobsOptions,
  BulkJobOptions,
  QueueOptions,
  JobState,
  JobType,
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
      throw new NotSupportedError(
        'Repeatable jobs are not yet wired through the native binding; the engine supports them but the JS surface is pending',
      )
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

    if (merged.attempts != null || merged.backoff != null) {
      // TODO(slice-8 native): pass retry override once
      // NativeProducer.addWithOptions is exposed.
    }

    const buf = encodePayload(data)
    const producer = await this.producer()
    let id: string
    if (merged.delay && merged.delay > 0) {
      id = merged.jobId
        ? await producer.addInWithId(merged.jobId, merged.delay, buf)
        : await producer.addIn(merged.delay, buf)
    } else if (merged.jobId) {
      id = await producer.addWithId(merged.jobId, buf)
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
