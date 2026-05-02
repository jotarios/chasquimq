/**
 * High-level `Worker` shim for chasquimq.
 *
 * Wraps the native `NativeConsumer` (NAPI binding over the Rust engine)
 * with an `EventEmitter`-flavored API: a user-supplied `Processor`
 * function runs once per delivered job, the MessagePack payload is
 * decoded into a typed `Job`, and lifecycle events fire at the
 * appropriate points (`active`, `completed`, `failed`, `error`).
 *
 * All scheduling, retry, DLQ, and ack work happens in the Rust engine —
 * this shim is a thin presentation layer.
 *
 * v1 scope (intentional):
 *   - Construct, run, close.
 *   - Emit `active` / `completed` / `failed` / `error` lifecycle events.
 *   - Decode MessagePack payload into a typed `Job` instance.
 *
 * Out of scope for v1 (stubbed below; throw `NotSupportedError`):
 *   - `pause()` / `resume()` / `isPaused()` (gating job dispatch
 *     client-side while the engine continues to pull jobs is not yet
 *     implemented).
 *   - `rateLimit()` (no leaky-bucket primitive in the engine yet).
 *   - Sandboxed processors via string/URL path (the constructor throws).
 *   - `stalled` / `drained` events (the engine's events stream lands in
 *     a later slice).
 */
import { EventEmitter } from 'node:events'
import { decode } from '@msgpack/msgpack'

import { NativeConsumer, type NativeConsumerOpts, type NativeJob } from '../index.js'

// ---------------------------------------------------------------------------
// Local placeholder Job
//
// The companion queue shim PR introduces the canonical Job class at
// `src-ts/job.ts`. When that PR merges first, this declaration will be
// replaced by `import { Job } from './job.js'`. Field names and shape
// are chosen so the merge is mechanical.
// ---------------------------------------------------------------------------

class JobLite<DataType = unknown, ResultType = unknown, NameType extends string = string> {
  /** Stream entry id assigned by Redis. */
  readonly id: string
  /** Logical job name. The engine doesn't carry a name today; defaults to ''. */
  readonly name: NameType
  /** Decoded payload data. */
  readonly data: DataType
  /** 1-indexed attempt count carried by the engine (1 = first delivery). */
  attemptsMade: number
  /** Set on the JS side after the processor resolves. */
  returnvalue?: ResultType
  /** Set on the JS side after the processor rejects. */
  failedReason?: string
  /** Unix-ms timestamp the job was added to the stream. */
  timestamp: number

  constructor(id: string, name: NameType, data: DataType, attempt: number, createdAtMs: number) {
    this.id = id
    this.name = name
    this.data = data
    this.attemptsMade = attempt
    this.timestamp = createdAtMs
  }

  toJSON(): { id: string; name: NameType; data: DataType; attemptsMade: number } {
    return { id: this.id, name: this.name, data: this.data, attemptsMade: this.attemptsMade }
  }
}

export type Job<DataType = unknown, ResultType = unknown, NameType extends string = string> =
  JobLite<DataType, ResultType, NameType>

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/**
 * Processor signature: a function the user supplies that runs once per
 * delivered job. Resolving the returned `Promise` acks the job;
 * rejecting it routes the job through the engine's retry path
 * (eventually DLQ once `maxAttempts` is exhausted).
 */
export type Processor<DataType = unknown, ResultType = unknown, NameType extends string = string> =
  (job: JobLite<DataType, ResultType, NameType>) => Promise<ResultType>

/**
 * Connection options accepted by the high-level shim. Internally
 * normalized to a single `redis://...` URL because that's what the
 * native binding accepts.
 */
export interface ConnectionOptions {
  host?: string
  port?: number
  password?: string
  username?: string
  db?: number
}

/**
 * Options passed to the high-level `Worker` constructor.
 */
export interface WorkerOptions {
  /** Redis connection. Required. */
  connection: ConnectionOptions

  /** Max in-flight handler invocations. Default 100. */
  concurrency?: number

  /**
   * If `true` (the default), the worker calls `.run()` automatically
   * on the next microtask. Set `false` to `.run()` explicitly later.
   */
  autorun?: boolean

  /**
   * Polling block timeout (ms) on the underlying `XREADGROUP` call.
   * Maps to `NativeConsumerOpts.blockMs`. Higher values reduce idle
   * Redis CPU; lower values shorten shutdown drain.
   */
  drainDelay?: number

  /**
   * Maximum total attempts per job (initial + retries). Maps to
   * `NativeConsumerOpts.maxAttempts`.
   */
  maxStalledCount?: number

  /**
   * Accepted; no-op. The engine uses `XACKDEL` so completed jobs are
   * already removed from the stream atomically with the ack.
   */
  removeOnComplete?: unknown

  /**
   * Accepted; no-op. Failed jobs that exhaust `maxAttempts` are routed
   * to the DLQ stream by the engine.
   */
  removeOnFail?: unknown

  /**
   * Accepted; no-op. chasquimq uses Redis Cluster hash-tag form
   * (`{chasqui:<queue>}:<suffix>`) for all keys; there is no tunable
   * prefix.
   */
  prefix?: string

  /** Optional consumer ID for the underlying `XREADGROUP CONSUMER`. */
  name?: string
}

class NotSupportedError extends Error {
  constructor(msg: string) {
    super(msg)
    this.name = 'NotSupportedError'
  }
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

/**
 * High-level `Worker`.
 *
 * ## Events
 *
 * - `ready`     — `()`. Fired once when `.run()` starts the engine loop.
 * - `active`    — `(job: Job, prev: string)`. Fired before each
 *   processor invocation. `prev` is reserved (always `''`).
 * - `completed` — `(job: Job, result: unknown, prev: string)`. Fired
 *   after the processor resolves. The engine acks the job.
 * - `failed`    — `(job: Job, err: Error, prev: string)`. Fired after
 *   the processor rejects. The error is re-thrown so the engine routes
 *   the job to retry-or-DLQ.
 * - `error`     — `(err: Error)`. Fired on engine-side errors surfaced
 *   from the native loop.
 * - `closing`   — `(msg: string)`. Fired at the start of `.close()`.
 * - `closed`    — `()`. Fired once shutdown completes.
 */
export class Worker<
  DataType = unknown,
  ResultType = unknown,
  NameType extends string = string,
> extends EventEmitter {
  readonly name: string
  readonly opts: WorkerOptions

  private native: NativeConsumer
  private processor: Processor<DataType, ResultType, NameType>
  private running = false
  private runPromise?: Promise<void>

  constructor(
    name: string,
    processor: string | URL | Processor<DataType, ResultType, NameType>,
    opts: WorkerOptions,
  ) {
    super()

    if (typeof processor === 'string' || processor instanceof URL) {
      throw new NotSupportedError(
        'Sandboxed processors (string/URL path) are not supported. Pass an inline Processor function.',
      )
    }

    this.name = name
    this.opts = opts
    this.processor = processor

    const url = buildRedisUrl(opts.connection)
    const nativeOpts: NativeConsumerOpts = {
      queueName: name,
      concurrency: opts.concurrency ?? 100,
      blockMs: opts.drainDelay ?? 5000,
      maxAttempts: opts.maxStalledCount ?? 3,
      consumerId: opts.name,
    }
    this.native = new NativeConsumer(url, nativeOpts)

    if (opts.autorun !== false) {
      // Defer to the next microtask so subscribers can attach listeners
      // (`worker.on('completed', ...)`, etc.) before the first event fires.
      queueMicrotask(() => {
        void this.run()
      })
    }
  }

  /**
   * Start the engine loop. Resolves once the engine drains (after
   * `.close()` is called). Calling `.run()` more than once returns the
   * same Promise — it does not start a second loop.
   */
  async run(): Promise<void> {
    if (this.running) return this.runPromise!
    this.running = true
    this.emit('ready')

    const handler = async (nativeJob: NativeJob): Promise<void> => {
      const data = decode(nativeJob.payload) as DataType
      // The engine doesn't carry job.name today; default to '' until the
      // queue shim's encoded envelope (with name) lands.
      const job = new JobLite<DataType, ResultType, NameType>(
        nativeJob.id,
        '' as NameType,
        data,
        nativeJob.attempt,
        Number(nativeJob.createdAtMs),
      )
      this.emit('active', job, '')
      try {
        const result = await this.processor(job)
        job.returnvalue = result
        this.emit('completed', job, result, '')
      } catch (err) {
        const e = err instanceof Error ? err : new Error(String(err))
        job.failedReason = e.message
        this.emit('failed', job, e, '')
        // TODO(slice-8 native): when err.name === 'UnrecoverableError',
        // signal terminal-fail to the engine so it routes immediately
        // to DLQ rather than running through the retry budget. The
        // native binding doesn't expose that variant today; for v1,
        // every rejection follows the standard retry-then-DLQ path.
        throw e
      }
    }

    this.runPromise = this.native.run(handler).catch((err: unknown) => {
      const e = err instanceof Error ? err : new Error(String(err))
      this.emit('error', e)
      throw e
    })
    return this.runPromise
  }

  /**
   * Shut down the worker. Best-effort: the engine drains its in-flight
   * handlers up to its configured shutdown deadline, then resolves.
   */
  async close(_force = false): Promise<void> {
    this.emit('closing', '')
    this.native.shutdown()
    if (this.runPromise) {
      try {
        await this.runPromise
      } catch {
        /* swallow — already surfaced via 'error' */
      }
    }
    this.running = false
    this.emit('closed')
  }

  /**
   * Not implemented in v1. Close and re-create the worker instead.
   * See class JSDoc for the v1 scope.
   */
  async pause(_doNotWaitActive = false): Promise<void> {
    throw new NotSupportedError(
      'Worker.pause is not implemented in v1; close and re-create instead',
    )
  }

  /** Not implemented in v1. */
  resume(): void {
    throw new NotSupportedError('Worker.resume is not implemented in v1')
  }

  /** Always `false` in v1. */
  isPaused(): boolean {
    return false
  }

  /** Whether the engine loop is currently running. */
  isRunning(): boolean {
    return this.running
  }

  /** Not implemented in v1. */
  async rateLimit(_expireTimeMs: number): Promise<void> {
    throw new NotSupportedError('Worker.rateLimit is not implemented in v1')
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function buildRedisUrl(c: ConnectionOptions): string {
  const host = c.host ?? '127.0.0.1'
  const port = c.port ?? 6379
  const auth = c.password
    ? `${c.username ?? ''}:${encodeURIComponent(c.password)}@`
    : ''
  const db = c.db != null ? `/${c.db}` : ''
  return `redis://${auth}${host}:${port}${db}`
}
