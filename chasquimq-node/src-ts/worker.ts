/**
 * High-level `Worker` shim for chasquimq.
 *
 * Wraps the native `Consumer` (NAPI binding over the Rust engine)
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
import './_dispose-polyfill.js'
import { EventEmitter } from 'node:events'
import { decode } from '@msgpack/msgpack'

import {
  Consumer as NativeConsumer,
  type ConsumerOpts as NativeConsumerOpts,
  type Job as NativeJob,
} from '../index.js'
import { Job } from './job.js'
import type { ConnectionOptions, JobsOptions } from './types.js'
import { encodePayload } from './encoding.js'
import { NotSupportedError } from './errors.js'

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
  (job: Job<DataType, ResultType, NameType>) => Promise<ResultType>

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
   * Maps to `ConsumerOpts.blockMs`. Higher values reduce idle Redis CPU;
   * lower values shorten shutdown drain.
   */
  drainDelay?: number

  /**
   * Maximum total attempts per job (initial + retries). Maps to
   * `ConsumerOpts.maxAttempts`.
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

  /**
   * Auto-spawn an embedded native `Scheduler` alongside the consumer
   * so repeatable / cron specs upserted via `Queue.add(name, data,
   * { repeat })` actually fire on this worker process.
   *
   * Default `true`. Set to `false` when the deployment runs a separate
   * scheduler process (or a sidecar) and you want this worker to be a
   * pure consumer. Multiple workers with `runScheduler: true` cooperate
   * via leader election (`SET NX EX` on
   * `{chasqui:<queue>}:scheduler:lock`) — only one fires at a time.
   */
  runScheduler?: boolean

  /**
   * Override scheduler tick interval when `runScheduler !== false`.
   * Default 1000ms. Lower values reduce per-spec fire jitter at the cost
   * of more idle Redis CPU; the lower bound on jitter is roughly this
   * interval.
   */
  schedulerTickMs?: number

  /**
   * If `true`, the engine persists each handler's resolved return value
   * under `{chasqui:<queue>}:result:<jobId>` with TTL `resultTtlMs`.
   * Read it back via `Queue.getJobResult(jobId)`.
   *
   * Default `false` — handlers that return `undefined` / `void` are the
   * common case, and persisting nothing is the cheapest path through
   * the engine. Opt in only when consumers need to fetch results.
   */
  storeResults?: boolean

  /**
   * Time-to-live (ms) for stored results when `storeResults = true`.
   * Default 3,600,000 (1h). Rounded up to whole seconds at the FFI
   * boundary because Redis `EX` only accepts integer seconds.
   */
  resultTtlMs?: number
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
  private closed = false
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
      runScheduler: opts.runScheduler !== false,
      schedulerTickMs: opts.schedulerTickMs,
      storeResults: opts.storeResults,
      resultTtlMs: opts.resultTtlMs,
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

    const storeResults = this.opts.storeResults === true
    const handler = async (
      nativeJob: NativeJob,
    ): Promise<Buffer | undefined> => {
      const data = decode(nativeJob.payload) as DataType
      // Worker-side jobs have no producer-supplied JobsOptions on the wire —
      // pass `{ timestamp }` so the canonical Job class still gets a
      // non-null opts object and a real timestamp.
      const opts: JobsOptions = { timestamp: Number(nativeJob.createdAtMs) }
      const job = new Job<DataType, ResultType, NameType>(
        (nativeJob.name as NameType) ?? ('' as NameType),
        data,
        opts,
        nativeJob.id,
      )
      job.attemptsMade = nativeJob.attempt
      this.emit('active', job, '')
      try {
        const result = await this.processor(job)
        job.returnvalue = result
        this.emit('completed', job, result, '')
        // When `storeResults` is opt'd-in and the user returned a defined
        // value, msgpack-encode it for the engine. `undefined` / `void`
        // collapses to `undefined`, which the native binding maps to
        // empty `Bytes` — engine then short-circuits to the ack-only
        // fast path (no result key written).
        if (storeResults && result !== undefined) {
          return encodePayload(result)
        }
        return undefined
      } catch (err) {
        const e = err instanceof Error ? err : new Error(String(err))
        job.failedReason = e.message
        this.emit('failed', job, e, '')
        // The native binding inspects `err.name` and maps
        // `'UnrecoverableError'` to `HandlerError::unrecoverable(...)` on
        // the Rust side, so the engine routes the job straight to the
        // DLQ (`DlqReason::Unrecoverable`) without consuming the retry
        // budget. Re-throw so the rejection propagates verbatim — the
        // binding sees the same error shape regardless of subclass.
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
   *
   * Idempotent — calling `close()` more than once awaits the in-flight
   * drain instead of re-shutting-down.
   */
  async close(_force = false): Promise<void> {
    if (this.closed) {
      if (this.runPromise) {
        try {
          await this.runPromise
        } catch {
          /* swallow — already surfaced via 'error' */
        }
      }
      return
    }
    this.closed = true
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
   * `await using` integration (TypeScript 5.2+). Routes through
   * {@link Worker.close}; mirrors Python's `async with` / `__aexit__`.
   */
  async [Symbol.asyncDispose](): Promise<void> {
    await this.close()
  }

  /**
   * `true` after the first {@link Worker.close} call. Mirrors
   * `Worker.is_closed` on the Python shim.
   */
  get isClosed(): boolean {
    return this.closed
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
  if (c.url) return applyTls(c.url, c.tls === true)
  const host = c.host ?? '127.0.0.1'
  const port = c.port ?? 6379
  const auth = c.password
    ? `${c.username ?? ''}:${encodeURIComponent(c.password)}@`
    : ''
  const db = c.db != null ? `/${c.db}` : ''
  const scheme = c.tls ? 'rediss' : 'redis'
  return `${scheme}://${auth}${host}:${port}${db}`
}

function applyTls(url: string, tls: boolean): string {
  if (!tls) return url
  const lower = url.toLowerCase()
  if (lower.startsWith('rediss://')) return url
  if (lower.startsWith('redis://')) return 'rediss://' + url.slice('redis://'.length)
  return 'rediss://' + url
}
