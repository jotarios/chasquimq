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
import { QueueEvents } from './queue-events.js'
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
   * Maximum stall attempts before the stalled-job detector relocates
   * the entry to the DLQ with `DlqReason::Stalled`. Maps to engine
   * `max_stalled_attempts` (default `1` — matches BullMQ's
   * `maxStalledCount` default).
   *
   * **v1.4.0 routing fix (BREAKING for the small set of users who
   * relied on the pre-v1.4 behavior).** Pre-v1.4 this field was
   * mis-routed to engine `max_attempts` (with a `?? 3` shim-side
   * fallback that masked the engine's real default of `25`). It now
   * routes to the semantically-correct `max_stalled_attempts` — stall
   * cycles before DLQ-as-`stalled`, not total handler attempts before
   * DLQ-as-`retries_exhausted`. To preserve the pre-v1.4 behavior of
   * "cap total attempts at 3", set `maxAttempts: 3` explicitly. A
   * one-time `WARN [chasquimq]` log fires per process when
   * `maxStalledCount` is set without `maxAttempts` so the breaking
   * change is loud at runtime.
   */
  maxStalledCount?: number

  /**
   * Maximum total attempts per job (initial + retries) before the
   * engine routes to the DLQ with `DlqReason::RetriesExhausted`. Maps
   * to engine `max_attempts` (default `25`). This is the canonical
   * name for what users pre-v1.4 thought `maxStalledCount` was doing
   * — see the {@link WorkerOptions.maxStalledCount} doc for the
   * migration story.
   */
  maxAttempts?: number

  /**
   * Override the stalled-job detector's scan-tick interval (ms).
   * Default `30_000` (mirrors the engine default).
   *
   * The detector inherits its `idle_threshold_ms` from `claim_min_idle_ms`
   * at the engine level so the per-crash counting invariant
   * (`tick == idle == claim_min_idle`) holds — see
   * `docs/engine.md#stalled-detection`. This option only sets the
   * scheduling cadence operators usually want to leave alone; in
   * practice the engine clamps this to match `claim_min_idle_ms` on
   * the embedded spawn.
   */
  stalledInterval?: number

  /**
   * Toggle the embedded stalled-job detector. Default `true`. Set to
   * `false` for pure-consumer benchmarks or deployments running a
   * separate detector process. Maps to
   * `ConsumerConfig::stalled_detector_enabled`.
   */
  stalledDetectorEnabled?: boolean

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

  /**
   * `MAXLEN ~` cap on the per-job log stream
   * (`{chasqui:<queue>}:log:<id>`). Default `1000`. Must be `>= 16` —
   * below that, Redis's `MAXLEN ~` rounding can leave the stream
   * effectively empty between writes (the engine rejects sub-minimum
   * values at startup). Maps to `ConsumerConfig::log_max_stream_len`.
   */
  logMaxLen?: number

  /**
   * Per-line byte cap for {@link Job.log}. Oversize lines are
   * truncated on a UTF-8 char boundary with a `[…truncated]` marker
   * appended. Default `4096`. Maps to
   * `ConsumerConfig::log_max_line_bytes`.
   */
  logMaxLineBytes?: number

  /**
   * Gate on the engine's `e=progress` events-stream entry emitted by
   * {@link Job.updateProgress}. The persisted progress key is always
   * written; setting this to `false` only mutes the events fan-out,
   * which a {@link QueueEvents} subscriber would otherwise observe on
   * the broadcast `'progress'` channel and the per-id
   * `'progress:<jobId>'` channel. Default `true`. Maps to
   * `ConsumerConfig::events_progress_enabled`.
   */
  eventsProgressEnabled?: boolean
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

/**
 * High-level `Worker`.
 *
 * ## Events
 *
 * Event names follow the familiar `EventEmitter` listener convention so
 * existing application code reads naturally. ChasquiMQ-specific events
 * are clearly marked.
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
 * - `drained`   — `()`. Fired when the engine observes a full→empty
 *   transition on the main stream (no more jobs to dispatch right now).
 *   Lazily subscribes to the cross-process events stream on the first
 *   `worker.on('drained', ...)` call; the subscriber is torn down on
 *   `.close()`. **Cross-process scope:** every worker on this queue
 *   receives `drained`, not just this one.
 * - `paused`    — `()`. Fired when `.pause()` is called. Process-local
 *   (does not reflect a cross-process `Queue.pause`).
 * - `resumed`   — `()`. Fired when `.resume()` is called. Process-local.
 *
 * - `progress`  — `(job: Job, progress: JobProgress)`. Fired every time
 *   a processor calls `await job.updateProgress(n)`. The engine writes
 *   the persisted progress key first, then emits an `e=progress` event
 *   onto the events stream; the worker subscribes to its own
 *   {@link QueueEvents} subscriber (lazily spawned the first time a
 *   `progress` listener attaches) and re-emits onto this EE so callers
 *   see `(job, n)` in the same process that ran the handler. Disable
 *   the events fan-out (and therefore this event) by setting
 *   `WorkerOptions.eventsProgressEnabled = false`.
 *
 * - `stalled`  — `(jobId: string, prev: string)`. Fired when the
 *   stalled-job detector observes this entry sitting idle past
 *   `idle_threshold_ms` for the `attempt`-th consecutive scan
 *   (under threshold; the relocate path emits a separate `dlq` event
 *   with `reason='stalled'`). `prev` is always `'active'`. **Cross-
 *   process scope:** every worker on this queue receives `stalled`,
 *   not just the one holding the entry. Lazily subscribes to the
 *   cross-process events stream on the first `worker.on('stalled',
 *   ...)` call; the subscriber is torn down on `.close()`.
 *
 * ## Listener names accepted for API stability but currently no-op
 *
 * (None — every event documented above is live.)
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
  /**
   * Lazily-constructed events-stream subscriber that fans the engine's
   * cross-process events (`drained`, `progress`) onto this worker's
   * `EventEmitter`. Created the first time a listener attaches to
   * `'drained'` or `'progress'`; torn down in `.close()`. Workers that
   * never subscribe to either pay no extra Redis connections.
   */
  private internalEvents?: QueueEvents
  /**
   * Resolves once the lazy internal subscriber has issued its first
   * `XREAD BLOCK`. `run()` awaits this (when set) so a user pattern of
   * `worker.on('drained' | 'progress', cb); queue.add(...)` doesn't
   * race the engine's first emit against the subscriber's connect+block.
   */
  private internalEventsReadyPromise?: Promise<void>
  /**
   * Map of in-flight `Job` instances by id, populated for the duration
   * of each processor invocation. Used by the `progress` event forwarder
   * to surface the same `Job` reference the handler is holding (so
   * `worker.on('progress', (job, n) => ...)` and the handler observe
   * identical state). Entries are removed in the handler's `finally`
   * so the map stays bounded to current concurrency.
   */
  private inflight: Map<string, Job<DataType, ResultType, NameType>> = new Map()

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

    // v1.4.0 routing fix: warn-once when `maxStalledCount` is set on
    // its own. Pre-v1.4 it routed to engine `max_attempts` (with a
    // `?? 3` shim-side fallback that masked the real default of 25);
    // it now routes to `max_stalled_attempts`. Surface the change so
    // upgraders don't silently get a different DLQ-routing outcome.
    if (opts.maxStalledCount != null && opts.maxAttempts == null) {
      warnMaxStalledCountSemanticsOnce(name)
    }

    const url = buildRedisUrl(opts.connection)
    const nativeOpts: NativeConsumerOpts = {
      queueName: name,
      concurrency: opts.concurrency ?? 100,
      blockMs: opts.drainDelay ?? 5000,
      // v1.4.0: maxAttempts is the canonical "total handler attempts
      // before DLQ-as-retries_exhausted" knob. `undefined` flows to
      // the engine default (25). Dropping the `?? 3` fallback that
      // pre-v1.4 masked the engine default — `undefined` now reaches
      // the engine literally as "use your default", which is what
      // users were already expecting under the old (mis-named) field.
      maxAttempts: opts.maxAttempts,
      // v1.4.0: maxStalledCount routes to max_stalled_attempts
      // (slice-12 stalled-detector). The semantic rename lives in
      // the doc on `WorkerOptions.maxStalledCount`.
      maxStalledAttempts: opts.maxStalledCount,
      stalledDetectorEnabled: opts.stalledDetectorEnabled,
      stalledDetectorTickMs: opts.stalledInterval,
      consumerId: opts.name,
      runScheduler: opts.runScheduler !== false,
      schedulerTickMs: opts.schedulerTickMs,
      storeResults: opts.storeResults,
      resultTtlMs: opts.resultTtlMs,
      reconnectMaxAttempts: opts.connection.reconnectMaxAttempts,
      logMaxLen: opts.logMaxLen,
      logMaxLineBytes: opts.logMaxLineBytes,
      eventsProgressEnabled: opts.eventsProgressEnabled,
    }
    // Plumb the optional credentialProvider through to the native
    // Consumer constructor. `undefined` (the common path) collapses to
    // the engine's default auth-from-URL behaviour; a function value
    // installs a `JsCredentialProvider` on the engine's
    // `ConnectionTuning::credential_provider` so fred invokes it on
    // every reconnect / AUTH cycle.
    this.native = new NativeConsumer(url, nativeOpts, opts.connection.credentialProvider)

    // Spawn the cross-process events-stream subscriber the first time
    // a user attaches a `drained`, `progress`, or `stalled` listener.
    // Using `newListener` (not a public method) keeps the API surface
    // plain `EventEmitter`-shaped — users just call
    // `worker.on('drained' | 'progress' | 'stalled', ...)`, no extra
    // setup. `stalled` is a slice-12 add: the stalled-job detector
    // emits `e=stalled` on the events stream which the lazy
    // subscriber forwards onto this Worker EE with the BullMQ-shaped
    // `(jobId, prev)` payload.
    this.on('newListener', (event: string) => {
      if (
        (event === 'drained' || event === 'progress' || event === 'stalled') &&
        !this.internalEvents &&
        !this.closed
      ) {
        this.spawnInternalSubscriber()
      }
    })

    if (opts.autorun !== false) {
      // Defer to the next microtask so subscribers can attach listeners
      // (`worker.on('completed', ...)`, etc.) before the first event fires.
      queueMicrotask(() => {
        void this.run()
      })
    }
  }

  /**
   * Lazily start a {@link QueueEvents} subscriber that forwards the
   * engine's cross-process events (`drained`, `progress`) onto this
   * worker's `EventEmitter`. Idempotent — calling twice has no effect
   * (the first call wins). Errors from the subscriber are forwarded to
   * this worker's `error` channel so application code only needs one
   * error subscription.
   *
   * Progress event semantics: the engine emits one `e=progress` entry
   * per `Job.updateProgress` call. This forwarder looks the live `Job`
   * up by id in {@link Worker.inflight} (populated for the duration of
   * the handler's run) so subscribers receive the same `Job` reference
   * the handler is holding — identical to BullMQ's `(job, progress)`
   * shape. Progress events for jobs whose handlers have already
   * resolved are dropped silently; they would race the cleanup of the
   * inflight map and arrive with no live `Job` to dispatch on.
   */
  private spawnInternalSubscriber(): void {
    if (this.internalEvents) return
    // `autorun: false` + explicit `await waitUntilReady` + explicit
    // `run()` lets us hold the worker's own `run()` until the
    // subscriber's first `XREAD BLOCK` is in flight. Without this, the
    // engine's first emit (which fires within a few hundred ms of
    // worker startup on a fresh queue) can race the subscriber's
    // connect+block and the event is lost. `lastEventId: '$'` keeps
    // the subscriber from replaying ancient events from a long-lived
    // queue; the race window we close is the connect-and-block latency
    // only.
    // `blockingTimeout: 1000` (vs the QueueEvents default 10s) keeps
    // `worker.close()` snappy — close awaits the in-flight `XREAD
    // BLOCK` to time out plus 1s grace. A 10s block-and-wait would
    // mean every worker shutdown drags for up to 11s before the
    // `closed` event fires; 1s + 1s grace = ~2s worst-case teardown.
    const events = new QueueEvents(this.name, {
      connection: this.opts.connection,
      autorun: false,
      blockingTimeout: 1000,
    })
    events.on('drained', () => {
      this.emit('drained')
    })
    events.on('progress', (payload: { jobId: string; name?: string; progress: number }) => {
      const job = this.inflight.get(payload.jobId)
      if (job) {
        // Mirror the persisted progress onto the local Job so listeners
        // and the handler observe consistent state. The handler itself
        // already set this via `updateProgress`; this branch covers
        // listeners that fire before the handler awaits.
        job.progress = payload.progress
        this.emit('progress', job, payload.progress)
      }
    })
    // Slice-12: forward `stalled` events with the BullMQ-shaped
    // `(jobId, prev)` payload. Cross-process scope (the detector is
    // leader-elected per queue, so any worker on the queue receives
    // the event regardless of which worker held the stalled entry).
    // Not Job-keyed because the entry is in mid-flight on some
    // OTHER worker (possibly even one that crashed); the local
    // `inflight` map wouldn't have a live Job for this id.
    events.on('stalled', (payload: { jobId: string; prev: string }) => {
      this.emit('stalled', payload.jobId, payload.prev)
    })
    // Forward subscriber errors onto the worker's single ``error``
    // channel so application code only needs one error subscription.
    // Caller MUST wire a ``worker.on('error', ...)`` listener — an
    // unhandled ``error`` emit crashes the Node process, same as for
    // any other ``EventEmitter``.
    events.on('error', (err: Error) => {
      if (!this.closed) {
        this.emit('error', err)
      }
    })
    this.internalEvents = events
    // Capture a ready promise that `run()` awaits before kicking the
    // native engine. `waitUntilReady()` establishes the ioredis
    // connection; the subsequent `run()` queues the first `XREAD
    // BLOCK` synchronously into ioredis's command queue, so once
    // `events.run()` has been invoked the subscription window is open
    // even though the outer run promise won't resolve until close.
    // Errors surface via the `error` forwarder above so the worker's
    // single error channel stays canonical.
    let resolveReady!: () => void
    this.internalEventsReadyPromise = new Promise<void>((res) => {
      resolveReady = res
    })
    void (async () => {
      try {
        await events.waitUntilReady()
        // Fire-and-forget — the loop runs until close. The XREAD
        // BLOCK lands in ioredis's send queue synchronously inside
        // `events.run()`, so once we yield past the first microtask
        // tick the subscriber is "live" for incoming events.
        void events.run().catch((err) => {
          if (!this.closed) {
            this.emit('error', err instanceof Error ? err : new Error(String(err)))
          }
        })
        // Yield once so the XREAD command flushes onto the socket
        // before we release the worker's startup gate.
        await new Promise<void>((r) => setImmediate(r))
        resolveReady()
      } catch (err) {
        // Release the gate on connect failure so `run()` never hangs;
        // the error itself surfaces via the `error` forwarder above.
        resolveReady()
        if (!this.closed) {
          this.emit('error', err instanceof Error ? err : new Error(String(err)))
        }
      }
    })()
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

    // If a `drained` or `progress` listener attached before `run()`
    // was called, hold the engine start until the subscriber's `XREAD
    // BLOCK` is in flight. Best-effort: the gate releases on
    // subscriber error too, so a Redis blip on the events stream
    // cannot wedge the worker.
    if (this.internalEventsReadyPromise) {
      await this.internalEventsReadyPromise
    }

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
      // Wire the native handle in BEFORE the handler runs so a processor
      // that calls `await job.updateProgress(n)` reaches the engine's
      // per-dispatch `JobHandle`. Track in `inflight` for the duration
      // of the handler so the `progress` events forwarder can surface
      // the same Job reference the handler is holding.
      job._attachNative(nativeJob)
      this.inflight.set(nativeJob.id, job)
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
      } finally {
        this.inflight.delete(nativeJob.id)
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
    // Tear down the lazy internal subscriber if one was started. Best-
    // effort: swallow errors so a transient Redis blip on close doesn't
    // mask the worker's own shutdown path.
    if (this.internalEvents) {
      await this.internalEvents.close().catch(() => {
        /* best-effort cleanup */
      })
      this.internalEvents = undefined
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
   * Pause this worker's reader at the next batch boundary. Jobs already
   * being processed run to completion; no new jobs are dispatched until
   * {@link Worker.resume}. Process-local (does not write the cross-process
   * Redis flag — use {@link Queue.pause} for queue-wide durable pause).
   * Idempotent. The `doNotWaitActive` argument is accepted for call-shape
   * stability but is a no-op: this method returns immediately and
   * in-flight jobs always drain in the background.
   */
  async pause(_doNotWaitActive = false): Promise<void> {
    this.native.pause()
    // Emit AFTER trip so a listener firing `pause()` synchronously
    // observes consistent state: `worker.isPaused() === true` by the
    // time `paused` fires. Process-local — fired from the local Worker,
    // not from the cross-process Redis pause flag.
    this.emit('paused')
  }

  /**
   * Resume a paused worker. The reader wakes immediately (no poll-interval
   * latency for the in-process path). Idempotent.
   */
  resume(): void {
    this.native.resume()
    this.emit('resumed')
  }

  /**
   * Whether this worker is paused via {@link Worker.pause}. Does not
   * reflect a cross-process {@link Queue.pause}.
   */
  isPaused(): boolean {
    return this.native.isPaused()
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

/**
 * Process-global latch for the v1.4.0 `maxStalledCount` semantics
 * warning. We warn at most once per process — the message is a
 * migration nag, not a per-queue diagnostic, and a multi-queue
 * deployment that sets `maxStalledCount` on each Worker shouldn't
 * spam N copies of the same prose. The first triggering queue's name
 * makes the warning concrete; subsequent triggers are suppressed.
 *
 * Stashed on `globalThis` (under a unique symbol) so multiple
 * loaded copies of this module — which happens under Vitest's
 * isolated test runner — still share one latch, matching the
 * "once per process" intent that a normal user app honors via the
 * single-instance CommonJS module cache.
 */
const WARN_LATCH_KEY = Symbol.for('chasquimq.maxStalledCountWarned')
type LatchHost = typeof globalThis & {
  [WARN_LATCH_KEY]?: boolean
}

function warnMaxStalledCountSemanticsOnce(queueName: string): void {
  const host = globalThis as LatchHost
  if (host[WARN_LATCH_KEY]) return
  host[WARN_LATCH_KEY] = true
  // eslint-disable-next-line no-console
  console.warn(
    `WARN [chasquimq] \`maxStalledCount\` now controls stall-attempts (not job-attempts) ` +
      `(first seen on queue "${queueName}"). Use \`maxAttempts\` for the prior behavior. ` +
      `This warning fires once per process; see CHANGELOG for v1.4.0.`,
  )
}

/**
 * Test-only: reset the warn-once latch so the next call to
 * `warnMaxStalledCountSemanticsOnce` fires again. Used by
 * `worker-stalled.test.ts` to exercise per-test scenarios without
 * carrying latch state between tests. Not part of the public API —
 * the underscore prefix signals "do not import".
 */
export function __resetMaxStalledCountWarnLatchForTests(): void {
  const host = globalThis as LatchHost
  host[WARN_LATCH_KEY] = false
}

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
