import './_dispose-polyfill.js'
import { EventEmitter } from 'node:events'
import IORedis, { type RedisOptions } from 'ioredis'

import type { ConnectionOptions } from './types.js'

export interface QueueEventsOptions {
  connection: ConnectionOptions
  prefix?: string         // ignored — chasquimq uses {chasqui:<queue>} hash tags
  autorun?: boolean       // default true
  lastEventId?: string    // start from this id; default '$' (only new)
  blockingTimeout?: number // XREAD BLOCK ms; default 10_000
}

export class QueueEvents extends EventEmitter {
  readonly name: string
  readonly opts: QueueEventsOptions
  private client: IORedis
  private streamKey: string
  private running = false
  private closed = false
  private runPromise?: Promise<void>
  private closePromise?: Promise<void>
  private blockingTimeoutMs: number

  constructor(name: string, opts: QueueEventsOptions) {
    super()
    this.name = name
    this.opts = opts
    this.streamKey = `{chasqui:${name}}:events`  // mirrors engine's events_key()
    this.blockingTimeoutMs = opts.blockingTimeout ?? 10_000

    const c = opts.connection
    const ioOpts: RedisOptions = {
      host: c.host ?? '127.0.0.1',
      port: c.port ?? 6379,
      password: c.password,
      username: c.username,
      db: c.db,
      lazyConnect: true,
      // Blocking XREAD requires maxRetriesPerRequest = null. Common pitfall.
      maxRetriesPerRequest: null,
      // ioredis auto-issues `CLIENT SETINFO LIB-NAME / LIB-VER` after AUTH/INFO
      // on every connect (introduced in ioredis 5.4 via PR #2011). Those
      // commands queue behind the user's first command and, on a client whose
      // first command is a blocking XREAD, race the close path: when the
      // socket end()s while SETINFO is still pending, ioredis flushes the
      // commandQueue with a `Connection is closed.` rejection that is NOT
      // attached to any awaited promise — it surfaces as an unhandled
      // rejection from `node_modules/ioredis/built/connectors/...`. We don't
      // surface client-info anywhere, so disabling SETINFO is free. See
      // ioredis#2025 for the upstream report; resolved in 5.8.2 for general
      // teardown but the blocked-XREAD shape still leaks via SETINFO.
      disableClientInfo: true,
    }
    this.client = new IORedis(ioOpts)
    // Operational error pass-through. Open errors during run() also surface
    // here in addition to the run-loop's xread reject path; close-time
    // errors are silenced because the SETINFO-removal above eliminates the
    // only known path that produces benign noise here.
    this.client.on('error', (err: Error) => {
      if (this.closed) return
      this.emit('error', err)
    })

    if (opts.autorun !== false) {
      queueMicrotask(() => { void this.run() })
    }
  }

  async waitUntilReady(): Promise<void> {
    if (this.client.status !== 'ready') {
      await this.client.connect().catch(() => {})  // tolerate already-connected
    }
  }

  async run(): Promise<void> {
    if (this.running) return this.runPromise!
    this.running = true
    await this.waitUntilReady()

    let lastId = this.opts.lastEventId ?? '$'
    type XReadResponse = Array<[stream: string, entries: Array<[id: string, fields: string[]]>]> | null
    // ioredis 5.x has a wide XREAD overload set; the variadic
    // `'BLOCK' | 'COUNT' | 'STREAMS' | ...` form trips its internal
    // generic resolution under strict tsc, so route the call through
    // an `any`-typed alias to bypass overload selection. The runtime
    // shape is well-defined by the Redis protocol — `XReadResponse`
    // captures the XRANGE-shaped flat key/value pairs we actually
    // receive on the wire.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const xreadAny = this.client.xread.bind(this.client) as (...args: unknown[]) => Promise<XReadResponse>
    this.runPromise = (async () => {
      while (this.running) {
        try {
          const res = await xreadAny(
            'BLOCK', this.blockingTimeoutMs, 'COUNT', 100,
            'STREAMS', this.streamKey, lastId,
          )
          if (!res || !this.running) continue
          for (const [, entries] of res) {
            for (const [id, fields] of entries) {
              lastId = id
              this.dispatchEntry(id, fields)
            }
          }
        } catch (err) {
          if (!this.running) break
          this.emit('error', err)
          // Backoff on transient errors. Network blip, no data loss — XREAD
          // will resume from lastId on the next iteration.
          await sleep(200 + Math.floor(Math.random() * 200))
        }
      }
    })()
    return this.runPromise
  }

  async close(): Promise<void> {
    // Idempotent + concurrency-safe: a second concurrent caller awaits
    // the in-flight close instead of returning before cleanup finishes.
    if (this.closePromise) return this.closePromise
    this.closed = true
    this.running = false
    this.closePromise = (async () => {
      if (this.runPromise) {
        // Wait for the current XREAD BLOCK to time out, then exit. Cap at
        // blockingTimeoutMs + 1s so close() doesn't hang forever.
        await Promise.race([
          this.runPromise,
          sleep(this.blockingTimeoutMs + 1000),
        ])
      }
      // disconnect(false) tears the socket down immediately without queueing
      // a QUIT command behind the in-flight BLOCK. `client.quit()` would
      // enqueue QUIT, and ioredis can't flush it while XREAD is mid-block
      // — the QUIT then resolves with a `Connection is closed.` rejection
      // when the socket ends, racing with the close path.
      this.client.disconnect(false)
    })()
    return this.closePromise
  }

  /**
   * `await using` integration (TypeScript 5.2+). Routes through
   * {@link QueueEvents.close}.
   */
  async [Symbol.asyncDispose](): Promise<void> {
    await this.close()
  }

  /**
   * `true` after the first {@link QueueEvents.close} call.
   */
  get isClosed(): boolean {
    return this.closed
  }

  private dispatchEntry(eventId: string, kv: string[]): void {
    const f: Record<string, string> = {}
    for (let i = 0; i + 1 < kv.length; i += 2) f[kv[i]!] = kv[i + 1]!
    const e = f['e']
    const jobId = f['id'] ?? ''
    // Slice 5 of name-on-the-wire: the engine emits `n` on per-job events
    // so subscribers can observe job kind without msgpack-decoding payload.
    // Missing/empty when the producer added the job without a name, or for
    // queue-scoped events like `drained`.
    const name = f['n'] ?? ''

    switch (e) {
      case 'waiting':
        this.emit('waiting', { jobId, name }, eventId)
        break
      case 'active': {
        const attempt = parseIntSafe(f['attempt'])
        this.emit('active', { jobId, name, prev: 'waiting', attempt }, eventId)
        // Per-job channel for `Job.waitUntilFinished` and other targeted
        // subscribers. Same payload shape as the broadcast event; the
        // narrower event name keeps the targeted-listener path off the
        // O(N-listeners) dispatch for the broadcast channel.
        if (jobId) this.emit(`active:${jobId}`, { jobId, name, prev: 'waiting', attempt }, eventId)
        break
      }
      case 'completed': {
        const attempt = parseIntSafe(f['attempt'])
        // `returnvalue` is intentionally `undefined` — the events stream
        // does not carry the handler's return bytes (that would
        // double-allocate the payload onto every subscriber). Callers
        // that need the value should pair this with `Queue.getJobResult`
        // (requires `WorkerOptions.storeResults = true`), which is what
        // `Job.waitUntilFinished` does internally.
        this.emit('completed', { jobId, name, attempt, returnvalue: undefined }, eventId)
        if (jobId) this.emit(`completed:${jobId}`, { jobId, name, attempt, returnvalue: undefined }, eventId)
        break
      }
      case 'failed': {
        const attempt = parseIntSafe(f['attempt'])
        const failedReason = f['reason'] ?? ''
        this.emit('failed', { jobId, name, failedReason, attempt }, eventId)
        if (jobId) this.emit(`failed:${jobId}`, { jobId, name, failedReason, attempt }, eventId)
        break
      }
      case 'retry-scheduled':
        // chasquimq-specific extension event; advanced subscribers use this
        // to observe retry scheduling decisions before they fire.
        this.emit('retry-scheduled', { jobId, name, attempt: parseIntSafe(f['attempt']), backoffMs: parseIntSafe(f['backoff_ms']) }, eventId)
        break
      case 'delayed':
        this.emit('delayed', { jobId, name, delay: parseIntSafe(f['delay_ms']) }, eventId)
        break
      case 'progress': {
        // Persisted progress lives at `{chasqui:<queue>}:progress:<id>` as
        // an ASCII decimal `u8`; the events-stream entry carries the same
        // value so per-job subscribers don't need a second GET round trip.
        const progress = parseIntSafe(f['progress'])
        this.emit('progress', { jobId, name, progress }, eventId)
        if (jobId) this.emit(`progress:${jobId}`, { jobId, name, progress }, eventId)
        break
      }
      case 'dlq':
        // The engine already emitted a `failed` event from the worker
        // before relocating to the DLQ — fanning out a second synthetic
        // `failed` here would double-fire on every UnrecoverableError /
        // retries-exhausted path. Surface only the chasquimq-specific
        // `retries-exhausted` channel so subscribers that wired off it
        // (high-level shim convention) keep working.
        this.emit('retries-exhausted', { jobId, name, attemptsMade: parseIntSafe(f['attempt']), reason: f['reason'] ?? '' }, eventId)
        // Forward the raw `dlq` event too — power-users (and the test-
        // app monitor) subscribe to it directly to observe the engine's
        // routing decision (retries_exhausted vs unrecoverable).
        this.emit('dlq', { jobId, name, reason: f['reason'] ?? '', attempt: parseIntSafe(f['attempt']) }, eventId)
        break
      case 'drained':
        this.emit('drained', eventId)
        break
      case 'stalled': {
        // Slice-12: emitted once per detector-INCR-under-threshold
        // observation. `attempt` is the current stall count
        // (1-indexed). `prev` is always `'active'` (every stalled
        // entry was PEL-resident when the detector saw it). Mirrors
        // the BullMQ `Worker.on('stalled', (jobId, prev))` shape so
        // shim subscribers see the familiar two-arg payload.
        const attempt = parseIntSafe(f['attempt'])
        const prev = f['prev'] ?? 'active'
        this.emit('stalled', { jobId, name, attempt, prev }, eventId)
        if (jobId) this.emit(`stalled:${jobId}`, { jobId, name, attempt, prev }, eventId)
        break
      }
      default:
        // Unknown event — forward as-is on a generic channel for future-compat.
        this.emit('unknown', { eventName: e, fields: f }, eventId)
    }
  }
}

function parseIntSafe(s: string | undefined): number {
  if (!s) return 0
  const n = parseInt(s, 10)
  return Number.isFinite(n) ? n : 0
}

function sleep(ms: number): Promise<void> {
  return new Promise((res) => setTimeout(res, ms))
}
