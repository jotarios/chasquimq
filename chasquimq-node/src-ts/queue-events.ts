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
  private runPromise?: Promise<void>
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
    }
    this.client = new IORedis(ioOpts)

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
    this.running = false
    if (this.runPromise) {
      // Wait for the current XREAD BLOCK to time out, then exit. Cap at
      // blockingTimeoutMs + 1s so close() doesn't hang forever.
      await Promise.race([
        this.runPromise,
        sleep(this.blockingTimeoutMs + 1000),
      ])
    }
    await this.client.quit().catch(() => {})
  }

  private dispatchEntry(eventId: string, kv: string[]): void {
    const f: Record<string, string> = {}
    for (let i = 0; i + 1 < kv.length; i += 2) f[kv[i]!] = kv[i + 1]!
    const e = f['e']
    const jobId = f['id'] ?? ''

    switch (e) {
      case 'waiting':
        this.emit('waiting', { jobId }, eventId)
        break
      case 'active':
        this.emit('active', { jobId, prev: 'waiting', attempt: parseIntSafe(f['attempt']) }, eventId)
        break
      case 'completed':
        this.emit('completed', { jobId, attempt: parseIntSafe(f['attempt']), returnvalue: undefined }, eventId)
        break
      case 'failed':
        this.emit('failed', { jobId, failedReason: f['reason'] ?? '', attempt: parseIntSafe(f['attempt']) }, eventId)
        break
      case 'retry-scheduled':
        // chasquimq-specific extension event; advanced subscribers use this
        // to observe retry scheduling decisions before they fire.
        this.emit('retry-scheduled', { jobId, attempt: parseIntSafe(f['attempt']), backoffMs: parseIntSafe(f['backoff_ms']) }, eventId)
        break
      case 'delayed':
        this.emit('delayed', { jobId, delay: parseIntSafe(f['delay_ms']) }, eventId)
        break
      case 'dlq':
        // The engine emits a single `dlq` event; surface both `failed` and
        // `retries-exhausted` for high-level shim users who expect the
        // latter as a separate signal.
        this.emit('failed', { jobId, failedReason: f['reason'] ?? 'retries-exhausted', attempt: parseIntSafe(f['attempt']) }, eventId)
        this.emit('retries-exhausted', { jobId, attemptsMade: parseIntSafe(f['attempt']) }, eventId)
        break
      case 'drained':
        this.emit('drained', eventId)
        break
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
