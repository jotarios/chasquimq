// Public types for the chasquimq high-level Queue / Worker shim.
//
// Field shapes are intentionally permissive: the high-level API mirrors
// common job-queue ergonomics so existing application code reads naturally,
// but the engine semantics (FIFO Streams, append-only entries, no
// per-job priority) are honored under the hood. Options that don't map
// cleanly are documented as ignored or `NotSupportedError`.

export type JobProgress = number | object

export interface ConnectionOptions {
  host?: string // default '127.0.0.1'
  port?: number // default 6379
  password?: string
  username?: string
  db?: number
  // Other connection-shaped fields are accepted and silently ignored;
  // chasquimq's native producer manages its own pool.
  [key: string]: unknown
}

export interface BackoffOptions {
  /** Strategy. Future engine variants may decode as `Unknown` and
   *  degrade to exponential at the consumer; the NAPI binding rejects
   *  unknown strings up-front, so keep this strict. */
  type: 'fixed' | 'exponential'
  /** Base delay in milliseconds. */
  delay?: number
  /** Cap on the computed backoff (per-attempt). */
  maxDelay?: number
  /** Multiplier for `exponential` (`delay * multiplier^(attempt-1)`).
   *  Ignored for `fixed`. */
  multiplier?: number
  /** Symmetric ±jitter applied per attempt. */
  jitterMs?: number
}

export interface JobsOptions {
  /** Delay in milliseconds before the job becomes processable. */
  delay?: number
  /** Total attempt budget. Overrides the queue-wide `maxAttempts`
   *  for this specific job; routed through the engine's per-job retry
   *  override carried inside the encoded `Job<T>`. */
  attempts?: number
  /** Per-job backoff override. Either a plain `number` (treated as a
   *  fixed delay in ms) or a `BackoffOptions` describing fixed /
   *  exponential strategy with optional cap, multiplier, and jitter. */
  backoff?: number | BackoffOptions
  /** Mostly a no-op — chasquimq XACKDELs on success by default. */
  removeOnComplete?: boolean | number
  /** Reserved for future DLQ trim policy. */
  removeOnFail?: boolean | number
  /** Ignored with a one-time console warning (Streams are FIFO). */
  priority?: number
  /** Stable client-supplied id. Routes through addWithId / addInWithId. */
  jobId?: string
  /** Ignored with a one-time console warning. */
  lifo?: boolean
  /** Submission timestamp in ms; default Date.now(). */
  timestamp?: number
  /**
   * Schedule a recurring job. Pass either `pattern` (cron) or `every` (ms),
   * not both. The spec is upserted on the first call; subsequent calls
   * with the same resolved key overwrite. See {@link RepeatOptions}.
   */
  repeat?: RepeatOptions
  /**
   * Stable key for the repeat spec. If unset, the engine derives one as
   * `<jobName>::<patternSignature>` (e.g. `cron:0 2 * * *:UTC`).
   * Re-upserting with the same resolved key is idempotent.
   */
  repeatJobKey?: string
  /** Throws NotSupportedError — parent/child flows are not supported. */
  parent?: { id: string; queue: string }
}

export interface RepeatOptions {
  /**
   * Cron expression. Accepts both 5-field (`m h dom mon dow`) and 6-field
   * (with leading seconds) syntax. Cannot be combined with `every`.
   */
  pattern?: string
  /**
   * Fixed millisecond interval between fires. First fire lands one
   * interval after upsert (no immediate fire). Cannot be combined with
   * `pattern`.
   */
  every?: number
  /** Maximum number of fires before the spec is removed. */
  limit?: number
  /**
   * Accepted; no-op in v1. The engine fires the first occurrence one
   * cadence after upsert (matching `every` semantics).
   */
  immediately?: boolean
  /**
   * Earliest fire time. Fires before this are skipped. `Date`,
   * milliseconds since epoch, or an ISO string.
   */
  startDate?: Date | string | number
  /**
   * Latest fire time. Once the next fire would land past this instant,
   * the engine removes the spec.
   */
  endDate?: Date | string | number
  /**
   * Cron timezone. Accepts `"UTC"` / `"Z"`, fixed offsets (`"+05:30"`),
   * or any IANA name (`"America/New_York"`). IANA names like
   * `'America/New_York'` are DST-aware: `0 2 * * *` fires at the local
   * 02:00 wall-clock on both sides of spring-forward / fall-back (the
   * underlying UTC instant shifts by one hour). Ignored when `every`
   * is set.
   */
  tz?: string
  /** Unused in v1. Reserved for future explicit-id-per-fire wiring. */
  jobId?: string
}

/**
 * Wire-compatible projection of {@link RepeatOptions} returned by
 * {@link Queue.getRepeatableJobs}. Carries no payload — only the schedule
 * and identity, so listing thousands of specs stays cheap.
 */
export interface RepeatableJobMeta {
  key: string
  jobName: string
  /** `'cron'` or `'every'`. */
  patternKind: 'cron' | 'every'
  /** Cron expression, when `patternKind === 'cron'`. */
  pattern?: string
  /** Cron timezone, when set. */
  tz?: string
  /** Interval in ms, when `patternKind === 'every'`. */
  every?: number
  nextFireMs: number
  limit?: number
  startAfterMs?: number
  endBeforeMs?: number
}

export type BulkJobOptions = Omit<JobsOptions, 'repeat'>

export interface QueueOptions {
  connection: ConnectionOptions
  /** Ignored — chasquimq uses `{chasqui:<queue>}` Cluster hash tags. */
  prefix?: string
  defaultJobOptions?: Partial<JobsOptions>
}

export type JobState =
  | 'waiting'
  | 'active'
  | 'completed'
  | 'failed'
  | 'delayed'
  | 'unknown'

export type JobType = JobState | 'paused' | 'prioritized' | 'waiting-children'
