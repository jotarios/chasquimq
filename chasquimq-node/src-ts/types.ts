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
  type: 'fixed' | 'exponential' | string
  delay?: number // ms
}

export interface JobsOptions {
  /** Delay in milliseconds before the job becomes processable. */
  delay?: number
  /** Total attempt budget (default 1). Routed through engine slice 8 (TODO). */
  attempts?: number
  /** Per-job backoff override; engine wiring pending. */
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
  /** Throws NotSupportedError until the native binding exposes slice 10. */
  repeat?: RepeatOptions
  /** Throws NotSupportedError — parent/child flows are not supported. */
  parent?: { id: string; queue: string }
}

export interface RepeatOptions {
  pattern?: string
  every?: number
  limit?: number
  immediately?: boolean
  startDate?: Date | string | number
  endDate?: Date | string | number
  tz?: string
  jobId?: string
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
