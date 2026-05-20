// Public types for the chasquimq high-level Queue / Worker shim.
//
// Field shapes are intentionally permissive: the high-level API mirrors
// common job-queue ergonomics so existing application code reads naturally,
// but the engine semantics (FIFO Streams, append-only entries, no
// per-job priority) are honored under the hood. Options that don't map
// cleanly are documented as ignored or `NotSupportedError`.
//
// Dual taxonomy note. The shim exposes two parallel families of types
// after the import-surface flatten: the high-level shapes here
// (`BackoffOptions` / `RepeatOptions` / `JobsOptions` / `RepeatableJobMeta`)
// are the canonical user-facing API for `Queue.add` / `Worker` /
// `Queue.getRepeatableJobs`. The wire-format twins re-exported from
// the native binding (`BackoffSpec` / `RepeatPattern` / `RepeatableSpec`
// / `RepeatableMeta`) mirror the engine's Rust structs 1:1 and are
// intended for power users calling the unwrapped `Producer` / `Consumer`
// classes directly. Both will appear in autocomplete; reach for the
// high-level shapes unless you've already opted into the native API.

export type JobProgress = number | object;

/**
 * Resolved shape returned by a {@link ConnectionOptions.credentialProvider}
 * callback. Both fields are optional — omitted username falls back to the
 * Redis `default` user, omitted password skips `AUTH` entirely for that
 * reconnect cycle. Mirrors the native `CredentialResponseJs` 1:1.
 */
export interface CredentialResponse {
  username?: string;
  password?: string;
}

/**
 * Async credential resolver. Invoked by the engine on every reconnect /
 * `AUTH` cycle, **not** per-job — the hot path is unaffected. Use it
 * for rotating-token auth sources (ElastiCache IAM, Vault, AWS Secrets
 * Manager, GCP Secret Manager, ...).
 *
 * `host` is the `host:port` of the server the engine is about to AUTH
 * against, or `null` when fred has not yet resolved a target (e.g. on
 * the initial `Config::from_url` parse). The callback should return a
 * fresh `{ username?, password? }` pair; a thrown error / rejected
 * Promise routes through fred's `reconnect_on_auth_error` policy and
 * is retried on the next attempt.
 */
export type CredentialProvider = (
  host: string | null,
) => Promise<CredentialResponse>;

export interface ConnectionOptions {
  host?: string; // default '127.0.0.1'
  port?: number; // default 6379
  password?: string;
  username?: string;
  db?: number;
  tls?: boolean;
  url?: string;
  /**
   * Connect in Redis Cluster mode. When `true`, the `host`/`port` are
   * treated as one seed node and the rest of the topology is discovered
   * via `CLUSTER SLOTS`. Ignored when an explicit `url` is given — pass a
   * `redis-cluster://` (or `rediss-cluster://`) URL instead. Default
   * `false` (single-node / replica-free).
   *
   * Every key for one queue shares a `{chasqui:<queue>}` hash tag, so a
   * queue's entire keyspace lands on a single slot and the engine's
   * multi-key Lua scripts stay atomic on a cluster. Cross-queue atomic
   * operations are not supported on a cluster (they do not exist in the
   * single-node engine either).
   */
  cluster?: boolean;
  /**
   * Opt-in async resolver for rotating-token auth. When set, the engine
   * invokes this callback on every reconnect / `AUTH` cycle and uses the
   * returned `{ username, password }` to authenticate against Redis.
   * `undefined` (the default) keeps the engine on inline auth from `url`
   * / `username` / `password`.
   *
   * **Note:** the credentialProvider hook only fires on auth / reconnect,
   * never per-job. Per-job overhead is zero.
   *
   * See {@link CredentialProvider} for the callback shape.
   */
  credentialProvider?: CredentialProvider;
  /**
   * Cap on the engine's exponential reconnect attempts. `0` (the
   * default) means retry forever. Set a positive value so a
   * permanently rejecting {@link credentialProvider} (revoked IAM
   * user, expired role) eventually gives up instead of looping on
   * reconnect indefinitely. Pair it with monitoring on reconnect
   * churn — a low cap turns a silent infinite retry into a loud,
   * surfaced failure.
   */
  reconnectMaxAttempts?: number;
  // Other connection-shaped fields are accepted and silently ignored;
  // chasquimq's native producer manages its own pool.
  [key: string]: unknown;
}

export interface BackoffOptions {
  /** Strategy. Future engine variants may decode as `Unknown` and
   *  degrade to exponential at the consumer; the NAPI binding rejects
   *  unknown strings up-front, so keep this strict. */
  type: "fixed" | "exponential";
  /** Base delay in milliseconds. */
  delay?: number;
  /** Cap on the computed backoff (per-attempt). */
  maxDelay?: number;
  /** Multiplier for `exponential` (`delay * multiplier^(attempt-1)`).
   *  Ignored for `fixed`. */
  multiplier?: number;
  /** Symmetric ±jitter applied per attempt. */
  jitterMs?: number;
}

export interface JobsOptions {
  /** Delay in milliseconds before the job becomes processable. */
  delay?: number;
  /** Total attempt budget. Overrides the queue-wide `maxAttempts`
   *  for this specific job; routed through the engine's per-job retry
   *  override carried inside the encoded `Job<T>`. */
  attempts?: number;
  /** Per-job backoff override. Either a plain `number` (treated as a
   *  fixed delay in ms) or a `BackoffOptions` describing fixed /
   *  exponential strategy with optional cap, multiplier, and jitter. */
  backoff?: number | BackoffOptions;
  /** Mostly a no-op — chasquimq XACKDELs on success by default. */
  removeOnComplete?: boolean | number;
  /** Reserved for future DLQ trim policy. */
  removeOnFail?: boolean | number;
  /** Ignored with a one-time console warning (Streams are FIFO). */
  priority?: number;
  /** Stable client-supplied id. Routes through addWithId / addInWithId. */
  jobId?: string;
  /** Ignored with a one-time console warning. */
  lifo?: boolean;
  /** Submission timestamp in ms; default Date.now(). */
  timestamp?: number;
  /**
   * Schedule a recurring job. Pass either `pattern` (cron) or `every` (ms),
   * not both. The spec is upserted on the first call; subsequent calls
   * with the same resolved key overwrite. See {@link RepeatOptions}.
   */
  repeat?: RepeatOptions;
  /**
   * Stable key for the repeat spec. If unset, the engine derives one as
   * `<jobName>::<patternSignature>` (e.g. `cron:0 2 * * *:UTC`).
   * Re-upserting with the same resolved key is idempotent.
   */
  repeatJobKey?: string;
  /** Throws NotSupportedError — parent/child flows are not supported. */
  parent?: { id: string; queue: string };
}

/**
 * Catch-up policy for repeatable specs whose scheduler was offline across
 * one or more fire windows. Mirrors the engine's `MissedFiresPolicy`:
 *
 * - `{ kind: 'skip' }` (default) — drop missed windows and resume on the
 *   first future fire. No thundering herd after a deploy / outage.
 * - `{ kind: 'fire-once' }` — emit a single job to represent the missed
 *   window(s); the spec then advances to its next future fire.
 * - `{ kind: 'fire-all', maxCatchup }` — replay each missed window up to
 *   `maxCatchup` fires before advancing. Use with care on cron specs
 *   that fire frequently (e.g. every minute) after a long downtime.
 */
export type MissedFiresOption =
  | { kind: "skip" }
  | { kind: "fire-once" }
  | { kind: "fire-all"; maxCatchup: number };

export interface RepeatOptions {
  /**
   * Cron expression. Accepts both 5-field (`m h dom mon dow`) and 6-field
   * (with leading seconds) syntax. Cannot be combined with `every`.
   */
  pattern?: string;
  /**
   * Fixed millisecond interval between fires. First fire lands one
   * interval after upsert (no immediate fire). Cannot be combined with
   * `pattern`.
   */
  every?: number;
  /** Maximum number of fires before the spec is removed. */
  limit?: number;
  /**
   * Accepted; no-op in v1. The engine fires the first occurrence one
   * cadence after upsert (matching `every` semantics).
   */
  immediately?: boolean;
  /**
   * Earliest fire time. Fires before this are skipped. `Date`,
   * milliseconds since epoch, or an ISO string.
   */
  startDate?: Date | string | number;
  /**
   * Latest fire time. Once the next fire would land past this instant,
   * the engine removes the spec.
   */
  endDate?: Date | string | number;
  /**
   * Cron timezone. Accepts `"UTC"` / `"Z"`, fixed offsets (`"+05:30"`),
   * or any IANA name (`"America/New_York"`). IANA names like
   * `'America/New_York'` are DST-aware: `0 2 * * *` fires at the local
   * 02:00 wall-clock on both sides of spring-forward / fall-back (the
   * underlying UTC instant shifts by one hour). Ignored when `every`
   * is set.
   */
  tz?: string;
  /** Unused in v1. Reserved for future explicit-id-per-fire wiring. */
  jobId?: string;
  /**
   * Catch-up policy when the scheduler was offline across one or more
   * fire windows. Default is `{ kind: 'skip' }` — drop missed windows
   * and resume on the first future fire. See {@link MissedFiresOption}.
   */
  missedFires?: MissedFiresOption;
}

/**
 * Wire-compatible projection of {@link RepeatOptions} returned by
 * {@link Queue.getRepeatableJobs}. Carries no payload — only the schedule
 * and identity, so listing thousands of specs stays cheap.
 */
export interface RepeatableJobMeta {
  key: string;
  jobName: string;
  /** `'cron'` or `'every'`. */
  patternKind: "cron" | "every";
  /** Cron expression, when `patternKind === 'cron'`. */
  pattern?: string;
  /** Cron timezone, when set. */
  tz?: string;
  /** Interval in ms, when `patternKind === 'every'`. */
  every?: number;
  nextFireMs: number;
  limit?: number;
  startAfterMs?: number;
  endBeforeMs?: number;
  /**
   * Catch-up policy this spec was upserted with. Absent when the policy
   * is the default `{ kind: 'skip' }` (the engine omits it from the
   * stored spec). See {@link MissedFiresOption}.
   */
  missedFires?: MissedFiresOption;
}

export type BulkJobOptions = Omit<JobsOptions, "repeat">;

export interface QueueOptions {
  connection: ConnectionOptions;
  /** Ignored — chasquimq uses `{chasqui:<queue>}` Cluster hash tags. */
  prefix?: string;
  defaultJobOptions?: Partial<JobsOptions>;
  /**
   * Sticky consumer-group name used by the introspector when answering
   * `getJob` / `getJobState` / `getJobCounts`. Must match whatever the
   * workers run under — defaults to `"default"` (the engine default).
   * Mirrors `consumer_group` on the Python `Queue`.
   */
  consumerGroup?: string;
}

export type JobState =
  | "waiting"
  | "active"
  | "completed"
  | "failed"
  | "delayed"
  | "unknown";

export type JobType = JobState | "paused" | "prioritized" | "waiting-children";
