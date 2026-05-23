// Single-namespace package entry point for chasquimq.
//
// Exports the high-level shim (`Queue` / `Worker` / `Job` / `QueueEvents`)
// and the unwrapped NAPI bindings (`Producer` / `Consumer` / `Scheduler`).
// Two intentional omissions: the native `Job` value-type (collides with the
// high-level `Job`; reach into `../index.js` if you need it) and `Promoter`
// (the engine `Consumer` auto-embeds promotion since PR #64).
//
// Maintenance trap: this list is hand-curated rather than `export *` so we
// can exclude the two above. When napi-rs regenerates `index.d.ts` and a
// binding type gets renamed, this list will silently drop the renamed
// export — run `npm run build:all:debug && npm run lint` after any binding
// rename to catch divergence.

export { Queue } from './queue.js'
export type { RemovalReport } from './queue.js'
export { Job } from './job.js'
export { Worker } from './worker.js'
export type { Processor, WorkerOptions } from './worker.js'
export { QueueEvents } from './queue-events.js'
export type { QueueEventsOptions } from './queue-events.js'
export { BackoffSpec } from './backoff.js'
export * from './types.js'
export * from './errors.js'
export { encodePayload, decodePayload } from './encoding.js'

export {
  Producer,
  Consumer,
  Scheduler,
  engineVersion,
} from '../index.js'
export type {
  ProducerOpts,
  ConsumerOpts,
  SchedulerOpts,
  RetryOpts,
  AddOptions,
  BackoffSpec as NativeBackoffSpec,
  DlqEntry,
  JobRetryOverride,
  NamedPayload,
  RepeatPattern,
  RepeatableSpec,
  RepeatableMeta,
  MissedFiresPolicy,
} from '../index.js'
