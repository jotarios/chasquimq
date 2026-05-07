// Single-namespace package entry point for chasquimq.
//
// Both the high-level shim (`Queue` / `Worker` / `Job` / `QueueEvents`)
// and the unwrapped NAPI bindings (`Producer` / `Consumer` /
// `Scheduler`) are re-exported from this module, so consumers do:
//
//   import { Queue, Worker, Producer, Consumer } from "chasquimq";
//
// `Promoter` is intentionally NOT re-exported here — symmetry with the
// Python shim, which has no `Promoter` pyclass. The engine `Consumer`
// auto-embeds promotion since PR #64; producer-only deployments that
// need the standalone `Promoter` can still import it from
// `chasquimq/index.js` (the underlying napi binding) directly.
//
// Maintenance trap: this list is hand-curated rather than `export *
// from '../index.js'` because that wildcard would surface `Promoter`
// alongside everything else and we can't selectively exclude it. Side
// effect — when napi-rs regenerates `index.d.ts` and a binding type
// gets renamed, this list will silently drop the renamed export. Run
// `npm run build:all:debug && npm run lint` after any binding rename
// to catch divergence; if a new binding type lands, add it here.
//
// The native binding's `Job` value type is re-exported as `NativeJob`
// to avoid colliding with the high-level `Job` class above; everything
// else is exported under its native name.

export { Queue } from './queue.js'
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
  Job as NativeJob,
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
