// Single-namespace package entry point for chasquimq.
//
// Both the high-level shim (`Queue` / `Worker` / `Job` / `QueueEvents`)
// and the unwrapped NAPI bindings (`Producer` / `Consumer` / `Promoter`
// / `Scheduler`) are re-exported from this module, so consumers do:
//
//   import { Queue, Worker, Producer, Consumer } from "chasquimq";
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
export * from './types.js'
export * from './errors.js'
export { encodePayload, decodePayload } from './encoding.js'

export {
  Producer,
  Consumer,
  Promoter,
  Scheduler,
  engineVersion,
} from '../index.js'
export type {
  Job as NativeJob,
  ProducerOpts,
  ConsumerOpts,
  PromoterOpts,
  SchedulerOpts,
  RetryOpts,
  AddOptions,
  BackoffSpec,
  DlqEntry,
  JobRetryOverride,
  NamedPayload,
  RepeatPattern,
  RepeatableSpec,
  RepeatableMeta,
  MissedFiresPolicy,
} from '../index.js'
