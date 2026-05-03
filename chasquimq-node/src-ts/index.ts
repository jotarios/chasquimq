// Package entry point for the high-level chasquimq shim. Importing
// `chasquimq` (vs `chasquimq/native`) gets you the `Queue` / `Job`
// ergonomic surface; the raw NAPI bindings stay reachable under the
// `./native` subpath for power users who want the unwrapped engine.

export { Queue } from './queue.js'
export { Job } from './job.js'
export { Worker } from './worker.js'
export type { Processor, WorkerOptions } from './worker.js'
export { QueueEvents } from './queue-events.js'
export type { QueueEventsOptions } from './queue-events.js'
export * from './types.js'
export * from './errors.js'
export { encodePayload, decodePayload } from './encoding.js'
