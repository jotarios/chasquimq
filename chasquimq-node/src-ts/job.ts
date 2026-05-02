// Lightweight `Job` value class for the high-level shim.
//
// v1 deliberately does NOT round-trip through Redis for any field on this
// object. The engine streams jobs via XREADGROUP / XACK and does not
// persist progress, return values, or per-job state metadata, so all the
// "lookup my job" methods are stubbed with `NotSupportedError` until a
// future slice exposes a stateful query path. Mutators that would require
// rewriting a stream entry (e.g. `update`) throw — Streams are append-only.

import type { JobsOptions, JobState, JobProgress } from './types.js'
import { NotSupportedError } from './errors.js'

export class Job<
  DataType = unknown,
  ResultType = unknown,
  NameType extends string = string,
> {
  readonly id: string
  readonly name: NameType
  readonly data: DataType
  readonly opts: JobsOptions
  attemptsMade: number = 0
  progress: JobProgress = 0
  returnvalue?: ResultType
  failedReason?: string
  stacktrace: string[] = []
  timestamp: number
  delay: number
  priority: number = 0
  processedOn?: number
  finishedOn?: number

  constructor(name: NameType, data: DataType, opts: JobsOptions, id: string) {
    this.id = id
    this.name = name
    this.data = data
    this.opts = opts
    this.timestamp = opts.timestamp ?? Date.now()
    this.delay = opts.delay ?? 0
  }

  /**
   * In-memory progress update. The engine does not persist progress yet;
   * the Worker shim will surface this via its `progress` event when called
   * from inside a processor.
   */
  async updateProgress(progress: JobProgress): Promise<void> {
    this.progress = progress
  }

  async log(_row: string): Promise<number> {
    throw new NotSupportedError('Job logs are not supported in v1')
  }

  async getState(): Promise<JobState | 'unknown'> {
    return 'unknown'
  }

  async remove(): Promise<void> {
    throw new NotSupportedError('Job.remove not supported in v1')
  }

  async retry(_state?: 'completed' | 'failed'): Promise<void> {
    throw new NotSupportedError(
      'Job.retry not supported in v1; use Queue-level replay',
    )
  }

  async discard(): Promise<void> {
    throw new NotSupportedError('Job.discard not supported in v1')
  }

  async update(_data: DataType): Promise<void> {
    throw new NotSupportedError(
      'Job.update not supported (Streams are append-only)',
    )
  }

  async updateData(d: DataType): Promise<void> {
    return this.update(d)
  }

  async isCompleted(): Promise<boolean> {
    return false
  }
  async isFailed(): Promise<boolean> {
    return false
  }
  async isDelayed(): Promise<boolean> {
    return this.delay > 0
  }
  async isActive(): Promise<boolean> {
    return false
  }
  async isWaiting(): Promise<boolean> {
    return false
  }

  toJSON(): object {
    return {
      id: this.id,
      name: this.name,
      data: this.data,
      opts: this.opts,
      attemptsMade: this.attemptsMade,
      progress: this.progress,
      returnvalue: this.returnvalue,
      failedReason: this.failedReason,
      timestamp: this.timestamp,
      delay: this.delay,
      priority: this.priority,
      processedOn: this.processedOn,
      finishedOn: this.finishedOn,
    }
  }
}
