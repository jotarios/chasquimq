// Error classes used by the high-level Queue / Job / Worker shim.
//
// Throwing a typed error (instead of a plain `Error`) lets application
// callers branch on `err.name === 'NotSupportedError'` to detect surface
// gaps that exist while the shim is being built out.

export class NotSupportedError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'NotSupportedError'
  }
}

export class UnrecoverableError extends Error {
  constructor(message?: string) {
    super(message ?? 'Unrecoverable')
    this.name = 'UnrecoverableError'
  }
}

export class RateLimitError extends Error {
  constructor(message?: string) {
    super(message ?? 'Rate limited')
    this.name = 'RateLimitError'
  }
}
