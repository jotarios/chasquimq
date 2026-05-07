// Vitest setup: filter known-benign ioredis teardown rejections so they
// don't fail the run.
//
// Background: ioredis with `lazyConnect: true` and an in-flight blocked
// XREAD can emit a Promise rejection during shutdown ("Connection is
// closed.") that escapes past the .catch() registered on the awaited
// command. The same shape can appear during fast-fail connect attempts
// in the same teardown window.
//
// QueueEvents already attaches a client-level `error` listener and quits
// on close, but the rejection in question comes through the standalone
// connector's promise chain, not the client EventEmitter, so it leaks.
// Suppressing it here keeps CI green while we file a follow-up to
// root-cause inside ioredis or replace the underlying client.

const KNOWN_BENIGN_RE = /Connection is closed\.?/

process.on('unhandledRejection', (reason) => {
  const msg =
    reason instanceof Error
      ? reason.message
      : typeof reason === 'string'
        ? reason
        : ''
  if (KNOWN_BENIGN_RE.test(msg)) {
    return
  }
  // Re-throw anything else so vitest still sees real unhandled rejections.
  throw reason
})
