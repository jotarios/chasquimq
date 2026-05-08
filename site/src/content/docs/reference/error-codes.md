---
title: Error codes
description: Stable CMQ-* error codes for every error the engine and shims emit, with When, Why, Fix, and See also for each.
sidebar:
  order: 7
---

Every error ChasquiMQ surfaces — whether from the Rust engine,
the Node shim, the Python shim, or the DLQ relocator — has a
stable `CMQ-*` code. Use the code as a search anchor when an
error message lands in a log: it never changes across releases.

## Numbering scheme

- `CMQ-0xx` — engine errors (`chasquimq` crate).
- `CMQ-1xx` — Node shim errors.
- `CMQ-2xx` — Python shim errors.

:::tip[Stability]
**Codes are stable.** Once assigned, a code is never reused for a
different error and never renumbered. New errors get new numbers
appended to the appropriate range. If a code is ever retired (no
ChasquiMQ release ever has done so as of 1.1), it stays retired.
:::

## Engine: `Error` enum

The Rust crate's [`Error`](/reference/rust-api/#error) enum
covers the five engine-side failure modes a producer or consumer
can surface to the caller.

### CMQ-001 — Redis connection failure

**When:** The engine's underlying `fred` client returned a
connection-level error: socket disconnected, AUTH rejected,
cluster-redirect storm, command timeout. Surfaced as
`Error::Redis(fred::error::Error)`.

**Why:** The engine could not complete a Redis round trip. Every
producer / consumer entry point can surface this; transient
errors on hot-path subsystems (promoter, retry relocator) are
caught with internal backoff, but the surface API does propagate.

**Fix:** Verify Redis is reachable and the URL is correct
(`redis://[user:pass@]host:port[/db]`). Check `redis-cli ping`
from the same host. For TLS or sentinel topologies see the
[Tune for throughput](/guides/tune-for-throughput/) guide. If the
connection works but the error persists, capture the inner
`fred::error::Error` — it carries the underlying failure kind.

**See also:** [Tune for throughput](/guides/tune-for-throughput/).

### CMQ-002 — Payload encode failed

**When:** `rmp_serde::encode::Error` from the producer side. A
caller-supplied payload could not be MessagePack-encoded — usually
a type the user's `Serialize` impl rejects, or recursion depth
overflow on a self-referential structure. Surfaced as
`Error::Encode`.

**Why:** ChasquiMQ encodes job payloads as MessagePack on the wire
and refuses to ship anything that won't round-trip through
`rmp-serde`.

**Fix:** Inspect the inner error message — it pinpoints the
offending field. The Node and Python shims encode via
`@msgpack/msgpack` / `msgpack-python` on the JS / Py side and
then hand opaque bytes to the engine, so this only fires from
direct Rust callers.

**See also:** [Wire format](/reference/wire-format/).

### CMQ-003 — Payload decode failed

**When:** `rmp_serde::decode::Error` on the consumer side, or
when reading bytes back from the DLQ. The consumer's reader
catches this on the hot path and routes the entry to the DLQ as
[`CMQ-021`](#cmq-021--dlq-decode-failed); `Error::Decode` from
the Rust API surface specifically means the caller invoked a
function that decoded itself (e.g. a custom DLQ inspector).

**Why:** Bytes on the wire don't decode into the expected `T`.
Common causes: a producer running an older / newer schema than
the consumer; a payload that was hand-written into the stream;
or a `T` whose `Deserialize` impl was tightened in an in-place
upgrade.

**Fix:** Pin the producer and consumer schemas to the same
version. Read [`docs/history.md`](https://github.com/jotarios/chasquimq/blob/main/docs/history.md)
for deploy-order rules around adding fields. For one-off rescue,
peek the entry with `chasqui dlq peek` and inspect the raw bytes.

**See also:** [Wire format](/reference/wire-format/).

### CMQ-004 — Configuration invalid

**When:** Config-validation failure. Surfaced as
`Error::Config(String)` with a human-readable detail. Common
triggers:

- Job name exceeds the 256-byte cap on the producer boundary.
- `add_bulk_with_options` called with `opts.id` set and
  `payloads.len() > 1`.
- `add_in` / `add_at` with a delay exceeding `max_delay_secs`.
- Cron expression that can't be parsed by `croner`.
- Unsupported timezone string (`"Mars/Olympus_Mons"`).
- Repeatable spec with `end_before_ms` already in the past.

**Why:** Caller-supplied input the engine refuses to ship to
Redis.

**Fix:** Read the inner `String` — it states exactly what was
rejected. Adjust the call to satisfy the constraint.

**See also:** [Rust API: Producer](/reference/rust-api/#producer),
[Repeatable jobs concept](/concepts/repeatable-jobs/).

### CMQ-005 — Engine shutdown signal

**When:** The engine's shutdown token fired during an in-flight
operation. Surfaced as `Error::Shutdown`.

**Why:** Caller invoked `consumer.shutdown()` or dropped the
`CancellationToken`. The error is informational — it confirms
the engine drained cleanly rather than crashing mid-operation.

**Fix:** Treat this as a normal shutdown signal in your control
flow. Do not retry; the engine is expected to be torn down.

**See also:** [Worker / Consumer](/reference/rust-api/#consumer).

## Engine: `HandlerError`

`HandlerError` is the type a handler returns to signal failure.
Two outcomes flow from it.

### CMQ-010 — Handler returned recoverable error → retry

**When:** A handler returned `Err(HandlerError::new(e))` where
`is_unrecoverable() == false`. The consumer bumps the attempt
counter, applies the configured backoff, and reschedules onto
the delayed ZSET via `RETRY_RESCHEDULE_SCRIPT`.

**Why:** Default behavior. The engine assumes any handler error
is transient unless explicitly told otherwise.

**Fix:** Not an error condition per se — this is the normal
retry path. If retries persist, inspect the handler's inner
error. Watch the `chasquimq_jobs_failed_total` counter and the
`chasquimq_retry_scheduled_total` companion metric to see
whether retries are succeeding (`Ok` events at attempt > 1) or
the job is stuck failing on the same attempt.

**See also:** [Concepts: retry and backoff](/concepts/retry-and-backoff/),
[Configure retries guide](/guides/configure-retries/).

### CMQ-011 — Handler signaled unrecoverable → DLQ

**When:** A handler returned `Err(HandlerError::unrecoverable(e))`,
or the Node / Python shim's `UnrecoverableError` was thrown /
raised. The consumer skips the retry path and routes the job
straight to the DLQ as
[`CMQ-024`](#cmq-024--dlq-unrecoverable).

**Why:** The handler told the engine the failure is terminal —
bad input, missing dependencies, permission denied, poison-pill
payload. Retrying would burn the budget for no possible win.

**Fix:** Find the underlying cause from the handler's inner
error message; fix the input or the handler logic. If you want
to re-run the now-fixed job, use
[`chasqui dlq replay`](/reference/cli/#chasqui-dlq-replay) to
move it back to the main stream with a fresh attempt budget.

**See also:** [Route to the DLQ guide](/guides/route-to-dlq/),
[Replay the DLQ guide](/guides/replay-the-dlq/).

## Engine: DLQ reasons

Every DLQ entry carries a `reason` string. The engine emits
`DlqRouted` metrics with the same reason; subscribers to the
events stream see it as the `reason` field on `dlq` events.

### CMQ-020 — DLQ: retries exhausted

**When:** The handler ran `max_attempts` times without
succeeding. The consumer routes the entry to the DLQ with
`DlqReason::RetriesExhausted`.

**Why:** Either the queue-wide `ConsumerConfig::max_attempts` or
the per-job `JobRetryOverride::max_attempts` was hit. The job
may also arrive past `max_attempts` after a CLAIM-recovery cycle
where the previous worker crashed mid-handler — same reason
applies.

**Fix:** Inspect the failing entry with
[`chasqui dlq peek`](/reference/cli/#chasqui-dlq-peek). Identify
the underlying handler error, fix it, then
[`chasqui dlq replay`](/reference/cli/#chasqui-dlq-replay) to
re-run with a fresh budget. If the budget itself is too tight,
bump `attempts` on `JobsOptions` (per job) or `maxStalledCount`
on `WorkerOptions` / `max_attempts` on Python `Worker` /
`ConsumerConfig::max_attempts` (queue-wide).

**See also:** [Replay the DLQ](/guides/replay-the-dlq/),
[DLQ and recovery concept](/concepts/dlq-and-recovery/).

### CMQ-021 — DLQ: decode failed

**When:** The consumer's reader couldn't `rmp_serde::from_slice`
the stream entry's `d` field into the consumer's `Job<T>`. The
entry never reaches a handler; it's routed to the DLQ as
`DlqReason::DecodeFailed`.

**Why:** Wire-format mismatch. A producer emitted bytes the
consumer can't decode — either schema drift on a long-running
deploy, hand-written bytes from a debugging session, or a
producer at a newer release than the consumer.

**Fix:** Pin producer and consumer to compatible schemas. The
[history doc](https://github.com/jotarios/chasquimq/blob/main/docs/history.md)
catalogues every deploy-order rule the wire format imposes
(notably: `Job::retry = Some(...)` requires the consumer to be
on slice-8-or-later before the first such payload ships). For a
one-off, `chasqui dlq peek` to inspect the bytes; if they
genuinely came from a third party, add a translator on the
producer side.

**See also:** [Wire format](/reference/wire-format/).

### CMQ-022 — DLQ: malformed entry

**When:** The stream entry is structurally invalid — missing the
`d` payload field, wrong field shape, garbage RESP framing. The
consumer routes it to the DLQ with `DlqReason::Malformed { reason }`,
where `reason` is a short static string (e.g. `"missing payload"`).

**Why:** Something wrote to `{chasqui:<queue>}:stream` that
isn't a ChasquiMQ producer. Direct `XADD`s from `redis-cli`,
external producers using a different framing, or a corrupted
write all land here.

**Fix:** Identify the source. The DLQ entry's `dlq_id` and
`source_id` correlate with the original `XADD`'s id, which the
producer's hostname / client ID is often discoverable from in
your monitoring. If the writer is a debugging tool, stop using
direct `XADD`. If it's a misbehaving in-process writer, fix the
caller.

**See also:** [Wire format](/reference/wire-format/).

### CMQ-023 — DLQ: oversize payload

**When:** The decoded `d` field exceeds
`ConsumerConfig::max_payload_bytes` (default 1 MiB). The
consumer routes the entry to the DLQ with
`DlqReason::OversizePayload` *without* decoding the inner
payload.

**Why:** Someone enqueued a job with a payload larger than the
queue's policy allows. Common cause: a job carrying a full
attached blob instead of a reference (S3 URL, content hash, DB id).

**Fix:** Refactor the producer to enqueue a reference, not the
blob. If the threshold is wrong, raise
`max_payload_bytes` on the consumer config — but pause to ask
whether shipping megabyte payloads through Redis Streams is the
right architecture.

**See also:** [Tune for throughput](/guides/tune-for-throughput/).

### CMQ-024 — DLQ: unrecoverable

**When:** The handler returned an unrecoverable error (see
[`CMQ-011`](#cmq-011--handler-signaled-unrecoverable--dlq)).
The consumer routes the entry to the DLQ with
`DlqReason::Unrecoverable` regardless of the remaining attempt
budget.

**Why:** The handler explicitly told the engine the failure is
terminal — retrying would only burn the budget.

**Fix:** Same as for [`CMQ-011`](#cmq-011--handler-signaled-unrecoverable--dlq).
Find the cause, fix the handler input or logic, and replay if
appropriate.

**See also:** [Route to the DLQ guide](/guides/route-to-dlq/),
[Replay the DLQ guide](/guides/replay-the-dlq/).

## Node shim

### CMQ-100 — Node: feature not supported

**When:** The Node shim threw a `NotSupportedError`. Triggered
by calls like `Queue.getJob`, `Queue.pause`, `Worker.pause`,
`Job.retry`, `Job.update`, `Worker.rateLimit`, or by passing a
sandboxed processor (string / URL path) to the `Worker`
constructor.

**Why:** The feature is intentionally not implemented in v1.
Either the engine doesn't expose the underlying primitive
(stateful job lookup, leaky-bucket rate limiting), or the
operation conflicts with the append-only Streams design (job
update).

**Fix:** Branch on `err.name === 'NotSupportedError'` and either
adopt the supported alternative (close-and-recreate instead of
pause/resume; replay the DLQ instead of `Job.retry`) or pin to
the documented surface. See the v1 scope notes on the
[Worker](/reference/node-api/#worker) and
[Job](/reference/node-api/#job) reference sections for the full
list of stubbed methods.

**See also:** [Node API](/reference/node-api/).

### CMQ-101 — Node: rate-limit

**When:** Reserved. `Worker.rateLimit` throws this in a future
slice when leaky-bucket rate limiting lands in the engine. v1
throws `NotSupportedError` instead.

**Why:** Currently a placeholder for forward compatibility.

**Fix:** Don't catch this code today; it cannot fire in v1.
Implement application-level rate limiting (token bucket, queue
depth thresholding) until engine support arrives.

**See also:** [Node API: Worker](/reference/node-api/#worker).

### CMQ-102 — Node: result wait timeout

**When:** `Job.waitForResult` polled the engine's stored result
key for `timeoutMs` without seeing a value. Throws
`WaitForResultTimeoutError`.

**Why:** Three distinguishable causes, not surfaced separately:

- The handler is still running.
- The handler resolved with `undefined` / `void` (no result key was ever written).
- The worker ran without `WorkerOptions.storeResults: true` (no result key was ever written).
- The result key existed but expired before the wait finished (`resultTtlMs` too short relative to `timeoutMs`).

**Fix:** Verify `storeResults: true` on the worker. Verify the
handler returns a non-undefined value when a result is needed.
Keep `resultTtlMs >= timeoutMs * 2` to avoid the TTL race. For
high-fanout workloads where polling is the cost driver, switch
to subscribing on `QueueEvents` instead.

**See also:** [Job.waitForResult](/reference/node-api/#jobwaitforresultopts),
[Enable result storage guide](/guides/enable-result-storage/).

## Python shim

### CMQ-200 — Python: feature not supported

**When:** The Python shim raised a `NotSupportedError`. Same
spirit as [`CMQ-100`](#cmq-100--node-feature-not-supported) —
the surface gap is by design in v1.

**Why:** Either the engine doesn't expose the underlying
primitive yet, or the operation conflicts with the append-only
Streams design.

**Fix:** Catch with `except NotSupportedError`. Adopt the
supported alternative (replay the DLQ instead of per-job retry,
close-and-recreate instead of pause). Also raised by the shim's
`_meta_from_dict` when a wire payload carries an unrecognized
pattern kind from a future engine release — in that case, upgrade
the Python shim to match the producer's engine version.

**See also:** [Python API](/reference/python-api/).

## Don't see your error here?

[File an issue](https://github.com/jotarios/chasquimq/issues/new)
with the error message verbatim and a minimal repro. We'll add a
code; the numbering scheme has plenty of room. Existing codes
won't be reassigned, so safe to bookmark or wire into log
correlation today.
