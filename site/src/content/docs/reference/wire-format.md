---
title: Wire format
description: The on-Redis byte layout — MessagePack envelope, stream entry fields, idempotent-add header, ack semantics. For the engineer at a redis-cli prompt.
sidebar:
  order: 8
---

This page describes the exact bytes ChasquiMQ writes to Redis.
You don't need it to use the queue — the high-level shims and
the engine API hide every detail. You do need it when you're
debugging at a `redis-cli` prompt, writing a translator, or
auditing what's on the wire.

For the long-form design history (especially around the
`name`-on-the-wire choice), read
[`docs/name-on-wire-design.md`](https://github.com/jotarios/chasquimq/blob/main/docs/name-on-wire-design.md)
in the repo.

## Keys

Every key uses the `{chasqui:<queue>}` Redis Cluster hash tag so
all keys for a queue land on the same slot.

| Key | Type | Purpose |
|---|---|---|
| `{chasqui:<q>}:stream` | Stream | Main work queue. |
| `{chasqui:<q>}:dlq` | Stream | Dead-letter queue. |
| `{chasqui:<q>}:delayed` | Sorted set | Delayed jobs, score = `run_at_ms`. |
| `{chasqui:<q>}:repeat` | Sorted set | Repeatable specs by `next_fire_ms`. |
| `{chasqui:<q>}:repeat:spec:<key>` | Hash | Full repeatable spec body, field `spec`. |
| `{chasqui:<q>}:events` | Stream | Per-queue event broadcast. |
| `{chasqui:<q>}:result:<jobId>` | String | Stored handler return bytes (TTL). |
| `{chasqui:<q>}:progress:<jobId>` | String | Latest progress value (ASCII decimal `u8`, 0..=100) written by `Job.updateProgress`. TTL = `result_ttl_secs`. |
| `{chasqui:<q>}:log:<jobId>` | Stream | Per-job log stream written by `Job.log`. `MAXLEN ~ log_max_stream_len`; `EXPIRE`d to `result_ttl_secs` on every append. One entry per call, field `line`. |
| `{chasqui:<q>}:dlid:<jobId>` | String | Idempotent-schedule dedup marker. |
| `{chasqui:<q>}:didx:<jobId>` | String | Side-index for `cancel_delayed`. |
| `{chasqui:<q>}:promoter:lock` | String | Promoter leader-election lock. |
| `{chasqui:<q>}:scheduler:lock` | String | Scheduler leader-election lock. |
| `{chasqui:<q>}:stalled:lock` | String | Stalled-job detector leader-election lock. |
| `{chasqui:<q>}:stalls:<jobId>` | String | Per-job stall counter. Written by `STALLED_SCAN_SCRIPT` (INCR + EXPIRE) when the detector observes the entry idle past threshold; `DEL`'d on successful ack (`JOB_OK_SCRIPT`) and on DLQ replay (`REPLAY_DLQ_SCRIPT`). Sliding TTL = `idle_threshold_ms * max_stalled_attempts * 2`. |

## Main stream entry

Each entry is written by `XADD` with idempotency. The full shape
is:

```
XADD {chasqui:<q>}:stream
     IDMP <producer_id> <jobId>
     MAXLEN ~ <max_stream_len>
     *
     d <msgpack_payload>
     [n <utf8_name>]
```

- `IDMP <producer_id> <jobId>` — Redis 8.6 idempotent-add header.
  The producer id is a UUID minted on `Producer::connect`. The
  job id is the engine-minted ULID (or the caller-supplied stable
  id when one was provided). A second `XADD` with the same
  `(producer_id, jobId)` pair on the same producer is a no-op at
  Redis. **Scope is the producer**: dedup is bounded to one
  `Producer` instance because each `connect` mints a new UUID.
- `MAXLEN ~ N` — approximate trim to `N` entries. The `~` lets
  Redis trim cheaply; expect actual length to oscillate up to
  several hundred entries above the cap.
- `*` — let Redis assign the entry id (`<ms>-<seq>`).
- `d` — the **payload field**: `rmp_serde`-encoded
  [`Job<T>`](/reference/rust-api/#jobt) bytes.
- `n` — the **name field**: optional UTF-8 dispatch name, capped
  at 256 bytes. Producer omits it entirely for unnamed jobs;
  consumer treats absent and empty as equivalent.

### MessagePack envelope (the `d` field)

The `d` field carries an `rmp-serde`-encoded `Job<T>` struct.
`rmp-serde` encodes structs as **positional arrays** (not maps),
so adding non-trailing fields is a wire-format break. The shape
is:

```
[id, payload, created_at_ms, attempt, retry?]
```

- `id` — `string`. ULID or caller-supplied stable id.
- `payload` — `T`-shaped (whatever the user produced).
- `created_at_ms` — `u64`. Submission time.
- `attempt` — `u32`. 0-indexed on the producer; bumped to
  1-indexed by the consumer before dispatch.
- `retry` — optional `JobRetryOverride`. **Trailing-optional
  with `skip_serializing_if = Option::is_none`**: a job with
  `retry = None` encodes as a 4-element array (the pre-slice-8
  shape); a job with `retry = Some(...)` encodes as 5 elements.
  An older consumer cannot decode a 5-element payload — see the
  [deploy-order rule](#deploy-order-rules) below.

Note that `Job::name` is `#[serde(skip)]` and never appears in
this array. The dispatch name lives at the Redis Streams framing
layer (the `n` field on the stream entry), not inside the
msgpack body. This is what makes name-based metric labels
(`chasquimq_jobs_completed_total{name="..."}`) work without
msgpack-decoding payload bytes — see
[`docs/name-on-wire-design.md`](https://github.com/jotarios/chasquimq/blob/main/docs/name-on-wire-design.md)
for the design discussion.

### `JobRetryOverride` shape

The trailing-optional `retry` slot, when present, encodes a
`JobRetryOverride` struct:

```
[max_attempts, backoff]
```

- `max_attempts` — `Option<u32>`.
- `backoff` — `Option<BackoffSpec>`.

Inner fields are **not** `skip_serializing_if`'d (positional
encoding makes that unsafe), so an inert override
(`{ max_attempts: None, backoff: None }`) still encodes as a
2-element array. See the test
`empty_override_with_no_inner_fields_set_is_inert` in
`chasquimq/src/job.rs:507` for the pin.

`BackoffSpec` encodes as:

```
[kind, delay_ms, max_delay_ms, multiplier, jitter_ms]
```

`kind` is the lowercase string `"fixed"` or `"exponential"`
(byte-identical to the legacy `kind: String` shape). Unknown
strings from a future SDK decode as `BackoffKind::Unknown` and
route through the exponential math at the consumer.

## Repeatable spec body

A repeatable spec is stored in the `spec` field of the
`{chasqui:<q>}:repeat:spec:<key>` hash as an `rmp-serde`-encoded
`StoredSpec` — again a **positional array**. The trailing fields
follow the same one-trailing-optional rule as the `Job<T>`
envelope:

```
[key, job_name, pattern, payload, limit, start_after_ms,
 end_before_ms, fired, missed_fires, retry?]
```

- `missed_fires` is **always serialized** (no `skip_serializing_if`).
  It used to be the lone trailing-optional slot, but `retry` took
  that role, so it became unconditional to keep its position fixed.
  A default-policy (`Skip`) spec therefore encodes as a **9-field**
  array — one field wider than the historical 8-field legacy shape.
  Old readers tolerate the extra trailing field; new readers default
  a missing one to `Skip`.
- `retry` is the new **trailing-optional** (`skip_serializing_if =
  Option::is_none`), the per-fire [`JobRetryOverride`](#jobretryoverride-shape)
  threaded onto every fired job. Omitted entirely when `None`, so a
  spec with no per-fire override still encodes as the 9-field shape.

A positional array can carry **at most one** trailing
conditionally-omitted field: two would create a hole where the
present value lands in the skipped field's slot at decode time.
Making `missed_fires` unconditional pins its position so `retry`
can own the trailing-optional slot cleanly. Specs written before
this layout existed (8-field legacy, or 9-field with a non-default
`missed_fires`) still decode — see the
[deploy-order rule](#deploy-order-rules) below.

## Delayed ZSET member

The delayed ZSET (`{chasqui:<q>}:delayed`) stores raw bytes as
each member with `score = run_at_ms`. The bytes are
**name-prefix-encoded** so the dispatch name survives the
delayed → stream promotion:

```
+----+-------+----------+
| ln | name  | payload  |
+----+-------+----------+
```

- `ln` — 1 byte unsigned, length of the `name` field
  (0..=255).
- `name` — `ln` bytes of UTF-8 dispatch name. Empty when the
  producer added the job without a name.
- `payload` — the rest: an `rmp_serde`-encoded `Job<T>`
  envelope, identical to the `d` field on the main-stream entry.

The promoter strips this prefix server-side (in Lua) and
re-emits the stream entry as `XADD ... d <payload> [n <name>]`,
so the dispatch name lands on the main stream entry's `n` field
verbatim.

## Idempotent-add semantics

Two flavors:

### Immediate path (Redis 8.6 `IDMP`)

`Producer::add` (and the named / bulk / options variants) emits
`XADD ... IDMP <producer_id> <jobId>`. Redis 8.6's IDMP header
makes the second `XADD` with the same `(producer_id, jobId)`
pair a no-op. Three caveats:

1. **Scope is the producer**, not the queue or the cluster. A
   second `Producer::connect` mints a new UUID, so dedup does
   not span process restarts.
2. **Bounded by `IDMP-MAXSIZE`**, an LRU on Redis. High-cardinality
   `jobId` workloads may silently lose dedup for the oldest
   entries.
3. The high-level `Queue.addUnique` shim still requires a
   non-empty `jobId` — without one, `IDMP` has nothing to dedup
   on.

### Delayed path (Lua `SET NX EX`)

`Producer::add_in_with_id` and friends route through a Lua
script that:

1. `SET NX EX` on `{chasqui:<q>}:dlid:<jobId>` with TTL
   `seconds_until_run + 3600`.
2. If the marker took, `ZADD {chasqui:<q>}:delayed`.
3. If the marker didn't take, no-op.

This is **strict and cross-process**: two different `Producer`
instances calling the same idempotent-schedule with the same
`jobId` will only schedule once. The 1h grace on the marker
ensures a delayed producer-retry can't race a successful
promotion. The marker is intentionally *not* deleted on
promotion — the side-index (`:didx:<jobId>`) is, but the dlid
marker stays alive on its TTL.

## Ack semantics

The hot path uses `XACKDEL` (Redis 8.2+) — atomic ack-and-delete
in one round trip — so completed jobs are removed from the stream
in lockstep with the ack. The Lua wrapper used by the
result-backend path is `JOB_OK_SCRIPT`:

```
1. XACKDEL the stream entry from the consumer group.
2. If XACKDEL deleted (ack succeeded), and the resolved Bytes are
   non-empty, SET {chasqui:<q>}:result:<jobId> EX <ttl> with the
   bytes.
3. If XACKDEL returned 0 (a concurrent CLAIM removed the entry
   first), skip the SET. No orphan results when the entry was
   already gone.
```

The producer reads the result back with a single `GET` against
the result key. `None` is returned for three indistinguishable
cases: not-yet-completed, key-expired, and never-written.

## Retry semantics

When a handler returns `Err(HandlerError::new(e))` and the
attempt budget is not exhausted, the consumer:

1. Computes `backoff_ms` from the per-job `JobRetryOverride`,
   falling back to the queue-wide `RetryConfig`.
2. Bumps `attempt` by 1 in the encoded `Job<T>` envelope.
3. Atomically `XACKDEL`s the in-flight stream entry and `ZADD`s
   the new one onto the delayed ZSET in a single Lua round trip
   (`RETRY_RESCHEDULE_SCRIPT`).
4. The dispatch name rides through the retry path — the
   delayed-ZSET member is the same length-prefixed encoding
   used by the producer's delayed path.

The promoter eventually moves the rescheduled job back to the
main stream when the score elapses.

## DLQ relocate

The DLQ relocator atomically moves an entry to the DLQ stream
and acks it from the main group:

```
XADD {chasqui:<q>}:dlq IDMP <producer_id> <source_id>
     MAXLEN ~ <dlq_max_stream_len>
     *
     d <payload>
     reason <reason>
     [detail <detail>]
     [n <name>]
```

The `IDMP <producer_id> <source_id>` pair dedups: the original
stream entry id becomes the dedup id on the DLQ side, so a CLAIM
race that tries to relocate the same entry twice is a no-op on
the second attempt.

The `reason` field is a stable string enum: `retries_exhausted`
(handler failed enough times), `decode_failed` (msgpack envelope
didn't parse), `malformed` (stream entry shape wrong; carries an
optional `detail` field), `oversize_payload`, `unrecoverable`
(handler raised `UnrecoverableError`), `stalled` (stalled-job
detector relocated a worker-crash loop after
`max_stalled_attempts` consecutive idle observations).

The stalled-detector relocate path uses a sibling script
(`RELOCATE_DLQ_PRE_ACKED_SCRIPT`) that skips the XACKDEL gate
because `STALLED_SCAN_SCRIPT` already removed the entry from the
PEL at threshold. The IDMP marker on the XADD is the dedup guard
on this path. The on-wire shape is byte-identical to the gated
script's XADD half, so DLQ subscribers can't tell the two paths
apart.

## Events stream

The `{chasqui:<q>}:events` stream uses **plain ASCII fields**
(not msgpack) so external subscribers can consume it with any
generic Redis client. Fields per event:

| Field | Type | When |
|---|---|---|
| `e` | string | Event name (`"waiting"`, `"active"`, `"completed"`, `"failed"`, `"retry-scheduled"`, `"delayed"`, `"dlq"`, `"drained"`, `"progress"`, `"stalled"`). |
| `id` | string | Job id. Absent for queue-scoped events. |
| `n` | string | Dispatch name. Absent / empty when no name was set. |
| `attempt` | int (decimal string) | Per-attempt events. For `stalled`: current stall count (1-indexed). |
| `backoff_ms` | int | `retry-scheduled`. |
| `delay_ms` | int | `delayed`. |
| `duration_us` | int | `completed`, `failed` — handler wall-clock duration. |
| `reason` | string | `failed`, `dlq` — DLQ reason. |
| `progress` | int (decimal string) | `progress` — clamped `0..=100` value the engine persisted. |
| `prev` | string | `stalled` — always `"active"` (every stalled entry was PEL-resident when the detector saw it). Mirrors the BullMQ `Worker.on('stalled', (jobId, prev))` payload shape. |
| `ts` | int | Emit time (epoch ms). |

Numeric fields are decimal strings on the wire; the Node and
Python subscribers coerce them to numbers at parse time.

## Deploy-order rules

Wire-format compatibility imposes constraints on rolling
deploys:

- **`Job::retry = Some(...)` requires consumer-first deploy.** A
  payload with the `retry` field set encodes as a 5-element
  msgpack array; pre-slice-8 consumers cannot decode it
  (positional decode rejects array-length mismatches). Roll out
  the new consumer everywhere first, then deploy producers that
  emit `retry = Some(...)`. Producing such a payload while a
  stale consumer is still running will route those jobs to the
  DLQ as [`CMQ-021`](/reference/error-codes/#cmq-021--dlq-decode-failed).
- **`StoredSpec.retry = Some(...)` requires scheduler-first deploy.**
  A repeatable spec carrying a per-fire retry override encodes as a
  10-element array (the trailing `retry` slot present); a
  pre-this-change scheduler rejects the array length and cannot
  decode it. Roll out new schedulers everywhere first, then start
  writing retry-bearing specs. A spec is re-encoded on every
  `upsert_repeatable`, so a stale scheduler only breaks if a *new*
  retry-bearing spec is written while it is still running — the same
  contract as `Job::retry`. (Before this change the deploy-order
  trigger was a non-`Skip` `RepeatableSpec.missed_fires`, which used
  to be the trailing-optional slot; it is now always present, so it
  no longer changes the array length on its own.)
- The default `Job::retry = None` and a retry-less repeatable spec
  encode identically to the pre-existing wire shape — for the spec,
  that's the 9-field default-policy shape an old reader still decodes
  — so the steady-state hot path is back-compatible in both
  directions.

The full deploy-order log lives in
[`docs/history.md`](https://github.com/jotarios/chasquimq/blob/main/docs/history.md).

## Inspecting at the CLI

To watch the bytes flow without writing code:

```bash
# Latest 5 entries on the main stream
redis-cli XRANGE '{chasqui:emails}:stream' - + COUNT 5

# Pending entries for the default group
redis-cli XPENDING '{chasqui:emails}:stream' default - + 10

# DLQ inspection (or use `chasqui dlq peek`)
redis-cli XRANGE '{chasqui:emails}:dlq' - + COUNT 5

# Delayed ZSET, oldest first
redis-cli ZRANGE '{chasqui:emails}:delayed' 0 4 WITHSCORES

# Tail the events stream
redis-cli XREAD BLOCK 0 STREAMS '{chasqui:emails}:events' '$'
```

The `d` field on each entry is binary msgpack — pipe it through
a small Python or Node script with `@msgpack/msgpack` /
`msgpack-python` to decode. The
[`chasqui dlq peek`](/reference/cli/#chasqui-dlq-peek) and
[`chasqui events`](/reference/cli/#chasqui-events) subcommands
do this rendering for you.

## See also

- [Rust API: Job types](/reference/rust-api/#job-types) — the canonical envelope shape.
- [Concepts: Redis Streams primer](/concepts/redis-streams-primer/) — what `XADD` / `XREADGROUP` / `XACK` actually do.
- [`docs/name-on-wire-design.md`](https://github.com/jotarios/chasquimq/blob/main/docs/name-on-wire-design.md) — why `name` lives at the framing layer instead of inside the msgpack body.
- [`docs/history.md`](https://github.com/jotarios/chasquimq/blob/main/docs/history.md) — every wire-format slice with deploy-order context.
