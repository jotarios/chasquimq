# Phase 3 — Node.js bindings via NAPI-RS

Status: design draft, v2 (BullMQ-compat scope). Implementation has not started.
This doc locks in the shape of the JS surface, the FFI seam, the
BullMQ-compat layer, and the build/release plan so the implementation can
land in narrow slices without revisiting first principles.

ChasquiMQ's product reason to exist is "fewer worker CPU cycles per job than
BullMQ on the same Redis." Phase 3 has to prove that claim *in BullMQ's own
language*, and ideally as a **drop-in `import` swap** — the migration
friction is the wedge. Two layers ship from one npm package: a BullMQ-compat
layer (default) and a `/native` surface that mirrors the Rust API for power
users. Anything that meaningfully bloats the per-job FFI cost is a Phase 3
regression, even if it ships ergonomic API.

## 1. Goal & non-goals

**Goal.** Ship `chasquimq` (npm) as a **drop-in BullMQ replacement for the
perf-curious**: `import { Queue, Worker } from "chasquimq"` should be a
code-mod-free swap for ~95% of BullMQ users (the API shape is identical for
the common path), while `import { Producer, Consumer, Promoter } from
"chasquimq/native"` remains available for power users who want the native
Rust-mirroring surface and the throughput it unlocks.

The hard win condition is unchanged: ≥3× BullMQ on `queue-add-bulk` and
`worker-concurrent` on the same M3 host as `benchmarks/baseline-bullmq.md`,
with a JS handler in the loop. The BullMQ-compat layer must hit that bar via
the same `chasquimq/native` surface underneath — the compat layer is a thin
TypeScript shim, not a parallel engine.

**Non-goals.**
- Python bindings (Phase 4, PyO3).
- CLI / TUI dashboard (Phase 4).
- Browser / Deno / WASM build. Node + Bun via N-API only.
- Bun-specific FFI fast path. Bun runs N-API; one binary covers both.
- Re-implementing any engine logic in JS. JS owns the handler function and
  the BullMQ-compat shim; nothing else.
- Bug-for-bug BullMQ parity. We match the public API shape, **not** internal
  Redis key layout, internal queue semantics, or every optional argument. See
  §3 for the explicit not-supported list.
- Redis Cluster from JS in the v1 cut — see "Risks & open questions".

## 2. Two-layer architecture

The npm package ships two import paths from a single binary. Both routes hit
the same Rust engine (`chasquimq` crate), the same Redis key layout
(`{chasqui:<queue>}:<suffix>`), and the same wire format (a `Job<T>` MessagePack
blob — see §6). The split is purely at the JS-surface layer.

```
                        ┌─────────────────────────────────────────────┐
                        │  npm package: chasquimq                     │
                        │                                             │
   import { Queue,      │   ┌─────────────────────────────────────┐   │
     Worker, Job,       │   │  BullMQ-compat layer (TS shim)      │   │
     QueueEvents }      │──▶│  - Queue / Worker / Job             │   │
   from "chasquimq"     │   │  - QueueEvents / JobScheduler       │   │
                        │   │  - @msgpack/msgpack on data:any     │   │
                        │   │  - JobsOptions ↔ engine call map    │   │
                        │   └────────────────┬────────────────────┘   │
                        │                    │                        │
                        │                    ▼ (calls into native)    │
                        │   ┌─────────────────────────────────────┐   │
   import { Producer,   │   │  Native layer (#[napi] classes)     │   │
     Consumer,          │──▶│  - Producer / Consumer / Promoter   │   │
     Promoter }         │   │  - opaque Buffer payloads           │   │
   from "chasquimq      │   │  - 1:1 mirror of Rust surface       │   │
     /native"           │   └────────────────┬────────────────────┘   │
                        │                    │                        │
                        │                    ▼  (N-API → Rust)        │
                        │   ┌─────────────────────────────────────┐   │
                        │   │  chasquimq-node (cdylib, this PR)   │   │
                        │   │  chasquimq engine (Rust)            │   │
                        │   └─────────────────────────────────────┘   │
                        └─────────────────────────────────────────────┘
```

The default import (`from "chasquimq"`) is the BullMQ-compat layer because
that's the wedge — most readers will be migrating from BullMQ, and the cost of
having to type `/native` is the right friction to push the throughput-curious
to the throughput-aware surface. The native layer is documented and supported,
not a private implementation detail; it just isn't the first thing in the
README.

**No third layer.** The compat shim does **not** call the engine through any
intermediate JS class — it constructs `Producer` / `Consumer` from
`/native` directly, holds them as private fields, and forwards. Two layers,
one binary, one engine.

## 3. BullMQ feature scope (CORE / DEFERRED / NOT-SUPPORTED)

The compat layer's job is to be the path of least resistance for migrating
BullMQ apps. Three tiers, scoped explicitly so users can read this doc and
decide whether to migrate today.

### 3.1 CORE — ships in v1 of the compat layer

**Queue.** Constructor + `add` / `addBulk` / `getJob` / `getJobs` /
`getJobCounts` / `pause` / `resume` / `close` / `remove` / `obliterate` /
`drain` / `clean` / `getJobState`. Connection options accepted: `host`, `port`,
`password`, `db`, `username`, `tls` (forwarded to `fred`); `maxRetriesPerRequest`
**accepted-and-ignored** with a one-time deprecation warning (we manage retries
inside the engine — the BullMQ knob doesn't map).

**Worker.** Constructor accepts inline async processor `(job) => Promise<R>`,
plus `run()` / `close()` / `pause()` / `resume()`. EventEmitter-compatible
events: `active`, `completed`, `failed`, `error`, `progress`, `stalled`,
`drained`, `closed`, `ready`. Processor return value is captured as
`returnvalue` (msgpack-encoded into the events stream — see slice 9).

**Job.** Read-only fields: `id`, `name`, `data`, `opts`, `attemptsMade`,
`progress`, `returnvalue`, `failedReason`, `timestamp`, `processedOn`,
`finishedOn`, `delay`, `priority`. Methods: `updateProgress(n | obj)`,
`getState()`, `remove()`, `retry()`, `discard()`, `update(data)`,
`isCompleted()`, `isFailed()`, `isDelayed()`, `isActive()`, `isWaiting()`,
`toJSON()`. Caveats on `progress` and `update(data)` are spelled out in §14.

**JobsOptions.** `delay`, `attempts`, `backoff` (`{ type: 'fixed'|'exponential',
delay }`), `removeOnComplete` (`true` / `false` / `{ count }`), `removeOnFail`
(`true` / `false` / `{ count }`), `priority` (best-effort — see §14),
`jobId` (stable id; routes to `add_with_id` / `add_in_with_id`), `timestamp`,
`lifo` (accepted-with-warn — Streams are FIFO).

**QueueEvents.** Cross-process subscriber over a per-queue Redis Stream
(`{chasqui:<queue>}:events`, see slice 9). Events: `waiting`, `active`,
`completed`, `failed`, `progress`, `delayed`, `stalled`, `removed`, `drained`,
`retries-exhausted`, `error`. Same `EventEmitter` API as `bullmq`'s
`QueueEvents` so existing `qe.on('completed', ...)` code is a copy-paste.

**Repeatable jobs.** `Queue.upsertJobScheduler(id, repeat, jobTemplate)` with
`repeat: { pattern: '*/5 * * * *' }` (cron) or `repeat: { every: 60_000 }`
(interval), backed by slice 10. `removeJobScheduler(id)` and
`getJobSchedulers()` round it out. We aim for BullMQ-format-compatible
scheduler ids (a hash of `name + cron pattern`) so a side-by-side migration
doesn't double-fire.

**`UnrecoverableError` sentinel.** Re-exported as
`import { UnrecoverableError } from "chasquimq"`. Throwing it from a processor
short-circuits to DLQ without consuming the remaining retry budget — this maps
to the engine's `HandlerError::Fail` variant from §11.

### 3.2 DEFERRED — present-but-throw `NotSupportedError`

Each of these is a named export (or option key) that exists in the type
signature but throws `NotSupportedError` (a class re-exported from
`"chasquimq"`) at runtime with a clear message and a link to a tracking
GitHub issue. The point is failing loud at the right call site, not silently
running broken code:

- `FlowProducer` and `Queue.addFlow()` (parent/child DAGs).
- `JobsOptions.parent` / `JobsOptions.dependencies` on `Queue.add()` (the
  per-job parent/child opts, even outside a `FlowProducer`).
- Sandboxed processors — passing a string path to `new Worker(name, '/path/to/processor.js')`.
  The compat layer detects the string-typed arg and throws.
- Advanced repeat options on `JobScheduler`: `repeat.limit`, `repeat.startDate`,
  `repeat.endDate`, `repeat.tz`, `repeat.immediately` (we ship `pattern` and
  `every` only in v1).
- Passing an existing `ioredis` instance via `connection: ioredisInstance`. We
  use `fred`. Accept a `RedisOptions` plain object; throw on a non-plain
  `connection` value.
- Custom backoff strategies (`backoff: { type: 'custom', strategy: fn }`). The
  engine's exponential backoff is configured queue-wide; per-job custom
  functions don't cross the FFI boundary cleanly.
- Deduplication beyond `jobId`. BullMQ's `deduplication: { id, ttl }` opts are
  rejected; use `jobId` (which routes to our idempotent
  `add_with_id` / `add_in_with_id` paths and dedupes inside the engine).

### 3.3 NOT-SUPPORTED — documented upfront, will not ship

- `keyPrefix` on connection options. ChasquiMQ uses `{chasqui:<queue>}` hash
  tags by design (Redis Cluster correctness — see CLAUDE.md). A user-supplied
  prefix would either break cluster routing or require a parallel key layout;
  not a v1 trade-off worth making. Throws on construction with a one-line
  explanation pointing at the cluster-correctness rationale.
- `getWaitingChildren` / `moveToWaitingChildren`. Children-of-flow APIs;
  meaningless without `FlowProducer`.
- `FlowProducer` DAGs in any form. Listed separately from "DEFERRED" because
  we don't currently plan to ship it — DAG semantics on Streams are a
  fundamental rewrite, not a slice.

### 3.4 Tier table (one-glance summary)

| Surface area                     | Tier          | Ships in v1? |
|----------------------------------|---------------|:------------:|
| `Queue` (basics)                 | CORE          | ✅           |
| `Worker` (inline processor)      | CORE          | ✅           |
| `Job` read fields + `toJSON`     | CORE          | ✅           |
| `JobsOptions.delay`              | CORE          | ✅           |
| `JobsOptions.attempts` / `backoff` | CORE        | ✅ (slice 8) |
| `JobsOptions.removeOnComplete`   | CORE          | ✅           |
| `JobsOptions.removeOnFail`       | CORE          | ✅ (DLQ-skip) |
| `JobsOptions.priority`           | CORE (best-effort) | ⚠️ ignore-with-warn |
| `JobsOptions.jobId`              | CORE          | ✅           |
| `JobsOptions.lifo`               | CORE          | ⚠️ accept-with-warn |
| `QueueEvents`                    | CORE          | ✅ (slice 9) |
| Repeatable jobs (cron / every)   | CORE          | ✅ (slice 10) |
| `UnrecoverableError`             | CORE          | ✅           |
| `FlowProducer` / DAGs            | NOT-SUPPORTED | ❌ (throws)  |
| `JobsOptions.parent` / `dependencies` | DEFERRED  | ❌ (`NotSupportedError`) |
| Sandboxed processor (string path) | DEFERRED     | ❌ (`NotSupportedError`) |
| Custom backoff strategy fn       | DEFERRED      | ❌ (`NotSupportedError`) |
| Advanced repeat (`limit`/`tz`/…) | DEFERRED      | ❌ (`NotSupportedError`) |
| Existing `ioredis` instance      | DEFERRED      | ❌ (`NotSupportedError`) |
| `keyPrefix`                      | NOT-SUPPORTED | ❌ (throws)  |
| `getWaitingChildren`             | NOT-SUPPORTED | ❌ (throws)  |

## 4. Engine slices required to unlock the compat layer

The compat layer is ~95% a TypeScript shim, but three engine capabilities are
load-bearing for the CORE tier and don't exist in the engine yet. They land
**in the engine crate**, ahead of the binding work, and are independently
useful to pure-Rust users. Each is its own slice / PR, marked `feat(engine):`
or finer scopes; existing slices 1–7 are the precedent for sizing.

### 4.1 Slice 8 — per-job retry overrides

**Why.** BullMQ lets you say `queue.add('job', data, { attempts: 5, backoff:
{ type: 'exponential', delay: 1000 } })` per call. ChasquiMQ today only takes
queue-wide `RetryConfig` on `ConsumerConfig`. Without per-job overrides, a
direct BullMQ swap silently demotes user intent.

**What.** Extend the encoded `Job<T>` envelope (sibling to `attempt` /
`created_at_ms`) with two optional fields:

```rust
struct Job<T> {
    id: JobId,
    payload: T,
    attempt: u32,
    created_at_ms: u64,
    // NEW (slice 8):
    max_attempts: Option<u32>,        // overrides ConsumerConfig.max_attempts
    backoff: Option<BackoffSpec>,     // overrides ConsumerConfig.retry
}

enum BackoffSpec {
    Fixed { delay_ms: u32 },
    Exponential { delay_ms: u32 },
}
```

The worker hot path checks the per-job override first, falls back to
`ConsumerConfig` defaults if absent. The retry-relocator Lua script preserves
both fields on re-encode (they're already round-tripping through the same
encode/decode path; the script is opaque to the payload).

**Wire compat.** Keep these `Option` so an existing-format payload (no
override) decodes without breakage. msgpack handles the missing-field case
natively.

**Producer surface.** `Producer::add_with_options(payload, RetryOverrides {
max_attempts, backoff })` — the compat layer is the only consumer of this in
v1, but the Rust API is the right shape for native users too.

### 4.2 Slice 9 — events stream

**Why.** `QueueEvents` is BullMQ's cross-process pub/sub for queue lifecycle
events (job completed in worker A, queue handler in process B reacts).
Without it the `Queue.add(...).waitUntilFinished(qe)` BullMQ idiom doesn't
work, and that idiom is deeply baked into a lot of BullMQ apps.

**What.** A per-queue Redis Stream `{chasqui:<queue>}:events`, capped at
`MAXLEN ~ N` (configurable, default ~100k), with structured entries written
by:

- The consumer (after `XACK`): `completed` (carries `returnvalue` bytes —
  see §6), `failed` (carries `failedReason` text + `attemptsMade`),
  `progress` (from `Job.updateProgress`), `active` (when a worker pulls), and
  `stalled` (CLAIM-recovery path).
- The promoter: `delayed` (when a delayed job is enqueued), `waiting`
  (when a delayed job promotes into the stream).
- The DLQ relocator: `retries-exhausted` and `removed` (after `XADD` to DLQ).
- The producer (`Queue.add`): `waiting` immediately on add (matches BullMQ's
  semantics for non-delayed jobs).
- The worker pool: `drained` (when its in-flight queue empties).

`QueueEvents` on the JS side is `XREAD BLOCK` from a small consumer-less
read position, no consumer group — every subscriber sees every event.
Subscribers track their own `last-id` cursor.

**Cost.** One extra `XADD` per terminal event. Pipeline-batchable on the
consumer ack flusher (already pipelined). Microbench before slice 9 lands
to validate the headline `worker-concurrent` number isn't damaged; if it is,
add an opt-in `events_enabled: bool` to `ConsumerConfig` and gate the writes.
The `chasquimq/native` API can leave it off; the BullMQ-compat layer turns
it on by default because BullMQ users expect it.

### 4.3 Slice 10 — repeatable jobs (`JobScheduler`)

**Why.** `Queue.upsertJobScheduler('nightly', { pattern: '0 3 * * *' }, {
name: 'nightly-rollup', data: {...} })` is one of the top-three BullMQ
features by usage. A migration story without it is "you also need to keep
running BullMQ for cron." We can't ship that.

**What.**

- `{chasqui:<queue>}:repeat` ZSET, score = next-fire-time-ms.
- `{chasqui:<queue>}:repeat:specs` hash, mapping scheduler-id →
  msgpack-encoded `RepeatSpec { pattern: Cron | Every, name, data, opts }`.
- `Producer::upsert_repeatable(id, spec)` API: writes both keys atomically
  (Lua), recomputes next fire time from spec.
- The promoter, in addition to its current delayed-set work, also processes
  the `:repeat` ZSET on each tick: due entries → enqueue a regular `Job<T>`
  in the stream (using the spec's `name` / `data` / `opts`) **and** ZADD
  next-fire-time back into the `:repeat` ZSET. Both ops in one Lua script
  for cron-tick atomicity.
- Cron expression parsing in the Rust producer with the `cron` crate
  (mature, widely used). On the JS side, `cron-parser` (already a BullMQ
  dep) is fine; we may not even need it if we forward the pattern string
  unchanged through to Rust.

**Failure mode.** A restart-during-tick must not skip a fire window. The
promoter's existing `SET NX EX` leader election + the Lua atomicity above
covers the "exactly one promoter fires the cron" case; the
"don't-fire-twice-on-clock-skew" case relies on the existing `redis.call('TIME')`
pattern from slice 1.

**Scheduler id format.** BullMQ uses
`SHA1(name + cron pattern)` (truncated). We match the algorithm so a paranoid
operator can run BullMQ + ChasquiMQ side-by-side during migration without
the same scheduler firing twice — useful enough during cutover that it's
worth one line of code to align.

### 4.4 Slice ordering

10 → 9 → 8 is *not* the right order. The right order is **8 → 9 → 10**:
slice 8 is independently shippable and has the smallest blast radius (the
encoded payload carries optional fields), slice 9 builds on the consumer's
already-pipelined ack path, slice 10 needs the promoter's existing leader
election and is the largest. Each is its own PR; the compat layer (slice 11+
in this doc's slicing, see §15) lands after all three.

## 5. JobsOptions → ChasquiMQ engine call map

This is the implementation contract for the JS shim author. Read top-to-bottom
when writing `Queue.add(name, data, opts)`; the right-hand column is the
exact engine call to issue.

| BullMQ option (`JobsOptions`)                   | Engine call                                        | Notes |
|-------------------------------------------------|----------------------------------------------------|-------|
| (none — bare `add`)                             | `Producer::add(payload)`                           | Non-delayed, non-idempotent |
| `delay: ms`                                     | `Producer::add_in(Duration::from_millis(ms), payload)` | |
| `delay: ms` + `timestamp` set                   | `Producer::add_at(ms_at, payload)`                 | `ms_at = timestamp + delay` |
| `jobId: "X"`                                    | `Producer::add_with_id("X", payload)`              | Idempotent on `IDMP <id>` |
| `jobId: "X"` + `delay: ms`                      | `Producer::add_in_with_id("X", Duration, payload)` | Slice 6 already shipped |
| `attempts: N` and/or `backoff: {...}`           | `Producer::add_with_options(payload, RetryOverrides {...})` | Engine slice 8 |
| `attempts:` + `delay:`                          | `Producer::add_in_with_options(...)`               | Slice 8 + the existing delayed path; one new method on Producer |
| `removeOnComplete: true`                        | (no extra call) `XACKDEL` already deletes on ack   | This is our default — BullMQ's `false` is what costs them |
| `removeOnComplete: false`                       | (no extra call) `XACK` only, retain entry          | Engine flag on `ConsumerConfig` to disable `XACKDEL` per call — small API addition |
| `removeOnComplete: { count: N }`                | `XADD MAXLEN ~ N` already caps stream length       | Reuse existing `max_stream_len` config; per-job override is moot since the cap is queue-wide |
| `removeOnFail: true`                            | DLQ-skip path (TODO — small engine flag)           | When attempts exhausted, drop entry instead of relocate to DLQ |
| `removeOnFail: false`                           | (default) DLQ relocate                             | |
| `removeOnFail: { count: N }`                    | `dlq_max_stream_len: N` (existing)                 | Already capped (slice 3) |
| `priority: N`                                   | (no engine call) ignore + emit one-time warn       | See §14 — Streams are FIFO; not lying about priority is the right call |
| `lifo: true`                                    | (no engine call) ignore + emit one-time warn       | Same — FIFO Stream invariant |
| `repeat: { pattern: cron }` or `{ every: ms }`  | `Producer::upsert_repeatable(id, spec)` (slice 10) | |
| `parent: {...}` / `dependencies: [...]`         | (compat shim) throw `NotSupportedError`            | DEFERRED tier |

`bulkAdd` / `addBulk` route through `Producer::add_bulk` /
`Producer::add_in_bulk` / `Producer::add_in_bulk_with_ids` as appropriate
(detect at the shim layer based on which fields each entry has). Mixed-mode
bulks (some delayed, some immediate) split into two engine calls; this is the
only place the shim does any real work.

`Worker(name, processor, opts)` constructs a `Consumer` underneath with
`queue_name = name`, `concurrency = opts.concurrency ?? 1` (BullMQ defaults
to 1; ours is 100 in the native API, but **we follow BullMQ's default in the
compat layer** to match user expectation). All other knobs (`lockDuration`,
`stalledInterval`, etc.) map onto `ConsumerConfig`'s claim-recovery / ack
settings; full table is implementation detail and lives in the binding's
source comments, not this doc.

## 6. MessagePack at the JS boundary

The native API (§9) keeps payloads opaque (`Buffer` in, `Buffer` out) for
exactly the perf reason laid out there: per-job decode + JS-object
materialization is the FFI cost the PRD calls out as the bottleneck. The
BullMQ-compat layer can't ship that — `data: any` is BullMQ's contract — so
the compat layer **does** pay that cost, *on the JS side*, where it's
visible and skippable.

### 6.1 Encoding strategy

The compat layer lists [`@msgpack/msgpack`](https://github.com/msgpack/msgpack-javascript)
as a top-level (non-optional) dependency. This is the only npm dep the package
adds beyond the platform-binary `optionalDependencies` from §12.

```ts
// In the compat shim's Queue.add(name, data, opts):
import { encode } from "@msgpack/msgpack";
import { Producer } from "chasquimq/native";

const payload = encode({ name, data });   // returns Uint8Array
await producer.add(Buffer.from(payload.buffer, payload.byteOffset, payload.byteLength));
```

The `Buffer.from(uint8.buffer, offset, length)` form is **zero-copy** —
it constructs a `Buffer` view over the same underlying `ArrayBuffer` that
`@msgpack/msgpack` allocated, no second allocation. (Confirm in slice 11
microbench; `node --inspect` heap profile makes this visible.)

On the consumer side:

```ts
// In the compat shim's Worker constructor's consumer.run(handler):
import { decode } from "@msgpack/msgpack";
import { Consumer } from "chasquimq/native";

consumer.run(async (job) => {
  const { name, data } = decode(job.payload) as { name: string; data: unknown };
  const bullmqJob = new Job(name, data, ...job, ...);
  return userProcessor(bullmqJob);
});
```

### 6.2 Wire-format compatibility with the native API

A `Job<T>` produced by `Queue.add()` (compat layer) has:
- the engine's standard envelope (id, attempt, created_at_ms — the Rust
  `Job<T>` struct from §9), with
- the inner `payload: T` slot containing msgpack bytes encoding the JS object
  `{ name: string, data: any }`.

A native consumer with `T = serde_json::Value` (or `T = HashMap<String,
rmpv::Value>`) decodes the inner payload and gets `{ name, data }` back as a
typed Rust value. **This is the migration story for users who want to start
with the BullMQ-compat layer and gradually move handlers to Rust** without
changing the producer side first. The two layers are wire-compatible by
construction.

The reverse direction works too: a Rust `Producer<MyJob>` can publish jobs
that a JS `Worker` consumes, as long as `MyJob` is wrapped in `{ name,
data }` shape on the Rust side. Document the wrapper helper.

### 6.3 Type round-trip honesty

What msgpack handles cleanly that JSON doesn't:
- `bigint` → tagged ext type (round-trip, no precision loss at >2^53).
- `Date` → tagged ext type (round-trip).
- `Uint8Array` / `Buffer` → native bin8/bin16/bin32 type (round-trip,
  zero-copy on decode).
- `Map` / `Set` → tagged ext type (round-trip with `extensionCodec` opt-in;
  document the snippet).
- `undefined` → `nil` (note: `undefined` is preserved as `null` on decode,
  same as JSON; flag in docs).

What msgpack doesn't handle, same as JSON / same as BullMQ:
- Functions (lost — there's no portable serialization).
- Symbols (lost).
- Circular references (throws — same as `JSON.stringify`).
- Class instances (decoded as plain objects — prototype is lost, same as
  JSON).

This matches BullMQ's existing limitations exactly for the common subset.
The bigint / Date / Buffer wins are the upgrade story; tell that story in the
README.

### 6.4 Throughput trade-off (msgpack vs JSON at the JS boundary)

[`@msgpack/msgpack`](https://github.com/msgpack/msgpack-javascript) benchmarks
land in the same order-of-magnitude as native `JSON.stringify` /
`JSON.parse` for typical job payloads:

- **Numeric / binary-heavy payloads:** msgpack is ~1.5–2× faster than JSON
  *and* smaller on the wire (varint encoding, native binary type, no string
  escaping). Net win on every dimension.
- **Tiny string-only payloads** (e.g. `{ to: "x@y.com", subject: "hi" }`):
  approximately wash. msgpack's per-field type byte costs a few nanoseconds;
  V8's `JSON.stringify` is hand-tuned C++. The decode side is closer (the
  msgpack JS decoder is plain JS).
- **Large mixed payloads:** msgpack wins on size, and size matters on the
  Redis wire — that's the whole reason the engine uses it on the Rust side.

Net: it's not a regression on the hot path, and on the typical payload it's
a small win. The native API's `Buffer`-passthrough remains the strict
upper bound for users who care to claim it.

## 7. Crate & package layout

New crate at `crates/chasquimq-node/` (peer of `chasquimq`,
`chasquimq-metrics`, `chasquimq-bench`). It depends on `chasquimq` as a path
dep and exposes a thin N-API class wrapper. **No queue logic lives here** —
this crate is glue and nothing else. Following the same rule as
`chasquimq-metrics`: keep observability/FFI baggage out of the engine crate so
pure-Rust users don't pay for it.

```
crates/
  chasquimq/                 # engine (unchanged)
  chasquimq-metrics/         # metrics-rs adapter (unchanged)
  chasquimq-node/            # NEW — napi-rs bindings
    Cargo.toml               # crate-type = ["cdylib"], napi = "3"
    build.rs                 # napi-build
    package.json             # @napi-rs/cli scripts, optionalDependencies
    src/lib.rs               # #[napi] classes: Producer, Consumer, Promoter
    src/payload.rs           # Buffer <-> Bytes bridging
    src/handler.rs           # ThreadsafeFunction wrapping
    index.d.ts               # generated by @napi-rs/cli
    npm/                     # per-platform sub-packages (generated)
```

**npm package name: `chasquimq`** (unscoped). Justification: the Rust crate is
`chasquimq`, the engine is `chasquimq`, the bench harness is `chasquimq-bench`
— there is no second SDK to disambiguate yet. Phase 4's Python package is
`chasquimq` on PyPI for the same reason; ecosystem-namespace collisions across
package registries are not real (npm and PyPI don't share names). If we
later ship `@chasquimq/cli` (Phase 4 dashboard) the scope appears then.
The platform sub-packages take the conventional N-API form
(`chasquimq-darwin-arm64`, `chasquimq-linux-x64-gnu`, etc.).

**Tokio runtime ownership.** napi-rs ships a built-in tokio runtime managed
per-process; `#[napi]` async functions schedule onto it. Use it. Do **not**
build a second runtime inside `chasquimq-node` — it works but multiplies
worker threads, blows up the thread count visible to schedulers, and makes
backpressure reasoning harder when both runtimes are spinning. The engine
already uses `tokio::spawn` everywhere and doesn't care which runtime hosts
it.

## 8. JS-facing API surface (native layer)

The JS surface is a 1:1 mirror of the Rust surface, minus the `T:
Serialize + DeserializeOwned` generic. JS payloads are byte buffers from the
binding's perspective — see "Payload bridging" for the trade-off.

### Producer

```ts
export interface ProducerConfig {
  queueName?: string;          // default: "default"
  poolSize?: number;           // default: 8
  maxStreamLen?: number;       // default: 1_000_000
  maxDelaySecs?: number;       // default: 30 days
}

export class Producer {
  static connect(redisUrl: string, config?: ProducerConfig): Promise<Producer>;

  add(payload: Buffer): Promise<string>;
  addWithId(id: string, payload: Buffer): Promise<string>;
  addBulk(payloads: Buffer[]): Promise<string[]>;

  addIn(delayMs: number, payload: Buffer): Promise<string>;
  addAt(runAtMs: number, payload: Buffer): Promise<string>;
  addInBulk(delayMs: number, payloads: Buffer[]): Promise<string[]>;

  // Forward-looking — assumes engine slice 6 (cancel/reschedule) has landed.
  // If it hasn't by the time Phase 3 starts, drop these from the v1 surface
  // and add them in a 3.x point release; do not stub them.
  addInWithId(id: string, delayMs: number, payload: Buffer): Promise<string>;
  cancelDelayed(id: string): Promise<boolean>;

  // DLQ tooling
  peekDlq(limit: number): Promise<DlqEntry[]>;
  replayDlq(limit: number): Promise<number>;
}

export interface DlqEntry {
  dlqId: string;
  sourceId: string;
  reason: string;
  detail: string | null;
  payload: Buffer;
}
```

### Consumer

```ts
export interface RetryConfig {
  initialBackoffMs?: number;   // default: 100
  maxBackoffMs?: number;       // default: 30_000
  multiplier?: number;         // default: 2.0
  jitterMs?: number;           // default: 100
}

export interface ConsumerConfig {
  queueName?: string;
  group?: string;
  consumerId?: string;
  batch?: number;              // default: 64
  blockMs?: number;            // default: 5_000
  concurrency?: number;        // default: 100 — MAX in-flight JS handlers
  maxAttempts?: number;        // default: 3
  ackBatch?: number;           // default: 256
  ackIdleMs?: number;          // default: 5
  shutdownDeadlineSecs?: number;
  retry?: RetryConfig;
  delayedEnabled?: boolean;
  // (others mirror ConsumerConfig 1:1; see config.rs)
}

export interface Job {
  id: string;
  payload: Buffer;             // raw msgpack bytes; decode in-handler
  attempt: number;             // 1-indexed, matches JobOutcome
  createdAtMs: number;
}

export type JobHandler = (job: Job) => Promise<void>;

export class Consumer {
  constructor(redisUrl: string, config?: ConsumerConfig);
  run(handler: JobHandler): Promise<void>;
  shutdown(): Promise<void>;   // resolves once drain completes or deadline hits
}
```

### Promoter

```ts
export interface PromoterConfig {
  queueName?: string;
  pollIntervalMs?: number;     // default: 100
  promoteBatch?: number;       // default: 256
  maxStreamLen?: number;
  lockTtlSecs?: number;
  holderId?: string;
}

export class Promoter {
  constructor(redisUrl: string, config?: PromoterConfig);
  run(): Promise<void>;
  shutdown(): Promise<void>;
}
```

`MetricsSink` is **not** exposed in v1. The whole point of `chasquimq-metrics`
is to keep observability infra out of the engine; we should not undo that by
making JS users build a sink. Phase 3.5 can wire a `prom-client` adapter once
we know what people actually ask for.

## 9. Payload bridging (native API)

This is the load-bearing decision for whether Phase 3 hits its number.

There are two options for how a JS payload crosses the FFI seam:

| Option | What JS sees | Per-job cost | Allocations |
|---|---|---|---|
| **A. Opaque `Buffer` (recommended)** | JS encodes msgpack itself (or whatever); engine treats payload as bytes | One copy in N-API to Rust `Bytes` (or `Buffer::from_napi_value` directly into the engine's `Vec<u8>`) | One per job, sized to payload |
| **B. Auto-decode to JS object** | Engine deserializes msgpack on the Rust side, then re-walks into a JS object via `napi_value` | msgpack decode + N-API object construction (string interning, `napi_create_object`, per-field sets) per job | Many; proportional to payload field count |

We ship **A** as the throughput path. Justification:

- The PRD's win condition is "3–5× BullMQ throughput, ≥50% less worker CPU."
  Option B's per-job decode + object materialization is exactly the kind of
  per-job CPU work the PRD calls out as the bottleneck of legacy queues.
- BullMQ already pays this cost (it serializes JSON on the Node side). If
  ChasquiMQ-node also pays a per-job decode cost, we burn the structural
  advantage at the FFI boundary.
- For ergonomic users we ship a tiny helper:
  ```ts
  import { decodePayload } from "chasquimq";
  const data = decodePayload(job.payload); // msgpack-lite or @msgpack/msgpack
  ```
  This is opt-in per handler. Apps that already think in `Buffer` (image
  pipelines, protobuf jobs, pre-serialized work) skip it entirely.

The Rust producer's MessagePack encoding (`Job<T>` struct → bytes) becomes
**JS's responsibility** for the JS-produced path: callers msgpack-encode their
own payload buffer, then pass it to `producer.add(buf)`. The engine wraps it
in the standard `Job` envelope on the Rust side (id, attempt, createdAtMs)
just as the Rust producer does today — only the inner `payload: T` slot is
opaque bytes from N-API's perspective.

## 10. Handler dispatch

The engine's `Consumer::run` takes `H: Fn(Job<T>) -> Future`. From JS, the
handler is a `(job) => Promise<void>`. The bridge uses
[`napi::threadsafe_function::ThreadsafeFunction`](https://napi.rs/docs/concepts/threadsafe-function)
(TSFN) so a tokio worker thread on the Rust side can call into the libuv loop
where the JS function actually runs.

Sketch of the dispatch path (no code, just the seam):

1. `Consumer::run` receives the JS handler. Wrap it in a `TSFN<JobCallArgs,
   ErrorStrategy::CalleeHandled>` once and clone the handle into each Rust
   worker.
2. Each worker, when it receives a `Job<Bytes>`, calls `tsfn.call_async(args)`
   and `.await`s the resulting `oneshot` from the JS-side resolver.
3. The JS side: TSFN invokes the JS handler with a `Job` object whose
   `payload` field is a zero-copy `Buffer` view of the engine's `Bytes`.
   Resolution / rejection of the returned Promise is bridged back to the Rust
   `oneshot` by a small JS shim that the binding installs.
4. Result maps as in §11 below.

**Backpressure.** The libuv event loop is single-threaded. If the engine has
`concurrency=100`, that means up to 100 Promises are *pending* on the loop at
once — but the loop itself executes them sequentially. This is fine: the
engine's existing `concurrency` knob already gates how many `Job<T>`s are
in-flight, so the TSFN call queue depth matches it. We do **not** spawn
additional worker threads on the JS side; the whole point is that ChasquiMQ
hides the consumer-pool complexity from JS apps.

The relevant comparison vs. BullMQ: a BullMQ worker has its handler invoked
serially (or with a JS-side concurrency promise pool). Same single-threaded
loop, same constraint. Our advantage is that everything *outside* the handler
— the `XREADGROUP`, the ack batching, the retry/DLQ relocation — happens off
the libuv loop on Rust threads, where BullMQ does it on the loop.

**libuv "GIL" implications.** A long-running synchronous JS handler will
starve every other Promise on the loop, including the TSFN resolver. This is
a JS hazard, not ours; document it in the README. If we ever observe TSFN
queue starvation in the worker bench, the fix is to recommend
`worker_threads` for CPU-heavy handlers, not to add more Rust threads.

## 11. Error mapping

The Rust handler signature returns `Result<(), HandlerError>`. The engine
already distinguishes between "retry" and "fail" *implicitly* by attempt
count: any `HandlerError` re-schedules with backoff until `max_attempts`,
then routes to DLQ. JS gets the same mapping, plus a way to short-circuit to
DLQ.

| JS side | Rust side | Engine action |
|---|---|---|
| Promise resolves `void` | `Ok(())` | `XACK`, success |
| Promise rejects with `Error` | `HandlerError::new(<Error>)` | retry with backoff (or DLQ at `max_attempts`) |
| Promise rejects with a `class FailedJob extends Error {}` (sentinel exported by the binding) | `HandlerError::new(<terminal>)` + signal to skip retries | route to DLQ immediately |
| Sync `throw` inside handler body | same as Promise rejection (the JS shim wraps the call in `try/await fn(job)`) | retry with backoff |
| Handler returns non-Promise truthy/falsy | wrap in resolved Promise; treat as `Ok(())` (matches `async function` semantics) | `XACK` |
| JS-side panic / uncaught exception escaping the TSFN | logged as error, route to DLQ with reason `panic` | DLQ (do not retry; a panicking handler is a code bug, not transient) |

The "skip retries" path needs a small engine addition: today
`HandlerError` is opaque and the engine always retries until `max_attempts`.
Phase 3 introduces `HandlerError::Fail` (terminal) vs `HandlerError::Retry`
(default), and the JS binding produces the right variant based on the
sentinel class. This is a breaking-but-additive engine change — gate it
behind an enum so existing `HandlerError::new(...)` callers default to
`Retry` and nothing breaks. Mark the commit with `feat(consumer)!:` per
[Commit conventions](../CLAUDE.md#commit-conventions).

## 12. Build & release

Use `@napi-rs/cli` (≥3.x) end to end. It's the only tool worth using here —
the other N-API CLIs are unmaintained or behind on N-API 9.

**CI matrix** (GitHub Actions):

| Target | Runner | Notes |
|---|---|---|
| `x86_64-unknown-linux-gnu` | `ubuntu-latest` | glibc baseline |
| `aarch64-unknown-linux-gnu` | `ubuntu-latest` (cross) | via `@napi-rs/cli` cross-build |
| `x86_64-unknown-linux-musl` | `ubuntu-latest` (cross) | Alpine support; cheap to add |
| `aarch64-unknown-linux-musl` | `ubuntu-latest` (cross) | same |
| `x86_64-apple-darwin` | `macos-13` | Intel macs |
| `aarch64-apple-darwin` | `macos-14` | Apple Silicon — also our bench host |
| `x86_64-pc-windows-msvc` | `windows-latest` | |

`aarch64-pc-windows-msvc` is skipped in v1 (low demand, doubles Windows
build time). Add when someone files an issue.

**npm shape.** `@napi-rs/cli` generates the standard structure: a tiny root
`chasquimq` package with `optionalDependencies` for each
`chasquimq-<platform>-<libc>` sub-package. Users `npm install chasquimq` and
npm picks the right binary.

**Versioning.** Track the Rust crate version 1:1 (`chasquimq` Rust crate at
`0.5.0` → `chasquimq` npm at `0.5.0`). Pre-1.0 the engine is
breaking-allowed; matching versions makes the support story trivial ("npm
0.x.y uses Rust crate 0.x.y, period"). Independent versioning is a Phase 4
problem if it ever becomes one.

**Prebuilds only — no install-time compilation.** `node-gyp` is exactly the
JS-ecosystem pain point we're trying to escape. If a platform isn't covered
by a prebuild, install fails loud rather than triggering a 5-minute
Rust toolchain bootstrap.

## 13. Benchmark plan

Mirror the four `bullmq-bench` scenarios in a new
`crates/chasquimq-bench/node/` (or sibling `bench/node/`) directory. Use the
same Bun runtime and the same M3 host as the existing
`benchmarks/baseline-bullmq.md` so numbers are directly comparable.

| Scenario | Mirrors | Pass condition |
|---|---|---|
| `queue-add` | BullMQ `queue-add` (10×10 payload, single producer) | ≥3× of 13,961 = 41,883 jobs/s |
| `queue-add-bulk` | BullMQ `queue-add-bulk` (bulk 50, tiny payload) | **≥3× of 60,828 = 182,484 jobs/s** ← headline |
| `worker-generic` | BullMQ `worker-generic` (single consumer) | ≥3× of 13,250 = 39,750 jobs/s |
| `worker-concurrent` | BullMQ `worker-concurrent` (concurrency=100) | **≥3× of 47,707 = 143,121 jobs/s** ← headline |

For the worker scenarios, the JS handler is an empty `async () => {}`. That
matches BullMQ's harness exactly. Capture process CPU% with `top -pid` for
both Bun runs (the producer/consumer process) and Redis, so we can validate
the "≥50% less worker CPU" claim from the PRD.

Acknowledge: *the JS handler is in the loop now*. The Rust-only Phase 1 and
Phase 2 numbers had no JS in the loop. Some headroom will be given back to
the TSFN call cost. The slice 5 metrics already capture
`handler_duration_us`; piping that into the bench output will let us
attribute exactly how much.

Publish results to `benchmarks/chasquimq-node-phase3.md` alongside the
existing reports.

## 14. Risks & open questions

1. **TSFN call overhead.** Per-call cost on N-API ≥9 is fast but not free —
   a few hundred nanoseconds per call on M3, plus the resolver Promise
   chain. At 143k jobs/s that's ~7 µs of budget per job total, of which TSFN
   is one chunk. We may need a microbench gate **before** writing the full
   binding: stub a no-op TSFN dispatch loop and measure raw call rate. If
   it can't sustain ~250k calls/s, the headline target is at risk and the
   answer is probably batched dispatch (one TSFN call → array of N jobs,
   handler signature changes).
2. **Buffer vs typed object.** §9 picks `Buffer`, but the call is data-free
   right now. Run a microbench in slice (a) of §15 — produce 100k jobs, time
   the consumer with both modes, before slice (b) commits the API.
3. **Tokio runtime ownership.** napi-rs's bundled runtime works, but if the
   user's Node app already spawns CPU-bound tasks on it (any `tokio::spawn`
   from another N-API addon), we share the thread pool. Document; don't
   try to own it.
4. **GitHub Actions cost.** Seven-target matrix per release tag, plus PR
   builds. At current GHA pricing this is fine for an open-source project,
   but if PR builds get noisy we restrict the full matrix to release tags
   and run a Linux-only smoke build per PR.
5. **Redis Cluster from JS.** The engine is already cluster-hash-tag-correct
   (`{chasqui:<queue>}:<suffix>`) and `fred` does cluster routing. The N-API
   binding inherits this for free *unless* JS apps want to point at a cluster
   URL with multiple seed nodes. v1 ships with a single `redisUrl` string
   that gets passed straight through to `fred`. If users need
   `["host1:6379", "host2:6379"]`, that's Phase 3.5 — not a v1 blocker.
6. **`HandlerError::Fail` engine change.** Phase 3 needs the terminal-error
   variant (§11) to land in the engine *before* the binding can ship its
   `FailedJob` sentinel honestly. Sequence the slices accordingly.
7. **Per-job priority on FIFO Streams.** BullMQ uses a separate prioritized
   ZSET (jobs are popped highest-priority-first). ChasquiMQ Streams are FIFO
   by construction — that's a Streams invariant, not a config knob. We can
   either (a) build a parallel priority ZSET and have the consumer interleave
   reads (significant new code path; per-job round trip back into priority
   order; defeats batched `XREADGROUP`), or (b) ignore-with-warn for v1.
   We pick (b). Document loudly. If users rely on `priority`, they need to
   stay on BullMQ for now — the migration story is "you don't need it" or
   "keep BullMQ until we ship Phase 3.5 priority lanes."
8. **`Job.progress` persistence.** BullMQ persists progress to a Redis hash
   so any process can read it back via `Job.fetch(id).progress`. ChasquiMQ
   doesn't have a per-job hash. Two options: (a) write a
   `{chasqui:<queue>}:progress:<job_id>` hash on every `updateProgress` call
   (one Redis round trip per call — costly on tight progress loops), (b)
   skip persistence and only emit `progress` events on the events stream
   (cheap, but `Job.progress` reads return whatever the in-process Worker
   last set — stale across processes). Lean toward (b) for v1 with a docs
   note; revisit if users complain. Tracked as part of slice 9 since the
   events stream lands there anyway.
9. **`Job.update(data)`.** BullMQ updates the job hash in-place. ChasquiMQ
   Streams are append-only — `XADD` gives you a new entry, not a mutation.
   Options: (a) replace by `XDEL` + `XADD` with same id (loses ordering;
   non-trivial under consumer-group pending), (b) only allow `update()` on
   delayed jobs that haven't promoted yet (we have the side-index from slice
   6/7, so this is feasible: `ZREM` + re-`ZADD` with new payload), (c) throw
   `NotSupportedError` for non-delayed jobs and support delayed-only.
   Pick (c). Document. Tracked as a slice-11 sub-task.
10. **Repeatable scheduler-id format.** Slice 10 mentions matching BullMQ's
    `SHA1(name + cron pattern)` truncation. Confirm BullMQ's exact format
    before committing — they've changed it across major versions. If the
    current format is too version-specific, we ship our own format and skip
    the side-by-side migration claim. Decide during slice 10 implementation,
    not now.
11. **Compat-layer integration tests.** We need a smoke test suite that
    runs *BullMQ's own examples* against ChasquiMQ (i.e., copy-paste their
    docs sample, swap the import, assert it works). This is the truth check
    for "drop-in replacement." Build it as part of slice (h) below.

## 15. Implementation slices

Land each as its own PR. Conventional Commits scopes shown in parens. The
slices below are split into **engine slices** (8 / 9 / 10 — work in the
`chasquimq` crate, useful to native Rust users on their own) and **binding
slices** (a / b / c / d / e from the original native-API plan, plus f / g /
h for the BullMQ-compat layer). Engine slices land first, in order; binding
slices follow.

### Engine slices (this is the §4 work)

**Engine slice 8 — per-job retry overrides.**
`feat(engine): per-job attempts/backoff overrides on Producer::add_with_options`.
Encoded `Job<T>` envelope gains `Option<u32> max_attempts` and
`Option<BackoffSpec> backoff`. Worker hot path checks per-job first, falls back
to `ConsumerConfig`. Retry-relocator Lua preserves both fields on re-encode
(round-trips through the same payload — script is opaque). Producer surface:
`add_with_options(payload, RetryOverrides {...})` plus `add_in_with_options`
for the delayed variant. msgpack-`Option`-friendly so existing payloads decode
without breakage.

**Engine slice 9 — events stream.**
`feat(engine): per-queue events stream + EventsConfig`. Implements the
`{chasqui:<queue>}:events` `XADD` writes from the consumer ack flusher (with
a single pipelined batch — no per-event round trip), the promoter, the DLQ
relocator, and the producer add-path. `MAXLEN ~ N` cap. Add
`EventsConfig { enabled: bool, max_stream_len: usize }` to both
`ConsumerConfig` and `ProducerConfig`; default `enabled: true` (the BullMQ-
compat layer is the primary consumer). Microbench gate: if `worker-concurrent`
regresses by more than 5%, default it off and document the toggle. Native
Rust users get a typed `EventsReader` API — same XREAD-no-group pattern.

**Engine slice 10 — repeatable jobs (`JobScheduler`).**
`feat(engine): repeatable jobs via JobScheduler`. New `:repeat` ZSET +
`:repeat:specs` hash. `Producer::upsert_repeatable(id, RepeatSpec)` /
`remove_repeatable(id)` / `list_repeatable()`. Promoter extension: on each
tick, also process due `:repeat` ZSET entries — atomic Lua that XADDs the
templated job and ZADDs next-fire-time. Cron parsing via the `cron` crate.
Scheduler-id format aligns with BullMQ (`SHA1(name + pattern)` truncated, see
§4.3 / Risk #10). Includes a standalone `repeatable_jobs.rs` example.

### Native binding slices

**Slice (a) — Crate scaffold + `Producer.add` only.**
`feat(node): scaffold chasquimq-node binding`. New crate, napi-rs build, one
class with one method, `index.d.ts` generation, single `node-bench/smoke.mjs`
that produces 1k jobs and a separate Rust smoke consumer drains them. No CI
matrix yet — local `napi build` only. Microbench the TSFN no-op call rate
here too (per Risk #1) so we have the number before slice (b) commits.

**Slice (b) — Consumer + handler dispatch.**
`feat(node): Consumer + ThreadsafeFunction handler dispatch`. Implements §10,
including the JS-side Promise resolver shim. Resolves the §9 Buffer-vs-object
microbench by including both modes behind a feature flag, then commits to
one. Adds the `HandlerError::Fail` engine change (separate PR ahead of this
one, marked `feat(consumer)!:`) and the `FailedJob` sentinel.

**Slice (c) — Delayed / retry / DLQ surface.**
`feat(node): delayed, bulk, DLQ producer/consumer surface`. Wires
`addIn`/`addAt`/`addBulk`/`addInBulk`/`peekDlq`/`replayDlq` and the
`Promoter` class. Pure surface work — engine is already feature-complete for
this. Includes `addInWithId` / `cancelDelayed` only if the engine slice 6
work has merged; otherwise punted to a 3.x patch.

**Slice (d) — CI matrix + npm publish.**
`build(node): GitHub Actions prebuild matrix`. The seven-target matrix from
§12, optional `npm publish` on tag. First publish is `0.x.0-rc.0` so we can
shake out install paths without burning the real version.

**Slice (e) — Benchmark report.**
`bench(node): Phase 3 BullMQ-equivalent suite`. Mirrors the four
`bullmq-bench` scenarios (§13), runs them on the same M3 host, publishes
`benchmarks/chasquimq-node-phase3.md` with measured numbers, CPU%, and a
side-by-side vs. `benchmarks/baseline-bullmq.md`. Includes the
`handler_duration_us` breakdown so the FFI overhead is auditable.

Slice (a) through (d) can land without re-baselining anything. Slice (e) is
the one that proves the *native* binding hits its target.

### BullMQ-compat layer slices

These ride on top of the native binding (slices a–e) and the engine slices
8 / 9 / 10. They land in `crates/chasquimq-node/ts/` (TypeScript shim) and
ship in the same npm package, behind the `chasquimq` (default) entry point.

**Slice (f) — `Queue` + `Worker` (CORE basics).**
`feat(node): BullMQ-compat Queue and Worker classes`. The TypeScript shim:
`Queue` (constructor + `add` / `addBulk` / `getJobCounts` / `pause` /
`resume` / `close` + the rest of the CORE Queue surface from §3.1), `Worker`
(inline processor + the EventEmitter event set), `Job` (read fields +
`updateProgress` / `getState` / etc.). Wires `@msgpack/msgpack` per §6.
Translates `JobsOptions` per §5. Ships `UnrecoverableError` and
`NotSupportedError` exports. Includes the deferred-tier throws and the
not-supported-tier throws — failing loud is the v1 contract. Engine slices
8 and 9 must be merged first.

**Slice (g) — `QueueEvents` + repeatable jobs.**
`feat(node): QueueEvents subscriber + JobScheduler`. JS-side
`QueueEvents` class wraps `XREAD` over the slice-9 events stream into the
EventEmitter API. `Queue.upsertJobScheduler` / `removeJobScheduler` /
`getJobSchedulers` route to the slice-10 Producer API. Engine slices 9 and
10 must be merged first. `Job.progress` writes a progress event; reads come
from the in-process `Worker`'s last-set value (per Risk #8 decision).

**Slice (h) — Compat conformance suite + benchmark report.**
`test(node): BullMQ migration smoke tests` + `bench(node): compat layer
benchmark`. Copies a curated set of BullMQ docs examples into
`bench/node/compat/` with a single `import` line swapped to `chasquimq`,
asserts they run end-to-end. Adds a fifth bench scenario alongside the §13
four (specifically: `queue-add` and `worker-concurrent` *via the
BullMQ-compat layer* rather than via `chasquimq/native`), so we have hard
numbers on the compat-layer overhead vs. the native API. The compat layer
must hit ≥3× BullMQ on `worker-concurrent` even with the msgpack
encode/decode in the JS handler frame — that's the headline claim.

**Compat-layer pass condition.** Same headline targets as §13 but measured
through `import { Queue, Worker } from "chasquimq"` instead of the native
import. If the compat layer regresses below 3× while the native layer
clears it, the gap is the price of `data: any` and is worth reporting in
the benchmark doc — but we do not call Phase 3 done until the compat path
clears the bar.
