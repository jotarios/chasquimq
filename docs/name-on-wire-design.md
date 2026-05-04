# Design — `name` on the wire (v1.0 decision)

Status: design draft. No implementation. The recommendation in §4 is one
option; the human picks before any code lands.

`Queue.add(name, data, options?)` accepts `name` then drops it before
`XADD`. Workers see `job.name = ""`. Pre-1.0 we can change the wire
without ceremony; post-1.0 it's a deprecation cycle. This doc lays out
the options.

## 1. Status quo

The producer encodes user `data` into msgpack, wraps it in a `Job<T>`
envelope (`{ id, payload, created_at_ms, attempt, retry? }` —
[`chasquimq/src/job.rs:91-117`](../chasquimq/src/job.rs)), msgpack-encodes
the envelope, and `XADD`s **one** field named `d` carrying the bytes:

```
XADD <stream> IDMP <producer_id> <iid> MAXLEN ~ <cap> * d <bytes>
```

Field name = `PAYLOAD_FIELD = "d"`
([`chasquimq/src/redis/keys.rs:1`](../chasquimq/src/redis/keys.rs); call
site [`commands.rs:330-349`](../chasquimq/src/redis/commands.rs)).
Consumer-side parser walks the entry's `[k, v, ...]` field list looking
for `d` and ignores unknown keys
([`parse.rs:160-177`](../chasquimq/src/redis/parse.rs)).

`name` does not appear anywhere on the wire. Both shims hand-build a
`Job` with `name=""` on the worker side
([`chasquimq-py/src/chasquimq/worker.py:130`](../chasquimq-py/src/chasquimq/worker.py),
[`chasquimq-node/src-ts/worker.ts:222-233`](../chasquimq-node/src-ts/worker.ts)).
The Python `_encoding.py` docstring pins the contract; the Node
`Queue.add` `console.warn`s the caller
([`queue.ts:84-89`](../chasquimq-node/src-ts/queue.ts)). The events
stream ([`events.rs`](../chasquimq/src/events.rs) §29-43) also carries
`id` / `attempt` / `reason` but no `name`.

## 2. Use cases that need `name`

- **Handler dispatch.** `if job.name == "send-email" then ... else ...`.
  The BullMQ idiom: one `Worker` per queue, branch on name.
- **BullMQ migration ergonomics.** Existing users assume `job.name`
  works.
- **Per-job-type metrics.** `MetricsSink` events tagged with `name` so
  histograms can be sliced by job kind without decoding payload.
- **Observability / tracing tags.** Span attributes, log fields, the
  events stream and `chasqui events <queue>` CLI rendering.

## 3. Options

### Option A — Tuple in payload

`msgpack([name, data])` in field `d`. Consumer decodes a 2-tuple, sets
`job.name = tuple[0]`. Cost: msgpack array header (~2 B) + UTF-8 name +
one extra envelope decode. Wire-breaking for both shims and any
`Producer<T>` user (T's shape changes).

### Option B — Separate stream entry field

`XADD ... d <payload-bytes> n <utf8-name>`. Consumer reads both fields
off the entry; `job.name = entry["n"]`. Decode cost: zero — `n` is a
string at the parse layer; the existing parser already ignores unknowns
so old consumers tolerate a new producer. Engine `Job<T>` envelope
unchanged. Cost: ~3 B Redis Streams field framing + UTF-8 name.

### Option C — msgpack envelope

`msgpack({"n": name, "d": data})` in field `d`. Variant of A but
key-tagged for forward compat. Largest wire size of the bunch (dict
header + key strings). Wire-breaking same way A is.

### Option D — Trail on the `Job<T>` array

Add a 6th positional field on `Job<T>` after `retry`,
trailing-optional, encoded `skip_serializing_if = Option::is_none`.
Cost: ~1 B array-len delta + UTF-8 name. Decode: one extra `Option`
field (~free, mirrors the slice-8 `retry` pattern). Same deploy-order
trap as slice 8: new producer + old consumer = decode failure → DLQ
([`job.rs:106-114`](../chasquimq/src/job.rs)).

### Option E — Don't ship

Document "we don't transport name; dispatch on `data.kind`." Drop the
warning by removing the API param. Zero wire change. Migration burden
falls on every BullMQ user.

## 4. Recommendation

**Option B.** Send `name` as a second `XADD` field next to `d`.

- **Performance.** Consumer hot path stays at one msgpack-decode per
  job. A / C add an envelope decode; D adds a struct field. B adds
  zero msgpack work — `n` is a Streams-level string.
- **Observability.** `MetricsSink` and `EventsWriter` run *before* the
  user handler decodes payload. B is the only option where `name` is a
  top-level Streams field — readable into a metric label or event field
  without touching the msgpack payload bytes. The engine is generic
  over `T` and treats payload as opaque `Bytes` until the worker
  decodes; A / C / D would force the engine to msgpack-decode payload
  to recover `name` for observability, breaking that contract.
- **Engine `Job<T>` stays clean.** `name` belongs at the framing layer
  (routing/observability metadata), not in the user-data envelope.
  Option B preserves that separation; D pollutes the `Producer<T>`
  generic with an engine-managed routing string.
- **Compat is forgiving.** The existing parser walks `[k, v, ...]` pairs
  and ignores unknowns, so a new producer + old consumer is a graceful
  degrade in B. A / C / D all break the decode hard.
- **Wire size.** B's overhead (Streams field framing + UTF-8 name) is
  ~equivalent to D's (msgpack array slot + UTF-8 name); both ~10–20 B
  per job. The bench guard in §5 catches any regression.

Argument against: Streams field framing has a per-field cost A / C / D
encode internally. Empirically ~3 B per pair; on `queue-add-bulk` (60k
jobs/s target) <0.5%, comfortably within the bench budget.

## 5. Migration plan if we ship before 1.0

- **Slice X (engine).** Add `n` to `xadd_args` + `xadd_dlq_args`. Extend
  `ParsedEntry` with `name: Option<String>`. Add `Job::name:
  Option<String>` to the public engine API (set by the parser, separate
  from the positional `Job<T>` envelope). Plumb `name` into
  `MetricsSink` events and `EventsWriter::emit_*` calls. Old producers
  emit no `n`; new consumers see `name = None`. New producer + old
  consumer: parser silently ignores `n`. No deploy-order constraint.
- **Slice X+1 (shims).** Both shims populate `name` on `Queue.add` and
  surface it on the worker side. Drop the warning at
  [`queue.ts:84-89`](../chasquimq-node/src-ts/queue.ts) and the contract
  note in
  [`_encoding.py:1-8`](../chasquimq-py/src/chasquimq/_encoding.py).
  Cross-shim contract test (landing in parallel) extended to assert
  `name` round-trips Python ↔ Node.
- **Slice X+2 (events + CLI).** `EventsWriter` adds `name` to per-job
  events. `chasqui events <queue>` renders it. `chasqui inspect` adds a
  per-name breakdown if cheap.
- **Bench guard.** Re-run `queue-add-bulk` and `worker-concurrent`;
  budget is 1% on either. Worse blocks.

## 6. Migration plan if we ship after 1.0

The wire-compat side is free — the parser already tolerates unknown
fields, and consumers can keep parsing the no-`n` shape forever. What
isn't free:

- `Job.name = ""` on the worker side becomes documented behavior, not a
  known-bad placeholder. Changing it is a breaking shim release.
- A rollout knob (`Queue.add(..., { transportName: true })`) goes
  default-off → default-on → removed across three releases.
- The events stream schema is documented; adding `name` requires a
  schema version bump.

Net: pre-1.0 ships in 3 slices over ~1 week. Post-1.0 ships in 3
releases over ~3 months plus a deprecation cycle. Same engineering work,
an order of magnitude more release-management work.

## 7. Open questions for the human

- **Format.** UTF-8 string. Length cap? 64 KiB matches most ID-like
  fields. Permitted characters: anything UTF-8; no regex enforcement.
- **Empty / missing semantics.** Engine surfaces `Option<String>`;
  shims surface `""` for both empty and missing. Promote either to a
  richer sentinel?
- **Repeatable jobs.** `RepeatableSpec` already carries `job_name`
  ([`repeat.rs`](../chasquimq/src/repeat.rs)); the scheduler should
  thread it into `n` on every fire automatically.
- **Events stream.** Every per-job event in
  [`events.rs`](../chasquimq/src/events.rs) should carry `name`. The
  CLI renders it.
- **DLQ.** `xadd_dlq_args`
  ([`commands.rs:389-411`](../chasquimq/src/redis/commands.rs)) should
  preserve `name` alongside the original payload.
