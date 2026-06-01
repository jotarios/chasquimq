---
title: DLQ and recovery
description: Why the DLQ is a stream, how replay works, and when to replay versus drop.
sidebar:
  order: 5
---

The dead-letter queue (DLQ) is `{chasqui:<queue>}:dlq` — another Redis Stream, sibling to the main queue. Its job is to hold entries that can't be delivered through the normal retry path so an operator can decide what to do with them.

## Why a stream, not a list

The decision to make the DLQ a Redis Stream (not a list, not a hash, not a separate Redis instance) follows from the same reasoning as the main queue:

- **Same operational primitives.** `XLEN`, `XRANGE`, `XADD`, `XACK` work the same way on the DLQ. The same `chasqui inspect` snapshot covers both. The same `XADD MAXLEN ~ N` cap bounds growth.
- **Atomic moves.** Replay is a single Lua script: `XACKDEL` from the DLQ, `XADD` to the main stream, with the `attempt` counter reset. No torn state if the script aborts.
- **Cluster-correct.** The DLQ uses the same `{chasqui:<queue>}:<suffix>` hash tag as the main stream, so atomic main↔DLQ moves stay on one slot.
- **Inspectable with the same tools.** `chasqui dlq peek` is just `XRANGE` on the DLQ stream with an extra histogram pass over the `reason` field.

The alternative — a parallel Redis list, a separate dead-job hash, a different store entirely — would mean two operational stories to learn, two sets of tools, two places to monitor. Symmetry is the design choice.

## Six reasons an entry lands in DLQ

The engine writes a `reason` field on every DLQ entry. Six values, three of which fire from the consumer side, two from the reader side, and one from the leader-elected stalled-job detector:

| Reason | Side | When it fires | Handler ran? |
|---|---|---|---|
| `retries_exhausted` | Consumer | Handler returned `Err` and `attempt + 1 >= max_attempts` | Yes |
| `unrecoverable` | Consumer | Handler threw `UnrecoverableError` | Yes (once) |
| `panic` | Consumer | Handler panicked / threw uncaught | Yes (once) |
| `decode_fail` | Reader | The entry's msgpack payload couldn't be decoded | No |
| `malformed` | Reader | The entry was missing required fields | No |
| `oversize` | Reader | The payload exceeded `max_payload_bytes` | No |
| `stalled` | Stalled detector | PEL entry observed idle past `idle_threshold_ms` for `max_stalled_attempts` consecutive scans — i.e. the entry kept being CLAIM-redelivered to crashing workers without ever completing. | Maybe (every dispatch crashed before ack) |

Reader-side reasons (`decode_fail` / `malformed` / `oversize`) carry `attempt: 0` because the handler never ran. Useful when triaging a backlog — a high `decode_fail` rate means a producer is writing in a different schema, not a handler bug.

## Stalled jobs

`retries_exhausted` and `stalled` look similar from a distance — both mean "this job couldn't finish" — but they catch different failure modes and require different operator responses.

- **`retries_exhausted`** = the handler ran `max_attempts` times and *every run failed*. The handler's logic, the input data, or a downstream service is broken. Replay the entries after shipping a fix.
- **`stalled`** = the handler kept *crashing the worker mid-execution* (segfault, OOM kill, infrastructure hiccup, kubelet OOM, deploy mid-job, etc) so the entry kept getting CLAIM-redelivered without an ack ever landing. The handler never even got to *fail* — it got interrupted. Replay won't help until the underlying worker-stability issue is resolved.

The stalled-job detector lives behind the `ConsumerConfig::stalled_detector_enabled` (default `true`) toggle. On every `tick_interval_ms` (default 30s, inherited from `claim_min_idle_ms` on the embedded spawn) the leader replica runs `XPENDING ... IDLE` against the consumer group's PEL, INCRs a per-job stall counter (`{chasqui:<queue>}:stalls:<job_id>`) for every entry past the idle threshold, and atomically relocates entries that hit `max_stalled_attempts` (default `2` — one extra tick of headroom over BullMQ's `maxStalledCount: 1` to avoid racing the reader's CLAIM-on-read recovery path) to the DLQ with `reason: "stalled"`. The counter is sliding-TTL'd and `DEL`'d on every terminal transition (successful ack, DLQ replay), so a one-off stall followed by success starts a fresh streak.

Subscribe to the `stalled` event on `Worker` (the in-process re-fan) or `QueueEvents` (cross-process, including the per-id channel `stalled:<jobId>`) to alert before threshold and decide whether to intervene.

## Routing into the DLQ is atomic

Moving an entry *into* the DLQ is one Lua script, not a two-step pipeline. The script `XACKDEL`s the source entry from the consumer group's pending list, then `XADD`s it into the DLQ — but only if the ack actually removed the entry. Both writes commit together server-side or not at all.

This matters under failure. If the move were a non-atomic pair (re-enqueue, then ack), a process crash between the two steps would leave the entry **both** in the DLQ **and** still pending on the main stream. The next idle-claim tick would re-deliver it and route a *second* copy into the DLQ. The single-script move closes that window: a relocate that gets interrupted either fully happened or never started, and a relocate retried after a lost reply finds nothing left to ack and writes nothing the second time. So an entry routes into the DLQ exactly once, even across crashes, reconnects, and concurrent consumers racing the same poisoned entry.

This is the same per-entry atomicity guarantee replay gives in the other direction — see below.

## Replay

`Producer::replay_dlq(limit)` (Rust), `Queue.replayDlq` (Node), `Queue.replay_dlq` (Python), and `chasqui dlq replay` are all the same primitive: a single Lua script that, for each entry up to `limit`:

1. Reads the entry from the DLQ stream.
2. Resets `attempt` to 0 (so the replayed job gets a fresh retry budget).
3. `XADD`s it to the main stream.
4. `XDEL`s it from the DLQ stream.

The script is atomic per entry. If the script aborts, no torn state — the entry is either fully replayed or fully unchanged.

## When to replay

- **You shipped a fix.** The handler now handles the failure mode that was producing `retries_exhausted`. Replay the affected entries.
- **The downstream resource came back online.** A cohort of jobs failed because Stripe / S3 / the database was down for 5 minutes. Replay them after recovery.
- **You raised the retry budget.** The original `attempts: 3` was too tight; you've changed to `attempts: 10`. Replay the entries that exhausted under the old budget.

## When NOT to replay

- **The bug is in the producer.** `decode_fail` / `malformed` / `oversize` entries will route back to DLQ on the first read. Fix the producer and drop the entries.
- **The handler is non-idempotent and the entry already partially completed.** A `retries_exhausted` entry may have produced side effects on its way to DLQ (sent the email, but then crashed before acking). Replaying re-runs the handler; if the handler isn't idempotent, you'll send the email twice.
- **The replay would just go back to DLQ.** If the underlying issue isn't fixed, replay just thrashes. Verify your fix on a small batch (`--limit 10`) before mass replay.

## Bounded growth

`ConsumerConfig::dlq_max_stream_len` (default 100,000) caps the DLQ via `XADD MAXLEN ~ N`. A runaway error rate may overshoot temporarily but won't grow unboundedly.

If your DLQ is growing fast, that's a signal — usually the consumer is broken (every job DLQ'ing) or the producer is broken (every entry malformed). Either way, the cap saves you from a Redis OOM while you investigate.

## Inspecting

```bash
chasqui dlq peek emails --limit 50
```

Renders:

- A histogram by `reason` (so you see `retries_exhausted: 12, unrecoverable: 2` at a glance).
- The most recent entries with `source_id`, `reason`, `attempt`, dispatch `name`, and the raw payload bytes.

In code, `Producer::peek_dlq(limit)` returns `Vec<DlqEntry>` with the same fields. Use it for app-level diagnostics or scheduled health checks.

## Idempotent replay

The replay path is **per-entry atomic**, but **not** idempotent across calls.

- Within one `replay_dlq(N)` call, the entries that were in the DLQ at the snapshot moment are moved exactly once.
- Calling `replay_dlq` twice — once at T=0, once at T=10 — moves whatever's in the DLQ at T=0 and again whatever's in the DLQ at T=10. If a replayed entry succeeded between the two calls, it's no longer in the DLQ; the second call doesn't see it.

The risk pattern: replay → fix is wrong → entry lands back in DLQ → replay again → repeat. Break the loop by peeking before each replay. See [Replay the DLQ](/guides/replay-the-dlq/).

## Operational pattern

The fix-the-bug-and-requeue workflow looks like:

1. `chasqui dlq peek emails` to see what's failing and why.
2. Decode a sample payload, reproduce the failure locally.
3. Ship the fix.
4. `chasqui dlq replay emails --limit 10` to verify the fix on a small cohort.
5. `chasqui watch emails` to check the DLQ doesn't grow.
6. `chasqui dlq replay emails --limit 1000` to drain the rest.

For the operational guide: [Replay the DLQ](/guides/replay-the-dlq/). For routing rules: [Route to the DLQ](/guides/route-to-dlq/).
