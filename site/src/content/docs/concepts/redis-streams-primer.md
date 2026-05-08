---
title: Redis Streams primer
description: The Redis 8.x Streams commands ChasquiMQ uses, and why we picked them over LPUSH/BRPOP.
sidebar:
  order: 2
---

ChasquiMQ is built on Redis Streams. If you've only used Redis as a key-value store or with `LPUSH`/`BRPOP`, this page covers the commands the engine actually issues and why each one matters.

For the canonical Redis docs, see [redis.io/docs/data-types/streams](https://redis.io/docs/latest/develop/data-types/streams/).

## What a Stream is

A Redis Stream is an append-only log. Each entry has:

- An auto-generated ID: `<ms-timestamp>-<sequence>` (e.g., `1731072123-0`). Lexicographically sortable.
- An associative bag of `key → value` pairs (the entry's "fields").

Streams support consumer groups, idle-claim recovery, atomic delete-on-ack (Redis 8.2+), idempotent producers (Redis 8.6+), and idle-pending reads (Redis 8.4+). All four are load-bearing for ChasquiMQ.

## The commands

### `XADD` — produce

```text
XADD <stream> <id|*> [MAXLEN ~ <count>] field1 value1 [field2 value2 ...]
```

- `*` lets Redis pick the ID. ChasquiMQ uses this — the ID is the engine's `job.id`.
- `MAXLEN ~ N` caps the stream length approximately. The `~` means "at least N, possibly a few more" — cheaper than exact trim. ChasquiMQ caps both the main stream and the DLQ.

ChasquiMQ writes `XADD {chasqui:<queue>} * d <msgpack-payload> n <utf8-name>` for every job. Two fields: `d` (binary payload) and `n` (UTF-8 dispatch name).

### `XADD ... IDMP` — idempotent produce (Redis 8.6+)

```text
XADD <stream> IDMP <producer_id> <job_id> * field1 value1 ...
```

The pair `(producer_id, job_id)` makes the publish at-most-once at the wire layer. A producer that retries an `XADD` after a network blip is safe — the second call returns the same ID without writing a second entry.

ChasquiMQ uses this for `addUnique(name, data, { jobId })` on the immediate path. Each `Producer` mints a fresh `producer_id` (UUID) at construction, so the IDMP scope is per producer instance. For cross-process strict dedup, use the delayed path's Lua-gated `SET NX EX` marker instead.

### `XREADGROUP` — consume

```text
XREADGROUP GROUP <group> <consumer> [COUNT N] [BLOCK ms] STREAMS <stream> >
```

Consumer groups give per-consumer **pending entries lists** (PELs) — a record of which entries each consumer has read but not yet acked. This is the "I crashed mid-handler" recovery path: when the consumer reconnects, the same entries are still in its PEL.

`>` is the "give me new entries" cursor. Pass an explicit ID to re-read the consumer's PEL.

### `XREADGROUP ... CLAIM` — idle-pending reads (Redis 8.4+)

```text
XREADGROUP GROUP <group> <consumer> [COUNT N] [BLOCK ms] CLAIM <min-idle-ms> ...
```

Combines a normal read with a CLAIM pass — entries that have been pending for longer than `min-idle-ms` are reassigned to the calling consumer in the same round trip.

Without it, you'd need a separate `XPENDING` + `XCLAIM` dance. ChasquiMQ uses `CLAIM` so the safety-net path (worker crashed after reading, before acking) is one command, not three.

### `XACK` — acknowledge

```text
XACK <stream> <group> <id> [<id> ...]
```

Removes entries from the consumer's PEL. The entry stays in the stream until trimmed or explicitly deleted. ChasquiMQ batches IDs and pipelines a single `XACK` per ack-flush window — see [Tune for throughput](/guides/tune-for-throughput/).

### `XACKDEL` — atomic ack-and-delete (Redis 8.2+)

```text
XACKDEL <stream> <group> <id> [<id> ...]
```

Acks **and** removes the entry from the stream in one round trip. ChasquiMQ uses this on the success path so completed jobs don't accumulate in the stream. Replaces the legacy "ack-then-delete" sequence (`XACK` followed by `XDEL`), which had a race window where the entry was acked but not yet deleted.

### `XPENDING` — inspect pending

Returns the consumer group's pending list with consumer assignment and idle time. ChasquiMQ uses it sparingly — the engine's metrics carry pending-count via the events stream and the consumer's CLAIM path is reactive (idle-pending reads on the next batch).

`chasqui inspect <queue>` issues one `XPENDING` to surface pending count.

### `XCLAIM` — manual claim

```text
XCLAIM <stream> <group> <consumer> <min-idle-ms> <id> [<id> ...]
```

Reassigns specific entries. ChasquiMQ does not call `XCLAIM` directly — the `CLAIM` argument on `XREADGROUP` covers the use case in fewer round trips.

### `XLEN`, `XRANGE`

Read-only inspection. `chasqui inspect` and `chasqui dlq peek` use them to render queue snapshots.

## Why Streams over `LPUSH`/`BRPOP`

The Redis docs themselves recommend Streams over lists for new queue implementations. The reasons:

- **Consumer groups.** Lists have one popper. Streams have N consumers in a group, each with its own PEL. Scaling out workers is a config knob, not a parallel-data-structure design exercise.
- **Idle-claim recovery.** A `BRPOP`'d entry is gone — if the worker crashes before processing, you lose the work unless you've built parallel "in-flight" tracking. With consumer groups, the entry stays in the consumer's PEL until acked; CLAIM re-delivers it on a different consumer after `min_idle_ms`.
- **Deterministic IDs.** Stream entries have monotonic IDs without an additional `INCR`. List entries are anonymous bytes; you'd `INCR` a separate counter and bake the ID in.
- **Atomic ack-and-delete.** `XACKDEL` is one round trip. The list pattern needs `BRPOP` (read), some app logic, `LREM` (delete from in-flight) — three round trips in the best case.
- **Idempotent produce.** `IDMP` exists. Lists have nothing equivalent; you'd build app-level dedup tables.
- **Approximate trim.** `XADD MAXLEN ~ N` caps growth without a separate sweeper. Lists need `LTRIM`.

Anti-pattern fans every "but lists are simpler" argument: each missing feature above is something you'd add yourself, badly, and call "battle-tested" three years later.

## Operational notes

- **`MAXLEN ~ N` is approximate.** Entries near the cap can be trimmed before they're read if consumers fall behind producers. Monitor `XLEN` against your consume rate; the silent failure mode is "job vanished." The engine emits `PromoterTick` with `depth` so this is observable.
- **Hash tags pin the slot.** Every chasqui key looks like `{chasqui:<queue>}:<suffix>`. The braces are Redis Cluster hash tags — they tell Redis "route on the inside." All queue keys (stream, delayed ZSET, DLQ, repeatable ZSET, events stream, result keys) co-locate.
- **Streams support BLPOP-style blocking.** `XREADGROUP BLOCK ms` blocks until either a new entry or `ms` elapse. ChasquiMQ uses 5_000ms by default; lower means shorter shutdown drain, higher means less idle Redis CPU.

For the deeper integration choices: [Architecture decisions](/concepts/architecture-decisions/).
