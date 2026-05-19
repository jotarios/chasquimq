---
title: Redis Cluster
description: Why one queue is one slot — the hash-tag invariant, ClusterHash::FirstKey routing, and what is deliberately out of scope on a cluster.
sidebar:
  order: 9
---

ChasquiMQ runs on a multi-shard Redis Cluster without any code on your side. You change one thing: the URL scheme. Everything else — atomic delayed promotion, DLQ relocation, result backends, idempotent scheduling — keeps the same guarantees it has on single-node Redis. This page explains *why* that works, so you can reason about it instead of trusting it.

## The problem a cluster creates

Redis Cluster splits the keyspace into 16384 hash slots, spread across shards. A multi-key command — or a Lua script that touches several keys — only runs if **every** key it touches lives in the **same** slot. Touch two keys on two slots and Redis rejects the whole command with `CROSSSLOT`.

ChasquiMQ leans on multi-key Lua scripts for its core correctness properties:

- `PROMOTE` moves due jobs from the delayed sorted set into the stream, atomically.
- `RETRY_RESCHEDULE` acks-and-deletes a stream entry and re-schedules it in one step.
- `RELOCATE_DLQ` acks a poisoned entry off the main stream and writes it to the DLQ, gated so a crash can never duplicate it.
- `JOB_OK` acks the entry and writes the handler's result under the same gate.
- `CANCEL_DELAYED`, `SCHEDULE_DELAYED_IDEMPOTENT`, the repeatable scripts — all multi-key.

If any of those scripts ever received keys on different slots, the queue would break on a cluster. So the entire design rests on one invariant.

## The invariant: one queue, one slot

Every key ChasquiMQ writes for a queue is named with a **hash tag**:

```text
{chasqui:<queue>}:stream
{chasqui:<queue>}:delayed
{chasqui:<queue>}:dlq
{chasqui:<queue>}:result:<job-id>
{chasqui:<queue>}:dlid:<job-id>      (dedup marker)
{chasqui:<queue>}:didx:<job-id>      (cancel side-index)
{chasqui:<queue>}:repeat
{chasqui:<queue>}:repeat:spec:<key>
{chasqui:<queue>}:promoter:lock
{chasqui:<queue>}:scheduler:lock
{chasqui:<queue>}:events
{chasqui:<queue>}:paused
```

Redis hashes only the substring between the first `{` and the first `}`. For every key above that is `chasqui:<queue>`. So **all** keys for one queue hash to exactly one slot — no matter which job, which result id, which lock. A queue's entire keyspace is co-located by construction, and every multi-key script is therefore single-slot.

The second half of the story is routing. Every command and script the engine issues is dispatched with `ClusterHash::FirstKey`, which tells the client to pick the target shard from the slot of the first key. Combined with the hash-tag invariant, that first key's slot *is* the queue's slot, so the command lands on the shard that owns the whole queue. No `CROSSSLOT`, ever.

```text
producer ──XADD────▶ {chasqui:orders}:stream  ─┐
producer ──ZADD────▶ {chasqui:orders}:delayed  │  same hash tag
consumer ──XACKDEL─▶ {chasqui:orders}:dlq      │  ⇒ same slot
engine  ──EVAL─────▶ PROMOTE(delayed, stream)  ─┘  ⇒ atomic, no CROSSSLOT
```

## How you connect

Pass a cluster URL scheme. One seed node is enough; the rest of the topology is discovered automatically via `CLUSTER SLOTS`, and `MOVED`/`ASK` redirections plus failover are handled inside the client.

- **Rust / Python:** `redis-cluster://seed:6379` (TLS: `rediss-cluster://`; Valkey: `valkey-cluster://` / `valkeys-cluster://`). Extra seeds: `?node=host:port`.
- **Node:** `connection.cluster: true` (composes with `tls: true`), or an explicit `redis-cluster://` URL.

There is no feature flag and no config field. `ConnectionTuning` — keepalive, reconnect policy, the rotating-token credential provider — applies to a clustered connection unchanged.

## The one cluster-specific cost: script caching

`SCRIPT LOAD` has no key, so on a cluster it lands on one arbitrary node. The first `EVALSHA` that routes to a *different* slot-owning node returns `NOSCRIPT`. ChasquiMQ already carries an inline-`EVAL` fallback for exactly this case: it runs the script body, which both succeeds and caches the script on that node. After one fallback per script per node, the steady state is pure `EVALSHA`. You pay a one-time, self-healing round trip — not a per-job tax.

## What is deliberately out of scope

- **Splitting one queue across slots.** A queue is single-slot by design. That is the price of atomic multi-key scripts, and it is the right trade: a single Redis Cluster slot still sustains the engine's throughput targets, and you scale by adding queues (which spread across slots naturally) rather than sharding one queue.
- **Cross-queue atomic operations.** There is no API that mutates two queues in one atomic step — not on a cluster, and not on single-node Redis either. This is a non-regression, not a cluster limitation.

If your bottleneck is ever a single queue saturating a single shard, that is a different conversation than "does cluster work" — it works; the question becomes queue partitioning at the application layer.
