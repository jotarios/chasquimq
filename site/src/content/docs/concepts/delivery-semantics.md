---
title: Delivery semantics
description: At-least-once on the wire, idempotent producer at the edge, and why exactly-once is a lie.
sidebar:
  order: 3
---

What ChasquiMQ guarantees, and what you have to do yourself.

## The default: at-least-once, on both sides

By default, every job is delivered to a handler **at least once**. That means:

- A handler that completes successfully will, in the absence of bugs, run exactly once.
- A handler may run more than once if the consumer crashes after the handler returns but before the engine acks. The crashed consumer's entry stays in the consumer group's pending entries list (PEL); the next consumer's idle-pending CLAIM picks it up and re-delivers.
- A handler may run more than once if a producer retries an `XADD` after a network blip without using `addUnique` — the same logical job lands on the stream twice.

This is the same guarantee Sidekiq, BullMQ, Celery, and almost every job queue worth using gives you. It's the right default because the alternative (at-most-once) silently drops work, and silent drops are worse than visible duplicates.

## What "exactly-once" actually means

You'll hear "exactly-once delivery" in marketing copy from queue vendors. It's a lie of layering.

**At the wire layer**, exactly-once doesn't exist on a network with failures. Either you don't ack until the work is done (potential duplicates if ack is lost) or you ack before doing the work (potential drops if work fails). Pick one.

**At the application layer**, exactly-once *effects* are achievable when:

- The handler is **idempotent**, meaning running it twice produces the same outcome as running it once.
- Or, the handler atomically marks the side effect as done in the same database transaction as its work.

ChasquiMQ helps with the first. The engine ensures at-least-once delivery. You ensure idempotent handlers. The product is exactly-once *effects*, which is what your users actually care about.

## The idempotent producer (Redis 8.6+)

The producer side is where most queues silently leak duplicates. A network blip mid-`XADD`, the client retries, two entries on the stream.

ChasquiMQ uses Redis 8.6's idempotent `XADD ... IDMP <producer_id> <job_id>` to seal that gap when you opt in.

```ts
await queue.addUnique(
  "welcome",
  { user: 42 },
  { jobId: "welcome:user:42" },
);
```

The pair `(producer_id, job_id)` is recorded by Redis. A second `XADD` with the same pair returns the existing entry ID without writing a second entry — the producer's network-retry path is now safe.

Two layers of guarantee:

- **Within one producer instance**, `addUnique` is strict. The IDMP scope is the producer's UUID (one per `Queue` construction); duplicate calls dedup at Redis.
- **Across producer instances** (different processes, restarts), the immediate path's IDMP scope resets — each new producer has a new UUID. For cross-process strict dedup, use a small `delay`; the delayed path uses a Lua-gated `SET NX EX` marker keyed by `(queue, job_id)` that is cross-process and persists for `delay + 1h grace`.

See [Idempotent add](/guides/idempotent-add/) for the operational pattern.

## The ack boundary

The handler-success-then-ack sequence has a race:

```
1. handler runs    ← side effects happen here
2. handler returns ok
3. engine pushes id into ack channel
4. ack flusher batches, pipelines XACKDEL
   ← if the worker crashes here, no ack reached Redis
5. Redis removes entry from PEL
```

Crash at step 4 → next reader's CLAIM finds the entry, re-delivers it. Your handler runs again.

This is the at-least-once boundary. There is no version of "ack first, run handler" that doesn't lose work; the engine commits to "run handler first, ack on completion" and tells you to make handlers idempotent.

## Idempotent handlers in practice

Three patterns, in order of strength:

1. **Natural idempotency.** The handler's work is idempotent by construction. "Set user.email = X." Running it twice produces the same end state. No bookkeeping needed.

2. **Idempotency token in app DB.** The handler writes a row to a `processed_jobs(job_id)` table at the same time as its side effect, in one transaction. Re-deliveries see the row exists and skip. Strong; works across crashes; requires a transactional store.

3. **Idempotent external API.** The handler issues an HTTP call with an `Idempotency-Key` header (Stripe-style). The downstream system is responsible for the dedup. Weakest, but often what you have.

The pattern to avoid: "I'll just check if the row exists before doing the side effect." That's a TOCTOU race. Two re-deliveries can both check, both see no row, both insert. Use a unique index or a transactional write.

## Read-side delivery semantics

Every reader uses the same `XREADGROUP CLAIM` call:

- New entries (`>`) are delivered to whichever consumer reads first.
- Pending entries idle for more than `claim_min_idle_ms` (default 30s) are reassigned to the calling consumer in the same round trip.

The `claim_min_idle_ms` is your "how patient am I before assuming the other consumer crashed" knob. Default 30s is generous — most handlers complete in milliseconds, and 30s of idle time is a strong signal of crash. Tighter values reduce recovery latency but risk re-delivering jobs whose handler is just slow.

## The retry path is also at-least-once

When a handler returns `Err`, the engine atomically `XACKDEL`s the original entry and `ZADD`s a fresh copy onto the delayed ZSET. That fresh entry has `attempt + 1`. The promoter promotes it back into the stream when its delay is up.

This is at-least-once: a crash between the `XACKDEL` and the `ZADD` (the script is one Lua script, so this can't actually happen) would be a problem; in practice, the script is atomic so the retry either lands or doesn't. The promoter is the only thing that promotes; the worker side never holds retry state in memory.

## TL;DR

| Surface | Guarantee | What you do |
|---|---|---|
| `Queue.add` (default) | At-least-once on the wire | Use `addUnique` if producer retries are likely |
| `Queue.addUnique` (immediate) | Strict per producer instance | Same UUID-scoped guarantee per process; use `delay` for cross-process |
| `Queue.addUnique` (delayed) | Strict cross-process | Lua-gated `SET NX EX` marker |
| Worker handler invocation | At-least-once | Make handlers idempotent |
| Retry path | At-least-once | Same: idempotent handlers |
| DLQ relocation | At-least-once | Idempotent under the engine's Lua scripts |

The product of "engine guarantees at-least-once" and "your handlers are idempotent" is exactly-once *effects*. That's what shipping looks like.

For the producer side: [Idempotent add](/guides/idempotent-add/). For the consumer side: [Retry and backoff](/concepts/retry-and-backoff/).
