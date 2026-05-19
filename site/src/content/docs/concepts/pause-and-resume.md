---
title: Pause and resume
description: Consumer-side stop-dispatch — process-local vs. durable pause, what drains, and why there's no producer-side flag.
sidebar:
  order: 8
---

Pause in ChasquiMQ is **consumer-side stop-dispatch**. When a queue is paused, consumers stop reading new jobs and stop handing them to handlers. Three things keep happening:

- **In-flight jobs drain.** A job already handed to a handler runs to completion. Pause stops *future* reads, it never truncates work in progress.
- **Producers keep enqueueing.** `Queue.add()` is unaffected. The stream backlog grows while paused — that is correct and intended. Resume drains it.
- **The promoter, scheduler, and relocators keep running.** Delayed jobs still get promoted into the stream, cron jobs still fire, retries still get rescheduled. Only the *dispatch to handlers* stops.

There is deliberately **no producer-side pause flag**. ChasquiMQ exists to keep the produce/consume hot path free of extra Redis round trips; gating every `add()` on a "is the queue paused?" check would be exactly the per-call overhead the design forbids. Pause is something consumers observe, not something producers enforce.

## Two levels: process-local and durable

There are two ways to pause, and they answer different questions.

**Process-local — `Worker.pause()`.** Stops one worker instance, in memory. Nothing is written to Redis. Resume is instant (the parked reader is woken the moment you call `resume()`). It does **not** survive a process restart — a fresh worker comes up running. Use it for "this worker, right now" control: backpressure from a downstream system, a graceful drain before a deploy of *this* process.

```ts
await worker.pause()   // just this worker stops
worker.resume()        // back to work, immediately
```

**Durable — `Queue.pause()` / `chasqui pause`.** Sets a cross-process flag (a Redis key, `{chasqui:<queue>}:paused`, with no TTL). Every consumer of the queue — on every host — parks at its next batch boundary. It **survives consumer restarts**: a worker started while the flag is set comes up parked before its first read. It stays paused until an explicit `resume`. Use it for queue-wide maintenance: pausing "emails" during a provider outage, freezing a queue while you inspect the DLQ.

```bash
chasqui pause emails     # every consumer of emails parks, durably
chasqui resume emails    # lift it everywhere
```

`Queue.pause()` (Node/Python) and `chasqui pause` toggle the same key — they're interchangeable.

## When does a paused consumer notice?

The reader checks both signals only at **batch boundaries** — between one `XREADGROUP` and the next, never per-job, never on the produce path. That keeps the not-paused path free: one atomic flag read plus one timestamp comparison per batch, no Redis round trip.

- **Process-local pause** is edge-triggered: `resume()` wakes the parked reader immediately.
- **Durable pause** is observed by a single `EXISTS` that is time-gated by `pause_poll_ms` (default 250 ms). So a `chasqui pause` is picked up within ~250 ms by an actively-running consumer, and a `chasqui resume` likewise. Tighten `pause_poll_ms` for faster cross-process reaction; the cost is one extra `EXISTS` per poll interval while idle, never on the hot path.

A consumer told to shut down while parked still shuts down cleanly and drains its in-flight work — pause never blocks a graceful stop.

## Idempotency

Every pause/resume entry point is idempotent. Pausing an already-paused queue is a no-op (no spurious wake of the reader). Resuming one that isn't paused is a no-op. Double-calling from racing operators or retried CLI invocations is safe.

## See also

- [Delivery semantics](/concepts/delivery-semantics/) — what "in-flight" means and why draining is at-least-once-safe.
- [`chasqui pause` / `chasqui resume`](/reference/cli/) — the CLI reference.
- [Options reference](/reference/options/) — `pause_poll_ms` and the per-surface method names.
