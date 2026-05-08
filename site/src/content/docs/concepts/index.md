---
title: Concepts
description: How ChasquiMQ is built and why.
sidebar:
  order: 0
---

The why behind every design decision. Read [Thinking in ChasquiMQ](/concepts/thinking-in-chasquimq/) first if you're new — it's the mental model that makes the rest of these pages make sense.

## Mental model

- [Thinking in ChasquiMQ](/concepts/thinking-in-chasquimq/) — Producer / Consumer / Stream / Job / DLQ as one diagram. Read this first.

## Wire layer

- [Redis Streams primer](/concepts/redis-streams-primer/) — `XADD`, `XREADGROUP`, `XACK`, `XACKDEL`, idempotent producer.
- [Delivery semantics](/concepts/delivery-semantics/) — at-least-once, idempotent producer, why exactly-once is a lie.

## Job lifecycle

- [Retry and backoff](/concepts/retry-and-backoff/) — how retries get rescheduled, the backoff math, the unrecoverable short-circuit.
- [DLQ and recovery](/concepts/dlq-and-recovery/) — why the DLQ is just another stream.
- [Result backends](/concepts/result-backends/) — why result storage is opt-in.
- [The scheduler](/concepts/the-scheduler/) — separate from the consumer for a reason.

## Architecture and trade-offs

- [Architecture decisions](/concepts/architecture-decisions/) — Rust + tokio, Streams, MessagePack, why no Lua.
- [Performance trade-offs](/concepts/performance-trade-offs/) — what we measured and what we kept.
