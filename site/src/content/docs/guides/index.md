---
title: How-to guides
description: Task-oriented recipes for production ChasquiMQ.
sidebar:
  order: 0
---

Each guide solves one problem in 30–150 lines. Code first, gotchas at the end.

## Production setup

- [Configure retries](/guides/configure-retries/) — `attempts`, `backoff` types, jitter, queue-wide vs per-job.
- [Idempotent add](/guides/idempotent-add/) — `addUnique`, `XADD ... IDMPAUTO`, the at-least-once safety boundary.
- [Enable result storage](/guides/enable-result-storage/) — opt in to `storeResults` and poll with `waitForResult`.

## Reliability

- [Route to the DLQ](/guides/route-to-dlq/) — `UnrecoverableError`, every `DlqReason`, when each fires.
- [Replay the DLQ](/guides/replay-the-dlq/) — `chasqui dlq replay` and the shim helper. Idempotency caveats.
- [Schedule repeatable jobs](/guides/schedule-repeatable-jobs/) — cron, `every`, `MissedFiresPolicy`.

## Operations

- [Observe the engine](/guides/observe-the-engine/) — `MetricsSink`, Prometheus adapter, `chasqui events`.

## Performance

- [Tune for throughput](/guides/tune-for-throughput/) — concurrency, batched acks, payload size, the `enableAutoPipelining` lesson.

## Migration

- [Migrate from BullMQ](/guides/migrate-from-bullmq/) — API mapping, what's compat, what's intentionally different.
- [Migrate from Sidekiq or Celery](/guides/migrate-from-sidekiq-celery/) — conceptual mapping for ruby / python shops.
