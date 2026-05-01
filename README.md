![ChasquiMQ](docs/chasquimq.jpeg)

# ChasquiMQ

ChasquiMQ is a Rust-native job queue / message broker built on **Redis**, MessagePack payloads, and aggressive pipelining.

Named after the *chasquis* — the relay runners of the Inca road system who carried messages across the Andes.

> **Status:** Phase 2 in progress. Phase 1 (MVP) shipped: producer, consumer pool, batched acks, DLQ, graceful shutdown. Phase 2 slice 1 lands delayed jobs (`add_in` / `add_at` / `add_in_bulk`) backed by Redis Sorted Sets and a leader-elected promoter. Public API is still pre-1.0 and will change.

## Headline numbers

On Apple M3, Redis 8.6 (loopback): **3.20× BullMQ** on bulk produce, **9.17× BullMQ** on concurrent consume.

Full numbers, methodology, caveats, and reproduction commands live in [`benchmarks/README.md`](benchmarks/).

## Why it's fast

The bottlenecks ChasquiMQ exists to escape, and what it does instead:

- **Redis Streams over `LPUSH`/`BRPOP`.** Consumer groups give us per-consumer pending lists, idle-claim recovery, and deterministic IDs without inventing them in user space.
- **MessagePack payloads via `rmp-serde`.** Binary, schema-flexible, smaller and faster than JSON on every hop.
- **Batched, pipelined `XACK`.** Acks accumulate in a bounded channel and flush as a single pipelined batch (`ack_batch` jobs or `ack_idle_ms` idle, whichever first). Per-job ack round trips are the silent killer in naive Streams consumers.
- **`XACKDEL` (Redis 8.2).** Atomic ack-and-delete in one round trip — no ack-then-delete dance.
- **`IDMP` idempotent `XADD` (Redis 8.6).** DLQ relocation is retry-safe at the Redis layer; producer retries after network blips don't double-publish.
- **Tokio multi-receiver dispatch.** `async-channel` fans batches to N workers without a shared `Mutex` on the receiver. Per-job work stays off the reader's hot path; DLQ moves run on a dedicated relocator task.
- **`Arc<str>` everywhere on the hot path.** Stream entry IDs and consumer/producer IDs are reference-counted, not cloned as `String`.

Anti-patterns we don't reach for: blocking Lua scripts, JSON payloads, per-job round trips.

## Quickstart

Requires **Rust 1.85+** (2024 edition) and **Redis 8.6+**.

```bash
# Redis (one-time)
docker run -d --name chasquimq-redis -p 6379:6379 redis:8.6

# build
cargo build --release
```

### Produce

```rust
use chasquimq::{Producer, ProducerConfig};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct EmailJob { to: String, subject: String }

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let producer = Producer::<EmailJob>::connect(
        "redis://127.0.0.1:6379",
        ProducerConfig { queue_name: "emails".into(), ..Default::default() },
    ).await?;

    producer.add(EmailJob {
        to: "ada@example.com".into(),
        subject: "hello from chasqui".into(),
    }).await?;

    // Schedule a job for one minute from now:
    producer.add_in(std::time::Duration::from_secs(60), EmailJob {
        to: "grace@example.com".into(),
        subject: "scheduled hi".into(),
    }).await?;

    Ok(())
}
```

### Consume

```rust
use chasquimq::{Consumer, ConsumerConfig};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let consumer = Consumer::<EmailJob>::new(
        "redis://127.0.0.1:6379",
        ConsumerConfig { queue_name: "emails".into(), concurrency: 100, ..Default::default() },
    );

    let shutdown = CancellationToken::new();
    consumer.run(|job| async move {
        send_email(&job.payload).await
            .map_err(|e| chasquimq::HandlerError::retryable(e.to_string()))
    }, shutdown).await?;

    Ok(())
}
```

Failed jobs are retried up to `max_attempts` times with exponential backoff; exhausted jobs land in the `<queue>:dlq` stream with their failure reason.

### Retry semantics

When a handler returns `Err` (or panics), the worker:
1. Encodes the job with `attempt += 1` and computes `run_at_ms = now + backoff(attempt)`.
2. Atomically (via Lua) `XACKDEL`s the original stream entry and `ZADD`s the re-encoded job onto the queue's delayed set.
3. The promoter promotes it back into the stream when due. Next handler invocation sees `job.attempt` incremented.

If `next_attempt >= max_attempts`, the entry goes straight to DLQ instead. Backoff is `min(initial * multiplier^(attempt-1), max) + jitter`. Defaults: `initial=100ms`, `multiplier=2`, `max=30s`, `jitter=100ms`. Configure via `ConsumerConfig::retry: RetryConfig`.

The classic `XREADGROUP CLAIM` mechanism (Redis 8.4 idle-pending reads) remains the safety net: if a worker dies mid-handler before the retry path runs, CLAIM re-delivers the entry on the next read, and the reader compares the in-payload `attempt` counter against `delivery_count` to detect retry-exhaustion regardless of which path produced the count.

### Delayed jobs

`Producer::add_in(delay, payload)` and `Producer::add_at(when, payload)` schedule jobs to fire later. Bulk variant is `Producer::add_in_bulk`. A `delay` of zero (or `add_at` in the past) fast-paths straight to the stream.

By default any `Consumer` with `delayed_enabled = true` (the default) runs an embedded promoter that moves due jobs from the delayed sorted set into the stream. Multiple consumers coordinate via a per-queue lock so only one promotes per tick. For producer-only deployments where no consumer runs locally, run a standalone [`Promoter`](chasquimq/examples/standalone_promoter.rs).

`max_delay_secs` on `ProducerConfig` (default 30 days) caps how far in the future jobs can be scheduled. Set to `0` to disable the cap.

**At-least-once under caller retry.** `add_in` / `add_at` / `add_in_bulk` are not idempotent across caller-driven retries: each call generates a fresh job id, and a retry after a network failure can land a duplicate scheduled job. (Compare with `Producer::add`, which uses Redis 8.6 `IDMP` for at-most-once delivery.) An explicit `add_in_with_id` is on the roadmap; until then, callers needing exactly-once delayed scheduling should retry only after confirming the previous call did not reach Redis (e.g., via `ZSCORE` on the delayed key).

### DLQ tooling

`Producer::peek_dlq(limit)` reads up to N DLQ entries with their failure metadata (`source_id`, `reason`, optional `detail`, raw payload bytes) without removing them — the inspection API.

`Producer::replay_dlq(limit)` moves up to N DLQ entries back into the main stream atomically. Each entry's `attempt` counter is reset to 0 before re-`XADD` so the replayed job gets a full retry budget (otherwise it'd land in DLQ again on first dispatch). The fix-the-bug-and-requeue workflow.

DLQ growth is capped via `ConsumerConfig::dlq_max_stream_len` (default 100,000). `XADD MAXLEN ~ N` is approximate so a runaway error rate may overshoot temporarily but won't grow unboundedly.

### Operational notes

- **Stream MAXLEN trim is approximate.** Both Phase 1 and the delayed-job promoter use `XADD MAXLEN ~ N`. If consumers fall sustainedly behind producers, entries near the cap can be trimmed before they are read. Monitor `XLEN` against your consume rate; the silent failure mode is "job vanished."
- **No `cancel_delayed` in v1.** Once `add_in`/`add_at` returns, there is no API to undo the schedule. Tracked for Phase 3.
- **Key format uses Redis Cluster hash tags** — every chasqui key looks like `{chasqui:<queue>}:<suffix>`. This is a pre-1.0 breaking change from earlier preview builds; redeploying against a Redis instance that holds old-format keys requires draining or manually renaming. New deployments are unaffected.

## Feature comparison

ChasquiMQ is perf-first and Phase 1; the table is honest about what isn't there yet. See the [Roadmap](#roadmap) for what's coming.

| Feature                       | ChasquiMQ        | BullMQ | Bull   | Bee     |
| :---------------------------- | :--------------: | :----: | :----: | :-----: |
| Backend                       | Redis            | Redis  | Redis  | Redis   |
| Language                      | Rust             | Node   | Node   | Node    |
| Wire format                   | MessagePack      | JSON   | JSON   | JSON    |
| Concurrency                   | ✓                | ✓      | ✓      | ✓       |
| Atomic ops (`XACKDEL`)        | ✓                | ✓      | ✓      | ✓       |
| Persistence                   | ✓                | ✓      | ✓      | ✓       |
| Pipelined / batched acks      | ✓ (default)      | opt-in | —      | —       |
| Idempotent produce (`IDMP`)   | ✓                | —      | —      | —       |
| Dead-letter queue             | ✓                | ✓      | ✓      | —       |
| Graceful shutdown             | ✓                | ✓      | ✓      | ✓       |
| Delayed jobs                  | ✓                | ✓      | ✓      | —       |
| Retries (exponential backoff) | ✓                | ✓      | ✓      | ✓       |
| Priorities                    | Phase 2+         | ✓      | ✓      | —       |
| Rate limiter                  | Phase 2+         | ✓      | ✓      | —       |
| Pause/Resume                  | Phase 2+         | ✓      | ✓      | —       |
| Repeatable / cron jobs        | Phase 2+         | ✓      | ✓      | —       |
| Parent/child dependencies     | Phase 2+         | ✓      | —      | —       |
| Sandboxed worker              | n/a (Rust)       | ✓      | ✓      | —       |
| UI                            | Phase 4          | ✓      | ✓      | —       |
| Node SDK                      | Phase 3          | ✓      | ✓      | ✓       |
| Python SDK                    | Phase 4          | ✓      | —      | —       |
| Optimized for                 | Throughput       | Jobs   | Jobs   | Messages |

If a row marked Phase 2/3/4 is blocking for you, please [open an issue](https://github.com/jotarios/chasquimq/issues) — it helps prioritize.

## Repo layout

```
chasquimq/         engine crate (producer, consumer, ack flusher, DLQ relocator)
chasquimq-bench/   benchmark harness — same scenarios as bullmq-bench
benchmarks/        results, methodology, reproduction (see benchmarks/README.md)
prd/               product requirements & design intent
spike/             exploratory throwaway code (not part of the engine)
```

## Roadmap

- **Phase 1:** Producer, consumer pool, batched pipelined acks, DLQ, graceful shutdown. ✅
- **Phase 2 (in progress):** Delayed jobs via sorted sets + Lua promoter ✅. Exponential retry backoff via delayed-ZSET re-scheduling ✅. DLQ replay + bounded growth ✅. Next: stats / observability.
- **Phase 3:** Node.js bindings via NAPI-RS — JS handlers driven by the Rust engine.
- **Phase 4:** Python bindings via PyO3, CLI monitoring dashboard.

Phase 1's API surface is intentionally small; expect breaking changes as Phase 2 lands.

## Contributing

PRs welcome. For anything beyond a small fix, please open an issue first; ChasquiMQ has load-bearing constraints (Streams, MessagePack, pipelined acks) that aren't obvious from the code alone.

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for dev setup, PR workflow, and what's in/out of scope.

## License

MIT — see [`LICENSE`](LICENSE).
