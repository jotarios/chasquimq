![ChasquiMQ](docs/chasquimq.jpeg)

# ChasquiMQ

ChasquiMQ is a Rust-native job queue / message broker built on **Redis**, MessagePack payloads, and aggressive pipelining.

Named after the *chasquis* — the relay runners of the Inca road system who carried messages across the Andes.

> **Status:** Phase 1 (MVP). Producer, consumer pool, batched acks, DLQ, graceful shutdown — usable, but the public API is pre-1.0 and will change.

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

Failed jobs are retried up to `max_attempts` times; exhausted jobs land in the `<queue>:dlq` stream with their failure reason.

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
| Delayed jobs                  | Phase 2          | ✓      | ✓      | —       |
| Retries (exponential backoff) | basic (Phase 1)  | ✓      | ✓      | ✓       |
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

- **Phase 1 (current):** Producer, consumer pool, batched pipelined acks, DLQ, graceful shutdown. ✅
- **Phase 2:** Delayed jobs (sorted sets), automatic retries with exponential backoff, richer DLQ tooling.
- **Phase 3:** Node.js bindings via NAPI-RS — JS handlers driven by the Rust engine.
- **Phase 4:** Python bindings via PyO3, CLI monitoring dashboard.

Phase 1's API surface is intentionally small; expect breaking changes as Phase 2 lands.

## Contributing

PRs welcome. For anything beyond a small fix, please open an issue first; ChasquiMQ has load-bearing constraints (Streams, MessagePack, pipelined acks) that aren't obvious from the code alone.

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for dev setup, PR workflow, and what's in/out of scope.

## License

MIT — see [`LICENSE`](LICENSE).
