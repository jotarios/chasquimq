![ChasquiMQ](docs/chasquimq.jpeg)

# ChasquiMQ

The fastest open-source message broker for Redis. Rust-native engine, MessagePack payloads, aggressive pipelining. Native Node.js and Python bindings — handlers run where you write them, the engine pulls jobs.

> **Status:** 1.0 polish complete; ready for the 1.0 tag. The public API is stable in shape; small breakages may still land before tagging.

Named after the *chasquis* — the relay runners of the Inca road system who carried messages across the Andes.

## Headline numbers

Apple M3, Redis 8.6 (loopback Docker), `bullmq-bench` vs `chasquimq-bench` on the same host. Measured 2026-05-07 under load avg ~1.8–4.3.

| Scenario | BullMQ 5.76.4 | ChasquiMQ | Ratio |
|---|---:|---:|---:|
| `queue-add-bulk` (50, tiny payload) | 54,455 jobs/s | **188,775 jobs/s** | **3.47×** |
| `worker-concurrent` (100 workers) | 45,643 jobs/s | **111,968 jobs/s** | **2.45×** |
| `queue-add` (single, 10×10 payload) | 13,245 jobs/s | 15,366 jobs/s | 1.16× |

`worker-concurrent` is the most CPU-contention-sensitive scenario in the suite; on a quiet host (load < 1) ChasquiMQ reaches ~419k jobs/s for an 8.78× ratio. See [`benchmarks/`](benchmarks/) for full methodology, distribution stats, and the canonical quiet-host run.

## Why it's fast

- **Redis Streams over `LPUSH`/`BRPOP`.** Consumer groups, idle-claim recovery, and atomic ack/delete primitives — not LIST polling.
- **MessagePack payloads** via `rmp-serde`. Binary, smaller, faster to encode than JSON on every hop.
- **Batched, pipelined `XACK`.** Acks accumulate in a bounded channel and flush as a single round trip — the silent killer in naive Streams consumers.
- **`XACKDEL` (Redis 8.2)** — atomic ack-and-delete, no ack-then-delete dance.
- **`IDMP` idempotent `XADD` (Redis 8.6)** — DLQ relocation and producer retries are wire-safe.
- **Tokio multi-receiver dispatch.** Per-job work stays off the reader's hot path; DLQ moves run on a dedicated relocator task.

Anti-patterns avoided: blocking Lua scripts, JSON payloads, per-job round trips.

## Quickstart — Rust

Requires Rust 1.85+ (2024 edition) and Redis 8.6+.

```bash
docker run -d --name chasquimq-redis -p 6379:6379 redis:8.6
cargo add chasquimq tokio --features tokio/macros,tokio/rt-multi-thread
```

```rust
use chasquimq::{Producer, Consumer, ProducerConfig, ConsumerConfig, HandlerError};
use serde::{Serialize, Deserialize};
use tokio_util::sync::CancellationToken;

#[derive(Serialize, Deserialize)]
struct EmailJob { to: String }

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let producer = Producer::<EmailJob>::connect(
        "redis://127.0.0.1:6379",
        ProducerConfig { queue_name: "emails".into(), ..Default::default() },
    ).await?;

    producer.add(EmailJob { to: "ada@example.com".into() }).await?;

    let consumer = Consumer::<EmailJob>::new(
        "redis://127.0.0.1:6379",
        ConsumerConfig { queue_name: "emails".into(), concurrency: 100, ..Default::default() },
    );

    consumer.run(|job| async move {
        println!("sending to {}", job.payload.to);
        Ok(bytes::Bytes::new())
    }, CancellationToken::new()).await?;

    Ok(())
}
```

Failed jobs retry with exponential backoff; exhausted ones land in the DLQ stream. See [`docs/engine.md`](docs/engine.md) for retry semantics, delayed jobs, idempotent scheduling, DLQ tooling, and observability hooks.

## Quickstart — Node.js

Single npm package, prebuilt binaries for `darwin` / `linux` / `win32` (arm64 + x64).

```bash
npm install chasquimq
```

```ts
import { Queue, Worker } from "chasquimq"

const queue = new Queue("emails", { connection: { host: "127.0.0.1", port: 6379 } })

// Stable jobId: second call dedups on the same id.
await queue.addUnique("welcome", { to: "alice@example.com" }, { jobId: "welcome:alice" })

const worker = new Worker("emails", async (job) => {
    console.log(`processing ${job.name} (${job.id})`)
    return { delivered: true }   // captured when storeResults: true
}, { connection: { host: "127.0.0.1", port: 6379 }, storeResults: true })

worker.on("completed", (job) => console.log(`sent ${job.name}`))

// Block on the result from anywhere.
const job = await queue.add("welcome", { to: "ada@example.com" })
const result = await job.waitForResult({ timeoutMs: 30_000 })
```

`Queue` / `Worker` / `Job` / `QueueEvents` are the user-facing surface. `Producer` / `Consumer` / `Promoter` / `Scheduler` are re-exported from the same package for power users. See [`chasquimq-node/README.md`](chasquimq-node/README.md).

## Quickstart — Python

abi3 wheels for Python 3.9+ on Linux (x86_64 + aarch64), macOS (x86_64 + aarch64), Windows (x86_64).

```bash
pip install chasquimq
```

```python
import asyncio
from chasquimq import Queue, Worker, Job

async def send_email(job: Job) -> dict:
    print(f"sending {job.data}")
    return {"delivered": True}   # captured when store_results=True

async def main() -> None:
    queue = Queue("emails")

    job = await queue.add("welcome", {"to": "ada@example.com"})
    await queue.add_unique("welcome", {"to": "alice@example.com"}, job_id="welcome:alice")

    worker = Worker("emails", send_email, store_results=True)
    asyncio.create_task(worker.run())

    result = await job.wait_for_result(timeout=30.0)
    print(result)

    await worker.close()
    await queue.close()

asyncio.run(main())
```

`Queue` / `Worker` / `Job` / `QueueEvents` are the user-facing surface. `Producer` / `Consumer` / `Scheduler` are re-exported from the same package for power users. See [`chasquimq-py/README.md`](chasquimq-py/README.md).

## CLI

Install the `chasqui` binary with `cargo binstall chasquimq-cli` (prebuilt tarball, ~3s), or grab a platform-specific binary from [Releases](https://github.com/jotarios/chasquimq/releases) (`curl -LsSf https://github.com/jotarios/chasquimq/releases/latest/download/chasquimq-cli-installer.sh | sh`), or `cargo install chasquimq-cli` from source:

```bash
chasqui inspect emails              # one-shot: stream depth, pending, DLQ, delayed, repeatable
chasqui watch emails                # auto-refreshing dashboard
chasqui dlq peek emails             # render DLQ entries with their failure reason
chasqui dlq replay emails --limit 50
chasqui repeatable list emails
chasqui events emails               # tail the events stream
```

## Feature comparison

| Feature | ChasquiMQ | BullMQ | Bull | Bee |
|:---|:---:|:---:|:---:|:---:|
| Backend | Redis | Redis | Redis | Redis |
| Language | Rust | Node | Node | Node |
| Wire format | MessagePack | JSON | JSON | JSON |
| Native Node SDK | ✓ | ✓ | ✓ | ✓ |
| Native Python SDK | ✓ | — | — | — |
| Pipelined / batched acks | ✓ (default) | opt-in | — | — |
| Idempotent produce (`IDMP`) | ✓ | — | — | — |
| Stable job IDs (`addUnique`) | ✓ | ✓ | ✓ | — |
| Result backends (`getJobResult` / `waitForResult`) | ✓ | ✓ | ✓ | — |
| Delayed jobs | ✓ | ✓ | ✓ | — |
| Idempotent delayed scheduling | ✓ | — | — | — |
| Cancel scheduled job | ✓ | ✓ | ✓ | — |
| Retries (exponential backoff) | ✓ | ✓ | ✓ | ✓ |
| Repeatable / cron jobs | ✓ | ✓ | ✓ | — |
| Dead-letter queue | ✓ | ✓ | ✓ | — |
| First-class observability hooks | ✓ (`MetricsSink`) | 3rd-party | 3rd-party | — |
| CLI dashboard | ✓ (`chasqui`) | 3rd-party | 3rd-party | — |
| Priorities | Future | ✓ | ✓ | — |
| Rate limiter | Future | ✓ | ✓ | — |
| Pause / Resume | Future | ✓ | ✓ | — |
| Parent / child dependencies | Future | ✓ | — | — |
| Web UI | Future | ✓ | ✓ | — |
| Optimized for | Throughput | Jobs | Jobs | Messages |

"Future" rows aren't on the current roadmap. If one is blocking for you, please [open an issue](https://github.com/jotarios/chasquimq/issues).

## Repo layout

```
chasquimq/                  engine crate
chasquimq-node/             Node.js bindings (NAPI-RS) + high-level shim
chasquimq-py/               Python bindings (PyO3) + high-level shim
chasquimq-cli/              `chasqui` binary
chasquimq-bench/            benchmark harness
chasquimq-metrics/          opt-in MetricsSink → metrics-rs / Prometheus adapter
benchmarks/                 results, methodology, reproduction
docs/                       design docs
```

## Roadmap

Phases 1–4 shipped (engine, delayed jobs + retries, Node bindings, Python bindings + CLI). 1.0 polish complete: stable `jobId` + `addUnique`, opt-in result backends, `Job.waitForResult` polling helper, `MissedFiresPolicy` on cron specs, `Python-handler-in-loop` + FFI buffer-copy benches.

Future v1.x candidates: priorities, rate limiter, pause/resume, parent/child dependencies, fair queues, web UI.

## Contributing

PRs welcome. For anything beyond a small fix, please open an issue first — ChasquiMQ has load-bearing constraints (Streams, MessagePack, pipelined acks) that aren't obvious from the code alone. See [`CONTRIBUTING.md`](CONTRIBUTING.md).

## License

MIT — see [`LICENSE`](LICENSE).
