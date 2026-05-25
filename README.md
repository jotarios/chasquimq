<p align="center"><img src="docs/chasquimq.svg" alt="ChasquiMQ" width="160"></p>

# ChasquiMQ

The fastest open-source message broker for Redis. Rust-native engine, MessagePack payloads, aggressive pipelining. Native Node.js and Python bindings — handlers run where you write them, the engine pulls jobs.

<p align="center">
  <a href="https://crates.io/crates/chasquimq"><img src="https://img.shields.io/crates/v/chasquimq?logo=rust&label=crates.io" alt="crates.io"></a>
  <a href="https://www.npmjs.com/package/chasquimq"><img src="https://img.shields.io/npm/v/chasquimq?logo=npm&label=npm" alt="npm"></a>
  <a href="https://pypi.org/project/chasquimq/"><img src="https://img.shields.io/pypi/v/chasquimq?logo=pypi&logoColor=white&label=PyPI" alt="PyPI"></a>
  <a href="https://github.com/jotarios/chasquimq/actions/workflows/ci.yml"><img src="https://github.com/jotarios/chasquimq/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue" alt="MIT License"></a>
</p>

<p align="center">
  <strong><a href="https://chasquimq.io">Website</a></strong> ·
  <strong><a href="https://chasquimq.io/start/getting-started/">Getting started</a></strong> ·
  <strong><a href="https://chasquimq.io/reference/">API reference</a></strong> ·
  <strong><a href="https://chasquimq.io/guides/">Guides</a></strong> ·
  <strong><a href="https://chasquimq.io/concepts/">Concepts</a></strong>
</p>

> **Status:** 1.0 shipped; 1.x cloud-Redis polish landed (TLS, connection tuning, rotating-token auth). The public API is stable in shape.

Named after the *chasquis* — the relay runners of the Inca road system who carried messages across the Andes.

## Headline numbers

Apple M3, Redis 8.6 (loopback Docker), `bullmq-bench` vs `chasquimq-bench` on the same host. Measured 2026-05-07 under load avg ~1.8–4.3.

| Scenario | BullMQ 5.76.4 | ChasquiMQ | Ratio |
|---|---:|---:|---:|
| `queue-add-bulk` (50, tiny payload) | 54,455 jobs/s | **188,775 jobs/s** | **3.47×** |
| `worker-concurrent` (100 workers) | 45,643 jobs/s | **111,968 jobs/s** | **2.45×** |
| `queue-add` (single, 10×10 payload) | 13,245 jobs/s | 15,366 jobs/s | 1.16× |

`worker-concurrent` is the most CPU-contention-sensitive scenario in the suite; on a quiet host (load < 1) ChasquiMQ reaches ~419k jobs/s for an 8.78× ratio. See [`benchmarks/`](benchmarks/) for full methodology, distribution stats, and the canonical quiet-host run.

**Latency, low-rate dispatch:** end-to-end p50 ~1 ms, p99.9 < 3 ms; engine-side handler dispatch p99.9 ~13 µs on a no-op handler. Same contended Mac. Methodology and caveats in [`benchmarks/latency-1.x.md`](benchmarks/latency-1.x.md).

## Why it's fast

- **Redis Streams over `LPUSH`/`BRPOP`.** Consumer groups, idle-claim recovery, and atomic ack/delete primitives — not LIST polling.
- **MessagePack payloads** via `rmp-serde`. Binary, smaller, faster to encode than JSON on every hop.
- **Batched, pipelined `XACK`.** Acks accumulate in a bounded channel and flush as a single round trip — the silent killer in naive Streams consumers.
- **`XACKDEL` (Redis 8.2)** — atomic ack-and-delete, no ack-then-delete dance.
- **Atomic DLQ relocation.** Routing a poisoned entry into the DLQ is one Lua script (ack-gate then re-enqueue), so a crash can't leave it both in the DLQ and pending — no duplicate on the next claim.
- **`IDMP` idempotent `XADD` (Redis 8.6)** — producer retries after network blips don't double-publish; also belt-and-suspenders on the DLQ relocate.
- **Tokio multi-receiver dispatch.** Per-job work stays off the reader's hot path; DLQ moves run on a dedicated relocator task.

Anti-patterns avoided: blocking Lua scripts, JSON payloads, per-job round trips.

## Quickstart — Rust

[`chasquimq` on crates.io](https://crates.io/crates/chasquimq) · [docs.rs](https://docs.rs/chasquimq). Requires Rust 1.85+ (2024 edition) and Redis 8.6+.

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

[`chasquimq` on npm](https://www.npmjs.com/package/chasquimq). Single npm package, prebuilt binaries for `darwin` / `linux` / `win32` (arm64 + x64).

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

[`chasquimq` on PyPI](https://pypi.org/project/chasquimq/). abi3 wheels for Python 3.9+ on Linux (x86_64 + aarch64), macOS (x86_64 + aarch64), Windows (x86_64).

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

Install the `chasqui` operator binary. Fastest is `cargo binstall chasquimq-cli` (prebuilt tarball, ~3s); `cargo install chasquimq-cli` builds from source. No Rust toolchain? Use the platform-specific installer (all assets on the [Releases page](https://github.com/jotarios/chasquimq/releases)):

```bash
curl -LsSf https://github.com/jotarios/chasquimq/releases/latest/download/chasquimq-cli-installer.sh | sh
```

Once installed:

```bash
chasqui inspect emails              # one-shot: stream depth, pending, DLQ, delayed, repeatable
chasqui watch emails                # auto-refreshing dashboard
chasqui dlq peek emails             # render DLQ entries with their failure reason
chasqui dlq replay emails --limit 50
chasqui repeatable list emails
chasqui events emails               # tail the events stream
chasqui pause emails                # durably pause every consumer of the queue
chasqui resume emails               # lift the pause
```

## Documentation

Full docs at **[chasquimq.io](https://chasquimq.io)**:

- [Getting started](https://chasquimq.io/start/getting-started/) — install, first job, retries, the CLI.
- [Concepts](https://chasquimq.io/concepts/) — delivery semantics, Redis Streams primer, retry/backoff, the scheduler, architecture decisions.
- [Guides](https://chasquimq.io/guides/) — configure retries, route/replay the DLQ, result storage, repeatable jobs, observe the engine, tune for throughput, produce from AWS Lambda, migrate from BullMQ / Sidekiq / Celery.
- [Reference](https://chasquimq.io/reference/) — [Rust](https://chasquimq.io/reference/rust-api/), [Node](https://chasquimq.io/reference/node-api/), and [Python](https://chasquimq.io/reference/python-api/) APIs, [CLI](https://chasquimq.io/reference/cli/), [options](https://chasquimq.io/reference/options/), [wire format](https://chasquimq.io/reference/wire-format/), [error codes](https://chasquimq.io/reference/error-codes/).
- [Benchmarks](https://chasquimq.io/benchmarks/) — methodology, the 1.0 numbers, regressions and floors.

In-repo: [`docs/engine.md`](docs/engine.md) (engine internals), [`docs/history.md`](docs/history.md) (slice-by-slice changelog).

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
| Event-driven completion wait (`waitUntilFinished`) | ✓ | ✓ | ✓ | — |
| Worker / Queue / Job event listeners (`EventEmitter`-style) | ✓ | ✓ | ✓ | partial |
| Persistent job progress (`updateProgress` / `progress` event) | ✓ | ✓ | ✓ | — |
| Per-job log stream (`Job.log` / `Queue.getJobLogs`) | ✓ | ✓ | ✓ | — |
| Delayed jobs | ✓ | ✓ | ✓ | — |
| Idempotent delayed scheduling | ✓ | — | — | — |
| Cancel scheduled job | ✓ | ✓ | ✓ | — |
| Retries (exponential backoff) | ✓ | ✓ | ✓ | ✓ |
| Repeatable / cron jobs | ✓ | ✓ | ✓ | — |
| Dead-letter queue | ✓ | ✓ | ✓ | — |
| First-class observability hooks | ✓ (`MetricsSink`) | 3rd-party | 3rd-party | — |
| TLS (`rediss://`, ElastiCache encryption-in-transit) | ✓ | ✓ | ✓ | ✓ |
| TCP keepalive + automatic reconnect tuning | ✓ (`ConnectionTuning`) | partial | — | — |
| Redis Cluster (multi-shard / ElastiCache Cluster) | ✓ (`redis-cluster://`) | ✓ | ✓ | — |
| Rotating-token credential hook (ElastiCache IAM) | ✓ (Rust) | — | — | — |
| CLI dashboard | ✓ (`chasqui`) | 3rd-party | 3rd-party | — |
| Priorities | Future | ✓ | ✓ | — |
| Rate limiter | Future | ✓ | ✓ | — |
| Pause / Resume | ✓ | ✓ | ✓ | — |
| Job maintenance (`remove` / `drain` / `clean` / `obliterate`) | ✓ | ✓ | ✓ | — |
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

1.x cloud-Redis polish (May 2026) added TLS (`rediss://`), TCP keepalive + reconnect-policy tuning, `Producer::shutdown` clean disconnect, and a `CredentialProvider` hook for rotating-token auth (ElastiCache IAM) — see [`docs/history.md`](docs/history.md#slice-11--aws-lambda-prerequisites-cloud-redis-polish) for the slice writeup.

Future v1.x candidates: priorities, rate limiter, parent/child dependencies, fair queues, web UI.

## Contributing

PRs welcome. For anything beyond a small fix, please open an issue first — ChasquiMQ has load-bearing constraints (Streams, MessagePack, pipelined acks) that aren't obvious from the code alone. See [`CONTRIBUTING.md`](CONTRIBUTING.md).

## License

MIT — see [`LICENSE`](LICENSE).
