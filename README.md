![ChasquiMQ](docs/chasquimq.jpeg)

# ChasquiMQ

ChasquiMQ is a Rust-native job queue / message broker built on **Redis**, MessagePack payloads, and aggressive pipelining.

Named after the *chasquis* — the relay runners of the Inca road system who carried messages across the Andes.

> **Status:** Phase 2 complete. Phase 1 (MVP) shipped: producer, consumer pool, batched acks, DLQ, graceful shutdown. Phase 2 has landed delayed jobs (`add_in` / `add_at` / `add_in_bulk` backed by Redis Sorted Sets + a leader-elected promoter), exponential-backoff retries via delayed-ZSET re-scheduling, DLQ inspect/replay tooling, full observability covering both the promoter and the consumer hot path (`MetricsSink` trait with `ReaderBatch` / `JobOutcome` / `RetryScheduled` / `DlqRouted` events; per-handler latency in microseconds; `chasquimq-metrics` adapter for Prometheus/OTel/StatsD with a `QueueLabeled<S>` wrapper for per-queue labels), idempotent delayed scheduling (`add_in_with_id` / `add_at_with_id` / `add_in_bulk_with_ids`), and `cancel_delayed` / `cancel_delayed_bulk`. CI runs rustfmt + clippy `-D warnings` + the full test suite against a `redis:8.6.2` service container on every PR. Public API is still pre-1.0 and will change.

## Headline numbers

On Apple M3, Redis 8.6 (loopback): **3.23× BullMQ** on bulk produce, **8.64× BullMQ** on concurrent consume. Phase 2 also lands delayed jobs at ~750k/s end-to-end and an exponential-backoff retry path at ~119k/s.

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

**Idempotent variants.** `add_in_with_id(id, delay, payload)` / `add_at_with_id(id, when, payload)` / `add_in_bulk_with_ids(delay, items)` accept a stable caller-supplied `JobId` and are safe under producer-driven retries. A single Lua script atomically `SET NX EX`s a dedup marker (`{chasqui:<queue>}:dlid:<job_id>`, TTL = `delay + 1h grace`) and only then `ZADD`s the encoded job — a retry after a network failure that already reached Redis is a no-op returning the same id. The marker grace covers the post-promote window so a delayed retry can't race a successful promotion. The plain `add_in` / `add_at` / `add_in_bulk` calls remain available and use a fresh ULID per call (at-least-once under caller retry, like the original Phase 2 slice 1 surface).

**Cancellation.** `Producer::cancel_delayed(&id) -> bool` removes a previously scheduled delayed job. Returns `true` only when the entry was atomically `ZREM`'d from the delayed ZSET; `false` covers "never scheduled", "side-index expired", and "promoter already moved it to the stream" (the cancel-vs-promote race lost). `cancel_delayed_bulk(&[id])` pipelines many. Both schedule and cancel paths execute as Lua under the queue's `{chasqui:<queue>}` hash tag, so they serialize at Redis — `(cancel returned true, job still delivered)` is impossible.

### DLQ tooling

`Producer::peek_dlq(limit)` reads up to N DLQ entries with their failure metadata (`source_id`, `reason`, optional `detail`, raw payload bytes) without removing them — the inspection API.

`Producer::replay_dlq(limit)` moves up to N DLQ entries back into the main stream atomically. Each entry's `attempt` counter is reset to 0 before re-`XADD` so the replayed job gets a full retry budget (otherwise it'd land in DLQ again on first dispatch). The fix-the-bug-and-requeue workflow.

DLQ growth is capped via `ConsumerConfig::dlq_max_stream_len` (default 100,000). `XADD MAXLEN ~ N` is approximate so a runaway error rate may overshoot temporarily but won't grow unboundedly.

### Observability

Every load-bearing engine subsystem emits structured events through the single `chasquimq::MetricsSink` trait:

| Event | Source | Carries |
| :--- | :--- | :--- |
| `PromoterTick` | promoter (per tick) | `promoted`, `depth`, `oldest_pending_lag_ms` |
| `LockOutcome` | promoter (transition-only) | `Acquired` / `Held` |
| `ReaderBatch` | consumer reader (per non-empty `XREADGROUP`) | `size`, `reclaimed` (CLAIM-recovery count) |
| `JobOutcome` | worker (per handler invocation) | `kind: Ok\|Err\|Panic`, 1-indexed `attempt`, `handler_duration_us` |
| `RetryScheduled` | retry relocator (only when the script gate fires) | 1-indexed `attempt`, `backoff_ms` |
| `DlqRouted` | DLQ relocator (after the relocate succeeds) | `reason: DlqReason`, `attempt` |

Operator identity: `chasquimq_jobs_completed_total + chasquimq_jobs_failed_total` = handler invocations. Reader-side DLQ paths (malformed entry / oversize payload / decode failure / retries-exhausted-on-arrival) emit `DlqRouted` only — the handler never ran, so they carry `attempt: 0`. Total inbound jobs = handler invocations + reader-DLQ.

Plug your own sink in via `PromoterConfig::metrics` or `ConsumerConfig::metrics` — the default is a zero-cost no-op sink, and `chasquimq::metrics::testing::InMemorySink` is provided for integration tests with derived rollup accessors (`jobs_completed()`, `dlq_count(reason)`, `total_retries()`, `last_handler_duration_us()`, etc.). Promoter depth and lag are computed inside the same Lua promote script, so the new observability adds no extra Redis round trips. The per-job hot path adds one `Instant::now()` pair plus one virtual call into a no-op via `catch_unwind` when `NoopSink` is wired (no measurable bench regression).

The engine itself has zero observability dependencies — the trait and a no-op default are all `chasquimq` ships. Two opt-in paths, both in the separate [`chasquimq-metrics`](chasquimq-metrics/) workspace crate (so end users only pull `metrics`-related deps if they actually want them):

- **`metrics-rs` facade route (recommended):** `chasquimq_metrics::MetricsFacadeSink` bridges into the [`metrics`](https://docs.rs/metrics) facade. Wrap with `chasquimq_metrics::QueueLabeled::new(MetricsFacadeSink::new(), "<queue-name>")` to add a `queue` label to every emitted metric (composes — stack additional wrappers for `tenant`, `region`, …). Install your `metrics_exporter_*` recorder of choice and wire the result into `PromoterConfig::metrics` / `ConsumerConfig::metrics`. One small dep (`metrics`). Working end-to-end example with `metrics-exporter-prometheus`: [`chasquimq-metrics/examples/facade_sink.rs`](chasquimq-metrics/examples/facade_sink.rs) — `cargo run --example facade_sink -p chasquimq-metrics`.
- **Direct Prometheus route:** [`chasquimq-metrics/examples/prometheus_sink.rs`](chasquimq-metrics/examples/prometheus_sink.rs) shows a hand-rolled `prometheus`-crate sink + `tiny_http` `/metrics` endpoint, for users who don't want the `metrics-rs` facade in the picture. Run with `cargo run --example prometheus_sink -p chasquimq-metrics`.

Adapter metric names follow Prometheus base-unit convention: durations are exposed as `chasquimq_handler_duration_seconds` and `chasquimq_retry_backoff_seconds` (engine events keep micros/ms internally; the adapter divides at the boundary). Histogram bucket configuration is recorder-side — tune via your Prometheus / OTel exporter, not in the adapter.

### Operational notes

- **Stream MAXLEN trim is approximate.** Both Phase 1 and the delayed-job promoter use `XADD MAXLEN ~ N`. If consumers fall sustainedly behind producers, entries near the cap can be trimmed before they are read. Monitor `XLEN` against your consume rate; the silent failure mode is "job vanished."
- **`cancel_delayed` only works for jobs scheduled via the `_with_id` API surface.** Cancel looks up the exact ZSET member through a side-index (`{chasqui:<queue>}:didx:<job_id>`) that is written only by `SCHEDULE_DELAYED_IDEMPOTENT_SCRIPT` — i.e. by `add_in_with_id` / `add_at_with_id` / `add_in_bulk_with_ids`. Jobs scheduled via the plain `add_in` / `add_at` / `add_in_bulk` go through a direct `ZADD` that doesn't populate the index, so cancel by id is a no-op (returns `false`) for those. Use the `_with_id` variants when cancellation is in scope.
- **Key format uses Redis Cluster hash tags** — every chasqui key looks like `{chasqui:<queue>}:<suffix>`. This is a pre-1.0 breaking change from earlier preview builds; redeploying against a Redis instance that holds old-format keys requires draining or manually renaming. New deployments are unaffected.

## Feature comparison

ChasquiMQ is perf-first and Phase 2 complete; the table is honest about what isn't there yet. See the [Roadmap](#roadmap) for what's coming.

| Feature                                | ChasquiMQ        | BullMQ | Bull   | Bee     |
| :------------------------------------- | :--------------: | :----: | :----: | :-----: |
| Backend                                | Redis            | Redis  | Redis  | Redis   |
| Language                               | Rust             | Node   | Node   | Node    |
| Wire format                            | MessagePack      | JSON   | JSON   | JSON    |
| Concurrency                            | ✓                | ✓      | ✓      | ✓       |
| Atomic ops (`XACKDEL`)                 | ✓                | ✓      | ✓      | ✓       |
| Persistence                            | ✓                | ✓      | ✓      | ✓       |
| Pipelined / batched acks               | ✓ (default)      | opt-in | —      | —       |
| Idempotent produce (`IDMP`)            | ✓                | —      | —      | —       |
| Dead-letter queue                      | ✓                | ✓      | ✓      | —       |
| Graceful shutdown                      | ✓                | ✓      | ✓      | ✓       |
| Delayed jobs                           | ✓                | ✓      | ✓      | —       |
| Idempotent delayed scheduling          | ✓                | —      | —      | —       |
| Cancel scheduled job                   | ✓                | ✓      | ✓      | —       |
| Retries (exponential backoff)          | ✓                | ✓      | ✓      | ✓       |
| First-class observability hooks        | ✓ (`MetricsSink`)| 3rd-party | 3rd-party | — |
| Priorities                             | Phase 3+         | ✓      | ✓      | —       |
| Rate limiter                           | Phase 3+         | ✓      | ✓      | —       |
| Pause/Resume                           | Phase 3+         | ✓      | ✓      | —       |
| Repeatable / cron jobs                 | Phase 3+         | ✓      | ✓      | —       |
| Parent/child dependencies              | Phase 3+         | ✓      | —      | —       |
| Sandboxed worker                       | n/a (Rust)       | ✓      | ✓      | —       |
| UI                                     | Phase 4          | ✓      | ✓      | —       |
| Node SDK                               | Phase 3          | ✓      | ✓      | ✓       |
| Python SDK                             | Phase 4          | ✓      | —      | —       |
| Optimized for                          | Throughput       | Jobs   | Jobs   | Messages |

If a row marked Phase 2/3/4 is blocking for you, please [open an issue](https://github.com/jotarios/chasquimq/issues) — it helps prioritize.

## Repo layout

```
chasquimq/                  engine crate (producer, consumer, promoter, ack flusher, DLQ relocator)
chasquimq-bench/            benchmark harness — same scenarios as bullmq-bench
chasquimq-metrics/          opt-in adapter: MetricsSink → metrics-rs facade + Prometheus example
benchmarks/                 results, methodology, reproduction (see benchmarks/README.md)
docs/                       design docs (e.g. Phase 3 NAPI-RS bindings)
prd/                        product requirements & design intent
spike/                      exploratory throwaway code (not part of the engine)
.github/workflows/ci.yml    CI: rustfmt, clippy -D warnings, full test suite vs redis:8.6.2
```

## Roadmap

- **Phase 1:** Producer, consumer pool, batched pipelined acks, DLQ, graceful shutdown. ✅
- **Phase 2:** ✅ Delayed jobs via sorted sets + Lua promoter; exponential retry backoff via delayed-ZSET re-scheduling; DLQ inspect/replay tooling + bounded growth; promoter observability hooks (`MetricsSink` trait + `chasquimq-metrics` adapter + Prometheus/OTel/StatsD examples); consumer / retry / DLQ observability hooks (`ReaderBatch`, `JobOutcome`, `RetryScheduled`, `DlqRouted` events; per-handler microsecond latency; `QueueLabeled<S>` adapter wrapper for per-queue labels); idempotent delayed scheduling (`add_in_with_id` / `add_at_with_id` / `add_in_bulk_with_ids` with Lua-gated dedup marker); cancellation (`cancel_delayed` / `cancel_delayed_bulk`); GitHub Actions CI.
- **Phase 3:** Node.js bindings via NAPI-RS — JS handlers driven by the Rust engine. [Design doc.](docs/phase3-napi-design.md)
- **Phase 4:** Python bindings via PyO3, CLI monitoring dashboard.

API is still pre-1.0; breaking changes are flagged with `!` in the commit and a `BREAKING CHANGE:` footer.

## Contributing

PRs welcome. For anything beyond a small fix, please open an issue first; ChasquiMQ has load-bearing constraints (Streams, MessagePack, pipelined acks) that aren't obvious from the code alone.

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for dev setup, PR workflow, and what's in/out of scope.

## License

MIT — see [`LICENSE`](LICENSE).
