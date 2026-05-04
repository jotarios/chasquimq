# Product Requirements Document (PRD): ChasquiMQ

**Tagline:** The fastest open-source message broker for Redis.

## 1. Product Vision & Objective
To build a high-performance, background job queue utilizing Redis as the underlying datastore, fundamentally architected to bypass the CPU and architectural bottlenecks of existing Node.js queues. ChasquiMQ will deliver maximum throughput and microsecond latency by utilizing native Redis Streams, binary serialization, and a core engine written entirely in Rust.

## 2. Target Audience
* Backend engineering teams experiencing CPU bottlenecks with Node.js/Python job workers.
* Infrastructure teams who want the throughput of Kafka or NATS, but are mandated to use their existing Redis clusters.
* Developers seeking a simpler, single-binary alternative to JVM-based messaging systems.

## 3. Core Architecture & "The Secret Sauce"
Unlike legacy Redis queues, ChasquiMQ will strictly avoid blocking Lua scripts and human-readable JSON payloads.

* **The Engine:** Rust (using the `tokio` asynchronous runtime).
* **The Datastore:** Redis 8.6+ (mandatory, to use modern Streams features: `IDMP`/`IDMPAUTO` for idempotent `XADD`, `XACKDEL` for atomic ack-and-delete, and `XREADGROUP ... CLAIM` for idle-pending reads in a single round trip). The original draft of this PRD said 5.0+ for "native Streams support"; the engine has since been built on the 8.x feature set and there is no fallback path for older Redis.
* **The Queue Primitive:** Redis Streams (`XADD`, `XREADGROUP`, `XACKDEL`).
* **The Delayed Primitive:** Redis Sorted Sets (`ZADD`, `ZRANGEBYSCORE`), promoted by a leader-elected Lua script.
* **The Serialization:** MessagePack (via `rmp-serde` for zero-overhead parsing).
* **The Network Strategy:** Aggressive connection multiplexing and pipelined acknowledgments.

## 4. Phased Rollout Strategy

| Phase | Focus Area | Key Deliverables | Out of Scope |
| :--- | :--- | :--- | :--- |
| **Phase 1 (MVP)** ✅ | **The Core Engine** | Pure Rust implementation. Push/Pull/Ack loop. MessagePack serialization. Basic error handling. | Delayed jobs, retries, multi-language SDKs. |
| **Phase 2** ✅ | **Scheduling & State** | Delayed jobs (via Sorted Sets) + leader-elected Lua promoter. Exponential retry backoff via delayed-ZSET re-scheduling. Dead-letter queue (DLQ) with inspect/replay tooling and bounded growth. First-class observability hooks (`MetricsSink` trait + `chasquimq-metrics` Prometheus/OTel/StatsD adapter). Idempotent delayed scheduling (`add_in_with_id` etc.). Cancellation (`cancel_delayed`). GitHub Actions CI. | Multi-language SDKs, complex parent/child job flows. |
| **Phase 3** ✅ | **The Node Killer** | Node.js native bindings via NAPI-RS. Allowing JavaScript workers to process jobs pulled by the Rust engine. Design doc: [`docs/phase3-napi-design.md`](../docs/phase3-napi-design.md). | Python/Go bindings. |
| **Phase 4** ✅ | **Ecosystem Expansion** | Python bindings via PyO3. CLI dashboard for monitoring queue health. Design doc: [`docs/phase4-pyo3-design.md`](../docs/phase4-pyo3-design.md). | Complex UI dashboards. |

## 5. Phase 1 (MVP) Strict Requirements

* **Producer:** Must be able to take a Rust `Job` struct, serialize it to MessagePack, and `XADD` it to a designated Redis Stream.
* **Consumer Pool:** Must utilize `tokio` to concurrently pull batches of jobs via `XREADGROUP`, distributing them to available async worker tasks.
* **Acknowledgment:** Upon successful execution by the worker task, the system must batch the job IDs and issue a pipelined `XACK` to Redis.

## 6. Success Metrics

* **Throughput:** Process 3x to 5x more jobs per second than alternatives on the exact same local Redis instance.
* **CPU Utilization:** Consume at least 50% less CPU overhead on the worker machine under maximum load compared to Node.js equivalents.
