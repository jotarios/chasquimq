# TODOS

Deferred work tracked outside of phase plans. Each entry: what, why, pros, cons, context, depends-on.

## Bench methodology follow-ups

### Latency histogram for `worker-concurrent`

- **What:** Capture per-job dispatch-to-ack latency in `worker-concurrent` and report p50/p95/p99 latency alongside throughput. Use `hdrhistogram` or a flat ring buffer + sort at end.
- **Why:** Throughput alone hides bad tail behavior. A consumer that does 400k jobs/s with p99 of 5s is much worse for a real product than 200k with p99 of 50ms.
- **Pros:** Closes the biggest gap in our perf claims. ~50 LOC. Lets us defend "low latency, not just high throughput" framing.
- **Cons:** Adds per-job overhead during measurement (timestamp at handler entry, again at ack flush — needs a way to thread the timestamp through the entry_id channel without bloating the hot path).
- **Context:** Surfaced in the bench-methodology critique. Not in Phase 1 because we lead with throughput vs. BullMQ.
- **Depends on / blocked by:** None.

### Variance-mode payload generator

- **What:** Replace `payload.clone()` per job with a generator that produces structurally-similar but bytewise-distinct payloads (e.g., different UUID values per job).
- **Why:** Today every job's MessagePack-encoded bytes are nearly identical (modulo the `Job::new` ULID + timestamp). Best-case branch prediction; encoder hits the hot cache path. Real-world payloads vary; we should bench what users will actually run.
- **Pros:** More honest absolute numbers. Doesn't change apples-to-apples ratio (BullMQ also clones one payload).
- **Cons:** Numbers will drop slightly; might invite unfavorable comparison against external published numbers that used the static-payload methodology.
- **Context:** Methodology critique #7.
- **Depends on / blocked by:** None.

## Phase 2

### Multi-stream sharding decision

- **What:** Decide whether ChasquiMQ Phase 1.5 / Phase 2 should ship multi-stream sharded queues, where a single logical queue is partitioned across N Redis streams (e.g., `chasqui:Q:stream-0` … `chasqui:Q:stream-N`) with consistent hashing, each stream having its own consumer group.
- **Why:** A single Redis stream is a single logical entity from Redis's perspective — its read/write throughput is bounded by Redis's per-stream CPU. If the Phase 1 spike (Step 0b in the plan) shows ChasquiMQ hits a Redis-side ceiling rather than a client-side ceiling, multi-stream sharding may be the only path to ≥3× BullMQ on `worker-concurrent`.
- **Pros:** True horizontal scaling within a single Redis. Removes the per-stream ceiling. Aligns with how Kafka/NATS scale.
- **Cons:** Significant API and implementation complexity (partitioning, rebalance, ordering guarantees within partition vs. across partitions). Doubles or triples the consumer code. Changes the user's mental model.
- **Context:** Outside-voice review raised this as a likely necessity given single-host contention. Phase 1 spike is the deciding signal: if we miss target on `worker-concurrent` AND flamegraph shows time spent in Redis-side waits (not ChasquiMQ code), sharding is on the table.
- **Depends on / blocked by:** Phase 1 spike results. Phase 1 final benchmark.

### Delayed-job ZSET memory encoding bench

- **What:** Benchmark the current "store full MessagePack-encoded `Job<T>` as ZSET member" approach against the alternative of storing `<26-byte ULID>` as the member with payload bytes in a companion `HSET` keyed by ULID. Compare memory per delayed job, promotion throughput, and `add_in` latency.
- **Why:** Redis ZSET encoding flips from compact ziplist to per-entry skiplist once any member exceeds `zset-max-ziplist-value` (default 64 bytes). Most non-trivial jobs cross that threshold, so today's encoding is permanently in skiplist mode. The id-plus-hash split keeps small queues in ziplist but adds a round trip on insert and an `HGET`/`HDEL` pair inside the promote script.
- **Pros:** Decision-quality data for a possible Phase 3 encoding switch; current approach can stand as documented if numbers favor it.
- **Cons:** ~200 LOC of harness + analysis. Will land after the basic delayed-jobs bench scenarios.
- **Depends on / blocked by:** Phase 2 slice 1 must be merged.

### Idempotent delayed enqueue (`add_in_with_id` / `add_at_with_id`)

- **What:** Caller-stable-id variants of `add_in` / `add_at` that dedup at the Redis layer. Likely shape: ZSET member becomes `<26-byte ulid><msgpack bytes>`, with the promote script slicing the ulid prefix to pass to `XADD ... IDMP <producer_id> <ulid> ...`. Or: store id in ZSET member, payload in companion hash, IDMP from the script.
- **Why:** `add_in` / `add_at` today generate a fresh ULID per call, so a caller-driven retry after a network blip will land a duplicate scheduled job. The README documents this caveat, but it's a real gap vs. `Producer::add` (which IS idempotent via `IDMP`).
- **Pros:** Closes the at-least-once gap. Restores symmetry with the immediate-enqueue API.
- **Cons:** Requires a decision on the ZSET encoding (see "Delayed-job ZSET memory encoding bench" above). Touches the promote script, which has been carefully verified.
- **Context:** Self-critique of Phase 2 slice 1 surfaced this as the most material correctness gap. Documented in the meantime.
- **Depends on / blocked by:** Delayed-job ZSET memory encoding bench (the encoding decision drives this).

### Cancel / reschedule a delayed job

- **What:** `Producer::cancel_delayed(job_id)` and `Producer::reschedule(job_id, new_run_at)` for jobs already in the delayed sorted set.
- **Why:** Real apps need "user changed their mind, undo the scheduled email," "support is intervening, fire it now," etc. v1 of delayed jobs intentionally omitted these to ship faster.
- **Pros:** Closes a feature parity gap with BullMQ. Required for any product that exposes delayed scheduling to end-users.
- **Cons:** Requires a companion `id → bytes` hash so we can locate by id without scanning the ZSET. Touches the producer surface and the promote script. Forces a decision on the encoding question above.
- **Context:** Outside-voice review flagged this as a likely follow-up.
- **Depends on / blocked by:** ZSET memory encoding bench. Phase 3 work.

### Strict-MAXLEN policy for queues that can't tolerate eviction

- **What:** A `ConsumerConfig` / `ProducerConfig` knob that switches stream `XADD` from `MAXLEN ~ N` (approximate) to `MAXLEN = N` (strict), guaranteeing entries aren't evicted before consumers read them.
- **Why:** Both Phase 1 and the delayed-job promoter today use approximate trim. If consumers fall behind producers, near-cap entries can be silently lost. Strict trim costs more per `XADD` (Redis must do exact bookkeeping) but eliminates the silent-loss class of bug.
- **Pros:** Correctness option for use cases that prefer back-pressure to data loss.
- **Cons:** Per-`XADD` cost; not a fit for the throughput-first default. Adds a dimension to the test matrix.
- **Depends on / blocked by:** None.

### Fencing tokens for leader election

- **What:** Redlock-style fencing tokens so a paused leader can't run a stale tick after another leader takes over.
- **Why:** Today the script's atomicity prevents double-*promotion*, but a long-paused leader can do a tick of duplicated *work* (one EVALSHA worth). Acceptable in normal operation; unacceptable if duplicated work has side effects (it doesn't here, but might once we add metrics emission).
- **Pros:** Tightens the correctness story for distributed deployments.
- **Cons:** Real fencing requires monotonic per-key counters (e.g., `INCR`) and including the token in every Redis op the leader does. Significant complexity for a problem we haven't measured.
- **Depends on / blocked by:** Need a measured incident or metric showing duplicated-work is hurting.

### External-process bench harness

- **What:** A second benchmark mode where the producer runs in one process, the consumer in another, and stopwatch boundaries come from Redis-side state (`XLEN == 0` AND `XPENDING == 0`) rather than a process-local counter.
- **Why:** The Phase 1 bench mirrors `bullmq-bench` exactly to keep apples-to-apples comparison defensible. But that means our numbers include the same kind of harness overhead BullMQ's numbers do. Once we've established the comparative win, an external-process bench gives a more accurate measurement of the engine itself for absolute claims (blog posts, marketing).
- **Pros:** True engine measurement, not engine+harness. Defensible against "BullMQ measured itself, you measured yourself, but you measured differently" criticism.
- **Cons:** Two harnesses to maintain. Numbers won't be directly comparable to BullMQ's published numbers. More setup complexity (two processes, IPC for signaling, watching Redis-side state).
- **Context:** Outside-voice review proposed this as a methodological correction. Phase 1 deliberately said no because it would break the bullmq-bench comparison the project exists to make.
- **Depends on / blocked by:** Phase 1 must complete first and produce a defensible apples-to-apples comparison. Then this becomes a Phase 2+ "fairness layer" addition.

### Consumer- and retry-side observability hooks

- **What:** Extend `MetricsSink` with consumer-loop hooks: `jobs_claimed_total`, `jobs_acked_total`, `jobs_dlqd_total`, `retry_rescheduled_total`, and tick-style events for ack-flusher latency / batch sizes.
- **Why:** The Phase 2 slice 4 observability surface only covers the promoter. Operators care more about end-to-end job throughput than scheduling lag — currently they have to derive it from `XLEN` polls.
- **Pros:** Closes the rest of the observability lake. Users get a single trait that covers every load-bearing engine subsystem. ~120 LOC of trait additions + threading + tests.
- **Cons:** Each new event added to the trait is technically a breaking change for users who implemented `MetricsSink` themselves (default-method bodies cushion this, but the trait surface grows). Need to decide whether each event family lives under a separate trait or stays on `MetricsSink`.
- **Context:** Surfaced in the `feat/observability` plan-eng-review on 2026-05-01. Promoter hooks shipped; consumer/retry/DLQ hooks deferred.
- **Depends on / blocked by:** None.
