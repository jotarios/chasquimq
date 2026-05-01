# TODOS

Deferred work tracked outside of phase plans. Each entry: what, why, pros, cons, context, depends-on.

## Phase 2

### `Producer::add_with_id(id, payload)`

- **What:** Add a `Producer::add_with_id(id: JobId, payload: T) -> Result<JobId>` method that uses the caller-supplied id as both the in-payload `Job.id` and the `IDMP <producer-id> <id>` idempotent-id, instead of generating a fresh ULID internally.
- **Why:** Phase 1's `Producer::add(payload)` generates the ULID internally. If a user wraps `add()` in their own retry loop (above the library), the second call gets a fresh ULID and Redis-side `IDMP` dedup does not fire — the job is double-published. `add_with_id` lets the user hold the id stable across retries.
- **Pros:** Closes the user-driven retry loophole. ~30 LOC + one test. Composes cleanly with the existing `Producer::add` (which can call into `add_with_id` internally with a generated id).
- **Cons:** Two API entry points instead of one. Caller must know not to reuse ids across logically-different jobs. Marginal Phase 1 value because no internal retry loop exists yet.
- **Context:** Phase 1 review surfaced the gap. Phase 2 introduces engine-side retries, which makes user-driven retry less common — but power users will still want the escape hatch.
- **Depends on / blocked by:** Phase 1 must ship `Producer::add` first. No Redis-side dependency.

### Multi-stream sharding decision

- **What:** Decide whether ChasquiMQ Phase 1.5 / Phase 2 should ship multi-stream sharded queues, where a single logical queue is partitioned across N Redis streams (e.g., `chasqui:Q:stream-0` … `chasqui:Q:stream-N`) with consistent hashing, each stream having its own consumer group.
- **Why:** A single Redis stream is a single logical entity from Redis's perspective — its read/write throughput is bounded by Redis's per-stream CPU. If the Phase 1 spike (Step 0b in the plan) shows ChasquiMQ hits a Redis-side ceiling rather than a client-side ceiling, multi-stream sharding may be the only path to ≥3× BullMQ on `worker-concurrent`.
- **Pros:** True horizontal scaling within a single Redis. Removes the per-stream ceiling. Aligns with how Kafka/NATS scale.
- **Cons:** Significant API and implementation complexity (partitioning, rebalance, ordering guarantees within partition vs. across partitions). Doubles or triples the consumer code. Changes the user's mental model.
- **Context:** Outside-voice review raised this as a likely necessity given single-host contention. Phase 1 spike is the deciding signal: if we miss target on `worker-concurrent` AND flamegraph shows time spent in Redis-side waits (not ChasquiMQ code), sharding is on the table.
- **Depends on / blocked by:** Phase 1 spike results. Phase 1 final benchmark.

### External-process bench harness

- **What:** A second benchmark mode where the producer runs in one process, the consumer in another, and stopwatch boundaries come from Redis-side state (`XLEN == 0` AND `XPENDING == 0`) rather than a process-local counter.
- **Why:** The Phase 1 bench mirrors `bullmq-bench` exactly to keep apples-to-apples comparison defensible. But that means our numbers include the same kind of harness overhead BullMQ's numbers do. Once we've established the comparative win, an external-process bench gives a more accurate measurement of the engine itself for absolute claims (blog posts, marketing).
- **Pros:** True engine measurement, not engine+harness. Defensible against "BullMQ measured itself, you measured yourself, but you measured differently" criticism.
- **Cons:** Two harnesses to maintain. Numbers won't be directly comparable to BullMQ's published numbers. More setup complexity (two processes, IPC for signaling, watching Redis-side state).
- **Context:** Outside-voice review proposed this as a methodological correction. Phase 1 deliberately said no because it would break the bullmq-bench comparison the project exists to make.
- **Depends on / blocked by:** Phase 1 must complete first and produce a defensible apples-to-apples comparison. Then this becomes a Phase 2+ "fairness layer" addition.
