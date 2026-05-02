# Contributing to ChasquiMQ

Thanks for the interest. ChasquiMQ is in **Phase 2 (nearly complete)** — the API surface is small and intentionally pre-1.0. Phase 1 (producer, consumer pool, batched acks, DLQ, graceful shutdown) shipped, and Phase 2 has landed delayed jobs, exponential retry backoff, DLQ inspect/replay tooling, and full observability covering both the promoter and the consumer hot path. Expect breaking changes through 1.0; flag them with `!` + a `BREAKING CHANGE:` footer.

## Before you start

If you're planning more than a small fix, **open an issue first** to align on direction. The product has load-bearing constraints that aren't obvious from the code alone — for example: Redis Streams (not lists), MessagePack (not JSON), pipelined acks (not per-job round trips). PRs that swap these out without discussion will be hard to merge.

## Dev setup

You'll need:

- **Rust 1.85+** (workspace uses 2024 edition; `rust-toolchain.toml` pins stable).
- **Redis 8.6+** running on `127.0.0.1:6379` for the integration tests and the bench harness.

```bash
git clone https://github.com/jotarios/chasquimq
cd chasquimq

# Redis
docker run -d --name chasquimq-redis -p 6379:6379 redis:8.6

# build + test
cargo build
cargo test
```

The bench harness is a separate crate:

```bash
cargo run -p chasquimq-bench --release -- --help
```

## Workflow

1. **Fork**, then branch off `main`.
2. **Keep PRs focused.** One concern per PR. A bug fix + a refactor + a doc tweak is three PRs.
3. **Run the basics before pushing:**
   ```bash
   cargo fmt
   cargo clippy --all-targets -- -D warnings
   cargo test
   ```
4. **Performance-sensitive changes:** run the bench harness *before* and *after* and include the numbers in the PR description. Reference scenarios in [`benchmarks/`](benchmarks/). A change that regresses `queue-add-bulk` or `worker-concurrent` needs an explicit justification (correctness wins are valid; "cleaner code" usually isn't).
5. **Commit messages:** follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) (see below).

## Commit messages

We use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/). The format is:

```
<type>(<optional scope>): <subject>

<optional body — wrap at 72 cols, explains *why*>

<optional footer — BREAKING CHANGE, refs, etc.>
```

Subject: imperative mood, ≤72 chars, no trailing period (`fix race in ack flusher`, not `fixed` / `fixing` / `fixes ...`).

### Types

| Type       | Use for                                                         |
|------------|-----------------------------------------------------------------|
| `feat`     | A new user-facing capability.                                   |
| `fix`      | A bug fix.                                                      |
| `perf`     | A change whose primary goal is throughput/latency improvement (include before/after numbers in the body). |
| `refactor` | Internal restructuring with no behavior change.                 |
| `bench`    | Changes to the bench harness, scenarios, or benchmark docs.     |
| `docs`     | Documentation only.                                             |
| `test`     | Adding or fixing tests.                                         |
| `build`    | Cargo / toolchain / dependency / CI changes.                    |
| `chore`    | Repo maintenance with no code-behavior impact (gitignore, etc.) |

### Scopes

Optional, but useful when the change is localized. Common scopes in this repo: `producer`, `consumer`, `ack`, `dlq`, `redis`, `bench`, `config`.

### Breaking changes

Mark with a `!` after the type/scope **and** a `BREAKING CHANGE:` footer:

```
feat(consumer)!: rename ConsumerConfig.batch to read_batch

BREAKING CHANGE: ConsumerConfig.batch is now ConsumerConfig.read_batch
to disambiguate from ack_batch. Update call sites accordingly.
```

Until 1.0, breaking changes are allowed without a major bump but **must** be flagged this way so the changelog catches them.

### Examples

```
feat(consumer): claim idle pending entries on startup
fix(ack): retry XACKDEL on transient connection drop
perf(producer): switch JobId clones to Arc<str> on the bulk path
refactor(consumer): split reader and worker into submodules
bench: add --scale flag to grow bench windows past noise floor
docs(benchmarks): clarify single-host caveat in headline table
chore: gitignore benchmarks/runs/
```

## What we'd love help with

Phase 2 wrap-up + Phase 3 lead-in (open scope):

- Latency instrumentation in the bench harness (currently only throughput is measured; per-job dispatch-to-ack p99 is the biggest gap).
- Worker CPU measurement *for BullMQ* in our harness, so the "≥50% less CPU" claim becomes defensible.
- A reclaimed-from-CLAIM integration test (slice 5 added the `ReaderBatch.reclaimed` signal but the path lacks a test — see `TODOS.md`).
- DLQ relocator double-write under retry — a pre-existing latent bug surfaced by slice 5; fixable with Redis 8.6 `XADD ... IDMP` (see `TODOS.md`).
- Idempotent delayed enqueue (`add_in_with_id` / `add_at_with_id`) — see `TODOS.md`.
- Phase 3 prep: NAPI-RS bindings exploration (Node.js handlers driven by the Rust engine).

Smaller wins:

- Sharper error messages (the `Error` enum is utilitarian — variants could carry more context).
- Doc examples for less obvious config knobs (`claim_min_idle_ms`, `dlq_inflight`, `max_payload_bytes`).
- Cleaner test fixtures around Redis connection setup.
- A `chasquimq-metrics` example that wires `QueueLabeled` into a multi-queue process so multi-tenant Prometheus scrapes are demonstrated end-to-end.

## What's out of scope (for now)

- **Non-Rust SDKs.** Node bindings come in Phase 3 via NAPI-RS, Python in Phase 4 via PyO3 — not by reimplementing logic. Don't open a JS rewrite.
- **Older Redis fallbacks.** ChasquiMQ targets Redis 8.6+ and uses modern Streams features (`XACKDEL`, `IDMP`). Don't add `version-detect-and-degrade` paths.
- **JSON payloads on the hot path.** MessagePack is load-bearing for the perf claim.

## Reporting bugs

Open an issue with:

- ChasquiMQ version (or commit SHA), Rust version, Redis version.
- Minimal reproduction — ideally a failing test or a small `main.rs`.
- What you expected vs. what happened.

Performance regressions are bugs too: if a release is slower than the previous one on the same hardware, the bench delta is the report.

## License

By contributing, you agree your contributions will be licensed under the MIT License (see [`LICENSE`](LICENSE)).
