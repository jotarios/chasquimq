# Phase 4 — PyO3 Bindings + CLI Dashboard Design

Companion to [`prd/prd.md`](../prd/prd.md). Mirror of [`docs/phase3-napi-design.md`](./phase3-napi-design.md), trimmed where the prior phase did the heavy lifting.

> **Status:** design (2026-05-03). Phase 3 (Node.js bindings) is complete. The engine surface needed for Phase 4 already exists — no engine slices are blocking. This doc plans the Python bindings (Track A) and CLI dashboard (Track B). Tracks ship in parallel; neither blocks the other.

## 1. Goal & non-goals

**Goal.** Ship a `chasquimq` Python package — Rust engine, asyncio-first API, BullMQ-equivalent semantics — published to PyPI as multi-platform wheels. Plus a `chasqui` CLI binary for queue inspection (depth, DLQ peek/replay, repeatable list, scheduler status, watch mode).

**Win condition.** A Python user can:
1. `pip install chasquimq`
2. `await queue.enqueue("send-email", {"to": "ada@example.com"})` from any asyncio app
3. Run `chasquimq.Worker(queue, handler).run()` and have the engine pull jobs and dispatch into the asyncio event loop without blocking the Rust reader
4. Run `chasqui inspect emails --watch` and see live depth + DLQ size + lag

The Python bindings clear the same Phase 2 throughput gates as the Node bindings (≥3× BullMQ on `queue-add-bulk`, ≥5× on `worker-concurrent`) when a benchmark equivalent is run with a Python handler in the loop.

**Non-goals.**
1. Sync API as the primary surface. Python's modern queue libraries (arq, SAQ, Taskiq) are asyncio-first; that is the API ChasquiMQ leads with. A sync convenience wrapper is acceptable but secondary.
2. Trio / anyio first-class. Asyncio only in v1.
3. Function-reference enqueue (Celery/Dramatiq/RQ idiom). String-named enqueue (arq/SAQ idiom) only — function-pointer marshalling across processes is a serialization rabbit hole and the rest of the ecosystem doesn't need it on Streams.
4. Result backends (`await job.result()`). Phase 5 if demand exists. The events stream covers `completed` / `failed` notifications today.
5. Web dashboard (Bull Board / Flower). PRD explicitly out-of-scopes "complex UI dashboards." CLI only.
6. Python ↔ Rust handler co-routing on a single OS thread. The Python event loop runs on a dedicated bridge thread; the engine reader runs on tokio.
7. Pickle / cloudpickle payloads. MessagePack only — same wire format as the Node bindings, so cross-language interop is preserved.
8. Repeatable parent/child / DAG flows. Same scope as Phase 3.

## 2. Two-layer architecture

```
┌─────────────────────────────────────────────────────┐
│ chasquimq Python package                            │
│                                                     │
│  ┌─────────────────────────────────────────────┐   │
│  │ chasquimq.Queue / Worker / Job / Events     │   │ ← high-level shim
│  │ (pure Python; msgpack on the Python side)   │   │   (src/chasquimq/)
│  └─────────────────────────────────────────────┘   │
│                       │                             │
│  ┌─────────────────────────────────────────────┐   │
│  │ chasquimq._native (NativeProducer/Consumer/ │   │ ← PyO3 layer
│  │ Promoter/Scheduler) — opaque bytes payloads │   │   (src-rs/)
│  └─────────────────────────────────────────────┘   │
│                       │                             │
└───────────────────────┼─────────────────────────────┘
                        │
                ┌───────┴────────┐
                │ chasquimq      │ ← engine (Rust crate, unchanged)
                │ (Rust)         │
                └────────────────┘
```

The native layer mirrors `chasquimq-node`'s split between `src/` (NAPI Rust) and `src-ts/` (TypeScript shim). Layout for Python:

- `chasquimq-py/` — workspace member.
  - `Cargo.toml` — `pyo3` + `pyo3-async-runtimes` deps, `cdylib` crate type.
  - `src-rs/` — Rust PyO3 bindings (mirrors `chasquimq-node/src/`).
  - `src/chasquimq/` — Python shim (mirrors `chasquimq-node/src-ts/`).
  - `pyproject.toml` — maturin build config, package metadata.
  - `tests/` — pytest harness against a Redis 8.6.2 service.

Single PyPI package: `chasquimq`. Multiple platform wheels (linux x86_64 / aarch64, macOS x86_64 / aarch64, windows x86_64). `abi3-py39` feature so one wheel per platform covers Python 3.9+.

## 3. Feature scope (3 tiers)

Same tiering structure as Phase 3 to keep the surface deliberate.

### CORE (v1 ships)

| Feature | Python surface |
| :--- | :--- |
| `Queue.enqueue(name, payload, *, delay=, attempts=, backoff=, job_id=)` | async (sync wrapper available) |
| `Queue.enqueue_bulk(jobs)` | async |
| `Queue.cancel_delayed(job_id)` | async |
| `Queue.peek_dlq(limit)` / `replay_dlq(limit)` | async |
| `Queue.upsert_repeatable(name, payload, repeat=...)` | async |
| `Queue.list_repeatable(limit)` / `remove_repeatable_by_key(key)` | async |
| `Worker(queue, handler, *, concurrency=, max_attempts=, run_scheduler=True)` | async; embeds scheduler unless opt-out |
| `Job` dataclass (`id, payload, attempt, created_at_ms`) | passed to handler |
| `QueueEvents(queue)` — `async for event in events:` | async |
| `RepeatPattern.cron(expression, tz=)` / `every(interval_ms=)` | constructor |
| `BackoffSpec.fixed(delay_ms, jitter_ms=)` / `exponential(initial_ms=, multiplier=, max_ms=, jitter_ms=)` | constructor |
| `UnrecoverableError` | exception → DLQ short-circuit |
| `chasqui inspect <queue>` — depth, lag, DLQ size, repeatable count | CLI |
| `chasqui dlq peek <queue> [--limit N]` | CLI |
| `chasqui dlq replay <queue> [--limit N]` | CLI |
| `chasqui repeatable list <queue>` | CLI |
| `chasqui watch <queue>` — refresh-on-interval table | CLI |
| `chasqui events <queue>` — tail events stream | CLI |

### DEFERRED (raise `NotSupportedError`)

| Feature | Reason |
| :--- | :--- |
| Function-reference enqueue (`@app.task` decorator + `task.delay(...)`) | Cross-process function marshalling; conflicts with msgpack-only payloads. Out for v1. |
| Result backends (`await job.result()`) | Future phase. |
| `MissedFiresPolicy` configurable from Python | Engine default `Skip` exposed; full enum exposed only if a user asks. |
| `MetricsSink` Python subclass | Not exposed in v1 — calling Python on every job hot-path event is too costly. The metrics path is for `metrics-rs` exporters in Rust. Python users wire metrics by tailing the events stream. |
| Sync API as primary | Async is primary; sync is a 5-line wrapper if a user demands it. |
| Trio / anyio backend | Asyncio only. |
| Trio-style structured concurrency | Out. |

### NOT-SUPPORTED (documented upfront)

- Pickle payloads. MessagePack only. (`payload` is restricted to msgpack-encodable types: dict, list, str, bytes, int, float, bool, None.)
- Custom Redis client pass-through. The native layer owns the connection (mirrors `chasquimq-node`).
- Web dashboard. CLI only.

## 4. Wire format compatibility

Inherits Phase 3's contract: `Job` is encoded as a 4- or 5-element msgpack array. Python and Node producers are interchangeable on the same queue. A Python worker can drain a stream produced by a Node worker and vice versa. This is the single largest reason ChasquiMQ exists — Streams + msgpack as the lingua franca.

## 5. Handler dispatch across the FFI

Python's analog to Node's TSFN is `pyo3_async_runtimes::tokio::into_future`. Pattern:

```
1. User registers `async def handler(job: Job) -> None`.
2. Worker.run() spawns a dedicated bridge thread that runs an asyncio loop
   bridged to the tokio runtime (pyo3_async_runtimes::tokio::run).
3. NativeConsumer's tokio task pulls a job, msgpack-decodes the payload
   on the Python side (held as opaque bytes across FFI to keep the engine
   hot path zero-copy).
4. Acquire GIL → call handler(job) → returns a coroutine object → drop GIL
   → into_future(coro) → `.await` on tokio.
5. Coroutine resolves → translate to `Result<(), HandlerError>`.
6. On Python exception, inspect __class__.__name__ == "UnrecoverableError"
   to decide HandlerError::unrecoverable(...) vs HandlerError::new(...).
   Mirror of the Node binding's name-based mapping.
```

The bridge thread is **shared** between the worker, the embedded scheduler, and the events subscriber, so a process running `Worker(...).run()` does not spawn three asyncio loops.

## 6. JobsOptions → engine call map

```
queue.enqueue(name, payload, delay=None, attempts=None, backoff=None, job_id=None)
  delay=None      → NativeProducer.add_with_options(payload_bytes, opts)
  delay=duration  → NativeProducer.add_in_with_options(delay_ms, payload_bytes, opts)
  delay=datetime  → NativeProducer.add_at_with_options(when_ms, payload_bytes, opts)

  opts = NativeAddOptions {
    id: job_id,
    retry: NativeJobRetryOverride { max_attempts: attempts, backoff: <translated> }
  }

queue.upsert_repeatable(name, payload, repeat=RepeatPattern.cron("0 9 * * *", tz="UTC"))
  → NativeProducer.upsert_repeatable(NativeRepeatableSpec)
  → engine SCHEDULE_REPEATABLE_SCRIPT
```

Same translation surface as `chasquimq-node/src/producer.rs`. Validate `kind` strings at the FFI boundary (no silent fallthrough) — same pattern.

## 7. Crate / package layout

- `chasquimq-py/Cargo.toml` — workspace member.
  - `[lib] crate-type = ["cdylib"]`
  - `[dependencies] pyo3 = { version = "0.23", features = ["extension-module", "abi3-py39"] }`
  - `pyo3-async-runtimes = { version = "0.26", features = ["tokio-runtime"] }`
  - `chasquimq = { path = "../chasquimq" }`
  - `tokio`, `rmp-serde`, `bytes`, `tracing` — same dep tree as `chasquimq-node`.
- `chasquimq-py/pyproject.toml` — `[build-system] requires = ["maturin>=1.7,<2.0"]`, `build-backend = "maturin"`.
- `chasquimq-py/src/chasquimq/__init__.py` — high-level shim, exports `Queue`, `Worker`, `Job`, `QueueEvents`, `RepeatPattern`, `BackoffSpec`, `UnrecoverableError`.
- `chasquimq-py/src/chasquimq/_native.pyi` — type stubs for the `cdylib` extension.
- `chasquimq-py/tests/` — pytest, async tests via `pytest-asyncio`.

The Python source layout is `src/chasquimq/` (the **src layout**) so editable installs (`maturin develop`) and PyPI installs use the same import path.

## 8. CLI dashboard (`chasquimq-cli` crate)

Separate Rust crate `chasquimq-cli`, ships the `chasqui` binary. Independent of the Python work — operators install it via `cargo install chasquimq-cli` or download a prebuilt binary.

```
chasqui inspect <queue>        # one-shot snapshot
chasqui watch <queue>          # auto-refresh table (1s default, --interval Ns)
chasqui dlq peek <queue>       # XRANGE + reason histogram
chasqui dlq replay <queue>     # producer.replay_dlq(limit)
chasqui repeatable list <queue>
chasqui repeatable remove <queue> <key>
chasqui events <queue>         # tail XREAD BLOCK on events stream
```

Implementation:
- `clap` for arg parsing.
- `comfy-table` for ASCII tables.
- `crossterm` for `watch` mode (clear + redraw on interval).
- `chasquimq` engine crate for all data access — no parallel Redis client, just `Producer<RawBytes>` + a few read-only `XLEN` / `ZCARD` / `ZRANGE` calls.

`chasqui inspect` queries (single Redis round trip per metric):
| Field | Redis call |
| :--- | :--- |
| stream depth | `XLEN {chasqui:<queue>}` |
| pending | `XPENDING {chasqui:<queue>} <group>` |
| DLQ depth | `XLEN {chasqui:<queue>}:dlq` |
| delayed depth | `ZCARD {chasqui:<queue>}:delayed` |
| oldest delayed lag | `ZRANGE {chasqui:<queue>}:delayed 0 0 WITHSCORES` |
| repeatable count | `ZCARD {chasqui:<queue>}:repeat` |

`chasqui dlq peek` reuses `Producer::peek_dlq` and renders a histogram by reason. `chasqui events` reuses the same XREAD-BLOCK pattern as `QueueEvents`.

`chasqui watch` pipelines the inspect queries every interval — single round trip via fred `Pipeline`.

## 9. CI / release

### Python (`chasquimq-py`)

`.github/workflows/py-ci.yml` — mirror of `node-ci.yml`:
- Build matrix: `linux-x86_64`, `linux-aarch64`, `macos-x86_64`, `macos-aarch64`, `windows-x86_64` via `PyO3/maturin-action@v1`.
- `abi3-py39` so each platform builds once for Python 3.9+.
- Test job: `ubuntu-latest`, downloads `linux-x86_64` wheel, runs `pytest` against Redis 8.6.2 service.
- Publish: gated on `chore(release):` commit prefix on `main`. Uses `PYPI_TOKEN` secret.

### CLI (`chasquimq-cli`)

`.github/workflows/cli-ci.yml`:
- Build matrix: same five platforms.
- Test job: runs `chasqui --version` and a smoke test against Redis 8.6.2.
- Publish: `cargo publish` gated on `chore(release):` commit prefix.
- Optional: `cargo dist` for prebuilt binary tarballs attached to GitHub releases.

## 10. Risks / open questions

1. **GIL contention at high throughput.** `worker-concurrent` at 415k jobs/s in Phase 2 is single-process Rust; with Python handlers in the loop, the GIL is the new ceiling. Slice C will measure the GIL hit and decide whether to spawn N Python sub-interpreters (PEP 684) or stay single-loop. Sub-interpreters are 3.13+ so they're behind an opt-in gate.
2. **`abi3` vs version-specific wheels.** Going `abi3` means accepting the abi3-stable subset of the C API. PyO3 supports `abi3-py39` first-class, so this is low-risk, but verify async-await sugar and `Coroutine` work under abi3.
3. **Bridge thread vs `pyo3_async_runtimes::tokio::main`.** The runtime crate offers two patterns: a `#[pyo3_async_runtimes::tokio::main]` macro that takes over the main thread, vs. spawning a bridge thread in the user's code. We pick the bridge thread because the user's program already owns its event loop — we can't take it over.
4. **Cancellation propagation.** Python `CancelledError` raised inside a handler — does the engine treat it as recoverable error (retry) or terminal (DLQ)? Decision: treat as retryable (matches Node behavior where `AbortError` is just an error). Document.
5. **Pip vs. uv.** Document both. Tests run under `pip` in CI; local dev README mentions `uv`.

## 11. Implementation slices

Two tracks ship in parallel. Each slice is one PR.

### Track A — Python bindings (`chasquimq-py`)

- **A1 (scaffold):** workspace member, `Cargo.toml`, `pyproject.toml`, `maturin develop` builds, hello-world `_native` module exporting one method, pytest stub.
- **A2 (native producer):** `NativeProducer` with `add` / `add_in` / `add_with_options` / `add_in_with_options` / `add_bulk` / `add_in_bulk` / `cancel_delayed` / `peek_dlq` / `replay_dlq` / `upsert_repeatable` / `list_repeatable` / `remove_repeatable_by_key`. All async via `future_into_py`. Pytest coverage against Redis.
- **A3 (native consumer):** `NativeConsumer.run(handler)` with TSFN-equivalent dispatch via `into_future`. Tests for: handler success → ack; handler raises → retry; handler raises `UnrecoverableError` → DLQ; panic in handler → DLQ. Concurrency setting honored.
- **A4 (high-level Python shim):** `Queue` / `Worker` / `Job` / `QueueEvents` in `src/chasquimq/`. `RepeatPattern` / `BackoffSpec` / `UnrecoverableError`. Worker auto-embeds scheduler unless `run_scheduler=False`. Pytest covers end-to-end produce → consume → DLQ → replay.
- **A5 (CI + publish):** maturin-action workflow, multi-platform build matrix, pytest in CI, gated publish on `chore(release):` commit prefix.

### Track B — CLI (`chasquimq-cli`)

- **B1 (scaffold):** workspace member, clap-based command tree, `chasqui inspect` against a real Redis with hardcoded key shape.
- **B2 (DLQ + repeatable subcommands):** `dlq peek`, `dlq replay`, `repeatable list`, `repeatable remove`. Reuse `Producer` for repeatable.
- **B3 (watch + events):** `watch <queue>` with crossterm refresh; `events <queue>` tailing via XREAD-BLOCK.
- **B4 (CI + release):** workflow, `cargo dist` for tarballs, gated publish.

### Order

Track A and Track B are independent. A1 → A2 → A3 → A4 → A5 are sequential. B1 → B2 → B3 → B4 are sequential. The two tracks can interleave.

A1 and B1 ship in parallel as the first batch.

## 12. Definition of done

Phase 4 ships when:
- `pip install chasquimq` works on Linux, macOS, Windows on Python 3.9+.
- `cargo install chasquimq-cli` produces a working `chasqui` binary on Linux + macOS.
- A `chasquimq-bench` benchmark scenario with a Python handler clears Phase 2 numbers ±10% (BullMQ-equivalent test).
- Phase 2 throughput numbers in `benchmarks/chasquimq-phase2-final.md` are not regressed by the engine work needed to support Phase 4 (none should be needed; the engine is ready).
- README.md, CONTRIBUTING.md, CLAUDE.md, prd/prd.md updated to mark Phase 4 complete.
