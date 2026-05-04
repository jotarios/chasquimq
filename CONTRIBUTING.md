# Contributing to ChasquiMQ

Thanks for the interest. ChasquiMQ is **Phase 3 complete** — the API surface is small and intentionally pre-1.0. Phase 1 (producer, consumer pool, batched acks, DLQ, graceful shutdown) shipped; Phase 2 added delayed jobs, exponential retry backoff, DLQ inspect/replay tooling, full observability across the promoter and the consumer hot path, idempotent delayed scheduling (`add_in_with_id` / `add_at_with_id` / `add_in_bulk_with_ids`), and cancellation (`cancel_delayed` / `cancel_delayed_bulk`); Phase 3 ships Node.js bindings via NAPI-RS — high-level shim (`Queue` / `Worker` / `Job` / `QueueEvents`), per-job retries, repeatable / cron jobs (DST-aware via `chrono-tz`), and `UnrecoverableError` short-circuit to DLQ. Phase 4 (Python via PyO3 + CLI dashboard) is next. Design doc for the Node bindings lives at [`docs/phase3-napi-design.md`](docs/phase3-napi-design.md). Expect breaking changes through 1.0; flag them with `!` + a `BREAKING CHANGE:` footer.

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

# build + test (full suite, matches CI)
cargo build
cargo test --workspace -- --include-ignored
```

> **Why `--include-ignored`?** The integration tests are gated with `#[ignore = "requires REDIS_URL"]` so a contributor running plain `cargo test` without Redis up doesn't see spurious connection failures. The annotation is a compile-time skip, not a runtime check — setting `REDIS_URL` alone won't un-ignore them. CI runs the full suite via `--include-ignored` against a `redis:8.6.2` service container; you should too. Plain `cargo test` runs the ~24 unit tests only.

The bench harness is a separate crate:

```bash
cargo run -p chasquimq-bench --release -- --help
```

## Workflow

1. **Fork**, then branch off `main`.
2. **Keep PRs focused.** One concern per PR. A bug fix + a refactor + a doc tweak is three PRs.
3. **Run the basics before pushing** (these match the CI gates in [`.github/workflows/ci.yml`](.github/workflows/ci.yml) — push will fail PR review otherwise):
   ```bash
   cargo fmt --all
   cargo clippy --all-targets --workspace -- -D warnings
   cargo test --workspace -- --include-ignored
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

## Producer API shape ladder

The producer's `add*` family looks dense; the surface follows a consistent shape. When adding a new entry point, mirror this layout — and when in doubt, prefer composition over a new method.

| Family | Purpose | Returns |
|---|---|---|
| `add` / `add_in` / `add_at` | Fire-and-forget. Engine generates a fresh ULID per call. | `JobId` |
| `add_with_options` / `add_in_with_options` / `add_at_with_options` | Single job with [`AddOptions`] (optional stable id, optional [`JobRetryOverride`]). | `JobId` |
| `add_with_id` / `add_in_with_id` / `add_at_with_id` | Single job, caller-supplied stable [`JobId`]. Idempotent under producer retry via `SET NX EX` on a dedup marker. | `JobId` |
| `add_bulk` / `add_in_bulk` | Batches of jobs, generated ULIDs, pipelined `XADD` / `ZADD`. | `Vec<JobId>` |
| `add_in_bulk_with_ids` | Batches with per-job stable ids — same idempotence model as `add_in_with_id`, applied per entry. | `Vec<JobId>` |
| `add_bulk_with_options` | Batches sharing one `AddOptions` (retry override applied to every job). `opts.id` is rejected when `payloads.len() > 1` (use `add_in_bulk_with_ids` for per-job ids). | `Vec<JobId>` |

Also: `cancel_delayed` / `cancel_delayed_bulk` for delayed-only entries (see [`Producer::cancel_delayed`]), and `peek_dlq` / `replay_dlq` for the DLQ tooling.

## What we'd love help with

Phase 3 polish + Phase 4 lead-in (open scope):

- **Native NAPI test coverage gaps.** The `chasquimq-node` Vitest suite covers happy paths for `Queue.add` / `Worker` / `QueueEvents` / repeatable / per-job retries / DLQ, but the bench-gate tests around the FFI buffer-copy (`Bytes::to_vec()` → JS `Buffer`) at sustained throughput aren't wired yet.
- **Engine `Consumer` ↔ `Scheduler` symmetry.** The Node `Worker` auto-spawns `NativeScheduler` alongside the consumer, but the engine `Consumer` only auto-embeds `Promoter`. Decide whether the engine should mirror the Node side (or document the asymmetry as intentional).
- **`MissedFiresPolicy` JS exposure.** Engine supports `Skip` / `FireOnce` / `FireAll { max_catchup }`; the Node shim only ever sends the default `Skip`. v2 follow-up.
- **Latency instrumentation in the bench harness.** Throughput-only today; per-job dispatch-to-ack p50/p95/p99 is the biggest gap in the perf claims. See `TODOS.md` → "Latency histogram for `worker-concurrent`".
- **Worker CPU measurement *for BullMQ*.** ChasquiMQ's CPU is instrumented; BullMQ's isn't. The PRD's "≥50% less worker CPU" target needs a parallel measurement before we can claim it.
- **Reclaimed-from-CLAIM integration test.** Slice 5 added the `ReaderBatch.reclaimed` signal; the existing tests cover only `reclaimed == 0`. See `TODOS.md`.
- **DLQ relocator double-write under retry.** Latent correctness gap fixable with Redis 8.6 `XADD ... IDMP` (the equivalent of what `RETRY_RESCHEDULE_SCRIPT` does for the retry path). See `TODOS.md`.
- **Cancel API design follow-up.** Cancel currently deletes the dedup marker on success, which partially undoes slice 6's at-least-once guarantee for a delayed-then-cancelled-then-retried producer call. Two clean resolutions on the table — see the comment thread on [PR #8](https://github.com/jotarios/chasquimq/pull/8) for the trade-off and pick one.
- **`reschedule_delayed`.** Counterpart to `cancel_delayed`. Real apps need "support is intervening, fire it now" / "user changed their mind, push it out a day". See `TODOS.md` → "Cancel / reschedule a delayed job".

Smaller wins:

- Sharper error messages (the `Error` enum is utilitarian — variants could carry more context).
- Doc examples for less obvious config knobs (`claim_min_idle_ms`, `dlq_inflight`, `max_payload_bytes`).
- Cleaner test fixtures around Redis connection setup.
- A `chasquimq-metrics` example that wires `QueueLabeled` into a multi-queue process so multi-tenant Prometheus scrapes are demonstrated end-to-end.

## What's out of scope (for now)

- **Non-Rust SDKs.** Node bindings come in Phase 3 via NAPI-RS, Python in Phase 4 via PyO3 — not by reimplementing logic. Don't open a JS rewrite.
- **Older Redis fallbacks.** ChasquiMQ targets Redis 8.6+ and uses modern Streams features (`XACKDEL`, `IDMP`). Don't add `version-detect-and-degrade` paths.
- **JSON payloads on the hot path.** MessagePack is load-bearing for the perf claim.

## Working on chasquimq-node

The Node.js bindings live in the `chasquimq-node/` crate. It exposes two layers: a high-level Queue/Worker shim (TypeScript, in `src-ts/`) and the raw NAPI bindings (Rust, in `src/`) generated via `napi-rs`. The shim wraps the bindings; the bindings wrap the engine. Either layer is importable from the published package.

Dev setup:

```bash
cd chasquimq-node
npm install
npm run build:debug   # builds the .node addon + dist/
REDIS_URL=redis://127.0.0.1:6379 npm test
```

`build:debug` compiles the Rust addon in debug mode (faster turnaround; use `npm run build` for release). The TypeScript shim compiles to `dist/` via `tsc`. Tests run against a real Redis — point `REDIS_URL` at your local 8.6+ instance.

Branch layout for new work: shim PRs typically branch off `feat/chasquimq-node-native` (the NAPI bindings PR) and live alongside it until it merges. Once `feat/chasquimq-node-native` lands on `main`, follow-ups can branch off `main` directly.

What's stubbed vs. what works:

- **Works:** `Queue.add` (with `{ delay, attempts, backoff, repeat, jobId }` all honored end-to-end), `Queue.addBulk`, `Queue.getRepeatableJobs` / `removeRepeatableByKey`, `Queue.close`; `Worker` with concurrency + `EventEmitter` events (`completed` / `failed`) + auto-spawned `Scheduler` (opt-out via `runScheduler: false`); `Job` reads, cross-process `QueueEvents` subscriber; DLQ peek/replay via the native producer; delayed cancel; `UnrecoverableError` short-circuit to DLQ.
- **Stubbed (`NotSupportedError`):** parent/child flows. (Outside the PRD's Phase 3 scope; tracked as a possible v1.x feature if user demand surfaces.)

See [`docs/phase3-napi-design.md`](docs/phase3-napi-design.md) for the full surface table and the rationale behind each stub.

## Working on chasquimq-py

The Python bindings live in the `chasquimq-py/` crate. Same two-layer split as the Node bindings: a high-level Python shim (in `src/chasquimq/`) wraps the raw PyO3 bindings (Rust, in `src-rs/`). Build orchestration is via `maturin`. Design doc: [`docs/phase4-pyo3-design.md`](docs/phase4-pyo3-design.md).

Dev setup:

```bash
cd chasquimq-py
python3 -m venv .venv && source .venv/bin/activate
pip install --upgrade pip
pip install maturin pytest pytest-asyncio redis msgpack
maturin develop   # builds the cdylib in-place into the active venv
REDIS_URL=redis://127.0.0.1:6379 pytest tests/ -v
```

`maturin develop` is the iteration loop (rebuilds `_native` and reinstalls into the venv). For a release-mode build of the wheel itself, `maturin build --release` produces a `.whl` under `target/wheels/`.

The package is `abi3-py39`, so a single platform wheel covers Python 3.9+. Wheels are published per platform (linux x86_64 + aarch64, macOS x86_64 + aarch64, windows x86_64) — see [`.github/workflows/py-ci.yml`](.github/workflows/py-ci.yml) for the build matrix and the gated PyPI publish.

## CI workflows

This repo has four CI workflows:

| Workflow | Triggers on | What it gates |
|---|---|---|
| [`ci.yml`](.github/workflows/ci.yml) | All pushes to `main` + every PR | Engine: rustfmt, clippy `-D warnings`, full `cargo test` against Redis 8.6.2 |
| [`node-ci.yml`](.github/workflows/node-ci.yml) | Pushes / PRs touching `chasquimq-node/`, `chasquimq/`, or the workflow itself | Multi-platform NAPI build matrix, vitest against Redis 8.6.2, gated npm publish on `chore(release):` |
| [`py-ci.yml`](.github/workflows/py-ci.yml) | Pushes / PRs touching `chasquimq-py/`, `chasquimq/`, or the workflow itself | Multi-platform `abi3` wheel build (maturin-action), pytest against Redis 8.6.2, gated PyPI publish (Trusted Publishing) on `chore(release):` |
| [`release.yml`](.github/workflows/release.yml) (CLI Release) | Git tag matching `**[0-9]+.[0-9]+.[0-9]+*` (e.g., `chasquimq-cli-v0.1.0`) | Multi-platform `chasqui` binary builds via [`cargo dist`](https://opensource.axo.dev/cargo-dist/), shell + powershell installers, attached to a GitHub Release |

Two release models live in the same repo. The Node and Python publishes are commit-message gated on `main`; the CLI publish is **tag-driven** via `cargo dist`, so it doesn't watch `chore(release):`. See [Releasing chasquimq-cli](#releasing-chasquimq-cli) below for the tag form.

## Releasing chasquimq-cli

The `chasqui` CLI ships as a prebuilt tarball per platform (Linux x86_64 + aarch64, macOS x86_64 + aarch64, Windows x86_64) plus shell and PowerShell installers, all attached to a [GitHub Release](https://github.com/jotarios/chasquimq/releases). The pipeline is driven by [`cargo dist`](https://opensource.axo.dev/cargo-dist/) and triggered by **git tags**, not by commit messages — this is intentional, and the standard cargo-dist release model.

To cut a release:

1. Bump `version` in [`chasquimq-cli/Cargo.toml`](chasquimq-cli/Cargo.toml) and commit on `main` (`chore(cli): bump to 0.2.0`).
2. Tag the commit with `chasquimq-cli-v<version>` and push the tag:
   ```bash
   git tag chasquimq-cli-v0.2.0
   git push origin chasquimq-cli-v0.2.0
   ```
3. The [`CLI Release`](.github/workflows/release.yml) workflow runs on the tag push, builds all five platform tarballs (plus the shell/powershell installers and a source tarball), and creates a GitHub Release with the artifacts attached.

Tag form: cargo-dist parses `<package-name>-v<version>` as a singular-package release. `chasquimq-cli-v0.1.0` releases only the CLI. (A bare `v0.1.0` tag would also work today since the CLI is the only `dist`-able crate in the workspace, but pinning to the namespaced form keeps the release scope unambiguous if more bin crates land later.)

Local validation before tagging:

```bash
cargo install cargo-dist --locked   # installs the `dist` binary
dist plan                           # dry-run: prints planned artifacts, no build
dist build --artifacts=local        # builds tarballs into target/distrib/ for the host target
```

The release workflow is regenerated by `cargo dist`. The only hand-edit on disk is the workflow `name:` field (`CLI Release`, distinguishing it from `Node CI` / `Python CI` in the GitHub Actions UI); that hand-edit is allowed via `allow-dirty = ["ci"]` in [`dist-workspace.toml`](dist-workspace.toml). The rest of the workflow body — jobs, matrix, plan/build/host steps — is owned by `cargo dist`. Don't hand-edit; instead, update the config in `dist-workspace.toml` and run `dist init` (or `dist generate`) to regenerate.

### Other workflows' release model

Tag-vs-commit-message gating, by workflow:

- `node-ci.yml` / `py-ci.yml` — gated by `startsWith(github.event.head_commit.message, 'chore(release):')`. Tag a commit with `chore(release): chasquimq-node@0.2.0` (or similar for py) to publish. Requires the version bump and the publish to live in the **same** commit on `main`.
- `release.yml` (CLI) — gated by the **git tag** itself (`tags: ['**[0-9]+.[0-9]+.[0-9]+*']`). The version bump is in `chasquimq-cli/Cargo.toml` on `main`; the publish fires on the tag push. Bump and tag are two separate steps.

## Reporting bugs

Open an issue with:

- ChasquiMQ version (or commit SHA), Rust version, Redis version.
- Minimal reproduction — ideally a failing test or a small `main.rs`.
- What you expected vs. what happened.

Performance regressions are bugs too: if a release is slower than the previous one on the same hardware, the bench delta is the report.

## License

By contributing, you agree your contributions will be licensed under the MIT License (see [`LICENSE`](LICENSE)).
