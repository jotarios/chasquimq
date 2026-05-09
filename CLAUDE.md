# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository status

**1.0 polish complete; ready for the 1.0 tag.** Phases 1–4 shipped (engine, delayed jobs + retries, Node bindings, Python bindings + CLI). The three PRD-listed 1.0 release blockers are done:

- Function-reference enqueue: stable `jobId` + `Queue.addUnique` on both shims.
- Per-job result backends: opt-in `Queue.getJobResult` + `Job.waitForResult` polling helpers.
- Bench coverage with a non-Rust handler: `python_handler_bench.py` + the Criterion FFI buffer-copy microbench.

**Slice-by-slice engineering history lives in [`docs/history.md`](docs/history.md)** — read that for the long-form context (Phase 2 slices, name-on-wire, the post-#62 polish slices, etc.). This file is the orientation doc; treat the history file as the changelog.

**Deferred 1.x follow-ups:**

- Opt-in result-write bench scenario (`store_results=true` sustained throughput).
- `maxmemory` eviction-behavior verification (engine behavior under Redis eviction policies has not been exercised end-to-end).

## Workspace

- `chasquimq/` — engine crate.
- `chasquimq-node/` — Node.js bindings via `napi-rs` + high-level shim.
- `chasquimq-py/` — Python bindings via PyO3 + high-level shim.
- `chasquimq-cli/` — `chasqui` operator binary.
- `chasquimq-bench/` — bench harness (parallel to `bullmq-bench`).
- `chasquimq-metrics/` — opt-in `MetricsSink` → `metrics-rs` / Prometheus adapter.

CI: `.github/workflows/ci.yml` (rustfmt, clippy `--all-targets --workspace -- -D warnings`, `cargo test --workspace -- --include-ignored` against a `redis:8.6.2` service container) — runs on push to `main` and every PR. Plus `.github/workflows/{node-ci,py-ci,cross-shim,release}.yml`.

## Key files for context

- `README.md` — public-facing pitch, headline numbers, three quickstarts, feature comparison.
- `CONTRIBUTING.md` — dev setup, PR workflow, commit conventions, in/out of scope.
- `docs/engine.md` — engine internals: retry semantics, delayed jobs, DLQ tooling, result backends, observability sinks, operational notes.
- `docs/history.md` — slice-by-slice engineering history.
- `prd/prd.md` — product requirements, source of truth for product intent.
- `benchmarks/README.md` — index for all bench reports.
- `benchmarks/baseline-bullmq.md` — measured BullMQ baseline on this host. **The numbers ChasquiMQ has to beat live here.** Read it before making any perf-related design choice.
- `benchmarks/chasquimq-1.0.md` — same-host 1.0 re-bench (today's contended-host BullMQ + ChasquiMQ side-by-side).
- `benchmarks/runs/` (gitignored) — raw logs land here locally; only the summary `.md` files are committed.

When updating user-facing docs, keep `README.md`, `CONTRIBUTING.md`, `benchmarks/README.md`, and this file in sync. Don't duplicate content across them — link instead.

The upstream BullMQ benchmark suite is **not vendored** — it's cloned at `~/Projects/experiments/bullmq-bench` (sibling to this repo). Treat it as external; don't edit it.

## Product

ChasquiMQ is a Redis-backed message broker / background job queue. Pitch: "the fastest open-source message broker for Redis." Goal is 3–5× throughput and ≥50% less worker CPU vs. Node.js queues (BullMQ, etc.) on the same Redis instance.

## Architecture (load-bearing constraints)

These are not preferences — they're the product's reason to exist. Do not silently swap them out.

- **Language:** Rust on the `tokio` async runtime. The whole core engine is Rust; other-language support comes via FFI bindings (NAPI-RS for Node, PyO3 for Python), not by rewriting logic in those languages.
- **Datastore:** Redis 8.6+ (latest stable, April 2026). The PRD originally said "5.0+"; this project targets the latest tech, so use the modern Streams feature set. Don't add fallback paths for older Redis.
- **Queue primitive:** Redis Streams (`XADD` produce, `XREADGROUP` consume, `XACK` acknowledge). Do **not** reach for `LPUSH`/`BRPOP` or other list-based patterns — bypassing those is a core differentiator.
- **Delayed jobs:** Redis Sorted Sets (`ZADD` with score = run-at timestamp, `ZRANGEBYSCORE` to promote due jobs into the stream).
- **Serialization:** MessagePack via `rmp-serde`. Job payloads are binary, not JSON. JSON anywhere on the hot path is a regression.
- **Network strategy:** Aggressive connection multiplexing and pipelined acks. Batch `XACK` calls; don't ack one job at a time.
- **Anti-patterns to avoid:** blocking Lua scripts, human-readable JSON payloads, per-job round trips. The PRD calls these out explicitly as the bottlenecks ChasquiMQ exists to escape.

### Modern Streams features to use (Redis 8.x)

- **Idempotent producer (8.6):** `XADD ... IDMPAUTO` (or `IDMP <id>`) for at-most-once delivery so producer retries after network failures don't double-publish. Reach for this before inventing application-level dedup.
- **Atomic delete-on-ack (8.2):** `XACKDEL` acks and removes a message in one round trip; `XDELEX` deletes with consumer-group awareness. Both replace ack-then-delete sequences and reduce Redis round trips.
- **Idle-pending reads (8.4):** Consumers can fetch new and idle pending messages in one call — relevant for retry/recovery without a separate `XPENDING`+`XCLAIM` dance.

### Rust client

Engine uses `redis-rs` for the producer/consumer hot paths and `fred` in `chasquimq-cli` for ergonomic pipelining. Both support pipelining and Streams natively. Don't introduce a third client.

## Phased scope

Stay inside the current phase unless the user asks to expand. Building Phase 2 features while Phase 1 is incomplete is scope creep.

- **Phase 1 (MVP) ✅:** Producer, tokio-based consumer pool, batched pipelined `XACK`. DLQ + graceful shutdown.
- **Phase 2 ✅:** Delayed jobs, exponential retry backoff, DLQ tooling, observability, idempotent delayed scheduling + cancel.
- **Phase 3 ✅:** Node.js bindings via `napi-rs`. High-level shim, per-job retries, repeatable / cron jobs (DST-aware), `UnrecoverableError` short-circuit.
- **Phase 4 ✅:** Python bindings via PyO3. abi3 wheels for 5 platforms. `chasqui` CLI for queue inspection.
- **1.0 polish ✅:** Function-reference enqueue (`jobId` + `addUnique`); per-job result backends (`getJobResult` + `waitForResult`); cross-language bench coverage (Python-handler-in-loop + FFI buffer-copy); flat import surface; `MissedFiresPolicy` on shims; native NAPI edge tests.

## Commit conventions

This repo uses **[Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/)**. Full reference and examples in `CONTRIBUTING.md`; the short version:

```
<type>(<optional scope>): <subject ≤72 chars, imperative, no trailing period>
```

Types in use: `feat`, `fix`, `perf`, `refactor`, `bench`, `docs`, `test`, `build`, `chore`. Common scopes: `producer`, `consumer`, `ack`, `dlq`, `redis`, `bench`, `config`, `node`, `py`, `cli`.

- **Pick the most specific type.** A bench-harness change is `bench:`, not `chore:`. A throughput improvement is `perf:`, not `refactor:` — and `perf:` commits should include before/after numbers in the body.
- **Breaking changes:** mark with `!` after the type/scope **and** a `BREAKING CHANGE:` footer. Until 1.0, breakage is allowed without a major bump but must be flagged this way so the changelog catches it.
- **Don't include `Co-Authored-By: Claude` trailers** (per user preference).

## Success metrics drive design choices

Performance is the product. When two implementations are close, prefer the one with fewer allocations, fewer Redis round trips, and less serialization work. Benchmark against a real Redis (8.6+) instance, not mocks — claims of 3–5× throughput need to be defensible on the same hardware as the comparison queue.

### The numbers to beat (BullMQ 5.76.4, Redis 8.6.2, Apple M3, no pipelining)

From `benchmarks/baseline-bullmq.md` — these are the 1× reference. Re-measure on the same host before claiming any win.

| Scenario | BullMQ mean | 3× target | 5× target |
|---|---:|---:|---:|
| `queue-add` (single, 10×10 payload) | 13,961 jobs/s | 41,883 | 69,805 |
| `queue-add-bulk` (bulk 50, tiny) | 60,828 jobs/s | 182,484 | 304,140 |
| `worker-generic` (single consumer) | 13,250 jobs/s | 39,750 | 66,250 |
| `worker-concurrent` (concurrency=100) | 47,707 jobs/s | 143,121 | 238,535 |

The two scenarios that matter for the headline claim: **bulk produce (~61k)** and **concurrent consume (~48k)**. Single-add and single-worker are latency-bound, not throughput tests.

Today's same-host run (2026-05-07, contended Mac host, load avg ~1.8–4.3): ChasquiMQ holds **3.47× BullMQ** on bulk produce; `worker-concurrent` falls to 2.45× under host contention but reproduces the canonical 8.78× on a quiet host. Full numbers in `benchmarks/chasquimq-1.0.md`.

### Lessons from running the baseline

- **`enableAutoPipelining` hurts the worker scenarios on loopback** (-38% on `worker-concurrent`). Pipelining is not a free win; prove it per scenario before turning it on by default in our engine.
- **CPU% is not measured** by `bullmq-bench`. The PRD's "≥50% less worker CPU" target needs us to instrument it ourselves; ChasquiMQ's own bench harness measures it.
- **Single-host contention** caps every number. Fine for our internal A/B against BullMQ on the same host, **not** comparable to BullMQ's published blog numbers.
- **`worker-concurrent` is host-load sensitive.** Quiet host (load < 1) reproduces the 8.78× claim; contended host floors at ~110k–120k jobs/s regardless of engine optimization. The host-load gate in `benchmarks/README.md` formalizes when contention is a valid explanation: only when `git diff <previous-baseline> -- chasquimq/` is empty.

### Reproducing the baseline

```bash
docker start chasquimq-bench-redis  # or: docker run -d --name chasquimq-bench-redis -p 6379:6379 redis:8.6
cd ~/Projects/experiments/bullmq-bench
BULLMQ_BENCH_REDIS_HOST=127.0.0.1 bun src/index.ts
```

Note: `bullmq-bench`'s `package.json` says `"bullmq": "latest"` but the lockfile pinned an older 4.x. Run `bun add bullmq@latest` after cloning if you re-baseline.

## Doc surfaces — keep in sync

When you ship a user-observable feature (new flag, config field, public method, behaviour change, breaking change, new dependency, performance characteristic), update **every** surface below that the change touches. Missing one is a real defect — past PRs in this repo shipped engine features that never reached the Starlight site, leaving users on docs.chasquimq.dev unaware they exist. Treat this as a checklist before opening any feature PR.

| Surface | When to update | What goes here |
|---|---|---|
| `README.md` (root) | New user-visible capability or tagline-worthy claim | Headline pitch, feature comparison table, three quickstarts. Do not duplicate engine internals. |
| `docs/engine.md` | New engine surface, config field, operational gotcha, or behaviour under a Redis policy | Operational notes (terse, one bullet per item). Internals: retry, delayed jobs, DLQ, results, observability. |
| `docs/history.md` | **Every** feature PR or slice that lands | Slice-by-slice engineering changelog. Authoritative log; future sessions read this for context. |
| `chasquimq-node/README.md` | Anything that shows up in the Node API surface | "What's in the box" table + quickstart + topical sections (TLS, retries, etc.). Mirror Python's structure. |
| `chasquimq-py/README.md` | Anything that shows up in the Python API surface | Same shape as the Node README. Mirror the Node structure deliberately. |
| `site/src/content/docs/start/*` | New "first 30 minutes" feature a user would hit immediately | Getting-started tutorials. Diátaxis: tutorial. |
| `site/src/content/docs/concepts/*` | New mental model, semantic guarantee, or design decision worth understanding | Diátaxis: explanation. Cross-link to engine.md, don't duplicate. |
| `site/src/content/docs/guides/*` | New deployment/ops/migration recipe | Diátaxis: how-to. One MDX per task. |
| `site/src/content/docs/reference/*` | New flag, env var, error code, public API, wire-format change | Diátaxis: reference. `node-api.md`, `python-api.md`, `rust-api.md`, `cli.md`, `options.md`, `wire-format.md`, `error-codes.md` — pick the right one(s). |
| `site/astro.config.mjs` | New file in `site/src/content/docs/` | Sidebar is hand-curated; an unlinked page renders but is invisible in nav. |
| `CLAUDE.md` (this file) | Phase boundary, deferred-followup status change, new load-bearing doc surface, new agent rule | Orientation only — link to the canonical source, don't duplicate. |

**Common slip-ups to actively guard against:**

- Adding an MDX file under `site/src/content/docs/...` but forgetting to register it in `site/astro.config.mjs`'s sidebar.
- Updating only `docs/engine.md` and one shim README. The Starlight site's `concepts/` or `reference/` page covering the same surface goes stale silently.
- Asymmetric Node/Python shim READMEs. If you add a section to one, mirror it in the other (same headings, same shape, different language).
- Skipping `docs/history.md`. It is the changelog; not adding to it means "what shipped, when, and why" is lost between sessions.
- Adding a new connection knob, config field, or env var without surfacing it in `site/src/content/docs/reference/options.md`.

**When you see a stale claim in any of these surfaces while doing other work, fix it inline rather than open a separate doc PR.** Cheaper than queueing the fix.

## Design System

**Always read [`site/design/DESIGN.md`](site/design/DESIGN.md) before making any UI/visual decisions for the docs site.** All tokens (color, type, spacing, radius, motion), the type ramp, the component primitives, the accessibility floor, and the explicit anti-patterns are defined there. **Do not deviate without explicit user approval.**

Companion specimens (standalone, frameworkless, openable in any browser):

- [`site/design/preview.html`](site/design/preview.html) — full visual contract (hero, buttons, code, table, lifecycle diagram, benchmark bars, swatches, type scale).
- [`site/design/colors.html`](site/design/colors.html) — every token with computed WCAG 2.2 contrast ratios, light + dark.
- [`site/design/typography.html`](site/design/typography.html) — Geist + Geist Mono + Instrument Serif at the full modular scale.

Token names are stable; if a value is missing, add it to `DESIGN.md` first, then to `site/src/styles/tokens.css`. The Starlight site consumes the design system, it does not define it.
