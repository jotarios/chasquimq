---
title: The scheduler
description: Why scheduling lives in a separate task, leader election, the promoter, and DST-aware cron.
sidebar:
  order: 7
---

ChasquiMQ has two moving parts that look like one: the **promoter** (handles delayed jobs) and the **scheduler** (handles repeatable specs). Both run as auto-spawned tokio tasks alongside every `Worker`, both use leader election so only one instance fires at a time, and both interact with the consumer indirectly via the main stream.

This page is the why and the how.

## Why scheduling is separate from the consumer

The consumer's hot path is `XREADGROUP` → handler → `XACKDEL`. The scheduler's job is to put new entries onto the stream when their fire time arrives. These are different concerns with different latency profiles:

- The consumer wants to be reading constantly, with handlers in flight, acks batching.
- The scheduler wants to wake up periodically, do a tiny `ZRANGEBYSCORE`, promote due entries, sleep. It's basically idle 90%+ of the time.

If the scheduler ran on the consumer's hot path, every read would have a "did anything become due since last tick" branch — cheap, but the wrong abstraction. As separate tasks they don't compete: the scheduler runs on a dedicated tokio task with `tokio::time::sleep`-based pacing.

## The promoter (delayed jobs)

The promoter promotes due entries from `{chasqui:<queue>}:delayed` (a Redis sorted set scored by run-at-ms) into the main stream.

Each tick:

1. `SET NX EX {chasqui:<queue>}:promoter:lock <holder_id> <ttl>` — acquire the leader lock. If another promoter holds it, this tick is a no-op (events: `LockOutcome::Held`).
2. If we got the lock (`LockOutcome::Acquired`): run the promote Lua script.
3. The script `ZRANGEBYSCORE` for `score <= now`, then for each due entry: `XADD` to the main stream, `ZREM` from the delayed set, `DEL` the `:didx:<id>` side-index. All atomic in one script.
4. Emit `PromoterTick { promoted, depth, oldest_pending_lag_ms }` so observers see how many fired and how stale the oldest delayed entry was.

The script returns `{promoted_count, depth, oldest_pending_lag_ms, [promoted_member_bytes...]}` — depth and lag come back in the same round trip, no extra `ZCARD` / `ZRANGE` calls.

`PromoterConfig::poll_interval_ms` (default 100) controls tick cadence. Tighter = lower retry-fire jitter, more idle Redis CPU. The 100ms default is a good balance for most workloads.

### Why leader election

Multiple workers in a process pool would otherwise all run the promoter and double-fire entries. The `SET NX EX` lock means only one worker per cluster fires per tick; the others are no-ops.

The lock TTL is generous (default 30s). If the lock-holder crashes mid-tick, the next acquire happens after the TTL — entries due during that window simply fire on the next promoter tick (≤30s late). For most workloads that's fine; for tight retry workflows, a tighter promoter is more important than a tighter lock TTL.

## The scheduler (repeatable specs)

The scheduler is the same shape as the promoter, but operating on `{chasqui:<queue>}:repeat` (a sorted set scored by next-fire-ms) plus a per-spec `{chasqui:<queue>}:repeat:spec:<key>` hash holding the encoded `RepeatSpec`.

Each tick:

1. Acquire the scheduler lock (same `SET NX EX` pattern, separate key: `{chasqui:<queue>}:scheduler:lock`).
2. Run the schedule Lua script.
3. The script `ZRANGEBYSCORE` for due specs. For each:
   - `HGET` the spec body.
   - Compute `next_fire_ms` from the cron expression / interval.
   - `XADD` the templated job to the main stream.
   - `ZADD` next-fire-time back into the `:repeat` ZSET.
   - If `MissedFiresPolicy::FireAll { max_catchup }` and we're behind, emit additional fires up to the cap.
4. All atomic in one script.

The scheduler tick interval is `scheduler_tick_ms` (default 1000ms — slower than the promoter because cron specs are typically minute / hour granularity).

### MissedFiresPolicy

When the scheduler was offline across one or more fire windows (deploy, restart, crashed leader-election holder), the policy decides what to do:

- **`Skip`** (default) — drop missed windows. Resume on the first future fire.
- **`FireOnce`** — emit one job to represent the missed windows, then advance.
- **`FireAll { max_catchup }`** — replay each missed window up to `max_catchup` fires, then advance.

The policy is stored on the spec, so it persists across scheduler restarts and is consulted on every catchup decision.

`SCHEDULE_REPEATABLE_SCRIPT` accepts a variable-length list of `(fire_at_ms, payload)` pairs so all catch-up fires plus the next ZADD happen in one atomic round trip. The script's return value reports how many fires happened so the metrics path can attribute them.

## DST-aware cron

Cron expressions are parsed at upsert time. Timezones can be:

- `"UTC"` / `"Z"` — fixed UTC offset.
- `"+05:30"` / `"-08:00"` — fixed numeric offset.
- `"America/New_York"` / `"Europe/Madrid"` — IANA name, resolved via `chrono-tz` (default-features off).

IANA names are DST-aware: a spec with `pattern: "0 2 * * *"` and `tz: "America/New_York"` fires at the local 02:00 wall-clock on both sides of spring-forward and fall-back. The underlying UTC instant shifts by one hour across the boundary.

Implementation note: `parse_tz` returns a `TzKind { Fixed | Named }` enum and `next_cron_after` dispatches once at the entry point so the hot path stays monomorphized — there's no per-cron-tick virtual call.

The 5-field syntax (`m h dom mon dow`) and 6-field syntax (with leading seconds) both parse. Any cron expression the `cron` crate accepts works.

## Scheduler IDs

Each spec has a stable resolved key:

- If the user supplied `repeatJobKey` on upsert, that's the key.
- Otherwise, the engine derives one as `<jobName>::<patternSignature>`. Examples:
  - `daily-rollup::cron:0 9 * * *:UTC`
  - `ping::every:60000`
  - `weekly-report::cron:0 9 * * 1:Europe/Madrid`

The pattern signature is deterministic, so re-upserting the same `(name, pattern, tz)` overwrites in place. The first call returns the freshly minted key; subsequent calls return the same key (idempotent upsert).

## Standalone promoter / scheduler

The `Worker` shim auto-embeds both. For producer-only deployments where no consumer runs locally — say, a service that enqueues jobs and lets a separate worker fleet drain them — you can run a standalone `Promoter` from a dedicated process. See [`chasquimq/examples/standalone_promoter.rs`](https://github.com/jotarios/chasquimq/blob/main/chasquimq/examples/standalone_promoter.rs).

The standalone scheduler is the same shape — instantiate `chasquimq::Scheduler` with the queue name and run it. Multiple schedulers across processes coordinate via the shared `:scheduler:lock`.

## Operational notes

- **Promoter tick = retry latency floor.** Tight retry policies (e.g., `backoff: { type: 'fixed', delay: 5 }`) bottom out at the promoter's tick interval. Tightening `PromoterConfig::poll_interval_ms` reduces this floor at the cost of idle Redis CPU.
- **Scheduler tick = cron jitter floor.** A `0 9 * * *` spec fires somewhere in the 1-second window after 09:00 UTC each day, depending on tick timing. Acceptable for cron; not acceptable for sub-second precision.
- **Both observe `MetricsSink`.** `PromoterTick` carries `oldest_pending_lag_ms` which is your "is the promoter falling behind" gauge. Spikes mean the cluster lost a tick or producer rate exceeded promote rate.
- **Pre-name-on-wire entries decode as `name=""`.** Old entries scheduled before the slice 5 name-on-wire change deliver with empty `name`. Handlers should branch on `name === ""` if they need to handle legacy entries.

For the operational guide: [Schedule repeatable jobs](/guides/schedule-repeatable-jobs/). For metrics: [Observe the engine](/guides/observe-the-engine/).
