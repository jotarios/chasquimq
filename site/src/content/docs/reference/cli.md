---
title: CLI
description: Reference for the chasqui operator binary — inspect, dlq, repeatable, watch, events.
sidebar:
  order: 5
---

`chasqui` is the operator binary: queue snapshots, DLQ inspection
and replay, repeatable-spec listing, a live-refreshing dashboard,
and an events-stream tail. One subcommand per task; every command
acts on a queue name and a Redis URL.

## Install

With `cargo binstall` (recommended if you have Rust):

```bash
cargo binstall chasquimq-cli
```

Pulls the prebuilt tarball from GitHub Releases (no source compile). Install `cargo-binstall` itself with `cargo install cargo-binstall` if you don't have it.

Without Rust (one-liner installer; macOS / Linux / Windows):

```bash
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/jotarios/chasquimq/releases/latest/download/chasquimq-cli-installer.sh | sh
```

Or download a platform-specific tarball from the [Releases page](https://github.com/jotarios/chasquimq/releases).

From source (Rust 1.85+):

```bash
cargo install chasquimq-cli
```

The crate is published to [crates.io](https://crates.io/crates/chasquimq-cli) for discoverability and `cargo install` / `cargo binstall` lookup. Binary releases ship via [cargo-dist](https://opensource.axo.dev/cargo-dist/) on GitHub Releases.

## Global flags

`--redis-url <URL>` is global — every subcommand accepts it. Defaults to `redis://127.0.0.1:6379`. Use `redis://host:port` for plain TCP, `rediss://` for TLS, or `redis://user:pass@host:port/db` for auth.

You can also set it once via the `CHASQUI_REDIS_URL` environment variable to avoid passing it every command:

```bash
export CHASQUI_REDIS_URL=rediss://prod-redis.example.com:6380
chasqui inspect emails        # uses the env var
chasqui dlq peek emails       # same
```

The flag overrides the env var when both are set.

`--group <NAME>` is **per-subcommand**, only exposed where it's meaningful (`inspect` and `watch`). Defaults to `default` (matches `ConsumerConfig::group`).

Queue names are the base name — without the `{chasqui:...}` hash-tag wrapping. The CLI computes the full Redis keys (`{chasqui:emails}:s`, `{chasqui:emails}:dlq`, etc.) from the base name.

## `chasqui inspect`

```bash
chasqui inspect <QUEUE> [--redis-url <URL>] [--group <NAME>]
```

One-shot snapshot of a queue. Reports stream depth, pending
count, DLQ depth, delayed depth, oldest delayed lag, and
repeatable-spec count.

**Flags:**

- `--redis-url <URL>` — **default `redis://127.0.0.1:6379`**.
- `--group <NAME>` — consumer group used to compute the pending count. **Default `default`** (matches `ConsumerConfig::group`).

**Example:**

```bash
chasqui inspect emails
# emails        stream=42  pending=0  dlq=3  delayed=11  oldest_delayed_lag=0ms  repeatable=2
```

**Under the hood:** `XLEN` on the main stream, `XPENDING` for the
group, `XLEN` on the DLQ, `ZCARD` on the delayed ZSET, `ZRANGE
WITHSCORES` for the oldest delayed entry, `ZCARD` on the
repeatable ZSET. Single round trip per metric.

## `chasqui dlq peek`

```bash
chasqui dlq peek <QUEUE> [--limit <N>] [--redis-url <URL>]
```

Render the next `N` DLQ entries with a histogram-by-reason summary.

**Flags:**

- `--limit <N>` — **default `20`**.
- `--redis-url <URL>` — **default `redis://127.0.0.1:6379`**.

**Example:**

```bash
chasqui dlq peek emails --limit 5
```

**Under the hood:** `XRANGE {chasqui:<queue>}:dlq - + COUNT <limit>`.
Each entry's `reason` field drives the histogram; the original
`source_id`, `name`, and `detail` are surfaced verbatim.

## `chasqui dlq replay`

```bash
chasqui dlq replay <QUEUE> [--limit <N>] [--yes] [--redis-url <URL>]
```

Atomically replay up to `N` DLQ entries back into the main stream.
The replayed jobs get a fresh retry budget — the `attempt` counter
is reset to `0` before re-`XADD`. Prompts for confirmation unless
`--yes` is passed.

**Flags:**

- `--limit <N>` — **default `100`**.
- `--yes` — skip the interactive confirmation.
- `--redis-url <URL>` — **default `redis://127.0.0.1:6379`**.

**Example:**

```bash
chasqui dlq replay emails --limit 50 --yes
# replayed 50 entries
```

**Under the hood:** Lua script that atomically `XRANGE`s the DLQ,
re-encodes each entry with `attempt = 0`, `XADD`s it back to the
main stream, and `XDEL`s it from the DLQ in a single round trip.
Reasons that the engine itself emitted (`retries_exhausted` etc.)
are preserved on the original entry only — replayed jobs start
clean.

## `chasqui repeatable list`

```bash
chasqui repeatable list <QUEUE> [--limit <N>] [--redis-url <URL>]
```

List repeatable specs ordered by next fire time, ascending.

**Flags:**

- `--limit <N>` — **default `100`**.
- `--redis-url <URL>` — **default `redis://127.0.0.1:6379`**.

**Example:**

```bash
chasqui repeatable list emails
```

**Under the hood:** `ZRANGE {chasqui:<queue>}:repeat 0 <limit-1>
WITHSCORES` followed by a pipelined `HGET <spec_hash> spec` per
key. Payloads are intentionally not fetched — the listing is a
metadata projection (see [`RepeatableMeta`](/reference/rust-api/#repeatablemeta)).

## `chasqui repeatable remove`

```bash
chasqui repeatable remove <QUEUE> <KEY> [--redis-url <URL>]
```

Remove a repeatable spec by its resolved key.

**Flags:**

- `--redis-url <URL>` — **default `redis://127.0.0.1:6379`**.

**Example:**

```bash
chasqui repeatable remove emails 'send-digest::cron:0 8 * * *:UTC'
# removed
```

**Under the hood:** Lua script that atomically `ZREM`s from the
repeat ZSET and `DEL`s the spec hash in a single round trip.

## `chasqui watch`

```bash
chasqui watch <QUEUE> [--interval-ms <MS>] [--redis-url <URL>] [--group <NAME>]
```

Auto-refreshing inspect table. Re-runs the inspect snapshot every
`--interval-ms` ms and renders a delta column for stream and DLQ
depth. Exits cleanly on Ctrl+C.

**Flags:**

- `--interval-ms <MS>` — **default `1000`**. Minimum `1`.
- `--redis-url <URL>` — **default `redis://127.0.0.1:6379`**.
- `--group <NAME>` — **default `default`**.

**Example:**

```bash
chasqui watch emails --interval-ms 500
```

**Under the hood:** the same metric round-trips as
[`chasqui inspect`](#chasqui-inspect), repeated on the timer. No
state lives in the CLI — a restart resets the delta column to `0`.

## `chasqui events`

```bash
chasqui events <QUEUE> [--from <ID>] [--redis-url <URL>]
```

Tail the per-queue events stream
(`{chasqui:<queue>}:events`). One line per event with an ISO-8601
timestamp and sorted `key=value` pairs.

**Flags:**

- `--from <ID>` — start id. **Default `$`** (only new events). Pass `0` to replay the entire retained history.
- `--redis-url <URL>` — **default `redis://127.0.0.1:6379`**.

**Example:**

```bash
chasqui events emails --from 0
# 2026-05-08T14:22:01.123Z  e=waiting   id=01HZA7…  n=welcome
# 2026-05-08T14:22:01.245Z  e=active    id=01HZA7…  n=welcome  attempt=1
# 2026-05-08T14:22:01.301Z  e=completed id=01HZA7…  n=welcome  attempt=1  duration_us=556
```

**Under the hood:** `XREAD BLOCK 5000 COUNT 100 STREAMS
{chasqui:<queue>}:events <id>` in a loop, decoding each entry's
fields into the line format. Numeric fields documented by the
engine schema (`attempt`, `backoff_ms`, `delay_ms`, `duration_us`,
`ts`) keep their integer rendering; everything else is printed
verbatim.

## `chasqui pause`

```bash
chasqui pause <QUEUE> [--redis-url <URL>]
```

Durably pause every consumer of `<QUEUE>`. Sets the cross-process
`{chasqui:<queue>}:paused` key (no TTL). Each running consumer parks
its reader at the next batch boundary: in-flight jobs drain, producers
keep enqueueing, the stream backlog grows. The pause persists across
consumer restarts — a worker started while the key is set comes up
parked before its first read.

**Flags:**

- `--redis-url <URL>` — **default `redis://127.0.0.1:6379`**.

**Example:**

```bash
chasqui pause emails
# queue emails paused; run `chasqui resume emails` to resume
```

Idempotent — pausing an already-paused queue is a no-op.

## `chasqui resume`

```bash
chasqui resume <QUEUE> [--redis-url <URL>]
```

Lift a durable pause set by `chasqui pause` (or `Queue.pause()`).
Deletes the `{chasqui:<queue>}:paused` key; each consumer resumes
within its `pause_poll_ms` window (default 250 ms). Idempotent —
resuming a queue that isn't paused is a no-op. Does not affect a
process-local `Worker.pause()`, which is in-memory and never touches
this key.

**Flags:**

- `--redis-url <URL>` — **default `redis://127.0.0.1:6379`**.

**Example:**

```bash
chasqui resume emails
# queue emails resumed
```

## See also

- [Pause and resume concept](/concepts/pause-and-resume/) — process-local vs. durable pause, and what drains.
- [DLQ and recovery concept](/concepts/dlq-and-recovery/) — when to peek vs. replay.
- [The scheduler concept](/concepts/the-scheduler/) — what `repeatable list` is showing you.
- [Observe the engine guide](/guides/observe-the-engine/) — how the events tail fits with metrics sinks.
- [Error codes](/reference/error-codes/) — what each `reason` in `dlq peek` means.
