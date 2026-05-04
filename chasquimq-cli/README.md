# chasquimq-cli

`chasqui` — a command-line dashboard for [ChasquiMQ](https://github.com/jotarios/chasquimq).

Phase 4 / Track B of the project. See [`docs/phase4-pyo3-design.md`](../docs/phase4-pyo3-design.md) §8 and §11
for the full command surface and roadmap.

## Status

Slice B2 complete. Today the CLI ships:

- `chasqui inspect` — one-shot snapshot of stream depth, pending, DLQ depth, delayed depth, oldest delayed lag, and repeatable count.
- `chasqui dlq peek` — render the next N DLQ entries plus a `reason` histogram.
- `chasqui dlq replay` — atomically replay up to N DLQ entries back into the main stream (resets the attempt counter).
- `chasqui repeatable list` — render repeatable specs ordered by next fire time.
- `chasqui repeatable remove` — remove a repeatable spec by key.

Subsequent slices add `watch` and `events`.

## Install

From the workspace root:

```bash
cargo install --path chasquimq-cli
```

## Usage

### `inspect`

```
chasqui inspect <queue> [--redis-url redis://127.0.0.1:6379] [--group default]
```

`--group` defaults to `default`, which matches the engine's
[`ConsumerConfig::group`](../chasquimq/src/config.rs) default. Pass the value you set
in your worker config if it's different.

### `dlq peek`

```
chasqui dlq peek <queue> [--limit N=20] [--redis-url URL]
```

Renders one row per DLQ entry — `dlq_id`, `source_id`, `reason`, `detail` (truncated
to 60 chars), payload size in bytes — followed by a histogram of `reason → count`
across the entries shown. Payload bytes are intentionally not decoded — they're
opaque msgpack and may carry user-defined types.

```
$ chasqui dlq peek smoketest-queue
┌───────────────────────────┬────────────┬───────────────────┬────────────────────┬───────────────┐
│ dlq peek: smoketest-queue ┆ source_id  ┆ reason            ┆ detail             ┆ payload bytes │
╞═══════════════════════════╪════════════╪═══════════════════╪════════════════════╪═══════════════╡
│ 1777863108519-0           ┆ test-job-1 ┆ retries_exhausted ┆ smoke test fixture ┆ 36            │
└───────────────────────────┴────────────┴───────────────────┴────────────────────┴───────────────┘

┌───────────────────┬───────┐
│ reason            ┆ count │
╞═══════════════════╪═══════╡
│ retries_exhausted ┆ 1     │
└───────────────────┴───────┘
```

### `dlq replay`

```
chasqui dlq replay <queue> [--limit N=100] [--redis-url URL] [--yes]
```

Atomically `XADD`s up to `--limit` DLQ entries back to the main stream and `XDEL`s
them from the DLQ. Each replayed entry's `attempt` counter is reset to 0 so the
job gets a fresh retry budget. Per-job `JobRetryOverride` settings are preserved
verbatim — replay does not silently revert to queue-wide retry config.

Without `--yes`, prints a confirmation prompt to stderr and reads a single line
from stdin. Anything other than `y` / `Y` / `yes` aborts with exit code 1.

```
$ chasqui dlq replay smoketest-queue --limit 1 --yes
replayed 1 of 1 entries
```

### `repeatable list`

```
chasqui repeatable list <queue> [--limit N=100] [--redis-url URL]
```

Lists repeatable specs ordered by next fire time (ascending). The payload bytes
are intentionally not fetched — listing thousands of specs returns one bulk
string per spec only.

```
$ chasqui repeatable list emails
┌──────────────────────────┬─────────────┬───────────────┬────┬────────────────┬───────┐
│ repeatable: emails       ┆ job_name    ┆ pattern       ┆ tz ┆ next fire (ms) ┆ limit │
╞══════════════════════════╪═════════════╪═══════════════╪════╪════════════════╪═══════╡
│ fixture-job::every:60000 ┆ fixture-job ┆ every 60000ms ┆ -  ┆ 1777863248084  ┆ -     │
└──────────────────────────┴─────────────┴───────────────┴────┴────────────────┴───────┘
```

For `Cron` patterns, `tz` shows the configured timezone (`UTC` when unset). For
`Every` patterns, `tz` is `-` and the pattern column carries `every Nms`.

### `repeatable remove`

```
chasqui repeatable remove <queue> <key> [--redis-url URL]
```

Atomically `ZREM`s the spec from the repeat ZSET and `DEL`s its spec hash in a
single Lua round trip — no half-removed state is observable to a concurrent
scheduler tick. Prints `removed` on success or `not found` if no spec with that
key existed.

## Sources

| Field | Redis call |
| :--- | :--- |
| stream depth | `XLEN {chasqui:<queue>}:stream` |
| pending | `XPENDING {chasqui:<queue>}:stream <group>` (summary form) |
| DLQ depth | `XLEN {chasqui:<queue>}:dlq` |
| delayed depth | `ZCARD {chasqui:<queue>}:delayed` |
| oldest delayed lag (ms) | `ZRANGE {chasqui:<queue>}:delayed 0 0 WITHSCORES` then `now_ms - score`, clamped at 0 |
| repeatable count | `ZCARD {chasqui:<queue>}:repeat` |

`inspect` issues all six in a single fred `Pipeline`, so a snapshot is one Redis
round trip. `dlq peek` / `dlq replay` / `repeatable list` / `repeatable remove`
all reuse the engine `Producer` directly — no parallel script paths.

When the consumer group does not exist yet, `inspect`'s `pending` cell renders
as `0 (group not created)` instead of erroring out.
