---
title: Python API
description: Complete reference for the chasquimq Python package — Queue, Worker, Job, QueueEvents, every option type, every method signature.
sidebar:
  order: 3
---

The Python shim wraps the Rust engine through a PyO3 binding and
ships abi3 wheels for cpython 3.10+. The high-level surface
(`Queue` / `Worker` / `Job` / `QueueEvents`) mirrors the Node API
shape so cross-language migrations stay mechanical.

Install:

```bash
pip install chasquimq
```

Import:

```python
from chasquimq import (
    Queue,
    Worker,
    Job,
    QueueEvents,
    QueueEvent,
    RepeatPattern,
    BackoffSpec,
    MissedFiresPolicy,
    RepeatableMeta,
    UnrecoverableError,
    NotSupportedError,
    version,
)
```

## On this page

- [Queue](#queue) — async producer for one queue.
- [Worker](#worker) — async consumer with a user-supplied handler.
- [Job](#job) — the dataclass passed to your handler.
- [QueueEvents](#queueevents) and [QueueEvent](#queueevent) — async-iterator subscriber.
- [RepeatPattern](#repeatpattern), [BackoffSpec](#backoffspec), [MissedFiresPolicy](#missedfirespolicy), [RepeatableMeta](#repeatablemeta) — schedule and retry value types.
- [Type aliases](#type-aliases) — `Handler`, `DelayMsLike`, `BackoffLike`, `RepeatLike`, `MissedFiresLike`.
- [Errors](#errors) — `NotSupportedError`, `UnrecoverableError`.
- [Native power-user surface](#native-power-user-surface) — the underscore-free `Producer` / `Consumer` / `Scheduler` re-exports.

## Queue

```python
class Queue:
    def __init__(
        self,
        name: str,
        *,
        redis_url: str = "redis://127.0.0.1:6379",
        tls: bool = False,
        max_stream_len: int | None = None,
        max_delay_secs: int | None = None,
    ) -> None: ...
```

High-level producer for a single ChasquiMQ queue. Construct one
per queue. The native producer pool is created lazily on the first
call. Safe to share across asyncio tasks and across threads.

- `name` — queue name.
- `redis_url` — **default `"redis://127.0.0.1:6379"`**.
- `tls` — **default `False`**. When `True`, upgrades a `redis://` `redis_url` (or schemeless `host:port`) to `rediss://`; already-`rediss://` URLs pass through. Engine negotiates TLS via fred's `enable-rustls-ring`; trust roots come from `rustls-native-certs`. For private CAs, set `SSL_CERT_FILE` to a PEM bundle before launching Python (takes precedence over the platform store).
- `max_stream_len` — `XADD MAXLEN ~` cap. **Default engine value: `1_000_000`.**
- `max_delay_secs` — reject `add` calls with `delay > this`. **Default engine value: 30 days.**

### `await queue.add(name, data, *, delay_ms, delay, attempts, backoff, job_id, repeat, missed_fires)`

```python
async def add(
    self,
    name: str,
    data: Any,
    *,
    delay_ms: DelayMsLike | None = None,
    delay: timedelta | None = None,
    attempts: int | None = None,
    backoff: BackoffLike | None = None,
    job_id: str | None = None,
    repeat: RepeatLike | None = None,
    missed_fires: MissedFiresLike | None = None,
) -> Job
```

Enqueue a single job. Returns a [`Job`](#job) whose `id` is the
engine-minted ULID, or the resolved spec key when `repeat` is set.

- `delay_ms` — schedule for future delivery in **milliseconds** (BullMQ-compatible). Accepts `int`, `float`, or a `datetime` for absolute fire time. Naive datetimes are treated as UTC.
- `delay` — alternative parallel kwarg accepting a `datetime.timedelta`. Passing both `delay_ms` and `delay` raises `ValueError`.
- `attempts` — per-job override of queue-wide `max_attempts`.
- `backoff` — per-job backoff override. `int` (fixed delay in ms), [`BackoffSpec`](#backoffspec), or a raw native dict.
- `job_id` — stable id for at-most-once / idempotent scheduling.
- `repeat` — upsert a repeatable spec. See [`RepeatPattern`](#repeatpattern).
- `missed_fires` — catch-up policy. Only meaningful with `repeat`; raises `ValueError` otherwise.

**Raises** `ValueError` for whitespace-only `job_id`, negative or
non-finite delays, or `missed_fires` set without `repeat`. Raises
`TypeError` for wrong-typed `delay`.

### `await queue.add_unique(name, data, *, job_id, ...)`

```python
async def add_unique(
    self,
    name: str,
    data: Any,
    *,
    job_id: str,
    delay_ms: DelayMsLike | None = None,
    delay: timedelta | None = None,
    attempts: int | None = None,
    backoff: BackoffLike | None = None,
    repeat: RepeatLike | None = None,
) -> Job
```

Idempotent variant. Requires `job_id`. Same semantics as the Node
[`addUnique`](/reference/node-api/#queueaddunique-name-data-opts):
strict cross-process dedup on the delayed path; immediate-path
dedup is bounded to a single producer.

### `await queue.add_bulk(jobs)`

```python
async def add_bulk(self, jobs: Sequence[dict]) -> list[Job]
```

Enqueue many jobs. Each entry is a dict with keys `name`, `data`,
and optional `delay_ms`, `delay`, `attempts`, `backoff`, `job_id`,
`repeat`. When all entries lack per-job overrides the call routes
through `add_bulk_named` (pipelined); otherwise degrades to a
per-entry `add()` loop.

### `await queue.cancel_delayed(job_id)`

```python
async def cancel_delayed(self, job_id: str) -> bool
```

Atomically remove a delayed job by its stable id. Returns `True`
if the job was still in the delayed ZSET and is now removed,
`False` if it was already promoted (or never existed).

### `await queue.peek_dlq(limit=20)`

```python
async def peek_dlq(self, limit: int = 20) -> list[dict]
```

Return up to `limit` DLQ entries oldest-first. Each dict carries
`dlq_id`, `source_id`, `reason`, optional `detail`, `name`, and
the raw `payload` bytes.

### `await queue.replay_dlq(limit=100)`

```python
async def replay_dlq(self, limit: int = 100) -> int
```

Move up to `limit` DLQ entries back into the main stream with
their attempt counter reset. Returns the number of entries
actually replayed.

### `await queue.upsert_repeatable_job(name, data, *, repeat, ...)`

```python
async def upsert_repeatable_job(
    self,
    name: str,
    data: Any,
    *,
    repeat: RepeatPattern,
    limit: int | None = None,
    start_after_ms: int | None = None,
    end_before_ms: int | None = None,
    attempts: int | None = None,
    backoff: BackoffLike | None = None,
    job_id: str | None = None,
    missed_fires: MissedFiresLike | None = None,
) -> Job
```

Upsert a repeatable / cron spec. Returns a `Job` whose `id` is
the resolved spec key — pair with
[`remove_repeatable_by_key`](#await-queueremove_repeatable_by_keykey)
to delete.

`attempts` / `backoff` / `job_id` are accepted for API symmetry
with `add` but not threaded into per-fire jobs in v1.

### `await queue.get_repeatable_jobs(limit=100)`

```python
async def get_repeatable_jobs(self, limit: int = 100) -> list[RepeatableMeta]
```

List repeatable specs ordered by next fire time, ascending.

### `await queue.remove_repeatable_by_key(key)`

```python
async def remove_repeatable_by_key(self, key: str) -> bool
```

Remove a repeatable spec by its resolved key.

### `await queue.get_job_result(job_id)`

```python
async def get_job_result(self, job_id: str) -> Any
```

Read the stored result for `job_id`. Returns the msgpack-decoded
value, or `None` for the three indistinguishable cases (job not
yet completed, result expired, no result was ever written).

### `await queue.get_job_result_bulk(job_ids)`

```python
async def get_job_result_bulk(self, job_ids: Sequence[str]) -> list[Any]
```

Pipelined bulk variant. Returns a list aligned by index with
`job_ids`.

### `await queue.get_job(job_id)`

```python
async def get_job(self, job_id: str) -> Optional[Job]
```

Look up a single job across the four queue surfaces (stream PEL,
delayed ZSET, main stream, DLQ, result key). Bounded scan; returns
`None` when the id isn't found in any surface. The returned `Job`'s
`data` is msgpack-decoded; engine state surfaces via `attempt` /
`name` / `created_at_ms`.

### `await queue.get_jobs(state="waiting", offset=0, limit=100, cursor=None)`

```python
async def get_jobs(
    self,
    state: str = "waiting",
    offset: int = 0,
    limit: int = 100,
    cursor: Optional[str] = None,
) -> list[Job]
```

Paginated listing within a single state. `state` is one of
`"waiting" | "active" | "delayed" | "completed" | "failed"`. Returns
just the first page. For multi-page sweeps, use `get_jobs_page`.

### `await queue.get_jobs_page(state, *, offset=0, limit=100, cursor=None)`

```python
async def get_jobs_page(
    self,
    state: str = "waiting",
    *,
    offset: int = 0,
    limit: int = 100,
    cursor: Optional[str] = None,
) -> Tuple[list[Job], Optional[str]]
```

Page + `next_cursor`; the cursor is `None` at the end of the range.
Cursor semantics depend on state:

- `"waiting"` / `"failed"`: cursor is the last stream entry id seen
  (exclusive). `offset` is honored only on the first call (when
  `cursor is None`).
- `"delayed"`: cursor is `score:offset_into_score` so a tied-fire-ms
  page boundary doesn't drop tied members (cron specs firing on the
  minute, identical absolute times).
- `"completed"`: cursor is the raw `SCAN` cursor (`"0"` resets to the
  start).

### `await queue.get_job_state(job_id)`

```python
async def get_job_state(self, job_id: str) -> str
```

One of `"waiting" | "active" | "delayed" | "completed" | "failed" |
"unknown"`. **Live-state-first**: a job that's been replayed from DLQ
resolves as `"waiting"` (not `"completed"`) during the race window.

### `await queue.get_job_counts(*types)`

```python
async def get_job_counts(self, *types: str) -> dict[str, int]
```

Per-state counts: `{"waiting", "active", "delayed", "completed",
"failed", "paused"}`. Pass no args for the full dict; pass one or
more state names to filter. `completed` is via bounded `SCAN` over
`result:*` keys — large keyspaces return a lower-bound figure (cap
configurable via the `CHASQUIMQ_COMPLETED_SCAN_CAP` env var on the
engine).

### `await queue.get_waiting_count()`, `get_active_count()`, `get_delayed_count()`, `get_completed_count()`, `get_failed_count()`

```python
async def get_waiting_count(self) -> int
async def get_active_count(self) -> int
async def get_delayed_count(self) -> int
async def get_completed_count(self) -> int
async def get_failed_count(self) -> int
```

Single-column convenience wrappers over `get_job_counts`.

### `await queue.count()`

```python
async def count(self) -> int
```

`waiting + active + delayed` — the number of jobs that could still
run.

### `await queue.pause()`, `await queue.resume()`, `await queue.is_paused()`

```python
async def pause(self) -> None
async def resume(self) -> None
async def is_paused(self) -> bool
```

Durable, cross-process pause. `pause()` sets the
`{chasqui:<queue>}:paused` key (no TTL); every consumer of the queue
parks its reader at the next batch boundary while in-flight jobs
drain and producers keep enqueueing. Survives consumer restarts until
`resume()`. Idempotent. Queue-wide analogue of
[`worker.pause()`](#workerpause-workerresume-workeris_paused); the
same key is toggled by `chasqui pause` / `chasqui resume`. See the
[Pause and resume concept](/concepts/pause-and-resume/).

### `await queue.close()`

```python
async def close(self) -> None
```

If a producer was lazily connected, awaits the underlying pool's
`QUIT` (clean disconnect) before clearing the handle. Idempotent.
Calling without ever issuing an `add()` is a no-op. Compatible with
`async with`:

```python
async with Queue("emails") as queue:
    await queue.add("welcome", {"to": "ada@example.com"})
# queue.close() runs at scope exit.
```

`close()` is for clean disconnect, not flush. Every `await queue.add()`
already waits for Redis to ack the `XADD` before resolving — by the
time the call returns, the message is committed. Hosts that may be
frozen the moment your handler returns (AWS Lambda, Cloud Run) can
return immediately after `add` resolves; calling `close()` is
optional polish.

### Properties

- `queue.name` — the queue name.
- `queue.is_closed` — `True` after the first `close()` call.

## Worker

```python
class Worker:
    def __init__(
        self,
        queue_name: str,
        handler: Handler,
        *,
        redis_url: str = "redis://127.0.0.1:6379",
        tls: bool = False,
        concurrency: int = 100,
        max_attempts: int = 25,
        group: str = "default",
        consumer_id: str | None = None,
        read_block_ms: int | None = None,
        read_count: int | None = None,
        claim_min_idle_ms: int | None = None,
        max_payload_bytes: int | None = None,
        dlq_max_stream_len: int | None = None,
        events_enabled: bool = True,
        delayed_enabled: bool = True,
        run_scheduler: bool = True,
        scheduler_tick_ms: int | None = None,
        store_results: bool = False,
        result_ttl_ms: int | None = None,
    ) -> None: ...
```

High-level async worker for a single queue. Construction does not
start the engine loop — call `await worker.run()`. The native
consumer auto-embeds a scheduler task (controlled by
`run_scheduler`); multiple workers cooperate via leader election
on `{chasqui:<queue>}:scheduler:lock`.

- `concurrency` — max in-flight handler invocations. **Default `100`.**
- `max_attempts` — total attempts per job. **Default `25`** (the engine cap; the Node shim's `maxStalledCount` defaults lower).
- `group` — consumer group name. **Default `"default"`.**
- `consumer_id` — optional XREADGROUP consumer name.
- `read_block_ms` — `XREADGROUP BLOCK` timeout. **Default engine: `5000`.**
- `read_count` — `XREADGROUP COUNT`. **Default engine: `64`.**
- `claim_min_idle_ms` — `CLAIM` recovery threshold. **Default engine: `30_000`.**
- `max_payload_bytes` — DLQ-route oversize payloads above this. **Default engine: `1_048_576` (1 MiB).**
- `dlq_max_stream_len` — `MAXLEN ~` cap on the DLQ stream. **Default engine: `100_000`.**
- `events_enabled` — write to the per-queue events stream. **Default `True`.**
- `delayed_enabled` — auto-spawn the delayed-job promoter. **Default `True`.**
- `run_scheduler` — auto-spawn the embedded scheduler. **Default `True`.**
- `scheduler_tick_ms` — scheduler tick interval. **Default engine: `1000`.**
- `store_results` — persist handler return values to the result key. **Default `False`.**
- `result_ttl_ms` — TTL for stored results. **Default engine: `3_600_000` (1h).** Rounded up to whole seconds at the FFI boundary.

### `await worker.run()`

```python
async def run(self) -> None
```

Start the engine loop and resolve once it drains. Idempotent —
calling `run()` more than once awaits the in-flight loop instead
of starting a second one.

### `await worker.close()`

```python
async def close(self) -> None
```

Signal shutdown. Trips the consumer's shutdown token; the
in-flight `run()` returns promptly. Safe to call from any
coroutine, any number of times. Does not await the engine task
itself — that avoids the double-await race when a caller invokes
`close` while `run` is still in flight.

`Worker` implements `__aenter__` / `__aexit__`:

```python
async with Worker("emails", handler) as worker:
    await worker.run()
```

### `worker.pause()`, `worker.resume()`, `worker.is_paused()`

```python
def pause(self) -> None
def resume(self) -> None
def is_paused(self) -> bool
```

Process-local pause for this worker instance (synchronous — no
`await`). `pause()` stops the reader at the next batch boundary;
jobs already in a handler run to completion; producers keep
enqueueing. `resume()` wakes the parked reader immediately. In-memory
only — does not write the cross-process flag and does not survive a
process restart; for queue-wide durable pause use
[`queue.pause()`](#await-queuepause-await-queueresume-await-queueis_paused).
Safe to call before `run()` (a pause requested while the native
consumer is still deferred via `credential_provider` is applied as
soon as it is constructed, so it parks before its first read).
Idempotent.

### Properties

- `worker.name`
- `worker.is_running` — engine task in flight.
- `worker.is_closed` — `True` after the first `close()` call.

### Type alias: `Handler`

```python
Handler = Callable[[Job], Awaitable[Any]]
```

The handler signature: an async function that takes one `Job` and
returns anything msgpack-encodable (or `None`).

## Job

```python
@dataclass
class Job:
    id: str
    name: str
    data: Any
    attempt: int
    created_at_ms: int
```

The lightweight value type passed to user handlers and returned
from `Queue.add`. v1 deliberately does not round-trip through
Redis for any field on this object — the engine streams jobs and
does not persist progress, return values, or per-job state.

### Properties

- `id` — engine-minted ULID, or resolved spec key for repeatable upserts.
- `name` — dispatch name from the engine `n` field. Empty when the producer added the job without a name (or when the delayed / repeatable path re-encoded without it).
- `data` — msgpack-decoded payload.
- `attempt` — 1-indexed attempt counter on the consumer side; `0` on the producer side.
- `created_at_ms` — submission time in epoch ms.
- `attempts_made` — alias of `attempt`, matches BullMQ naming.

The dataclass is frozen-by-equality on the public fields; an
optional internal `_queue` backreference is set when the job is
returned by `Queue.add` so [`wait_for_result`](#await-jobwait_for_resulttimeoutpoll_interval)
works without an extra connection. It is excluded from `repr` and
`__eq__` to keep the dataclass shape stable.

### `await job.wait_for_result(*, timeout, poll_interval)`

```python
async def wait_for_result(
    self,
    *,
    timeout: float = 30.0,
    poll_interval: float = 0.1,
) -> Any | None
```

Poll until the engine's stored result becomes readable, until
`timeout` elapses, or until the surrounding `asyncio.Task` is
cancelled. Returns the decoded handler return value on success.

- `timeout` — total seconds. **Default `30.0`.**
- `poll_interval` — seconds between polls. **Default `0.1`.**

**Raises** `asyncio.TimeoutError` on deadline. Raises `RuntimeError`
when called on a `Job` produced by a worker handler (no `_queue`
backreference). Raises `ValueError` for non-positive `timeout` or
`poll_interval`.

:::caution[The void-handler trap]
If the handler returned `None`, *or* the worker ran without
`store_results=True`, no result key is ever written. The polling
loop has no way to distinguish that case from "not yet completed",
so this method will time out. After 1s of polling with no key
present the method emits a one-shot `RuntimeWarning` to surface
the most common cause. For high-fanout workloads, subscribe to
[`QueueEvents`](#queueevents) instead.
:::

## QueueEvents

```python
class QueueEvents:
    def __init__(
        self,
        queue_name: str,
        *,
        redis_url: str = "redis://127.0.0.1:6379",
        tls: bool = False,
        last_event_id: str = "$",
        block_ms: int = 5_000,
        count: int = 100,
    ) -> None: ...
```

Cross-process subscriber to the engine's events stream
(`{chasqui:<queue>}:events`). Built on `redis.asyncio` because the
events stream is a generic Redis Stream (ASCII fields, not
msgpack), so a thin async-redis client is the simplest path.

- `last_event_id` — start id. **Default `"$"`** (only new). Pass `"0"` to replay history.
- `block_ms` — `XREAD BLOCK` timeout. **Default `5000`.**
- `count` — `XREAD COUNT`. **Default `100`.**

### Async iteration

```python
events = QueueEvents("emails")
async for ev in events:
    print(ev.name, ev.job_id, ev.fields)
```

Yields [`QueueEvent`](#queueevent) values. Iteration ends when
[`close()`](#await-queueeventsclose) is called.

### `await queue_events.close()`

```python
async def close(self) -> None
```

Stop iteration and release the Redis connection.

## QueueEvent

```python
@dataclass(frozen=True)
class QueueEvent:
    name: str
    job_id: str | None
    job_name: str
    fields: dict[str, Any]
```

One event from the engine.

- `name` — event identifier (`"waiting"`, `"active"`, `"completed"`, `"failed"`, `"retry-scheduled"`, `"delayed"`, `"dlq"`, `"drained"`).
- `job_id` — `None` for queue-scoped events (`"drained"`).
- `job_name` — dispatch name from the `n` field. Empty when the engine omitted `n`.
- `fields` — remaining decoded fields verbatim. Numeric fields documented by the engine schema (`attempt`, `backoff_ms`, `delay_ms`, `duration_us`, `ts`) are coerced to `int` at parse time; a malformed entry whose value can't be parsed silently falls back to the raw string.

## RepeatPattern

```python
@dataclass(frozen=True)
class RepeatPattern:
    kind: str
    expression: str | None = None
    tz: str | None = None
    interval_ms: int | None = None
```

Recurring-job schedule descriptor. Build instances via the
factory methods so `kind` is set consistently.

### `RepeatPattern.cron(expression, *, tz=None)`

```python
@staticmethod
def cron(expression: str, *, tz: str | None = None) -> "RepeatPattern"
```

Cron-driven pattern. `expression` accepts both 5-field
(`m h dom mon dow`) and 6-field (with leading seconds) syntax.
`tz` may be `"UTC"` / `"Z"`, a fixed offset (`"+05:30"`), or any
IANA name (`"America/New_York"`). IANA names are DST-aware: a
2 AM cron fires at the local 02:00 wall-clock on both sides of
spring-forward / fall-back.

### `RepeatPattern.every(interval_ms)`

```python
@staticmethod
def every(interval_ms: int) -> "RepeatPattern"
```

Fixed-interval pattern. First fire lands one `interval_ms` after
upsert (no immediate fire). `interval_ms` must be positive — the
engine rejects zero.

### `pattern.to_dict()`

Serializes to the wire-format dict the native producer accepts.

## BackoffSpec

```python
@dataclass(frozen=True)
class BackoffSpec:
    kind: str
    delay_ms: int
    max_delay_ms: int | None = None
    multiplier: float | None = None
    jitter_ms: int | None = None
```

Per-job retry backoff override. Pair with `attempts` on
[`Queue.add`](#await-queueaddname-data--delay_ms-delay-attempts-backoff-job_id-repeat-missed_fires)
to scope retry behavior to a single job.

### `BackoffSpec.fixed(delay_ms, jitter_ms=None)`

Fixed delay between retries.

### `BackoffSpec.exponential(initial_ms, *, multiplier=2.0, max_ms=None, jitter_ms=None)`

Exponential backoff. `multiplier` defaults to **`2.0`**.

```python
await queue.add("send", payload, attempts=5, backoff=BackoffSpec.exponential(1_000, max_ms=30_000))
```

## MissedFiresPolicy

```python
@dataclass(frozen=True)
class MissedFiresPolicy:
    kind: str
    max_catchup: int | None = None
```

Catch-up policy for windows missed during scheduler downtime.

### `MissedFiresPolicy.skip()`

Drop missed windows; resume on the first future fire. **Default.**
Safe; no thundering herd after a deploy / outage.

### `MissedFiresPolicy.fire_once()`

Emit one job to represent the missed window(s), then advance.

### `MissedFiresPolicy.fire_all(max_catchup)`

Replay each missed window up to `max_catchup` fires. `max_catchup`
must be a positive integer (`>= 1`); `0` is rejected because the
engine's loop short-circuits there, making it wire-distinct but
semantically equivalent to `skip()`.

## RepeatableMeta

```python
@dataclass(frozen=True)
class RepeatableMeta:
    key: str
    job_name: str
    pattern: RepeatPattern
    next_fire_ms: int
    limit: int | None = None
    start_after_ms: int | None = None
    end_before_ms: int | None = None
    missed_fires: MissedFiresPolicy | None = None
```

Wire-compatible projection returned by
[`Queue.get_repeatable_jobs`](#await-queueget_repeatable_jobslimit100).
Carries no payload — listing thousands of specs stays cheap.

## Type aliases

### `DelayMsLike`

```python
DelayMsLike = Union[int, float, datetime]
```

Anything `Queue.add` accepts as `delay_ms`:

- `int` — milliseconds (BullMQ-compatible).
- `float` — milliseconds; fractional accepted; truncated to `int`.
- `datetime.datetime` — absolute fire time. Naive datetimes are treated as UTC.

For seconds-friendly relative durations, pass a `timedelta` via the
parallel `delay` kwarg instead.

### `BackoffLike`

```python
BackoffLike = Union[int, BackoffSpec, dict]
```

- `int` — fixed delay in ms.
- `BackoffSpec` — typed builder.
- `dict` — raw native shape (advanced; bypasses validation).

### `RepeatLike`

```python
RepeatLike = Union[RepeatPattern, dict]
```

### `MissedFiresLike`

```python
MissedFiresLike = Union[MissedFiresPolicy, dict]
```

Raw dict shape: `{"kind": "skip" | "fire-once" | "fire-all", "max_catchup"?: int}`.

### `Handler`

```python
Handler = Callable[[Job], Awaitable[Any]]
```

The signature `Worker` accepts.

## Errors

The shim raises typed exceptions:

- [`UnrecoverableError`](/reference/error-codes/#cmq-011--handler-signaled-unrecoverable--dlq) — raise from a handler to skip retries and route to the DLQ. Subclassing is supported via MRO-aware `issubclass` check on the native side.
- [`NotSupportedError`](/reference/error-codes/#cmq-200--python-feature-not-supported) — caller asked for a v1-stubbed feature.

```python
from chasquimq import UnrecoverableError

async def handler(job):
    if not job.data.get("user_id"):
        raise UnrecoverableError("missing user_id; no retry possible")
```

## Native power-user surface

The underscore-free re-exports `Producer`, `Consumer`, `Scheduler`
are the unwrapped PyO3 binding classes. Reach for them only when
you have a measured reason to bypass the high-level layer — most
callers will never need them. The signatures live in
`chasquimq-py/src/chasquimq/_native.pyi`.

```python
from chasquimq import Producer, Consumer, Scheduler, version

print(version())  # engine binding crate version
```

- `Producer(redis_url, queue_name, *, pool_size, max_stream_len, max_delay_secs)` — sync constructor.
- `Consumer(redis_url, queue_name, *, concurrency, max_attempts, group, ...)` — sync constructor with the full engine config surface.
- `Scheduler(redis_url, queue_name, *, tick_interval_ms, batch, max_stream_len, lock_ttl_secs, holder_id)` — sync constructor.

These map directly onto the engine's
[`ProducerConfig`](/reference/rust-api/#producerconfig) /
[`ConsumerConfig`](/reference/rust-api/#consumerconfig) /
[`SchedulerConfig`](/reference/rust-api/#schedulerconfig) — see
the Rust reference for the canonical field meanings.

:::caution
The internal `_native._Job` pyclass is intentionally
underscore-prefixed and is **not part of any public surface**.
The only `Job` type users should reference is
[`chasquimq.Job`](#job).
:::

### `version()`

```python
def version() -> str: ...
```

Returns the version of the binding crate.
