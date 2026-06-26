# chasquimq (Python)

Python bindings for [ChasquiMQ](https://github.com/jotarios/chasquimq) — the fastest open-source message broker for Redis. The Rust engine pulls jobs; Python `asyncio` handlers process them.

> **Status:** 1.0. abi3 wheels for Python 3.9+ on Linux (x86_64 + aarch64), macOS (x86_64 + aarch64), Windows (x86_64).

## Install

```bash
pip install chasquimq
```

## Quickstart

```python
import asyncio
from chasquimq import Queue, Worker, Job, BackoffSpec, UnrecoverableError


async def send_email(job: Job) -> dict:
    to = job.data["to"]
    print(f"sending to {to} (attempt {job.attempts_made + 1})")
    if "@unrecoverable" in to:
        raise UnrecoverableError(f"hard bounce: {to}")
    return {"sent_at": time.time(), "to": to}


async def main() -> None:
    async with Queue("emails") as queue, \
               Worker("emails", send_email, store_results=True) as worker:

        # Plain enqueue.
        await queue.add("welcome", {"to": "ada@example.com"})

        # Stable jobId — second call with the same id is a no-op (idempotent).
        await queue.add_unique(
            "welcome", {"to": "alice@example.com"},
            job_id="welcome:alice",
        )

        # Per-job retry with exponential backoff.
        await queue.add(
            "welcome", {"to": "grace@flaky.example"},
            attempts=3,
            backoff=BackoffSpec.exponential(100, multiplier=2.0, max_ms=10_000),
        )

        # Delayed enqueue (in milliseconds; for `timedelta` use delay= instead).
        await queue.add("welcome", {"to": "ka@later.example"}, delay_ms=2_000)

        # Block on a single job's result, with timeout.
        job = await queue.add("welcome", {"to": "ada@example.com"})
        result = await job.wait_for_result(timeout=30.0)
        print(result)

        # Drain the worker.
        await worker.run()


asyncio.run(main())
```

## What's in the box

| Surface | What it does |
|---|---|
| `Queue` | Producer + queue inspection. `add` / `add_bulk` / `add_unique` / `get_job` / `get_jobs` / `get_jobs_page` / `get_job_state` / `get_job_counts` / `get_job_logs` / `get_waiting_count` / `get_active_count` / `get_delayed_count` / `get_completed_count` / `get_failed_count` / `count` / `get_job_result` / `peek_dlq` / `replay_dlq` / `cancel_delayed` / `get_repeatable_jobs` / `remove_repeatable_by_key` / `pause` / `resume` / `is_paused` / `remove` / `remove_report` / `drain` / `clean` / `obliterate`. Async context manager. |
| `Worker` | Consumer pool. asyncio-first dispatch, opt-in result storage (`store_results=True`), graceful shutdown, `pause` / `resume` / `is_paused`, listener API (`on`/`off`/`once` for `ready` / `active` / `completed` / `failed` / `error` / `closing` / `closed` / `drained` / `paused` / `resumed` / `progress` / `stalled`). Async context manager. |
| `Job` | Dataclass returned by `Queue.add`. Has `id`, `name`, `data`, `attempts_made`, `progress`, `stalled_count`, `update_progress(n)` (Worker-side only), `log(line)` (Worker-side only), `wait_for_result(timeout=)`, `wait_until_finished(queue_events, timeout=)`. |
| `QueueEvents` | Async-iterator + listener API over the engine events stream. Cross-process pub/sub for `waiting` / `active` / `completed` / `failed` / `dlq` / `retry-scheduled` / `delayed` / `drained` / `stalled` / `retries-exhausted`, plus per-id channels (`completed:<jobId>` / `failed:<jobId>` / `active:<jobId>` / `stalled:<jobId>`) for targeted subscribers. |
| `BackoffSpec` | Builders: `.fixed(delay_ms)` / `.exponential(initial_ms, multiplier, max_ms, jitter_ms)`. |
| `RepeatPattern` | Builders: `.cron(expr, tz=)` / `.every(interval_ms)`. DST-aware via IANA tz names. |
| `MissedFiresPolicy` | `.skip()` / `.fire_once()` / `.fire_all(max_catchup)` for cron catch-up after scheduler downtime. |
| `UnrecoverableError` | Raise from your handler to bypass retries and route the job directly to DLQ. |

### TLS / `rediss://`

For TLS-fronted Redis (ElastiCache encryption-in-transit, or any non-cluster Redis with TLS), set `tls=True` on `Queue` / `Worker` / `QueueEvents`, or pass a `rediss://` URL directly:

```python
async with Queue("emails", redis_url="redis://my-cluster.cache.amazonaws.com:6379", tls=True) as queue:
    ...
# or:
async with Queue("emails", redis_url="rediss://my-cluster.cache.amazonaws.com:6379") as queue:
    ...
```

Trust roots come from the platform store via `rustls-native-certs`: keychain on macOS, the OS CA bundle on Linux (probed by `openssl-probe`), system store on Windows — so AWS Trust CA-signed endpoints work out of the box. For private CAs, point `SSL_CERT_FILE` at a PEM bundle before launching Python; that env var takes precedence over the platform store.

### Redis Cluster

For a multi-shard Redis Cluster (ElastiCache Cluster mode enabled, self-hosted Redis Cluster), pass a `redis-cluster://` URL as `redis_url`. Layer TLS with `tls=True` (the cluster scheme is preserved, becoming `rediss-cluster://`) or pass `rediss-cluster://` directly:

```python
async with Queue("emails", redis_url="redis-cluster://seed.cache.amazonaws.com:6379") as queue:
    ...
# TLS + cluster (tls=True keeps the -cluster scheme):
async with Queue("emails", redis_url="redis-cluster://seed.cache.amazonaws.com:6379", tls=True) as queue:
    ...
# or an explicit URL (extra seeds via ?node=):
async with Queue("emails", redis_url="rediss-cluster://seed:6379?node=seed2:6379") as queue:
    ...
```

One seed node is enough — the rest of the topology is discovered automatically, and `MOVED`/`ASK` redirections plus failover are handled for you. Every key for a queue shares a `{chasqui:<queue>}` hash tag, so the queue's whole keyspace (stream, delayed, DLQ, results, locks, events) lives on a single slot and the engine's atomic operations stay correct. A queue is single-slot by design; cross-queue atomic operations are not supported on a cluster (they are not supported on single-node Redis either). `Worker` and `QueueEvents` take the same `redis_url`, so point all three at the same `*-cluster://` seed.

### Rotating IAM tokens / `credential_provider`

For Redis deployments that use short-lived auth tokens — most notably **AWS ElastiCache IAM auth**, where tokens expire roughly every 15 minutes — pass an async `credential_provider` callback. The engine calls it before every `AUTH` / `HELLO` command (initial connect and every reconnect), so a long-lived `Queue` / `Worker` stays authenticated through token rotation without rebuilding.

```python
from typing import Optional, Tuple

import aioboto3  # or your preferred async AWS SDK

from chasquimq import Queue, Worker


async def elasticache_credentials(
    host: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    """Called by the engine before every AUTH/HELLO.

    ``host`` is the target server as ``"hostname:port"`` (or ``None`` when
    fred has no specific endpoint to report — e.g. cluster bootstrap).
    Returns ``(username, password)``; either side may be ``None``.
    """
    session = aioboto3.Session()
    async with session.client("elasticache") as ec:
        token = await ec.generate_iam_auth_token(...)
    return ("my-iam-user", token)


async with Queue(
    "emails",
    redis_url="rediss://my-cluster.cache.amazonaws.com:6379",
    credential_provider=elasticache_credentials,
) as queue, Worker(
    "emails",
    send_email,
    redis_url="rediss://my-cluster.cache.amazonaws.com:6379",
    credential_provider=elasticache_credentials,
) as worker:
    ...
```

Notes:

- **Construction is deferred when a `credential_provider` is supplied.** The callback dispatches back to the asyncio loop that constructed the `Queue` / `Worker`, so the engine waits until the first awaited method (`queue.add`, `worker.run`, ...) to open the pool — that's the moment a running loop is guaranteed.
- **Auth errors trigger reconnect.** The engine's default `reconnect_on_auth_error = true` means a token-fetch failure is retried on the next AUTH, with exponential backoff. Raise from your callback (or return stale credentials) and the next reconnect picks up a fresh token. By default a permanently broken provider retry-loops inside fred forever; bound it with [`reconnect_max_attempts`](#bounding-reconnect-attempts) so it gives up after N attempts instead.
- **Same callback for both `Queue` and `Worker`.** Pass the same async function to each — the native producer and consumer each capture their own asyncio-loop reference internally.

### Bounding reconnect attempts

By default the engine reconnects forever (`reconnect_max_attempts=0`). That's the right behaviour for a transient network blip, but a permanently rejecting `credential_provider` — a revoked IAM user, an expired role — will loop on reconnect indefinitely instead of surfacing the failure. Cap it with the `reconnect_max_attempts` keyword:

```python
async with Queue(
    "emails",
    redis_url="rediss://my-cluster.cache.amazonaws.com:6379",
    credential_provider=elasticache_credentials,
    # Give up after 10 failed reconnects instead of looping forever.
    reconnect_max_attempts=10,
) as queue, Worker(
    "emails",
    send_email,
    redis_url="rediss://my-cluster.cache.amazonaws.com:6379",
    credential_provider=elasticache_credentials,
    reconnect_max_attempts=10,
) as worker:
    ...
```

Available on both `Queue` (producer pool) and `Worker` (consumer). `0` or `None` (the default) keeps the unbounded behaviour. Pair a positive cap with alerting on reconnect churn so a bounded failure is loud, not silent.

### Capping payload size

The producer rejects any job whose encoded (MessagePack) payload exceeds `max_payload_bytes` with an error, *before* it ever reaches Redis — the produce-side mirror of the consumer's oversize-on-read cap (which routes too-big entries to the DLQ). Both default to **1 MiB**; set both to the same value for symmetric produce/consume semantics:

```python
async with Queue(
    "emails",
    # Reject any add* / repeatable upsert over 256 KiB before the write.
    max_payload_bytes=256 * 1024,
) as queue:
    ...
```

`None` (the default) keeps the engine default of 1 MiB. An oversize job in a bulk call fails the whole call atomically with no partial write.

### Pausing and resuming

Two levels of pause, both consumer-side: workers stop pulling new jobs, jobs already in flight finish, producers keep enqueueing.

`Worker.pause()` is **process-local** — it stops just that worker instance. Resume is instant. (These are synchronous calls — no `await`.)

```python
worker = Worker("emails", handler, redis_url=url)
task = asyncio.create_task(worker.run())

worker.pause()        # this worker stops dispatching new jobs
# ...in-flight handlers still finish; queue.add() still works...
worker.resume()       # back to processing
worker.is_paused()    # => False
```

`Queue.pause()` is **durable and cross-process** — it sets a Redis flag every consumer of the queue honours, and it survives worker restarts until you `resume()`. Use it for queue-wide maintenance (a worker started while paused comes up paused).

```python
async with Queue("emails", redis_url=url) as queue:
    await queue.pause()           # every worker of "emails" parks
    assert await queue.is_paused()
    await queue.resume()          # lift it everywhere
```

The same durable flag is what the CLI's `chasqui pause <queue>` / `chasqui resume <queue>` toggle. Both surfaces are idempotent — double-pause / double-resume are no-ops.

### Subscribing to events

Two layered surfaces: per-worker listeners for in-process observation, and the cross-process `QueueEvents` stream for fan-out across workers, dashboards, and ops tooling. Listener callbacks may be plain functions or `async def` coroutines — async callbacks are scheduled on the running loop.

In-process `Worker` events fire on the local worker only:

```python
async with Worker("emails", handler, redis_url=url) as worker:
    worker.on("ready",     lambda: print("engine loop started"))
    worker.on("active",    lambda job: print("running", job.id))
    worker.on("completed", lambda job, result: print("ok", job.id, result))
    worker.on("failed",    lambda job, err: print("failed", job.id, err))
    worker.on("error",     lambda err: print("engine error", err))
    worker.on("drained",   lambda: print("queue is empty"))
    worker.on("paused",    lambda: print("worker parked"))
    worker.on("resumed",   lambda: print("worker resumed"))
    worker.on("closing",   lambda: print("shutting down"))
    worker.on("closed",    lambda: print("shutdown complete"))
    await worker.run()
```

`drained` is the only `Worker` event that requires a cross-process subscription (the engine emits it on the events stream, since "no jobs left" is a queue-wide observation). It's lazily wired: the first `worker.on('drained', ...)` call spawns an embedded `QueueEvents` subscriber, torn down on `worker.close()`. Workers that never subscribe pay no extra Redis connections.

Cross-process `QueueEvents` listens on the events stream — every process running a `QueueEvents` instance for the same queue sees every transition, regardless of which worker emitted it. Two surfaces are available; pick one (they share the same Redis connection but only one consumer of XREAD at a time):

```python
from chasquimq import QueueEvents

# Listener API (EventEmitter-shaped)
events = QueueEvents("emails", redis_url=url)
events.on("waiting",          lambda payload, eid: ...)
events.on("active",           lambda payload, eid: ...)
events.on("completed",        lambda payload, eid: ...)
events.on("failed",           lambda payload, eid: ...)  # payload['failedReason']
events.on("drained",          lambda eid: ...)
events.on("retry-scheduled",  lambda payload, eid: ...)
events.on("dlq",              lambda payload, eid: ...)
events.on("retries-exhausted", lambda payload, eid: ...)
# Per-id channels — fire only for the named job. Used internally by
# Job.wait_until_finished; useful for targeted UI updates without
# filtering every broadcast event by jobId.
events.on(f"completed:{job_id}", lambda payload, eid: ...)
events.on(f"failed:{job_id}",    lambda payload, eid: ...)
await events.wait_until_ready()   # subscriber's first XREAD BLOCK is in flight
# ... do work ...
await events.close()

# Async-iterator surface (the original; one consumer)
events = QueueEvents("emails", redis_url=url)
async for ev in events:
    print(ev.name, ev.job_id, ev.fields)
```

`completed` payloads from the events stream do **not** carry the handler's return value (that would double-allocate the payload onto every subscriber). To read the value, pair `QueueEvents` with `Queue.get_job_result(job_id)` and run the worker with `store_results=True`. The next section's `Job.wait_until_finished` does this for you.

`progress` fires every time a processor calls `await job.update_progress(n)` — the worker lazily spawns an embedded `QueueEvents` subscriber on the first `progress` listener (same zero-cost-when-unused pattern as `drained`) and re-emits the cross-process `e=progress` event as `(job, n)`. See the [Progress and logs](#progress-and-logs) section below. `stalled` is accepted on `Worker` for parity but is currently no-op — the engine doesn't emit a stalled-detector transition yet; attach it safely, it'll start firing when the corresponding engine work lands.

### Awaiting a single job's completion

`Job.wait_until_finished(queue_events, timeout=...)` is the event-driven completion-wait, mirroring the Node shim. Subscribes to the per-id `completed:<job_id>` / `failed:<job_id>` channels and resolves / raises on the first to fire:

```python
events = QueueEvents("emails", redis_url=url)
# Force the subscriber loop to start so wait_until_ready is awaitable.
events.on("completed", lambda *_a: None)
await events.wait_until_ready()

job = await queue.add("send", {"to": "ada@example.com"})

try:
    # Returns the handler's return value (requires store_results=True
    # on the worker; otherwise returns None). Raises
    # RuntimeError(failedReason) on failure.
    # `timeout` in seconds — omit for an unbounded wait.
    result = await job.wait_until_finished(events, timeout=30.0)
    print("sent:", result)
except WaitUntilFinishedTimeoutError:
    print("no terminal event within deadline")
except RuntimeError as err:
    print("job failed:", err)
finally:
    await events.close()
```

Distinct from `Job.wait_for_result(timeout=...)`: that one polls the `Queue.get_job_result` Redis key and **requires** `store_results=True` to detect completion at all. `wait_until_finished` is event-driven, so it detects completion even when no result key was written — but it cannot tell you a job that already finished before the call wired up. Pick `wait_until_finished` for low-latency awaits of jobs you're about to enqueue; pick `wait_for_result` for "did this id ever finish".

### Inspect jobs

Read-only introspection across the queue's stream, delayed ZSET, DLQ, and result-key surfaces. Bounded scans; no secondary index; zero impact on the producer / consumer hot paths.

```python
async with Queue("emails", redis_url=url) as queue:
    counts = await queue.get_job_counts()
    # => {"waiting": ..., "active": ..., "delayed": ..., "completed": ..., "failed": ..., "paused": ...}

    state = await queue.get_job_state("job-123")
    # => "waiting" | "active" | "delayed" | "completed" | "failed" | "unknown"

    job = await queue.get_job("job-123")
    # => Job | None  (data is msgpack-decoded; engine state on attempt / name / created_at_ms)

    page = await queue.get_jobs("waiting", offset=0, limit=50)
    # => list[Job]

    # Multi-page sweeps with cursor:
    jobs, cursor = await queue.get_jobs_page("delayed", limit=100)
    while cursor is not None:
        more, cursor = await queue.get_jobs_page("delayed", limit=100, cursor=cursor)
        jobs.extend(more)
```

State resolution is **live-state-first**: pending (PEL) → delayed → waiting → DLQ → result. A job that's been replayed from DLQ resolves as `"waiting"`, not `"completed"`, during the race window. A missing id resolves to `"unknown"` and `get_job` returns `None`.

Counts are cheap (~5 Redis round trips, one per state column). `completed` runs a bounded `SCAN` over the `result:*` keyspace under the per-queue hash tag — large keyspaces return a lower-bound figure (the cap is configurable via `CHASQUIMQ_COMPLETED_SCAN_CAP` on the engine, default 10,000).

`Queue(consumer_group=...)` (optional, default `"default"`) configures which consumer group's PEL is read for the `active` column. Set it on the `Queue` if your `Worker` uses a non-default `group`.

```python
queue = Queue("emails", redis_url=url, consumer_group="primary")
```

### Maintenance

Tear individual jobs — or a whole queue — down. All four methods are off the hot path; every scan is bounded so a single call never blocks Redis.

```python
async with Queue("emails", redis_url=url) as queue:
    # Remove one job from everywhere it lives — delayed stage, a waiting
    # or in-flight stream entry, the DLQ, and the stored result.
    surfaces = await queue.remove("job-123")
    # => number of surfaces the job was removed from (0 if not found)

    report = await queue.remove_report("job-123")
    # => {"delayed": ..., "stream": ..., "dlq": ..., "result": ...}

    # Clear every waiting job. In-flight jobs keep running. Delayed jobs
    # go too unless you pass delayed=False.
    drained = await queue.drain()           # => count removed
    await queue.drain(delayed=False)        # keep scheduled future jobs

    # Age- + state-filtered bulk delete. Removes up to `limit` jobs in
    # the given state older than `grace_ms`; returns the removed ids.
    ids = await queue.clean(60_000, 1000, "completed")
    #     clean(grace_ms, limit, "completed" | "failed" | "delayed" | "waiting")

    # Nuke the entire queue keyspace. Returns the key count removed.
    await queue.obliterate()
```

`remove` is idempotent — a job id that exists on no surface resolves without error (count `0`). `clean` ignores `grace_ms` for `"completed"` (a stored result has no creation timestamp; rely on result TTL for time-based expiry). `clean` with `"active"` is a no-op — use `remove` for the deliberate per-job case. `obliterate` cannot be undone.

### Progress and logs

In-handler write surface for live job state. The engine persists progress to a side-channel Redis key (`{chasqui:<queue>}:progress:<id>` STRING, TTL = `result_ttl_secs`) and appends log lines to a per-job stream (`{chasqui:<queue>}:log:<id>` STREAM with `MAXLEN ~`, TTL = `result_ttl_secs`). The msgpack `Job` envelope is untouched — wire-format-stable.

```python
async def handler(job: Job) -> dict:
    await job.update_progress(10)
    await job.log("connecting to SMTP")

    # ... work ...
    await job.update_progress(50)
    await job.log("envelope sent")

    # ... work ...
    await job.update_progress(100)
    return {"delivered": True}

worker = Worker("emails", handler, redis_url=url)
```

Read the latest progress (no extra round trip — pipelined into the existing introspector lookup) and the log lines from anywhere:

```python
async with Queue("emails", redis_url=url) as queue:
    job = await queue.get_job("job-123")
    print(job.progress)  # => 50

    lines, count = await queue.get_job_logs("job-123")
    # lines: list[str]   (captured `line` field values, asc order)
    # count: int         (current XLEN — not len(lines) — for paginating callers)

    # Page from the tail; matches BullMQ's getLogs(-100, -1) convention.
    tail, _ = await queue.get_job_logs("job-123", start=-100, end=-1)
```

Tuning (all `Worker` keyword args):

| Option | Default | Controls |
|---|---:|---|
| `log_max_stream_len` | `1000` | `MAXLEN ~` cap on each per-job log stream (must be `>= 16`). |
| `log_max_line_bytes` | `4096` | Per-line byte cap; oversize lines truncate on a UTF-8 char boundary with a `[…truncated]` marker. |
| `events_progress_enabled` | `True` | When `False`, mutes the `e=progress` events-stream entry — the persisted progress key is still written. Useful for high-rate progress reporters that don't need cross-process fan-out. |

Subscribe to the cross-process `progress` event via `QueueEvents` (broadcast `progress`, per-id `progress:<job_id>`) or the in-process `worker.on('progress', ...)` re-fan. **Read-only Job guard:** calling `update_progress` / `log` on a Job returned by `Queue.get_job` / `Queue.add` raises `RuntimeError("Job.update_progress() requires the Job be passed to your worker handler; Jobs returned by Queue.get_job() are read-only")` — only Jobs handed to a `Worker` processor carry the live native handle.

### Stalled-job detection

The CLAIM-on-read safety net recovers a single mid-handler crash, but it can't bound a *loop* of worker crashes against the same entry. Each redelivery resets the idle clock, `delivery_count` rises forever, and the only terminal route is DLQ-as-`retries_exhausted` — which conflates handler-failure loops with worker-crash loops. The stalled-job detector is the active sibling: a leader-elected background task spawned alongside the scheduler that scans the PEL on a tick, INCRs a per-job stall counter for entries idle past the threshold, and atomically relocates them to the DLQ as a distinct `stalled` reason once the counter reaches `max_stalled_attempts`.

```python
import asyncio
from chasquimq.worker import Worker
from chasquimq.queue_events import QueueEvents


async def handler(job):
    ...

worker = Worker(
    "emails",
    handler,
    redis_url="redis://127.0.0.1:6379",
    # Stall ceiling — default `2` (one extra tick of headroom over
    # BullMQ's `maxStalledCount: 1` to avoid racing CLAIM-on-read).
    max_stalled_attempts=2,
    # Total handler attempts before DLQ-as-retries_exhausted.
    max_attempts=25,
)


# Cross-process scope — every worker on this queue receives the event,
# not just the one holding the entry. `prev` is always `'active'`.
def on_stalled(job_id: str, prev: str) -> None:
    print(f"stalled: {job_id} (was {prev})")


worker.on("stalled", on_stalled)


# Or subscribe to the cross-process `e=stalled` event directly:
events = QueueEvents("emails", redis_url="redis://127.0.0.1:6379")


def on_stalled_event(payload: dict, _event_id: str) -> None:
    # payload['attempt'] = current stall count (1-indexed)
    print(payload)


events.on("stalled", on_stalled_event)
```

| Option | Default | Controls |
|---|---:|---|
| `max_stalled_attempts` | `2` | Stall cycles past `idle_threshold_ms` before DLQ-as-`stalled`. One extra tick of headroom over BullMQ's `maxStalledCount: 1` to avoid racing the reader's CLAIM-on-read recovery path. |
| `max_attempts` | `25` | Total handler attempts (initial + retries) before DLQ-as-`retries_exhausted`. Per-job override via `Queue.add(name, data, attempts=N)`. |
| `stalled_detector_enabled` | `True` | Toggle the embedded detector. Set `False` for pure-consumer benchmarks or deployments running a separate detector process. |
| `stalled_interval_ms` | `30_000` | Scan-tick interval (ms). The embedded spawn overrides this from the engine's `claim_min_idle_ms` to preserve the per-crash counting invariant (`tick == idle == claim_min_idle`); rarely worth setting. |

### Repeatable jobs

`upsert_repeatable_job` upserts a recurring *spec* — `(name, payload, pattern)`. The engine fires a fresh job from it on each window. The return value's `id` is the resolved spec key; pair it with `remove_repeatable_by_key` to delete.

```python
from chasquimq import BackoffSpec, RepeatPattern

# Cron (DST-aware via IANA tz), or RepeatPattern.every(ms) for a fixed interval.
spec = await queue.upsert_repeatable_job(
    "nightly-sync",
    {"source": "crm"},
    repeat=RepeatPattern.cron("0 3 * * *", tz="UTC"),
)

await queue.remove_repeatable_by_key(spec.id)
```

**Per-fire retry overrides.** `attempts` and `backoff` on the same call set a per-fire override: every job the scheduler mints from the spec carries it, so its `max_attempts` / `backoff` win over the worker's queue-wide config — exactly like `attempts` / `backoff` on a one-off `Queue.add`. Omit them to inherit the queue-wide retry policy.

```python
await queue.upsert_repeatable_job(
    "nightly-sync",
    {"source": "crm"},
    repeat=RepeatPattern.cron("0 3 * * *", tz="UTC"),
    # Every fire gets at most 3 attempts, regardless of the worker's
    # max_attempts.
    attempts=3,
    backoff=BackoffSpec.exponential(1_000, max_ms=30_000),
)
```

> **`job_id` is not supported on a repeatable upsert** — passing `job_id` raises `NotSupportedError`. The scheduler mints a fresh id per fire, so a caller-pinned id would silently not stick; use the resolved spec key (the returned `spec.id`) as the stable handle. Stable-id-per-fire is a tracked follow-up.

## Power-user surface

The native engine handles ship from the same top-level package:

```python
from chasquimq import Producer, Consumer, Scheduler
```

There is one user-facing `Job` — the high-level dataclass returned by `Queue.add` and passed to your `Worker` handler. The native binding's wire-format pyclass is internal-only (`chasquimq._native._Job`) and not re-exported (mirrors the Node shim).

## Build from source

```bash
cd chasquimq-py
python -m venv .venv && source .venv/bin/activate
pip install maturin
maturin develop          # editable install
pytest tests/            # smoke + integration tests (requires Redis 8.6+)
maturin build --release  # wheels under target/wheels/
```

## See also

- [Main repo README](https://github.com/jotarios/chasquimq#readme) — pitch, headline numbers, feature comparison
- [Engine internals](https://github.com/jotarios/chasquimq/blob/main/docs/engine.md) — retry semantics, delayed jobs, result backends, observability
- [Phase 4 design doc](https://github.com/jotarios/chasquimq/blob/main/docs/phase4-pyo3-design.md) — the PyO3 binding architecture

## License

MIT — see [LICENSE](https://github.com/jotarios/chasquimq/blob/main/LICENSE) at the workspace root.
