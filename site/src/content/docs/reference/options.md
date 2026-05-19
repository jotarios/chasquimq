---
title: Options index
description: Cross-language cheat sheet for every tunable option — Node, Python, and Rust names side by side, grouped by what they control.
sidebar:
  order: 6
---

A consolidated index of every tunable option across all three
public surfaces. The page you reach for when you're tuning
concurrency, retry budgets, payload size, or DLQ depth and need
to know what the field is called in each language.

For full prose on each field, follow the link to the canonical
reference. Every default below is the value applied when the
option is omitted; **bold** is the default in the cell where it
exists.

## Connection

| Option | Node | Python | Rust | Controls |
|---|---|---|---|---|
| Redis URL or host:port | `connection.host`, `connection.port`, `connection.password`, `connection.username`, `connection.db`, `connection.url` | `redis_url` | `redis_url` (passed to `connect` / `Consumer::new`) | Redis endpoint. Node default `127.0.0.1:6379`; Python and Rust default `redis://127.0.0.1:6379`. Pass `connection.url` on Node, or a full string on Python/Rust, to bypass the host/port builder. |
| TLS (`rediss://`) | `connection.tls: boolean` (**false**) | `Queue(tls=False)`, `Worker(tls=False)`, `QueueEvents(tls=False)` | scheme of `redis_url` | When `true`, the shim upgrades a `redis://` URL or schemeless host:port to `rediss://`; otherwise pass the URL directly. The engine negotiates TLS via fred's `enable-rustls-ring` feature; trust roots come from `rustls-native-certs`, with `SSL_CERT_FILE` taking precedence for private CAs. |
| Redis Cluster mode | `connection.cluster: boolean` (**false**) or a `redis-cluster://` URL | `redis-cluster://` / `rediss-cluster://` `redis_url` | scheme of `redis_url` (`redis-cluster://`, `rediss-cluster://`, `valkey-cluster://`, `valkeys-cluster://`) | Connect to a multi-shard Redis Cluster. The host/port is one seed; the rest of the topology is discovered via `CLUSTER SLOTS`. Extra seeds via `?node=host:port`. On Node, `cluster: true` composes with `tls: true` (→ `rediss-cluster://`); an explicit `url` wins. Python is URL-driven: `tls=True` preserves the `-cluster` scheme. A queue's whole keyspace shares a `{chasqui:<queue>}` hash tag → single slot per queue; cross-queue atomic ops are unsupported on a cluster. See [Redis Cluster](/concepts/redis-cluster/). |
| TCP keepalive interval (s) | (engine default) | (engine default) | `ConnectionTuning::tcp_keepalive_secs` (**60**) | Idle interval before TCP keepalive probes start. Set to `0` to disable. Matters for environments that drop idle TCP silently (AWS NAT 350s cutoff). |
| TCP keepalive probe spacing (s) | (engine default) | (engine default) | `ConnectionTuning::tcp_keepalive_interval_secs` (**10**) | Spacing between probes after the first one fires. |
| Reconnect attempts | `connection.reconnectMaxAttempts` (**0** = unbounded) | `reconnect_max_attempts=` (**0/None** = unbounded) | `ConnectionTuning::reconnect_max_attempts` (**0** = unbounded) | Max reconnect attempts on transient failure. `0` = retry forever with exponential backoff. Set a positive value to bound a permanently rejecting credential provider instead of letting it loop forever on reconnect. |
| Reconnect min delay (ms) | (engine default) | (engine default) | `ConnectionTuning::reconnect_min_delay_ms` (**100**) | First reconnect delay. |
| Reconnect max delay (ms) | (engine default) | (engine default) | `ConnectionTuning::reconnect_max_delay_ms` (**30_000**) | Cap on the exponential reconnect backoff. |
| Reconnect backoff base | (engine default) | (engine default) | `ConnectionTuning::reconnect_backoff_base` (**2**) | Exponential growth factor. |
| Reconnect jitter (ms) | (engine default) | (engine default) | `ConnectionTuning::reconnect_jitter_ms` (**50**) | ±jitter on each reconnect; decorrelates fleets. |
| Connection timeout (ms) | (engine default) | (engine default) | `ConnectionTuning::connection_timeout_ms` (**10_000**) | Per-attempt deadline for TCP+TLS+AUTH handshake. |
| Rotating-token credential provider | `connection.credentialProvider` (`(host) => Promise<{ username?, password? }>`) | `credential_provider=` (`async (host) -> (username, password)`) | `ConnectionTuning::credential_provider: Option<Arc<dyn CredentialProvider>>` (**`None`**) | fred-side hook called before every `AUTH` / `HELLO` (initial connect + every reconnect). Use for ElastiCache IAM auth (15-min token rotation); paired with the `reconnect_on_auth_error` default a long-lived pool survives rotation without rebuilding. Bound a permanently rejecting callback with the **Reconnect attempts** row above (`connection.reconnectMaxAttempts` / `reconnect_max_attempts=`); the default `0` keeps the unbounded behaviour. |
| Producer pool size | (managed by binding) | (managed by binding) | `ProducerConfig::pool_size` (**8**) | Number of connections in the producer pool. |
| Cluster prefix | `connection.prefix` (no-op) | n/a | n/a | ChasquiMQ uses `{chasqui:<queue>}` Cluster hash tags; there is no tunable prefix. |

## Concurrency

| Option | Node | Python | Rust | Controls |
|---|---|---|---|---|
| In-flight handlers per worker | `WorkerOptions.concurrency` (**100**) | `Worker(concurrency=100)` | `ConsumerConfig::concurrency` (**100**) | Max parallel handler invocations. |
| `XREADGROUP COUNT` | `WorkerOptions.drainDelay` indirectly via `block_ms`; explicit `read_count` exposed only on the native `Consumer` | `Worker(read_count=...)` | `ConsumerConfig::batch` (**64**) | Stream entries fetched per read. |
| `XREADGROUP BLOCK` (ms) | `WorkerOptions.drainDelay` (**5000**) | `Worker(read_block_ms=...)` (default engine **5000**) | `ConsumerConfig::block_ms` (**5000**) | How long the reader blocks waiting for new entries. Higher reduces idle CPU; lower shortens shutdown drain. |
| CLAIM-recovery threshold (ms) | (native `ConsumerOpts.claimMinIdleMs`) | `Worker(claim_min_idle_ms=...)` (default engine **30_000**) | `ConsumerConfig::claim_min_idle_ms` (**30_000**) | Idle time after which `XREADGROUP ... CLAIM` reclaims an in-flight entry. |
| Ack flush batch | (engine default) | (engine default) | `ConsumerConfig::ack_batch` (**256**) | Pipelined `XACK` size. |
| Ack flush idle (ms) | (engine default) | (engine default) | `ConsumerConfig::ack_idle_ms` (**5**) | Max time to wait before flushing a partial ack batch. |
| Shutdown drain deadline (s) | (engine default) | (engine default) | `ConsumerConfig::shutdown_deadline_secs` (**30**) | Max time the engine waits for in-flight handlers on shutdown. |

## Retries and backoff

| Option | Node | Python | Rust | Controls |
|---|---|---|---|---|
| Total attempts (queue-wide) | `WorkerOptions.maxStalledCount` (**3**) | `Worker(max_attempts=25)` | `ConsumerConfig::max_attempts` (**3**) | Total attempts per job before DLQ. |
| Total attempts (per job) | `JobsOptions.attempts` | `Queue.add(attempts=...)` | `JobRetryOverride::max_attempts` | Per-job override of the queue-wide value. |
| Initial backoff (ms) | (set via `BackoffSpec.exponential(initialMs)`) | `BackoffSpec.exponential(initial_ms)` | `RetryConfig::initial_backoff_ms` (**100**) | Base delay for the first retry. |
| Max backoff (ms) | `BackoffOptions.maxDelay` | `BackoffSpec(max_delay_ms=...)` | `RetryConfig::max_backoff_ms` (**30_000**) | Cap on the computed backoff per attempt. |
| Multiplier | `BackoffOptions.multiplier` (**2** when built via `BackoffSpec.exponential`) | `BackoffSpec.exponential(multiplier=2.0)` | `RetryConfig::multiplier` (**2.0**) | Exponential growth factor. |
| Jitter (ms) | `BackoffOptions.jitterMs` | `BackoffSpec(jitter_ms=...)` | `RetryConfig::jitter_ms` (**100**) | Symmetric ±jitter applied per retry. |
| Backoff strategy (per job) | `JobsOptions.backoff` (`number` or `BackoffOptions`) | `Queue.add(backoff=...)` | `JobRetryOverride::backoff: BackoffSpec` | Override the queue-wide curve for this job. |

## Result storage

| Option | Node | Python | Rust | Controls |
|---|---|---|---|---|
| Persist handler return values | `WorkerOptions.storeResults` (**false**) | `Worker(store_results=False)` | `ConsumerConfig::store_results` (**false**) | When `true`, the engine writes each non-empty handler return to `{chasqui:<queue>}:result:<jobId>`. |
| Result key TTL (ms / s) | `WorkerOptions.resultTtlMs` (**3_600_000**, rounded up to s) | `Worker(result_ttl_ms=3_600_000)` | `ConsumerConfig::result_ttl_secs` (**3600**) | TTL for stored results. |
| Result-writer batch size | n/a | n/a | `ConsumerConfig::result_batch` (**64**) | Max completed jobs flushed per pipelined result-write round trip. Larger amortizes RTT; smaller lowers result-visibility latency. Only consulted when `store_results = true`. |
| Result-writer idle flush (ms) | n/a | n/a | `ConsumerConfig::result_idle_ms` (**5**) | Idle deadline before a partial result-writer batch flushes. Caps worst-case wait for a trailing result under low concurrency. |
| Result wait timeout | `Job.waitForResult({ timeoutMs })` (**30_000**) | `Job.wait_for_result(timeout=30.0)` | (use `Producer::get_result` directly) | Caller-side polling timeout. |
| Result wait poll interval | `Job.waitForResult({ intervalMs })` (**100**) | `Job.wait_for_result(poll_interval=0.1)` | n/a | Polling frequency. |

## Pause / resume

| Option | Node | Python | Rust | Controls |
|---|---|---|---|---|
| Process-local pause | `Worker.pause()` / `.resume()` / `.isPaused()` | `Worker.pause()` / `.resume()` / `.is_paused()` | `Consumer::pause_control()` → `PauseControl::{pause,resume,is_paused}` | In-memory stop-dispatch for one worker. Resume is instant (edge-triggered). Idempotent. Does not survive process restart. |
| Durable cross-process pause | `Queue.pause()` / `.resume()` / `.isPaused()` | `Queue.pause()` / `.resume()` / `.is_paused()` | `Producer::{pause,resume,is_paused}` | Sets/clears `{chasqui:<queue>}:paused` (no TTL). Every consumer of the queue parks; survives consumer restarts until `resume`. Also via `chasqui pause <queue>` / `chasqui resume <queue>`. |
| Cross-process pause poll (ms) | n/a | n/a | `ConsumerConfig::pause_poll_ms` (**250**) | How often a consumer re-checks the durable pause key, and the worst-case latency for a cross-process pause/resume to be observed. Not on the per-job hot path: when not paused the reader pays one atomic load + one time comparison per batch, no Redis round trip. |

## Scheduler

| Option | Node | Python | Rust | Controls |
|---|---|---|---|---|
| Auto-spawn embedded scheduler | `WorkerOptions.runScheduler` (**true**) | `Worker(run_scheduler=True)` | `ConsumerConfig::run_scheduler` (**true**) | Whether `Consumer::run` spawns a `Scheduler<T>` task. |
| Scheduler tick interval (ms) | `WorkerOptions.schedulerTickMs` (**1000**) | `Worker(scheduler_tick_ms=...)` (default engine **1000**) | `SchedulerConfig::tick_interval_ms` (**1000**) | How often the leader drains due specs. Lower bound on per-spec fire jitter. |
| Specs hydrated per tick | (engine default) | (engine default) | `SchedulerConfig::batch` (**256**) | Max specs materialized per tick. |
| Scheduler lock TTL (s) | (engine default) | (engine default) | `SchedulerConfig::lock_ttl_secs` (**5**) | Leader-election lock TTL. |
| Auto-spawn promoter | (engine default) | `Worker(delayed_enabled=True)` | `ConsumerConfig::delayed_enabled` (**true**) | Whether `Consumer::run` spawns a `Promoter` task. |
| Promoter poll interval (ms) | (engine default) | (engine default) | `ConsumerConfig::delayed_poll_interval_ms` (**100**) | How often the promoter checks the delayed ZSET. |
| Promote batch | (engine default) | (engine default) | `ConsumerConfig::delayed_promote_batch` (**256**) | Max promotions per tick. |
| Promoter lock TTL (s) | (engine default) | (engine default) | `ConsumerConfig::delayed_lock_ttl_secs` (**5**) | Leader-election lock TTL. |
| Catch-up policy | `RepeatOptions.missedFires` (**`{ kind: 'skip' }`**) | `Queue.add(missed_fires=MissedFiresPolicy.skip())` | `RepeatableSpec::missed_fires` (**`Skip`**) | What to do with windows missed during scheduler downtime. |

## DLQ

| Option | Node | Python | Rust | Controls |
|---|---|---|---|---|
| DLQ stream `MAXLEN ~` cap | (engine default) | `Worker(dlq_max_stream_len=...)` (default engine **100_000**) | `ConsumerConfig::dlq_max_stream_len` (**100_000**) | Max retained DLQ entries. |
| DLQ relocator inflight | (engine default) | (engine default) | `ConsumerConfig::dlq_inflight` (**32**) | Bounded channel size for the DLQ relocator. |
| Retry relocator inflight | (engine default) | (engine default) | `ConsumerConfig::retry_inflight` (**64**) | Bounded channel size for the retry relocator. |
| Oversize-payload threshold (bytes) | (engine default) | `Worker(max_payload_bytes=...)` (default engine **1_048_576**) | `ConsumerConfig::max_payload_bytes` (**1_048_576**) | Entries above this go straight to the DLQ as `OversizePayload`. |
| DLQ peek limit | `Queue.peekDlq(limit)` (**20**) | `Queue.peek_dlq(limit=20)` | `Producer::peek_dlq(limit)` | How many DLQ entries to inspect. |
| DLQ replay limit | `Queue.replayDlq(limit)` (**100**) | `Queue.replay_dlq(limit=100)` | `Producer::replay_dlq(limit)` | How many DLQ entries to atomically replay. |
| Unrecoverable signal | `throw new UnrecoverableError(...)` | `raise UnrecoverableError(...)` | `Err(HandlerError::unrecoverable(e))` | Skip retries; route straight to the DLQ. |

## Observability

| Option | Node | Python | Rust | Controls |
|---|---|---|---|---|
| Enable events stream | (engine default) | `Worker(events_enabled=True)` | `ConsumerConfig::events_enabled` (**true**) | Whether the engine writes to `{chasqui:<queue>}:events`. |
| Events stream `MAXLEN ~` | (engine default) | (engine default) | `ConsumerConfig::events_max_stream_len` (**100_000**) | Cap on retained events. |
| `MetricsSink` implementation | (use the native `Consumer` directly to swap) | (use the native `Consumer` directly to swap) | `ConsumerConfig::metrics: Arc<dyn MetricsSink>` (**`NoopSink`**) | In-process observability adapter (Prometheus, OpenTelemetry, etc.). |
| QueueEvents subscriber start id | `QueueEventsOptions.lastEventId` (**`"$"`**) | `QueueEvents(last_event_id="$")` | (build your own with `XREAD`) | Where to start tailing the events stream. |
| QueueEvents block timeout (ms) | `QueueEventsOptions.blockingTimeout` (**10_000**) | `QueueEvents(block_ms=5000)` | n/a | `XREAD BLOCK` timeout. |

## Producer-side caps

| Option | Node | Python | Rust | Controls |
|---|---|---|---|---|
| Main stream `MAXLEN ~` | (engine default) | `Queue(max_stream_len=...)` (default engine **1_000_000**) | `ProducerConfig::max_stream_len` (**1_000_000**) | Cap on the main stream. |
| Max scheduling delay (s) | (engine default) | `Queue(max_delay_secs=...)` (default engine **2_592_000**) | `ProducerConfig::max_delay_secs` (**30 days**) | Reject `add_in` / `add_at` whose delay exceeds this. |
| Max payload size (bytes) | `ProducerOpts.maxPayloadBytes` (native; default engine **1_048_576**) | `Queue(max_payload_bytes=...)` (default engine **1_048_576**) | `ProducerConfig::max_payload_bytes` (**1_048_576**) | Reject any `add*` / `upsert_repeatable` whose encoded payload exceeds this, *before* the Redis write. Mirrors the consumer-side oversize-payload threshold for symmetric produce/consume semantics. |
| Job name length (bytes) | (256, enforced at FFI) | (256, enforced at FFI) | `MAX_NAME_LEN` (**256**) | UTF-8 dispatch name cap. |

## See also

- [Node API](/reference/node-api/), [Python API](/reference/python-api/), [Rust API](/reference/rust-api/) — the canonical type definitions.
- [Tune for throughput guide](/guides/tune-for-throughput/) — which options actually move the headline numbers.
- [Concepts: retry and backoff](/concepts/retry-and-backoff/) — what the retry curve does and how it interacts with the DLQ.
