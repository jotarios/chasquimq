use crate::redis::keys::{NAME_FIELD, PAYLOAD_FIELD};
use bytes::Bytes;
use fred::types::Value;

/// Returns `{promoted, depth, oldest_pending_lag_ms, promoted_members}` so
/// the caller can emit observability signals without paying for an extra
/// `ZCARD` / `ZRANGE` round trip, and so the caller can clean up per-job
/// side-index keys (`didx:<job_id>`) for the entries that just moved to
/// the stream.
///
/// - `promoted` — number of entries moved from the delayed ZSET to the stream.
/// - `depth` — `ZCARD` after promotion.
/// - `oldest_pending_lag_ms` — `now - min_score_in_zset` for the oldest entry
///   **still pending after this tick's promotion finished**, or `0` if the
///   ZSET is empty or the oldest remaining entry is still future-dated.
///   In a healthy steady state this is `0` most ticks — it becomes positive
///   only when a real backlog forms.
/// - `promoted_members` — array of raw msgpack-encoded `Job<T>` byte strings
///   that were promoted this tick. The caller decodes the `JobId` from each
///   and pipelines `DEL didx:<id>` to clean up the side-index written at
///   schedule time. The dedup marker (`dlid:<id>`) is **deliberately not
///   touched here** — its remaining TTL covers the post-promote window in
///   which a delayed producer-retry could otherwise duplicate-schedule.
pub(crate) const PROMOTE_SCRIPT: &str = r#"
local time = redis.call('TIME')
local now_ms = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now_ms, 'LIMIT', 0, tonumber(ARGV[1]))
for _, bytes in ipairs(due) do
  redis.call('XADD', KEYS[2], 'MAXLEN', '~', tonumber(ARGV[2]), '*', 'd', bytes)
  redis.call('ZREM', KEYS[1], bytes)
end
local depth = redis.call('ZCARD', KEYS[1])
local lag_ms = 0
if depth > 0 then
  local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
  if oldest[2] then
    local diff = now_ms - tonumber(oldest[2])
    if diff > 0 then lag_ms = diff end
  end
end
return {#due, depth, lag_ms, due}
"#;

/// Atomically acknowledge-and-delete a stream entry from the consumer group's
/// pending list, then re-schedule it onto the delayed sorted set. The XACKDEL
/// gate (only ZADD on successful ack-and-delete) is what makes this script
/// idempotent under client-side retry: if the relocator's first EVALSHA
/// committed server-side but its reply was lost, the client's retry sees
/// XACKDEL return -1 (or 0) for the already-removed id and correctly skips
/// the ZADD — preventing a duplicate scheduled retry.
///
/// KEYS[1] = stream, KEYS[2] = delayed
/// ARGV[1] = group, ARGV[2] = entry_id, ARGV[3] = run_at_ms, ARGV[4] = encoded_bytes
pub(crate) const RETRY_RESCHEDULE_SCRIPT: &str = r#"
local result = redis.call('XACKDEL', KEYS[1], ARGV[1], 'IDS', 1, ARGV[2])
local first
if type(result) == 'table' then
  first = tonumber(result[1])
else
  first = tonumber(result)
end
if first == 1 then
  redis.call('ZADD', KEYS[2], tonumber(ARGV[3]), ARGV[4])
  return 1
end
return 0
"#;

/// Replays up to ARGV[1] entries from the DLQ stream (KEYS[1]) back into the
/// main stream (KEYS[2]). For each entry, the caller has already decoded the
/// DLQ payload, reset Job::attempt to 0, re-encoded, and read the source
/// entry's `n` field — the script just does the move atomically.
///
/// ARGV[1] = max_stream_len (for XADD MAXLEN ~)
/// ARGV[2..] = triples of (dlq_entry_id, replay_payload_bytes, name)
///   where `name` is the source DLQ entry's `n` field, or the empty string
///   if the source had no `n` (pre-name-on-wire producers, or reader-side
///   DLQ routes for malformed entries). An empty `name` is omitted from the
///   re-emitted XADD entirely so the replay shape matches an unnamed
///   producer's path byte-for-byte.
///
/// **Concurrent-replay safety**: XDEL is checked first; XADD only happens if
/// XDEL returned 1 (the entry actually existed and was removed). If a second
/// concurrent replay reaches the same dlq_id, its XDEL returns 0 and that
/// triple is skipped — no duplicate XADD to the main stream. The atomic
/// ordering (XDEL gate, then XADD inside the same script invocation) is what
/// makes concurrent replays correct without an external lock.
pub(crate) const REPLAY_DLQ_SCRIPT: &str = r#"
local dlq = KEYS[1]
local stream = KEYS[2]
local max_stream_len = ARGV[1]
local replayed = 0
local i = 2
while i <= #ARGV do
  local dlq_id = ARGV[i]
  local payload = ARGV[i + 1]
  local name = ARGV[i + 2]
  local deleted = redis.call('XDEL', dlq, dlq_id)
  if deleted == 1 then
    if name ~= nil and name ~= '' then
      redis.call('XADD', stream, 'MAXLEN', '~', max_stream_len, '*', 'd', payload, 'n', name)
    else
      redis.call('XADD', stream, 'MAXLEN', '~', max_stream_len, '*', 'd', payload)
    end
    replayed = replayed + 1
  end
  i = i + 3
end
return replayed
"#;

/// Releases the lock at KEYS[1] only if its current value is ARGV[1] — i.e.
/// only if we still hold it. A paused promoter that wakes up after its lease
/// expired and another holder took over must NOT delete the new holder's
/// lock. Returns 1 if released, 0 if held by someone else (or absent).
pub(crate) const RELEASE_LOCK_SCRIPT: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"#;

/// Idempotent delayed-schedule. Sets a per-job-id dedup marker with `SET NX EX`
/// and only `ZADD`s the encoded payload onto the delayed ZSET on a fresh
/// reservation. A second invocation with the same `JobId` (e.g. from a
/// network-driven caller retry) returns 0 and does not duplicate the job.
///
/// The marker TTL is `seconds_until_run + grace` so the marker outlives the
/// scheduled fire time long enough that a delayed retry of the producer call
/// cannot race a successful promotion. The grace constant is owned by the
/// caller (see `Producer::DEDUP_MARKER_GRACE_SECS`).
///
/// On a fresh reservation we also write the side-index key
/// `{chasqui:<queue>}:didx:<job_id>` whose value is the exact encoded ZSET
/// member. `Producer::cancel_delayed` uses this to `ZREM` precisely without
/// scanning. The side-index TTL matches the dedup marker — after a successful
/// promotion the key just expires naturally; the promoter never touches it
/// because the cancel script handles the "already promoted" race correctly
/// (GET hits, ZREM returns 0 → cancel returns 0).
///
/// KEYS[1] = dedup marker key (`{chasqui:<queue>}:dlid:<job_id>`)
/// KEYS[2] = delayed ZSET key (`{chasqui:<queue>}:delayed`)
/// KEYS[3] = side-index key (`{chasqui:<queue>}:didx:<job_id>`)
/// ARGV[1] = marker / index TTL in seconds
/// ARGV[2] = run_at_ms (ZADD score)
/// ARGV[3] = encoded payload bytes (ZSET member + side-index value)
///
/// Returns 1 if newly scheduled, 0 if a duplicate was suppressed.
pub(crate) const SCHEDULE_DELAYED_IDEMPOTENT_SCRIPT: &str = r#"
local set_res = redis.call('SET', KEYS[1], '1', 'NX', 'EX', tonumber(ARGV[1]))
if set_res == false then
  return 0
end
redis.call('ZADD', KEYS[2], tonumber(ARGV[2]), ARGV[3])
redis.call('SET', KEYS[3], ARGV[3], 'EX', tonumber(ARGV[1]))
return 1
"#;

/// Cancel a delayed job by `JobId`. Looks up the exact encoded ZSET member
/// via the side-index, `ZREM`s it from the delayed ZSET, and clears the
/// dedup marker so the same id can be rescheduled later.
///
/// Cancel-vs-promote race: both paths execute as Lua on a single shard
/// (everything shares the `{chasqui:<queue>}` hash tag), so they serialize
/// at Redis. The three observable outcomes are:
/// - cancel runs first → ZREM returns 1 → promoter's later ZRANGEBYSCORE
///   doesn't see it → job never delivered, cancel returns true.
/// - promoter runs first → side-index still resolves (TTL outlives promote),
///   ZREM returns 0 (already gone) → cancel returns false, job delivered.
/// - side-index already expired or never existed → GET returns nil →
///   cancel returns false. (Not strictly distinguishable from "promoted long
///   ago" — the bool return value collapses both into "not cancelled".)
///
/// On the ZREM-miss path we deliberately do NOT delete the dedup marker:
/// leaving it in place preserves the post-promote idempotence guarantee a
/// late producer retry depends on. The stale side-index will TTL out on its
/// own.
///
/// KEYS[1] = delayed ZSET key (`{chasqui:<queue>}:delayed`)
/// KEYS[2] = side-index key   (`{chasqui:<queue>}:didx:<job_id>`)
/// KEYS[3] = dedup marker key (`{chasqui:<queue>}:dlid:<job_id>`)
///
/// Returns 1 if the entry was removed from the ZSET, 0 otherwise.
pub(crate) const CANCEL_DELAYED_SCRIPT: &str = r#"
local member = redis.call('GET', KEYS[2])
if not member then
  return 0
end
local removed = redis.call('ZREM', KEYS[1], member)
if removed == 0 then
  return 0
end
redis.call('DEL', KEYS[2])
redis.call('DEL', KEYS[3])
return 1
"#;

/// Atomic upsert of a repeatable spec: writes the spec hash and the repeat
/// ZSET entry in a single round trip. Re-upserting the same key overwrites
/// the spec (same hash key) and bumps the next-fire score (ZADD with no
/// XX/NX flag). Returns 1 unconditionally so callers don't need to special-
/// case the "first write" vs. "overwrite" reply.
///
/// KEYS[1] = repeat ZSET (`{chasqui:<queue>}:repeat`)
/// KEYS[2] = spec hash (`{chasqui:<queue>}:repeat:spec:<key>`)
/// ARGV[1] = next_fire_ms (ZADD score)
/// ARGV[2] = spec_key (ZADD member)
/// ARGV[3] = encoded `StoredSpec` bytes (HSET field `spec`)
pub(crate) const UPSERT_REPEATABLE_SCRIPT: &str = r#"
redis.call('ZADD', KEYS[1], tonumber(ARGV[1]), ARGV[2])
redis.call('HSET', KEYS[2], 'spec', ARGV[3])
return 1
"#;

/// Atomic remove of a repeatable spec: ZREM from the repeat ZSET, then DEL
/// the spec hash. Returns 1 if the ZREM removed an entry, 0 otherwise. The
/// hash is deleted unconditionally so a stale spec hash without a ZSET
/// entry is also reaped (defensive against partially-aborted upserts).
///
/// KEYS[1] = repeat ZSET
/// KEYS[2] = spec hash
/// ARGV[1] = spec_key
pub(crate) const REMOVE_REPEATABLE_SCRIPT: &str = r#"
local removed = redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('DEL', KEYS[2])
return removed
"#;

/// Schedule one or more fires of a repeatable spec, in a single round trip.
///
/// The Rust scheduler picks the due spec out of the repeat ZSET, decodes
/// the spec hash, builds a list of (fire_at_ms, encoded `Job<T>` bytes)
/// pairs (one for the on-time fire, plus extras for any catch-up windows
/// when [`crate::repeat::MissedFiresPolicy::FireAll`] is set), and hands
/// it all to this script in a single call. The script:
///
/// 1. For each `(fire_at_ms, payload)` pair, dispatches it: XADD into the
///    stream when `fire_at_ms <= now_ms`, otherwise ZADD into the delayed
///    ZSET (so the Promoter picks it up at the right time). HINCRBY's
///    `fired` once per dispatched fire. If `fired` would exceed `limit`,
///    stops dispatching mid-batch and falls through to removal.
/// 2. After dispatching, decides removal vs reschedule. Removes the spec
///    entirely (ZREM + DEL) when the limit is hit, when `next_fire_ms >
///    end_before_ms`, or when `next_fire_ms <= 0` (caller's signal that
///    the pattern has no more fires). Otherwise ZADDs `next_fire_ms` as
///    the new score in the repeat ZSET.
///
/// Lua serialization gives us a strong invariant: the scheduler's
/// "decode → fire (one or many) → reschedule" sequence cannot be
/// interrupted by another scheduler tick. Catch-up replays of N missed
/// windows happen atomically with the `next_fire_ms` ZADD — no partial
/// state is ever visible if the caller crashes mid-script. The leader-
/// election lock already excludes concurrent scheduler ticks, but this
/// is belt-and-suspenders for the rare ABA case across leader handover.
///
/// KEYS[1] = stream key
/// KEYS[2] = delayed ZSET
/// KEYS[3] = repeat ZSET
/// KEYS[4] = spec hash
/// ARGV[1] = now_ms
/// ARGV[2] = next_fire_ms (when the next iteration should fire; 0 = no
///           more iterations, remove the spec)
/// ARGV[3] = max_stream_len (for XADD MAXLEN ~)
/// ARGV[4] = spec_key (member in the repeat ZSET)
/// ARGV[5] = limit (0 = unlimited)
/// ARGV[6] = end_before_ms (0 = no end-bound)
/// ARGV[7] = fire_count (N — number of (fire_at_ms, payload) pairs that
///           follow). 0 is legal (Skip policy with no on-time fire — just
///           reschedule).
/// ARGV[8 .. 7+2*N] = interleaved `(fire_at_ms_i, payload_i)` pairs, in
///                    chronological order. Each pair is two ARGV slots.
///
/// Returns `{fired_now, removed}` where:
/// - `fired_now` is the count of jobs actually dispatched this call
///   (XADD + ZADD combined). 0 when ARGV[7] is 0 or when `limit` was
///   already exhausted before this call.
/// - `removed` is `1` if the spec was removed (limit hit / end_before
///   passed / next_fire_ms == 0), `0` otherwise.
pub(crate) const SCHEDULE_REPEATABLE_SCRIPT: &str = r#"
local now_ms = tonumber(ARGV[1])
local next_fire_ms = tonumber(ARGV[2])
local max_stream_len = tonumber(ARGV[3])
local spec_key = ARGV[4]
local limit = tonumber(ARGV[5])
local end_before_ms = tonumber(ARGV[6])
local fire_count = tonumber(ARGV[7])

local fired_now = 0
local hit_limit = false
local i = 0
while i < fire_count do
  local fire_at_ms = tonumber(ARGV[8 + i * 2])
  local payload = ARGV[9 + i * 2]
  if limit > 0 then
    local fired_so_far = tonumber(redis.call('HGET', KEYS[4], 'fired')) or 0
    if fired_so_far >= limit then
      hit_limit = true
      break
    end
  end
  if fire_at_ms <= now_ms then
    redis.call('XADD', KEYS[1], 'MAXLEN', '~', max_stream_len, '*', 'd', payload)
  else
    redis.call('ZADD', KEYS[2], fire_at_ms, payload)
  end
  redis.call('HINCRBY', KEYS[4], 'fired', 1)
  fired_now = fired_now + 1
  i = i + 1
end

local fired = tonumber(redis.call('HGET', KEYS[4], 'fired')) or 0
local removed = 0
local exhausted = (limit > 0 and fired >= limit) or hit_limit
local past_end = (end_before_ms > 0 and next_fire_ms > end_before_ms)
local no_next = (next_fire_ms <= 0)
if exhausted or past_end or no_next then
  redis.call('ZREM', KEYS[3], spec_key)
  redis.call('DEL', KEYS[4])
  removed = 1
else
  redis.call('ZADD', KEYS[3], next_fire_ms, spec_key)
end

return {fired_now, removed}
"#;

pub(crate) const ACQUIRE_LOCK_SCRIPT: &str = r#"
local cur = redis.call('GET', KEYS[1])
if cur == false then
  redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[2]))
  return 1
end
if cur == ARGV[1] then
  redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
  return 1
end
return 0
"#;

pub(crate) fn xadd_args(
    stream_key: &str,
    producer_id: &str,
    iid: &str,
    max_stream_len: u64,
    bytes: Bytes,
    name: &str,
) -> Vec<Value> {
    let mut args: Vec<Value> = Vec::with_capacity(10 + (!name.is_empty() as usize) * 2);
    args.push(Value::from(stream_key));
    args.push(Value::from("IDMP"));
    args.push(Value::from(producer_id));
    args.push(Value::from(iid));
    args.push(Value::from("MAXLEN"));
    args.push(Value::from("~"));
    args.push(Value::from(max_stream_len as i64));
    args.push(Value::from("*"));
    args.push(Value::from(PAYLOAD_FIELD));
    args.push(Value::Bytes(bytes));
    if !name.is_empty() {
        args.push(Value::from(NAME_FIELD));
        args.push(Value::from(name));
    }
    args
}

pub(crate) fn xreadgroup_args(
    group: &str,
    consumer: &str,
    batch: usize,
    block_ms: u64,
    claim_min_idle_ms: u64,
    stream_key: &str,
) -> Vec<Value> {
    vec![
        Value::from("GROUP"),
        Value::from(group),
        Value::from(consumer),
        Value::from("COUNT"),
        Value::from(batch as i64),
        Value::from("BLOCK"),
        Value::from(block_ms as i64),
        Value::from("CLAIM"),
        Value::from(claim_min_idle_ms as i64),
        Value::from("STREAMS"),
        Value::from(stream_key),
        Value::from(">"),
    ]
}

pub(crate) fn xackdel_args(stream_key: &str, group: &str, ids: &[impl AsRef<str>]) -> Vec<Value> {
    let mut args: Vec<Value> = Vec::with_capacity(4 + ids.len());
    args.push(Value::from(stream_key));
    args.push(Value::from(group));
    args.push(Value::from("IDS"));
    args.push(Value::from(ids.len() as i64));
    for id in ids {
        args.push(Value::from(id.as_ref()));
    }
    args
}

/// XADD args for relocating a stream entry into the DLQ.
/// Carries the original payload plus source_id/reason/optional detail metadata,
/// and the optional `n` field if the source entry had one — preserved verbatim
/// so DLQ inspectors and the future replay path can route by name.
#[allow(clippy::too_many_arguments)]
pub(crate) fn xadd_dlq_args(
    dlq_key: &str,
    producer_id: &str,
    source_id: &str,
    payload: Bytes,
    reason: &str,
    detail: Option<&str>,
    max_stream_len: u64,
    name: &str,
) -> Vec<Value> {
    let mut args: Vec<Value> =
        Vec::with_capacity(16 + (detail.is_some() as usize + !name.is_empty() as usize) * 2);
    args.push(Value::from(dlq_key));
    args.push(Value::from("IDMP"));
    args.push(Value::from(producer_id));
    args.push(Value::from(source_id));
    args.push(Value::from("MAXLEN"));
    args.push(Value::from("~"));
    args.push(Value::from(max_stream_len as i64));
    args.push(Value::from("*"));
    args.push(Value::from(PAYLOAD_FIELD));
    args.push(Value::Bytes(payload));
    if !name.is_empty() {
        args.push(Value::from(NAME_FIELD));
        args.push(Value::from(name));
    }
    args.push(Value::from("source_id"));
    args.push(Value::from(source_id));
    args.push(Value::from("reason"));
    args.push(Value::from(reason));
    if let Some(d) = detail {
        args.push(Value::from("detail"));
        args.push(Value::from(d));
    }
    args
}

pub(crate) fn zadd_delayed_args(delayed_key: &str, run_at_ms: i64, bytes: Bytes) -> Vec<Value> {
    vec![
        Value::from(delayed_key),
        Value::from(run_at_ms),
        Value::Bytes(bytes),
    ]
}

pub(crate) fn evalsha_promote_args(
    sha: &str,
    delayed_key: &str,
    stream_key: &str,
    limit: usize,
    max_stream_len: u64,
) -> Vec<Value> {
    vec![
        Value::from(sha),
        Value::from(2_i64),
        Value::from(delayed_key),
        Value::from(stream_key),
        Value::from(limit as i64),
        Value::from(max_stream_len as i64),
    ]
}

pub(crate) fn eval_promote_args(
    script: &str,
    delayed_key: &str,
    stream_key: &str,
    limit: usize,
    max_stream_len: u64,
) -> Vec<Value> {
    vec![
        Value::from(script),
        Value::from(2_i64),
        Value::from(delayed_key),
        Value::from(stream_key),
        Value::from(limit as i64),
        Value::from(max_stream_len as i64),
    ]
}

pub(crate) fn script_load_args(script: &str) -> Vec<Value> {
    vec![Value::from("LOAD"), Value::from(script)]
}

pub(crate) fn xrange_args(stream_key: &str, limit: usize) -> Vec<Value> {
    vec![
        Value::from(stream_key),
        Value::from("-"),
        Value::from("+"),
        Value::from("COUNT"),
        Value::from(limit as i64),
    ]
}

pub(crate) fn evalsha_replay_args(
    sha: &str,
    dlq_key: &str,
    stream_key: &str,
    max_stream_len: u64,
    triples: &[(String, Bytes, String)],
) -> Vec<Value> {
    let mut args: Vec<Value> = Vec::with_capacity(5 + triples.len() * 3);
    args.push(Value::from(sha));
    args.push(Value::from(2_i64));
    args.push(Value::from(dlq_key));
    args.push(Value::from(stream_key));
    args.push(Value::from(max_stream_len as i64));
    for (id, bytes, name) in triples {
        args.push(Value::from(id.as_str()));
        args.push(Value::Bytes(bytes.clone()));
        args.push(Value::from(name.as_str()));
    }
    args
}

pub(crate) fn eval_replay_args(
    script: &str,
    dlq_key: &str,
    stream_key: &str,
    max_stream_len: u64,
    triples: &[(String, Bytes, String)],
) -> Vec<Value> {
    let mut args: Vec<Value> = Vec::with_capacity(5 + triples.len() * 3);
    args.push(Value::from(script));
    args.push(Value::from(2_i64));
    args.push(Value::from(dlq_key));
    args.push(Value::from(stream_key));
    args.push(Value::from(max_stream_len as i64));
    for (id, bytes, name) in triples {
        args.push(Value::from(id.as_str()));
        args.push(Value::Bytes(bytes.clone()));
        args.push(Value::from(name.as_str()));
    }
    args
}

pub(crate) fn evalsha_retry_args(
    sha: &str,
    stream_key: &str,
    delayed_key: &str,
    group: &str,
    entry_id: &str,
    run_at_ms: i64,
    bytes: Bytes,
) -> Vec<Value> {
    vec![
        Value::from(sha),
        Value::from(2_i64),
        Value::from(stream_key),
        Value::from(delayed_key),
        Value::from(group),
        Value::from(entry_id),
        Value::from(run_at_ms),
        Value::Bytes(bytes),
    ]
}

pub(crate) fn eval_retry_args(
    script: &str,
    stream_key: &str,
    delayed_key: &str,
    group: &str,
    entry_id: &str,
    run_at_ms: i64,
    bytes: Bytes,
) -> Vec<Value> {
    vec![
        Value::from(script),
        Value::from(2_i64),
        Value::from(stream_key),
        Value::from(delayed_key),
        Value::from(group),
        Value::from(entry_id),
        Value::from(run_at_ms),
        Value::Bytes(bytes),
    ]
}

pub(crate) fn eval_acquire_lock_args(
    script: &str,
    lock_key: &str,
    holder_id: &str,
    ttl_secs: u64,
) -> Vec<Value> {
    vec![
        Value::from(script),
        Value::from(1_i64),
        Value::from(lock_key),
        Value::from(holder_id),
        Value::from(ttl_secs as i64),
    ]
}

pub(crate) fn eval_release_lock_args(script: &str, lock_key: &str, holder_id: &str) -> Vec<Value> {
    vec![
        Value::from(script),
        Value::from(1_i64),
        Value::from(lock_key),
        Value::from(holder_id),
    ]
}

pub(crate) fn evalsha_schedule_delayed_idempotent_args(
    sha: &str,
    marker_key: &str,
    delayed_key: &str,
    index_key: &str,
    marker_ttl_secs: u64,
    run_at_ms: i64,
    bytes: Bytes,
) -> Vec<Value> {
    vec![
        Value::from(sha),
        Value::from(3_i64),
        Value::from(marker_key),
        Value::from(delayed_key),
        Value::from(index_key),
        Value::from(marker_ttl_secs as i64),
        Value::from(run_at_ms),
        Value::Bytes(bytes),
    ]
}

pub(crate) fn eval_schedule_delayed_idempotent_args(
    script: &str,
    marker_key: &str,
    delayed_key: &str,
    index_key: &str,
    marker_ttl_secs: u64,
    run_at_ms: i64,
    bytes: Bytes,
) -> Vec<Value> {
    vec![
        Value::from(script),
        Value::from(3_i64),
        Value::from(marker_key),
        Value::from(delayed_key),
        Value::from(index_key),
        Value::from(marker_ttl_secs as i64),
        Value::from(run_at_ms),
        Value::Bytes(bytes),
    ]
}

pub(crate) fn evalsha_cancel_delayed_args(
    sha: &str,
    delayed_key: &str,
    index_key: &str,
    marker_key: &str,
) -> Vec<Value> {
    vec![
        Value::from(sha),
        Value::from(3_i64),
        Value::from(delayed_key),
        Value::from(index_key),
        Value::from(marker_key),
    ]
}

pub(crate) fn eval_cancel_delayed_args(
    script: &str,
    delayed_key: &str,
    index_key: &str,
    marker_key: &str,
) -> Vec<Value> {
    vec![
        Value::from(script),
        Value::from(3_i64),
        Value::from(delayed_key),
        Value::from(index_key),
        Value::from(marker_key),
    ]
}

pub(crate) fn evalsha_upsert_repeatable_args(
    sha: &str,
    repeat_key: &str,
    spec_hash_key: &str,
    next_fire_ms: i64,
    spec_key: &str,
    bytes: Bytes,
) -> Vec<Value> {
    vec![
        Value::from(sha),
        Value::from(2_i64),
        Value::from(repeat_key),
        Value::from(spec_hash_key),
        Value::from(next_fire_ms),
        Value::from(spec_key),
        Value::Bytes(bytes),
    ]
}

pub(crate) fn eval_upsert_repeatable_args(
    script: &str,
    repeat_key: &str,
    spec_hash_key: &str,
    next_fire_ms: i64,
    spec_key: &str,
    bytes: Bytes,
) -> Vec<Value> {
    vec![
        Value::from(script),
        Value::from(2_i64),
        Value::from(repeat_key),
        Value::from(spec_hash_key),
        Value::from(next_fire_ms),
        Value::from(spec_key),
        Value::Bytes(bytes),
    ]
}

pub(crate) fn evalsha_remove_repeatable_args(
    sha: &str,
    repeat_key: &str,
    spec_hash_key: &str,
    spec_key: &str,
) -> Vec<Value> {
    vec![
        Value::from(sha),
        Value::from(2_i64),
        Value::from(repeat_key),
        Value::from(spec_hash_key),
        Value::from(spec_key),
    ]
}

pub(crate) fn eval_remove_repeatable_args(
    script: &str,
    repeat_key: &str,
    spec_hash_key: &str,
    spec_key: &str,
) -> Vec<Value> {
    vec![
        Value::from(script),
        Value::from(2_i64),
        Value::from(repeat_key),
        Value::from(spec_hash_key),
        Value::from(spec_key),
    ]
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn evalsha_schedule_repeatable_args(
    sha: &str,
    stream_key: &str,
    delayed_key: &str,
    repeat_key: &str,
    spec_hash_key: &str,
    now_ms: i64,
    next_fire_ms: i64,
    max_stream_len: u64,
    spec_key: &str,
    limit: u64,
    end_before_ms: u64,
    fires: &[(i64, Bytes)],
) -> Vec<Value> {
    let mut args: Vec<Value> = Vec::with_capacity(12 + fires.len() * 2);
    args.push(Value::from(sha));
    args.push(Value::from(4_i64));
    args.push(Value::from(stream_key));
    args.push(Value::from(delayed_key));
    args.push(Value::from(repeat_key));
    args.push(Value::from(spec_hash_key));
    args.push(Value::from(now_ms));
    args.push(Value::from(next_fire_ms));
    args.push(Value::from(max_stream_len as i64));
    args.push(Value::from(spec_key));
    args.push(Value::from(limit as i64));
    args.push(Value::from(end_before_ms as i64));
    args.push(Value::from(fires.len() as i64));
    for (fire_at_ms, bytes) in fires {
        args.push(Value::from(*fire_at_ms));
        args.push(Value::Bytes(bytes.clone()));
    }
    args
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn eval_schedule_repeatable_args(
    script: &str,
    stream_key: &str,
    delayed_key: &str,
    repeat_key: &str,
    spec_hash_key: &str,
    now_ms: i64,
    next_fire_ms: i64,
    max_stream_len: u64,
    spec_key: &str,
    limit: u64,
    end_before_ms: u64,
    fires: &[(i64, Bytes)],
) -> Vec<Value> {
    let mut args: Vec<Value> = Vec::with_capacity(12 + fires.len() * 2);
    args.push(Value::from(script));
    args.push(Value::from(4_i64));
    args.push(Value::from(stream_key));
    args.push(Value::from(delayed_key));
    args.push(Value::from(repeat_key));
    args.push(Value::from(spec_hash_key));
    args.push(Value::from(now_ms));
    args.push(Value::from(next_fire_ms));
    args.push(Value::from(max_stream_len as i64));
    args.push(Value::from(spec_key));
    args.push(Value::from(limit as i64));
    args.push(Value::from(end_before_ms as i64));
    args.push(Value::from(fires.len() as i64));
    for (fire_at_ms, bytes) in fires {
        args.push(Value::from(*fire_at_ms));
        args.push(Value::Bytes(bytes.clone()));
    }
    args
}

pub(crate) fn evalsha_acquire_lock_args(
    sha: &str,
    lock_key: &str,
    holder_id: &str,
    ttl_secs: u64,
) -> Vec<Value> {
    vec![
        Value::from(sha),
        Value::from(1_i64),
        Value::from(lock_key),
        Value::from(holder_id),
        Value::from(ttl_secs as i64),
    ]
}
