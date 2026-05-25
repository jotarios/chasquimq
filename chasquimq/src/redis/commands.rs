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
/// - `promoted_members` — array of msgpack-encoded `Job<T>` byte strings
///   (with the slice-3 length-prefix already stripped) that were promoted
///   this tick. The caller decodes the `JobId` from each and pipelines
///   `DEL didx:<id>` to clean up the side-index written at schedule time.
///   The dedup marker (`dlid:<id>`) is **deliberately not touched here** —
///   its remaining TTL covers the post-promote window in which a delayed
///   producer-retry could otherwise duplicate-schedule.
///
/// **ZSET member format (slice 3)**: each member is
/// `[u32_le name_len][name utf8][msgpack Job<T>]`. The script strips the
/// prefix in Lua and re-emits the name as the stream entry's `n` field
/// when non-empty (matching `xadd_args`'s shape on the immediate path).
/// See `chasquimq/src/redis/delayed_member.rs` for the encoder.
///
/// **Malformed members are cleansed, not fatal.** A member shorter than the
/// 4-byte length prefix, or whose declared `name_len` runs past the buffer,
/// is `ZREM`'d and skipped instead of aborting the EVAL — matching the bounds
/// checks in `decode_delayed_member`. Without this guard a single poison
/// member permanently wedges promotion for the whole queue (every tick
/// re-reads it and re-errors before the per-member cleanup runs).
pub(crate) const PROMOTE_SCRIPT: &str = r#"
local time = redis.call('TIME')
local now_ms = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now_ms, 'LIMIT', 0, tonumber(ARGV[1]))
local payloads = {}
local np = 0
for i, member in ipairs(due) do
  -- Parse the slice-3 length-prefixed member:
  --   [u32_le name_len][name utf8][msgpack payload]
  -- A malformed/short member must NOT abort the whole EVAL: string.byte on a
  -- <4-byte member returns nil, and arithmetic on nil errors out before the
  -- per-member ZREM below, so the poison member is never removed and every
  -- subsequent promote tick re-reads + re-errors -> promotion permanently
  -- wedged for this queue. Mirror the Rust decoder (delayed_member.rs:
  -- decode_delayed_member): bounds-check first, and on a bad member cleanse
  -- it with ZREM and skip emitting a job. payloads stays a dense array (own
  -- counter np, not the loop index i) so a skipped member can't punch a nil
  -- hole that truncates the reply for valid members after it.
  if #member < 4 then
    redis.call('ZREM', KEYS[1], member)
  else
    local b1, b2, b3, b4 = string.byte(member, 1, 4)
    local name_len = b1 + (b2 * 256) + (b3 * 65536) + (b4 * 16777216)
    if name_len > #member - 4 then
      redis.call('ZREM', KEYS[1], member)
    else
      local name = name_len > 0 and string.sub(member, 5, 4 + name_len) or ''
      local payload = string.sub(member, 5 + name_len)
      if name ~= '' then
        redis.call('XADD', KEYS[2], 'MAXLEN', '~', tonumber(ARGV[2]), '*', 'd', payload, 'n', name)
      else
        redis.call('XADD', KEYS[2], 'MAXLEN', '~', tonumber(ARGV[2]), '*', 'd', payload)
      end
      redis.call('ZREM', KEYS[1], member)
      np = np + 1
      payloads[np] = payload
    end
  end
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
return {#due, depth, lag_ms, payloads}
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
///
/// **Encoded bytes shape (slice 3)**: `encode_delayed_member(name, msgpack)`
/// — the caller writes a length-prefixed member so the promoter can later
/// re-emit `n` on the stream entry. Pre-slice-3 callers wrote raw msgpack
/// here; under slice 3 that shape no longer parses on the promoter side.
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

/// Atomically relocate a poisoned stream entry into the DLQ. `XACKDEL`s the
/// source entry from the consumer group's pending list first, then — only if
/// the ack actually removed the entry — `XADD`s it into the DLQ stream. The
/// move runs as one Lua invocation so the two writes can never be split by a
/// crash or a dropped connection: Redis either runs both or neither.
///
/// **Why the gate matters (the duplicate-on-retry bug this closes).** The
/// relocator used to issue `XADD` (re-enqueue into the DLQ) and then
/// `XACKDEL` (remove from the main stream) as a non-atomic pipeline. If the
/// process died after the `XADD` committed but before the `XACKDEL`, the
/// entry was *both* in the DLQ *and* still pending on the main stream — the
/// next CLAIM tick re-claimed it and routed a duplicate into the DLQ. The
/// `IDMP` argument only papered over this while its dedup marker survived DLQ
/// trimming. Gate-first + single-script atomicity is the real fix; `IDMP` is
/// now belt-and-suspenders.
///
/// **Idempotent under client retry.** If the script committed server-side but
/// its reply was lost and the relocator retried, the retry's `XACKDEL` finds
/// nothing to ack (`-1`/`0`) and the `XADD` is skipped — no duplicate. This
/// mirrors `RETRY_RESCHEDULE_SCRIPT`'s contract for the retry-reschedule path.
///
/// KEYS[1] = stream_key, KEYS[2] = dlq_key
/// ARGV[1] = group, ARGV[2] = entry_id, ARGV[3] = producer_id (IDMP scope),
/// ARGV[4] = source_id (IDMP id + `source_id` field), ARGV[5] = payload bytes,
/// ARGV[6] = reason, ARGV[7] = max_stream_len (XADD MAXLEN ~),
/// ARGV[8] = name ('' = omit the `n` field), ARGV[9] = detail ('' = omit the
/// `detail` field)
///
/// The re-emitted `XADD` field order is the DLQ entry contract:
/// `IDMP <producer_id> <source_id> MAXLEN ~ <max> * d <payload> [n <name>]
/// source_id <source_id> reason <reason> [detail <detail>]`. `peek_dlq` /
/// `replay_dlq` parse against exactly this shape, so it must stay in lockstep
/// with `parse_dlq_entry` in `producer/dlq.rs`.
///
/// Returns 1 (relocated) or 0 (gate lost — a concurrent CLAIM/manual ack
/// already removed the entry, so no DLQ write happened).
pub(crate) const RELOCATE_DLQ_SCRIPT: &str = r#"
local result = redis.call('XACKDEL', KEYS[1], ARGV[1], 'IDS', 1, ARGV[2])
local first
if type(result) == 'table' then
  first = tonumber(result[1])
else
  first = tonumber(result)
end
if first ~= 1 then
  return 0
end
local args = {KEYS[2], 'IDMP', ARGV[3], ARGV[4], 'MAXLEN', '~', ARGV[7], '*', 'd', ARGV[5]}
if ARGV[8] ~= nil and ARGV[8] ~= '' then
  args[#args + 1] = 'n'
  args[#args + 1] = ARGV[8]
end
args[#args + 1] = 'source_id'
args[#args + 1] = ARGV[4]
args[#args + 1] = 'reason'
args[#args + 1] = ARGV[6]
if ARGV[9] ~= nil and ARGV[9] ~= '' then
  args[#args + 1] = 'detail'
  args[#args + 1] = ARGV[9]
end
redis.call('XADD', unpack(args))
return 1
"#;

/// Pre-acked DLQ relocate (slice 12). Counterpart of [`RELOCATE_DLQ_SCRIPT`]
/// for the stalled-detector path: the caller has already XACKDEL'd the
/// source entry out of the PEL (in `STALLED_SCAN_SCRIPT`'s threshold-hit
/// branch), so the relocator must NOT re-issue XACKDEL (which would
/// return -1 / 0 → "gate lost" → DLQ write skipped → orphan ack).
///
/// This script does only the IDMP-XADD into the DLQ stream, mirroring
/// the field shape of [`RELOCATE_DLQ_SCRIPT`]'s XADD half byte-for-byte
/// so DLQ subscribers can't tell the two paths apart on the wire. The
/// IDMP marker on the XADD provides the dedup guard — concurrent
/// relocator retries of the same `source_id` are no-ops.
///
/// KEYS[1] = dlq_key (no stream_key — we don't touch the source stream)
/// ARGV[1] = producer_id (IDMP scope)
/// ARGV[2] = source_id (IDMP id + `source_id` field)
/// ARGV[3] = payload bytes
/// ARGV[4] = reason
/// ARGV[5] = max_stream_len (XADD MAXLEN ~)
/// ARGV[6] = name ('' = omit the `n` field)
/// ARGV[7] = detail ('' = omit the `detail` field)
///
/// Returns 1 unconditionally — the IDMP-XADD has its own dedup, and
/// reporting "did we relocate" with a script-level boolean would be
/// misleading (Redis collapses dedup-suppressed XADDs into a successful
/// reply too).
pub(crate) const RELOCATE_DLQ_PRE_ACKED_SCRIPT: &str = r#"
local args = {KEYS[1], 'IDMP', ARGV[1], ARGV[2], 'MAXLEN', '~', ARGV[5], '*', 'd', ARGV[3]}
if ARGV[6] ~= nil and ARGV[6] ~= '' then
  args[#args + 1] = 'n'
  args[#args + 1] = ARGV[6]
end
args[#args + 1] = 'source_id'
args[#args + 1] = ARGV[2]
args[#args + 1] = 'reason'
args[#args + 1] = ARGV[4]
if ARGV[7] ~= nil and ARGV[7] ~= '' then
  args[#args + 1] = 'detail'
  args[#args + 1] = ARGV[7]
end
redis.call('XADD', unpack(args))
return 1
"#;

/// Replays up to ARGV[1] entries from the DLQ stream (KEYS[1]) back into the
/// main stream (KEYS[2]). For each entry, the caller has already decoded the
/// DLQ payload, reset Job::attempt to 0, re-encoded, and read the source
/// entry's `n` field + `job_id` — the script just does the move atomically.
///
/// ARGV[1] = max_stream_len (for XADD MAXLEN ~)
/// ARGV[2..] = **quads** of (dlq_entry_id, replay_payload_bytes, name, job_id)
///   where `name` is the source DLQ entry's `n` field, or the empty string
///   if the source had no `n` (pre-name-on-wire producers, or reader-side
///   DLQ routes for malformed entries). `job_id` is the envelope's `Job.id`
///   so the script can DEL the per-job stall counter (slice 12) — if the
///   replayed entry was originally stalled, leaving the counter in place
///   would let a fresh stall streak inherit the old count. Empty `job_id`
///   collapses to "no counter to DEL" (reader-side DLQ routes for entries
///   whose envelope never decoded).
///
/// **Wire-format note (slice 12 ARGV shape change)**: pre-slice-12, this
/// script took triples of `(dlq_id, payload, name)`. Slice 12 widened to
/// quads to carry `job_id`. The Lua and the matching Rust argument builders
/// must roll together — there is no rolling-deploy skew window. Same
/// contract as every other engine script (the script body and its caller
/// are versioned as one unit, not independently).
///
/// **Concurrent-replay safety**: XDEL is checked first; XADD only happens if
/// XDEL returned 1 (the entry actually existed and was removed). If a second
/// concurrent replay reaches the same dlq_id, its XDEL returns 0 and that
/// quad is skipped — no duplicate XADD to the main stream. The atomic
/// ordering (XDEL gate, then XADD inside the same script invocation) is what
/// makes concurrent replays correct without an external lock.
///
/// **Cluster hash-tag note**: the stall counter key shares the queue's
/// `{chasqui:<queue>}` hash tag with the stream/dlq keys, so the per-quad
/// DEL stays on the same Cluster slot as the XDEL + XADD pair.
pub(crate) const REPLAY_DLQ_SCRIPT: &str = r#"
local dlq = KEYS[1]
local stream = KEYS[2]
local max_stream_len = ARGV[1]
local replayed = 0
-- Extract the {chasqui:<queue>} hash tag from KEYS[1] so we can
-- synthesize the per-job stall-counter key for the DEL without an
-- extra ARGV slot per quad.
local tag_start, tag_end = string.find(dlq, '{[^}]+}')
local tag = tag_start and string.sub(dlq, tag_start, tag_end) or ''
local i = 2
while i <= #ARGV do
  local dlq_id = ARGV[i]
  local payload = ARGV[i + 1]
  local name = ARGV[i + 2]
  local job_id = ARGV[i + 3]
  local deleted = redis.call('XDEL', dlq, dlq_id)
  if deleted == 1 then
    if name ~= nil and name ~= '' then
      redis.call('XADD', stream, 'MAXLEN', '~', max_stream_len, '*', 'd', payload, 'n', name)
    else
      redis.call('XADD', stream, 'MAXLEN', '~', max_stream_len, '*', 'd', payload)
    end
    -- Best-effort DEL of the slice-12 stall counter. A replayed
    -- entry that was originally stalled would otherwise inherit the
    -- previous streak (TTL is sliding on INCR; an old counter under
    -- TTL would let the new dispatch hit threshold prematurely).
    -- Empty job_id (reader-side DLQ routes with undecodable
    -- envelopes) and missing-key DEL both no-op.
    if tag ~= '' and job_id ~= nil and job_id ~= '' then
      redis.call('DEL', tag .. ':stalls:' .. job_id)
    end
    replayed = replayed + 1
  end
  i = i + 4
end
return replayed
"#;

/// Atomic XACKDEL + result-store. Acks-and-deletes the stream entry from the
/// consumer group's pending list, then — only if the ack actually removed the
/// entry — writes the handler's return bytes to `result_key` with TTL
/// `ttl_secs`. The XACKDEL gate prevents orphan result writes when a
/// concurrent CLAIM has already removed the entry. An empty `result_bytes`
/// payload is a deliberate skip-write signal: `JOB_OK_SCRIPT` is invoked
/// only when the worker opted in to result storage *and* the handler
/// returned a non-empty value.
///
/// **Maxmemory eviction safety (defense-in-depth).** Redis 8 lets scripts
/// run when `used_memory >= maxmemory` under `noeviction` by default; the
/// integration test in `tests/maxmemory.rs` exercises the exact path. The
/// `#!lua flags=allow-oom` shebang pins the contract explicitly so a
/// future Redis that flips the default cannot regress the behavior, and
/// `redis.pcall` swallows an OOM rejection on the SET if it ever fired.
/// XACKDEL always commits (it frees memory, never adds), and the result-
/// write is best-effort: if the SET is rejected by the eviction policy
/// the script returns `1` (ack succeeded) and the result is dropped —
/// consistent with the documented `Producer::get_result` contract that
/// `None` collapses "expired" and "never written."
///
/// KEYS[1] = stream_key, KEYS[2] = result_key, KEYS[3] = stall_counter_key
/// ARGV[1] = group, ARGV[2] = entry_id, ARGV[3] = result_bytes, ARGV[4] = ttl_secs
///
/// Returns the XACKDEL count (1 if the entry was acked, 0 otherwise).
///
/// **Slice 12 (stall-counter cleanup)**: KEYS[3] is the per-job stall
/// counter key. On a successful ack (`first == 1`) the script DELs it
/// unconditionally — a one-off stall followed by success should start
/// fresh counter on the next stall, not inherit the previous streak
/// under the sliding TTL. `redis.pcall` swallows OOM/maxmemory
/// rejections so the ack still commits. Non-existent counters DEL to
/// `0` and don't surface as errors — the common path (job never
/// stalled) costs one cheap DEL of a missing key.
pub(crate) const JOB_OK_SCRIPT: &str = r#"#!lua flags=allow-oom
local result = redis.call('XACKDEL', KEYS[1], ARGV[1], 'IDS', 1, ARGV[2])
local first
if type(result) == 'table' then
  first = tonumber(result[1])
else
  first = tonumber(result)
end
-- XACKDEL returns 1 (acked + removed), -1 (id not found), or 0 (not in
-- group); only 1 means we own this delivery — both other values mean a
-- concurrent CLAIM/replay won the race and SET is correctly skipped.
--
-- redis.pcall lets the SET fail silently under maxmemory-policy noeviction:
-- if Redis is OOM and rejects the write, we still return success so the
-- ack commits and the entry doesn't stay pending. The result is lost,
-- which matches the documented `None == expired-or-never-written`
-- contract on `Producer::get_result`.
if first == 1 then
  if #ARGV[3] > 0 then
    redis.pcall('SET', KEYS[2], ARGV[3], 'EX', tonumber(ARGV[4]))
  end
  -- Slice 12: clear the per-job stall counter on the ack-success
  -- path so a one-off stall doesn't leave a stale streak under TTL.
  redis.pcall('DEL', KEYS[3])
end
return first
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
/// **Encoded bytes shape (slice 3)**: `encode_delayed_member(name, msgpack)`.
/// The same prefixed bytes serve as both the ZSET member and the
/// side-index value, so a later `cancel_delayed` ZREMs by exact byte
/// match without decoding the prefix.
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
/// ARGV[7] = fire_count (N — number of (fire_at_ms, member) pairs that
///           follow). 0 is legal (Skip policy with no on-time fire — just
///           reschedule).
/// ARGV[8 .. 7+2*N] = interleaved `(fire_at_ms_i, member_i)` pairs, in
///                    chronological order. Each pair is two ARGV slots.
///
/// **Member format (slice 3)**: each `member_i` is the slice-3
/// length-prefixed delayed-ZSET shape
/// (`[u32_le name_len][name utf8][msgpack Job<T>]`). For on-time fires
/// (`fire_at_ms <= now_ms`) the script splits the prefix and emits
/// `XADD ... d <payload> [n <name>]`; for future fires the prefixed bytes
/// are written verbatim into the delayed ZSET so the promoter sees the
/// same shape as a producer-supplied delayed add.
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
  local member = ARGV[9 + i * 2]
  if limit > 0 then
    local fired_so_far = tonumber(redis.call('HGET', KEYS[4], 'fired')) or 0
    if fired_so_far >= limit then
      hit_limit = true
      break
    end
  end
  if fire_at_ms <= now_ms then
    -- Split the slice-3 length-prefixed member:
    --   [u32_le name_len][name utf8][msgpack payload]
    -- Bounds-check before string.byte so a malformed/short member can't abort
    -- the whole EVAL (string.byte on a <4-byte member returns nil; arithmetic
    -- on nil errors and the whole fire batch aborts). Mirror the Rust decoder
    -- (delayed_member.rs: decode_delayed_member). A poison member is dropped
    -- (it can't be dispatched), so it must NOT count toward fired / fired_now
    -- -- counting it would burn a fire slot against the repeatable limit and
    -- could prematurely retire the spec. The rest of the batch fires normally.
    local valid = false
    local name = ''
    local payload = nil
    if #member >= 4 then
      local b1, b2, b3, b4 = string.byte(member, 1, 4)
      local name_len = b1 + (b2 * 256) + (b3 * 65536) + (b4 * 16777216)
      if name_len <= #member - 4 then
        valid = true
        name = name_len > 0 and string.sub(member, 5, 4 + name_len) or ''
        payload = string.sub(member, 5 + name_len)
      end
    end
    if valid then
      if name ~= '' then
        redis.call('XADD', KEYS[1], 'MAXLEN', '~', max_stream_len, '*', 'd', payload, 'n', name)
      else
        redis.call('XADD', KEYS[1], 'MAXLEN', '~', max_stream_len, '*', 'd', payload)
      end
      redis.call('HINCRBY', KEYS[4], 'fired', 1)
      fired_now = fired_now + 1
    end
  else
    redis.call('ZADD', KEYS[2], fire_at_ms, member)
    redis.call('HINCRBY', KEYS[4], 'fired', 1)
    fired_now = fired_now + 1
  end
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

/// Slice 12 — stalled-job detector tick.
///
/// One round trip that, for a batch of pending-and-idle entries the
/// Rust caller already pre-decoded via XPENDING + pipelined XRANGE,
/// atomically:
///
/// 1. INCRs each entry's per-job stall counter
///    (`{chasqui:<queue>}:stalls:<job_id>`).
/// 2. EXPIREs the counter at `counter_ttl_secs` (sliding TTL — a job
///    that genuinely keeps stalling doesn't have its counter evicted
///    between ticks).
/// 3. If `n < max_stalled_attempts`: records the entry's INCR so the
///    Rust caller can emit `e=stalled` (best-effort).
/// 4. Else: tries to XACKDEL the entry from the consumer group's PEL.
///    The XACKDEL is the gate — if it returns 1, the entry is now ours
///    to relocate to the DLQ; the Rust caller fetches the payload and
///    sends onto `dlq_tx`. If it returns 0/-1, a concurrent
///    CLAIM/replay/manual-ack already removed the entry; we DEL the
///    counter (it's stale) and the threshold-hit slot stays empty so
///    no Rust-side relocate fires.
///
/// **Why the script doesn't XADD into the DLQ itself**: the DLQ write
/// reuses the existing `run_relocator` pipeline so the IDMP-XADD wire
/// shape, the `e=dlq` emit, the dlq_inflight backpressure budget, and
/// the per-element retry are all reused. One canonical write path
/// instead of two.
///
/// **Cluster correctness**: the script extracts the `{chasqui:<queue>}`
/// hash tag from `KEYS[1]` (the stream key) and synthesizes every
/// stall counter key from that tag. Every key the script touches
/// shares the same hash tag, so the whole Lua call routes to one
/// slot under Redis Cluster.
///
/// **Reply shape (must match `chasquimq::stalled::parse_stalled_reply`):**
/// ```text
/// {
///   scanned_count,                              -- ARGV[4] (echo)
///   [{arg_index, n}, ...],                      -- incremented (n < threshold)
///   [{arg_index, n}, ...],                      -- threshold_hits (gate held)
/// }
/// ```
/// `arg_index` is the 0-based index into the `(entry_id, job_id)` pair
/// array the caller sent in; Rust correlates back to the full
/// `(entry_id, job_id, name, payload)` triple it has.
///
/// KEYS[1] = stream_key
/// ARGV[1] = group
/// ARGV[2] = max_stalled_attempts (counter ceiling)
/// ARGV[3] = counter_ttl_secs (sliding EXPIRE)
/// ARGV[4] = entry_count (N)
/// ARGV[5..4+2N] = interleaved pairs of (entry_id, job_id)
pub(crate) const STALLED_SCAN_SCRIPT: &str = r#"
local tag_start, tag_end = string.find(KEYS[1], '{[^}]+}')
if not tag_start then
  return redis.error_reply('stream_key missing {chasqui:<queue>} hash tag')
end
local tag = string.sub(KEYS[1], tag_start, tag_end)
local group = ARGV[1]
local max_attempts = tonumber(ARGV[2])
local ttl_secs = tonumber(ARGV[3])
local n_entries = tonumber(ARGV[4])

local incremented = {}
local threshold_hits = {}
local i = 0
while i < n_entries do
  local entry_id = ARGV[5 + i * 2]
  local job_id = ARGV[6 + i * 2]
  local stall_key = tag .. ':stalls:' .. job_id
  local n = redis.call('INCR', stall_key)
  redis.call('EXPIRE', stall_key, ttl_secs)

  if n < max_attempts then
    incremented[#incremented + 1] = {i, n}
  else
    -- Threshold hit: try to gate the entry out of the PEL. XACKDEL
    -- returns 1 (acked + removed), -1 (id not found), or 0 (not in
    -- group). Only `1` means we own this delivery and the DLQ
    -- relocate should fire; anything else is a "gate lost" and we
    -- DEL the counter (it's stale) and skip the threshold-hit slot.
    local ackdel = redis.call('XACKDEL', KEYS[1], group, 'IDS', 1, entry_id)
    local first
    if type(ackdel) == 'table' then first = tonumber(ackdel[1])
    else first = tonumber(ackdel) end
    redis.call('DEL', stall_key)
    if first == 1 then
      threshold_hits[#threshold_hits + 1] = {i, n}
    end
  end
  i = i + 1
end

return {n_entries, incremented, threshold_hits}
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

/// Atomically remove a single stream entry by its **stream entry id**, both
/// acking it out of the consumer group's pending list and deleting it from
/// the stream. Used by `Producer::remove` for the waiting / active branch.
///
/// The caller has already located the entry id via a bounded `XRANGE` scan
/// (the job id lives inside the msgpack envelope, not in the entry id, so a
/// pure-Lua match is impossible). This script just does the two-step delete
/// atomically so a concurrent claim / replay can never observe the entry
/// half-removed.
///
/// `XACKDEL` does ack + delete in one Redis primitive. It returns `1`
/// (acked and removed), `-1` (id not in the stream), or `0` (in the stream
/// but not pending in this group — i.e. a *waiting* entry). For a waiting
/// entry `XACKDEL` returns `0` and does NOT delete it, so we follow up with
/// an unconditional `XDEL` that removes the waiting entry regardless of PEL
/// membership. The final return is `1` when the entry existed and is now
/// gone (either path), `0` when it was already absent — the idempotent,
/// report-what-actually-happened contract `Producer::remove` documents.
///
/// KEYS[1] = stream_key
/// ARGV[1] = group, ARGV[2] = entry_id
///
/// Returns 1 if the entry was removed, 0 if it was already gone.
pub(crate) const REMOVE_STREAM_ENTRY_SCRIPT: &str = r#"
local ackdel = redis.call('XACKDEL', KEYS[1], ARGV[1], 'IDS', 1, ARGV[2])
local first
if type(ackdel) == 'table' then
  first = tonumber(ackdel[1])
else
  first = tonumber(ackdel)
end
if first == 1 then
  return 1
end
-- first is 0 (waiting, not pending) or -1 (already gone). XDEL removes a
-- waiting entry; on an already-gone id it returns 0 — exactly the
-- idempotent answer we want.
local deleted = redis.call('XDEL', KEYS[1], ARGV[2])
return deleted
"#;

/// Drain the *waiting* entries from a stream — every entry NOT currently in
/// the consumer group's pending list — without touching in-flight (pending)
/// jobs. Used by `Producer::drain`.
///
/// A ChasquiMQ stream mixes waiting and active entries on the one Redis
/// Stream (a delivered-but-unacked entry stays in the stream and is also
/// referenced by the group's PEL). BullMQ keeps its wait list as a separate
/// Redis list, so its drain is a single `DEL`; ours has to subtract the
/// pending set first.
///
/// The script walks `XRANGE - + COUNT <limit>`, builds a lookup of the
/// group's pending entry ids from `XPENDING ... <limit>`, and `XDEL`s every
/// scanned entry that is not pending. `limit` bounds both the scan and the
/// pending fetch so a single invocation can never block Redis on an
/// unbounded stream; the caller loops until a pass deletes nothing.
///
/// A fresh queue with no consumer group yet makes `XPENDING` raise
/// `NOGROUP`; `redis.pcall` swallows that and the pending set is simply
/// empty (every entry counts as waiting), which is correct.
///
/// KEYS[1] = stream_key
/// ARGV[1] = group, ARGV[2] = limit
///
/// Returns the count of entries deleted in this pass.
pub(crate) const DRAIN_STREAM_SCRIPT: &str = r#"
local limit = tonumber(ARGV[2])
if limit <= 0 then
  return 0
end
local entries = redis.call('XRANGE', KEYS[1], '-', '+', 'COUNT', limit)
if #entries == 0 then
  return 0
end
local pending = {}
local pend = redis.pcall('XPENDING', KEYS[1], ARGV[1], '-', '+', limit)
if type(pend) == 'table' and pend.err == nil then
  for _, p in ipairs(pend) do
    pending[p[1]] = true
  end
end
local deleted = 0
for _, entry in ipairs(entries) do
  local id = entry[1]
  if not pending[id] then
    deleted = deleted + redis.call('XDEL', KEYS[1], id)
  end
end
return deleted
"#;

/// Bulk-delete a known set of stream entry ids by **stream entry id**, both
/// acking them out of the consumer group's PEL and deleting them from the
/// stream. Used by `Producer::clean` for the waiting / failed (DLQ) states
/// after the caller has age-filtered and capped the id list Rust-side.
///
/// Each id goes through the same `XACKDEL`-then-`XDEL` fallback as
/// `REMOVE_STREAM_ENTRY_SCRIPT`: `XACKDEL` removes a pending entry, the
/// `XDEL` fallback removes a waiting one. Doing the whole batch in one Lua
/// invocation keeps `clean` to a single round trip instead of one per id.
///
/// KEYS[1] = stream_key (or dlq_key — both are streams)
/// ARGV[1] = group, ARGV[2..] = entry ids to delete
///
/// Returns the count of entries actually removed.
pub(crate) const CLEAN_STREAM_SCRIPT: &str = r#"
local removed = 0
local i = 2
while i <= #ARGV do
  local id = ARGV[i]
  local ackdel = redis.call('XACKDEL', KEYS[1], ARGV[1], 'IDS', 1, id)
  local first
  if type(ackdel) == 'table' then
    first = tonumber(ackdel[1])
  else
    first = tonumber(ackdel)
  end
  if first == 1 then
    removed = removed + 1
  else
    removed = removed + redis.call('XDEL', KEYS[1], id)
  end
  i = i + 1
end
return removed
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

/// EVALSHA argument vector for [`REPLAY_DLQ_SCRIPT`]. Slice 12 widened
/// the per-entry tuple from triples `(dlq_id, payload, name)` to **quads**
/// `(dlq_id, payload, name, job_id)` so the script can DEL the per-job
/// stall counter (`{chasqui:<queue>}:stalls:<job_id>`). The script and
/// this builder roll together — no rolling-deploy skew.
pub(crate) fn evalsha_replay_args(
    sha: &str,
    dlq_key: &str,
    stream_key: &str,
    max_stream_len: u64,
    quads: &[(String, Bytes, String, String)],
) -> Vec<Value> {
    let mut args: Vec<Value> = Vec::with_capacity(5 + quads.len() * 4);
    args.push(Value::from(sha));
    args.push(Value::from(2_i64));
    args.push(Value::from(dlq_key));
    args.push(Value::from(stream_key));
    args.push(Value::from(max_stream_len as i64));
    for (id, bytes, name, job_id) in quads {
        args.push(Value::from(id.as_str()));
        args.push(Value::Bytes(bytes.clone()));
        args.push(Value::from(name.as_str()));
        args.push(Value::from(job_id.as_str()));
    }
    args
}

/// EVAL fallback for [`REPLAY_DLQ_SCRIPT`]. See [`evalsha_replay_args`]
/// for the slice-12 ARGV-shape change.
pub(crate) fn eval_replay_args(
    script: &str,
    dlq_key: &str,
    stream_key: &str,
    max_stream_len: u64,
    quads: &[(String, Bytes, String, String)],
) -> Vec<Value> {
    let mut args: Vec<Value> = Vec::with_capacity(5 + quads.len() * 4);
    args.push(Value::from(script));
    args.push(Value::from(2_i64));
    args.push(Value::from(dlq_key));
    args.push(Value::from(stream_key));
    args.push(Value::from(max_stream_len as i64));
    for (id, bytes, name, job_id) in quads {
        args.push(Value::from(id.as_str()));
        args.push(Value::Bytes(bytes.clone()));
        args.push(Value::from(name.as_str()));
        args.push(Value::from(job_id.as_str()));
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

/// EVALSHA argument vector for [`RELOCATE_DLQ_SCRIPT`]. `name` and `detail`
/// are passed as empty strings when absent; the script omits the matching
/// field so a relocated entry's shape matches the DLQ entry contract
/// documented on [`RELOCATE_DLQ_SCRIPT`] exactly.
#[allow(clippy::too_many_arguments)]
pub(crate) fn evalsha_relocate_dlq_args(
    sha: &str,
    stream_key: &str,
    dlq_key: &str,
    group: &str,
    entry_id: &str,
    producer_id: &str,
    source_id: &str,
    payload: Bytes,
    reason: &str,
    max_stream_len: u64,
    name: &str,
    detail: Option<&str>,
) -> Vec<Value> {
    vec![
        Value::from(sha),
        Value::from(2_i64),
        Value::from(stream_key),
        Value::from(dlq_key),
        Value::from(group),
        Value::from(entry_id),
        Value::from(producer_id),
        Value::from(source_id),
        Value::Bytes(payload),
        Value::from(reason),
        Value::from(max_stream_len as i64),
        Value::from(name),
        Value::from(detail.unwrap_or("")),
    ]
}

/// EVALSHA argument vector for [`RELOCATE_DLQ_PRE_ACKED_SCRIPT`].
/// Used by the stalled-detector's relocate path where the source entry
/// is already XACKDEL'd out of the PEL.
#[allow(clippy::too_many_arguments)]
pub(crate) fn evalsha_relocate_dlq_pre_acked_args(
    sha: &str,
    dlq_key: &str,
    producer_id: &str,
    source_id: &str,
    payload: Bytes,
    reason: &str,
    max_stream_len: u64,
    name: &str,
    detail: Option<&str>,
) -> Vec<Value> {
    vec![
        Value::from(sha),
        Value::from(1_i64),
        Value::from(dlq_key),
        Value::from(producer_id),
        Value::from(source_id),
        Value::Bytes(payload),
        Value::from(reason),
        Value::from(max_stream_len as i64),
        Value::from(name),
        Value::from(detail.unwrap_or("")),
    ]
}

/// EVAL fallback for [`RELOCATE_DLQ_PRE_ACKED_SCRIPT`].
#[allow(clippy::too_many_arguments)]
pub(crate) fn eval_relocate_dlq_pre_acked_args(
    script: &str,
    dlq_key: &str,
    producer_id: &str,
    source_id: &str,
    payload: Bytes,
    reason: &str,
    max_stream_len: u64,
    name: &str,
    detail: Option<&str>,
) -> Vec<Value> {
    vec![
        Value::from(script),
        Value::from(1_i64),
        Value::from(dlq_key),
        Value::from(producer_id),
        Value::from(source_id),
        Value::Bytes(payload),
        Value::from(reason),
        Value::from(max_stream_len as i64),
        Value::from(name),
        Value::from(detail.unwrap_or("")),
    ]
}

/// EVAL fallback for [`RELOCATE_DLQ_SCRIPT`] when the cached SHA is unknown
/// to the server (`NOSCRIPT`). Identical to [`evalsha_relocate_dlq_args`]
/// apart from passing the script body in place of the SHA — the same
/// two-builder convention used by the retry and replay paths.
#[allow(clippy::too_many_arguments)]
pub(crate) fn eval_relocate_dlq_args(
    script: &str,
    stream_key: &str,
    dlq_key: &str,
    group: &str,
    entry_id: &str,
    producer_id: &str,
    source_id: &str,
    payload: Bytes,
    reason: &str,
    max_stream_len: u64,
    name: &str,
    detail: Option<&str>,
) -> Vec<Value> {
    vec![
        Value::from(script),
        Value::from(2_i64),
        Value::from(stream_key),
        Value::from(dlq_key),
        Value::from(group),
        Value::from(entry_id),
        Value::from(producer_id),
        Value::from(source_id),
        Value::Bytes(payload),
        Value::from(reason),
        Value::from(max_stream_len as i64),
        Value::from(name),
        Value::from(detail.unwrap_or("")),
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

/// EVALSHA argument vector for [`JOB_OK_SCRIPT`]. Slice 12 added
/// `stall_counter_key` as KEYS[3] so the script can DEL the per-job
/// stall counter on a successful ack.
#[allow(clippy::too_many_arguments)]
pub(crate) fn evalsha_job_ok_args(
    sha: &str,
    stream_key: &str,
    result_key: &str,
    stall_counter_key: &str,
    group: &str,
    entry_id: &str,
    result_bytes: Bytes,
    ttl_secs: u64,
) -> Vec<Value> {
    vec![
        Value::from(sha),
        Value::from(3_i64),
        Value::from(stream_key),
        Value::from(result_key),
        Value::from(stall_counter_key),
        Value::from(group),
        Value::from(entry_id),
        Value::Bytes(result_bytes),
        Value::from(ttl_secs as i64),
    ]
}

/// EVAL fallback for [`JOB_OK_SCRIPT`]. See [`evalsha_job_ok_args`] for
/// the slice-12 KEYS layout change.
#[allow(clippy::too_many_arguments)]
pub(crate) fn eval_job_ok_args(
    script: &str,
    stream_key: &str,
    result_key: &str,
    stall_counter_key: &str,
    group: &str,
    entry_id: &str,
    result_bytes: Bytes,
    ttl_secs: u64,
) -> Vec<Value> {
    vec![
        Value::from(script),
        Value::from(3_i64),
        Value::from(stream_key),
        Value::from(result_key),
        Value::from(stall_counter_key),
        Value::from(group),
        Value::from(entry_id),
        Value::Bytes(result_bytes),
        Value::from(ttl_secs as i64),
    ]
}

/// `XPENDING <stream> <group> IDLE <idle_ms> - + <count>` extended-form.
/// Used by the stalled-detector to find PEL entries that have sat idle
/// past the detector's threshold. `NOGROUP` (fresh queue, no consumer
/// group yet) surfaces as a `fred::error::Error` the caller treats as
/// "empty scan, sleep tick" — same posture as `DRAIN_STREAM_SCRIPT`'s
/// `redis.pcall(XPENDING)` branch.
pub(crate) fn xpending_idle_args(
    stream_key: &str,
    group: &str,
    idle_ms: u64,
    count: u64,
) -> Vec<Value> {
    vec![
        Value::from(stream_key),
        Value::from(group),
        Value::from("IDLE"),
        Value::from(idle_ms as i64),
        Value::from("-"),
        Value::from("+"),
        Value::from(count as i64),
    ]
}

/// `XRANGE <stream> <id> <id>` — single-entry fetch by stream entry id.
/// Used by the stalled-detector for the per-entry envelope decode pass
/// (pipelined across the batch).
pub(crate) fn xrange_id_args(stream_key: &str, entry_id: &str) -> Vec<Value> {
    vec![
        Value::from(stream_key),
        Value::from(entry_id),
        Value::from(entry_id),
    ]
}

/// EVALSHA argument vector for [`STALLED_SCAN_SCRIPT`]. `pairs` are the
/// `(entry_id, job_id)` tuples the Rust caller already pre-decoded from
/// the XPENDING + pipelined XRANGE round.
pub(crate) fn evalsha_stalled_scan_args(
    sha: &str,
    stream_key: &str,
    group: &str,
    max_stalled_attempts: u32,
    counter_ttl_secs: u64,
    pairs: &[(String, String)],
) -> Vec<Value> {
    let mut args: Vec<Value> = Vec::with_capacity(6 + pairs.len() * 2);
    args.push(Value::from(sha));
    args.push(Value::from(1_i64));
    args.push(Value::from(stream_key));
    args.push(Value::from(group));
    args.push(Value::from(max_stalled_attempts as i64));
    args.push(Value::from(counter_ttl_secs as i64));
    args.push(Value::from(pairs.len() as i64));
    for (entry_id, job_id) in pairs {
        args.push(Value::from(entry_id.as_str()));
        args.push(Value::from(job_id.as_str()));
    }
    args
}

/// EVAL fallback for [`STALLED_SCAN_SCRIPT`].
pub(crate) fn eval_stalled_scan_args(
    script: &str,
    stream_key: &str,
    group: &str,
    max_stalled_attempts: u32,
    counter_ttl_secs: u64,
    pairs: &[(String, String)],
) -> Vec<Value> {
    let mut args: Vec<Value> = Vec::with_capacity(6 + pairs.len() * 2);
    args.push(Value::from(script));
    args.push(Value::from(1_i64));
    args.push(Value::from(stream_key));
    args.push(Value::from(group));
    args.push(Value::from(max_stalled_attempts as i64));
    args.push(Value::from(counter_ttl_secs as i64));
    args.push(Value::from(pairs.len() as i64));
    for (entry_id, job_id) in pairs {
        args.push(Value::from(entry_id.as_str()));
        args.push(Value::from(job_id.as_str()));
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

/// EVALSHA argument vector for [`REMOVE_STREAM_ENTRY_SCRIPT`].
pub(crate) fn evalsha_remove_stream_entry_args(
    sha: &str,
    stream_key: &str,
    group: &str,
    entry_id: &str,
) -> Vec<Value> {
    vec![
        Value::from(sha),
        Value::from(1_i64),
        Value::from(stream_key),
        Value::from(group),
        Value::from(entry_id),
    ]
}

/// EVAL fallback argument vector for [`REMOVE_STREAM_ENTRY_SCRIPT`].
pub(crate) fn eval_remove_stream_entry_args(
    script: &str,
    stream_key: &str,
    group: &str,
    entry_id: &str,
) -> Vec<Value> {
    vec![
        Value::from(script),
        Value::from(1_i64),
        Value::from(stream_key),
        Value::from(group),
        Value::from(entry_id),
    ]
}

/// EVALSHA argument vector for [`DRAIN_STREAM_SCRIPT`].
pub(crate) fn evalsha_drain_stream_args(
    sha: &str,
    stream_key: &str,
    group: &str,
    limit: u64,
) -> Vec<Value> {
    vec![
        Value::from(sha),
        Value::from(1_i64),
        Value::from(stream_key),
        Value::from(group),
        Value::from(limit as i64),
    ]
}

/// EVAL fallback argument vector for [`DRAIN_STREAM_SCRIPT`].
pub(crate) fn eval_drain_stream_args(
    script: &str,
    stream_key: &str,
    group: &str,
    limit: u64,
) -> Vec<Value> {
    vec![
        Value::from(script),
        Value::from(1_i64),
        Value::from(stream_key),
        Value::from(group),
        Value::from(limit as i64),
    ]
}

/// EVALSHA argument vector for [`CLEAN_STREAM_SCRIPT`]. `entry_ids` are
/// the stream entry ids the caller already age-filtered and capped.
pub(crate) fn evalsha_clean_stream_args(
    sha: &str,
    stream_key: &str,
    group: &str,
    entry_ids: &[String],
) -> Vec<Value> {
    let mut args: Vec<Value> = Vec::with_capacity(4 + entry_ids.len());
    args.push(Value::from(sha));
    args.push(Value::from(1_i64));
    args.push(Value::from(stream_key));
    args.push(Value::from(group));
    for id in entry_ids {
        args.push(Value::from(id.as_str()));
    }
    args
}

/// EVAL fallback argument vector for [`CLEAN_STREAM_SCRIPT`].
pub(crate) fn eval_clean_stream_args(
    script: &str,
    stream_key: &str,
    group: &str,
    entry_ids: &[String],
) -> Vec<Value> {
    let mut args: Vec<Value> = Vec::with_capacity(4 + entry_ids.len());
    args.push(Value::from(script));
    args.push(Value::from(1_i64));
    args.push(Value::from(stream_key));
    args.push(Value::from(group));
    for id in entry_ids {
        args.push(Value::from(id.as_str()));
    }
    args
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Structural regression guard. Both delayed-member decoders previously
    /// did `string.byte(member, 1, 4)` immediately followed by `name_len`
    /// arithmetic with no bounds check; a member shorter than the 4-byte
    /// prefix made `string.byte` return nil, the arithmetic erred on nil, and
    /// the whole EVAL aborted before any per-member cleanup ran — permanently
    /// wedging promotion. The end-to-end proof lives in
    /// `tests/delayed.rs::poison_member_does_not_wedge_promotion` (live
    /// Redis). This cheap unit check makes sure a future edit can't silently
    /// strip the guard back out of either script.
    ///
    /// The guard mirrors `delayed_member::decode_delayed_member`: reject a
    /// member shorter than the prefix (`#member < 4` / `#member >= 4`) and
    /// reject a declared name length that overruns the buffer
    /// (`name_len > #member - 4` / `name_len <= #member - 4`).
    fn asserts_bounds_guard(script: &str, what: &str) {
        assert!(
            script.contains("string.byte(member, 1, 4)"),
            "{what}: expected the length-prefixed member decode to still exist"
        );
        let has_short_guard = script.contains("#member < 4") || script.contains("#member >= 4");
        assert!(
            has_short_guard,
            "{what}: missing the short-member bounds guard (#member vs 4) — \
             a <4-byte member will abort the EVAL and wedge promotion"
        );
        let has_overrun_guard =
            script.contains("name_len > #member - 4") || script.contains("name_len <= #member - 4");
        assert!(
            has_overrun_guard,
            "{what}: missing the oversized-name_len guard (name_len vs \
             #member - 4) — a member whose declared name length overruns \
             the buffer will mis-slice the payload"
        );
    }

    #[test]
    fn promote_script_guards_malformed_members() {
        asserts_bounds_guard(PROMOTE_SCRIPT, "PROMOTE_SCRIPT");
    }

    #[test]
    fn schedule_repeatable_script_guards_malformed_members() {
        asserts_bounds_guard(SCHEDULE_REPEATABLE_SCRIPT, "SCHEDULE_REPEATABLE_SCRIPT");
    }
}
