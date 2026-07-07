"""Cross-shim rate-limiter verifier (Python side).

Runs a rate-LIMITED worker against QUEUE for a fixed observation window
and asserts the number of jobs it (plus any other worker on the queue)
admits stays BOUNDED by the shared per-queue token bucket. Exits 0 when
the observed completion count is within the expected ceiling, else 1.

The strongest cross-shim proof: run this Python limited worker alongside a
Node limited worker (worker.ts with LIMITER_MAX) on the SAME queue. Both
FFI paths land on the same engine ``ConsumerConfig.rate_limit`` and draw
from ONE Redis bucket, so the COMBINED completion count over the window is
bounded by roughly the single limiter's cap — not 2x. This verifier reads
the total drained across the queue (via ``Queue.get_job_counts`` completed
+ its own local tally) but the authoritative assertion is on how many jobs
were removed from the stream within the window.

Env vars:
  QUEUE               — required, queue name (already seeded by a producer).
  LIMITER_MAX         — required, tokens per window (the shared bucket cap).
  LIMITER_DURATION_MS — required, window length in ms.
  OBSERVE_WINDOWS     — optional, number of windows to observe (default 2).
  EXPECT_MAX          — required, upper bound on total jobs completed
                        (queue-wide) within the observation window. Set
                        generously to allow the cold-start full-bucket burst
                        plus per-window refill.
  EXPECT_MIN          — optional, lower bound (default 1) to prove some jobs
                        were admitted.
  REDIS_URL           — optional.
"""

from __future__ import annotations

import asyncio
import os
import sys

from chasquimq import Queue, Worker


async def main() -> int:
    queue_name = os.environ["QUEUE"]
    limiter_max = int(os.environ["LIMITER_MAX"])
    limiter_duration_ms = int(os.environ["LIMITER_DURATION_MS"])
    observe_windows = float(os.environ.get("OBSERVE_WINDOWS", "2"))
    expect_max = int(os.environ["EXPECT_MAX"])
    expect_min = int(os.environ.get("EXPECT_MIN", "1"))
    redis_url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")

    # Count what THIS worker completes locally. The queue-wide total is
    # (this worker) + (any concurrent worker on the same queue). We read the
    # queue-wide drained count from the stream length delta instead, below.
    local_completed = 0

    async def handler(_job) -> None:
        nonlocal local_completed
        local_completed += 1

    # Snapshot the pending stream depth before observing so we can compute
    # queue-wide drained = before - after (bounded by the shared limiter,
    # regardless of how many workers drain it).
    counts_queue = Queue(queue_name, redis_url=redis_url)
    try:
        before_counts = await counts_queue.get_job_counts()
    finally:
        await counts_queue.close()
    before_waiting = int(before_counts.get("waiting", 0)) + int(
        before_counts.get("active", 0)
    )

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=8,
        max_attempts=1,
        read_block_ms=100,
        delayed_enabled=False,
        run_scheduler=False,
        stalled_detector_enabled=False,
        rate_limit_max=limiter_max,
        rate_limit_duration_ms=limiter_duration_ms,
    )
    run_task = asyncio.create_task(worker.run())
    try:
        await asyncio.sleep(observe_windows * (limiter_duration_ms / 1000.0))
    finally:
        await worker.close()
        try:
            await asyncio.wait_for(run_task, timeout=5.0)
        except (asyncio.TimeoutError, BaseException):
            pass

    after_queue = Queue(queue_name, redis_url=redis_url)
    try:
        after_counts = await after_queue.get_job_counts()
    finally:
        await after_queue.close()
    after_waiting = int(after_counts.get("waiting", 0)) + int(
        after_counts.get("active", 0)
    )

    # Queue-wide jobs drained during the observation window. This is the
    # number the SHARED bucket bounds; it counts what every worker on the
    # queue (Node + Python) admitted, not just this process.
    drained_queue_wide = max(before_waiting - after_waiting, 0)

    print(
        f"[py-verify-rl] local_completed={local_completed} "
        f"drained_queue_wide={drained_queue_wide} "
        f"(before_waiting={before_waiting} after_waiting={after_waiting}) "
        f"limiter={limiter_max}/{limiter_duration_ms}ms "
        f"observe_windows={observe_windows} "
        f"expect_min={expect_min} expect_max={expect_max}"
    )

    if drained_queue_wide < expect_min:
        print(
            f"[py-verify-rl] ERROR: too few drained ({drained_queue_wide} < "
            f"{expect_min}); limiter may have stalled the queue entirely",
            file=sys.stderr,
        )
        return 1
    if drained_queue_wide > expect_max:
        print(
            f"[py-verify-rl] ERROR: limiter breached — {drained_queue_wide} "
            f"jobs drained queue-wide within the window (> {expect_max}). If a "
            f"Node worker ran concurrently, the two FFI paths are NOT sharing "
            f"one bucket.",
            file=sys.stderr,
        )
        return 1

    print(
        f"[py-verify-rl] OK — {drained_queue_wide} jobs drained queue-wide, "
        f"within [{expect_min}, {expect_max}] for shared limiter "
        f"{limiter_max}/{limiter_duration_ms}ms"
    )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
