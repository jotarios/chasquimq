"""Cross-shim wire-format test fixture: Python worker.

Consumes COUNT jobs from QUEUE. Each payload must be a dict with
integer ``i`` in [0, COUNT) and string ``tag`` matching EXPECT_TAG.
Exits 0 on full distinct-id coverage within TIMEOUT_SECS, else 1.

The worker leaves the engine default ``delayed_enabled=True`` regardless
of the producer's MODE — a worker should drain whatever's available, so
gating the embedded promoter on a producer-side env var creates a fragile
two-process coupling (forget MODE on the worker side and the ZSET sits
forever). This mirrors the Node fixture's approach.

Env vars:
  QUEUE, COUNT — required.
  EXPECT_JOB_NAME  — optional. When non-empty, the handler asserts
                      ``job.name == EXPECT_JOB_NAME`` for every job, so
                      a regression that drops `name` on either shim's
                      wire path is caught here.
  STORE_RESULT  — optional. When ``"1"`` the worker enables
                   ``store_results=True`` so the engine persists the
                   handler's return value at
                   ``{chasqui:<QUEUE>}:result:<jobId>`` for the verifier
                   to read back. Default off.
  RESULT_VALUE  — optional, JSON-encoded. When set, the handler returns
                   ``json.loads(RESULT_VALUE)`` instead of ``None``. The
                   verifier asserts this value round-trips through the
                   shim's msgpack wire format.
  EXPECT_TAG, TIMEOUT_SECS, REDIS_URL — optional.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys

from chasquimq import Job, Worker


async def main() -> int:
    queue_name = os.environ["QUEUE"]
    count = int(os.environ["COUNT"])
    expect_tag = os.environ.get("EXPECT_TAG", "py")
    expect_job_name = os.environ.get("EXPECT_JOB_NAME", "")
    timeout_secs = float(os.environ.get("TIMEOUT_SECS", "30"))
    redis_url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")
    store_results = os.environ.get("STORE_RESULT", "") == "1"
    result_value_raw = os.environ.get("RESULT_VALUE", "")
    result_value = json.loads(result_value_raw) if result_value_raw else None

    seen: set[int] = set()
    done = asyncio.Event()
    errors: list[str] = []

    async def handler(job: Job):
        data = job.data
        if not isinstance(data, dict):
            errors.append(f"payload not a dict: {data!r}")
            done.set()
            return None
        i = data.get("i")
        tag = data.get("tag")
        if not isinstance(i, int) or i < 0 or i >= count:
            errors.append(f"i out of range: {i!r}")
            done.set()
            return None
        if tag != expect_tag:
            errors.append(f"tag mismatch: got {tag!r}, want {expect_tag!r}")
            done.set()
            return None
        if expect_job_name and job.name != expect_job_name:
            errors.append(
                f"name mismatch: got {job.name!r}, want {expect_job_name!r}"
            )
            done.set()
            return None
        seen.add(i)
        if len(seen) >= count:
            done.set()
        return result_value

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=8,
        max_attempts=1,
        read_block_ms=200,
        run_scheduler=False,
        store_results=store_results,
    )

    run_task = asyncio.create_task(worker.run())
    try:
        try:
            await asyncio.wait_for(done.wait(), timeout=timeout_secs)
        except asyncio.TimeoutError:
            print(
                f"[py-worker] TIMEOUT after {timeout_secs}s — saw {len(seen)}/{count}",
                file=sys.stderr,
            )
            return 1
    finally:
        await worker.close()
        try:
            await asyncio.wait_for(run_task, timeout=5.0)
        except asyncio.TimeoutError:
            pass
        await asyncio.sleep(0)

    if errors:
        for e in errors:
            print(f"[py-worker] ERROR: {e}", file=sys.stderr)
        return 1
    if len(seen) != count:
        print(
            f"[py-worker] coverage gap: saw {len(seen)}/{count} (missing: "
            f"{sorted(set(range(count)) - seen)[:10]}...)",
            file=sys.stderr,
        )
        return 1

    print(
        f"[py-worker] OK — drained {count} distinct jobs with "
        f"tag={expect_tag!r} name={expect_job_name!r}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
