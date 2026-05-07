"""Cross-shim result-backend verifier (Python side).

Reads job IDs one-per-line from JOB_IDS_FILE, calls
``Queue.get_job_result`` for each, and asserts the returned value
deep-equals ``json.loads(EXPECT_RESULT)``. Exits 0 on full match,
else 1.

Used after the worker has drained, so the engine's ok-result writer
has already persisted each result key.

Env vars:
  QUEUE         — required, queue name.
  JOB_IDS_FILE  — required, path written by the producer.
  EXPECT_RESULT — required, JSON-encoded expected handler return value.
  TIMEOUT_SECS  — optional, polling deadline per id (default 10).
  REDIS_URL     — optional.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys

from chasquimq import Queue


async def _wait_for_result(queue: Queue, job_id: str, timeout_s: float):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while loop.time() < deadline:
        result = await queue.get_job_result(job_id)
        if result is not None:
            return result
        await asyncio.sleep(0.05)
    return None


async def main() -> int:
    queue_name = os.environ["QUEUE"]
    ids_file = os.environ["JOB_IDS_FILE"]
    expect_raw = os.environ["EXPECT_RESULT"]
    timeout_secs = float(os.environ.get("TIMEOUT_SECS", "10"))
    redis_url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")

    expect = json.loads(expect_raw)

    with open(ids_file, "r", encoding="utf-8") as fh:
        ids = [line.strip() for line in fh if line.strip()]
    if not ids:
        print(f"[py-verify] ERROR: {ids_file!r} contains no ids", file=sys.stderr)
        return 1

    queue = Queue(queue_name, redis_url=redis_url)
    mismatches: list[str] = []
    misses: list[str] = []
    try:
        for jid in ids:
            got = await _wait_for_result(queue, jid, timeout_secs)
            if got is None:
                misses.append(jid)
                continue
            if got != expect:
                mismatches.append(f"{jid}: got {got!r} want {expect!r}")
    finally:
        await queue.close()

    if misses:
        for jid in misses[:5]:
            print(f"[py-verify] ERROR: no result for id={jid}", file=sys.stderr)
        if len(misses) > 5:
            print(
                f"[py-verify] ... and {len(misses) - 5} more missing",
                file=sys.stderr,
            )
        return 1
    if mismatches:
        for m in mismatches[:5]:
            print(f"[py-verify] ERROR: {m}", file=sys.stderr)
        return 1

    print(
        f"[py-verify] OK — {len(ids)} results round-tripped, "
        f"expect={expect!r}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
