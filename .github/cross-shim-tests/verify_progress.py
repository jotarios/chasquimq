"""Cross-shim progress + log verifier (Python side).

Reads job IDs one-per-line from JOB_IDS_FILE, then for each id asserts:

  - ``Queue.get_job(id).progress == EXPECT_PROGRESS``
  - ``Queue.get_job_logs(id) == (EXPECT_LOGS, len(EXPECT_LOGS))``

so a regression that breaks the per-job progress STRING or log Stream
wire format on either shim surfaces here. Exits 0 on full match, else 1.

Pair with a worker run that set ``EMIT_PROGRESS=1`` and
``STORE_RESULT=1`` (the latter keeps the completed job discoverable by
the introspector's result-key probe).

Env vars:
  QUEUE           — required, queue name.
  JOB_IDS_FILE    — required, path written by the producer.
  EXPECT_PROGRESS — optional, default ``75``.
  EXPECT_LOGS     — optional, JSON-encoded list[str], default
                     ``["step 1","step 2"]``.
  TIMEOUT_SECS    — optional, polling deadline per id (default 10).
  REDIS_URL       — optional.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys

from chasquimq import Queue


async def _wait_for_progress(
    queue: Queue, job_id: str, expect: int, timeout_s: float
) -> int | None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while loop.time() < deadline:
        job = await queue.get_job(job_id)
        if job is not None and job.progress == expect:
            return job.progress
        await asyncio.sleep(0.05)
    job = await queue.get_job(job_id)
    return None if job is None else job.progress


async def _wait_for_logs(
    queue: Queue, job_id: str, expect_count: int, timeout_s: float
) -> tuple[list[str], int]:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while loop.time() < deadline:
        logs, count = await queue.get_job_logs(job_id)
        if count >= expect_count:
            return logs, count
        await asyncio.sleep(0.05)
    return await queue.get_job_logs(job_id)


async def main() -> int:
    queue_name = os.environ["QUEUE"]
    ids_file = os.environ["JOB_IDS_FILE"]
    expect_progress = int(os.environ.get("EXPECT_PROGRESS", "75"))
    expect_logs_raw = os.environ.get(
        "EXPECT_LOGS", '["step 1","step 2"]'
    )
    expect_logs = json.loads(expect_logs_raw)
    timeout_secs = float(os.environ.get("TIMEOUT_SECS", "10"))
    redis_url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")

    if not isinstance(expect_logs, list) or not all(
        isinstance(s, str) for s in expect_logs
    ):
        print(
            "[py-verify-progress] ERROR: EXPECT_LOGS must be a JSON list of strings",
            file=sys.stderr,
        )
        return 1

    with open(ids_file, "r", encoding="utf-8") as fh:
        ids = [line.strip() for line in fh if line.strip()]
    if not ids:
        print(
            f"[py-verify-progress] ERROR: {ids_file!r} contains no ids",
            file=sys.stderr,
        )
        return 1

    queue = Queue(queue_name, redis_url=redis_url)
    progress_errors: list[str] = []
    log_errors: list[str] = []
    try:
        for jid in ids:
            got_progress = await _wait_for_progress(
                queue, jid, expect_progress, timeout_secs
            )
            if got_progress != expect_progress:
                progress_errors.append(
                    f"{jid}: progress got {got_progress!r} want {expect_progress!r}"
                )

            got_logs, got_count = await _wait_for_logs(
                queue, jid, len(expect_logs), timeout_secs
            )
            if got_logs != expect_logs or got_count != len(expect_logs):
                log_errors.append(
                    f"{jid}: logs got ({got_logs!r}, {got_count!r}) "
                    f"want ({expect_logs!r}, {len(expect_logs)!r})"
                )
    finally:
        await queue.close()

    if progress_errors:
        for e in progress_errors[:5]:
            print(f"[py-verify-progress] ERROR: {e}", file=sys.stderr)
        return 1
    if log_errors:
        for e in log_errors[:5]:
            print(f"[py-verify-progress] ERROR: {e}", file=sys.stderr)
        return 1

    print(
        f"[py-verify-progress] OK — {len(ids)} jobs round-tripped "
        f"progress={expect_progress} logs={expect_logs!r}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
