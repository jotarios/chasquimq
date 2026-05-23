import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { Queue, Worker } from "../dist/index.js";

const REDIS_URL = process.env.REDIS_URL;
const skipIfNoRedis = REDIS_URL ? describe : describe.skip;

function parseConn(url: string) {
  const u = new URL(url);
  return {
    host: u.hostname || "127.0.0.1",
    port: u.port ? Number(u.port) : 6379,
    password: u.password || undefined,
    username: u.username || undefined,
    db:
      u.pathname && u.pathname !== "/"
        ? Number(u.pathname.slice(1))
        : undefined,
  };
}

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs: number,
  label: string,
): Promise<void> {
  const start = Date.now();
  while (true) {
    if (await predicate()) return;
    if (Date.now() - start > timeoutMs)
      throw new Error(`waitFor(${label}) timed out after ${timeoutMs}ms`);
    await new Promise((r) => setTimeout(r, 25));
  }
}

skipIfNoRedis("Queue maintenance API", () => {
  let queueName: string;
  let queue: Queue<{ msg: string }>;

  beforeEach(() => {
    queueName = `qmq-mnt-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    queue = new Queue<{ msg: string }>(queueName, {
      connection: parseConn(REDIS_URL!),
    });
  });

  afterEach(async () => {
    await queue.close().catch(() => {});
  });

  // ---- remove -----------------------------------------------------------

  it("remove deletes a waiting job and reports the surface", async () => {
    const job = await queue.add("w", { msg: "remove-me" });
    await queue.add("w", { msg: "keep-me" });

    const count = await queue.remove(job.id);
    expect(count).toBe(1);

    const report = await queue.removeReport("never-existed");
    expect(report.delayed).toBe(false);
    expect(report.stream).toBe(false);
    expect(report.dlq).toBe(false);
    expect(report.result).toBe(false);

    const counts = await queue.getJobCounts();
    expect(counts.waiting).toBe(1);
  });

  it("remove of a missing id is idempotent (count 0, no throw)", async () => {
    const count = await queue.remove("does-not-exist");
    expect(count).toBe(0);
  });

  it("remove deletes a delayed job", async () => {
    const job = await queue.add(
      "d",
      { msg: "delayed" },
      { delay: 3600_000, jobId: "delayed-remove-target" },
    );
    let counts = await queue.getJobCounts();
    expect(counts.delayed).toBe(1);

    const report = await queue.removeReport(job.id);
    expect(report.delayed).toBe(true);

    counts = await queue.getJobCounts();
    expect(counts.delayed).toBe(0);
  });

  // ---- drain ------------------------------------------------------------

  it("drain clears waiting and delayed jobs", async () => {
    for (let i = 0; i < 6; i++) {
      await queue.add("w", { msg: `w${i}` });
    }
    for (let i = 0; i < 3; i++) {
      await queue.add("d", { msg: `d${i}` }, { delay: 3600_000 });
    }
    let counts = await queue.getJobCounts();
    expect(counts.waiting).toBe(6);
    expect(counts.delayed).toBe(3);

    const removed = await queue.drain();
    expect(removed).toBe(9);

    counts = await queue.getJobCounts();
    expect(counts.waiting).toBe(0);
    expect(counts.delayed).toBe(0);
  });

  it("drain(false) keeps delayed jobs", async () => {
    await queue.add("w", { msg: "w" });
    await queue.add("d", { msg: "d" }, { delay: 3600_000 });

    const removed = await queue.drain(false);
    expect(removed).toBe(1);

    const counts = await queue.getJobCounts();
    expect(counts.waiting).toBe(0);
    expect(counts.delayed).toBe(1);
  });

  // ---- clean ------------------------------------------------------------

  it("clean(waiting, grace=0) removes all waiting jobs and returns ids", async () => {
    for (let i = 0; i < 4; i++) {
      await queue.add("w", { msg: `w${i}` });
    }
    const removed = await queue.clean(0, 100, "waiting");
    expect(removed).toHaveLength(4);

    const counts = await queue.getJobCounts();
    expect(counts.waiting).toBe(0);
  });

  it("clean grace window excludes recent jobs", async () => {
    for (let i = 0; i < 3; i++) {
      await queue.add("w", { msg: `w${i}` });
    }
    const removed = await queue.clean(3_600_000, 100, "waiting");
    expect(removed).toHaveLength(0);

    const counts = await queue.getJobCounts();
    expect(counts.waiting).toBe(3);
  });

  it("clean limit caps removals", async () => {
    for (let i = 0; i < 8; i++) {
      await queue.add("w", { msg: `w${i}` });
    }
    const removed = await queue.clean(0, 3, "waiting");
    expect(removed).toHaveLength(3);

    const counts = await queue.getJobCounts();
    expect(counts.waiting).toBe(5);
  });

  it("clean(delayed) removes delayed jobs", async () => {
    for (let i = 0; i < 3; i++) {
      await queue.add("d", { msg: `d${i}` }, { delay: 3600_000 });
    }
    const removed = await queue.clean(0, 100, "delayed");
    expect(removed).toHaveLength(3);

    const counts = await queue.getJobCounts();
    expect(counts.delayed).toBe(0);
  });

  it("clean(failed) removes DLQ entries", async () => {
    const worker = new Worker<{ msg: string }>(
      queueName,
      async () => {
        throw new Error("intentional failure");
      },
      {
        connection: parseConn(REDIS_URL!),
        concurrency: 1,
        // `maxStalledCount` maps to the engine's maxAttempts: 1 → straight
        // to the DLQ on the first failure.
        maxStalledCount: 1,
      },
    );
    for (let i = 0; i < 3; i++) {
      await queue.add("f", { msg: `f${i}` });
    }
    await waitFor(
      async () => (await queue.getJobCounts()).failed >= 3,
      10_000,
      "dlq-populated",
    );
    await worker.close();

    const removed = await queue.clean(0, 100, "failed");
    expect(removed).toHaveLength(3);

    const counts = await queue.getJobCounts();
    expect(counts.failed).toBe(0);
  }, 20_000);

  // ---- obliterate -------------------------------------------------------

  it("obliterate tears the whole queue down", async () => {
    for (let i = 0; i < 5; i++) {
      await queue.add("w", { msg: `w${i}` });
    }
    await queue.add("d", { msg: "d" }, { delay: 3600_000 });

    const removed = await queue.obliterate();
    expect(removed).toBeGreaterThanOrEqual(2);

    const counts = await queue.getJobCounts();
    expect(counts.waiting).toBe(0);
    expect(counts.delayed).toBe(0);
    expect(counts.failed).toBe(0);
  });

  it("obliterate then re-add works on a fresh queue", async () => {
    await queue.add("w", { msg: "before" });
    await queue.obliterate();

    const job = await queue.add("w", { msg: "after" });
    expect(job.id).toBeTruthy();

    const counts = await queue.getJobCounts();
    expect(counts.waiting).toBe(1);
  });

  it("obliterate on an empty queue is idempotent", async () => {
    const first = await queue.obliterate();
    expect(first).toBe(0);
    const second = await queue.obliterate();
    expect(second).toBe(0);
  });
});
