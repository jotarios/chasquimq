import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { Queue } from "../dist/index.js";

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

skipIfNoRedis("Queue introspection", () => {
  let queueName: string;
  let queue: Queue<{ msg: string }>;

  beforeEach(() => {
    queueName = `qmq-introspect-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    queue = new Queue<{ msg: string }>(queueName, {
      connection: parseConn(REDIS_URL!),
    });
  });

  afterEach(async () => {
    await queue.close().catch(() => {});
  });

  it("getJobCounts on an empty queue returns all zeros", async () => {
    const counts = await queue.getJobCounts();
    expect(counts.waiting).toBe(0);
    expect(counts.active).toBe(0);
    expect(counts.delayed).toBe(0);
    expect(counts.failed).toBe(0);
    expect(counts.paused).toBe(0);
  });

  it('getJobState returns "unknown" for a missing id', async () => {
    const state = await queue.getJobState("does-not-exist");
    expect(state).toBe("unknown");
  });

  it("getJob returns undefined for a missing id", async () => {
    const job = await queue.getJob("does-not-exist");
    expect(job).toBeUndefined();
  });

  it('getJobState returns "waiting" for an unread entry', async () => {
    const job = await queue.add("hello", { msg: "hi" });
    const state = await queue.getJobState(job.id);
    expect(state).toBe("waiting");
  });

  it("getJob retrieves a waiting job with msgpack-decoded payload", async () => {
    const job = await queue.add("hello", { msg: "round-trip" });
    const fetched = await queue.getJob(job.id);
    expect(fetched).toBeDefined();
    expect(fetched!.id).toBe(job.id);
    expect(fetched!.name).toBe("hello");
    expect(fetched!.data).toEqual({ msg: "round-trip" });
    expect(fetched!.attemptsMade).toBe(0);
  });

  it('getJobState returns "delayed" for a future-scheduled job', async () => {
    const job = await queue.add(
      "delayed",
      { msg: "later" },
      { delay: 3600_000, jobId: "stable-delay-id" },
    );
    expect(job.id).toBe("stable-delay-id");
    const state = await queue.getJobState(job.id);
    expect(state).toBe("delayed");
  });

  it("getJobCounts reflects waiting and delayed jobs", async () => {
    for (let i = 0; i < 4; i++) {
      await queue.add("w", { msg: `w${i}` });
    }
    for (let i = 0; i < 2; i++) {
      await queue.add("d", { msg: `d${i}` }, { delay: 3600_000 });
    }
    const counts = await queue.getJobCounts();
    expect(counts.waiting).toBe(4);
    expect(counts.delayed).toBe(2);
  });

  it("getJobCounts filters by requested types", async () => {
    await queue.add("w", { msg: "one" });
    const filtered = await queue.getJobCounts("waiting");
    expect(Object.keys(filtered).sort()).toEqual(["waiting"]);
    expect(filtered.waiting).toBe(1);
  });

  it("getJobs paginates waiting state", async () => {
    for (let i = 0; i < 7; i++) {
      await queue.add("w", { msg: `n${i}` });
    }
    const page = await queue.getJobs("waiting");
    expect(page).toHaveLength(7);
    expect(page[0]!.data).toEqual({ msg: "n0" });
  });

  it("getJobs supports start/end slicing", async () => {
    for (let i = 0; i < 5; i++) {
      await queue.add("w", { msg: `n${i}` });
    }
    // start=1, end=3 → 3 entries (indices 1, 2, 3).
    const slice = await queue.getJobs("waiting", 1, 3);
    expect(slice).toHaveLength(3);
    expect(slice[0]!.data).toEqual({ msg: "n1" });
  });

  it("isPaused / pause / resume reflect via getJobCounts", async () => {
    await queue.pause();
    expect(await queue.isPaused()).toBe(true);
    const counts = await queue.getJobCounts();
    expect(counts.paused).toBe(1);
    await queue.resume();
    expect(await queue.isPaused()).toBe(false);
    const counts2 = await queue.getJobCounts();
    expect(counts2.paused).toBe(0);
  });

  it("getJob.name preserves the dispatch name from the n field", async () => {
    const job = await queue.add("send-email", { msg: "subject" });
    const fetched = await queue.getJob(job.id);
    expect(fetched).toBeDefined();
    expect(fetched!.name).toBe("send-email");
  });

  it("getJobs with multiple states throws NotSupportedError", async () => {
    await expect(queue.getJobs(["waiting", "delayed"] as any)).rejects.toThrow(
      /NotSupported|single JobState/i,
    );
  });

  it("consumerGroup option scopes the introspector and does not error on NOGROUP", async () => {
    // Custom consumer group, no consumer ever started — XPENDING under
    // that group returns NOGROUP. The shim's introspector must swallow
    // that and report waiting count instead of erroring.
    const queueWithGroup = new Queue<{ msg: string }>(queueName, {
      connection: parseConn(REDIS_URL!),
      consumerGroup: "never-actually-used",
    });
    try {
      await queueWithGroup.add("w", { msg: "orphan" });
      const counts = await queueWithGroup.getJobCounts();
      expect(counts.waiting).toBe(1);
      expect(counts.active).toBe(0);
    } finally {
      await queueWithGroup.close().catch(() => {});
    }
  });
});
