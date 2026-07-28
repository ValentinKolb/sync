import { beforeEach, expect, test } from "bun:test";
import { redis } from "bun";
import { job } from "../index";

const uid = (name: string): string => `${name}-${Date.now()}-${Math.floor(Math.random() * 1_000_000)}`;

const waitFor = async (pred: () => boolean, timeoutMs = 10_000, pollMs = 20): Promise<void> => {
  const start = Date.now();
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error(`waitFor timed out after ${timeoutMs}ms`);
    await Bun.sleep(pollMs);
  }
};

beforeEach(async () => {
  const keys = await redis.send("KEYS", ["sync:job:*"]);
  if (Array.isArray(keys) && keys.length > 0) {
    await redis.send("DEL", keys as string[]);
  }
});

// ==========================
// Redelivery via ctx.reschedule increments failureCount
// ==========================

test("ctx.reschedule drives redelivery; failureCount tracks attempt increments", async () => {
  const seenFailureCounts: number[] = [];
  let succeededAt = -1;

  const worker = job({
    id: uid("reschedule-failure-count"),
    process: async ({ ctx }) => {
      seenFailureCounts.push(ctx.failureCount);
      if (ctx.failureCount < 3) throw new Error(`fail ${ctx.failureCount}`);
      succeededAt = ctx.failureCount;
    },
    after: async ({ ctx }) => {
      if (ctx.error && ctx.failureCount < 5) ctx.reschedule({ delayMs: 30 });
    },
  });

  await worker.submit({ key: "r:1" });
  await waitFor(() => succeededAt >= 0, 10_000);

  expect(seenFailureCounts).toEqual([0, 1, 2, 3]);
  expect(succeededAt).toBe(3);

  worker.stop();
});

// ==========================
// Idempotency key survives across run lifecycle
// ==========================

test("idempotency key stays claimed while process is running; concurrent submit reuses jobId", async () => {
  let processing = false;
  let allowFinish = false;

  const worker = job({
    id: uid("key-persistence"),
    defaults: { leaseMs: 10_000 },
    process: async () => {
      processing = true;
      while (!allowFinish) await Bun.sleep(20);
    },
  });

  const id1 = await worker.submit({ key: "chat:1" });
  await waitFor(() => processing);

  const id2 = await worker.submit({ key: "chat:1" });
  expect(id2).toBe(id1);

  allowFinish = true;
  await Bun.sleep(200);

  const id3 = await worker.submit({ key: "chat:1" });
  expect(id3).not.toBe(id1);

  worker.stop();
}, 15_000);

// ==========================
// Terminal failure and resubmit
// ==========================

test("terminal failure (no reschedule) releases key; resubmit with same key runs fresh", async () => {
  const attempts: number[] = [];
  let afterCalls = 0;

  const worker = job({
    id: uid("fail-release"),
    process: async ({ ctx }) => {
      attempts.push(ctx.failureCount);
      throw new Error("boom");
    },
    after: async () => {
      afterCalls += 1;
      // no reschedule → terminal
    },
  });

  const id1 = await worker.submit({ key: "item:1" });
  await waitFor(() => afterCalls === 1);

  const id2 = await worker.submit({ key: "item:1" });
  expect(id2).not.toBe(id1);
  await waitFor(() => afterCalls === 2);
  expect(attempts[1]).toBe(0);

  worker.stop();
});

// ==========================
// Reschedule holds key across delay window
// ==========================

test("reschedule via ctx.reschedule keeps key claimed across delayMs window", async () => {
  const attempts: number[] = [];
  let done = false;

  const worker = job({
    id: uid("reschedule-hold"),
    process: async ({ ctx }) => {
      attempts.push(ctx.failureCount);
      if (ctx.failureCount < 2) throw new Error("keep going");
      done = true;
    },
    after: async ({ ctx }) => {
      if (ctx.error && ctx.failureCount < 2) ctx.reschedule({ delayMs: 300 });
    },
  });

  const id1 = await worker.submit({ key: "item:1" });
  await waitFor(() => attempts.length === 1);

  const id2 = await worker.submit({ key: "item:1" });
  expect(id2).toBe(id1);

  await waitFor(() => done, 5_000);
  expect(attempts).toEqual([0, 1, 2]);

  worker.stop();
});

// ==========================
// Multiple concurrent workers (same definition id)
// ==========================

test("two workers with same definition id do not double-process the same delivery", async () => {
  const processedBy: string[] = [];
  const sharedPrefix = `sync:job:shared-${Date.now()}`;

  const wa = job({
    id: "shared-def",
    prefix: sharedPrefix,
    process: async () => {
      processedBy.push("a");
    },
  });
  const wb = job({
    id: "shared-def",
    prefix: sharedPrefix,
    process: async () => {
      processedBy.push("b");
    },
  });

  await wa.submit({ key: "k1" });
  await Bun.sleep(500);

  expect(processedBy.length).toBe(1);

  wa.stop();
  wb.stop();
});

// ==========================
// stop mid-process
// ==========================

test("worker.stop while process is running does not prevent the in-flight process from completing", async () => {
  let completedNormally = false;

  const worker = job({
    id: uid("stop-mid"),
    process: async () => {
      await Bun.sleep(150);
      completedNormally = true;
    },
  });

  await worker.submit({ key: "x" });
  await Bun.sleep(50);
  worker.stop();
  await Bun.sleep(300);

  expect(completedNormally).toBe(true);
});

// ==========================
// Idempotency claim lifecycle
// ==========================

test("a claim stranded by a crash before enqueue is re-enqueued, not silently dropped", async () => {
  const id = uid("stranded-claim");
  const runs: string[] = [];
  const j = job<{ v: number }>({
    id,
    process: async ({ ctx }) => {
      runs.push(ctx.key);
    },
  });

  // Exactly the state a pod leaves behind when it dies after claimKey and
  // before workQueue.send: a claim with no queue message, past the grace window.
  const idemKey = `sync:job:${id}:idempotency:orders/1`;
  await redis.send("SET", [
    idemKey,
    JSON.stringify({ jobId: "77", enqueued: false, claimedAt: Date.now() - 120_000 }),
  ]);

  const jobId = await j.submit({ key: "orders/1", input: { v: 1 } });
  expect(jobId).toBe("77"); // the caller's original jobId stays valid

  await waitFor(() => runs.length === 1);
  expect(runs).toEqual(["orders/1"]);
  j.stop();
});

test("a claim still inside the grace window is not enqueued twice", async () => {
  const id = uid("grace-claim");
  const j = job<{ v: number }>({ id, process: async () => {} });

  const idemKey = `sync:job:${id}:idempotency:orders/2`;
  await redis.send("SET", [idemKey, JSON.stringify({ jobId: "88", enqueued: false, claimedAt: Date.now() })]);

  expect(await j.submit({ key: "orders/2", input: { v: 1 } })).toBe("88");
  // Nothing was enqueued: a concurrent submit still owns that window.
  expect(await redis.send("LLEN", [`sync:job:queue:default:${id}:work:ready`])).toBe(0);
  j.stop();
});

test("a stale terminal release cannot delete a claim a later submit took over", async () => {
  const id = uid("stale-release");
  const j = job<{ v: number }>({ id, process: async () => {} });

  const idemKey = `sync:job:${id}:idempotency:orders/3`;
  await j.submit({ key: "orders/3", input: { v: 1 } });
  await waitFor(async () => true);

  // A later submit takes the key over under a different jobId.
  await redis.send("SET", [idemKey, JSON.stringify({ jobId: "999", enqueued: true, claimedAt: Date.now() })]);

  // The first job's terminal release must be a no-op against the new owner.
  await waitFor(async () => true, 500);
  const stillThere = await redis.send("GET", [idemKey]);
  expect(stillThere).not.toBeNull();
  expect(JSON.parse(stillThere as string).jobId).toBe("999");
  j.stop();
});

test("a reschedule chain keeps the idempotency claim alive past keyTtlMs", async () => {
  const id = uid("ttl-refresh");
  const idemKey = `sync:job:${id}:idempotency:orders/4`;
  // Whether the dedup claim was still held at the start of each attempt.
  const claimHeld: boolean[] = [];

  const j = job<{ v: number }>({
    id,
    defaults: { keyTtlMs: 1_000 },
    process: async () => {
      claimHeld.push((await redis.send("GET", [idemKey])) !== null);
      throw new Error("retry me");
    },
    after: ({ ctx }) => {
      if (ctx.error && ctx.failureCount < 6) ctx.reschedule({ delayMs: 500 });
    },
  });

  await j.submit({ key: "orders/4", input: { v: 1 } });
  // Four attempts at ~500ms apart span well past the 1s TTL.
  await waitFor(() => claimHeld.length >= 4, 20_000);
  j.stop();

  // Without a refresh the claim expires mid-chain, and a fanout submit would
  // then enqueue a second concurrent job for the same key.
  expect(claimHeld.slice(0, 4)).toEqual([true, true, true, true]);
});
