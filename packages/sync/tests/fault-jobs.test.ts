import { beforeEach, expect, test } from "bun:test";
import { redis } from "bun";
import { job, queue } from "../index";

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

test("two live workers with the same id process each key exactly once", async () => {
  const processedBy: string[] = [];
  const sharedPrefix = `sync:job:shared-${Date.now()}`;

  const wa = job<{ n: number }>({
    id: "shared-def",
    prefix: sharedPrefix,
    process: async () => {
      processedBy.push("a");
      await Bun.sleep(30);
    },
  });
  const wb = job<{ n: number }>({
    id: "shared-def",
    prefix: sharedPrefix,
    process: async () => {
      processedBy.push("b");
      await Bun.sleep(30);
    },
  });

  // Both handles must actually consume: a second same-id handle used to be a
  // silent no-op whose process callback was unreachable, so a single consumer
  // satisfied the old assertion on its own.
  const keys = Array.from({ length: 12 }, (_, i) => `k${i}`);
  for (const [index, key] of keys.entries()) {
    await (index % 2 === 0 ? wa : wb).submit({ key, input: { n: 1 } });
  }

  await waitFor(() => processedBy.length >= keys.length, 20_000);
  await Bun.sleep(500); // let any duplicate surface

  // Exactly one dispatch per key across both workers, and both did work.
  expect(processedBy.length).toBe(keys.length);
  expect(processedBy.includes("a")).toBe(true);
  expect(processedBy.includes("b")).toBe(true);

  wa.stop();
  wb.stop();
}, 30_000);

test("stop cancels the in-flight callback via ctx.signal", async () => {
  const id = uid("stop-signal");
  let abortedDuringRun = false;
  let observedAfterStop = false;
  let afterCalled = false;
  const events: string[] = [];

  const worker = job({
    id,
    defaults: { leaseMs: 100 },
    trace: (event) => {
      events.push(event.type);
    },
    process: async ({ ctx }) => {
      // `ctx.signal.aborted` used to be false for the entire life of process,
      // making the documented cancellation pattern inoperative.
      for (let i = 0; i < 100; i++) {
        if (ctx.signal.aborted) {
          abortedDuringRun = true;
          return;
        }
        await Bun.sleep(20);
      }
      observedAfterStop = true;
    },
    after: async () => {
      afterCalled = true;
    },
  });

  await worker.submit({ key: "cancel-me" });
  await Bun.sleep(150);
  worker.stop();
  await Bun.sleep(300);

  expect(abortedDuringRun).toBe(true);
  expect(observedAfterStop).toBe(false);
  expect(afterCalled).toBe(false);
  expect(worker.metric()).toEqual({ dispatches: 0, failures: 0, reschedules: 0 });
  expect(events).toEqual(["submitted", "started"]);

  const competitor = queue({ id: `${id}:work`, prefix: "sync:job:queue" }).reader();
  let redelivered = null;
  while (!redelivered) {
    redelivered = await competitor.recv({ wait: false });
    if (!redelivered) await Bun.sleep(20);
  }
  expect(await redelivered.ack()).toBe(true);
});

test("losing the lease during a heartbeat aborts the running callback", async () => {
  const id = uid("lease-loss-signal");
  const observed: boolean[] = [];
  let running = false;
  let afterCalled = false;
  const events: string[] = [];

  const worker = job({
    id,
    defaults: { leaseMs: 100 },
    trace: (event) => {
      events.push(event.type);
    },
    process: async ({ ctx }) => {
      running = true;
      // Outlive the lease while another consumer takes the delivery over, then
      // heartbeat: this attempt must be told to wind down instead of racing the
      // copy that is now owned by someone else.
      await Bun.sleep(900);
      await ctx.heartbeat();
      observed.push(ctx.signal.aborted);
    },
    after: async () => {
      afterCalled = true;
    },
  });

  await worker.submit({ key: "lease-loss" });
  await waitFor(() => running, 10_000);

  // A second consumer on the same internal work queue: its non-waiting recv
  // forces maintenance, which reaps the expired lease and hands the message on.
  const competitor = queue({ id: `${id}:work`, prefix: "sync:job:queue" }).reader();
  await Bun.sleep(100);
  let stolen = null;
  while (!stolen && observed.length === 0) {
    stolen = await competitor.recv({ wait: false });
    if (!stolen) await Bun.sleep(50);
  }

  await waitFor(() => observed.length >= 1, 20_000);
  worker.stop();
  await stolen?.ack();

  expect(observed[0]).toBe(true);
  expect(afterCalled).toBe(false);
  expect(worker.metric()).toEqual({ dispatches: 0, failures: 0, reschedules: 0 });
  expect(events).toEqual(["submitted", "started"]);
}, 30_000);

// ==========================
// stop mid-process
// ==========================

test("submit after stop waits for the aborted callback before restarting", async () => {
  let active = 0;
  let maxActive = 0;
  let firstStarted = false;
  const processed: string[] = [];

  const worker = job({
    id: uid("stop-restart"),
    process: async ({ ctx }) => {
      active += 1;
      maxActive = Math.max(maxActive, active);
      try {
        if (ctx.key === "first") {
          firstStarted = true;
          while (!ctx.signal.aborted) await Bun.sleep(10);
          await Bun.sleep(50);
          return;
        }
        processed.push(ctx.key);
      } finally {
        active -= 1;
      }
    },
  });

  await worker.submit({ key: "first" });
  await waitFor(() => firstStarted);
  worker.stop();
  await worker.submit({ key: "second" });

  await waitFor(() => processed.includes("second"));
  expect(processed).toEqual(["second"]);
  expect(maxActive).toBe(1);
  worker.stop();
});

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

test("recovery after a successful enqueue does not duplicate the queued job", async () => {
  const id = uid("ambiguous-enqueue");
  const runs: string[] = [];
  const j = job<{ v: number }>({
    id,
    process: async ({ ctx }) => {
      runs.push(ctx.key);
    },
  });
  const workQueue = queue<{
    jobId: string;
    key: string;
    input: { v: number };
    keyTtlMs: number;
    leaseMs: number;
  }>({
    id: `${id}:work`,
    prefix: "sync:job:queue",
  });

  const idemKey = `sync:job:${id}:idempotency:orders/ambiguous`;
  await redis.send("SET", [
    idemKey,
    JSON.stringify({ jobId: "77", enqueued: false, claimedAt: Date.now() - 120_000 }),
    "PX",
    "60000",
  ]);
  await workQueue.send({
    data: {
      jobId: "77",
      key: "orders/ambiguous",
      input: { v: 1 },
      keyTtlMs: 60_000,
      leaseMs: 30_000,
    },
    idempotencyKey: "77",
    idempotencyTtlMs: 60_000,
  });

  expect(await j.submit({ key: "orders/ambiguous", input: { v: 1 }, keyTtlMs: 60_000 })).toBe("77");
  await waitFor(() => runs.length >= 1);
  await Bun.sleep(200);
  expect(runs).toEqual(["orders/ambiguous"]);
  j.stop();
});

test("a deterministic queue send error releases the pending claim", async () => {
  const id = uid("send-error-claim");
  const key = "orders/oversized";
  const idemKey = `sync:job:${id}:idempotency:${key}`;
  const runs: string[] = [];
  const j = job<{ body: string }>({
    id,
    process: async ({ ctx }) => {
      runs.push(ctx.input.body);
    },
  });

  await expect(j.submit({ key, input: { body: "x".repeat(200_000) } })).rejects.toThrow(
    "payload exceeds limit",
  );
  expect(await redis.send("GET", [idemKey])).toBeNull();

  await j.submit({ key, input: { body: "recovered" }, keyTtlMs: 60_000 });
  await waitFor(() => runs.length === 1);
  expect(runs).toEqual(["recovered"]);
  j.stop();
});

test("an ambiguous successful queue send starts the worker and keeps one job", async () => {
  const id = uid("ambiguous-send-worker");
  const targetSeqKey = `sync:job:queue:default:${id}:work:seq`;
  const runs: string[] = [];
  const j = job({
    id,
    process: async ({ ctx }) => {
      runs.push(ctx.key);
    },
  });
  const originalSend = redis.send.bind(redis);
  let responseLost = false;

  redis.send = (async (command, args) => {
    const result = await originalSend(command, args);
    if (!responseLost && command === "EVAL" && args.includes(targetSeqKey)) {
      responseLost = true;
      const error = new Error("connection reset after write") as Error & { code: string };
      error.code = "ECONNRESET";
      throw error;
    }
    return result;
  }) as typeof redis.send;

  try {
    await expect(j.submit({ key: "orders/ambiguous" })).rejects.toThrow("connection reset after write");
  } finally {
    redis.send = originalSend as typeof redis.send;
  }

  expect(responseLost).toBe(true);
  await waitFor(() => runs.length === 1);
  await Bun.sleep(100);
  expect(runs).toEqual(["orders/ambiguous"]);
  j.stop();
});

test("an immediate retry re-enqueues a transport failure that happened before the queue write", async () => {
  const id = uid("prewrite-send-retry");
  const targetSeqKey = `sync:job:queue:default:${id}:work:seq`;
  const seen: number[] = [];
  const j = job<{ value: number }>({
    id,
    process: async ({ ctx }) => {
      seen.push(ctx.input.value);
    },
  });
  const originalSend = redis.send.bind(redis);
  let failedBeforeWrite = false;

  redis.send = (async (command, args) => {
    if (!failedBeforeWrite && command === "EVAL" && args.includes(targetSeqKey)) {
      failedBeforeWrite = true;
      const error = new Error("connection refused before write") as Error & { code: string };
      error.code = "ECONNREFUSED";
      throw error;
    }
    return await originalSend(command, args);
  }) as typeof redis.send;

  try {
    await expect(j.submit({ key: "orders/retry", input: { value: 1 } })).rejects.toThrow(
      "connection refused before write",
    );
  } finally {
    redis.send = originalSend as typeof redis.send;
  }

  expect(failedBeforeWrite).toBe(true);
  expect(await j.submit({ key: "orders/retry", input: { value: 2 } })).toBe("1");
  await waitFor(() => seen.length === 1);
  expect(seen).toEqual([1]);
  j.stop();
});

test("a retry near claim expiry cannot accept stale work under a newer claim", async () => {
  const id = uid("expiring-send-retry");
  const targetSeqKey = `sync:job:queue:default:${id}:work:seq`;
  const seen: number[] = [];
  const j = job<{ value: number }>({
    id,
    process: async ({ ctx }) => {
      seen.push(ctx.input.value);
    },
  });
  const originalSend = redis.send.bind(redis);
  let failFirst = true;
  redis.send = (async (command, args) => {
    if (failFirst && command === "EVAL" && args.includes(targetSeqKey)) {
      failFirst = false;
      const error = new Error("connection refused before write") as Error & { code: string };
      error.code = "ECONNREFUSED";
      throw error;
    }
    return await originalSend(command, args);
  }) as typeof redis.send;
  try {
    await expect(
      j.submit({ key: "orders/expiring", input: { value: 1 }, keyTtlMs: 1_000 }),
    ).rejects.toThrow("connection refused before write");
  } finally {
    redis.send = originalSend as typeof redis.send;
  }

  let releaseSend = (): void => {};
  const sendGate = new Promise<void>((resolve) => {
    releaseSend = resolve;
  });
  let retrySendStarted = false;
  redis.send = (async (command, args) => {
    if (!retrySendStarted && command === "EVAL" && args.includes(targetSeqKey)) {
      retrySendStarted = true;
      await sendGate;
    }
    return await originalSend(command, args);
  }) as typeof redis.send;

  try {
    const staleRetry = j.submit({ key: "orders/expiring", input: { value: 99 }, keyTtlMs: 1_000 });
    await waitFor(() => retrySendStarted);
    await Bun.sleep(1_100);
    const currentJobId = await j.submit({
      key: "orders/expiring",
      input: { value: 2 },
      keyTtlMs: 1_000,
    });
    expect(currentJobId).toBe("2");
    await waitFor(() => j.metric().dispatches === 1);
    expect(await redis.get(`sync:job:${id}:idempotency:orders/expiring`)).toBeNull();
    releaseSend();
    await expect(staleRetry).rejects.toThrow("lost idempotency claim");
  } finally {
    releaseSend();
    redis.send = originalSend as typeof redis.send;
  }

  await waitFor(() => seen.length === 1);
  await Bun.sleep(100);
  expect(seen).toEqual([2]);
  j.stop();
});

test("a stale attempt cannot delete a claim a later submit took over", async () => {
  const id = uid("stale-release");
  let started = false;
  let release: (() => void) | undefined;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const j = job<{ v: number }>({
    id,
    process: async () => {
      started = true;
      await gate;
    },
  });

  const idemKey = `sync:job:${id}:idempotency:orders/3`;
  await j.submit({ key: "orders/3", input: { v: 1 } });
  await waitFor(() => started);

  // A later submit takes the key over under a different jobId.
  await redis.send("SET", [idemKey, JSON.stringify({ jobId: "999", enqueued: true, claimedAt: Date.now() })]);

  // The first job is fenced before terminal completion and must not touch the
  // new owner or report a successful dispatch.
  release?.();
  await Bun.sleep(100);
  const stillThere = await redis.send("GET", [idemKey]);
  expect(stillThere).not.toBeNull();
  expect(JSON.parse(stillThere as string).jobId).toBe("999");
  expect(j.metric().dispatches).toBe(0);
  j.stop();
});

test("reschedule does not nack after the idempotency claim changes owner", async () => {
  const id = uid("reschedule-fence");
  const key = "orders/fenced";
  const idemKey = `sync:job:${id}:idempotency:${key}`;
  let afterDone = false;
  const j = job({
    id,
    process: async () => {},
    after: async ({ ctx }) => {
      await redis.send("SET", [idemKey, "999", "PX", "60000"]);
      ctx.reschedule({ delayMs: 10_000 });
      afterDone = true;
    },
  });

  await j.submit({ key });
  await waitFor(() => afterDone);
  await Bun.sleep(100);

  expect(j.metric().reschedules).toBe(0);
  expect(await redis.send("ZSCORE", [`sync:job:queue:default:${id}:work:delayed`, "1"])).toBeNull();
  expect(await redis.send("GET", [idemKey])).toBe("999");
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
