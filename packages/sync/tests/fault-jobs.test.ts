import { afterAll, beforeEach, expect, test } from "bun:test";
import { redis } from "bun";
import { job as createJob, queue, type JobConfig, type JobHandle } from "../index";

const uid = (name: string): string => `${name}-${Date.now()}-${Math.floor(Math.random() * 1_000_000)}`;
const TEST_PREFIX = `test:fault-jobs:${process.pid}:${uid("run")}`;
const job = <Input = void, Result = unknown>(
  config: JobConfig<Input, Result>,
): JobHandle<Input> =>
  createJob({ ...config, prefix: config.prefix ?? TEST_PREFIX });
const jobQueueBase = (id: string): string =>
  `sync:queue:namespace:v2:${encodeURIComponent(JSON.stringify([`${TEST_PREFIX}:queue`, "default", `${id}:work`]))}`;
const jobClaimKey = (id: string, key: string): string =>
  `sync:job:claim:v2:${encodeURIComponent(JSON.stringify([TEST_PREFIX, id, key]))}`;
const internalJobQueue = <T>(id: string) =>
  queue<T>({
    id: `${id}:work`,
    prefix: `${TEST_PREFIX}:queue`,
  });

const waitFor = async (pred: () => boolean, timeoutMs = 10_000, pollMs = 20): Promise<void> => {
  const start = Date.now();
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error(`waitFor timed out after ${timeoutMs}ms`);
    await Bun.sleep(pollMs);
  }
};

const cleanup = async (): Promise<void> => {
  const [legacyKeys, claimKeys, receiptKeys, queueKeys] = await Promise.all([
    redis.send("KEYS", [`${TEST_PREFIX}:*`]),
    redis.send("KEYS", [
      `sync:job:claim:v2:${encodeURIComponent(`["${TEST_PREFIX}`)}*`,
    ]),
    redis.send("KEYS", [
      `sync:job:enqueue-receipt:v2:${encodeURIComponent(`["${TEST_PREFIX}`)}*`,
    ]),
    redis.send("KEYS", [
      `sync:queue:namespace:v2:${encodeURIComponent(`["${TEST_PREFIX}:queue`)}*`,
    ]),
  ]);
  const keys = [
    ...(Array.isArray(legacyKeys) ? legacyKeys : []),
    ...(Array.isArray(claimKeys) ? claimKeys : []),
    ...(Array.isArray(receiptKeys) ? receiptKeys : []),
    ...(Array.isArray(queueKeys) ? queueKeys : []),
  ];
  if (keys.length > 0) {
    await redis.send("DEL", keys as string[]);
  }
};

beforeEach(cleanup);
afterAll(cleanup);

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
  await waitFor(() => worker.metric().dispatches === 1);

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
  await waitFor(() => worker.metric().failures === 1);

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

  const wa = job<{ n: number }>({
    id: "shared-def",
    process: async () => {
      processedBy.push("a");
      await Bun.sleep(30);
    },
  });
  const wb = job<{ n: number }>({
    id: "shared-def",
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

  const competitor = internalJobQueue(id).reader();
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
  const competitor = internalJobQueue(id).reader();
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
  let started = false;
  let completedNormally = false;

  const worker = job({
    id: uid("stop-mid"),
    process: async () => {
      started = true;
      await Bun.sleep(150);
      completedNormally = true;
    },
  });

  await worker.submit({ key: "x" });
  await waitFor(() => started);
  worker.stop();
  await waitFor(() => completedNormally);

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
  const idemKey = jobClaimKey(id, "orders/1");
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

  const idemKey = jobClaimKey(id, "orders/2");
  await redis.send("SET", [idemKey, JSON.stringify({ jobId: "88", enqueued: false, claimedAt: Date.now() })]);

  expect(await j.submit({ key: "orders/2", input: { v: 1 } })).toBe("88");
  // Nothing was enqueued: a concurrent submit still owns that window.
  expect(await redis.send("LLEN", [`${jobQueueBase(id)}:ready`])).toBe(0);
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
  const workQueue = internalJobQueue<{
    jobId: string;
    key: string;
    input: { v: number };
    keyTtlMs: number;
    leaseMs: number;
  }>(id);

  const idemKey = jobClaimKey(id, "orders/ambiguous");
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
  const idemKey = jobClaimKey(id, key);
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
  const targetSeqKey = `${jobQueueBase(id)}:seq`;
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

test("a cold worker runs accepted work when submit-side mark faults", async () => {
  const id = uid("mark-fault-worker");
  const key = "orders/accepted";
  const claimKey = jobClaimKey(id, key);
  const targetSeqKey = `${jobQueueBase(id)}:seq`;
  const runs: string[] = [];
  const j = job({
    id,
    process: async ({ ctx }) => {
      runs.push(ctx.key);
    },
  });
  const originalSend = redis.send.bind(redis);
  let queueSendSucceeded = false;
  let markFaulted = false;

  redis.send = (async (command, args) => {
    if (
      !markFaulted
      && command === "EVAL"
      && args[2] === claimKey
      && args.at(-1) === "submit"
    ) {
      if (!queueSendSucceeded) throw new Error("submit mark ran before queue send");
      markFaulted = true;
      const error = new Error("connection reset during submit mark") as Error & { code: string };
      error.code = "ECONNRESET";
      throw error;
    }
    const result = await originalSend(command, args);
    if (command === "EVAL" && args.includes(targetSeqKey)) queueSendSucceeded = true;
    return result;
  }) as typeof redis.send;

  try {
    await expect(j.submit({ key })).rejects.toThrow("connection reset during submit mark");
    expect(queueSendSucceeded).toBe(true);
    expect(markFaulted).toBe(true);
    await waitFor(() => j.metric().dispatches === 1);
    await Bun.sleep(100);
    expect(runs).toEqual([key]);
  } finally {
    redis.send = originalSend as typeof redis.send;
    j.stop();
  }
});

test("worker trace stays ordered when the committed submit mark reply is lost", async () => {
  const id = uid("committed-mark-reply");
  const key = "orders/traced";
  const claimKey = jobClaimKey(id, key);
  const events: string[] = [];
  const j = job({
    id,
    trace: (event) => {
      events.push(event.type);
    },
    process: async () => {},
  });
  const originalSend = redis.send.bind(redis);
  let markCommitted = false;
  let releaseReply = (): void => {};
  const replyGate = new Promise<void>((resolve) => {
    releaseReply = resolve;
  });

  redis.send = (async (command, args) => {
    if (
      !markCommitted
      && command === "EVAL"
      && args[2] === claimKey
      && args.at(-1) === "submit"
    ) {
      await originalSend(command, args);
      markCommitted = true;
      await replyGate;
      const error = new Error("connection reset after submit mark") as Error & { code: string };
      error.code = "ECONNRESET";
      throw error;
    }
    return await originalSend(command, args);
  }) as typeof redis.send;

  try {
    const submitted = j.submit({ key });
    await waitFor(() => markCommitted);
    await waitFor(() => events.includes("finished"));
    expect(events).toEqual(["submitted", "started", "succeeded", "finished"]);

    releaseReply();
    await expect(submitted).rejects.toThrow("connection reset after submit mark");
  } finally {
    releaseReply();
    redis.send = originalSend as typeof redis.send;
    j.stop();
  }
});

test("concurrent submitters confirm a job after its warmed worker finishes", async () => {
  const id = uid("worker-before-submit-mark");
  const key = "orders/fast";
  const idemKey = jobClaimKey(id, key);
  const receiptKey =
    `sync:job:enqueue-receipt:v2:${encodeURIComponent(JSON.stringify([TEST_PREFIX, id, key, "2"]))}`;
  const runs: string[] = [];
  const events: string[] = [];
  const worker = job({
    id,
    trace: (event) => {
      events.push(event.type);
    },
    process: async ({ ctx }) => {
      runs.push(ctx.key);
    },
  });

  await worker.submit({ key: "warm-up" });
  await waitFor(() => runs.includes("warm-up"));
  await waitFor(() => worker.metric().dispatches === 1);
  await waitFor(() => events.includes("finished"));
  events.length = 0;

  const originalSend = redis.send.bind(redis);
  let blockedMarks = 0;
  let releaseMark = (): void => {};
  const markGate = new Promise<void>((resolve) => {
    releaseMark = resolve;
  });
  redis.send = (async (command, args) => {
    if (
      command === "EVAL"
      && args[2] === idemKey
      && args.at(-1) === "submit"
    ) {
      blockedMarks += 1;
      await markGate;
    }
    return await originalSend(command, args);
  }) as typeof redis.send;

  try {
    const submitted = [worker.submit({ key }), worker.submit({ key })];
    await waitFor(() => blockedMarks === 2);
    await waitFor(() => runs.includes(key));
    await waitFor(() => worker.metric().dispatches === 2);
    expect(await redis.get(idemKey)).toBeNull();
    expect(await redis.get(receiptKey)).toBe("2");

    releaseMark();
    await expect(Promise.all(submitted)).resolves.toEqual(["2", "2"]);
    expect(await redis.get(receiptKey)).toBe("2");
    expect(Number(await redis.send("PTTL", [receiptKey]))).toBeGreaterThan(0);
    await Bun.sleep(100);
    expect(runs.filter((run) => run === key)).toHaveLength(1);
    await waitFor(() => events.includes("finished"));
    expect(events).toEqual(["submitted", "started", "succeeded", "finished"]);
    expect(events.filter((event) => event === "submitted")).toHaveLength(1);
  } finally {
    releaseMark();
    redis.send = originalSend as typeof redis.send;
    worker.stop();
  }
});

test("a worker refreshes the receipt after one concurrent submitter marks enqueued", async () => {
  const id = uid("worker-after-submit-mark");
  const key = "orders/marked";
  const idemKey = jobClaimKey(id, key);
  const receiptKey =
    `sync:job:enqueue-receipt:v2:${encodeURIComponent(JSON.stringify([TEST_PREFIX, id, key, "2"]))}`;
  const runs: string[] = [];
  const events: string[] = [];
  const worker = job({
    id,
    trace: (event) => {
      events.push(event.type);
    },
    process: async ({ ctx }) => {
      runs.push(ctx.key);
    },
  });

  await worker.submit({ key: "warm-up" });
  await waitFor(() => worker.metric().dispatches === 1);
  await waitFor(() => events.includes("finished"));
  events.length = 0;

  const originalSend = redis.send.bind(redis);
  let submitMarks = 0;
  let firstMarkCommitted = false;
  let secondMarkBlocked = false;
  let signalBothMarks = (): void => {};
  const bothMarksGate = new Promise<void>((resolve) => {
    signalBothMarks = resolve;
  });
  let signalFirstMark = (): void => {};
  const firstMarkGate = new Promise<void>((resolve) => {
    signalFirstMark = resolve;
  });
  let releaseSecondMark = (): void => {};
  const secondMarkGate = new Promise<void>((resolve) => {
    releaseSecondMark = resolve;
  });

  redis.send = (async (command, args) => {
    if (command === "EVAL" && args[2] === idemKey && args.at(-1) === "submit") {
      submitMarks += 1;
      if (submitMarks === 1) {
        await bothMarksGate;
        const result = await originalSend(command, args);
        firstMarkCommitted = true;
        signalFirstMark();
        return result;
      }
      if (submitMarks === 2) {
        secondMarkBlocked = true;
        signalBothMarks();
        await secondMarkGate;
      }
    }
    if (command === "EVAL" && args[2] === idemKey && args.at(-1) === "worker") {
      await firstMarkGate;
    }
    return await originalSend(command, args);
  }) as typeof redis.send;

  try {
    const submitted = [worker.submit({ key }), worker.submit({ key })];
    await waitFor(() => firstMarkCommitted && secondMarkBlocked);
    await waitFor(() => worker.metric().dispatches === 2);
    expect(await redis.get(idemKey)).toBeNull();
    expect(await redis.get(receiptKey)).toBe("2");

    releaseSecondMark();
    await expect(Promise.all(submitted)).resolves.toEqual(["2", "2"]);
    expect(Number(await redis.send("PTTL", [receiptKey]))).toBeGreaterThan(0);
    expect(runs.filter((run) => run === key)).toHaveLength(1);
    expect(events).toEqual(["submitted", "started", "succeeded", "finished"]);
  } finally {
    signalBothMarks();
    signalFirstMark();
    releaseSecondMark();
    redis.send = originalSend as typeof redis.send;
    worker.stop();
  }
});

test("worker enqueue receipts stay bounded when the delivery lease exceeds the key TTL cap", async () => {
  const maxKeyTtlMs = 30 * 24 * 60 * 60 * 1_000;
  const id = uid("bounded-worker-receipt");
  const key = "orders/long-lease";
  const receiptKey =
    `sync:job:enqueue-receipt:v2:${encodeURIComponent(JSON.stringify([TEST_PREFIX, id, key, "1"]))}`;
  let runs = 0;
  const worker = job({
    id,
    defaults: { leaseMs: maxKeyTtlMs + 10_000 },
    process: async () => {
      runs += 1;
    },
  });

  try {
    await expect(worker.submit({ key })).resolves.toBe("1");
    await waitFor(() => worker.metric().dispatches === 1);

    const receiptTtlMs = Number(await redis.send("PTTL", [receiptKey]));
    expect(receiptTtlMs).toBeGreaterThan(0);
    expect(receiptTtlMs).toBeLessThanOrEqual(maxKeyTtlMs);
    expect(runs).toBe(1);
  } finally {
    worker.stop();
  }
});

test("pending retries cannot renew a claim past the queue idempotency fence", async () => {
  const id = uid("pending-claim-expiry");
  const key = "orders/pending";
  const claimKey = jobClaimKey(id, key);
  const queueBase = jobQueueBase(id);
  const queueIdempotencyKey = `${queueBase}:idempotency:1`;
  const seen: number[] = [];
  const j = job<{ value: number }>({
    id,
    process: async ({ ctx }) => {
      seen.push(ctx.input.value);
    },
  });
  const originalSend = redis.send.bind(redis);
  let faultedMarks = 0;

  redis.send = (async (command, args) => {
    if (
      command === "EVAL"
      && args[2] === claimKey
      && args[4] === "1"
      && args.at(-1) === "submit"
    ) {
      faultedMarks += 1;
      throw new Error("faulted submit mark");
    }
    return await originalSend(command, args);
  }) as typeof redis.send;

  try {
    await expect(
      j.submit({ key, input: { value: 1 }, delayMs: 5_000, keyTtlMs: 1_000 }),
    ).rejects.toThrow("faulted submit mark");
    const pending = await redis.get(claimKey);
    expect(pending).not.toBeNull();
    expect(await redis.get(queueIdempotencyKey)).toBe("1");
    expect(await redis.send("HLEN", [`${queueBase}:messages`])).toBe(1);

    // Put the claim and queue fence at the same near-expiry point. Retrying the
    // captured payload must leave the pending record and its deadline intact.
    await redis.send("PEXPIRE", [claimKey, "500"]);
    await redis.send("PEXPIRE", [queueIdempotencyKey, "500"]);
    await expect(
      j.submit({ key, input: { value: 99 }, keyTtlMs: 1_000 }),
    ).rejects.toThrow("faulted submit mark");
    expect(await redis.get(claimKey)).toBe(pending);
    expect(Number(await redis.send("PTTL", [claimKey]))).toBeLessThanOrEqual(500);
    expect(await redis.send("HLEN", [`${queueBase}:messages`])).toBe(1);

    while (await redis.get(queueIdempotencyKey)) {
      await Bun.sleep(5);
    }
    expect(await redis.get(claimKey)).toBeNull();
  } finally {
    redis.send = originalSend as typeof redis.send;
  }

  const replacement = await j.submit({ key, input: { value: 2 }, keyTtlMs: 1_000 });
  expect(replacement).toBe("2");
  await waitFor(() => j.metric().dispatches === 1);
  expect(faultedMarks).toBe(2);
  expect(seen).toEqual([2]);
  j.stop();
});

test("an immediate retry re-enqueues a transport failure that happened before the queue write", async () => {
  const id = uid("prewrite-send-retry");
  const targetSeqKey = `${jobQueueBase(id)}:seq`;
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
  const targetSeqKey = `${jobQueueBase(id)}:seq`;
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
    expect(await redis.get(jobClaimKey(id, "orders/expiring"))).toBeNull();
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

  const idemKey = jobClaimKey(id, "orders/3");
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
  const idemKey = jobClaimKey(id, key);
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
  expect(await redis.send("ZSCORE", [`${jobQueueBase(id)}:delayed`, "1"])).toBeNull();
  expect(await redis.send("GET", [idemKey])).toBe("999");
  j.stop();
});

test("a reschedule chain keeps the idempotency claim alive past keyTtlMs", async () => {
  const id = uid("ttl-refresh");
  const idemKey = jobClaimKey(id, "orders/4");
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
}, 20_000);

test("terminal settlement atomically acknowledges work and releases its claim", async () => {
  const id = uid("atomic-terminal");
  const key = "orders/atomic";
  const idemKey = jobClaimKey(id, key);
  const queueBase = jobQueueBase(id);
  let runs = 0;
  let terminalCommitted = false;
  const j = job({
    id,
    process: async () => {
      runs += 1;
    },
  });
  const originalSend = redis.send.bind(redis);

  redis.send = (async (command, args) => {
    if (!terminalCommitted && command === "EVAL" && args[1] === "7" && args.includes(idemKey)) {
      const result = await originalSend(command, args);
      if (Number(result) > 0) {
        terminalCommitted = true;
        const error = new Error("connection reset after terminal commit") as Error & { code: string };
        error.code = "ECONNRESET";
        throw error;
      }
      return result;
    }
    return await originalSend(command, args);
  }) as typeof redis.send;

  try {
    const first = await j.submit({ key });
    await waitFor(() => terminalCommitted);
    redis.send = originalSend as typeof redis.send;

    expect(await redis.send("GET", [idemKey])).toBeNull();
    expect(await redis.send("HGET", [`${queueBase}:messages`, first])).toBeNull();
    expect(await redis.send("HLEN", [`${queueBase}:deliveries`])).toBe(0);

    const second = await j.submit({ key });
    expect(second).not.toBe(first);
    await waitFor(() => runs === 2);
  } finally {
    redis.send = originalSend as typeof redis.send;
    j.stop();
  }
});

test("active claim outlives a short key TTL until its delivery lease ends", async () => {
  const attempts: number[] = [];
  const j = job({
    id: uid("active-claim-lease"),
    defaults: { leaseMs: 3_000, keyTtlMs: 1_000 },
    process: async ({ ctx }) => {
      attempts.push(ctx.failureCount);
      if (ctx.failureCount === 0) {
        await Bun.sleep(1_100);
        throw new Error("retry");
      }
    },
    after: ({ ctx }) => {
      if (ctx.error) ctx.reschedule();
    },
  });

  try {
    await j.submit({ key: "orders/slow" });
    await waitFor(() => attempts.length === 2, 8_000);
    await waitFor(() => j.metric().dispatches === 1);
    expect(attempts).toEqual([0, 1]);
    expect(j.metric().reschedules).toBe(1);
    expect(j.metric().dispatches).toBe(1);
  } finally {
    j.stop();
  }
});

test("heartbeat after stop does not extend the delivery or claim", async () => {
  const id = uid("stopped-heartbeat");
  const key = "orders/stopped";
  const queueBase = jobQueueBase(id);
  const idemKey = jobClaimKey(id, key);
  let started = false;
  let heartbeatDone = false;
  let release: (() => void) | undefined;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const j = job({
    id,
    defaults: { leaseMs: 1_000, keyTtlMs: 1_000 },
    process: async ({ ctx }) => {
      started = true;
      await gate;
      await ctx.heartbeat({ leaseMs: 5_000 });
      heartbeatDone = true;
    },
  });

  try {
    await j.submit({ key });
    await waitFor(() => started);
    const deliveryId = String(await redis.send("HGET", [`${queueBase}:delivery-owners`, "1"]));
    const leaseBefore = await redis.send("ZSCORE", [`${queueBase}:leases`, deliveryId]);

    j.stop();
    release?.();
    await waitFor(() => heartbeatDone);

    expect(await redis.send("ZSCORE", [`${queueBase}:leases`, deliveryId])).toBe(leaseBefore);
    expect(Number(await redis.send("PTTL", [idemKey]))).toBeLessThanOrEqual(1_000);
  } finally {
    release?.();
    j.stop();
  }
});
