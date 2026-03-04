import { beforeEach, expect, test } from "bun:test";
import { redis } from "bun";
import { z } from "zod";
import { job } from "../index";

const uid = (name: string): string => `${name}-${Date.now()}-${Math.floor(Math.random() * 1_000_000)}`;

beforeEach(async () => {
  const keys = await redis.send("KEYS", ["sync:job:*"]);
  if (Array.isArray(keys) && keys.length > 0) {
    await redis.send("DEL", keys as string[]);
  }
});

test("submit + join completes a job", async () => {
  const sendOrderMail = job({
    id: uid("job-complete"),
    schema: z.object({ value: z.number() }),
    process: async ({ input }) => input.value * 2,
  });

  const id = await sendOrderMail.submit({ input: { value: 2 } });
  const terminal = await sendOrderMail.join({ id, timeoutMs: 5_000 });

  expect(terminal.status).toBe("completed");
  expect(terminal.result).toBe(4);
});

test("retry then complete", async () => {
  let calls = 0;

  const worker = job({
    id: uid("job-retry"),
    schema: z.object({}),
    process: async () => {
      calls += 1;
      if (calls < 2) throw new Error("first failure");
      return "ok";
    },
  });

  const id = await worker.submit({
    input: {},
    maxAttempts: 2,
    backoff: { kind: "fixed", baseMs: 50 },
  });

  const terminal = await worker.join({ id, timeoutMs: 8_000 });

  expect(terminal.status).toBe("completed");
  expect(terminal.result).toBe("ok");
  expect(calls).toBe(2);
});

test("cancel marks job as cancelled", async () => {
  const worker = job({
    id: uid("job-cancel"),
    schema: z.object({ id: z.string() }),
    process: async () => {
      await Bun.sleep(100);
      return "done";
    },
  });

  const id = await worker.submit({
    input: { id: "a" },
    delayMs: 200,
  });

  await worker.cancel({ id, reason: "user-request" });

  const terminal = await worker.join({ id, timeoutMs: 5_000 });
  expect(terminal.status).toBe("cancelled");
});

test("events expose topic reader/live API", async () => {
  const worker = job({
    id: uid("job-events"),
    schema: z.object({ value: z.number() }),
    process: async ({ input }) => input.value + 1,
  });

  const id = await worker.submit({ input: { value: 5 } });
  const terminal = await worker.join({ id, timeoutMs: 5_000 });
  expect(terminal.status).toBe("completed");

  const events = worker.events(id);
  const reader = events.reader("orchestrator");

  const types: string[] = [];
  for await (const event of reader.stream({ wait: false })) {
    types.push(event.data.type);
    await event.commit();
  }

  expect(types.includes("submitted")).toBe(true);
  expect(types.includes("started")).toBe(true);
  expect(types.includes("completed")).toBe(true);
  expect(typeof events.live).toBe("function");
});

test("submit deduplicates by key", async () => {
  const worker = job({
    id: uid("job-dedup"),
    schema: z.object({ value: z.number() }),
    process: async ({ input }) => input.value,
  });

  const a = await worker.submit({ input: { value: 1 }, key: "same-key" });
  const b = await worker.submit({ input: { value: 1 }, key: "same-key" });

  expect(a).toBe(b);
  const terminal = await worker.join({ id: a, timeoutMs: 5_000 });
  expect(terminal.status).toBe("completed");
});

test("join returns timed_out when timeout is reached", async () => {
  const worker = job({
    id: uid("job-join-timeout"),
    schema: z.object({}),
    process: async () => "ok",
  });

  const id = await worker.submit({ input: {}, delayMs: 500 });
  const terminal = await worker.join({ id, timeoutMs: 50 });

  expect(terminal.status).toBe("timed_out");
  expect(terminal.error?.code).toBe("JOIN_TIMEOUT");
});

test("job fails permanently after max attempts", async () => {
  const worker = job({
    id: uid("job-fail"),
    schema: z.object({}),
    process: async () => {
      throw new Error("boom");
    },
  });

  const id = await worker.submit({ input: {}, maxAttempts: 1 });
  const terminal = await worker.join({ id, timeoutMs: 5_000 });

  expect(terminal.status).toBe("failed");
  expect(terminal.error?.message).toBe("boom");
});

test("job reaches timed_out status when lease is exceeded", async () => {
  const worker = job({
    id: uid("job-timeout"),
    schema: z.object({}),
    process: async () => {
      await Bun.sleep(120);
      return "late";
    },
  });

  const id = await worker.submit({ input: {}, leaseMs: 40, maxAttempts: 1 });
  const terminal = await worker.join({ id, timeoutMs: 5_000 });

  expect(terminal.status).toBe("timed_out");
});

test("parallel submit with same key deduplicates atomically", async () => {
  const worker = job({
    id: uid("job-parallel-dedup"),
    schema: z.object({ v: z.number() }),
    process: async ({ input }) => input.v,
  });

  const results = await Promise.all(
    Array.from({ length: 10 }, () =>
      worker.submit({ input: { v: 1 }, key: "race-key" }),
    ),
  );

  const unique = new Set(results);
  expect(unique.size).toBe(1);

  const terminal = await worker.join({ id: results[0], timeoutMs: 5_000 });
  expect(terminal.status).toBe("completed");
});

test("stop() halts worker processing", async () => {
  let processed = 0;

  const worker = job({
    id: uid("job-stop"),
    schema: z.object({}),
    process: async () => {
      processed += 1;
      return "ok";
    },
  });

  const id = await worker.submit({ input: {} });
  const terminal = await worker.join({ id, timeoutMs: 5_000 });
  expect(terminal.status).toBe("completed");
  expect(processed).toBe(1);

  worker.stop();

  // After stop, submitting starts the worker again
  const id2 = await worker.submit({ input: {} });
  const terminal2 = await worker.join({ id: id2, timeoutMs: 5_000 });
  expect(terminal2.status).toBe("completed");
  expect(processed).toBe(2);

  worker.stop();
});

test("ctx.signal is aborted on timeout", async () => {
  let signalAborted = false;

  const worker = job({
    id: uid("job-signal"),
    schema: z.object({}),
    process: async ({ ctx }) => {
      ctx.signal.addEventListener("abort", () => {
        signalAborted = true;
      });
      await Bun.sleep(200);
      return "late";
    },
  });

  const id = await worker.submit({ input: {}, leaseMs: 40, maxAttempts: 1 });
  const terminal = await worker.join({ id, timeoutMs: 5_000 });

  expect(terminal.status).toBe("timed_out");
  // Give abort handler time to fire
  await Bun.sleep(20);
  expect(signalAborted).toBe(true);
});

test("ctx.signal is aborted on error", async () => {
  let signalAborted = false;

  const worker = job({
    id: uid("job-signal-err"),
    schema: z.object({}),
    process: async ({ ctx }) => {
      ctx.signal.addEventListener("abort", () => {
        signalAborted = true;
      });
      throw new Error("boom");
    },
  });

  const id = await worker.submit({ input: {}, maxAttempts: 1 });
  const terminal = await worker.join({ id, timeoutMs: 5_000 });

  expect(terminal.status).toBe("failed");
  await Bun.sleep(20);
  expect(signalAborted).toBe(true);
});

test("state TTL is isolated per job", async () => {
  const worker = job({
    id: uid("job-ttl-isolation"),
    schema: z.object({ v: z.number() }),
    process: async ({ input }) => input.v,
  });

  const id1 = await worker.submit({ input: { v: 1 } });
  const id2 = await worker.submit({ input: { v: 2 } });

  const t1 = await worker.join({ id: id1, timeoutMs: 5_000 });
  const t2 = await worker.join({ id: id2, timeoutMs: 5_000 });
  expect(t1.status).toBe("completed");
  expect(t2.status).toBe("completed");

  // Both states exist as independent Redis keys
  const raw1 = await redis.get(`sync:job:${worker.id}:state:${id1}`);
  const raw2 = await redis.get(`sync:job:${worker.id}:state:${id2}`);
  expect(raw1).not.toBeNull();
  expect(raw2).not.toBeNull();

  // Each key has its own TTL
  const ttl1 = await redis.send("PTTL", [`sync:job:${worker.id}:state:${id1}`]);
  const ttl2 = await redis.send("PTTL", [`sync:job:${worker.id}:state:${id2}`]);
  expect(Number(ttl1)).toBeGreaterThan(0);
  expect(Number(ttl2)).toBeGreaterThan(0);
});

test("cancel during running job is detected by worker", async () => {
  let started = false;

  const worker = job({
    id: uid("job-cancel-running"),
    schema: z.object({}),
    process: async () => {
      started = true;
      await Bun.sleep(300);
      return "done";
    },
  });

  const id = await worker.submit({ input: {}, leaseMs: 5_000 });

  // Wait for the process to start
  while (!started) await Bun.sleep(10);

  await worker.cancel({ id, reason: "mid-flight" });
  const terminal = await worker.join({ id, timeoutMs: 5_000 });
  expect(terminal.status).toBe("cancelled");
});

test("cancel on already-completed job is a no-op", async () => {
  const worker = job({
    id: uid("job-cancel-noop"),
    schema: z.object({}),
    process: async () => "done",
  });

  const id = await worker.submit({ input: {} });
  const terminal = await worker.join({ id, timeoutMs: 5_000 });
  expect(terminal.status).toBe("completed");

  // Cancel after completion should not change the state
  await worker.cancel({ id, reason: "too late" });

  const raw = await redis.get(`sync:job:${worker.id}:state:${id}`);
  const state = JSON.parse(raw!);
  expect(state.status).toBe("completed");
});

test("multiple concurrent joins on same job all resolve", async () => {
  const worker = job({
    id: uid("job-multi-join"),
    schema: z.object({}),
    process: async () => {
      await Bun.sleep(50);
      return "result";
    },
  });

  const id = await worker.submit({ input: {} });

  const [t1, t2, t3] = await Promise.all([
    worker.join({ id, timeoutMs: 5_000 }),
    worker.join({ id, timeoutMs: 5_000 }),
    worker.join({ id, timeoutMs: 5_000 }),
  ]);

  expect(t1.status).toBe("completed");
  expect(t2.status).toBe("completed");
  expect(t3.status).toBe("completed");
  expect(t1.result).toBe("result");
});

test("exponential backoff emits retry events with increasing nextAt", async () => {
  const worker = job({
    id: uid("job-exp-backoff"),
    schema: z.object({}),
    process: async () => {
      throw new Error("retry me");
    },
  });

  const id = await worker.submit({
    input: {},
    maxAttempts: 3,
    backoff: { kind: "exp", baseMs: 100 },
  });

  const terminal = await worker.join({ id, timeoutMs: 15_000 });
  expect(terminal.status).toBe("failed");

  // Read retry events and check that nextAt gaps increase
  const events = worker.events(id);
  const reader = events.reader("backoff-check");
  const retryDeltas: number[] = [];

  for await (const event of reader.stream({ wait: false })) {
    if (event.data.type === "retry") {
      retryDeltas.push(event.data.nextAt - event.data.ts);
    }
    await event.commit();
  }

  // Should have 2 retry events (attempts 1 and 2 fail, attempt 3 fails terminally)
  expect(retryDeltas.length).toBe(2);
  // First retry: baseMs * 2^0 = 100, second retry: baseMs * 2^1 = 200
  expect(retryDeltas[0]).toBeGreaterThanOrEqual(80); // ~100ms with some slack
  expect(retryDeltas[1]).toBeGreaterThanOrEqual(160); // ~200ms with some slack
  expect(retryDeltas[1]).toBeGreaterThan(retryDeltas[0]);
});

test("cancel emits cancelled event", async () => {
  const worker = job({
    id: uid("job-cancel-event"),
    schema: z.object({}),
    process: async () => "ok",
  });

  const id = await worker.submit({ input: {}, delayMs: 200 });
  await worker.cancel({ id, reason: "manual" });

  const events = worker.events(id);
  const reader = events.reader("cancel-reader");
  const types: string[] = [];

  for await (const event of reader.stream({ wait: false })) {
    types.push(event.data.type);
    await event.commit();
  }

  expect(types.includes("cancelled")).toBe(true);
});

// ==========================
// Race condition tests
// ==========================

test("cancel racing with completed write — final state is consistent", async () => {
  // This test characterizes the race between cancel() and the worker
  // writing "completed" state. We run multiple iterations to provoke
  // the timing-dependent interleaving.
  for (let attempt = 0; attempt < 20; attempt++) {
    let processCompleted = false;

    const worker = job({
      id: uid("job-cancel-race"),
      schema: z.object({}),
      process: async () => {
        await Bun.sleep(20);
        processCompleted = true;
        return "done";
      },
    });

    const id = await worker.submit({ input: {}, leaseMs: 5_000 });

    // Wait until the process is likely completing
    while (!processCompleted) await Bun.sleep(5);

    // Race: cancel right as the worker is finishing
    await worker.cancel({ id, reason: "race" });

    const terminal = await worker.join({ id, timeoutMs: 5_000 });

    // The state should be one of the two — never something else
    expect(["completed", "cancelled"]).toContain(terminal.status);

    // Verify Redis state matches what join reported
    const raw = await redis.get(`sync:job:${worker.id}:state:${id}`);
    expect(raw).not.toBeNull();
    const state = JSON.parse(raw!);
    expect(state.status).toBe(terminal.status);

    worker.stop();
  }
});

test("lease expiry during processing does not cause double execution", async () => {
  let execCount = 0;

  const worker = job({
    id: uid("job-no-double"),
    schema: z.object({}),
    process: async () => {
      execCount += 1;
      // Sleep less than leaseMs so withTimeout doesn't fire,
      // but close enough that lease timing could be tight
      await Bun.sleep(30);
      return "ok";
    },
  });

  const id = await worker.submit({ input: {}, leaseMs: 80, maxAttempts: 1 });
  const terminal = await worker.join({ id, timeoutMs: 5_000 });

  expect(terminal.status).toBe("completed");
  expect(execCount).toBe(1);
});

test("multiple rapid submits produce independent jobs", async () => {
  let count = 0;

  const worker = job({
    id: uid("job-rapid-submit"),
    schema: z.object({ n: z.number() }),
    process: async ({ input }) => {
      count += 1;
      return input.n * 2;
    },
  });

  // Submit 10 jobs rapidly without idempotency keys
  const ids = await Promise.all(
    Array.from({ length: 10 }, (_, i) =>
      worker.submit({ input: { n: i } }),
    ),
  );

  // All should have unique IDs
  expect(new Set(ids).size).toBe(10);

  // All should complete
  const results = await Promise.all(
    ids.map((id) => worker.join({ id, timeoutMs: 10_000 })),
  );

  for (const r of results) {
    expect(r.status).toBe("completed");
  }
  expect(count).toBe(10);
});

test("worker recovers when state key is missing but queue message exists", async () => {
  let runs = 0;

  const worker = job({
    id: uid("job-recover-missing-state"),
    schema: z.object({ v: z.number() }),
    process: async ({ input }) => {
      runs += 1;
      return input.v * 3;
    },
  });

  const id = await worker.submit({
    input: { v: 7 },
    key: "recover-state-key",
    delayMs: 100,
  });

  await Bun.sleep(20);
  await redis.del(`sync:job:${worker.id}:state:${id}`);

  const terminal = await worker.join({ id, timeoutMs: 6_000 });
  expect(terminal.status).toBe("completed");
  expect(terminal.result).toBe(21);
  expect(runs).toBe(1);
});

test("cancel remains authoritative if written before completion finalize", async () => {
  let started = false;

  const worker = job({
    id: uid("job-cancel-authoritative"),
    schema: z.object({}),
    process: async () => {
      started = true;
      await Bun.sleep(120);
      return "done";
    },
  });

  const id = await worker.submit({ input: {}, leaseMs: 5_000 });
  while (!started) await Bun.sleep(5);

  await worker.cancel({ id, reason: "force-cancel" });
  const terminal = await worker.join({ id, timeoutMs: 5_000 });
  expect(terminal.status).toBe("cancelled");

  const raw = await redis.get(`sync:job:${worker.id}:state:${id}`);
  expect(raw).not.toBeNull();
  const state = JSON.parse(raw!);
  expect(state.status).toBe("cancelled");

  worker.stop();
});
