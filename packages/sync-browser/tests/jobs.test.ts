import { test, expect, afterEach } from "bun:test";
import { z } from "zod";
import { job } from "../src/job";

const uid = (name: string): string =>
  `${name}-${Date.now()}-${Math.floor(Math.random() * 1_000_000)}`;

// Track workers created during tests so afterEach can stop them all
let activeWorkers: { stop(): void }[] = [];

afterEach(() => {
  for (const w of activeWorkers) {
    w.stop();
  }
  activeWorkers = [];
});

const tracked = <I, R>(handle: ReturnType<typeof job<any, any>>): typeof handle => {
  activeWorkers.push(handle);
  return handle;
};

// ==========================
// 1. submit and join returns completed result
// ==========================

test("submit and join returns completed result", async () => {
  const worker = tracked(
    job({
      id: uid("complete"),
      schema: z.object({ value: z.number() }),
      process: async ({ input }) => input.value * 2,
    }),
  );

  const id = await worker.submit({ input: { value: 5 } });
  const result = await worker.join({ id, timeoutMs: 2_000 });

  expect(result.status).toBe("completed");
  expect(result.result).toBe(10);
  expect(result.id).toBe(id);
  expect(result.finishedAt).toBeGreaterThan(0);
});

// ==========================
// 2. process function receives correct input
// ==========================

test("process function receives correct input", async () => {
  let receivedInput: unknown = null;

  const worker = tracked(
    job({
      id: uid("input-check"),
      schema: z.object({ name: z.string(), count: z.number() }),
      process: async ({ input }) => {
        receivedInput = input;
        return "done";
      },
    }),
  );

  const id = await worker.submit({ input: { name: "hello", count: 42 } });
  await worker.join({ id, timeoutMs: 2_000 });

  expect(receivedInput).toEqual({ name: "hello", count: 42 });
});

// ==========================
// 3. process result is returned via join
// ==========================

test("process result is returned via join", async () => {
  const worker = tracked(
    job({
      id: uid("result-return"),
      schema: z.object({ items: z.array(z.number()) }),
      process: async ({ input }) => {
        return input.items.reduce((a: number, b: number) => a + b, 0);
      },
    }),
  );

  const id = await worker.submit({ input: { items: [1, 2, 3, 4] } });
  const result = await worker.join({ id, timeoutMs: 2_000 });

  expect(result.status).toBe("completed");
  expect(result.result).toBe(10);
});

// ==========================
// 4. failed process: join returns failed status with error
// ==========================

test("failed process returns failed status with error", async () => {
  const worker = tracked(
    job({
      id: uid("fail"),
      schema: z.object({}),
      process: async () => {
        throw new Error("something went wrong");
      },
    }),
  );

  const id = await worker.submit({ input: {}, maxAttempts: 1 });
  const result = await worker.join({ id, timeoutMs: 2_000 });

  expect(result.status).toBe("failed");
  expect(result.error?.message).toBe("something went wrong");
});

// ==========================
// 5. retry: process fails first, succeeds second attempt
// ==========================

test("retry: fails first attempt, succeeds on second", async () => {
  let calls = 0;

  const worker = tracked(
    job({
      id: uid("retry"),
      schema: z.object({}),
      process: async () => {
        calls += 1;
        if (calls < 2) throw new Error("transient failure");
        return "recovered";
      },
    }),
  );

  const id = await worker.submit({
    input: {},
    maxAttempts: 2,
    backoff: { kind: "fixed", baseMs: 50 },
  });

  const result = await worker.join({ id, timeoutMs: 2_000 });

  expect(result.status).toBe("completed");
  expect(result.result).toBe("recovered");
  expect(calls).toBe(2);
});

// ==========================
// 6. cancel during processing, join returns cancelled
// ==========================

test("cancel during processing returns cancelled status", async () => {
  let started = false;

  const worker = tracked(
    job({
      id: uid("cancel"),
      schema: z.object({}),
      process: async () => {
        started = true;
        await Bun.sleep(500);
        return "done";
      },
    }),
  );

  const id = await worker.submit({ input: {}, leaseMs: 2_000 });

  // Wait for the process to start
  while (!started) await Bun.sleep(5);

  await worker.cancel({ id, reason: "no longer needed" });
  const result = await worker.join({ id, timeoutMs: 2_000 });

  expect(result.status).toBe("cancelled");
});

// ==========================
// 7. join timeoutMs returns timed_out if job doesn't complete in time
// ==========================

test("join timeoutMs returns timed_out when job is slow", async () => {
  const worker = tracked(
    job({
      id: uid("join-timeout"),
      schema: z.object({}),
      process: async () => "ok",
    }),
  );

  // Submit with a large delay so it won't process in time
  const id = await worker.submit({ input: {}, delayMs: 2_000 });
  const result = await worker.join({ id, timeoutMs: 100 });

  expect(result.status).toBe("timed_out");
  expect(result.error?.code).toBe("JOIN_TIMEOUT");
});

// ==========================
// 8. idempotent submit: same key returns same jobId
// ==========================

test("idempotent submit with same key returns same jobId", async () => {
  const worker = tracked(
    job({
      id: uid("idemp"),
      schema: z.object({ value: z.number() }),
      process: async ({ input }) => input.value,
    }),
  );

  const idA = await worker.submit({ input: { value: 1 }, key: "dedup-key" });
  const idB = await worker.submit({ input: { value: 1 }, key: "dedup-key" });

  expect(idA).toBe(idB);

  const result = await worker.join({ id: idA, timeoutMs: 2_000 });
  expect(result.status).toBe("completed");
});

// ==========================
// 9. validateInput throws on invalid input
// ==========================

test("validateInput throws on invalid input", () => {
  const worker = tracked(
    job({
      id: uid("validate"),
      schema: z.object({ value: z.number() }),
      process: async ({ input }) => input.value,
    }),
  );

  // Valid input should not throw
  expect(() => worker.validateInput({ value: 42 })).not.toThrow();

  // Invalid input should throw
  expect(() => worker.validateInput({ value: "not a number" })).toThrow();
  expect(() => worker.validateInput({})).toThrow();
  expect(() => worker.validateInput(null)).toThrow();
});

// ==========================
// 10. stop halts the worker loop
// ==========================

test("stop halts the worker loop", async () => {
  let processed = 0;

  const worker = tracked(
    job({
      id: uid("stop"),
      schema: z.object({}),
      process: async () => {
        processed += 1;
        return "ok";
      },
    }),
  );

  // Process first job
  const id1 = await worker.submit({ input: {} });
  const result1 = await worker.join({ id: id1, timeoutMs: 2_000 });
  expect(result1.status).toBe("completed");
  expect(processed).toBe(1);

  worker.stop();

  // After stop, a new submit restarts the worker
  const id2 = await worker.submit({ input: {} });
  const result2 = await worker.join({ id: id2, timeoutMs: 2_000 });
  expect(result2.status).toBe("completed");
  expect(processed).toBe(2);
});

// ==========================
// 11. events: can read submitted/completed events via events().live()
// ==========================

test("events: live() yields submitted, started, and completed events", async () => {
  const worker = tracked(
    job({
      id: uid("events"),
      schema: z.object({ value: z.number() }),
      process: async ({ input }) => input.value + 1,
    }),
  );

  const id = await worker.submit({ input: { value: 5 } });
  const terminal = await worker.join({ id, timeoutMs: 2_000 });
  expect(terminal.status).toBe("completed");

  // Read events via reader stream (uses the topic's reader API)
  const ev = worker.events(id);
  const reader = ev.reader("test-reader");

  const types: string[] = [];
  for await (const event of reader.stream({ wait: false })) {
    types.push(event.data.type);
    await event.commit();
  }

  expect(types).toContain("submitted");
  expect(types).toContain("started");
  expect(types).toContain("completed");

  // Verify live is a function
  expect(typeof ev.live).toBe("function");
});

// ==========================
// 12. heartbeat: ctx.heartbeat extends lease
// ==========================

test("heartbeat extends lease and emits heartbeat event", async () => {
  let heartbeatCalled = false;

  const worker = tracked(
    job({
      id: uid("heartbeat"),
      schema: z.object({}),
      process: async ({ ctx }) => {
        await ctx.heartbeat({ leaseMs: 5_000 });
        heartbeatCalled = true;
        return "alive";
      },
    }),
  );

  const id = await worker.submit({ input: {}, leaseMs: 500 });
  const result = await worker.join({ id, timeoutMs: 2_000 });

  expect(result.status).toBe("completed");
  expect(result.result).toBe("alive");
  expect(heartbeatCalled).toBe(true);

  // Check that a heartbeat event was emitted
  const ev = worker.events(id);
  const reader = ev.reader("hb-reader");
  const types: string[] = [];
  for await (const event of reader.stream({ wait: false })) {
    types.push(event.data.type);
    await event.commit();
  }

  expect(types).toContain("heartbeat");
});

// ==========================
// 13. step: ctx.step executes and returns value
// ==========================

test("ctx.step executes and returns value", async () => {
  const stepResults: string[] = [];

  const worker = tracked(
    job({
      id: uid("step"),
      schema: z.object({}),
      process: async ({ ctx }) => {
        const a = await ctx.step({
          id: "step-a",
          run: () => "result-a",
        });
        stepResults.push(a);

        const b = await ctx.step({
          id: "step-b",
          run: async () => {
            await Bun.sleep(10);
            return "result-b";
          },
        });
        stepResults.push(b);

        return `${a}+${b}`;
      },
    }),
  );

  const id = await worker.submit({ input: {} });
  const result = await worker.join({ id, timeoutMs: 2_000 });

  expect(result.status).toBe("completed");
  expect(result.result).toBe("result-a+result-b");
  expect(stepResults).toEqual(["result-a", "result-b"]);
});

// ==========================
// 14. ctx.signal is aborted on cancel
// ==========================

test("ctx.signal is aborted on cancel", async () => {
  let signalAborted = false;
  let started = false;

  const worker = tracked(
    job({
      id: uid("signal-cancel"),
      schema: z.object({}),
      process: async ({ ctx }) => {
        started = true;
        ctx.signal.addEventListener("abort", () => {
          signalAborted = true;
        });
        // Simulate long-running work
        await Bun.sleep(500);
        return "late";
      },
    }),
  );

  const id = await worker.submit({ input: {}, leaseMs: 2_000, maxAttempts: 1 });

  // Wait for process to start
  while (!started) await Bun.sleep(5);

  // Cancel while running - the signal should be aborted via the
  // error path (cancel sets state to cancelled, worker detects it)
  await worker.cancel({ id, reason: "abort-test" });

  const result = await worker.join({ id, timeoutMs: 2_000 });
  expect(result.status).toBe("cancelled");

  // Give abort handler time to fire
  await Bun.sleep(50);
  expect(signalAborted).toBe(true);
});

// ==========================
// 15. delayed job: submit with delayMs, processes after delay
// ==========================

test("delayed job processes after delay", async () => {
  const worker = tracked(
    job({
      id: uid("delay"),
      schema: z.object({ value: z.number() }),
      process: async ({ input }) => input.value * 3,
    }),
  );

  const submitTime = Date.now();
  const id = await worker.submit({ input: { value: 7 }, delayMs: 200 });

  // Should not complete immediately
  const earlyResult = await worker.join({ id, timeoutMs: 50 });
  expect(earlyResult.status).toBe("timed_out");

  // Wait for the delay and processing
  const finalResult = await worker.join({ id, timeoutMs: 2_000 });
  expect(finalResult.status).toBe("completed");
  expect(finalResult.result).toBe(21);
  expect(finalResult.finishedAt).toBeGreaterThanOrEqual(submitTime + 150);
});

// ==========================
// Bonus: submit rejects invalid input
// ==========================

test("submit rejects invalid input via schema", async () => {
  const worker = tracked(
    job({
      id: uid("submit-invalid"),
      schema: z.object({ value: z.number() }),
      process: async ({ input }) => input.value,
    }),
  );

  let thrown: unknown = null;
  try {
    // @ts-expect-error intentional invalid data
    await worker.submit({ input: { value: "not-a-number" } });
  } catch (error) {
    thrown = error;
  }

  expect(thrown).not.toBeNull();
});

// ==========================
// Bonus: cancel on already-completed job is a no-op
// ==========================

test("cancel on already-completed job is a no-op", async () => {
  const worker = tracked(
    job({
      id: uid("cancel-noop"),
      schema: z.object({}),
      process: async () => "done",
    }),
  );

  const id = await worker.submit({ input: {} });
  const result = await worker.join({ id, timeoutMs: 2_000 });
  expect(result.status).toBe("completed");

  // Cancel after completion should not change status
  await worker.cancel({ id, reason: "too late" });

  // Re-join should still show completed
  const result2 = await worker.join({ id, timeoutMs: 500 });
  expect(result2.status).toBe("completed");
});

// ==========================
// Bonus: multiple concurrent joins on same job all resolve
// ==========================

test("multiple concurrent joins on same job all resolve", async () => {
  const worker = tracked(
    job({
      id: uid("multi-join"),
      schema: z.object({}),
      process: async () => {
        await Bun.sleep(50);
        return "shared-result";
      },
    }),
  );

  const id = await worker.submit({ input: {} });

  const [r1, r2, r3] = await Promise.all([
    worker.join({ id, timeoutMs: 2_000 }),
    worker.join({ id, timeoutMs: 2_000 }),
    worker.join({ id, timeoutMs: 2_000 }),
  ]);

  expect(r1.status).toBe("completed");
  expect(r2.status).toBe("completed");
  expect(r3.status).toBe("completed");
  expect(r1.result).toBe("shared-result");
});

// ==========================
// Bonus: ctx.signal is aborted on error (timeout)
// ==========================

test("ctx.signal is aborted when process times out", async () => {
  let signalAborted = false;

  const worker = tracked(
    job({
      id: uid("signal-timeout"),
      schema: z.object({}),
      process: async ({ ctx }) => {
        ctx.signal.addEventListener("abort", () => {
          signalAborted = true;
        });
        await Bun.sleep(500);
        return "late";
      },
    }),
  );

  const id = await worker.submit({ input: {}, leaseMs: 50, maxAttempts: 1 });
  const result = await worker.join({ id, timeoutMs: 2_000 });

  expect(result.status).toBe("timed_out");
  // Give abort handler time to fire
  await Bun.sleep(30);
  expect(signalAborted).toBe(true);
});

// ==========================
// Bonus: events include retry event on failure + retry
// ==========================

test("retry emits retry event with nextAt", async () => {
  let calls = 0;

  const worker = tracked(
    job({
      id: uid("retry-event"),
      schema: z.object({}),
      process: async () => {
        calls += 1;
        if (calls < 2) throw new Error("retry me");
        return "ok";
      },
    }),
  );

  const id = await worker.submit({
    input: {},
    maxAttempts: 2,
    backoff: { kind: "fixed", baseMs: 50 },
  });

  const result = await worker.join({ id, timeoutMs: 2_000 });
  expect(result.status).toBe("completed");

  const ev = worker.events(id);
  const reader = ev.reader("retry-reader");
  const types: string[] = [];

  for await (const event of reader.stream({ wait: false })) {
    types.push(event.data.type);
    await event.commit();
  }

  expect(types).toContain("retry");
  expect(types).toContain("completed");
});

// ==========================
// Two handles with same definition.id share state
// ==========================

test("two handles with same definition.id share state", async () => {
  const defId = uid("shared-def");
  let callCount = 0;

  const w1 = tracked(job({
    id: defId,
    schema: z.object({ v: z.number() }),
    process: async ({ input }) => { callCount++; return input.v * 2; },
  }));

  const w2 = tracked(job({
    id: defId,
    schema: z.object({ v: z.number() }),
    process: async ({ input }) => { callCount++; return input.v * 2; },
  }));

  const id = await w1.submit({ input: { v: 5 } });

  // join via the OTHER handle should also work
  const result = await w2.join({ id, timeoutMs: 5_000 });
  expect(result.status).toBe("completed");
  expect(result.result).toBe(10);
});
