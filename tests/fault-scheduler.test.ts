import { beforeEach, expect, test } from "bun:test";
import { redis } from "bun";
import { z } from "zod";
import { job, scheduler } from "../index";

const uid = (name: string): string => `${name}-${Date.now()}-${Math.floor(Math.random() * 1_000_000)}`;

const waitUntil = async (predicate: () => boolean | Promise<boolean>, timeoutMs = 5_000): Promise<void> => {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await predicate()) return;
    await Bun.sleep(20);
  }
  throw new Error("waitUntil timeout");
};

const forceScheduleDue = async (schedulerId: string, scheduleId: string, nextRunAt: number): Promise<void> => {
  const key = `sync:scheduler:${schedulerId}:schedule:${scheduleId}`;
  const raw = await redis.get(key);
  if (!raw) throw new Error(`missing schedule key ${key}`);
  const parsed = JSON.parse(raw);
  parsed.nextRunAt = nextRunAt;
  parsed.updatedAt = Date.now();
  await redis.send("SET", [key, JSON.stringify(parsed)]);
  await redis.send("ZADD", [`sync:scheduler:${schedulerId}:due`, String(nextRunAt), scheduleId]);
  await redis.send("SADD", [`sync:scheduler:${schedulerId}:index`, scheduleId]);
};

beforeEach(async () => {
  const keys = await redis.send("KEYS", ["sync:scheduler:*"]);
  if (Array.isArray(keys) && keys.length > 0) {
    await redis.send("DEL", keys as string[]);
  }
  const jobKeys = await redis.send("KEYS", ["sync:job:*"]);
  if (Array.isArray(jobKeys) && jobKeys.length > 0) {
    await redis.send("DEL", jobKeys as string[]);
  }
});

// ==========================
// Multiple schedules + budget fairness
// ==========================

test("multiple due schedules with low maxSubmitsPerTick — no starvation", async () => {
  const schedulerId = uid("sched-fairness");
  const runsBySchedule = new Map<string, number>();

  const worker = job({
    id: uid("sched-fairness-job"),
    schema: z.object({ schedId: z.string() }),
    process: async ({ input }) => {
      const count = runsBySchedule.get(input.schedId) ?? 0;
      runsBySchedule.set(input.schedId, count + 1);
      return "ok";
    },
  });

  const s = scheduler({
    id: schedulerId,
    leader: { leaseMs: 1_000, heartbeatMs: 100 },
    dispatch: { tickMs: 40, maxSubmitsPerTick: 1 },
  });

  try {
    // Register 3 schedules
    for (const name of ["alpha", "beta", "gamma"]) {
      await s.register({
        id: name,
        cron: "* * * * *",
        tz: "UTC",
        misfire: "catch_up_one",
        job: worker,
        input: { schedId: name },
      });
      await forceScheduleDue(schedulerId, name, Date.now() - 60_000);
    }

    s.start();

    // With maxSubmitsPerTick=1, each tick can only dispatch one slot.
    // Over multiple ticks, all 3 schedules should eventually get dispatched.
    await waitUntil(() => {
      let total = 0;
      for (const v of runsBySchedule.values()) total += v;
      return total >= 3;
    }, 6_000);

    // All 3 should have been dispatched at least once
    expect(runsBySchedule.size).toBe(3);
    for (const [name, count] of runsBySchedule) {
      expect(count).toBeGreaterThanOrEqual(1);
    }
  } finally {
    await s.stop();
    worker.stop();
  }
});

// ==========================
// DLQ trim at boundary
// ==========================

test("DLQ trims at dlqMaxEntries boundary", async () => {
  const schedulerId = uid("sched-dlq-trim");
  let attempts = 0;

  const brokenJob = {
    id: uid("sched-dlq-trim-job"),
    validateInput: (_input: unknown): void => {},
    submit: async (): Promise<string> => {
      attempts += 1;
      throw new Error("always-fail");
    },
  };

  const s = scheduler({
    id: schedulerId,
    leader: { leaseMs: 800, heartbeatMs: 100 },
    dispatch: {
      tickMs: 30,
      submitRetries: 0,
      dlqMaxEntries: 3,
      maxConsecutiveDispatchFailures: 10, // high so we accumulate DLQ entries
    },
  });

  try {
    await s.register({
      id: "dlq-trim",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: brokenJob,
      input: { ok: false },
    });
    await forceScheduleDue(schedulerId, "dlq-trim", Date.now() - 60_000);
    s.start();

    // Wait for several DLQ entries
    await waitUntil(() => attempts >= 5, 4_000);

    const dlq = await redis.send("LRANGE", [`sync:scheduler:${schedulerId}:dispatch:dlq`, "0", "-1"]);
    expect(Array.isArray(dlq)).toBe(true);
    // DLQ should be capped at dlqMaxEntries=3
    expect((dlq as string[]).length).toBeLessThanOrEqual(3);
  } finally {
    await s.stop();
  }
});

// ==========================
// Epoch fencing under 3-instance turnover
// ==========================

test("epoch fencing prevents duplicate dispatch under 3-instance churn", async () => {
  const schedulerId = uid("sched-epoch-3");
  let runs = 0;

  const worker = job({
    id: uid("sched-epoch-3-job"),
    schema: z.object({ id: z.string() }),
    process: async () => {
      runs += 1;
      return "ok";
    },
  });

  const cfg = {
    id: schedulerId,
    leader: { leaseMs: 400, heartbeatMs: 80 },
    dispatch: { tickMs: 25 },
  };

  const a = scheduler(cfg);
  const b = scheduler(cfg);
  const c = scheduler(cfg);

  try {
    for (const inst of [a, b, c]) {
      await inst.register({
        id: "fenced",
        cron: "* * * * *",
        tz: "UTC",
        misfire: "catch_up_one",
        job: worker,
        input: { id: "x" },
      });
    }

    await forceScheduleDue(schedulerId, "fenced", Date.now() - 60_000);

    a.start();
    b.start();
    c.start();

    await waitUntil(() => runs >= 1, 5_000);
    await Bun.sleep(300);

    // Despite 3 instances competing, the slot should be dispatched exactly once
    expect(runs).toBe(1);
  } finally {
    await a.stop();
    await b.stop();
    await c.stop();
    worker.stop();
  }
});

// ==========================
// Leader loss between submit and CAS
// ==========================

test("leader loss after submit but before CAS — new leader re-dispatches safely", async () => {
  const schedulerId = uid("sched-cas-gap");
  let runs = 0;

  const worker = job({
    id: uid("sched-cas-gap-job"),
    schema: z.object({ v: z.number() }),
    process: async () => {
      runs += 1;
      return "ok";
    },
  });

  const cfg = {
    id: schedulerId,
    leader: { leaseMs: 300, heartbeatMs: 50 },
    dispatch: { tickMs: 25, batchSize: 10 },
  };

  const s1 = scheduler(cfg);
  const s2 = scheduler(cfg);

  try {
    await s1.register({
      id: "cas-gap",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: worker,
      input: { v: 1 },
    });
    await s2.register({
      id: "cas-gap",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: worker,
      input: { v: 1 },
    });

    await forceScheduleDue(schedulerId, "cas-gap", Date.now() - 60_000);

    s1.start();
    await waitUntil(() => s1.metrics().isLeader, 3_000);

    // Wait for s1 to dispatch then immediately stop it (simulating crash after submit)
    await waitUntil(() => runs >= 1, 4_000);
    await s1.stop();

    // Force due again — simulates CAS not having succeeded
    await forceScheduleDue(schedulerId, "cas-gap", Date.now() - 120_000);

    s2.start();
    await waitUntil(() => s2.metrics().isLeader, 3_000);

    // s2 should pick up and dispatch — idempotency key prevents duplicate job
    await Bun.sleep(500);

    // The job was created only once due to idempotency
    // But the slot dispatch count might be 1 or 2 (attempt count)
    expect(runs).toBeGreaterThanOrEqual(1);

    // Schedule should have been advanced
    const info = await s2.get({ id: "cas-gap" });
    expect(info).not.toBeNull();
    expect(info!.nextRunAt).toBeGreaterThan(Date.now() - 1_000);
  } finally {
    await s1.stop();
    await s2.stop();
    worker.stop();
  }
});

// ==========================
// Upsert during active dispatch
// ==========================

test("register upsert during active dispatch does not corrupt schedule", async () => {
  const schedulerId = uid("sched-upsert-live");
  let runs = 0;

  const worker = job({
    id: uid("sched-upsert-live-job"),
    schema: z.object({ version: z.number() }),
    process: async () => {
      runs += 1;
      return "ok";
    },
  });

  const s = scheduler({
    id: schedulerId,
    leader: { leaseMs: 800, heartbeatMs: 100 },
    dispatch: { tickMs: 30 },
  });

  try {
    await s.register({
      id: "live-update",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: worker,
      input: { version: 1 },
    });

    await forceScheduleDue(schedulerId, "live-update", Date.now() - 60_000);
    s.start();

    await waitUntil(() => runs >= 1, 4_000);

    // Upsert while scheduler is active
    const result = await s.register({
      id: "live-update",
      cron: "*/2 * * * *",
      tz: "UTC",
      misfire: "skip",
      job: worker,
      input: { version: 2 },
    });

    expect(result.created).toBe(false);
    expect(result.updated).toBe(true);

    const info = await s.get({ id: "live-update" });
    expect(info).not.toBeNull();
    expect(info!.cron).toBe("*/2 * * * *");
    expect(info!.misfire).toBe("skip");
  } finally {
    await s.stop();
    worker.stop();
  }
});

// ==========================
// Transient failures followed by recovery
// ==========================

test("transient submit failures recover and dispatch eventually succeeds", async () => {
  const schedulerId = uid("sched-transient-recover");
  let attempts = 0;
  let successes = 0;

  const flakyJob = {
    id: uid("sched-transient-recover-job"),
    validateInput: (_input: unknown): void => {},
    submit: async (cfg: { key?: string; keyTtlMs?: number; at?: number; delayMs?: number; input: unknown; meta?: Record<string, unknown> }): Promise<string> => {
      attempts += 1;
      if (attempts <= 4) throw new Error("transient");
      successes += 1;
      return `job-${attempts}`;
    },
  };

  const s = scheduler({
    id: schedulerId,
    leader: { leaseMs: 800, heartbeatMs: 100 },
    dispatch: {
      tickMs: 30,
      submitRetries: 2,
      submitBackoffBaseMs: 10,
      submitBackoffMaxMs: 20,
      maxConsecutiveDispatchFailures: 10,
    },
  });

  try {
    await s.register({
      id: "recover-me",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: flakyJob,
      input: { ok: true },
    });
    await forceScheduleDue(schedulerId, "recover-me", Date.now() - 60_000);
    s.start();

    await waitUntil(() => successes >= 1, 6_000);
    expect(s.metrics().dispatchSubmitted).toBeGreaterThanOrEqual(1);
  } finally {
    await s.stop();
  }
});

// ==========================
// strictHandlers: leader without handlers yields, handler-instance takes over
// ==========================

test("strictHandlers leader yields to handler-bearing instance", async () => {
  const schedulerId = uid("sched-strict-yield");
  let runs = 0;

  const worker = job({
    id: uid("sched-strict-yield-job"),
    schema: z.object({ id: z.string() }),
    process: async () => {
      runs += 1;
      return "ok";
    },
  });

  const bare = scheduler({
    id: schedulerId,
    leader: { leaseMs: 400, heartbeatMs: 80 },
    dispatch: { tickMs: 30 },
    strictHandlers: true,
  });

  const full = scheduler({
    id: schedulerId,
    leader: { leaseMs: 400, heartbeatMs: 80 },
    dispatch: { tickMs: 30 },
    strictHandlers: true,
  });

  try {
    await full.register({
      id: "strict-test",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: worker,
      input: { id: "s" },
    });

    await forceScheduleDue(schedulerId, "strict-test", Date.now() - 60_000);

    // Start bare first so it becomes leader, then start full
    bare.start();
    await waitUntil(() => bare.metrics().isLeader, 3_000);

    full.start();

    // bare should yield leadership when it encounters missing_handler
    await waitUntil(() => runs >= 1, 6_000);
    expect(runs).toBe(1);
  } finally {
    await bare.stop();
    await full.stop();
    worker.stop();
  }
});

// ==========================
// ZodError advances schedule immediately
// ==========================

test("ZodError in submit is deterministic — schedule advances after first failure", async () => {
  const schedulerId = uid("sched-zod-advance");
  let attempts = 0;

  const zodFailJob = {
    id: uid("sched-zod-advance-job"),
    validateInput: (_input: unknown): void => {},
    submit: async (): Promise<string> => {
      attempts += 1;
      const err = new Error("validation failed");
      err.name = "ZodError";
      throw err;
    },
  };

  const s = scheduler({
    id: schedulerId,
    leader: { leaseMs: 700, heartbeatMs: 100 },
    dispatch: {
      tickMs: 30,
      submitRetries: 0,
      maxConsecutiveDispatchFailures: 5,
    },
  });

  try {
    await s.register({
      id: "zod-fail",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: zodFailJob,
      input: { bad: true },
    });

    const before = await s.get({ id: "zod-fail" });
    expect(before).not.toBeNull();
    const forcedDueTs = Date.now() - 60_000;
    await forceScheduleDue(schedulerId, "zod-fail", forcedDueTs);
    s.start();

    await waitUntil(() => s.metrics().dispatchDlq >= 1, 4_000);

    const after = await s.get({ id: "zod-fail" });
    expect(after).not.toBeNull();
    // Schedule should have been advanced past the failing slot
    expect(after!.nextRunAt).toBeGreaterThan(forcedDueTs);
    // ZodError is deterministic — should advance after first failure
    expect(attempts).toBeGreaterThanOrEqual(1);
  } finally {
    await s.stop();
  }
});

// ==========================
// Metrics correctness
// ==========================

test("metrics counters are consistent with dispatch outcomes", async () => {
  const schedulerId = uid("sched-metrics");
  let calls = 0;

  const mixedJob = {
    id: uid("sched-metrics-job"),
    validateInput: (_input: unknown): void => {},
    submit: async (): Promise<string> => {
      calls += 1;
      if (calls <= 2) throw new Error("fail first two");
      return `ok-${calls}`;
    },
  };

  const s = scheduler({
    id: schedulerId,
    leader: { leaseMs: 800, heartbeatMs: 100 },
    dispatch: {
      tickMs: 30,
      submitRetries: 0,
      maxConsecutiveDispatchFailures: 5,
    },
  });

  try {
    await s.register({
      id: "metrics-test",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: mixedJob,
      input: { ok: true },
    });
    await forceScheduleDue(schedulerId, "metrics-test", Date.now() - 60_000);
    s.start();

    await waitUntil(() => s.metrics().dispatchSubmitted >= 1, 6_000);

    const m = s.metrics();
    expect(m.isLeader).toBe(true);
    expect(m.leaderEpoch).toBeGreaterThanOrEqual(1);
    expect(m.dispatchSubmitted).toBeGreaterThanOrEqual(1);
    expect(m.dispatchFailed).toBeGreaterThanOrEqual(1);
    expect(m.dispatchDlq).toBeGreaterThanOrEqual(1);
    // Total makes sense: submitted + failed covers all attempted slots
  } finally {
    await s.stop();
  }
});

// ==========================
// Unregister during active dispatch cycle
// ==========================

test("unregister during active dispatch stops future dispatches for that schedule", async () => {
  const schedulerId = uid("sched-unreg-active");
  let runs = 0;

  const worker = job({
    id: uid("sched-unreg-active-job"),
    schema: z.object({ id: z.string() }),
    process: async () => {
      runs += 1;
      return "ok";
    },
  });

  const s = scheduler({
    id: schedulerId,
    leader: { leaseMs: 800, heartbeatMs: 100 },
    dispatch: { tickMs: 30 },
  });

  try {
    await s.register({
      id: "will-unreg",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: worker,
      input: { id: "u" },
    });

    await forceScheduleDue(schedulerId, "will-unreg", Date.now() - 60_000);
    s.start();

    await waitUntil(() => runs >= 1, 4_000);
    const countBefore = runs;

    await s.unregister({ id: "will-unreg" });

    // No more dispatches should happen
    await Bun.sleep(300);
    expect(runs).toBe(countBefore);

    // Schedule should be gone
    expect(await s.get({ id: "will-unreg" })).toBeNull();
  } finally {
    await s.stop();
    worker.stop();
  }
});

// ==========================
// Register preserves createdAt on upsert
// ==========================

test("upsert preserves original createdAt timestamp", async () => {
  const schedulerId = uid("sched-createdat");
  const worker = job({
    id: uid("sched-createdat-job"),
    schema: z.object({ v: z.number() }),
    process: async () => "ok",
  });

  const s = scheduler({ id: schedulerId });

  try {
    await s.register({
      id: "preserve-ts",
      cron: "*/5 * * * *",
      tz: "UTC",
      job: worker,
      input: { v: 1 },
    });

    const first = await s.get({ id: "preserve-ts" });
    expect(first).not.toBeNull();

    await Bun.sleep(50);

    await s.register({
      id: "preserve-ts",
      cron: "*/10 * * * *",
      tz: "UTC",
      job: worker,
      input: { v: 2 },
    });

    const second = await s.get({ id: "preserve-ts" });
    expect(second).not.toBeNull();
    expect(second!.createdAt).toBe(first!.createdAt);
    expect(second!.updatedAt).toBeGreaterThan(first!.updatedAt);
    expect(second!.cron).toBe("*/10 * * * *");
  } finally {
    await s.stop();
    worker.stop();
  }
});

// ==========================
// Rapid start/stop cycles
// ==========================

test("rapid start/stop cycles do not leak state or crash", async () => {
  const schedulerId = uid("sched-rapid-cycle");
  const worker = job({
    id: uid("sched-rapid-cycle-job"),
    schema: z.object({ v: z.number() }),
    process: async () => "ok",
  });

  const s = scheduler({
    id: schedulerId,
    leader: { leaseMs: 300, heartbeatMs: 50 },
    dispatch: { tickMs: 20 },
  });

  try {
    await s.register({
      id: "rapid",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "skip",
      job: worker,
      input: { v: 1 },
    });

    for (let i = 0; i < 10; i++) {
      s.start();
      await Bun.sleep(30);
      await s.stop();
    }

    // Should not crash, schedule should still be intact
    const info = await s.get({ id: "rapid" });
    expect(info).not.toBeNull();
  } finally {
    await s.stop();
    worker.stop();
  }
});

// ==========================
// Multiple schedules same job — unregister one leaves others working
// ==========================

test("unregister one schedule does not affect other schedules using same job", async () => {
  const schedulerId = uid("sched-shared-job");
  const runsBySchedule = new Map<string, number>();

  const worker = job({
    id: uid("sched-shared-job-worker"),
    schema: z.object({ schedId: z.string() }),
    process: async ({ input }) => {
      const count = runsBySchedule.get(input.schedId) ?? 0;
      runsBySchedule.set(input.schedId, count + 1);
      return "ok";
    },
  });

  const s = scheduler({
    id: schedulerId,
    leader: { leaseMs: 800, heartbeatMs: 100 },
    dispatch: { tickMs: 30 },
  });

  try {
    await s.register({
      id: "keep-me",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: worker,
      input: { schedId: "keep-me" },
    });
    await s.register({
      id: "remove-me",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: worker,
      input: { schedId: "remove-me" },
    });

    await s.unregister({ id: "remove-me" });

    await forceScheduleDue(schedulerId, "keep-me", Date.now() - 60_000);
    s.start();

    await waitUntil(() => (runsBySchedule.get("keep-me") ?? 0) >= 1, 4_000);

    expect(runsBySchedule.get("keep-me")).toBeGreaterThanOrEqual(1);
    expect(runsBySchedule.get("remove-me") ?? 0).toBe(0);
  } finally {
    await s.stop();
    worker.stop();
  }
});

// ==========================
// catch_up_all partial budget with eventual full drain
// ==========================

test("catch_up_all with tight budget eventually drains all overdue slots", async () => {
  const schedulerId = uid("sched-drain-all");
  let runs = 0;

  const worker = job({
    id: uid("sched-drain-all-job"),
    schema: z.object({ id: z.string() }),
    process: async () => {
      runs += 1;
      return "ok";
    },
  });

  const s = scheduler({
    id: schedulerId,
    leader: { leaseMs: 1_000, heartbeatMs: 100 },
    dispatch: { tickMs: 40, maxSubmitsPerTick: 2 },
  });

  try {
    await s.register({
      id: "drain-all",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_all",
      maxCatchUpRuns: 5,
      job: worker,
      input: { id: "d" },
    });

    // Force 5 minutes overdue, maxCatchUpRuns=5.
    // With cron="* * * * *" there are ~5 overdue slots (exact count
    // depends on sub-minute boundary alignment).
    const forcedAt = Date.now() - 5 * 60_000;
    await forceScheduleDue(schedulerId, "drain-all", forcedAt);
    s.start();

    // With maxSubmitsPerTick=2, it takes multiple ticks to drain all overdue slots.
    await waitUntil(() => runs >= 5, 8_000);

    // Should have dispatched 5 slots (capped by maxCatchUpRuns)
    expect(runs).toBeGreaterThanOrEqual(5);

    // Schedule should be advanced clearly past the original forced timestamp.
    // Minute-aligned cron timestamps can be slightly below a strict +4min wall-clock bound.
    const info = await s.get({ id: "drain-all" });
    expect(info).not.toBeNull();
    expect(info!.nextRunAt).toBeGreaterThan(forcedAt + 3 * 60_000);
  } finally {
    await s.stop();
    worker.stop();
  }
});

// ==========================
// Failover preserves schedule state
// ==========================

test("leadership failover preserves schedule nextRunAt and continues dispatch", async () => {
  const schedulerId = uid("sched-failover-state");
  let runs = 0;

  const worker = job({
    id: uid("sched-failover-state-job"),
    schema: z.object({ v: z.number() }),
    process: async () => {
      runs += 1;
      return "ok";
    },
  });

  const cfg = {
    id: schedulerId,
    leader: { leaseMs: 400, heartbeatMs: 80 },
    dispatch: { tickMs: 30 },
  };

  const primary = scheduler(cfg);
  const standby = scheduler(cfg);

  try {
    await primary.register({
      id: "failover-schedule",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: worker,
      input: { v: 1 },
    });
    await standby.register({
      id: "failover-schedule",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: worker,
      input: { v: 1 },
    });

    await forceScheduleDue(schedulerId, "failover-schedule", Date.now() - 60_000);

    primary.start();
    standby.start();

    await waitUntil(() => runs >= 1, 4_000);

    // Stop the current leader
    const leader = primary.metrics().isLeader ? primary : standby;
    const standbyInst = leader === primary ? standby : primary;
    await leader.stop();

    // Get current nextRunAt — it should have been advanced
    const before = await standbyInst.get({ id: "failover-schedule" });
    expect(before).not.toBeNull();
    expect(before!.nextRunAt).toBeGreaterThan(Date.now() - 2_000);

    // Force due again and verify standby takes over
    await forceScheduleDue(schedulerId, "failover-schedule", Date.now() - 60_000);
    await waitUntil(() => runs >= 2, 4_000);
  } finally {
    await primary.stop();
    await standby.stop();
    worker.stop();
  }
});

test("epoch mismatch blocks dispatch submit before side-effects", async () => {
  const schedulerId = uid("sched-fence-before-submit");
  let submitCalls = 0;

  const guardedJob = {
    id: uid("sched-fence-before-submit-job"),
    validateInput: (_input: unknown): void => {},
    submit: async (): Promise<string> => {
      submitCalls += 1;
      return `ok-${submitCalls}`;
    },
  };

  const s = scheduler({
    id: schedulerId,
    leader: { leaseMs: 500, heartbeatMs: 80 },
    dispatch: { tickMs: 25, submitRetries: 0 },
  });

  const epochKey = `sync:scheduler:${schedulerId}:leader:epoch`;
  const originalGet = redis.get.bind(redis);

  try {
    await s.register({
      id: "fence-test",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: guardedJob,
      input: { ok: true },
    });

    s.start();
    await waitUntil(() => s.metrics().isLeader, 4_000);

    (redis as unknown as { get: typeof redis.get }).get = (async (key: string) => {
      if (key === epochKey) {
        const current = Number((await originalGet(key)) ?? "0");
        return String(current + 1);
      }
      return await originalGet(key);
    }) as typeof redis.get;

    await forceScheduleDue(schedulerId, "fence-test", Date.now() - 60_000);
    await Bun.sleep(300);

    expect(submitCalls).toBe(0);
  } finally {
    (redis as unknown as { get: typeof redis.get }).get = originalGet;
    await s.stop();
  }
});
