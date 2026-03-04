import { beforeEach, expect, test } from "bun:test";
import { redis } from "bun";
import { z } from "zod";
import { job, scheduler } from "../index";

const uid = (name: string): string => `${name}-${Date.now()}-${Math.floor(Math.random() * 1_000_000)}`;

const waitUntil = async (predicate: () => boolean, timeoutMs = 5_000): Promise<void> => {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (predicate()) return;
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

test("register is idempotent for duplicate schedule ids", async () => {
  const schedulerId = uid("sched-idem");
  const worker = job({
    id: uid("sched-idem-job"),
    schema: z.object({ n: z.number() }),
    process: async () => "ok",
  });

  const s = scheduler({ id: schedulerId });
  try {
    const a = await s.register({
      id: "sync-cleanup",
      cron: "*/5 * * * *",
      tz: "Europe/Berlin",
      job: worker,
      input: { n: 1 },
    });
    const b = await s.register({
      id: "sync-cleanup",
      cron: "*/5 * * * *",
      tz: "Europe/Berlin",
      job: worker,
      input: { n: 1 },
    });

    expect(a.created).toBe(true);
    expect(b.created).toBe(false);
    expect(b.updated).toBe(true);

    const info = await s.get({ id: "sync-cleanup" });
    expect(info).not.toBeNull();
    expect(info?.id).toBe("sync-cleanup");

    const all = await s.list();
    expect(all.length).toBe(1);
    expect(all[0].id).toBe("sync-cleanup");
  } finally {
    await s.stop();
    worker.stop();
  }
});

test("register upserts existing schedule without unregister gap", async () => {
  const schedulerId = uid("sched-upsert");
  const worker = job({
    id: uid("sched-upsert-job"),
    schema: z.object({ n: z.number() }),
    process: async () => "ok",
  });

  const s = scheduler({ id: schedulerId });
  try {
    const a = await s.register({
      id: "sync-upsert",
      cron: "*/5 * * * *",
      tz: "UTC",
      job: worker,
      input: { n: 1 },
    });
    expect(a.created).toBe(true);

    const before = await s.get({ id: "sync-upsert" });
    expect(before).not.toBeNull();

    const b = await s.register({
      id: "sync-upsert",
      cron: "*/10 * * * *",
      tz: "UTC",
      job: worker,
      input: { n: 2 },
    });

    expect(b.created).toBe(false);
    expect(b.updated).toBe(true);

    const after = await s.get({ id: "sync-upsert" });
    expect(after).not.toBeNull();
    expect(after!.cron).toBe("*/10 * * * *");
    expect(after!.nextRunAt).toBeGreaterThan(Date.now() - 1_000);
  } finally {
    await s.stop();
    worker.stop();
  }
});

test("two scheduler instances dispatch a due slot only once", async () => {
  const schedulerId = uid("sched-dual");
  let runs = 0;

  const worker = job({
    id: uid("sched-dual-job"),
    schema: z.object({ task: z.string() }),
    process: async () => {
      runs += 1;
      return "ok";
    },
  });

  const cfg = {
    id: schedulerId,
    leader: { leaseMs: 400, heartbeatMs: 100 },
    dispatch: { tickMs: 30, batchSize: 50 },
  };

  const s1 = scheduler(cfg);
  const s2 = scheduler(cfg);

  try {
    await s1.register({
      id: "shared-sync",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: worker,
      input: { task: "sync" },
    });
    await s2.register({
      id: "shared-sync",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: worker,
      input: { task: "sync" },
    });

    await forceScheduleDue(schedulerId, "shared-sync", Date.now() - 60_000);

    s1.start();
    s2.start();

    await waitUntil(() => runs === 1, 4_000);
    await Bun.sleep(200);
    expect(runs).toBe(1);
  } finally {
    await s1.stop();
    await s2.stop();
    worker.stop();
  }
});

test("leadership failover keeps scheduling alive when one node stops", async () => {
  const schedulerId = uid("sched-failover");
  let runs = 0;

  const worker = job({
    id: uid("sched-failover-job"),
    schema: z.object({ v: z.number() }),
    process: async () => {
      runs += 1;
      return "ok";
    },
  });

  const cfg = {
    id: schedulerId,
    leader: { leaseMs: 500, heartbeatMs: 100 },
    dispatch: { tickMs: 30, batchSize: 20 },
  };

  const s1 = scheduler(cfg);
  const s2 = scheduler(cfg);

  try {
    await s1.register({
      id: "periodic-sync",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: worker,
      input: { v: 1 },
    });
    await s2.register({
      id: "periodic-sync",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: worker,
      input: { v: 1 },
    });

    await forceScheduleDue(schedulerId, "periodic-sync", Date.now() - 120_000);

    s1.start();
    s2.start();
    await waitUntil(() => runs === 1, 4_000);

    const firstLeader = s1.metrics().isLeader ? s1 : s2;
    await firstLeader.stop();

    await forceScheduleDue(schedulerId, "periodic-sync", Date.now() - 180_000);
    await waitUntil(() => runs === 2, 4_000);
  } finally {
    await s1.stop();
    await s2.stop();
    worker.stop();
  }
});

test("misfire skip does not catch up overdue executions", async () => {
  const schedulerId = uid("sched-skip");
  let runs = 0;

  const worker = job({
    id: uid("sched-skip-job"),
    schema: z.object({ id: z.string() }),
    process: async () => {
      runs += 1;
      return "ok";
    },
  });

  const s = scheduler({
    id: schedulerId,
    leader: { leaseMs: 400, heartbeatMs: 100 },
    dispatch: { tickMs: 30, batchSize: 20 },
  });

  try {
    await s.register({
      id: "cleanup",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "skip",
      job: worker,
      input: { id: "x" },
    });

    await forceScheduleDue(schedulerId, "cleanup", Date.now() - 6 * 60_000);

    s.start();
    await Bun.sleep(350);
    expect(runs).toBe(0);

    const info = await s.get({ id: "cleanup" });
    expect(info).not.toBeNull();
    expect(info!.nextRunAt).toBeGreaterThan(Date.now());
  } finally {
    await s.stop();
    worker.stop();
  }
});

test("misfire catch_up_one catches up only one overdue run", async () => {
  const schedulerId = uid("sched-catch-one");
  let runs = 0;

  const worker = job({
    id: uid("sched-catch-one-job"),
    schema: z.object({ id: z.string() }),
    process: async () => {
      runs += 1;
      return "ok";
    },
  });

  const s = scheduler({
    id: schedulerId,
    leader: { leaseMs: 400, heartbeatMs: 100 },
    dispatch: { tickMs: 30, batchSize: 20 },
  });

  try {
    await s.register({
      id: "sync",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: worker,
      input: { id: "y" },
    });

    await forceScheduleDue(schedulerId, "sync", Date.now() - 12 * 60_000);

    s.start();
    await waitUntil(() => runs === 1, 4_000);
    await Bun.sleep(300);
    expect(runs).toBe(1);
  } finally {
    await s.stop();
    worker.stop();
  }
});

test("catch_up_all dispatches multiple overdue slots and respects maxCatchUpRuns", async () => {
  const schedulerId = uid("sched-catch-all");
  let runs = 0;

  const worker = job({
    id: uid("sched-catch-all-job"),
    schema: z.object({ id: z.string() }),
    process: async () => {
      runs += 1;
      return "ok";
    },
  });

  const s = scheduler({
    id: schedulerId,
    leader: { leaseMs: 5_000, heartbeatMs: 500 },
    dispatch: { tickMs: 200, batchSize: 20 },
  });

  try {
    await s.register({
      id: "sync-all",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_all",
      maxCatchUpRuns: 3,
      job: worker,
      input: { id: "z" },
    });

    await forceScheduleDue(schedulerId, "sync-all", Date.now() - 10 * 60_000);

    s.start();
    await Bun.sleep(120);
    await s.stop();
    expect(runs).toBe(3);
  } finally {
    await s.stop();
    worker.stop();
  }
});

test("register rejects invalid cron expression", async () => {
  const s = scheduler({ id: uid("sched-bad-cron") });
  const worker = job({
    id: uid("sched-bad-cron-job"),
    schema: z.object({ ok: z.boolean() }),
    process: async () => "ok",
  });

  try {
    let thrown: unknown = null;
    try {
      await s.register({
        id: "bad-cron",
        cron: "* * *",
        tz: "UTC",
        job: worker,
        input: { ok: true },
      });
    } catch (error) {
      thrown = error;
    }
    expect(thrown).not.toBeNull();
  } finally {
    await s.stop();
    worker.stop();
  }
});

test("unregister removes schedule from storage and stops dispatch", async () => {
  const schedulerId = uid("sched-unreg");
  let runs = 0;

  const worker = job({
    id: uid("sched-unreg-job"),
    schema: z.object({ id: z.string() }),
    process: async () => {
      runs += 1;
      return "ok";
    },
  });

  const s = scheduler({
    id: schedulerId,
    leader: { leaseMs: 400, heartbeatMs: 100 },
    dispatch: { tickMs: 30, batchSize: 20 },
  });

  try {
    await s.register({
      id: "cleanup-unreg",
      cron: "* * * * *",
      tz: "UTC",
      job: worker,
      input: { id: "u" },
    });
    await s.unregister({ id: "cleanup-unreg" });

    expect(await s.get({ id: "cleanup-unreg" })).toBeNull();
    expect((await s.list()).length).toBe(0);

    // Even if someone manually re-adds due entry, scheduler should clean it up.
    await redis.send("ZADD", [`sync:scheduler:${schedulerId}:due`, String(Date.now() - 60_000), "cleanup-unreg"]);

    s.start();
    await Bun.sleep(250);
    expect(runs).toBe(0);

    const due = await redis.send("ZRANGE", [`sync:scheduler:${schedulerId}:due`, "0", "-1"]);
    expect(Array.isArray(due) ? due.includes("cleanup-unreg") : false).toBe(false);
  } finally {
    await s.stop();
    worker.stop();
  }
});

test("register rejects invalid timezone", async () => {
  const s = scheduler({ id: uid("sched-bad-tz") });
  const worker = job({
    id: uid("sched-bad-tz-job"),
    schema: z.object({ ok: z.boolean() }),
    process: async () => "ok",
  });

  try {
    let thrown: unknown = null;
    try {
      await s.register({
        id: "bad-tz",
        cron: "* * * * *",
        tz: "Not/A_Real_Timezone",
        job: worker,
        input: { ok: true },
      });
    } catch (error) {
      thrown = error;
    }

    expect(thrown).not.toBeNull();
  } finally {
    await s.stop();
    worker.stop();
  }
});

test("register validates input via job handle before persisting schedule", async () => {
  const schedulerId = uid("sched-input-validate");
  const s = scheduler({ id: schedulerId });
  const worker = job({
    id: uid("sched-input-validate-job"),
    schema: z.object({ count: z.number().int().min(1) }),
    process: async () => "ok",
  });

  try {
    let thrown: unknown = null;
    try {
      await s.register({
        id: "bad-input",
        cron: "* * * * *",
        tz: "UTC",
        job: worker,
        // @ts-expect-error intentional invalid payload
        input: { count: 0 },
      });
    } catch (error) {
      thrown = error;
    }

    expect(thrown).not.toBeNull();
    expect(await s.get({ id: "bad-input" })).toBeNull();
  } finally {
    await s.stop();
    worker.stop();
  }
});

test("cron day-of-week range 1-7 is accepted", async () => {
  const s = scheduler({ id: uid("sched-dow-range") });
  const worker = job({
    id: uid("sched-dow-range-job"),
    schema: z.object({ ok: z.boolean() }),
    process: async () => "ok",
  });

  try {
    const result = await s.register({
      id: "dow-1-7",
      cron: "0 0 * * 1-7",
      tz: "UTC",
      job: worker,
      input: { ok: true },
    });
    expect(result.created).toBe(true);
  } finally {
    await s.stop();
    worker.stop();
  }
});

test("scheduler retries transient submit failures before success", async () => {
  const schedulerId = uid("sched-retry-submit");
  let attempts = 0;
  const submittedKeys: string[] = [];

  const flakyJob = {
    id: uid("sched-retry-submit-job"),
    validateInput: (_input: unknown): void => {},
    submit: async (cfg: { key?: string; keyTtlMs?: number; at?: number; delayMs?: number; input: unknown; meta?: Record<string, unknown> }): Promise<string> => {
      attempts += 1;
      if (cfg.key) submittedKeys.push(cfg.key);
      if (attempts < 3) throw new Error("transient");
      return "ok";
    },
  };

  const s = scheduler({
    id: schedulerId,
    leader: { leaseMs: 600, heartbeatMs: 100 },
    dispatch: {
      tickMs: 30,
      submitRetries: 3,
      submitBackoffBaseMs: 10,
      submitBackoffMaxMs: 30,
      maxSubmitsPerTick: 5,
    },
  });

  try {
    await s.register({
      id: "retry-me",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: flakyJob,
      input: { ok: true },
    });
    await forceScheduleDue(schedulerId, "retry-me", Date.now() - 2 * 60_000);
    s.start();

    await waitUntil(() => attempts >= 3, 4_000);
    const m = s.metrics();
    expect(m.dispatchSubmitted).toBe(1);
    expect(m.dispatchRetried).toBe(2);
    expect(submittedKeys.length).toBeGreaterThanOrEqual(3);
  } finally {
    await s.stop();
  }
});

test("scheduler writes dispatch failures to dlq after retries exhausted", async () => {
  const schedulerId = uid("sched-dlq");
  let attempts = 0;

  const brokenJob = {
    id: uid("sched-dlq-job"),
    validateInput: (_input: unknown): void => {},
    submit: async (_cfg: { key?: string; keyTtlMs?: number; at?: number; delayMs?: number; input: unknown; meta?: Record<string, unknown> }): Promise<string> => {
      attempts += 1;
      throw new Error("always-fail");
    },
  };

  const s = scheduler({
    id: schedulerId,
    leader: { leaseMs: 600, heartbeatMs: 100 },
    dispatch: {
      tickMs: 30,
      submitRetries: 1,
      submitBackoffBaseMs: 10,
      submitBackoffMaxMs: 20,
      dlqMaxEntries: 10,
    },
  });

  try {
    await s.register({
      id: "dlq-me",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: brokenJob,
      input: { ok: true },
    });
    await forceScheduleDue(schedulerId, "dlq-me", Date.now() - 60_000);
    s.start();

    await waitUntil(() => s.metrics().dispatchDlq >= 1, 4_000);
    expect(attempts).toBeGreaterThanOrEqual(2); // initial + retry

    const dlq = await redis.send("LRANGE", [`sync:scheduler:${schedulerId}:dispatch:dlq`, "0", "-1"]);
    expect(Array.isArray(dlq)).toBe(true);
    expect((dlq as string[]).length).toBeGreaterThanOrEqual(1);
    const entry = JSON.parse(String((dlq as string[])[0]));
    expect(entry.scheduleId).toBe("dlq-me");
  } finally {
    await s.stop();
  }
});

test("scheduler advances schedule after repeated deterministic failures", async () => {
  const schedulerId = uid("sched-advance-failures");
  let attempts = 0;

  const badInputJob = {
    id: uid("sched-advance-failures-job"),
    validateInput: (_input: unknown): void => {},
    submit: async (_cfg: { key?: string; keyTtlMs?: number; at?: number; delayMs?: number; input: unknown; meta?: Record<string, unknown> }): Promise<string> => {
      attempts += 1;
      const err = new Error("invalid input shape");
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
      maxConsecutiveDispatchFailures: 2,
    },
  });

  try {
    await s.register({
      id: "fail-advance",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: badInputJob,
      input: { ok: false },
    });
    const before = await s.get({ id: "fail-advance" });
    expect(before).not.toBeNull();
    const forcedDueTs = Date.now() - 60_000;
    await forceScheduleDue(schedulerId, "fail-advance", forcedDueTs);
    s.start();

    await waitUntil(() => s.metrics().dispatchDlq >= 1, 4_000);
    const after = await s.get({ id: "fail-advance" });
    expect(after).not.toBeNull();
    expect(after!.nextRunAt).toBeGreaterThan(forcedDueTs);
    expect(attempts).toBeGreaterThanOrEqual(1);
  } finally {
    await s.stop();
  }
});

test("strictHandlers causes leader without handlers to yield", async () => {
  const schedulerId = uid("sched-strict-handlers");
  let runs = 0;

  const worker = job({
    id: uid("sched-strict-handlers-job"),
    schema: z.object({ id: z.string() }),
    process: async () => {
      runs += 1;
      return "ok";
    },
  });

  const withoutHandlers = scheduler({
    id: schedulerId,
    leader: { leaseMs: 500, heartbeatMs: 100 },
    dispatch: { tickMs: 40 },
    strictHandlers: true,
  });

  const withHandlers = scheduler({
    id: schedulerId,
    leader: { leaseMs: 500, heartbeatMs: 100 },
    dispatch: { tickMs: 40 },
    strictHandlers: true,
  });

  try {
    await withHandlers.register({
      id: "strict-sync",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: worker,
      input: { id: "a" },
    });

    await forceScheduleDue(schedulerId, "strict-sync", Date.now() - 60_000);

    withoutHandlers.start();
    withHandlers.start();

    await waitUntil(() => runs === 1, 5_000);
    expect(runs).toBe(1);
  } finally {
    await withoutHandlers.stop();
    await withHandlers.stop();
    worker.stop();
  }
});

test("catch_up_all with maxSubmitsPerTick drains overdue slots across ticks", async () => {
  const schedulerId = uid("sched-budget-drain");
  let runs = 0;

  const worker = job({
    id: uid("sched-budget-drain-job"),
    schema: z.object({ id: z.string() }),
    process: async () => {
      runs += 1;
      return "ok";
    },
  });

  const s = scheduler({
    id: schedulerId,
    leader: { leaseMs: 800, heartbeatMs: 100 },
    dispatch: {
      tickMs: 40,
      maxSubmitsPerTick: 2,
    },
  });

  try {
    await s.register({
      id: "budgeted-catchup",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_all",
      maxCatchUpRuns: 6,
      job: worker,
      input: { id: "b" },
    });

    await forceScheduleDue(schedulerId, "budgeted-catchup", Date.now() - 8 * 60_000);
    s.start();
    await waitUntil(() => runs >= 6, 6_000);
    expect(runs).toBe(6);
  } finally {
    await s.stop();
    worker.stop();
  }
});

test("non-deterministic dispatch failures advance after configured failure threshold", async () => {
  const schedulerId = uid("sched-failure-threshold");
  let attempts = 0;

  const flaky = {
    id: uid("sched-failure-threshold-job"),
    validateInput: (_input: unknown): void => {},
    submit: async (_cfg: { key?: string; keyTtlMs?: number; at?: number; delayMs?: number; input: unknown; meta?: Record<string, unknown> }): Promise<string> => {
      attempts += 1;
      throw new Error("network-timeout-ish");
    },
  };

  const s = scheduler({
    id: schedulerId,
    leader: { leaseMs: 800, heartbeatMs: 100 },
    dispatch: {
      tickMs: 30,
      submitRetries: 0,
      maxConsecutiveDispatchFailures: 3,
    },
  });

  try {
    await s.register({
      id: "flaky-threshold",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: flaky,
      input: { v: 1 },
    });

    await forceScheduleDue(schedulerId, "flaky-threshold", Date.now() - 60_000);
    const before = await s.get({ id: "flaky-threshold" });
    expect(before).not.toBeNull();

    s.start();
    await waitUntil(() => attempts >= 3, 6_000);
    await waitUntil(() => {
      const m = s.metrics();
      return m.dispatchDlq >= 3;
    }, 6_000);

    const after = await s.get({ id: "flaky-threshold" });
    expect(after).not.toBeNull();
    expect(after!.nextRunAt).toBeGreaterThan(before!.nextRunAt);
  } finally {
    await s.stop();
  }
});

test("register upsert can switch job handler for existing schedule id", async () => {
  const schedulerId = uid("sched-switch-job");
  let aRuns = 0;
  let bRuns = 0;

  const jobA = job({
    id: uid("sched-switch-job-a"),
    schema: z.object({ id: z.string() }),
    process: async () => {
      aRuns += 1;
      return "a";
    },
  });

  const jobB = job({
    id: uid("sched-switch-job-b"),
    schema: z.object({ id: z.string() }),
    process: async () => {
      bRuns += 1;
      return "b";
    },
  });

  const s = scheduler({
    id: schedulerId,
    leader: { leaseMs: 800, heartbeatMs: 100 },
    dispatch: { tickMs: 30 },
  });

  try {
    await s.register({
      id: "switchable",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: jobA,
      input: { id: "x" },
    });

    await s.register({
      id: "switchable",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: jobB,
      input: { id: "x" },
    });

    await forceScheduleDue(schedulerId, "switchable", Date.now() - 60_000);
    s.start();

    await waitUntil(() => bRuns === 1, 4_000);
    expect(aRuns).toBe(0);
    expect(bRuns).toBe(1);
  } finally {
    await s.stop();
    jobA.stop();
    jobB.stop();
  }
});

test("strictHandlers false keeps scheduler running but skips missing handlers", async () => {
  const schedulerId = uid("sched-non-strict");
  const s = scheduler({
    id: schedulerId,
    strictHandlers: false,
    leader: { leaseMs: 700, heartbeatMs: 100 },
    dispatch: { tickMs: 30 },
  });

  // Insert a schedule directly without registering handler on this instance.
  const scheduleId = "missing-handler";
  const now = Date.now();
  const scheduleKey = `sync:scheduler:${schedulerId}:schedule:${scheduleId}`;
  const schedule = {
    id: scheduleId,
    cron: "* * * * *",
    tz: "UTC",
    misfire: "catch_up_one",
    maxCatchUpRuns: 1,
    jobId: "unregistered-job-id",
    input: { v: 1 },
    createdAt: now,
    updatedAt: now,
    nextRunAt: now - 60_000,
    consecutiveDispatchFailures: 0,
  };

  try {
    await redis.send("SET", [scheduleKey, JSON.stringify(schedule)]);
    await redis.send("ZADD", [`sync:scheduler:${schedulerId}:due`, String(schedule.nextRunAt), scheduleId]);
    await redis.send("SADD", [`sync:scheduler:${schedulerId}:index`, scheduleId]);

    s.start();
    await waitUntil(() => s.metrics().dispatchSkipped >= 1, 4_000);
    expect(s.metrics().isLeader).toBe(true);
  } finally {
    await s.stop();
  }
});

test("chaos: scheduler recovers from intermittent redis read failures", async () => {
  const schedulerId = uid("sched-chaos-redis");
  let runs = 0;

  const worker = job({
    id: uid("sched-chaos-redis-job"),
    schema: z.object({ id: z.string() }),
    process: async () => {
      runs += 1;
      return "ok";
    },
  });

  const s = scheduler({
    id: schedulerId,
    leader: { leaseMs: 900, heartbeatMs: 120 },
    dispatch: { tickMs: 30, submitRetries: 1 },
  });

  const redisObj = redis as unknown as { send: (...args: unknown[]) => Promise<unknown> };
  const originalSend = redisObj.send;
  let injected = 0;
  redisObj.send = async (...args: unknown[]) => {
    const cmd = String(args[0] ?? "");
    if (cmd === "ZRANGEBYSCORE" && injected < 4) {
      injected += 1;
      throw new Error("injected redis timeout");
    }
    return await originalSend.call(redis, ...args);
  };

  try {
    await s.register({
      id: "chaos-redis",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: worker,
      input: { id: "r" },
    });
    await forceScheduleDue(schedulerId, "chaos-redis", Date.now() - 60_000);

    s.start();
    await waitUntil(() => runs === 1, 8_000);
    expect(s.metrics().tickErrors).toBeGreaterThanOrEqual(1);
  } finally {
    redisObj.send = originalSend;
    await s.stop();
    worker.stop();
  }
});

test("chaos: rapid leader churn across three instances keeps slot execution single", async () => {
  const schedulerId = uid("sched-chaos-churn");
  let runs = 0;

  const worker = job({
    id: uid("sched-chaos-churn-job"),
    schema: z.object({ id: z.string() }),
    process: async () => {
      runs += 1;
      return "ok";
    },
  });

  const cfg = {
    id: schedulerId,
    leader: { leaseMs: 350, heartbeatMs: 80 },
    dispatch: { tickMs: 20 },
  };

  const a = scheduler(cfg);
  const b = scheduler(cfg);
  const c = scheduler(cfg);

  try {
    await a.register({
      id: "churn",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: worker,
      input: { id: "x" },
    });
    await b.register({
      id: "churn",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: worker,
      input: { id: "x" },
    });
    await c.register({
      id: "churn",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: worker,
      input: { id: "x" },
    });

    await forceScheduleDue(schedulerId, "churn", Date.now() - 60_000);
    a.start();
    b.start();
    c.start();

    // Introduce churn by rapidly cycling one instance.
    for (let i = 0; i < 6; i++) {
      await Bun.sleep(40);
      await b.stop();
      b.start();
    }

    await waitUntil(() => runs === 1, 6_000);
    expect(runs).toBe(1);
  } finally {
    await a.stop();
    await b.stop();
    await c.stop();
    worker.stop();
  }
});

test("chaos: forward clock jump with skip policy does not replay backlog", async () => {
  const schedulerId = uid("sched-chaos-clock");
  let runs = 0;

  const worker = job({
    id: uid("sched-chaos-clock-job"),
    schema: z.object({ id: z.string() }),
    process: async () => {
      runs += 1;
      return "ok";
    },
  });

  const s = scheduler({
    id: schedulerId,
    leader: { leaseMs: 800, heartbeatMs: 100 },
    dispatch: { tickMs: 25 },
  });

  const realNow = Date.now;
  const base = realNow();
  let shifted = false;
  Date.now = () => (shifted ? base + 2 * 60 * 60 * 1000 : realNow());

  try {
    await s.register({
      id: "clock-jump",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "skip",
      job: worker,
      input: { id: "clk" },
    });

    await forceScheduleDue(schedulerId, "clock-jump", base - 60_000);
    shifted = true;
    s.start();
    await Bun.sleep(300);

    expect(runs).toBe(0);
    const info = await s.get({ id: "clock-jump" });
    expect(info).not.toBeNull();
    expect(info!.nextRunAt).toBeGreaterThan(base + 2 * 60 * 60 * 1000 - 1_000);
  } finally {
    Date.now = realNow;
    await s.stop();
    worker.stop();
  }
});
