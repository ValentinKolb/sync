import { expect, setDefaultTimeout, test } from "bun:test";
import { redis } from "bun";
import { z } from "zod";
import { job, scheduler } from "../../index";

// Manual scheduler recurrence tests.
// These live under tests/manual/ so they are not picked up by the default package scripts.
//
// Run explicitly with:
//   REDIS_URL=redis://127.0.0.1:6399 bun test tests/manual/scheduler-repeat.manual.test.ts

setDefaultTimeout(30_000);

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

test("same schedule dispatches across three separate overdue cycles", async () => {
  const schedulerId = uid("sched-repeat");
  const scheduleId = "repeatable";
  const seenRuns: number[] = [];

  const worker = job({
    id: uid("sched-repeat-job"),
    schema: z.object({ run: z.number() }),
    process: async ({ input }) => {
      seenRuns.push(input.run);
      return input.run;
    },
  });

  const s = scheduler({
    id: schedulerId,
    leader: { leaseMs: 800, heartbeatMs: 100 },
    dispatch: { tickMs: 30, batchSize: 10 },
  });

  try {
    await s.register({
      id: scheduleId,
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: worker,
      input: { run: 1 },
    });

    s.start();

    for (let cycle = 1; cycle <= 3; cycle += 1) {
      await forceScheduleDue(schedulerId, scheduleId, Date.now() - cycle * 60_000);
      await waitUntil(() => seenRuns.length >= cycle, 5_000);
      expect(seenRuns.length).toBe(cycle);

      const info = await s.get({ id: scheduleId });
      expect(info).not.toBeNull();
      expect(info!.nextRunAt).toBeGreaterThan(Date.now() - 1_000);
    }

    expect(seenRuns).toEqual([1, 1, 1]);
    expect(s.metrics().dispatchSubmitted).toBe(3);
  } finally {
    await s.stop();
    worker.stop();
  }
});

test("scheduled dispatch still works after triggerNow", async () => {
  const schedulerId = uid("sched-repeat-trigger");
  const scheduleId = "repeat-after-trigger";
  let runs = 0;

  const worker = job({
    id: uid("sched-repeat-trigger-job"),
    schema: z.object({ ok: z.boolean() }),
    process: async () => {
      runs += 1;
      return "ok";
    },
  });

  const s = scheduler({
    id: schedulerId,
    leader: { leaseMs: 800, heartbeatMs: 100 },
    dispatch: { tickMs: 30, batchSize: 10 },
  });

  try {
    await s.register({
      id: scheduleId,
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: worker,
      input: { ok: true },
    });

    s.start();

    await forceScheduleDue(schedulerId, scheduleId, Date.now() - 60_000);
    await waitUntil(() => runs >= 1, 5_000);

    await s.triggerNow({ id: scheduleId, key: "manual-1" });
    await waitUntil(() => runs >= 2, 5_000);

    await forceScheduleDue(schedulerId, scheduleId, Date.now() - 120_000);
    await waitUntil(() => runs >= 3, 5_000);

    expect(runs).toBe(3);
    expect(s.metrics().dispatchSubmitted).toBe(2);
    expect(s.metrics().triggerSubmitted).toBe(1);
  } finally {
    await s.stop();
    worker.stop();
  }
});

test("non-strict scheduler continues dispatching a valid repeating schedule even with a missing-handler schedule present", async () => {
  const schedulerId = uid("sched-repeat-nonstrict");
  const scheduleId = "valid-repeat";
  let runs = 0;

  const worker = job({
    id: uid("sched-repeat-nonstrict-job"),
    schema: z.object({ ok: z.boolean() }),
    process: async () => {
      runs += 1;
      return "ok";
    },
  });

  const s = scheduler({
    id: schedulerId,
    strictHandlers: false,
    leader: { leaseMs: 800, heartbeatMs: 100 },
    dispatch: { tickMs: 30, batchSize: 1 },
  });

  try {
    const now = Date.now();
    await redis.send("SET", [`sync:scheduler:${schedulerId}:schedule:a-missing`, JSON.stringify({
      id: "a-missing",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      maxCatchUpRuns: 1,
      jobId: "missing-job",
      input: { ok: true },
      createdAt: now,
      updatedAt: now,
      nextRunAt: now - 60_000,
      consecutiveDispatchFailures: 0,
    })]);
    await redis.send("ZADD", [`sync:scheduler:${schedulerId}:due`, String(now - 60_000), "a-missing"]);
    await redis.send("SADD", [`sync:scheduler:${schedulerId}:index`, "a-missing"]);

    await s.register({
      id: scheduleId,
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: worker,
      input: { ok: true },
    });

    s.start();

    for (let cycle = 1; cycle <= 3; cycle += 1) {
      await forceScheduleDue(schedulerId, scheduleId, Date.now() - cycle * 60_000);
      await waitUntil(() => runs >= cycle, 5_000);
    }

    expect(runs).toBe(3);
    expect(s.metrics().dispatchSkipped).toBeGreaterThanOrEqual(1);
    expect(s.metrics().dispatchSubmitted).toBe(3);
  } finally {
    await s.stop();
    worker.stop();
  }
});

test("same schedule continues dispatching across scheduler restarts", async () => {
  const schedulerId = uid("sched-repeat-restart");
  const scheduleId = "repeat-across-restarts";
  const workerId = uid("sched-repeat-restart-job");
  let runs = 0;

  const makeWorker = () =>
    job({
      id: workerId,
      schema: z.object({ ok: z.boolean() }),
      process: async () => {
        runs += 1;
        return runs;
      },
    });

  const makeScheduler = () =>
    scheduler({
      id: schedulerId,
      leader: { leaseMs: 800, heartbeatMs: 100 },
      dispatch: { tickMs: 30, batchSize: 10 },
    });

  const worker1 = makeWorker();
  const s1 = makeScheduler();

  try {
    await s1.register({
      id: scheduleId,
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: worker1,
      input: { ok: true },
    });

    s1.start();

    await forceScheduleDue(schedulerId, scheduleId, Date.now() - 60_000);
    await waitUntil(() => runs >= 1, 5_000);

    await s1.stop();
    worker1.stop();

    const worker2 = makeWorker();
    const s2 = makeScheduler();
    try {
      await s2.register({
        id: scheduleId,
        cron: "* * * * *",
        tz: "UTC",
        misfire: "catch_up_one",
        job: worker2,
        input: { ok: true },
      });

      s2.start();

      for (let cycle = 2; cycle <= 3; cycle += 1) {
        await forceScheduleDue(schedulerId, scheduleId, Date.now() - cycle * 60_000);
        await waitUntil(() => runs >= cycle, 5_000);
      }

      expect(runs).toBe(3);
      expect(s2.metrics().dispatchSubmitted).toBe(2);
    } finally {
      await s2.stop();
      worker2.stop();
    }
  } finally {
    await s1.stop();
    worker1.stop();
  }
});

test("two schedules sharing the same job keep dispatching across repeated cycles", async () => {
  const schedulerId = uid("sched-repeat-shared-job");
  const seen: string[] = [];
  const counts = { alpha: 0, beta: 0 };

  const worker = job({
    id: uid("sched-repeat-shared-job-worker"),
    schema: z.object({ name: z.enum(["alpha", "beta"]) }),
    process: async ({ input }) => {
      seen.push(input.name);
      counts[input.name] += 1;
      return input.name;
    },
  });

  const s = scheduler({
    id: schedulerId,
    leader: { leaseMs: 800, heartbeatMs: 100 },
    dispatch: { tickMs: 30, batchSize: 1 },
  });

  try {
    await s.register({
      id: "alpha-schedule",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: worker,
      input: { name: "alpha" },
    });
    await s.register({
      id: "beta-schedule",
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: worker,
      input: { name: "beta" },
    });

    s.start();

    for (let cycle = 1; cycle <= 3; cycle += 1) {
      await forceScheduleDue(schedulerId, "alpha-schedule", Date.now() - cycle * 60_000 - 1_000);
      await forceScheduleDue(schedulerId, "beta-schedule", Date.now() - cycle * 60_000);
      await waitUntil(() => counts.alpha >= cycle && counts.beta >= cycle, 5_000);
    }

    expect(counts.alpha).toBe(3);
    expect(counts.beta).toBe(3);
    expect(seen.length).toBe(6);
    expect(s.metrics().dispatchSubmitted).toBe(6);
  } finally {
    await s.stop();
    worker.stop();
  }
});

test("multiple triggerNow calls do not break later repeated scheduled dispatches", async () => {
  const schedulerId = uid("sched-repeat-manual-interleave");
  const scheduleId = "repeat-with-multiple-manual-triggers";
  let runs = 0;

  const worker = job({
    id: uid("sched-repeat-manual-worker"),
    schema: z.object({ ok: z.boolean() }),
    process: async () => {
      runs += 1;
      return runs;
    },
  });

  const s = scheduler({
    id: schedulerId,
    leader: { leaseMs: 800, heartbeatMs: 100 },
    dispatch: { tickMs: 30, batchSize: 10 },
  });

  try {
    await s.register({
      id: scheduleId,
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: worker,
      input: { ok: true },
    });

    s.start();

    await forceScheduleDue(schedulerId, scheduleId, Date.now() - 60_000);
    await waitUntil(() => runs >= 1, 5_000);

    const manual1 = await s.triggerNow({ id: scheduleId, key: "manual-1" });
    const manual1Terminal = await worker.join({ id: manual1, timeoutMs: 5_000 });
    expect(manual1Terminal.status).toBe("completed");

    const manual2 = await s.triggerNow({ id: scheduleId, key: "manual-2" });
    const manual2Terminal = await worker.join({ id: manual2, timeoutMs: 5_000 });
    expect(manual2Terminal.status).toBe("completed");

    await forceScheduleDue(schedulerId, scheduleId, Date.now() - 120_000);
    await waitUntil(() => runs >= 4, 5_000);

    await forceScheduleDue(schedulerId, scheduleId, Date.now() - 180_000);
    await waitUntil(() => runs >= 5, 5_000);

    expect(runs).toBe(5);
    expect(s.metrics().dispatchSubmitted).toBe(3);
    expect(s.metrics().triggerSubmitted).toBe(2);
  } finally {
    await s.stop();
    worker.stop();
  }
});

test("cancelling a long-running scheduled run aborts it and later cycles still dispatch", async () => {
  const schedulerId = uid("sched-repeat-cancel");
  const scheduleId = "repeat-after-cancel";
  const metrics: Array<{ type: string; scheduleId?: string; jobId?: string }> = [];
  let starts = 0;
  let aborts = 0;
  let quickRuns = 0;

  const worker = job({
    id: uid("sched-repeat-cancel-worker"),
    schema: z.object({ ok: z.boolean() }),
    process: async ({ ctx }) => {
      starts += 1;
      if (starts === 1) {
        await Promise.race([
          new Promise<void>((resolve) => {
            ctx.signal.addEventListener("abort", () => {
              aborts += 1;
              resolve();
            }, { once: true });
          }),
          Bun.sleep(3_000),
        ]);
        return "cancelled-first-run";
      }

      quickRuns += 1;
      return "ok";
    },
  });

  const s = scheduler({
    id: schedulerId,
    leader: { leaseMs: 800, heartbeatMs: 100 },
    dispatch: { tickMs: 30, batchSize: 10 },
    onMetric: (metric) => {
      metrics.push(metric as { type: string; scheduleId?: string; jobId?: string });
    },
  });

  try {
    await s.register({
      id: scheduleId,
      cron: "* * * * *",
      tz: "UTC",
      misfire: "catch_up_one",
      job: worker,
      input: { ok: true },
    });

    s.start();

    await forceScheduleDue(schedulerId, scheduleId, Date.now() - 60_000);
    await waitUntil(() => starts >= 1, 5_000);
    await waitUntil(
      () => metrics.some((metric) => metric.type === "dispatch_submitted" && metric.scheduleId === scheduleId),
      5_000,
    );

    const firstDispatch = metrics.find(
      (metric) => metric.type === "dispatch_submitted" && metric.scheduleId === scheduleId,
    );
    expect(firstDispatch?.jobId).toBeDefined();

    await worker.cancel({ id: firstDispatch!.jobId!, reason: "cancel-first-scheduled-run" });
    const cancelled = await worker.join({ id: firstDispatch!.jobId!, timeoutMs: 5_000 });
    expect(cancelled.status).toBe("cancelled");
    await waitUntil(() => aborts === 1, 2_000);

    for (let cycle = 2; cycle <= 3; cycle += 1) {
      await forceScheduleDue(schedulerId, scheduleId, Date.now() - cycle * 60_000);
      await waitUntil(() => starts >= cycle, 5_000);
    }

    expect(starts).toBe(3);
    expect(aborts).toBe(1);
    expect(quickRuns).toBe(2);
    expect(s.metrics().dispatchSubmitted).toBe(3);
  } finally {
    await s.stop();
    worker.stop();
  }
});

test("leadership handover keeps repeated schedule dispatching across multiple cycles", async () => {
  const schedulerId = uid("sched-repeat-failover");
  const scheduleId = "repeat-failover";
  let runs = 0;

  const worker = job({
    id: uid("sched-repeat-failover-worker"),
    schema: z.object({ ok: z.boolean() }),
    process: async () => {
      runs += 1;
      return runs;
    },
  });

  const cfg = {
    id: schedulerId,
    leader: { leaseMs: 500, heartbeatMs: 100 },
    dispatch: { tickMs: 30, batchSize: 10 },
  } as const;

  const s1 = scheduler(cfg);
  const s2 = scheduler(cfg);
  const s3 = scheduler(cfg);

  try {
    for (const sched of [s1, s2, s3]) {
      await sched.register({
        id: scheduleId,
        cron: "* * * * *",
        tz: "UTC",
        misfire: "catch_up_one",
        job: worker,
        input: { ok: true },
      });
    }

    s1.start();
    s2.start();

    await forceScheduleDue(schedulerId, scheduleId, Date.now() - 60_000);
    await waitUntil(() => runs >= 1, 5_000);

    const firstLeader = s1.metrics().isLeader ? s1 : s2;
    await firstLeader.stop();

    s3.start();

    await forceScheduleDue(schedulerId, scheduleId, Date.now() - 120_000);
    await waitUntil(() => runs >= 2, 5_000);

    const secondLeader = s2.metrics().isLeader ? s2 : s3;
    await secondLeader.stop();

    const remaining = secondLeader === s2 ? s3 : s2;
    if (!remaining.metrics().isLeader) {
      await waitUntil(() => remaining.metrics().isLeader, 5_000);
    }

    await forceScheduleDue(schedulerId, scheduleId, Date.now() - 180_000);
    await waitUntil(() => runs >= 3, 5_000);

    expect(runs).toBe(3);
  } finally {
    await s1.stop();
    await s2.stop();
    await s3.stop();
    worker.stop();
  }
});
