import { expect, setDefaultTimeout, test } from "bun:test";
import { z } from "zod";
import { scheduler } from "../../src/browser/scheduler";
import { job } from "../../src/browser/job";
import { createMemoryStore } from "../../src/browser/store";

setDefaultTimeout(20_000);

let counter = 0;
const uid = (label: string): string => `${label}-${++counter}-${Date.now()}`;

const waitUntil = async (predicate: () => boolean | Promise<boolean>, timeoutMs = 5_000): Promise<void> => {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await predicate()) return;
    await Bun.sleep(20);
  }
  throw new Error("waitUntil timeout");
};

const withFakeNow = async (run: (clock: { now: () => number; advanceMinutes: (minutes?: number) => void }) => Promise<void>): Promise<void> => {
  const originalNow = Date.now;
  let fakeNow = Date.UTC(2026, 0, 1, 0, 0, 0);
  Date.now = (): number => fakeNow;

  try {
    await run({
      now: () => fakeNow,
      advanceMinutes: (minutes = 1): void => {
        fakeNow += minutes * 60_000;
      },
    });
  } finally {
    Date.now = originalNow;
  }
};

test("browser schedule keeps dispatching after repeated triggerNow interleaving", async () => {
  await withFakeNow(async ({ advanceMinutes }) => {
    const schedulerId = uid("browser-repeat-trigger");
    const scheduleId = uid("browser-repeat-trigger-schedule");
    let runs = 0;

    const worker = job({
      id: uid("browser-repeat-trigger-worker"),
      schema: z.object({ ok: z.boolean() }),
      process: async () => {
        runs += 1;
        return runs;
      },
    });

    const s = scheduler({
      id: schedulerId,
      dispatch: { tickMs: 20, batchSize: 10 },
      leader: { leaseMs: 500, heartbeatMs: 50 },
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
      await waitUntil(() => s.metrics().isLeader, 2_000);

      advanceMinutes(1);
      await waitUntil(() => runs >= 1, 2_000);

      const manual1 = await s.triggerNow({ id: scheduleId, key: "manual-1" });
      expect((await worker.join({ id: manual1, timeoutMs: 2_000 })).status).toBe("completed");

      const manual2 = await s.triggerNow({ id: scheduleId, key: "manual-2" });
      expect((await worker.join({ id: manual2, timeoutMs: 2_000 })).status).toBe("completed");

      advanceMinutes(1);
      await waitUntil(() => runs >= 4, 2_000);

      advanceMinutes(1);
      await waitUntil(() => runs >= 5, 2_000);

      expect(runs).toBe(5);
      expect(s.metrics().dispatchSubmitted).toBe(3);
      expect(s.metrics().triggerSubmitted).toBe(2);
    } finally {
      await s.stop();
      worker.stop();
    }
  });
});

test("browser schedule continues across restart and repeated cycles with a shared store", async () => {
  await withFakeNow(async ({ advanceMinutes }) => {
    const store = createMemoryStore();
    const schedulerId = uid("browser-repeat-restart");
    const scheduleId = uid("browser-repeat-restart-schedule");
    const workerId = uid("browser-repeat-restart-worker");
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

    const w1 = makeWorker();
    const s1 = scheduler({
      id: schedulerId,
      dispatch: { tickMs: 20, batchSize: 10 },
      leader: { leaseMs: 500, heartbeatMs: 50 },
      store,
    });

    try {
      await s1.register({
        id: scheduleId,
        cron: "* * * * *",
        tz: "UTC",
        misfire: "catch_up_one",
        job: w1,
        input: { ok: true },
      });

      s1.start();
      await waitUntil(() => s1.metrics().isLeader, 2_000);

      advanceMinutes(1);
      await waitUntil(() => runs >= 1, 2_000);
    } finally {
      await s1.stop();
      w1.stop();
    }

    advanceMinutes(2);

    const w2 = makeWorker();
    const s2 = scheduler({
      id: schedulerId,
      dispatch: { tickMs: 20, batchSize: 10 },
      leader: { leaseMs: 500, heartbeatMs: 50 },
      store,
    });

    try {
      await s2.register({
        id: scheduleId,
        cron: "* * * * *",
        tz: "UTC",
        misfire: "catch_up_one",
        job: w2,
        input: { ok: true },
      });

      s2.start();
      await waitUntil(() => s2.metrics().isLeader, 2_000);
      await waitUntil(() => runs >= 2, 2_000);

      advanceMinutes(1);
      await waitUntil(() => runs >= 3, 2_000);

      expect(runs).toBe(3);
      expect(s2.metrics().dispatchSubmitted).toBe(2);
    } finally {
      await s2.stop();
      w2.stop();
    }
  });
});

test("two browser schedules sharing the same job keep dispatching across repeated cycles", async () => {
  await withFakeNow(async ({ advanceMinutes }) => {
    const schedulerId = uid("browser-repeat-shared-job");
    const counts = { alpha: 0, beta: 0 };

    const worker = job({
      id: uid("browser-repeat-shared-job-worker"),
      schema: z.object({ name: z.enum(["alpha", "beta"]) }),
      process: async ({ input }) => {
        counts[input.name] += 1;
        return input.name;
      },
    });

    const s = scheduler({
      id: schedulerId,
      dispatch: { tickMs: 20, batchSize: 1 },
      leader: { leaseMs: 500, heartbeatMs: 50 },
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
      await waitUntil(() => s.metrics().isLeader, 2_000);

      for (let cycle = 1; cycle <= 3; cycle += 1) {
        advanceMinutes(1);
        await waitUntil(() => counts.alpha >= cycle && counts.beta >= cycle, 2_000);
      }

      expect(counts.alpha).toBe(3);
      expect(counts.beta).toBe(3);
      expect(s.metrics().dispatchSubmitted).toBe(6);
    } finally {
      await s.stop();
      worker.stop();
    }
  });
});

test("browser non-strict scheduler does not starve a valid repeating schedule behind a missing handler", async () => {
  await withFakeNow(async ({ advanceMinutes }) => {
    const schedulerId = uid("browser-repeat-nonstrict");
    const validScheduleId = "b-valid";
    let runs = 0;

    const missingJob = {
      id: uid("browser-missing-job"),
      submit: async () => "missing-job-id",
      validateInput: () => {},
    };

    const staging = scheduler({
      id: schedulerId,
      strictHandlers: false,
      dispatch: { tickMs: 20, batchSize: 1 },
      leader: { leaseMs: 500, heartbeatMs: 50 },
    });

    const worker = job({
      id: uid("browser-repeat-nonstrict-worker"),
      schema: z.object({ ok: z.boolean() }),
      process: async () => {
        runs += 1;
        return true;
      },
    });

    const active = scheduler({
      id: schedulerId,
      strictHandlers: false,
      dispatch: { tickMs: 20, batchSize: 1 },
      leader: { leaseMs: 500, heartbeatMs: 50 },
    });

    try {
      await staging.register({
        id: "a-missing",
        cron: "* * * * *",
        tz: "UTC",
        misfire: "catch_up_one",
        job: missingJob,
        input: { ok: true },
      });

      await active.register({
        id: validScheduleId,
        cron: "* * * * *",
        tz: "UTC",
        misfire: "catch_up_one",
        job: worker,
        input: { ok: true },
      });

      active.start();
      await waitUntil(() => active.metrics().isLeader, 2_000);

      for (let cycle = 1; cycle <= 3; cycle += 1) {
        advanceMinutes(1);
        await waitUntil(() => runs >= cycle, 2_000);
      }

      expect(runs).toBe(3);
      expect(active.metrics().dispatchSubmitted).toBe(3);
      expect(active.metrics().dispatchSkipped).toBeGreaterThanOrEqual(1);
    } finally {
      await active.stop();
      await staging.stop();
      worker.stop();
    }
  });
});

test("browser unregister during an active run keeps the current run but prevents later cycles", async () => {
  await withFakeNow(async ({ advanceMinutes }) => {
    const schedulerId = uid("browser-repeat-unregister");
    const scheduleId = uid("browser-repeat-unregister-schedule");
    let starts = 0;
    let finishes = 0;
    let releaseCurrentRun!: () => void;
    const releasePromise = new Promise<void>((resolve) => {
      releaseCurrentRun = resolve;
    });

    const worker = job({
      id: uid("browser-repeat-unregister-worker"),
      schema: z.object({ ok: z.boolean() }),
      process: async () => {
        starts += 1;
        if (starts === 1) {
          await releasePromise;
        }
        finishes += 1;
        return finishes;
      },
    });

    const s = scheduler({
      id: schedulerId,
      dispatch: { tickMs: 20, batchSize: 10 },
      leader: { leaseMs: 500, heartbeatMs: 50 },
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
      await waitUntil(() => s.metrics().isLeader, 2_000);

      advanceMinutes(1);
      await waitUntil(() => starts >= 1, 2_000);

      await s.unregister({ id: scheduleId });
      releaseCurrentRun();
      await waitUntil(() => finishes >= 1, 2_000);

      advanceMinutes(2);
      await Bun.sleep(120);

      expect(finishes).toBe(1);
      expect(await s.get({ id: scheduleId })).toBeNull();
      expect(s.metrics().dispatchSubmitted).toBe(1);
    } finally {
      await s.stop();
      worker.stop();
    }
  });
});
