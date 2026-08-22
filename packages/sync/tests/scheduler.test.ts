import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import type { NatsConnection } from "@nats-io/nats-core";
import { createSync } from "../src/sync.ts";
import type { Sync } from "../src/sync.ts";
import type { ScheduleContext } from "../src/scheduler.ts";
import { connectToCluster, uniqueName } from "./cluster.ts";
import { cleanupNamespaces, testNamespace, waitFor } from "./helpers.ts";

let nc: NatsConnection;
let sync: Sync;
const namespace = testNamespace();

/** Publish a crafted broker-style tick directly to a schedule's tick subject. */
const publishFakeTick = async (schedulerId: string, scheduleId: string): Promise<void> => {
  const js = jetstream(nc);
  const jsm = await jetstreamManager(nc);
  for await (const info of jsm.streams.list()) {
    const meta = info.config.metadata;
    if (
      meta?.["sync.kind"] === "scheduler" &&
      meta["sync.namespace"] === namespace &&
      meta["sync.id"] === schedulerId &&
      !info.config.name.startsWith("KV_")
    ) {
      const tickSubject = info.config.subjects.find((s) => s.endsWith(".tick.*"))!.replace("*", Buffer.from(scheduleId).toString("base64url"));
      await js.publish(tickSubject, JSON.stringify({ scheduleId, trigger: "schedule" }));
      return;
    }
  }
  throw new Error("scheduler stream not found");
};

beforeAll(async () => {
  nc = await connectToCluster({ name: "scheduler-test" });
  sync = createSync({ connection: nc, namespace, application: "tests" });
  await sync.ready();
});

afterAll(async () => {
  await sync.drain({ timeoutMs: 2_000 });
  await cleanupNamespaces(nc, [namespace]);
  await nc.close();
});

describe("scheduler definitions", () => {
  test("create validates cron and timezone and reports created/updated", async () => {
    const scheduler = sync.scheduler({ id: "defs" });
    const first = await scheduler.create({ id: "daily", cron: "0 3 * * *", process: async () => {} });
    expect(first).toEqual({ created: true, updated: false });
    const same = await scheduler.create({ id: "daily", cron: "0 3 * * *", process: async () => {} });
    expect(same).toEqual({ created: false, updated: false });
    const changed = await scheduler.create({ id: "daily", cron: "30 4 * * *", timezone: "Europe/Berlin", process: async () => {} });
    expect(changed).toEqual({ created: false, updated: true });

    await expect(scheduler.create({ id: "bad", cron: "not a cron", process: async () => {} })).rejects.toThrow("invalid cron");
    await expect(
      scheduler.create({ id: "badtz", cron: "* * * * *", timezone: "Mars/Olympus", process: async () => {} }),
    ).rejects.toThrow();

    const info = await scheduler.get({ id: "daily" });
    expect(info!.cron).toBe("30 4 * * *");
    expect(info!.timezone).toBe("Europe/Berlin");
    expect(info!.misfire).toBe("latest");
    expect(info!.handlerAvailable).toBe(true);
    expect(info!.nextRunAt.getTime()).toBeGreaterThan(Date.now());
    expect((await scheduler.list()).map((s) => s.id)).toContain("daily");
    expect(await scheduler.get({ id: "missing" })).toBeNull();
  });

  test("delete cancels the broker schedule and removes state", async () => {
    const scheduler = sync.scheduler({ id: "del" });
    await scheduler.create({ id: "gone", cron: "* * * * *", process: async () => {} });
    expect(await scheduler.delete({ id: "gone" })).toBe(true);
    expect(await scheduler.delete({ id: "gone" })).toBe(false);
    expect(await scheduler.get({ id: "gone" })).toBeNull();
    await expect(scheduler.runNow({ id: "gone", requestId: "r1" })).rejects.toThrow("does not exist");
  });
});

describe("scheduler execution", () => {
  test("runNow executes with manual trigger; repeated requestId is one run", async () => {
    const scheduler = sync.scheduler({ id: "manual" });
    const runs: ScheduleContext[] = [];
    await scheduler.create({ id: "task", cron: "0 0 1 1 *", process: async (context) => {
      runs.push(context);
    } });
    const worker = await scheduler.start();

    const first = await scheduler.runNow({ id: "task", requestId: "req-1" });
    const dup = await scheduler.runNow({ id: "task", requestId: "req-1" });
    expect(dup.runId).toBe(first.runId);
    await waitFor(() => runs.length >= 1, 15_000);
    await Bun.sleep(500);
    expect(runs).toHaveLength(1);
    expect(runs[0]!.trigger).toBe("manual");
    expect(runs[0]!.runId).toBe("task:manual:req-1");
    expect(runs[0]!.runNumber).toBe(1);

    const second = await scheduler.runNow({ id: "task", requestId: "req-2" });
    expect(second.runId).toBe("task:manual:req-2");
    await waitFor(() => runs.length >= 2, 15_000);
    expect(runs[1]!.runNumber).toBe(2);
    const info = await scheduler.get({ id: "task" });
    expect(info!.runNumber).toBe(2);
    await worker.drain();
  }, 30_000);

  test("misfire latest coalesces accumulated ticks to the newest slot", async () => {
    const scheduler = sync.scheduler({ id: "latest" });
    const executed: string[] = [];
    await scheduler.create({ id: "coalesce", cron: "0 0 1 1 *", misfire: "latest", process: async (context) => {
      executed.push(context.runId);
    } });
    // Three accumulated broker ticks while every worker was offline.
    await publishFakeTick("latest", "coalesce");
    await publishFakeTick("latest", "coalesce");
    await publishFakeTick("latest", "coalesce");
    const worker = await scheduler.start();
    await waitFor(() => executed.length >= 1, 15_000);
    await Bun.sleep(700);
    expect(executed).toHaveLength(1); // only the newest slot ran
    await worker.drain();
  }, 30_000);

  test("misfire all executes every retained slot", async () => {
    const scheduler = sync.scheduler({ id: "all" });
    const executed: number[] = [];
    await scheduler.create({ id: "each", cron: "0 0 1 1 *", misfire: "all", process: async (context) => {
      executed.push(context.runNumber);
    } });
    await publishFakeTick("all", "each");
    await publishFakeTick("all", "each");
    await publishFakeTick("all", "each");
    const worker = await scheduler.start();
    await waitFor(() => executed.length >= 3, 20_000);
    expect(executed).toEqual([1, 2, 3]);
    await worker.drain();
  }, 30_000);

  test("failed runs retry with backoff, then count as failures", async () => {
    const scheduler = sync.scheduler({
      id: "failing",
      delivery: { maxAttempts: 2, backoffMs: [200], ackWaitMs: 5_000 },
    });
    const attempts: number[] = [];
    await scheduler.create({ id: "broken", cron: "0 0 1 1 *", process: async (context) => {
      attempts.push(context.attempt);
      throw new Error("handler down");
    } });
    const worker = await scheduler.start();
    await scheduler.runNow({ id: "broken", requestId: "go" });
    await waitFor(() => attempts.length >= 2, 15_000);
    await waitFor(async () => (await scheduler.get({ id: "broken" }))!.failureCount === 1, 15_000);
    expect(attempts).toEqual([1, 2]);
    await worker.drain();
  }, 30_000);

  test("runs of one schedule never overlap even with spare concurrency", async () => {
    const scheduler = sync.scheduler({ id: "serial" });
    let active = 0;
    let maxActive = 0;
    let runs = 0;
    await scheduler.create({ id: "slow", cron: "0 0 1 1 *", process: async () => {
      active += 1;
      maxActive = Math.max(maxActive, active);
      await Bun.sleep(300);
      active -= 1;
      runs += 1;
    } });
    const worker = await scheduler.start({ concurrency: 4 });
    await scheduler.runNow({ id: "slow", requestId: "a" });
    await scheduler.runNow({ id: "slow", requestId: "b" });
    await scheduler.runNow({ id: "slow", requestId: "c" });
    await waitFor(() => runs >= 3, 20_000);
    expect(maxActive).toBe(1);
    await worker.drain();
  }, 30_000);

  test("broker produces real cron ticks while no worker is running", async () => {
    const scheduler = sync.scheduler({ id: "broker" });
    const executed: ScheduleContext[] = [];
    // Every minute; the broker fires this without any Sync worker.
    await scheduler.create({ id: "minutely", cron: "* * * * *", misfire: "latest", process: async (context) => {
      executed.push(context);
    } });
    // Wait (workerless) until the broker has produced at least one tick.
    const jsm = await jetstreamManager(nc);
    let tickSeen = false;
    const deadline = Date.now() + 70_000;
    while (!tickSeen && Date.now() < deadline) {
      for await (const info of jsm.streams.list()) {
        const meta = info.config.metadata;
        if (meta?.["sync.kind"] === "scheduler" && meta["sync.namespace"] === namespace && meta["sync.id"] === "broker") {
          tickSeen = info.state.messages > 1; // schedule message + >=1 tick
        }
      }
      if (!tickSeen) await Bun.sleep(1_000);
    }
    expect(tickSeen).toBe(true);
    // A worker starting later processes the offline tick.
    const worker = await scheduler.start();
    await waitFor(() => executed.length >= 1, 15_000);
    expect(executed[0]!.trigger).toBe("schedule");
    expect(executed[0]!.slot.getTime()).toBeGreaterThan(Date.now() - 120_000);
    await scheduler.delete({ id: "minutely" });
    await worker.drain();
  }, 100_000);
});
