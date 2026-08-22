import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import type { NatsConnection } from "@nats-io/nats-core";
import { createSync } from "../../src/v6/sync.ts";
import type { Sync } from "../../src/v6/sync.ts";
import { connectToCluster } from "./cluster.ts";
import { cleanupNamespaces, testNamespace, waitFor } from "./helpers.ts";

type Input = { total: number };
type Item = { key: string };

let nc: NatsConnection;
let sync: Sync;
const namespace = testNamespace();

/** Source of `total` numbered items, paged by cursor. */
const pagedPull =
  (batch: number) =>
  async ({ input, cursor, limit }: { input: Input; cursor: number | null; limit: number }) => {
    const start = cursor ?? 0;
    const end = Math.min(start + Math.min(limit, batch), input.total);
    const items: Item[] = [];
    for (let i = start; i < end; i++) items.push({ key: `item-${i}` });
    return { items, nextCursor: end >= input.total ? null : end };
  };

beforeAll(async () => {
  nc = await connectToCluster({ name: "pump-test" });
  sync = createSync({ connection: nc, namespace, application: "tests" });
  await sync.ready();
});

afterAll(async () => {
  await sync.drain({ timeoutMs: 2_000 });
  await cleanupNamespaces(nc, [namespace]);
  await nc.close();
});

describe("pump", () => {
  test("drains a finite source across pages and completes", async () => {
    const dispatched: string[] = [];
    const pump = sync.pump<Input, number, Item>({
      id: "drain",
      batchSize: 4,
      dispatchConcurrency: 2,
      pull: pagedPull(4),
      dispatch: async ({ item }) => {
        dispatched.push(item.key);
      },
    });
    await pump.start({ key: "run-1", input: { total: 10 } });
    const worker = await pump.process({ concurrency: 2 });
    await waitFor(async () => (await pump.get({ key: "run-1" }))?.status === "completed", 20_000);
    const state = await pump.get({ key: "run-1" });
    expect(state!.dispatched).toBe(10);
    expect(state!.cursor).toBeNull();
    expect(dispatched.toSorted()).toEqual(Array.from({ length: 10 }, (_, i) => `item-${i}`).toSorted());
    // start() on a completed run restarts it; on a running run it is a no-op.
    await worker.drain();
  }, 30_000);

  test("transient dispatch failure retries without repeating checkpointed items", async () => {
    const calls = new Map<string, number>();
    let failOnce = true;
    const pump = sync.pump<Input, number, Item>({
      id: "retry",
      batchSize: 5,
      retry: { maxAttempts: 3, backoffMs: [500] },
      pull: pagedPull(5),
      dispatch: async ({ item }) => {
        calls.set(item.key, (calls.get(item.key) ?? 0) + 1);
        if (failOnce && item.key === "item-3") {
          failOnce = false;
          throw new Error("transient sink failure");
        }
      },
    });
    await pump.start({ key: "run-r", input: { total: 5 } });
    const worker = await pump.process();
    await waitFor(async () => (await pump.get({ key: "run-r" }))?.status === "completed", 20_000);
    const state = await pump.get({ key: "run-r" });
    expect(state!.dispatched).toBe(5);
    expect(state!.failureCount).toBe(0); // reset after a successful page
    // Items 0-2 were checkpointed before the failure and never repeated.
    expect(calls.get("item-0")).toBe(1);
    expect(calls.get("item-1")).toBe(1);
    expect(calls.get("item-2")).toBe(1);
    // Item 3 failed once and was retried.
    expect(calls.get("item-3")).toBe(2);
    await worker.drain();
  }, 30_000);

  test("permanent failure lands in failed state with the last error", async () => {
    const pump = sync.pump<Input, number, Item>({
      id: "fail",
      retry: { maxAttempts: 2, backoffMs: [200] },
      pull: pagedPull(3),
      dispatch: async () => {
        throw new Error("sink is down");
      },
    });
    await pump.start({ key: "run-f", input: { total: 3 } });
    const worker = await pump.process();
    await waitFor(async () => (await pump.get({ key: "run-f" }))?.status === "failed", 20_000);
    const state = await pump.get({ key: "run-f" });
    expect(state!.failureCount).toBe(2);
    expect(state!.lastError).toContain("sink is down");
    await worker.drain();
  }, 30_000);

  test("cancel stops an active run and keeps its state", async () => {
    let dispatchedCount = 0;
    const pump = sync.pump<Input, number, Item>({
      id: "cancel",
      batchSize: 2,
      pull: pagedPull(2),
      dispatch: async () => {
        dispatchedCount += 1;
        await Bun.sleep(150);
      },
    });
    await pump.start({ key: "run-c", input: { total: 100 } });
    const worker = await pump.process();
    await waitFor(() => dispatchedCount >= 2, 15_000);
    expect(await pump.cancel({ key: "run-c" })).toBe(true);
    await waitFor(async () => (await pump.get({ key: "run-c" }))?.status === "canceled", 15_000);
    const count = dispatchedCount;
    await Bun.sleep(600);
    expect(dispatchedCount).toBeLessThanOrEqual(count + 2); // stops promptly
    expect(await pump.cancel({ key: "run-c" })).toBe(false); // already terminal
    await worker.drain();
  }, 30_000);

  test("reconcile repairs lost wake-ups: accepted work survives a dead worker", async () => {
    // A worker "dies" mid-run: its handler hangs, drain force-aborts it. The
    // KV state stays queued/running; a later process() reconciles and finishes.
    let firstWorkerHangs = true;
    const done: string[] = [];
    const pump = sync.pump<Input, number, Item>({
      id: "recover",
      batchSize: 3,
      leaseMs: 2_000,
      pull: pagedPull(3),
      dispatch: async ({ item }) => {
        if (firstWorkerHangs) {
          await new Promise(() => {}); // never settles — simulates a crash
        }
        done.push(item.key);
      },
    });
    await pump.start({ key: "run-x", input: { total: 3 } });
    const dying = await pump.process();
    await waitFor(async () => (await pump.get({ key: "run-x" }))?.status === "running", 15_000);
    await dying.drain({ timeoutMs: 200 }); // force-abort the hung handler

    firstWorkerHangs = false;
    const rescuer = await pump.process();
    await waitFor(async () => (await pump.get({ key: "run-x" }))?.status === "completed", 45_000);
    expect(done.toSorted()).toEqual(["item-0", "item-1", "item-2"]);
    await rescuer.drain();
  }, 60_000);

  test("start is idempotent for active runs and restarts terminal runs", async () => {
    const pump = sync.pump<Input, number, Item>({
      id: "restart",
      pull: pagedPull(5),
      dispatch: async () => {},
    });
    await pump.start({ key: "run-s", input: { total: 2 } });
    await pump.start({ key: "run-s", input: { total: 999 } }); // ignored: active
    expect((await pump.get({ key: "run-s" }))!.input.total).toBe(2);

    const worker = await pump.process();
    await waitFor(async () => (await pump.get({ key: "run-s" }))?.status === "completed", 20_000);

    await pump.start({ key: "run-s", input: { total: 4 } }); // restart after terminal
    await waitFor(async () => {
      const state = await pump.get({ key: "run-s" });
      return state?.status === "completed" && state.input.total === 4;
    }, 20_000);
    expect((await pump.get({ key: "run-s" }))!.dispatched).toBe(4);
    await worker.drain();
  }, 45_000);
});
