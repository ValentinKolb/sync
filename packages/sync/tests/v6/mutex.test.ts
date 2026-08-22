import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import type { NatsConnection } from "@nats-io/nats-core";
import { createSync } from "../../src/v6/sync.ts";
import type { Sync } from "../../src/v6/sync.ts";
import type { Lock } from "../../src/v6/mutex.ts";
import { connectToCluster } from "./cluster.ts";
import { cleanupNamespaces, testNamespace } from "./helpers.ts";

let nc: NatsConnection;
let sync: Sync;
const namespace = testNamespace();

beforeAll(async () => {
  nc = await connectToCluster({ name: "mutex-test" });
  sync = createSync({ connection: nc, namespace, application: "tests" });
  await sync.ready();
});

afterAll(async () => {
  await sync.drain({ timeoutMs: 2_000 });
  await cleanupNamespaces(nc, [namespace]);
  await nc.close();
});

describe("mutex", () => {
  test("exclusive acquire; second holder waits for release", async () => {
    const mutex = sync.mutex({ id: "excl", retry: { attempts: 0 } });
    const lock = await mutex.acquire("resource-a");
    expect(lock).not.toBeNull();
    expect(await mutex.acquire("resource-a")).toBeNull();
    // Independent resources do not contend.
    const other = await mutex.acquire("resource-b");
    expect(other).not.toBeNull();
    expect(await mutex.release(lock!)).toBe(true);
    const next = await mutex.acquire("resource-a");
    expect(next).not.toBeNull();
    await mutex.release(next!);
    await mutex.release(other!);
  });

  test("expired lease frees the resource; fencing token grows monotonically", async () => {
    const mutex = sync.mutex({ id: "expiry", ttlMs: 1_000, retry: { attempts: 0 } });
    const first = await mutex.acquire("job-runner");
    expect(first).not.toBeNull();
    await Bun.sleep(2_500);
    const second = await mutex.acquire("job-runner");
    expect(second).not.toBeNull();
    expect(second!.fence).toBeGreaterThan(first!.fence);
    await mutex.release(second!);
  }, 15_000);

  test("stale owner cannot extend or release after expiry and reacquisition", async () => {
    const mutex = sync.mutex({ id: "stale", ttlMs: 1_000, retry: { attempts: 0 } });
    const stale = await mutex.acquire("shared");
    expect(stale).not.toBeNull();
    await Bun.sleep(2_500);
    const fresh = await mutex.acquire("shared");
    expect(fresh).not.toBeNull();
    expect(await mutex.extend(stale!)).toBe(false);
    expect(await mutex.release(stale!)).toBe(false);
    // The fresh lock is untouched by the stale calls.
    expect(await mutex.extend(fresh!)).toBe(true);
    await mutex.release(fresh!);
  }, 15_000);

  test("extend keeps the lease alive beyond the original TTL and preserves the fence", async () => {
    const mutex = sync.mutex({ id: "extend", ttlMs: 1_000, retry: { attempts: 0 } });
    const lock = await mutex.acquire("renewing");
    expect(lock).not.toBeNull();
    const fence = lock!.fence;
    for (let i = 0; i < 3; i++) {
      await Bun.sleep(600);
      expect(await mutex.extend(lock!)).toBe(true);
    }
    expect(lock!.fence).toBe(fence);
    expect(await mutex.acquire("renewing")).toBeNull();
    await mutex.release(lock!);
  }, 15_000);

  test("withLock runs the function under the lock and always releases", async () => {
    const mutex = sync.mutex({ id: "with", retry: { attempts: 0 } });
    const result = await mutex.withLock("crit", async (lock: Lock) => {
      expect(await mutex.acquire("crit")).toBeNull();
      return "done";
    });
    expect(result).toBe("done");
    const after = await mutex.acquire("crit");
    expect(after).not.toBeNull();
    await mutex.release(after!);

    // Function failure still releases.
    await expect(
      mutex.withLock("crit", async () => {
        throw new Error("inner");
      }),
    ).rejects.toThrow("inner");
    const again = await mutex.acquire("crit");
    expect(again).not.toBeNull();
    await mutex.release(again!);
  });

  test("contending acquirers serialize on one resource", async () => {
    const mutex = sync.mutex({ id: "contend", ttlMs: 5_000, retry: { attempts: 50, delayMs: 50 } });
    let holders = 0;
    let maxHolders = 0;
    const critical = async (): Promise<void> => {
      const value = await mutex.withLock("hot", async () => {
        holders += 1;
        maxHolders = Math.max(maxHolders, holders);
        await Bun.sleep(50);
        holders -= 1;
        return true;
      });
      expect(value).toBe(true);
    };
    await Promise.all(Array.from({ length: 8 }, critical));
    expect(maxHolders).toBe(1);
  }, 30_000);
});
