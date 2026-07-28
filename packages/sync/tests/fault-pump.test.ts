import { afterEach, beforeEach, expect, test } from "bun:test";
import { redis } from "bun";
import { pump, type PumpHandle } from "../index";

const handles: PumpHandle<unknown, unknown>[] = [];
const uid = (name: string): string => `${name}-${Date.now()}-${Math.floor(Math.random() * 1_000_000)}`;

const track = <I, C>(handle: PumpHandle<I, C>): PumpHandle<I, C> => {
  handles.push(handle as PumpHandle<unknown, unknown>);
  return handle;
};

const waitFor = async (
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 10_000,
  pollMs = 20,
): Promise<void> => {
  const startedAt = Date.now();
  while (!(await predicate())) {
    if (Date.now() - startedAt > timeoutMs) {
      throw new Error(`waitFor timed out after ${timeoutMs}ms`);
    }
    await Bun.sleep(pollMs);
  }
};

beforeEach(async () => {
  const keys = await redis.send("KEYS", ["test:pump:fault:*"]);
  if (Array.isArray(keys) && keys.length > 0) await redis.send("DEL", keys as string[]);
});

afterEach(() => {
  for (const handle of handles.splice(0)) handle.stop();
});

test("lease loss fences the stale worker before its item checkpoint", async () => {
  const id = uid("lease-loss");
  const key = "run";
  const prefix = "test:pump:fault";
  const stateKey = `${prefix}:${id}:run:${encodeURIComponent(key)}`;
  const dueKey = `${prefix}:${id}:due`;
  const calls: string[] = [];
  let stoleLease = false;

  const worker = track(pump<{ source: string }, number, { key: string }>({
    id,
    prefix,
    defaults: { leaseMs: 100, heartbeatMs: 20 },
    pull: () => ({
      items: [{ key: "a" }, { key: "b" }],
      nextCursor: null,
    }),
    dispatch: async ({ item }) => {
      calls.push(item.key);
      if (item.key !== "a" || stoleLease) return;
      stoleLease = true;

      const state = JSON.parse((await redis.get(stateKey))!) as Record<string, unknown>;
      state.leaseToken = "stolen";
      state.leaseUntil = Date.now() - 1;
      await redis.set(stateKey, JSON.stringify(state));
      await redis.send("ZADD", [dueKey, String(Date.now()), encodeURIComponent(key)]);
    },
  }));

  await worker.start({ key, input: { source: "fault" } });
  await waitFor(async () => (await worker.get({ key }))?.state === "completed");

  expect(calls).toEqual(["a", "a", "b"]);
  expect((await worker.get({ key }))?.dispatched).toBe(2);
});
