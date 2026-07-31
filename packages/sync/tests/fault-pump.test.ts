import { afterEach, beforeEach, expect, test } from "bun:test";
import { redis } from "bun";
import { pump, type PumpHandle } from "../index";

const handles: PumpHandle<unknown, unknown>[] = [];
const uid = (name: string): string => `${name}-${Date.now()}-${Math.floor(Math.random() * 1_000_000)}`;
const PUMP_PREFIX = "test:pump:fault";
const pumpBaseKey = (prefix: string, id: string): string =>
  `sync:pump:namespace:v2:${encodeURIComponent(JSON.stringify([prefix, id]))}`;
const pumpNamespacePattern = (prefix: string): string =>
  `sync:pump:namespace:v2:${encodeURIComponent(`${JSON.stringify([prefix]).slice(0, -1)},`)}*`;

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

const cleanup = async (): Promise<void> => {
  const matches = await Promise.all([
    redis.send("KEYS", [`${PUMP_PREFIX}:*`]),
    redis.send("KEYS", [pumpNamespacePattern(PUMP_PREFIX)]),
  ]);
  const keys = matches.flatMap((value) => Array.isArray(value) ? value.map(String) : []);
  if (keys.length > 0) await redis.send("DEL", keys);
};

beforeEach(cleanup);

afterEach(async () => {
  for (const handle of handles.splice(0)) handle.stop();
  await Bun.sleep(20);
  await cleanup();
});

test("lease loss fences the stale worker before its item checkpoint", async () => {
  const id = uid("lease-loss");
  const key = "run";
  const prefix = PUMP_PREFIX;
  const baseKey = pumpBaseKey(prefix, id);
  const stateKey = `${baseKey}:run:${encodeURIComponent(key)}`;
  const dueKey = `${baseKey}:due`;
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
