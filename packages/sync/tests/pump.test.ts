import { afterEach, beforeEach, expect, test } from "bun:test";
import { redis } from "bun";
import { pump, type PumpHandle } from "../index";

type Input = { source: string };
type Cursor = number;
type Item = { key: string; value: number };

const handles: PumpHandle<unknown, unknown>[] = [];
const uid = (name: string): string => `${name}-${Date.now()}-${Math.floor(Math.random() * 1_000_000)}`;

const track = <I, C>(handle: PumpHandle<I, C>): PumpHandle<I, C> => {
  handles.push(handle as PumpHandle<unknown, unknown>);
  return handle;
};

const waitFor = async (
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 5_000,
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
  const keys = await redis.send("KEYS", ["test:pump:*"]);
  if (Array.isArray(keys) && keys.length > 0) await redis.send("DEL", keys as string[]);
});

afterEach(() => {
  for (const handle of handles.splice(0)) handle.stop();
});

test("processes pages sequentially and exposes durable progress", async () => {
  const values = [1, 2, 3, 4, 5];
  const pulledFrom: Array<number | null> = [];
  const dispatched: number[] = [];

  const worker = track(pump<Input, Cursor, Item>({
    id: uid("pages"),
    prefix: "test:pump",
    batchSize: 2,
    pull: ({ cursor, limit }) => {
      pulledFrom.push(cursor);
      const start = cursor ?? 0;
      const rows = values.slice(start, start + limit);
      const end = start + rows.length;
      return {
        items: rows.map((value) => ({ key: String(value), value })),
        nextCursor: end < values.length ? end : null,
      };
    },
    dispatch: ({ item }) => {
      dispatched.push(item.value);
    },
  }));

  const started = await worker.start({
    key: "backfill:1",
    input: { source: "messages" },
    meta: { requestedBy: "test" },
  });
  expect(started.state).toBe("queued");

  await waitFor(async () => (await worker.get({ key: "backfill:1" }))?.state === "completed");

  expect(pulledFrom).toEqual([null, 2, 4]);
  expect(dispatched).toEqual(values);
  expect(await worker.get({ key: "backfill:1" })).toMatchObject({
    state: "completed",
    dispatched: 5,
    failureCount: 0,
    input: { source: "messages" },
    meta: { requestedBy: "test" },
  });
});

test("start is idempotent for the same key", async () => {
  let pulls = 0;
  const worker = track(pump<Input, Cursor, Item>({
    id: uid("idempotent"),
    prefix: "test:pump",
    pull: async () => {
      pulls += 1;
      await Bun.sleep(50);
      return { items: [], nextCursor: null };
    },
    dispatch: () => {},
  }));

  const first = await worker.start({ key: "same", input: { source: "first" } });
  const second = await worker.start({ key: "same", input: { source: "ignored" } });

  expect(second.createdAt).toBe(first.createdAt);
  expect(second.input).toEqual({ source: "first" });
  await waitFor(async () => (await worker.get({ key: "same" }))?.state === "completed");
  expect(pulls).toBe(1);
});

test("failed dispatch retries the persisted page without pulling again", async () => {
  let pulls = 0;
  let failedOnce = false;
  const calls: string[] = [];

  const worker = track(pump<Input, Cursor, Item>({
    id: uid("retry-page"),
    prefix: "test:pump",
    retry: { maxAttempts: 3, baseMs: 10, maxMs: 10, jitter: 0 },
    pull: () => {
      pulls += 1;
      return {
        items: [
          { key: "a", value: 1 },
          { key: "b", value: 2 },
          { key: "c", value: 3 },
        ],
        nextCursor: null,
      };
    },
    dispatch: ({ item }) => {
      calls.push(item.key);
      if (item.key === "b" && !failedOnce) {
        failedOnce = true;
        throw new Error("temporary");
      }
    },
  }));

  await worker.start({ key: "retry", input: { source: "messages" } });
  await waitFor(async () => (await worker.get({ key: "retry" }))?.state === "completed");

  expect(pulls).toBe(1);
  expect(calls).toEqual(["a", "b", "b", "c"]);
  expect((await worker.get({ key: "retry" }))?.dispatched).toBe(3);
});

test("preserves empty JSON containers across Redis state transitions", async () => {
  type ShapeInput = { filters: string[] };
  type ShapeCursor = { page: number; tokens: string[] };
  type ShapeItem = { key: string; tags: string[]; attributes: Record<string, never> };

  const seenInputs: ShapeInput[] = [];
  const seenItems: ShapeItem[] = [];

  const worker = track(pump<ShapeInput, ShapeCursor, ShapeItem>({
    id: uid("json-shapes"),
    prefix: "test:pump",
    pull: ({ input, cursor }) => {
      seenInputs.push(input);
      if (cursor === null) {
        return {
          items: [{ key: "a", tags: [], attributes: {} }],
          nextCursor: { page: 1, tokens: [] },
        };
      }
      expect(cursor).toEqual({ page: 1, tokens: [] });
      return { items: [], nextCursor: null };
    },
    dispatch: ({ input, item }) => {
      seenInputs.push(input);
      seenItems.push(item);
    },
  }));

  await worker.start({ key: "shapes", input: { filters: [] } });
  await waitFor(async () => (await worker.get({ key: "shapes" }))?.state === "completed");

  expect(seenInputs).toEqual([
    { filters: [] },
    { filters: [] },
    { filters: [] },
  ]);
  expect(seenItems).toEqual([{ key: "a", tags: [], attributes: {} }]);
});

test("another worker resumes the persisted page after local stop", async () => {
  const id = uid("handover");
  let secondItemStarted = false;
  const firstWorkerCalls: string[] = [];
  const secondWorkerCalls: string[] = [];

  const first = track(pump<Input, Cursor, Item>({
    id,
    prefix: "test:pump",
    defaults: { leaseMs: 100, heartbeatMs: 20 },
    pull: () => ({
      items: [
        { key: "a", value: 1 },
        { key: "b", value: 2 },
        { key: "c", value: 3 },
      ],
      nextCursor: null,
    }),
    dispatch: async ({ item, signal }) => {
      firstWorkerCalls.push(item.key);
      if (item.key !== "b") return;
      secondItemStarted = true;
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(resolve, 5_000);
        signal.addEventListener("abort", () => {
          clearTimeout(timer);
          reject(new Error("stopped"));
        }, { once: true });
      });
    },
  }));

  await first.start({ key: "handover", input: { source: "messages" } });
  await waitFor(() => secondItemStarted);
  first.stop();

  const second = track(pump<Input, Cursor, Item>({
    id,
    prefix: "test:pump",
    defaults: { leaseMs: 100, heartbeatMs: 20 },
    pull: () => {
      throw new Error("persisted page must be reused");
    },
    dispatch: ({ item }) => {
      secondWorkerCalls.push(item.key);
    },
  }));

  await waitFor(async () => (await second.get({ key: "handover" }))?.state === "completed");

  expect(firstWorkerCalls).toEqual(["a", "b"]);
  expect(secondWorkerCalls).toEqual(["b", "c"]);
  expect((await second.get({ key: "handover" }))?.dispatched).toBe(3);
});

test("cancel aborts the active callback and prevents further items", async () => {
  let firstStarted = false;
  const calls: string[] = [];

  const worker = track(pump<Input, Cursor, Item>({
    id: uid("cancel"),
    prefix: "test:pump",
    defaults: { heartbeatMs: 20 },
    pull: () => ({
      items: [
        { key: "a", value: 1 },
        { key: "b", value: 2 },
      ],
      nextCursor: null,
    }),
    dispatch: async ({ item, signal }) => {
      calls.push(item.key);
      firstStarted = true;
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(resolve, 5_000);
        signal.addEventListener("abort", () => {
          clearTimeout(timer);
          reject(new Error("canceled"));
        }, { once: true });
      });
    },
  }));

  await worker.start({ key: "cancel", input: { source: "messages" } });
  await waitFor(() => firstStarted);
  expect(await worker.cancel({ key: "cancel" })).toBe(true);
  await Bun.sleep(100);

  expect(calls).toEqual(["a"]);
  expect((await worker.get({ key: "cancel" }))?.state).toBe("canceled");
  expect(await worker.cancel({ key: "cancel" })).toBe(false);
});

test("automatically heartbeats the lease during a long pull", async () => {
  const id = uid("heartbeat");
  let releasePull!: () => void;
  let pullStarted = false;

  const worker = track(pump<Input, Cursor, Item>({
    id,
    prefix: "test:pump",
    defaults: { leaseMs: 90, heartbeatMs: 20 },
    pull: async () => {
      pullStarted = true;
      await new Promise<void>((resolve) => {
        releasePull = resolve;
      });
      return { items: [], nextCursor: null };
    },
    dispatch: () => {},
  }));

  await worker.start({ key: "heartbeat", input: { source: "messages" } });
  await waitFor(() => pullStarted);

  const stateKey = `test:pump:${id}:run:${encodeURIComponent("heartbeat")}`;
  const first = JSON.parse((await redis.get(stateKey))!) as { leaseUntil: number };
  await Bun.sleep(70);
  const second = JSON.parse((await redis.get(stateKey))!) as { leaseUntil: number };
  expect(second.leaseUntil).toBeGreaterThan(first.leaseUntil);

  releasePull();
  await waitFor(async () => (await worker.get({ key: "heartbeat" }))?.state === "completed");
});

test("rejects non-serializable input and terminally fails oversized pages", async () => {
  const invalid = track(pump<{ createdAt: Date }, Cursor, Item>({
    id: uid("invalid-input"),
    prefix: "test:pump",
    pull: () => ({ items: [], nextCursor: null }),
    dispatch: () => {},
  }));

  await expect(invalid.start({
    key: "invalid",
    input: { createdAt: new Date() },
  })).rejects.toThrow("plain objects and arrays");

  const oversized = track(pump<Input, Cursor, Item>({
    id: uid("oversized"),
    prefix: "test:pump",
    limits: { pageBytes: 20 },
    retry: { maxAttempts: 1 },
    pull: () => ({
      items: [{ key: "large", value: 123456789 }],
      nextCursor: null,
    }),
    dispatch: () => {},
  }));

  await oversized.start({ key: "oversized", input: { source: "messages" } });
  await waitFor(async () => (await oversized.get({ key: "oversized" }))?.state === "failed");
  expect((await oversized.get({ key: "oversized" }))?.lastError).toContain("page exceeds limit");
});

test("terminal state expires after retention", async () => {
  const worker = track(pump<Input, Cursor, Item>({
    id: uid("retention"),
    prefix: "test:pump",
    defaults: { terminalRetentionMs: 60 },
    pull: () => ({ items: [], nextCursor: null }),
    dispatch: () => {},
  }));

  await worker.start({ key: "expires", input: { source: "messages" } });
  await waitFor(async () => (await worker.get({ key: "expires" }))?.state === "completed");
  await Bun.sleep(90);
  expect(await worker.get({ key: "expires" })).toBeNull();
});
