import { afterEach, beforeEach, expect, test } from "bun:test";
import { redis } from "bun";
import { pump, type PumpHandle } from "../index";

type Input = { source: string };
type Cursor = number;
type Item = { key: string; value: number };

const handles: PumpHandle<unknown, unknown>[] = [];
const PUMP_PREFIX = "test:pump:unit";
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

const cleanup = async (): Promise<void> => {
  const keys = await redis.send("KEYS", [`${PUMP_PREFIX}:*`]);
  if (Array.isArray(keys) && keys.length > 0) await redis.send("DEL", keys as string[]);
};

beforeEach(cleanup);

afterEach(async () => {
  for (const handle of handles.splice(0)) handle.stop();
  await Bun.sleep(20);
  await cleanup();
});

test("rejects non-finite numeric configuration", () => {
  const base = {
    id: uid("finite-config"),
    pull: () => ({ items: [], nextCursor: null }),
    dispatch: () => {},
  };

  expect(() => pump({ ...base, batchSize: Number.NaN })).toThrow(/batchSize must be finite/);
  expect(() => pump({ ...base, defaults: { leaseMs: Number.POSITIVE_INFINITY } })).toThrow(
    /defaults\.leaseMs must be finite/,
  );
  expect(() => pump({ ...base, limits: { pageBytes: Number.NaN } })).toThrow(/limits\.pageBytes must be finite/);
  expect(() => pump({ ...base, retry: { maxAttempts: Number.POSITIVE_INFINITY } })).toThrow(
    /retry\.maxAttempts must be finite/,
  );
  expect(() => pump({ ...base, defaults: { terminalRetentionMs: 1.5 } })).toThrow(
    /defaults\.terminalRetentionMs must be a positive safe integer/,
  );
  expect(() =>
    pump({ ...base, defaults: { terminalRetentionMs: Number.MAX_SAFE_INTEGER + 1 } }),
  ).toThrow(/defaults\.terminalRetentionMs must be a positive safe integer/);
});

test("processes pages sequentially and exposes durable progress", async () => {
  const values = [1, 2, 3, 4, 5];
  const pulledFrom: Array<number | null> = [];
  const dispatched: number[] = [];

  const worker = track(pump<Input, Cursor, Item>({
    id: uid("pages"),
    prefix: PUMP_PREFIX,
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
    prefix: PUMP_PREFIX,
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

test("empty pages without cursor progress fail after maxAttempts", async () => {
  let pulls = 0;
  const worker = track(pump<Input, Cursor, Item>({
    id: uid("stalled"),
    prefix: PUMP_PREFIX,
    retry: { maxAttempts: 2, baseMs: 10, maxMs: 10, jitter: 0 },
    pull: ({ cursor }) => {
      pulls += 1;
      return { items: [], nextCursor: cursor ?? 1 };
    },
    dispatch: () => {},
  }));

  await worker.start({ key: "stalled", input: { source: "messages" } });
  await waitFor(async () => (await worker.get({ key: "stalled" }))?.state === "failed");

  expect(pulls).toBe(3);
  expect(await worker.get({ key: "stalled" })).toMatchObject({
    state: "failed",
    cursor: 1,
    failureCount: 2,
    lastError: "pull returned a page without advancing the cursor",
  });
});

test("object cursors with different property order still count as stalled", async () => {
  let pulls = 0;
  const worker = track(pump<Input, { page: number; shard: number }, Item>({
    id: uid("canonical-cursor"),
    prefix: PUMP_PREFIX,
    retry: { maxAttempts: 1, baseMs: 10, maxMs: 10, jitter: 0 },
    pull: ({ cursor }) => {
      pulls += 1;
      return cursor === null
        ? { items: [], nextCursor: { page: 1, shard: 2 } }
        : { items: [], nextCursor: { shard: 2, page: 1 } };
    },
    dispatch: () => {},
  }));

  await worker.start({ key: "canonical", input: { source: "messages" } });
  await waitFor(async () => (await worker.get({ key: "canonical" }))?.state === "failed");

  expect(pulls).toBe(2);
  expect(await worker.get({ key: "canonical" })).toMatchObject({
    state: "failed",
    failureCount: 1,
    lastError: "pull returned a page without advancing the cursor",
  });
});

test("failed dispatch retries the persisted page without pulling again", async () => {
  let pulls = 0;
  let failedOnce = false;
  const calls: string[] = [];

  const worker = track(pump<Input, Cursor, Item>({
    id: uid("retry-page"),
    prefix: PUMP_PREFIX,
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
  const seenCursors: Array<ShapeCursor | null> = [];

  const worker = track(pump<ShapeInput, ShapeCursor, ShapeItem>({
    id: uid("json-shapes"),
    prefix: PUMP_PREFIX,
    pull: ({ input, cursor }) => {
      seenInputs.push(input);
      seenCursors.push(cursor);
      if (cursor === null) {
        return {
          items: [{ key: "a", tags: [], attributes: {} }],
          nextCursor: { page: 1, tokens: [] },
        };
      }
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
  expect(seenCursors).toEqual([null, { page: 1, tokens: [] }]);
});

test("another worker resumes the persisted page after local stop", async () => {
  const id = uid("handover");
  let secondItemStarted = false;
  const firstWorkerCalls: string[] = [];
  const secondWorkerCalls: string[] = [];

  const first = track(pump<Input, Cursor, Item>({
    id,
    prefix: PUMP_PREFIX,
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
    prefix: PUMP_PREFIX,
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
    prefix: PUMP_PREFIX,
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
    prefix: PUMP_PREFIX,
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

  const stateKey = `${PUMP_PREFIX}:${id}:run:${encodeURIComponent("heartbeat")}`;
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
    prefix: PUMP_PREFIX,
    pull: () => ({ items: [], nextCursor: null }),
    dispatch: () => {},
  }));

  await expect(invalid.start({
    key: "invalid",
    input: { createdAt: new Date() },
  })).rejects.toThrow("plain objects and arrays");

  const oversized = track(pump<Input, Cursor, Item>({
    id: uid("oversized"),
    prefix: PUMP_PREFIX,
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

test("rejects every unsupported JSON input shape before persisting a run", async () => {
  const circular: Record<string, unknown> = {};
  circular.self = circular;
  const invalidInputs: Array<[string, unknown]> = [
    ["undefined", { value: undefined }],
    ["function", { value: () => undefined }],
    ["symbol", { value: Symbol("value") }],
    ["bigint", { value: 1n }],
    ["non-finite", { value: Number.NaN }],
    ["circular", circular],
    ["non-plain", { value: new Date() }],
  ];

  for (const [name, input] of invalidInputs) {
    const worker = track(pump<unknown, Cursor, Item>({
      id: uid(`invalid-${name}`),
      prefix: PUMP_PREFIX,
      pull: () => ({ items: [], nextCursor: null }),
      dispatch: () => {},
    }));

    await expect(worker.start({ key: name, input })).rejects.toThrow();
    expect(await worker.get({ key: name })).toBeNull();
  }
});

test("terminally fails after the configured number of callback failures", async () => {
  let pulls = 0;
  const worker = track(pump<Input, Cursor, Item>({
    id: uid("terminal-failure"),
    prefix: PUMP_PREFIX,
    retry: { maxAttempts: 2, baseMs: 10, maxMs: 10, jitter: 0 },
    pull: () => {
      pulls += 1;
      throw new Error("provider unavailable");
    },
    dispatch: () => {},
  }));

  await worker.start({ key: "terminal", input: { source: "messages" } });
  await waitFor(async () => (await worker.get({ key: "terminal" }))?.state === "failed");

  expect(pulls).toBe(2);
  expect(await worker.get({ key: "terminal" })).toMatchObject({
    state: "failed",
    failureCount: 2,
    lastError: "provider unavailable",
  });
});

test("progress resets the stalled-page failure counter", async () => {
  let pulls = 0;
  const worker = track(pump<Input, Cursor, Item>({
    id: uid("stall-reset"),
    prefix: PUMP_PREFIX,
    retry: { maxAttempts: 2, baseMs: 10, maxMs: 10, jitter: 0 },
    pull: () => {
      pulls += 1;
      if (pulls === 1) return { items: [], nextCursor: 1 };
      if (pulls === 2) return { items: [], nextCursor: 1 };
      if (pulls === 3) return { items: [{ key: "progress", value: 1 }], nextCursor: 2 };
      if (pulls === 4) return { items: [], nextCursor: 2 };
      return { items: [], nextCursor: null };
    },
    dispatch: () => {},
  }));

  await worker.start({ key: "reset", input: { source: "messages" } });
  await waitFor(async () => (await worker.get({ key: "reset" }))?.state === "completed");

  expect(pulls).toBe(5);
  expect(await worker.get({ key: "reset" })).toMatchObject({
    state: "completed",
    dispatched: 1,
    failureCount: 0,
  });
});

test("a progressing page resets a prior stall before dispatch retry accounting", async () => {
  let pulls = 0;
  let failedOnce = false;
  const dispatches: string[] = [];
  const worker = track(pump<Input, Cursor, Item>({
    id: uid("stall-dispatch-reset"),
    prefix: PUMP_PREFIX,
    retry: { maxAttempts: 2, baseMs: 10, maxMs: 10, jitter: 0 },
    pull: () => {
      pulls += 1;
      if (pulls === 1) return { items: [], nextCursor: 1 };
      if (pulls === 2) return { items: [], nextCursor: 1 };
      if (pulls === 3) return { items: [{ key: "a", value: 1 }], nextCursor: 2 };
      return { items: [], nextCursor: null };
    },
    dispatch: ({ item }) => {
      dispatches.push(item.key);
      if (!failedOnce) {
        failedOnce = true;
        throw new Error("temporary dispatch failure");
      }
    },
  }));

  await worker.start({ key: "reset", input: { source: "messages" } });
  await waitFor(async () => (await worker.get({ key: "reset" }))?.state === "completed");

  expect(pulls).toBe(4);
  expect(dispatches).toEqual(["a", "a"]);
  expect(await worker.get({ key: "reset" })).toMatchObject({
    state: "completed",
    dispatched: 1,
    failureCount: 0,
  });
});

test("a non-empty page with an unchanged cursor dispatches at least once and fails boundedly", async () => {
  let pulls = 0;
  const dispatches: string[] = [];
  const worker = track(pump<Input, Cursor, Item>({
    id: uid("non-empty-stall"),
    prefix: PUMP_PREFIX,
    retry: { maxAttempts: 2, baseMs: 10, maxMs: 10, jitter: 0 },
    pull: () => {
      pulls += 1;
      if (pulls === 1) return { items: [], nextCursor: 1 };
      return { items: [{ key: "same", value: pulls }], nextCursor: 1 };
    },
    dispatch: ({ item }) => {
      dispatches.push(item.key);
    },
  }));

  await worker.start({ key: "bounded", input: { source: "messages" } });
  await waitFor(async () => (await worker.get({ key: "bounded" }))?.state === "failed");

  expect(pulls).toBe(3);
  expect(dispatches).toEqual(["same", "same"]);
  expect(await worker.get({ key: "bounded" })).toMatchObject({
    state: "failed",
    dispatched: 2,
    failureCount: 2,
    lastError: "pull returned a page without advancing the cursor",
  });
});

test("malformed persisted states are removed and their keys are reusable", async () => {
  const id = uid("malformed");
  const dueKey = `${PUMP_PREFIX}:${id}:due`;
  const malformed = [
    { key: "invalid-json", raw: "{not-json" },
    { key: "invalid-shape", raw: "{}" },
    { key: "primitive-string", raw: JSON.stringify("broken") },
    { key: "primitive-number", raw: JSON.stringify(42) },
    { key: "primitive-boolean", raw: JSON.stringify(true) },
    { key: "primitive-null", raw: "null" },
  ];
  const now = Date.now();
  for (const [index, entry] of malformed.entries()) {
    const member = encodeURIComponent(entry.key);
    await redis.set(`${PUMP_PREFIX}:${id}:run:${member}`, entry.raw);
    await redis.send("ZADD", [dueKey, String(now - malformed.length + index), member]);
  }

  let pulls = 0;
  const worker = track(pump<Input, Cursor, Item>({
    id,
    prefix: PUMP_PREFIX,
    pull: () => {
      pulls += 1;
      return { items: [], nextCursor: null };
    },
    dispatch: () => {},
  }));

  await worker.start({ key: "healthy", input: { source: "messages" } });
  await waitFor(async () => (await worker.get({ key: "healthy" }))?.state === "completed");

  await waitFor(async () => {
    for (const entry of malformed) {
      const member = encodeURIComponent(entry.key);
      if (await redis.get(`${PUMP_PREFIX}:${id}:run:${member}`)) return false;
      if (await redis.send("ZSCORE", [dueKey, member])) return false;
    }
    return true;
  });

  expect(pulls).toBe(1);
  for (const entry of malformed) {
    expect(await worker.get({ key: entry.key })).toBeNull();
    await worker.start({ key: entry.key, input: { source: "messages" } });
    await waitFor(async () => (await worker.get({ key: entry.key }))?.state === "completed");
  }
  expect(pulls).toBe(malformed.length + 1);
});

test("a stale worker cannot checkpoint after its lease token is replaced", async () => {
  const id = uid("stale-commit");
  const key = "run";
  const stateKey = `${PUMP_PREFIX}:${id}:run:${encodeURIComponent(key)}`;
  let dispatchStarted = false;
  let releaseDispatch!: () => void;
  let dispatchFinished = false;

  const worker = track(pump<Input, Cursor, Item>({
    id,
    prefix: PUMP_PREFIX,
    defaults: { leaseMs: 1_000, heartbeatMs: 100 },
    pull: () => ({
      items: [{ key: "a", value: 1 }],
      nextCursor: null,
    }),
    dispatch: async () => {
      dispatchStarted = true;
      await new Promise<void>((resolve) => {
        releaseDispatch = resolve;
      });
      dispatchFinished = true;
    },
  }));

  await worker.start({ key, input: { source: "messages" } });
  await waitFor(() => dispatchStarted);

  const stolen = JSON.parse((await redis.get(stateKey))!) as Record<string, unknown>;
  stolen.leaseToken = "replacement";
  stolen.leaseUntil = Date.now() + 5_000;
  await redis.set(stateKey, JSON.stringify(stolen));
  releaseDispatch();
  await waitFor(() => dispatchFinished);
  await Bun.sleep(50);

  const persisted = JSON.parse((await redis.get(stateKey))!) as Record<string, unknown>;
  expect(persisted.leaseToken).toBe("replacement");
  expect(persisted.pageNextIndex).toBe(0);
  expect(persisted.dispatched).toBe(0);
  expect((await worker.get({ key }))?.state).toBe("running");
});

test("terminal state expires after retention", async () => {
  const worker = track(pump<Input, Cursor, Item>({
    id: uid("retention"),
    prefix: PUMP_PREFIX,
    defaults: { terminalRetentionMs: 60 },
    pull: () => ({ items: [], nextCursor: null }),
    dispatch: () => {},
  }));

  await worker.start({ key: "expires", input: { source: "messages" } });
  await waitFor(async () => (await worker.get({ key: "expires" }))?.state === "completed");
  await Bun.sleep(90);
  expect(await worker.get({ key: "expires" })).toBeNull();
});
