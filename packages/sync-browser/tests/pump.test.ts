import { afterEach, expect, test } from "bun:test";
import { pump, type PumpHandle } from "../src/pump";
import { createMemoryStore } from "../src/store";

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
  pollMs = 10,
): Promise<void> => {
  const startedAt = Date.now();
  while (!(await predicate())) {
    if (Date.now() - startedAt > timeoutMs) {
      throw new Error(`waitFor timed out after ${timeoutMs}ms`);
    }
    await Bun.sleep(pollMs);
  }
};

afterEach(() => {
  for (const handle of handles.splice(0)) handle.stop();
});

test("processes multiple pages with the default configuration", async () => {
  const values = [1, 2, 3];
  const dispatched: number[] = [];

  const worker = track(pump<Input, Cursor, Item>({
    id: uid("pages"),
    batchSize: 2,
    pull: ({ cursor, limit }) => {
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

  await worker.start({ key: "run", input: { source: "messages" } });
  await waitFor(async () => (await worker.get({ key: "run" }))?.state === "completed");

  expect(dispatched).toEqual(values);
  expect(await worker.get({ key: "run" })).toMatchObject({
    state: "completed",
    dispatched: 3,
    input: { source: "messages" },
  });
});

test("start is idempotent for a persisted key", async () => {
  const store = createMemoryStore();
  let pulls = 0;
  const worker = track(pump<Input, Cursor, Item>({
    id: uid("idempotent"),
    store,
    pull: () => {
      pulls += 1;
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

test("retry resumes the active page from its item checkpoint", async () => {
  let pulls = 0;
  let failedOnce = false;
  const calls: string[] = [];

  const worker = track(pump<Input, Cursor, Item>({
    id: uid("retry"),
    retry: { maxAttempts: 3, baseMs: 5, maxMs: 5, jitter: 0 },
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
});

test("a new handle resumes a page from a shared store", async () => {
  const store = createMemoryStore();
  const id = uid("resume");
  let blocked = false;
  const resumed: string[] = [];

  const first = track(pump<Input, Cursor, Item>({
    id,
    store,
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
      if (item.key !== "b") return;
      blocked = true;
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(resolve, 5_000);
        signal.addEventListener("abort", () => {
          clearTimeout(timer);
          reject(new Error("stopped"));
        }, { once: true });
      });
    },
  }));

  await first.start({ key: "resume", input: { source: "messages" } });
  await waitFor(() => blocked);
  first.stop();

  const second = track(pump<Input, Cursor, Item>({
    id,
    store,
    pull: () => {
      throw new Error("persisted page must be reused");
    },
    dispatch: ({ item }) => {
      resumed.push(item.key);
    },
  }));

  await waitFor(async () => (await second.get({ key: "resume" }))?.state === "completed");
  expect(resumed).toEqual(["b", "c"]);
});

test("cancel prevents dispatching the next item", async () => {
  let started = false;
  const calls: string[] = [];

  const worker = track(pump<Input, Cursor, Item>({
    id: uid("cancel"),
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
      started = true;
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
  await waitFor(() => started);
  expect(await worker.cancel({ key: "cancel" })).toBe(true);
  await Bun.sleep(50);

  expect(calls).toEqual(["a"]);
  expect((await worker.get({ key: "cancel" }))?.state).toBe("canceled");
});

test("terminal state expires from the configured store", async () => {
  const store = createMemoryStore();
  const worker = track(pump<Input, Cursor, Item>({
    id: uid("retention"),
    store,
    defaults: { terminalRetentionMs: 30 },
    pull: () => ({ items: [], nextCursor: null }),
    dispatch: () => {},
  }));

  await worker.start({ key: "expires", input: { source: "messages" } });
  await waitFor(async () => (await worker.get({ key: "expires" }))?.state === "completed");
  await Bun.sleep(50);
  expect(await worker.get({ key: "expires" })).toBeNull();
});

test("trace failures do not change pump state", async () => {
  const originalWarn = console.warn;
  console.warn = () => {};
  try {
    const worker = track(pump<Input, Cursor, Item>({
      id: uid("trace"),
      trace: () => {
        throw new Error("trace unavailable");
      },
      pull: () => ({ items: [{ key: "a", value: 1 }], nextCursor: null }),
      dispatch: () => {},
    }));

    await worker.start({ key: "trace", input: { source: "messages" } });
    await waitFor(async () => (await worker.get({ key: "trace" }))?.state === "completed");
    expect((await worker.get({ key: "trace" }))?.dispatched).toBe(1);
  } finally {
    console.warn = originalWarn;
  }
});

test("a fast run does not starve a run that became due earlier", async () => {
  const order: string[] = [];

  const worker = track(
    pump<Input, Cursor, Item>({
      id: uid("starvation"),
      batchSize: 1,
      defaults: { delayMs: 0 },
      pull: ({ cursor, input }) => {
        const start = cursor ?? 0;
        return start >= 4
          ? { items: [], nextCursor: null }
          : { items: [{ key: `${input.source}:${start}`, value: start }], nextCursor: start + 1 };
      },
      dispatch: async ({ item }) => {
        order.push(item.key);
        await Bun.sleep(5);
      },
    }),
  );

  // `a` uses delayMs 0, so it re-arms nextRunAt to now after every page and
  // looks due on every poll. Taking the first eligible key in store order let
  // it run to completion before `b` was ever claimed.
  await worker.start({ key: "a", input: { source: "a" } });
  await worker.start({ key: "b", input: { source: "b" } });

  await waitFor(
    async () =>
      (await worker.get({ key: "a" }))?.state === "completed" &&
      (await worker.get({ key: "b" }))?.state === "completed",
    20_000,
  );

  expect(order.filter((k) => k.startsWith("a")).length).toBe(4);
  expect(order.filter((k) => k.startsWith("b")).length).toBe(4);

  // Interleaved rather than fully serialised: b's first item lands before a's last.
  const firstB = order.findIndex((k) => k.startsWith("b"));
  const lastA = order.map((k) => k.startsWith("a")).lastIndexOf(true);
  expect(firstB).toBeLessThan(lastA);
}, 30_000);
