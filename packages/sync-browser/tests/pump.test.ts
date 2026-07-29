import { afterEach, expect, test } from "bun:test";
import { pump, type PumpHandle } from "../src/pump";
import { createMemoryStore, type MemoryStore } from "../src/store";

type Input = { source: string };
type Cursor = number;
type Item = { key: string; value: number };

const handles: PumpHandle<unknown, unknown>[] = [];
const stores: MemoryStore[] = [];
const uid = (name: string): string => `${name}-${Date.now()}-${Math.floor(Math.random() * 1_000_000)}`;

const track = <I, C>(handle: PumpHandle<I, C>): PumpHandle<I, C> => {
  handles.push(handle as PumpHandle<unknown, unknown>);
  return handle;
};

const trackStore = (store = createMemoryStore()): MemoryStore => {
  stores.push(store);
  return store;
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
  for (const store of stores.splice(0)) store.clear();
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
  const store = trackStore();
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

test("default same-id handles share persisted runs", async () => {
  const id = uid("shared-default");
  let releasePull!: () => void;
  const pull = async (): Promise<{ items: Item[]; nextCursor: null }> => {
    await new Promise<void>((resolve) => {
      releasePull = resolve;
    });
    return { items: [], nextCursor: null };
  };

  const first = track(pump<Input, Cursor, Item>({
    id,
    pull,
    dispatch: () => {},
  }));
  const second = track(pump<Input, Cursor, Item>({
    id,
    pull,
    dispatch: () => {},
  }));

  await first.start({ key: "shared", input: { source: "first" } });
  await waitFor(() => releasePull !== undefined);

  expect(await second.get({ key: "shared" })).toMatchObject({
    key: "shared",
    input: { source: "first" },
    state: "running",
  });

  releasePull();
  await waitFor(async () => (await second.get({ key: "shared" }))?.state === "completed");
});

test("empty pages without cursor progress fail after maxAttempts", async () => {
  let pulls = 0;
  const worker = track(pump<Input, Cursor, Item>({
    id: uid("stalled"),
    retry: { maxAttempts: 2, baseMs: 5, maxMs: 5, jitter: 0 },
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
  const store = trackStore();
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
  let abortObserved = false;
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
          abortObserved = true;
          reject(new Error("canceled"));
        }, { once: true });
      });
    },
  }));

  await worker.start({ key: "cancel", input: { source: "messages" } });
  await waitFor(() => started);
  expect(await worker.cancel({ key: "cancel" })).toBe(true);
  await waitFor(() => abortObserved);

  expect(calls).toEqual(["a"]);
  expect((await worker.get({ key: "cancel" }))?.state).toBe("canceled");
  expect(await worker.cancel({ key: "cancel" })).toBe(false);
});

test("terminally fails after the configured number of callback failures", async () => {
  let pulls = 0;
  const worker = track(pump<Input, Cursor, Item>({
    id: uid("terminal-failure"),
    retry: { maxAttempts: 2, baseMs: 5, maxMs: 5, jitter: 0 },
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

test("terminally fails a page that exceeds pageBytes without dispatching it", async () => {
  let dispatches = 0;
  const worker = track(pump<Input, Cursor, Item>({
    id: uid("page-bytes"),
    limits: { pageBytes: 20 },
    retry: { maxAttempts: 1 },
    pull: () => ({
      items: [{ key: "large", value: 123456789 }],
      nextCursor: null,
    }),
    dispatch: () => {
      dispatches += 1;
    },
  }));

  await worker.start({ key: "oversized", input: { source: "messages" } });
  await waitFor(async () => (await worker.get({ key: "oversized" }))?.state === "failed");

  expect(dispatches).toBe(0);
  expect((await worker.get({ key: "oversized" }))?.lastError).toContain("page exceeds limit");
});

test("accepts a page exactly at the server pageBytes boundary", async () => {
  const items: Item[] = [{ key: "boundary", value: 1 }];
  const nextCursor = null;
  const pageBytes = new TextEncoder().encode(JSON.stringify({ items, nextCursor })).byteLength;
  let dispatches = 0;
  const worker = track(pump<Input, Cursor, Item>({
    id: uid("page-bytes-boundary"),
    limits: { pageBytes },
    retry: { maxAttempts: 1 },
    pull: () => ({ items, nextCursor }),
    dispatch: () => {
      dispatches += 1;
    },
  }));

  await worker.start({ key: "boundary", input: { source: "messages" } });
  await waitFor(async () => (await worker.get({ key: "boundary" }))?.state === "completed");

  expect(dispatches).toBe(1);
});

test("rejects every unsupported JSON input shape before persisting a run", async () => {
  const circular: Record<string, unknown> = {};
  circular.self = circular;
  const invalidInputs: Array<[string, unknown]> = [
    ["undefined", { value: undefined }],
    ["function", { value: () => undefined }],
    ["symbol", { value: Symbol("value") }],
    ["bigint", { value: 1n }],
    ["non-finite", { value: Number.POSITIVE_INFINITY }],
    ["circular", circular],
    ["non-plain", { value: new Date() }],
  ];

  for (const [name, input] of invalidInputs) {
    const store = trackStore();
    const worker = track(pump<unknown, Cursor, Item>({
      id: uid(`invalid-${name}`),
      store,
      pull: () => ({ items: [], nextCursor: null }),
      dispatch: () => {},
    }));

    await expect(worker.start({ key: name, input })).rejects.toThrow();
    expect(await worker.get({ key: name })).toBeNull();
  }
});

test("progress resets the stalled-page failure counter", async () => {
  let pulls = 0;
  const worker = track(pump<Input, Cursor, Item>({
    id: uid("stall-reset"),
    retry: { maxAttempts: 2, baseMs: 5, maxMs: 5, jitter: 0 },
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
    retry: { maxAttempts: 2, baseMs: 5, maxMs: 5, jitter: 0 },
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
    retry: { maxAttempts: 2, baseMs: 5, maxMs: 5, jitter: 0 },
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

test("malformed persisted state is ignored without invoking callbacks", async () => {
  const store = trackStore();
  const id = uid("malformed");
  const key = "broken";
  let pulls = 0;
  store.set(`sync:pump:${id}:run:${encodeURIComponent(key)}`, "not-a-pump-state");

  const worker = track(pump<Input, Cursor, Item>({
    id,
    store,
    pull: () => {
      pulls += 1;
      return { items: [], nextCursor: null };
    },
    dispatch: () => {},
  }));

  await Bun.sleep(300);
  expect(pulls).toBe(0);
  expect(await worker.get({ key })).toBeNull();
});

test("a stale worker cannot checkpoint after its lease token is replaced", async () => {
  const store = trackStore();
  const id = uid("stale-commit");
  const key = "run";
  const stateKey = `sync:pump:${id}:run:${encodeURIComponent(key)}`;
  let dispatchStarted = false;
  let releaseDispatch!: () => void;
  let dispatchFinished = false;

  const worker = track(pump<Input, Cursor, Item>({
    id,
    store,
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

  const stolen = store.get(stateKey) as Record<string, unknown>;
  stolen.leaseToken = "replacement";
  stolen.leaseUntil = Date.now() + 5_000;
  store.set(stateKey, stolen);
  releaseDispatch();
  await waitFor(() => dispatchFinished);
  await Bun.sleep(30);

  const persisted = store.get(stateKey) as {
    activePage: { nextIndex: number };
    dispatched: number;
    leaseToken: string;
  };
  expect(persisted.leaseToken).toBe("replacement");
  expect(persisted.activePage.nextIndex).toBe(0);
  expect(persisted.dispatched).toBe(0);
  expect((await worker.get({ key }))?.state).toBe("running");
});

test("different explicit stores isolate handles with the same id", async () => {
  const id = uid("store-isolation");
  const firstStore = trackStore();
  const secondStore = trackStore();
  let releaseFirst!: () => void;
  let firstStarted = false;

  const first = track(pump<Input, Cursor, Item>({
    id,
    store: firstStore,
    pull: async () => {
      firstStarted = true;
      await new Promise<void>((resolve) => {
        releaseFirst = resolve;
      });
      return { items: [], nextCursor: null };
    },
    dispatch: () => {},
  }));
  const second = track(pump<Input, Cursor, Item>({
    id,
    store: secondStore,
    pull: () => ({ items: [], nextCursor: null }),
    dispatch: () => {},
  }));

  await first.start({ key: "same", input: { source: "first" } });
  await waitFor(() => firstStarted);
  expect(await second.get({ key: "same" })).toBeNull();

  await second.start({ key: "same", input: { source: "second" } });
  await waitFor(async () => (await second.get({ key: "same" }))?.state === "completed");
  expect((await second.get({ key: "same" }))?.input).toEqual({ source: "second" });

  releaseFirst();
  await waitFor(async () => (await first.get({ key: "same" }))?.state === "completed");
});

test("terminal state expires from the configured store", async () => {
  const store = trackStore();
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
