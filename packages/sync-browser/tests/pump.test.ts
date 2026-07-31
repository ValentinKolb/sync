import { afterEach, expect, test } from "bun:test";
import { migrateLegacyPumpState, pump, type PumpHandle } from "../src/pump";
import {
  createMemoryStore,
  LocalStorageStore,
  type MemoryStore,
  type Store,
} from "../src/store";

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

const pumpStateKey = (prefix: string, id: string, key: string): string =>
  `sync:pump:browser:v2:${encodeURIComponent(JSON.stringify([prefix, id]))}:run:${encodeURIComponent(key)}`;

const legacyPumpStateKey = (prefix: string, id: string, key: string): string =>
  `${prefix}:${id}:run:${encodeURIComponent(key)}`;

const persistedPumpState = (
  key: string,
  cursor: unknown,
  state: "waiting" | "completed" = "waiting",
): Record<string, unknown> => {
  const now = Date.now();
  return {
    version: 1,
    key,
    input: { source: "legacy" },
    cursor,
    state,
    dispatched: 4,
    failureCount: 0,
    ...(state === "waiting" ? { nextRunAt: now } : {}),
    createdAt: now - 1_000,
    updatedAt: now,
  };
};

const createLocalStorageMock = (): Storage => {
  const values = new Map<string, string>();
  return {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => {
      values.set(key, value);
    },
    removeItem: (key: string) => {
      values.delete(key);
    },
    key: (index: number) => [...values.keys()][index] ?? null,
    get length() {
      return values.size;
    },
    clear: () => values.clear(),
  } as Storage;
};

const forwardingStore = (
  store: MemoryStore,
  overrides: Partial<Pick<Store, "set" | "del">>,
): Store => ({
  get: (key) => store.get(key),
  set: overrides.set ?? ((key, value, ttlMs) => store.set(key, value, ttlMs)),
  del: overrides.del ?? ((key) => store.del(key)),
  keys: (prefix) => store.keys(prefix),
});

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

test("colon-rich pump identities keep persisted runs isolated", async () => {
  const store = trackStore();
  const base = uid("identity");
  const first = track(pump<Input, Cursor, Item>({
    id: "b",
    prefix: `${base}:a`,
    store,
    pull: () => ({ items: [], nextCursor: null }),
    dispatch: () => {},
  }));
  const second = track(pump<Input, Cursor, Item>({
    id: "a:b",
    prefix: base,
    store,
    pull: () => ({ items: [], nextCursor: null }),
    dispatch: () => {},
  }));

  const firstState = await first.start({ key: "run", input: { source: "first" } });
  const secondState = await second.start({ key: "run", input: { source: "second" } });

  expect(firstState.input).toEqual({ source: "first" });
  expect(secondState.input).toEqual({ source: "second" });
});

test("pump operations fail loudly while exact legacy state needs migration", async () => {
  const store = trackStore();
  const id = uid("legacy-required");
  const key = "run";
  const state = persistedPumpState(key, 4);
  store.set(legacyPumpStateKey("sync:pump", id, key), state);
  store.set(pumpStateKey("sync:pump", id, key), state);
  let pulls = 0;

  const worker = track(pump<Input, Cursor, Item>({
    id,
    store,
    pull: () => {
      pulls += 1;
      return { items: [], nextCursor: null };
    },
    dispatch: () => {},
  }));

  await Bun.sleep(20);
  await expect(worker.get({ key })).rejects.toThrow(/migrateLegacyPumpState/);
  await expect(worker.cancel({ key })).rejects.toThrow(/migrateLegacyPumpState/);
  await expect(worker.start({ key, input: { source: "new" } })).rejects.toThrow(/migrateLegacyPumpState/);
  expect(pulls).toBe(0);
  expect(store.get(pumpStateKey("sync:pump", id, key))).toEqual(state);
});

test("corrupt or key-mismatched exact legacy state prevents a fresh start", async () => {
  const invalidStates: Array<[string, unknown]> = [
    ["corrupt", "not-a-pump-state"],
    ["key-mismatched", persistedPumpState("different", 4)],
  ];

  for (const [name, state] of invalidStates) {
    const store = trackStore();
    const id = uid(`legacy-invalid-${name}`);
    const key = "run";
    const legacyKey = legacyPumpStateKey("sync:pump", id, key);
    store.set(legacyKey, state);

    const worker = track(pump<Input, Cursor, Item>({
      id,
      store,
      pull: () => ({ items: [], nextCursor: null }),
      dispatch: () => {},
    }));

    await expect(worker.start({ key, input: { source: "new" } })).rejects.toThrow(
      /invalid legacy pump state/,
    );
    expect(store.get(legacyKey)).toEqual(state);
    expect(store.get(pumpStateKey("sync:pump", id, key))).toBeUndefined();
  }
});

test("legacy migration preserves the cursor and a new worker resumes it", async () => {
  const store = trackStore();
  const id = uid("legacy-resume");
  const key = "run";
  const legacyKey = legacyPumpStateKey("sync:pump", id, key);
  store.set(legacyKey, persistedPumpState(key, 4));

  store.set(legacyPumpStateKey("sync:pump", id, "other"), persistedPumpState("other", 8));

  expect(migrateLegacyPumpState({ store, id, key })).toEqual({ status: "migrated" });
  expect(store.get(legacyKey)).toBeUndefined();
  expect(store.get(legacyPumpStateKey("sync:pump", id, "other"))).toMatchObject({ cursor: 8 });
  expect(store.get(pumpStateKey("sync:pump", id, key))).toMatchObject({ cursor: 4 });

  const cursors: Array<number | null> = [];
  const worker = track(pump<Input, Cursor, Item>({
    id,
    store,
    pull: ({ cursor }) => {
      cursors.push(cursor);
      return { items: [], nextCursor: null };
    },
    dispatch: () => {},
  }));

  await waitFor(async () => (await worker.get({ key }))?.state === "completed");
  expect(cursors).toEqual([4]);
});

test("legacy migration accepts an identical destination and retries idempotently", () => {
  const store = trackStore();
  const id = uid("legacy-idempotent");
  const key = "run";
  const state = persistedPumpState(key, 4);
  const legacyKey = legacyPumpStateKey("sync:pump", id, key);
  const destinationKey = pumpStateKey("sync:pump", id, key);
  store.set(legacyKey, state);
  store.set(destinationKey, state);

  expect(migrateLegacyPumpState({ store, id, key })).toEqual({ status: "already-migrated" });
  expect(store.get(legacyKey)).toBeUndefined();
  expect(store.get(destinationKey)).toEqual(state);
  expect(migrateLegacyPumpState({ store, id, key })).toEqual({ status: "already-migrated" });
});

test("legacy migration rejects conflicting destination state", () => {
  const store = trackStore();
  const id = uid("legacy-conflict");
  const key = "run";
  const legacyKey = legacyPumpStateKey("sync:pump", id, key);
  const destinationKey = pumpStateKey("sync:pump", id, key);
  store.set(legacyKey, persistedPumpState(key, 4));
  store.set(destinationKey, persistedPumpState(key, 9));

  expect(() => migrateLegacyPumpState({ store, id, key })).toThrow(
    /conflicting migrated pump state/,
  );
  expect(store.get(legacyKey)).toMatchObject({ cursor: 4 });
  expect(store.get(destinationKey)).toMatchObject({ cursor: 9 });
});

test("legacy migration rejects state bound to a different legacy key", () => {
  const store = trackStore();
  const id = uid("legacy-binding");
  const legacyKey = legacyPumpStateKey("sync:pump", id, "requested");
  store.set(legacyKey, persistedPumpState("different", 4));

  expect(() => migrateLegacyPumpState({ store, id, key: "requested" })).toThrow(
    /invalid legacy pump state/,
  );
  expect(store.get(legacyKey)).toBeDefined();
});

test("legacy migration leaves the source intact when the destination write fails", () => {
  const memory = trackStore();
  const id = uid("legacy-set-failure");
  const key = "run";
  const legacyKey = legacyPumpStateKey("sync:pump", id, key);
  const destinationKey = pumpStateKey("sync:pump", id, key);
  const state = persistedPumpState(key, 4);
  memory.set(legacyKey, state);
  const store = forwardingStore(memory, {
    set: (target, value, ttlMs) => {
      if (target === destinationKey) throw new Error("destination unavailable");
      memory.set(target, value, ttlMs);
    },
  });

  expect(() => migrateLegacyPumpState({ store, id, key })).toThrow(/destination unavailable/);
  expect(memory.get(legacyKey)).toEqual(state);
  expect(memory.get(destinationKey)).toBeUndefined();
});

test("legacy migration retries idempotently after source deletion fails", () => {
  const memory = trackStore();
  const id = uid("legacy-delete-failure");
  const key = "run";
  const legacyKey = legacyPumpStateKey("sync:pump", id, key);
  const destinationKey = pumpStateKey("sync:pump", id, key);
  const state = persistedPumpState(key, 4);
  memory.set(legacyKey, state);
  let failDelete = true;
  const store = forwardingStore(memory, {
    del: (target) => {
      if (target === legacyKey && failDelete) {
        failDelete = false;
        throw new Error("source delete unavailable");
      }
      memory.del(target);
    },
  });

  expect(() => migrateLegacyPumpState({ store, id, key })).toThrow(/source delete unavailable/);
  expect(memory.get(legacyKey)).toEqual(state);
  expect(memory.get(destinationKey)).toEqual(state);

  expect(migrateLegacyPumpState({ store, id, key })).toEqual({ status: "already-migrated" });
  expect(memory.get(legacyKey)).toBeUndefined();
  expect(memory.get(destinationKey)).toEqual(state);
});

test("legacy migration uses the operator-selected identity for colon collisions", () => {
  const store = trackStore();
  const key = "run";
  const first = { prefix: "root:a", id: "b" };
  const second = { prefix: "root", id: "a:b" };
  const sharedLegacyKey = legacyPumpStateKey(first.prefix, first.id, key);
  expect(sharedLegacyKey).toBe(legacyPumpStateKey(second.prefix, second.id, key));
  store.set(sharedLegacyKey, persistedPumpState(key, 4));

  expect(migrateLegacyPumpState({ store, ...first, key })).toEqual({ status: "migrated" });
  expect(store.get(pumpStateKey(first.prefix, first.id, key))).toMatchObject({ cursor: 4 });
  expect(store.get(pumpStateKey(second.prefix, second.id, key))).toBeUndefined();
  expect(migrateLegacyPumpState({ store, ...second, key })).toEqual({ status: "not-found" });
});

test("corrupt LocalStorage legacy state blocks migration and a fresh start", async () => {
  const originalLocalStorage = globalThis.localStorage;
  globalThis.localStorage = createLocalStorageMock();
  try {
    const storagePrefix = "pump-corrupt";
    const store = new LocalStorageStore(storagePrefix);
    const id = uid("legacy-local-storage");
    const key = "run";
    const legacyKey = legacyPumpStateKey("sync:pump", id, key);
    const destinationKey = pumpStateKey("sync:pump", id, key);
    localStorage.setItem(`${storagePrefix}:${legacyKey}`, "not-json{{{");

    expect(() => migrateLegacyPumpState({ store, id, key })).toThrow(/invalid stored value/);

    const worker = track(pump<Input, Cursor, Item>({
      id,
      store,
      pull: () => ({ items: [], nextCursor: null }),
      dispatch: () => {},
    }));
    await expect(worker.start({ key, input: { source: "new" } })).rejects.toThrow(
      /invalid stored value/,
    );
    expect(localStorage.getItem(`${storagePrefix}:${legacyKey}`)).toBe("not-json{{{");
    expect(localStorage.getItem(`${storagePrefix}:${destinationKey}`)).toBeNull();
  } finally {
    globalThis.localStorage = originalLocalStorage;
  }
});

test("legacy migration applies bounded retention to terminal state", async () => {
  const store = trackStore();
  const id = uid("legacy-terminal");
  const key = "run";
  const destinationKey = pumpStateKey("sync:pump", id, key);
  store.set(
    legacyPumpStateKey("sync:pump", id, key),
    persistedPumpState(key, 4, "completed"),
  );

  migrateLegacyPumpState({ store, id, key, terminalRetentionMs: 30 });
  expect(store.get(destinationKey)).toMatchObject({ state: "completed", cursor: 4 });
  await Bun.sleep(50);
  expect(store.get(destinationKey)).toBeUndefined();
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

test("composed and decomposed Unicode cursor keys stall despite reversed insertion order", async () => {
  let pulls = 0;
  const worker = track(pump<Input, Record<string, number>, Item>({
    id: uid("canonical-cursor"),
    retry: { maxAttempts: 1, baseMs: 5, maxMs: 5, jitter: 0 },
    pull: ({ cursor }) => {
      pulls += 1;
      return cursor === null
        ? { items: [], nextCursor: { ["\u00e9"]: 1, ["e\u0301"]: 2 } }
        : { items: [], nextCursor: { ["e\u0301"]: 2, ["\u00e9"]: 1 } };
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

test("malformed persisted states are removed before pull and their keys are reusable", async () => {
  const store = trackStore();
  const id = uid("malformed");
  const malformed = [
    { key: "invalid-string", value: "not-a-pump-state" },
    { key: "invalid-shape", value: {} },
  ];
  for (const entry of malformed) {
    store.set(pumpStateKey("sync:pump", id, entry.key), entry.value);
  }

  let pulls = 0;
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
  for (const entry of malformed) {
    expect(await worker.get({ key: entry.key })).toBeNull();
    expect(store.get(pumpStateKey("sync:pump", id, entry.key))).toBeUndefined();
    await worker.start({ key: entry.key, input: { source: "messages" } });
    await waitFor(async () => (await worker.get({ key: entry.key }))?.state === "completed");
  }
  expect(pulls).toBe(2);
});

test("a stale worker cannot checkpoint after its lease token is replaced", async () => {
  const store = trackStore();
  const id = uid("stale-commit");
  const key = "run";
  const stateKey = pumpStateKey("sync:pump", id, key);
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
