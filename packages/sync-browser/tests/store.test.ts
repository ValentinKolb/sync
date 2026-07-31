import { test, expect, beforeEach, describe } from "bun:test";
import { MemoryStore } from "../src/store";

let store: MemoryStore;

beforeEach(() => {
  store = new MemoryStore();
});

// ==========================
// Basic get / set / del
// ==========================

describe("get", () => {
  test("returns undefined for a missing key", () => {
    expect(store.get("nonexistent")).toBeUndefined();
  });
});

describe("set and get roundtrip", () => {
  test("string value", () => {
    store.set("k", "hello");
    expect(store.get("k")).toBe("hello");
  });

  test("number value", () => {
    store.set("k", 42);
    expect(store.get("k")).toBe(42);
  });

  test("object value", () => {
    const obj = { a: 1, b: "two", nested: { c: true } };
    store.set("k", obj);
    expect(store.get("k")).toEqual(obj);
  });

  test("array value", () => {
    const arr = [1, "two", null, { x: 3 }];
    store.set("k", arr);
    expect(store.get("k")).toEqual(arr);
  });

  test("boolean value", () => {
    store.set("k", false);
    expect(store.get("k")).toBe(false);
  });

  test("null value", () => {
    store.set("k", null);
    expect(store.get("k")).toBeNull();
  });

  test("overwriting a key replaces the value", () => {
    store.set("k", "first");
    store.set("k", "second");
    expect(store.get("k")).toBe("second");
  });
});

describe("del", () => {
  test("removes an existing key", () => {
    store.set("k", "value");
    expect(store.get("k")).toBe("value");
    store.del("k");
    expect(store.get("k")).toBeUndefined();
  });

  test("is a no-op for a non-existent key", () => {
    // Should not throw
    store.del("ghost");
    expect(store.get("ghost")).toBeUndefined();
  });

  test("does not affect other keys", () => {
    store.set("a", 1);
    store.set("b", 2);
    store.del("a");
    expect(store.get("a")).toBeUndefined();
    expect(store.get("b")).toBe(2);
  });
});

// ==========================
// keys
// ==========================

describe("keys", () => {
  test("returns all keys when no prefix is given", () => {
    store.set("x", 1);
    store.set("y", 2);
    store.set("z", 3);
    expect(store.keys().sort()).toEqual(["x", "y", "z"]);
  });

  test("returns empty array when store is empty", () => {
    expect(store.keys()).toEqual([]);
  });

  test("filters by prefix", () => {
    store.set("user:1", "Alice");
    store.set("user:2", "Bob");
    store.set("item:1", "Sword");
    expect(store.keys("user:").sort()).toEqual(["user:1", "user:2"]);
  });

  test("returns empty array when no keys match prefix", () => {
    store.set("a", 1);
    expect(store.keys("z:")).toEqual([]);
  });

  test("prefix is exact startsWith match", () => {
    store.set("abc", 1);
    store.set("ab", 2);
    store.set("a", 3);
    expect(store.keys("ab").sort()).toEqual(["ab", "abc"]);
  });
});

// ==========================
// TTL
// ==========================

describe("TTL", () => {
  test("value is available immediately after set with ttlMs", () => {
    store.set("k", "value", 200);
    expect(store.get("k")).toBe("value");
  });

  test("value is gone after TTL expires", async () => {
    store.set("k", "value", 100);
    expect(store.get("k")).toBe("value");
    await Bun.sleep(200);
    expect(store.get("k")).toBeUndefined();
  });

  test("keys() excludes expired entries", async () => {
    store.set("a", 1, 100);
    store.set("b", 2); // no TTL
    await Bun.sleep(200);
    expect(store.keys()).toEqual(["b"]);
  });

  test("overwriting a key resets the TTL timer", async () => {
    store.set("k", "v1", 100);
    await Bun.sleep(50);
    // Overwrite with a fresh TTL
    store.set("k", "v2", 200);
    await Bun.sleep(100);
    // Original TTL (100ms) would have expired, but the new one (200ms) hasn't
    expect(store.get("k")).toBe("v2");
    await Bun.sleep(200);
    // Now the new TTL has expired
    expect(store.get("k")).toBeUndefined();
  });

  test("overwriting with no TTL removes the expiry", async () => {
    store.set("k", "v1", 100);
    store.set("k", "v2"); // no TTL
    await Bun.sleep(200);
    expect(store.get("k")).toBe("v2");
  });

  test("del clears the TTL timer", async () => {
    store.set("k", "value", 200);
    store.del("k");
    // Re-set without TTL after del
    store.set("k", "new-value");
    await Bun.sleep(250);
    // The old timer should not have deleted the new value
    expect(store.get("k")).toBe("new-value");
  });

  test("zero ttlMs is treated as no TTL", async () => {
    store.set("k", "value", 0);
    await Bun.sleep(30);
    expect(store.get("k")).toBe("value");
  });

  test("negative ttlMs is treated as no TTL", async () => {
    store.set("k", "value", -10);
    await Bun.sleep(30);
    expect(store.get("k")).toBe("value");
  });

  test("ttl beyond the native timer limit does not expire early", async () => {
    store.set("k", "value", 2_147_483_648);
    await Bun.sleep(10);
    expect(store.get("k")).toBe("value");
    store.clear();
  });
});

// ==========================
// clear
// ==========================

describe("clear", () => {
  test("removes all keys", () => {
    store.set("a", 1);
    store.set("b", 2);
    store.set("c", 3);
    store.clear();
    expect(store.keys()).toEqual([]);
    expect(store.get("a")).toBeUndefined();
    expect(store.get("b")).toBeUndefined();
    expect(store.get("c")).toBeUndefined();
  });

  test("clears TTL timers so they do not fire after clear", async () => {
    store.set("k", "value", 100);
    store.clear();
    // Re-set the same key with no TTL
    store.set("k", "fresh");
    await Bun.sleep(200);
    // The old timer should not have deleted the fresh value
    expect(store.get("k")).toBe("fresh");
  });

  test("is safe to call on an empty store", () => {
    store.clear();
    expect(store.keys()).toEqual([]);
  });

  test("store is usable after clear", () => {
    store.set("x", 1);
    store.clear();
    store.set("y", 2);
    expect(store.get("y")).toBe(2);
    expect(store.keys()).toEqual(["y"]);
  });
});

// ==========================
// LocalStorageStore
// ==========================

// Simple localStorage mock for testing
const createLocalStorageMock = () => {
  const store = new Map<string, string>();
  return {
    getItem: (key: string) => store.get(key) ?? null,
    setItem: (key: string, value: string) => store.set(key, value),
    removeItem: (key: string) => store.delete(key),
    key: (index: number) => [...store.keys()][index] ?? null,
    get length() { return store.size; },
    clear: () => store.clear(),
  } as unknown as Storage;
};

import { LocalStorageStore, StoreWriteError, createLocalStorageStore } from "../src/store";
import { StoreWriteError as PublicStoreWriteError } from "../index";
import { mutex } from "../src/mutex";

// We need to polyfill localStorage for Bun
const originalLocalStorage = globalThis.localStorage;

test("StoreWriteError is exported from the browser entrypoint", () => {
  expect(PublicStoreWriteError).toBe(StoreWriteError);
});

test("LocalStorageStore: get returns undefined for missing key", () => {
  globalThis.localStorage = createLocalStorageMock();
  try {
    const store = new LocalStorageStore("test");
    expect(store.get("missing")).toBeUndefined();
  } finally {
    globalThis.localStorage = originalLocalStorage;
  }
});

test("LocalStorageStore: set and get roundtrip", () => {
  globalThis.localStorage = createLocalStorageMock();
  try {
    const store = new LocalStorageStore("test");
    store.set("key1", { hello: "world" });
    expect(store.get("key1")).toEqual({ hello: "world" });
    store.set("key2", 42);
    expect(store.get("key2")).toBe(42);
    store.set("key3", "string");
    expect(store.get("key3")).toBe("string");
    store.set("key4", null);
    expect(store.get("key4")).toBeNull();
  } finally {
    globalThis.localStorage = originalLocalStorage;
  }
});

test("LocalStorageStore: del removes key", () => {
  globalThis.localStorage = createLocalStorageMock();
  try {
    const store = new LocalStorageStore("test");
    store.set("k", "v");
    expect(store.get("k")).toBe("v");
    store.del("k");
    expect(store.get("k")).toBeUndefined();
  } finally {
    globalThis.localStorage = originalLocalStorage;
  }
});

test("LocalStorageStore: keys returns all keys with prefix filter", () => {
  globalThis.localStorage = createLocalStorageMock();
  try {
    const store = new LocalStorageStore("test");
    store.set("a:1", 1);
    store.set("a:2", 2);
    store.set("b:1", 3);
    expect(store.keys().sort()).toEqual(["a:1", "a:2", "b:1"]);
    expect(store.keys("a:").sort()).toEqual(["a:1", "a:2"]);
    expect(store.keys("b:")).toEqual(["b:1"]);
    expect(store.keys("c:")).toEqual([]);
  } finally {
    globalThis.localStorage = originalLocalStorage;
  }
});

test("LocalStorageStore: TTL expiry works", async () => {
  globalThis.localStorage = createLocalStorageMock();
  try {
    const store = new LocalStorageStore("test");
    store.set("ttl-key", "value", 100);
    expect(store.get("ttl-key")).toBe("value");
    await Bun.sleep(200);
    expect(store.get("ttl-key")).toBeUndefined();
  } finally {
    globalThis.localStorage = originalLocalStorage;
  }
});

test("LocalStorageStore restores its own TTLs after reload", async () => {
  globalThis.localStorage = createLocalStorageMock();
  try {
    const now = Date.now();
    localStorage.setItem("reload:expired", JSON.stringify({ value: "old", expiresAt: now - 1 }));
    localStorage.setItem("reload:live", JSON.stringify({ value: "fresh", expiresAt: now + 50 }));
    localStorage.setItem("reload-other:foreign", JSON.stringify({ value: "keep", expiresAt: now - 1 }));

    const store = new LocalStorageStore("reload");
    expect(store.get("live")).toBe("fresh");
    expect(localStorage.getItem("reload:expired")).toBeNull();
    expect(localStorage.getItem("reload-other:foreign")).not.toBeNull();

    await Bun.sleep(100);
    expect(localStorage.getItem("reload:live")).toBeNull();
    expect(localStorage.getItem("reload-other:foreign")).not.toBeNull();
  } finally {
    globalThis.localStorage = originalLocalStorage;
  }
});

test("LocalStorageStore: TTL beyond the native timer limit does not expire early", async () => {
  globalThis.localStorage = createLocalStorageMock();
  try {
    const store = new LocalStorageStore("test");
    store.set("ttl-key", "value", 2_147_483_648);
    await Bun.sleep(10);
    expect(store.get("ttl-key")).toBe("value");
    store.clear();
  } finally {
    globalThis.localStorage = originalLocalStorage;
  }
});

test("LocalStorageStore: overwrite resets TTL timer", async () => {
  globalThis.localStorage = createLocalStorageMock();
  try {
    const store = new LocalStorageStore("test");
    store.set("k", "v1", 100);
    await Bun.sleep(60);
    store.set("k", "v2", 200);
    await Bun.sleep(100); // 160ms total — first TTL would have fired
    expect(store.get("k")).toBe("v2"); // still alive
    await Bun.sleep(200);
    expect(store.get("k")).toBeUndefined(); // second TTL fired
  } finally {
    globalThis.localStorage = originalLocalStorage;
  }
});

test("LocalStorageStore: corrupt entries fail closed and remain visible", () => {
  globalThis.localStorage = createLocalStorageMock();
  try {
    const store = new LocalStorageStore("test");
    localStorage.setItem("test:corrupt", "not-json{{{");
    expect(() => store.get("corrupt")).toThrow(/invalid stored value for key "corrupt"/);
    expect(store.keys()).toContain("corrupt");
  } finally {
    globalThis.localStorage = originalLocalStorage;
  }
});

test("LocalStorageStore: malformed envelopes fail closed", () => {
  globalThis.localStorage = createLocalStorageMock();
  try {
    const store = new LocalStorageStore("test");
    localStorage.setItem("test:missing-value", JSON.stringify({ expiresAt: null }));
    localStorage.setItem("test:invalid-expiry", JSON.stringify({ value: 1, expiresAt: "later" }));

    expect(() => store.get("missing-value")).toThrow(/invalid stored value/);
    expect(() => store.get("invalid-expiry")).toThrow(/invalid stored value/);
    expect(store.keys().sort()).toEqual(["invalid-expiry", "missing-value"]);
  } finally {
    globalThis.localStorage = originalLocalStorage;
  }
});

test("LocalStorageStore: custom prefix isolates data", () => {
  globalThis.localStorage = createLocalStorageMock();
  try {
    const store1 = new LocalStorageStore("app1");
    const store2 = new LocalStorageStore("app2");
    store1.set("key", "from-app1");
    store2.set("key", "from-app2");
    expect(store1.get("key")).toBe("from-app1");
    expect(store2.get("key")).toBe("from-app2");
  } finally {
    globalThis.localStorage = originalLocalStorage;
  }
});

test("LocalStorageStore: clear removes only prefixed keys", () => {
  globalThis.localStorage = createLocalStorageMock();
  try {
    const store = new LocalStorageStore("myapp");
    store.set("a", 1);
    store.set("b", 2);
    // Add a key with different prefix directly
    localStorage.setItem("other:key", "value");
    store.clear();
    expect(store.get("a")).toBeUndefined();
    expect(store.get("b")).toBeUndefined();
    expect(localStorage.getItem("other:key")).toBe("value");
  } finally {
    globalThis.localStorage = originalLocalStorage;
  }
});

test("createLocalStorageStore factory works", () => {
  globalThis.localStorage = createLocalStorageMock();
  try {
    const store = createLocalStorageStore("factory-test");
    store.set("k", 42);
    expect(store.get("k")).toBe(42);
  } finally {
    globalThis.localStorage = originalLocalStorage;
  }
});

test("a module coordinates through separate LocalStorageStore handles", async () => {
  globalThis.localStorage = createLocalStorageMock();
  try {
    const first = mutex({
      id: "local-storage",
      prefix: "test:mx",
      retryCount: 0,
      store: createLocalStorageStore("module-integration"),
    });
    const second = mutex({
      id: "local-storage",
      prefix: "test:mx",
      retryCount: 0,
      store: createLocalStorageStore("module-integration"),
    });

    const held = await first.acquire("resource");
    expect(held).not.toBeNull();
    expect(await second.acquire("resource")).toBeNull();

    await first.release(held!);
    const acquired = await second.acquire("resource");
    expect(acquired).not.toBeNull();
    await second.release(acquired!);
  } finally {
    globalThis.localStorage = originalLocalStorage;
  }
});

// ==========================
// LocalStorageStore.keys() index shift
// ==========================

test("LocalStorageStore.keys does not skip a live key for each expired key", () => {
  globalThis.localStorage = createLocalStorageMock();
  try {
    // An already-expired record with no timer behind it: exactly what a key
    // written by a since-closed tab leaves behind, since timers are tab-local.
    localStorage.setItem("shift:a", JSON.stringify({ value: 1, expiresAt: Date.now() - 1 }));

    const store = new LocalStorageStore("shift");
    store.set("b", 2);
    store.set("c", 3);

    // localStorage is index addressed, so deleting the expired entry
    // mid-iteration shifted every later item down and skipped exactly one live
    // key per expired key — silently stalling pump, the only caller of keys().
    expect(store.keys().sort()).toEqual(["b", "c"]);
  } finally {
    globalThis.localStorage = originalLocalStorage;
  }
});

test("MemoryStore hands out snapshots, not live references", () => {
  const memory = new MemoryStore();
  const payload = { retries: 0, tags: ["a"] };

  memory.set("k", payload);
  payload.retries = 99;

  // The stored value must not change under the writer, and a reader mutating
  // what it got back must not change what the next reader sees.
  expect((memory.get("k") as typeof payload).retries).toBe(0);
  (memory.get("k") as typeof payload).tags.push("b");
  expect((memory.get("k") as typeof payload).tags).toEqual(["a"]);
});

test("a rejected localStorage write keeps its own classification", () => {
  const mock = createLocalStorageMock();
  globalThis.localStorage = mock;
  try {
    const store = new LocalStorageStore("quota");
    (mock as unknown as { setItem: () => void }).setItem = (): void => {
      throw new Error("QuotaExceededError");
    };

    // An unwrapped quota error surfaced from topic.pub() or pump's writeState
    // mid-dispatch and was misreported as a user dispatch failure, eventually
    // marking the run permanently failed.
    expect(() => store.set("k", 1)).toThrow(StoreWriteError);
  } finally {
    globalThis.localStorage = originalLocalStorage;
  }
});

test("MemoryStore preserves structured values while returning snapshots", () => {
  const memory = new MemoryStore();
  const value = {
    date: new Date("2026-01-02T03:04:05.000Z"),
    map: new Map([["key", "value"]]),
    bigint: 1n,
  };

  memory.set("value", value);
  const stored = memory.get("value") as typeof value;
  expect(stored).toEqual(value);
  expect(stored).not.toBe(value);
  expect(stored.date).not.toBe(value.date);
  expect(stored.map).not.toBe(value.map);
});

test("LocalStorageStore retains its JSON value semantics", () => {
  const mock = createLocalStorageMock();
  globalThis.localStorage = mock;
  try {
    const local = new LocalStorageStore("json-domain");
    const value = {
      date: new Date("2026-01-02T03:04:05.000Z"),
      map: new Map([["key", "value"]]),
      nan: Number.NaN,
      nested: { missing: undefined },
    };

    local.set("value", value);
    expect(local.get("value")).toEqual({
      date: "2026-01-02T03:04:05.000Z",
      map: {},
      nan: null,
      nested: {},
    });
    expect(() => local.set("bigint", 1n)).toThrow(StoreWriteError);
  } finally {
    globalThis.localStorage = originalLocalStorage;
  }
});

test("unsupported top-level LocalStorage values preserve prior data and TTL", async () => {
  const mock = createLocalStorageMock();
  globalThis.localStorage = mock;
  try {
    const local = new LocalStorageStore("unsupported");
    local.set("key", "old", 50);
    const previous = localStorage.getItem("unsupported:key");
    const setItem = mock.setItem.bind(mock);
    let writes = 0;
    mock.setItem = ((key: string, value: string): void => {
      writes += 1;
      setItem(key, value);
    }) as typeof mock.setItem;

    for (const value of [
      undefined,
      () => undefined,
      Symbol("value"),
      { toJSON: () => undefined },
    ]) {
      expect(() => local.set("key", value, 500)).toThrow(StoreWriteError);
      expect(localStorage.getItem("unsupported:key")).toBe(previous);
    }

    expect(writes).toBe(0);
    expect(local.get("key")).toBe("old");
    await Bun.sleep(80);
    expect(local.get("key")).toBeUndefined();
  } finally {
    globalThis.localStorage = originalLocalStorage;
  }
});

test("failed MemoryStore replacement preserves the previous TTL", async () => {
  const memory = new MemoryStore();
  memory.set("key", "old", 50);

  expect(() => memory.set("key", () => {})).toThrow(StoreWriteError);
  expect(memory.get("key")).toBe("old");
  await Bun.sleep(80);
  expect(memory.get("key")).toBeUndefined();
});

test("failed LocalStorageStore replacement preserves the previous TTL", async () => {
  const mock = createLocalStorageMock();
  globalThis.localStorage = mock;
  try {
    const local = new LocalStorageStore("failed-replacement");
    local.set("key", "old", 50);
    const setItem = mock.setItem.bind(mock);
    let fail = true;
    mock.setItem = ((key: string, value: string): void => {
      if (fail) {
        fail = false;
        throw new Error("quota");
      }
      setItem(key, value);
    }) as typeof mock.setItem;

    expect(() => local.set("key", "new", 500)).toThrow(StoreWriteError);
    expect(local.get("key")).toBe("old");
    await Bun.sleep(80);
    expect(local.get("key")).toBeUndefined();
  } finally {
    globalThis.localStorage = originalLocalStorage;
  }
});
