import { test, expect, describe } from "bun:test";
import { z } from "zod";
import {
  registry,
  RegistryCapacityError,
  RegistryPayloadTooLargeError,
} from "../../src/browser/registry";

const schema = z.object({ name: z.string() });

const makeRegistry = (opts?: {
  maxEntries?: number;
  maxPayloadBytes?: number;
}) => {
  return registry({
    id: `test-${Date.now()}-${Math.random()}`,
    schema,
    limits: {
      maxEntries: opts?.maxEntries,
      maxPayloadBytes: opts?.maxPayloadBytes,
      eventRetentionMs: 60_000,
      tombstoneRetentionMs: 60_000,
    },
  });
};

// ==========================
// upsert
// ==========================

describe("upsert", () => {
  test("creates entry with correct fields", async () => {
    const reg = makeRegistry();
    const entry = await reg.upsert({
      key: "svc/a",
      value: { name: "alpha" },
      ttlMs: 5000,
    });

    expect(entry.key).toBe("svc/a");
    expect(entry.value).toEqual({ name: "alpha" });
    expect(typeof entry.version).toBe("string");
    expect(entry.status).toBe("active");
    expect(typeof entry.createdAt).toBe("number");
    expect(entry.createdAt).toBeGreaterThan(0);
    expect(typeof entry.updatedAt).toBe("number");
    expect(entry.updatedAt).toBeGreaterThan(0);
    expect(entry.ttlMs).toBe(5000);
    expect(entry.expiresAt).not.toBeNull();
    expect(entry.expiresAt!).toBeGreaterThanOrEqual(entry.updatedAt);
  });

  test("updates existing entry and changes version", async () => {
    const reg = makeRegistry();
    const first = await reg.upsert({
      key: "svc/a",
      value: { name: "v1" },
    });
    const second = await reg.upsert({
      key: "svc/a",
      value: { name: "v2" },
    });

    expect(second.key).toBe("svc/a");
    expect(second.value).toEqual({ name: "v2" });
    expect(second.status).toBe("active");
    expect(second.version).not.toBe(first.version);
    expect(second.createdAt).toBe(first.createdAt);
    expect(second.updatedAt).toBeGreaterThanOrEqual(first.updatedAt);
    expect(second.ttlMs).toBeNull();
    expect(second.expiresAt).toBeNull();
  });
});

// ==========================
// get
// ==========================

describe("get", () => {
  test("returns entry", async () => {
    const reg = makeRegistry();
    await reg.upsert({ key: "svc/a", value: { name: "alpha" } });

    const entry = await reg.get({ key: "svc/a" });
    expect(entry).not.toBeNull();
    expect(entry!.key).toBe("svc/a");
    expect(entry!.value).toEqual({ name: "alpha" });
  });

  test("returns null for non-existent key", async () => {
    const reg = makeRegistry();
    const entry = await reg.get({ key: "svc/missing" });
    expect(entry).toBeNull();
  });
});

// ==========================
// remove
// ==========================

describe("remove", () => {
  test("deletes entry", async () => {
    const reg = makeRegistry();
    await reg.upsert({ key: "svc/a", value: { name: "alpha" } });

    const removed = await reg.remove({ key: "svc/a" });
    expect(removed).toBe(true);

    const entry = await reg.get({ key: "svc/a" });
    expect(entry).toBeNull();
  });

  test("returns false for non-existent key", async () => {
    const reg = makeRegistry();
    const removed = await reg.remove({ key: "svc/missing" });
    expect(removed).toBe(false);
  });

  test("with reason", async () => {
    const reg = makeRegistry();
    await reg.upsert({ key: "svc/a", value: { name: "alpha" } });

    // Set up a reader before removing so we can verify the reason in the event
    const reader = reg.reader({ key: "svc/a" });

    const removed = await reg.remove({ key: "svc/a", reason: "shutdown" });
    expect(removed).toBe(true);

    const event = await reader.recv({ wait: false });
    expect(event).not.toBeNull();
    expect(event!.type).toBe("delete");
    if (event!.type === "delete") {
      expect(event!.reason).toBe("shutdown");
    }
  });
});

// ==========================
// touch
// ==========================

describe("touch", () => {
  test("extends TTL", async () => {
    const reg = makeRegistry();
    const entry = await reg.upsert({
      key: "svc/a",
      value: { name: "alpha" },
      ttlMs: 100,
    });

    expect(entry.expiresAt).not.toBeNull();

    const result = await reg.touch({ key: "svc/a", ttlMs: 5000 });
    expect(result.ok).toBe(true);
    expect(result.version).toBeDefined();
    // touch does NOT bump version (matches server behavior)
    expect(result.version).toBe(entry.version);
    expect(result.expiresAt).toBeDefined();
    expect(result.expiresAt!).toBeGreaterThan(entry.expiresAt!);
  });

  test("returns ok false for non-existent key", async () => {
    const reg = makeRegistry();
    const result = await reg.touch({ key: "svc/missing", ttlMs: 5000 });
    expect(result.ok).toBe(false);
    expect(result.version).toBeUndefined();
    expect(result.expiresAt).toBeUndefined();
  });
});

// ==========================
// list
// ==========================

describe("list", () => {
  test("returns all entries sorted by key", async () => {
    const reg = makeRegistry();
    await reg.upsert({ key: "svc/c", value: { name: "charlie" } });
    await reg.upsert({ key: "svc/a", value: { name: "alpha" } });
    await reg.upsert({ key: "svc/b", value: { name: "bravo" } });

    const result = await reg.list();
    expect(result.entries).toHaveLength(3);
    expect(result.entries[0]!.key).toBe("svc/a");
    expect(result.entries[1]!.key).toBe("svc/b");
    expect(result.entries[2]!.key).toBe("svc/c");
    expect(typeof result.cursor).toBe("string");
  });

  test("with prefix filter", async () => {
    const reg = makeRegistry();
    await reg.upsert({ key: "svc/a", value: { name: "alpha" } });
    await reg.upsert({ key: "svc/b", value: { name: "bravo" } });
    await reg.upsert({ key: "db/x", value: { name: "x-ray" } });

    const result = await reg.list({ prefix: "svc/" });
    expect(result.entries).toHaveLength(2);
    expect(result.entries.every((e) => e.key.startsWith("svc/"))).toBe(true);
  });

  test("with afterKey pagination", async () => {
    const reg = makeRegistry();
    await reg.upsert({ key: "svc/a", value: { name: "alpha" } });
    await reg.upsert({ key: "svc/b", value: { name: "bravo" } });
    await reg.upsert({ key: "svc/c", value: { name: "charlie" } });
    await reg.upsert({ key: "svc/d", value: { name: "delta" } });

    const result = await reg.list({ afterKey: "svc/b" });
    expect(result.entries).toHaveLength(2);
    expect(result.entries[0]!.key).toBe("svc/c");
    expect(result.entries[1]!.key).toBe("svc/d");
  });

  test("with limit", async () => {
    const reg = makeRegistry();
    await reg.upsert({ key: "svc/a", value: { name: "alpha" } });
    await reg.upsert({ key: "svc/b", value: { name: "bravo" } });
    await reg.upsert({ key: "svc/c", value: { name: "charlie" } });

    const result = await reg.list({ limit: 2 });
    expect(result.entries).toHaveLength(2);
    expect(result.entries[0]!.key).toBe("svc/a");
    expect(result.entries[1]!.key).toBe("svc/b");
  });
});

// ==========================
// cas (compare-and-swap)
// ==========================

describe("cas", () => {
  test("succeeds when version matches", async () => {
    const reg = makeRegistry();
    const entry = await reg.upsert({
      key: "svc/a",
      value: { name: "alpha" },
    });

    const result = await reg.cas({
      key: "svc/a",
      version: entry.version,
      value: { name: "updated" },
    });

    expect(result.ok).toBe(true);
    expect(result.entry).toBeDefined();
    expect(result.entry!.value).toEqual({ name: "updated" });
    expect(result.entry!.version).not.toBe(entry.version);
  });

  test("fails when version mismatches", async () => {
    const reg = makeRegistry();
    await reg.upsert({ key: "svc/a", value: { name: "alpha" } });

    const result = await reg.cas({
      key: "svc/a",
      version: "wrong-version",
      value: { name: "updated" },
    });

    expect(result.ok).toBe(false);
    expect(result.entry).toBeUndefined();

    // Original value should be unchanged
    const current = await reg.get({ key: "svc/a" });
    expect(current!.value).toEqual({ name: "alpha" });
  });
});

// ==========================
// TTL expiry
// ==========================

describe("TTL expiry", () => {
  test("entry expires after short TTL", async () => {
    const reg = makeRegistry();
    await reg.upsert({
      key: "svc/a",
      value: { name: "alpha" },
      ttlMs: 100,
    });

    // Entry should exist immediately
    const before = await reg.get({ key: "svc/a" });
    expect(before).not.toBeNull();

    // Wait for TTL to expire
    await Bun.sleep(250);

    const after = await reg.get({ key: "svc/a" });
    expect(after).toBeNull();
  });
});

// ==========================
// Capacity limit
// ==========================

describe("capacity limit", () => {
  test("throws RegistryCapacityError when maxEntries exceeded", async () => {
    const reg = makeRegistry({ maxEntries: 2 });
    await reg.upsert({ key: "svc/a", value: { name: "alpha" } });
    await reg.upsert({ key: "svc/b", value: { name: "bravo" } });

    expect(
      reg.upsert({ key: "svc/c", value: { name: "charlie" } })
    ).rejects.toThrow(RegistryCapacityError);
  });
});

// ==========================
// Payload size limit
// ==========================

describe("payload size limit", () => {
  test("throws RegistryPayloadTooLargeError when payload exceeds maxPayloadBytes", async () => {
    const reg = makeRegistry({ maxPayloadBytes: 32 });

    const longName = "x".repeat(200);
    expect(
      reg.upsert({ key: "svc/a", value: { name: longName } })
    ).rejects.toThrow(RegistryPayloadTooLargeError);
  });
});

// ==========================
// reader - root log
// ==========================

describe("reader (root log)", () => {
  test("receives events on root log", async () => {
    const reg = makeRegistry();

    // Create reader before events happen
    const reader = reg.reader();

    await reg.upsert({ key: "svc/a", value: { name: "alpha" } });
    await reg.upsert({ key: "svc/b", value: { name: "bravo" } });

    const event1 = await reader.recv({ wait: false });
    expect(event1).not.toBeNull();
    expect(event1!.type).toBe("upsert");
    if (event1!.type === "upsert") {
      expect(event1!.entry.key).toBe("svc/a");
      expect(event1!.entry.value).toEqual({ name: "alpha" });
    }

    const event2 = await reader.recv({ wait: false });
    expect(event2).not.toBeNull();
    expect(event2!.type).toBe("upsert");
    if (event2!.type === "upsert") {
      expect(event2!.entry.key).toBe("svc/b");
    }
  });
});

// ==========================
// reader - key filter
// ==========================

describe("reader (key filter)", () => {
  test("receives events filtered by key", async () => {
    const reg = makeRegistry();

    const reader = reg.reader({ key: "svc/a" });

    await reg.upsert({ key: "svc/a", value: { name: "alpha" } });
    await reg.upsert({ key: "svc/b", value: { name: "bravo" } });
    await reg.upsert({ key: "svc/a", value: { name: "alpha-v2" } });

    const event1 = await reader.recv({ wait: false });
    expect(event1).not.toBeNull();
    expect(event1!.type).toBe("upsert");
    if (event1!.type === "upsert") {
      expect(event1!.entry.key).toBe("svc/a");
      expect(event1!.entry.value).toEqual({ name: "alpha" });
    }

    const event2 = await reader.recv({ wait: false });
    expect(event2).not.toBeNull();
    expect(event2!.type).toBe("upsert");
    if (event2!.type === "upsert") {
      expect(event2!.entry.key).toBe("svc/a");
      expect(event2!.entry.value).toEqual({ name: "alpha-v2" });
    }

    // No more events for this key
    const event3 = await reader.recv({ wait: false });
    expect(event3).toBeNull();
  });
});

// ==========================
// reader - prefix filter
// ==========================

describe("reader (prefix filter)", () => {
  test("receives events filtered by prefix", async () => {
    const reg = makeRegistry();

    // Prefix "svc/" matches keys like "svc/x/..." where "svc/" is an ancestor prefix
    const reader = reg.reader({ prefix: "svc/" });

    await reg.upsert({ key: "svc/a", value: { name: "alpha" } });
    await reg.upsert({ key: "db/x", value: { name: "x-ray" } });
    await reg.upsert({ key: "svc/b", value: { name: "bravo" } });

    const event1 = await reader.recv({ wait: false });
    expect(event1).not.toBeNull();
    expect(event1!.type).toBe("upsert");
    if (event1!.type === "upsert") {
      expect(event1!.entry.key).toBe("svc/a");
    }

    const event2 = await reader.recv({ wait: false });
    expect(event2).not.toBeNull();
    expect(event2!.type).toBe("upsert");
    if (event2!.type === "upsert") {
      expect(event2!.entry.key).toBe("svc/b");
    }

    // "db/x" should not appear on this reader
    const event3 = await reader.recv({ wait: false });
    expect(event3).toBeNull();
  });
});

// ==========================
// Additional coverage
// ==========================

test("touch without ttlMs reuses existing TTL", async () => {
  const reg = registry({
    id: `touch-reuse-${Date.now()}`,
    schema: z.object({ name: z.string() }),
  });
  const entry = await reg.upsert({ key: "svc/a", value: { name: "a" }, ttlMs: 5000 });
  expect(entry.expiresAt).not.toBeNull();

  await Bun.sleep(50);
  const result = await reg.touch({ key: "svc/a" });
  expect(result.ok).toBe(true);
  expect(result.expiresAt).toBeDefined();
  expect(result.expiresAt!).toBeGreaterThan(entry.expiresAt!);
  // Version should NOT change on touch
  expect(result.version).toBe(entry.version);
});

test("touch without ttlMs on non-TTL entry returns ok false", async () => {
  const reg = registry({
    id: `touch-no-ttl-${Date.now()}`,
    schema: z.object({ name: z.string() }),
  });
  await reg.upsert({ key: "svc/a", value: { name: "a" } }); // no ttlMs
  const result = await reg.touch({ key: "svc/a" });
  expect(result.ok).toBe(false);
});

test("cas preserves existing TTL", async () => {
  const reg = registry({
    id: `cas-ttl-${Date.now()}`,
    schema: z.object({ name: z.string() }),
  });
  const entry = await reg.upsert({ key: "svc/a", value: { name: "a" }, ttlMs: 5000 });
  const result = await reg.cas({ key: "svc/a", version: entry.version, value: { name: "b" } });
  expect(result.ok).toBe(true);
  expect(result.entry).toBeDefined();
  expect(result.entry!.value.name).toBe("b");
  expect(result.entry!.expiresAt).not.toBeNull();
  // TTL should be preserved (roughly same remaining time)
  expect(result.entry!.expiresAt!).toBeGreaterThan(Date.now());
});

test("key validation rejects invalid keys", async () => {
  const reg = registry({
    id: `key-validation-${Date.now()}`,
    schema: z.object({ name: z.string() }),
  });
  await expect(reg.upsert({ key: "/leading", value: { name: "a" } })).rejects.toThrow("must not start");
  await expect(reg.upsert({ key: "trailing/", value: { name: "a" } })).rejects.toThrow("must not end");
  await expect(reg.upsert({ key: "double//slash", value: { name: "a" } })).rejects.toThrow("must not contain");
});

test("reader receives events filtered by prefix", async () => {
  const reg = registry({
    id: `reader-prefix-${Date.now()}`,
    schema: z.object({ name: z.string() }),
  });

  // Set up reader BEFORE mutations
  const reader = reg.reader({ prefix: "apps/web/" });

  await reg.upsert({ key: "apps/web/a", value: { name: "a" } });
  await reg.upsert({ key: "apps/api/b", value: { name: "b" } }); // different prefix

  const ev1 = await reader.recv({ wait: false });
  expect(ev1).not.toBeNull();
  expect(ev1!.type).toBe("upsert");
  if (ev1!.type === "upsert") {
    expect(ev1!.entry.key).toBe("apps/web/a");
  }

  // The apps/api/b event should NOT appear on this reader
  const ev2 = await reader.recv({ wait: false });
  expect(ev2).toBeNull();
});

// ==========================
// list with status expired
// ==========================

test("list with status expired returns tombstones", async () => {
  const reg = registry({
    id: `list-expired-${Date.now()}`,
    schema: z.object({ name: z.string() }),
    limits: { tombstoneRetentionMs: 60_000 },
  });
  const entry = await reg.upsert({ key: "svc/a", value: { name: "a" }, ttlMs: 50 });
  await Bun.sleep(100);
  const expired = await reg.list({ status: "expired" });
  expect(expired.entries.length).toBe(1);
  expect(expired.entries[0]!.key).toBe("svc/a");
  expect(expired.entries[0]!.status).toBe("expired");
});

// ==========================
// get with includeExpired
// ==========================

test("get with includeExpired returns expired entry", async () => {
  const reg = registry({
    id: `get-expired-${Date.now()}`,
    schema: z.object({ name: z.string() }),
    limits: { tombstoneRetentionMs: 60_000 },
  });
  await reg.upsert({ key: "svc/a", value: { name: "a" }, ttlMs: 50 });
  await Bun.sleep(100);
  const gone = await reg.get({ key: "svc/a" });
  expect(gone).toBeNull();
  const expired = await reg.get({ key: "svc/a", includeExpired: true });
  expect(expired).not.toBeNull();
  expect(expired!.status).toBe("expired");
});

// ==========================
// list nextKey pagination
// ==========================

test("list returns nextKey when more entries exist", async () => {
  const reg = registry({
    id: `nextkey-${Date.now()}`,
    schema: z.object({ name: z.string() }),
  });
  await reg.upsert({ key: "a", value: { name: "a" } });
  await reg.upsert({ key: "b", value: { name: "b" } });
  await reg.upsert({ key: "c", value: { name: "c" } });
  const page1 = await reg.list({ limit: 2 });
  expect(page1.entries.length).toBe(2);
  expect(page1.nextKey).toBe("c");
  const page2 = await reg.list({ afterKey: page1.entries[1]!.key, limit: 2 });
  expect(page2.entries.length).toBe(1);
  expect(page2.nextKey).toBeUndefined();
});
