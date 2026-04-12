import { test, expect } from "bun:test";
import { mutex, LockError } from "../../src/browser/mutex";
import { createMemoryStore } from "../../src/browser/store";

// ==========================
// Basic acquire / release
// ==========================

test("acquires lock successfully", async () => {
  const store = createMemoryStore();
  const m = mutex({ id: "basic", prefix: "test:mx", store });

  expect(m.id).toBe("basic");

  const lock = await m.acquire("resource:1");
  expect(lock).not.toBeNull();
  expect(lock!.resource).toBe("test:mx:basic:resource:1");
  expect(lock!.value).toHaveLength(32); // 16 bytes hex
  expect(lock!.ttl).toBe(10_000); // default TTL
  expect(lock!.expiration).toBeGreaterThan(Date.now() - 100);

  await m.release(lock!);
});

test("release unlocks resource", async () => {
  const store = createMemoryStore();
  const m = mutex({ id: "release", prefix: "test:mx", retryCount: 0, store });

  const lock1 = await m.acquire("resource:2");
  expect(lock1).not.toBeNull();

  await m.release(lock1!);

  // After release, the same resource can be acquired again
  const lock2 = await m.acquire("resource:2");
  expect(lock2).not.toBeNull();
  expect(lock2!.value).not.toBe(lock1!.value); // new lock token

  await m.release(lock2!);
});

test("cannot acquire already-locked resource", async () => {
  const store = createMemoryStore();
  const m = mutex({ id: "exclusive", prefix: "test:mx", retryCount: 0, store });

  const lock1 = await m.acquire("resource:3");
  expect(lock1).not.toBeNull();

  const lock2 = await m.acquire("resource:3");
  expect(lock2).toBeNull();

  await m.release(lock1!);
});

// ==========================
// TTL expiration
// ==========================

test("lock expires after TTL", async () => {
  const store = createMemoryStore();
  const m = mutex({
    id: "expire",
    prefix: "test:mx",
    retryCount: 0,
    defaultTtl: 100,
    store,
  });

  const lock1 = await m.acquire("resource:4");
  expect(lock1).not.toBeNull();

  // Lock is held, so a second acquire fails
  const lock2 = await m.acquire("resource:4");
  expect(lock2).toBeNull();

  // Wait for TTL to expire
  await Bun.sleep(250);

  // Now the lock should have expired and we can reacquire
  const lock3 = await m.acquire("resource:4");
  expect(lock3).not.toBeNull();

  await m.release(lock3!);
});

// ==========================
// withLock
// ==========================

test("withLock executes function and releases", async () => {
  const store = createMemoryStore();
  const m = mutex({ id: "withlock", prefix: "test:mx", retryCount: 0, store });

  const result = await m.withLock("resource:5", async (lock) => {
    expect(lock).not.toBeNull();
    expect(lock.resource).toBe("test:mx:withlock:resource:5");
    return 42;
  });

  expect(result).toBe(42);

  // Lock should be released, so we can acquire again
  const lock = await m.acquire("resource:5");
  expect(lock).not.toBeNull();
  await m.release(lock!);
});

test("withLock returns null when lock unavailable", async () => {
  const store = createMemoryStore();
  const m = mutex({ id: "withlock-null", prefix: "test:mx", retryCount: 0, store });

  // Hold the lock
  const held = await m.acquire("resource:6");
  expect(held).not.toBeNull();

  // withLock should return null when it cannot acquire
  const result = await m.withLock("resource:6", async () => {
    throw new Error("should not execute");
  });

  expect(result).toBeNull();

  await m.release(held!);
});

// ==========================
// withLockOrThrow
// ==========================

test("withLockOrThrow throws LockError when unavailable", async () => {
  const store = createMemoryStore();
  const m = mutex({ id: "throw", prefix: "test:mx", retryCount: 0, store });

  const held = await m.acquire("resource:7");
  expect(held).not.toBeNull();

  let thrown: unknown = null;
  try {
    await m.withLockOrThrow("resource:7", async () => {
      throw new Error("should not execute");
    });
  } catch (error) {
    thrown = error;
  }

  expect(thrown).toBeInstanceOf(LockError);
  expect((thrown as LockError).resource).toBe("resource:7");
  expect((thrown as LockError).message).toContain("resource:7");

  await m.release(held!);
});

// ==========================
// LockError
// ==========================

test("LockError has correct resource property", () => {
  const err = new LockError("my-resource");

  expect(err).toBeInstanceOf(Error);
  expect(err).toBeInstanceOf(LockError);
  expect(err.name).toBe("LockError");
  expect(err.resource).toBe("my-resource");
  expect(err.message).toBe("Failed to acquire lock on resource: my-resource");
});

// ==========================
// extend
// ==========================

test("extend refreshes TTL", async () => {
  const store = createMemoryStore();
  const m = mutex({
    id: "extend",
    prefix: "test:mx",
    retryCount: 0,
    defaultTtl: 100,
    store,
  });

  const lock = await m.acquire("resource:8");
  expect(lock).not.toBeNull();

  const originalExpiration = lock!.expiration;

  // Wait a bit, then extend
  await Bun.sleep(30);
  const extended = await m.extend(lock!, 5_000);
  expect(extended).toBe(true);
  expect(lock!.ttl).toBe(5_000);
  expect(lock!.expiration).toBeGreaterThan(originalExpiration);

  // Lock should still be held
  const lock2 = await m.acquire("resource:8");
  expect(lock2).toBeNull();

  await m.release(lock!);
});

test("extend fails for released lock", async () => {
  const store = createMemoryStore();
  const m = mutex({ id: "extend-fail", prefix: "test:mx", retryCount: 0, store });

  const lock = await m.acquire("resource:9");
  expect(lock).not.toBeNull();

  await m.release(lock!);

  const extended = await m.extend(lock!, 5_000);
  expect(extended).toBe(false);
});

// ==========================
// Long resource names
// ==========================

test("long resource names are hashed", async () => {
  const store = createMemoryStore();
  const m = mutex({ id: "hash", prefix: "test:mx", retryCount: 0, store });

  // Create a resource name longer than 128 characters
  const longName = "a".repeat(200);
  const lock = await m.acquire(longName);
  expect(lock).not.toBeNull();

  // The resource key should contain a hash, not the full name
  expect(lock!.resource.length).toBeLessThan(longName.length);
  expect(lock!.resource).toContain("hash:");

  await m.release(lock!);

  // Acquiring the same long name again should work on the same hashed key
  const lock2 = await m.acquire(longName);
  expect(lock2).not.toBeNull();
  expect(lock2!.resource).toBe(lock!.resource);

  await m.release(lock2!);
});

// ==========================
// Retry behavior
// ==========================

test("retry succeeds when lock becomes available", async () => {
  const store = createMemoryStore();
  const m = mutex({
    id: "retry",
    prefix: "test:mx",
    retryCount: 5,
    retryDelay: 50,
    defaultTtl: 80,
    store,
  });

  // Acquire initial lock with short TTL
  const lock1 = await m.acquire("resource:10");
  expect(lock1).not.toBeNull();

  // Start acquiring — should eventually succeed after lock1 expires
  const lock2Promise = m.acquire("resource:10");

  const lock2 = await lock2Promise;
  expect(lock2).not.toBeNull();

  await m.release(lock2!);
});

// ==========================
// withLock releases on error
// ==========================

test("withLock releases lock even on error", async () => {
  const store = createMemoryStore();
  const m = mutex({ id: "error-release", prefix: "test:mx", retryCount: 0, store });

  let thrown: unknown = null;
  try {
    await m.withLock("resource:11", async () => {
      throw new Error("intentional error");
    });
  } catch (error) {
    thrown = error;
  }

  expect(thrown).toBeInstanceOf(Error);
  expect((thrown as Error).message).toBe("intentional error");

  // Lock should have been released even though the function threw
  const lock = await m.acquire("resource:11");
  expect(lock).not.toBeNull();
  await m.release(lock!);
});

// ==========================
// Shared store exclusivity
// ==========================

test("two mutex instances with shared store coordinate", async () => {
  const { createMemoryStore } = await import("../../src/browser/store");
  const sharedStore = createMemoryStore();
  const m1 = mutex({ id: "shared", prefix: "test:mx", store: sharedStore, retryCount: 0 });
  const m2 = mutex({ id: "shared", prefix: "test:mx", store: sharedStore, retryCount: 0 });

  const lock1 = await m1.acquire("res");
  expect(lock1).not.toBeNull();

  // m2 should NOT be able to acquire while m1 holds the lock
  const lock2 = await m2.acquire("res");
  expect(lock2).toBeNull();

  await m1.release(lock1!);

  // Now m2 can acquire
  const lock3 = await m2.acquire("res");
  expect(lock3).not.toBeNull();
  await m2.release(lock3!);
});
