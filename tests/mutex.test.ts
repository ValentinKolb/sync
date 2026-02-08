import { test, expect, beforeEach } from "bun:test";
import { redis } from "bun";
import { mutex, LockError } from "../index";

// Clean up Redis before each test
beforeEach(async () => {
  const keys = await redis.send("KEYS", ["mutex:test:*"]);
  if (Array.isArray(keys) && keys.length > 0) {
    await redis.send("DEL", keys as string[]);
  }
});

test("acquires lock successfully", async () => {
  const m = mutex.create({ prefix: "mutex:test" });

  const lock = await m.acquire("resource:1");
  expect(lock).not.toBeNull();
  expect(lock!.resource).toBe("mutex:test:resource:1");
  expect(lock!.value).toHaveLength(32); // 16 bytes hex = 32 chars

  await m.release(lock!);
});

test("only one lock can be held at a time", async () => {
  const m = mutex.create({
    prefix: "mutex:test",
    retryCount: 0, // Don't retry
  });

  const lock1 = await m.acquire("resource:2");
  expect(lock1).not.toBeNull();

  const lock2 = await m.acquire("resource:2");
  expect(lock2).toBeNull();

  await m.release(lock1!);
});

test("lock is released and can be acquired again", async () => {
  const m = mutex.create({
    prefix: "mutex:test",
    retryCount: 0,
  });

  const lock1 = await m.acquire("resource:3");
  expect(lock1).not.toBeNull();

  await m.release(lock1!);

  const lock2 = await m.acquire("resource:3");
  expect(lock2).not.toBeNull();

  await m.release(lock2!);
});

test("lock expires after TTL", async () => {
  const m = mutex.create({
    prefix: "mutex:test",
    retryCount: 0,
    defaultTtl: 100, // 100ms TTL
  });

  const lock1 = await m.acquire("resource:4");
  expect(lock1).not.toBeNull();

  // Wait for lock to expire
  await Bun.sleep(150);

  // Should be able to acquire now
  const lock2 = await m.acquire("resource:4");
  expect(lock2).not.toBeNull();

  await m.release(lock2!);
});

test("withLock executes function and releases lock", async () => {
  const m = mutex.create({ prefix: "mutex:test" });

  let executed = false;
  const result = await m.withLock("resource:5", async () => {
    executed = true;
    return 42;
  });

  expect(executed).toBe(true);
  expect(result).toBe(42);

  // Lock should be released, can acquire again
  const lock = await m.acquire("resource:5");
  expect(lock).not.toBeNull();
  await m.release(lock!);
});

test("withLock releases lock on error", async () => {
  const m = mutex.create({ prefix: "mutex:test", retryCount: 0 });

  try {
    await m.withLock("resource:6", async () => {
      throw new Error("test error");
    });
  } catch (e) {
    expect((e as Error).message).toBe("test error");
  }

  // Lock should be released
  const lock = await m.acquire("resource:6");
  expect(lock).not.toBeNull();
  await m.release(lock!);
});

test("withLock returns null when lock cannot be acquired", async () => {
  const m = mutex.create({ prefix: "mutex:test", retryCount: 0 });

  // Hold the lock
  const lock1 = await m.acquire("resource:7");

  // Try withLock - should return null
  const result = await m.withLock("resource:7", async () => {
    return 42;
  });

  expect(result).toBeNull();

  await m.release(lock1!);
});

test("withLockOrThrow throws LockError when lock cannot be acquired", async () => {
  const m = mutex.create({ prefix: "mutex:test", retryCount: 0 });

  // Hold the lock
  const lock1 = await m.acquire("resource:8");

  try {
    await m.withLockOrThrow("resource:8", async () => {
      return 42;
    });
    expect(true).toBe(false); // Should not reach
  } catch (e) {
    expect(e).toBeInstanceOf(LockError);
    expect((e as LockError).resource).toBe("resource:8");
  }

  await m.release(lock1!);
});

test("extend prolongs lock TTL", async () => {
  const m = mutex.create({
    prefix: "mutex:test",
    retryCount: 0,
    defaultTtl: 100,
  });

  const lock = await m.acquire("resource:9");
  expect(lock).not.toBeNull();

  // Extend the lock
  const extended = await m.extend(lock!, 1000);
  expect(extended).toBe(true);
  expect(lock!.ttl).toBe(1000);

  // Wait past original TTL
  await Bun.sleep(150);

  // Lock should still be held (can't acquire)
  const lock2 = await m.acquire("resource:9");
  expect(lock2).toBeNull();

  await m.release(lock!);
});

test("extend fails if lock was lost", async () => {
  const m = mutex.create({
    prefix: "mutex:test",
    retryCount: 0,
    defaultTtl: 100,
  });

  const lock = await m.acquire("resource:10");
  expect(lock).not.toBeNull();

  // Wait for lock to expire
  await Bun.sleep(150);

  // Try to extend expired lock
  const extended = await m.extend(lock!, 1000);
  expect(extended).toBe(false);
});

test("different resources can be locked independently", async () => {
  const m = mutex.create({ prefix: "mutex:test", retryCount: 0 });

  const lockA = await m.acquire("resource:a");
  const lockB = await m.acquire("resource:b");

  expect(lockA).not.toBeNull();
  expect(lockB).not.toBeNull();

  await m.release(lockA!);
  await m.release(lockB!);
});

test("retries with delay when lock is held", async () => {
  const m = mutex.create({
    prefix: "mutex:test",
    retryCount: 5,
    retryDelay: 50,
    defaultTtl: 100,
  });

  // Acquire lock that will expire
  const lock1 = await m.acquire("resource:11");
  expect(lock1).not.toBeNull();

  // Try to acquire - should retry and eventually succeed
  const start = Date.now();
  const lock2 = await m.acquire("resource:11");
  const elapsed = Date.now() - start;

  expect(lock2).not.toBeNull();
  expect(elapsed).toBeGreaterThanOrEqual(50); // At least one retry

  await m.release(lock2!);
});

test("long resources are hashed", async () => {
  const m = mutex.create({ prefix: "mutex:test", retryCount: 0 });

  const longResource = "resource:" + "b".repeat(200);
  const lock = await m.acquire(longResource);

  expect(lock).not.toBeNull();
  expect(lock!.resource).toStartWith("mutex:test:hash:");
  expect(lock!.resource.length).toBe("mutex:test:hash:".length + 64);

  await m.release(lock!);
});
