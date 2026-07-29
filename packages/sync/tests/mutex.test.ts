import { test, expect, beforeEach } from "bun:test";
import { redis } from "bun";
import { mutex, LockError } from "../index";

beforeEach(async () => {
  const keys = await redis.send("KEYS", ["test:mx:*"]);
  if (Array.isArray(keys) && keys.length > 0) {
    await redis.send("DEL", keys as string[]);
  }
});

test("acquires lock successfully", async () => {
  const m = mutex({ id: "basic", prefix: "test:mx" });

  expect(m.id).toBe("basic");

  const lock = await m.acquire("resource:1");
  expect(lock).not.toBeNull();
  expect(lock!.resource).toBe("test:mx:basic:resource:1");
  expect(lock!.value).toHaveLength(32);

  await m.release(lock!);
});

test("only one lock can be held at a time", async () => {
  const m = mutex({ id: "exclusive", prefix: "test:mx", retryCount: 0 });

  const lock1 = await m.acquire("resource:2");
  expect(lock1).not.toBeNull();

  const lock2 = await m.acquire("resource:2");
  expect(lock2).toBeNull();

  await m.release(lock1!);
});

test("lock is released and can be acquired again", async () => {
  const m = mutex({ id: "reacquire", prefix: "test:mx", retryCount: 0 });

  const lock1 = await m.acquire("resource:3");
  expect(lock1).not.toBeNull();

  await m.release(lock1!);

  const lock2 = await m.acquire("resource:3");
  expect(lock2).not.toBeNull();

  await m.release(lock2!);
});

test("lock expires after TTL", async () => {
  const m = mutex({
    id: "expire",
    prefix: "test:mx",
    retryCount: 0,
    defaultTtl: 100,
  });

  const lock1 = await m.acquire("resource:4");
  expect(lock1).not.toBeNull();

  await Bun.sleep(150);

  const lock2 = await m.acquire("resource:4");
  expect(lock2).not.toBeNull();

  await m.release(lock2!);
});

test("withLock executes function and releases lock", async () => {
  const m = mutex({ id: "withlock", prefix: "test:mx" });

  let executed = false;
  const result = await m.withLock("resource:5", async () => {
    executed = true;
    return 42;
  });

  expect(executed).toBe(true);
  expect(result).toBe(42);

  const lock = await m.acquire("resource:5");
  expect(lock).not.toBeNull();
  await m.release(lock!);
});

test("withLock releases lock on error", async () => {
  const m = mutex({ id: "withlock-err", prefix: "test:mx", retryCount: 0 });

  let thrown: unknown = null;
  try {
    await m.withLock("resource:6", async () => {
      throw new Error("test error");
    });
  } catch (e) {
    thrown = e;
  }

  expect((thrown as Error).message).toBe("test error");

  const lock = await m.acquire("resource:6");
  expect(lock).not.toBeNull();
  await m.release(lock!);
});

test("withLock returns null when lock cannot be acquired", async () => {
  const m = mutex({ id: "withlock-null", prefix: "test:mx", retryCount: 0 });

  const lock1 = await m.acquire("resource:7");

  const result = await m.withLock("resource:7", async () => {
    return 42;
  });

  expect(result).toBeNull();

  await m.release(lock1!);
});

test("withLockOrThrow throws LockError when lock cannot be acquired", async () => {
  const m = mutex({ id: "throw", prefix: "test:mx", retryCount: 0 });

  const lock1 = await m.acquire("resource:8");

  let thrown: unknown = null;
  try {
    await m.withLockOrThrow("resource:8", async () => {
      return 42;
    });
  } catch (e) {
    thrown = e;
  }

  expect(thrown).toBeInstanceOf(LockError);
  expect((thrown as LockError).resource).toBe("resource:8");

  await m.release(lock1!);
});

test("extend prolongs lock TTL", async () => {
  const m = mutex({
    id: "extend",
    prefix: "test:mx",
    retryCount: 0,
    defaultTtl: 100,
  });

  const lock = await m.acquire("resource:9");
  expect(lock).not.toBeNull();

  const extended = await m.extend(lock!, 1000);
  expect(extended).toBe(true);
  expect(lock!.ttl).toBe(1000);

  await Bun.sleep(150);

  const lock2 = await m.acquire("resource:9");
  expect(lock2).toBeNull();

  await m.release(lock!);
});

test("extend fails if lock was lost", async () => {
  const m = mutex({
    id: "extend-lost",
    prefix: "test:mx",
    retryCount: 0,
    defaultTtl: 100,
  });

  const lock = await m.acquire("resource:10");
  expect(lock).not.toBeNull();

  await Bun.sleep(150);

  const extended = await m.extend(lock!, 1000);
  expect(extended).toBe(false);
});

test("different resources can be locked independently", async () => {
  const m = mutex({ id: "independent", prefix: "test:mx", retryCount: 0 });

  const lockA = await m.acquire("resource:a");
  const lockB = await m.acquire("resource:b");

  expect(lockA).not.toBeNull();
  expect(lockB).not.toBeNull();

  await m.release(lockA!);
  await m.release(lockB!);
});

test("retries with delay when lock is held", async () => {
  const m = mutex({
    id: "retry",
    prefix: "test:mx",
    retryCount: 5,
    retryDelay: 50,
    defaultTtl: 100,
  });

  const lock1 = await m.acquire("resource:11");
  expect(lock1).not.toBeNull();

  const start = Date.now();
  const lock2 = await m.acquire("resource:11");
  const elapsed = Date.now() - start;

  expect(lock2).not.toBeNull();
  expect(elapsed).toBeGreaterThanOrEqual(50);

  await m.release(lock2!);
});

test("long resources are hashed", async () => {
  const m = mutex({ id: "long", prefix: "test:mx", retryCount: 0 });

  const longResource = "resource:" + "b".repeat(200);
  const lock = await m.acquire(longResource);

  expect(lock).not.toBeNull();
  expect(lock!.resource).toStartWith("test:mx:long:hash:");
  expect(lock!.resource.length).toBe("test:mx:long:hash:".length + 64);

  await m.release(lock!);
});

// ==========================
// Additional consistency tests
// ==========================

test("release by non-owner does not release the lock", async () => {
  const m = mutex({ id: "non-owner", prefix: "test:mx", retryCount: 0 });

  const real = await m.acquire("resource:owned");
  expect(real).not.toBeNull();

  const fake: typeof real = {
    resource: real!.resource,
    value: "not-the-real-value",
    ttl: real!.ttl,
    expiration: real!.expiration,
  };

  await m.release(fake);

  // Real lock should still be held
  const attempt = await m.acquire("resource:owned");
  expect(attempt).toBeNull();

  await m.release(real!);
});

test("concurrent acquire on same resource — exactly one wins", async () => {
  const m = mutex({ id: "race", prefix: "test:mx", retryCount: 0 });

  const results = await Promise.all(
    Array.from({ length: 10 }, () => m.acquire("resource:contested")),
  );

  const acquired = results.filter((r) => r !== null);
  expect(acquired.length).toBe(1);

  await m.release(acquired[0]!);
});

test("different mutex ids are isolated", async () => {
  const m1 = mutex({ id: "scope-a", prefix: "test:mx", retryCount: 0 });
  const m2 = mutex({ id: "scope-b", prefix: "test:mx", retryCount: 0 });

  const lock1 = await m1.acquire("shared-name");
  expect(lock1).not.toBeNull();

  const lock2 = await m2.acquire("shared-name");
  expect(lock2).not.toBeNull();

  await m1.release(lock1!);
  await m2.release(lock2!);
});

test("withLock provides mutual exclusion for concurrent operations", async () => {
  const m = mutex({ id: "mutual", prefix: "test:mx", retryCount: 3, retryDelay: 20, defaultTtl: 2000 });

  let counter = 0;
  const iterations = 5;

  const increment = async (): Promise<void> => {
    await m.withLock("counter", async () => {
      const current = counter;
      await Bun.sleep(10);
      counter = current + 1;
    });
  };

  await Promise.all(Array.from({ length: iterations }, () => increment()));

  expect(counter).toBe(iterations);
});

test("rejects invalid lock TTLs before changing Redis state", async () => {
  const invalidTtls = [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, Number.MAX_SAFE_INTEGER + 1];
  for (const ttl of invalidTtls) {
    expect(() => mutex({ id: "invalid-default", prefix: "test:mx", defaultTtl: ttl })).toThrow(
      /positive safe integer/,
    );
  }

  const m = mutex({ id: "invalid-operation", prefix: "test:mx", retryCount: 0 });
  for (const ttl of invalidTtls) {
    await expect(m.acquire("resource", ttl)).rejects.toThrow(/positive safe integer/);
  }

  const lock = await m.acquire("resource");
  expect(lock).not.toBeNull();
  for (const ttl of invalidTtls) {
    await expect(m.extend(lock!, ttl)).rejects.toThrow(/positive safe integer/);
  }
  expect(await m.acquire("resource")).toBeNull();
  await m.release(lock!);
});

test("rejects invalid retry configuration before acquiring", () => {
  const invalidCounts = [-1, 0.5, Number.NaN, Number.POSITIVE_INFINITY, Number.MAX_SAFE_INTEGER + 1];
  for (const retryCount of invalidCounts) {
    expect(() => mutex({ id: "invalid-retry-count", retryCount })).toThrow(/retryCount/);
  }

  const invalidDelays = [-1, 0.5, Number.NaN, Number.POSITIVE_INFINITY, Number.MAX_SAFE_INTEGER + 1];
  for (const retryDelay of invalidDelays) {
    expect(() => mutex({ id: "invalid-retry-delay", retryDelay })).toThrow(/retryDelay/);
  }
});
