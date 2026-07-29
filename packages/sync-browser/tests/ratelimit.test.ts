import { test, expect } from "bun:test";
import { ratelimit, RateLimitError } from "../src/ratelimit";
import { createMemoryStore, type Store } from "../src/store";

// ==========================
// Allows requests within limit
// ==========================

test("allows requests within limit", async () => {
  const store = createMemoryStore();
  const limiter = ratelimit({
    id: "basic",
    limit: 5,
    windowSecs: 60,
    store,
  });

  expect(limiter.id).toBe("basic");

  for (let i = 0; i < 5; i++) {
    const result = await limiter.check("user:1");
    expect(result.limited).toBe(false);
  }
});

// ==========================
// Blocks requests over limit
// ==========================

test("blocks requests over limit", async () => {
  const store = createMemoryStore();
  const limiter = ratelimit({
    id: "block",
    limit: 3,
    windowSecs: 60,
    store,
  });

  for (let i = 0; i < 3; i++) {
    await limiter.check("user:2");
  }

  const result = await limiter.check("user:2");
  expect(result.limited).toBe(true);
  expect(result.remaining).toBe(0);
});

// ==========================
// Returns correct remaining count
// ==========================

test("returns correct remaining count", async () => {
  const store = createMemoryStore();
  const limiter = ratelimit({
    id: "remaining",
    limit: 5,
    windowSecs: 60,
    store,
  });

  // Each call consumes one slot. Since there's no previous window, the
  // weighted count equals the current count exactly.
  const remainings: number[] = [];
  for (let i = 0; i < 5; i++) {
    const result = await limiter.check("user:rem");
    remainings.push(result.remaining);
  }

  // remaining should decrease monotonically: 4, 3, 2, 1, 0
  expect(remainings).toEqual([4, 3, 2, 1, 0]);
});

// ==========================
// checkOrThrow throws RateLimitError when limited
// ==========================

test("checkOrThrow throws RateLimitError when limited", async () => {
  const store = createMemoryStore();
  const limiter = ratelimit({
    id: "throw",
    limit: 1,
    windowSecs: 60,
    store,
  });

  // First call is allowed
  const first = await limiter.checkOrThrow("user:3");
  expect(first.limited).toBe(false);

  // Second call should throw
  let thrown: unknown = null;
  try {
    await limiter.checkOrThrow("user:3");
  } catch (e) {
    thrown = e;
  }

  expect(thrown).toBeInstanceOf(RateLimitError);
  expect(thrown).toBeInstanceOf(Error);
  expect((thrown as RateLimitError).message).toBe("Rate limit exceeded");
});

// ==========================
// RateLimitError has correct properties
// ==========================

test("RateLimitError has correct properties (remaining, resetIn)", async () => {
  const store = createMemoryStore();
  const limiter = ratelimit({
    id: "error-props",
    limit: 1,
    windowSecs: 10,
    store,
  });

  await limiter.check("user:props");

  let thrown: unknown = null;
  try {
    await limiter.checkOrThrow("user:props");
  } catch (e) {
    thrown = e;
  }

  expect(thrown).toBeInstanceOf(RateLimitError);
  const err = thrown as RateLimitError;

  expect(err.name).toBe("RateLimitError");
  expect(err.remaining).toBe(0);
  expect(err.resetIn).toBeGreaterThan(0);
  expect(err.resetIn).toBeLessThanOrEqual(10_000); // windowSecs * 1000
});

// ==========================
// Different identifiers have separate limits
// ==========================

test("different identifiers have separate limits", async () => {
  const store = createMemoryStore();
  const limiter = ratelimit({
    id: "separate",
    limit: 2,
    windowSecs: 60,
    store,
  });

  // Exhaust limit for user:a
  await limiter.check("user:a");
  await limiter.check("user:a");
  const resultA = await limiter.check("user:a");
  expect(resultA.limited).toBe(true);

  // user:b should still have capacity
  const resultB = await limiter.check("user:b");
  expect(resultB.limited).toBe(false);
  expect(resultB.remaining).toBe(1);
});

// ==========================
// Sliding window: old window count decreases over time
// ==========================

test("sliding window: old window count decreases over time", async () => {
  const store = createMemoryStore();
  const limiter = ratelimit({
    id: "sliding",
    limit: 10,
    windowSecs: 1,
    store,
  });

  // Use up 8 of 10 slots
  for (let i = 0; i < 8; i++) {
    await limiter.check("user:5");
  }

  // Wait half a window so we cross into the next window.
  // The previous window's 8 requests are weighted by (1 - elapsedRatio).
  // At ~0.5 elapsed that weight is ~0.5, so weighted ≈ 4 + 1 = 5 → not limited.
  await Bun.sleep(600);

  const result = await limiter.check("user:5");
  expect(result.limited).toBe(false);
  expect(result.remaining).toBeLessThan(10);
  expect(result.remaining).toBeGreaterThan(0);
});

// ==========================
// Long identifiers are hashed
// ==========================

test("long identifiers are hashed (test with 200+ char identifier)", async () => {
  const store = createMemoryStore();
  const limiter = ratelimit({
    id: "hash",
    limit: 2,
    windowSecs: 60,
    store,
  });

  const longId = "user:" + "x".repeat(200);
  expect(longId.length).toBeGreaterThan(128);

  const r1 = await limiter.check(longId);
  expect(r1.limited).toBe(false);
  expect(r1.remaining).toBe(1);

  const r2 = await limiter.check(longId);
  expect(r2.limited).toBe(false);
  expect(r2.remaining).toBe(0);

  // Third call should be limited — proves the hashed key is consistent
  const r3 = await limiter.check(longId);
  expect(r3.limited).toBe(true);
  expect(r3.remaining).toBe(0);
});

test("two different long identifiers hash to different keys", async () => {
  const store = createMemoryStore();
  const limiter = ratelimit({
    id: "hash-diff",
    limit: 1,
    windowSecs: 60,
    store,
  });

  const longA = "a".repeat(200);
  const longB = "b".repeat(200);

  await limiter.check(longA);
  const blockedA = await limiter.check(longA);
  expect(blockedA.limited).toBe(true);

  // longB should not be limited — different hash
  const resultB = await limiter.check(longB);
  expect(resultB.limited).toBe(false);
});

// ==========================
// Custom store is accepted
// ==========================

test("custom store is accepted", async () => {
  const data = new Map<string, { value: unknown; expiresAt: number | null }>();

  const customStore: Store = {
    get(key: string) {
      const entry = data.get(key);
      if (!entry) return undefined;
      if (entry.expiresAt !== null && Date.now() >= entry.expiresAt) {
        data.delete(key);
        return undefined;
      }
      return entry.value;
    },
    set(key: string, value: unknown, ttlMs?: number) {
      const expiresAt = ttlMs != null && ttlMs > 0 ? Date.now() + ttlMs : null;
      data.set(key, { value, expiresAt });
    },
    del(key: string) {
      data.delete(key);
    },
    keys(prefix?: string) {
      const result: string[] = [];
      for (const k of data.keys()) {
        if (prefix === undefined || k.startsWith(prefix)) result.push(k);
      }
      return result;
    },
  };

  const limiter = ratelimit({
    id: "custom-store",
    limit: 2,
    windowSecs: 60,
    store: customStore,
  });

  const r1 = await limiter.check("user:custom");
  expect(r1.limited).toBe(false);
  expect(r1.remaining).toBe(1);

  const r2 = await limiter.check("user:custom");
  expect(r2.limited).toBe(false);
  expect(r2.remaining).toBe(0);

  const r3 = await limiter.check("user:custom");
  expect(r3.limited).toBe(true);

  // Verify data was stored in our custom store
  expect(data.size).toBeGreaterThan(0);
});

// ==========================
// resetIn is positive when limited
// ==========================

test("resetIn is positive when limited", async () => {
  const store = createMemoryStore();
  const limiter = ratelimit({
    id: "reset-positive",
    limit: 1,
    windowSecs: 10,
    store,
  });

  await limiter.check("user:reset");
  const result = await limiter.check("user:reset");

  expect(result.limited).toBe(true);
  expect(result.resetIn).toBeGreaterThan(0);
  expect(result.resetIn).toBeLessThanOrEqual(10_000);
});

test("resetIn is positive even when not limited", async () => {
  const store = createMemoryStore();
  const limiter = ratelimit({
    id: "reset-not-limited",
    limit: 10,
    windowSecs: 5,
    store,
  });

  const result = await limiter.check("user:ok");
  expect(result.limited).toBe(false);
  expect(result.resetIn).toBeGreaterThan(0);
  expect(result.resetIn).toBeLessThanOrEqual(5_000);
});

// ==========================
// Concurrent checks are consistent
// ==========================

test("concurrent checks are consistent (JS is single-threaded so all should be atomic)", async () => {
  const store = createMemoryStore();
  const limiter = ratelimit({
    id: "concurrent",
    limit: 5,
    windowSecs: 60,
    store,
  });

  // Fire 10 concurrent checks for the same identifier
  const results = await Promise.all(
    Array.from({ length: 10 }, () => limiter.check("user:race")),
  );

  const limited = results.filter((r) => r.limited).length;
  const notLimited = results.filter((r) => !r.limited).length;

  // Since the store is synchronous and JS is single-threaded, each
  // `check()` completes its synchronous store reads/writes before the
  // next microtask runs.  Exactly 5 should be allowed.
  expect(notLimited).toBe(5);
  expect(limited).toBe(5);

  // The remaining count should monotonically decrease for the allowed ones
  const allowedRemaining = results
    .filter((r) => !r.limited)
    .map((r) => r.remaining);
  for (let i = 1; i < allowedRemaining.length; i++) {
    expect(allowedRemaining[i]).toBeLessThanOrEqual(allowedRemaining[i - 1]);
  }
});

// ==========================
// Edge: default store is created when none provided
// ==========================

test("uses internal memory store when no store is provided", async () => {
  const limiter = ratelimit({
    id: "default-store",
    limit: 2,
    windowSecs: 60,
  });

  const r1 = await limiter.check("u");
  expect(r1.limited).toBe(false);
  const r2 = await limiter.check("u");
  expect(r2.limited).toBe(false);
  const r3 = await limiter.check("u");
  expect(r3.limited).toBe(true);
});

test("windowSecs must be a positive integer", () => {
  expect(() => ratelimit({ id: "zero-window", limit: 1, windowSecs: 0 })).toThrow(/positive integer/);
  expect(() => ratelimit({ id: "fractional-window", limit: 1, windowSecs: 0.5 })).toThrow(
    /positive integer/,
  );
  expect(() => ratelimit({ id: "safe-window", limit: 1, windowSecs: 1 })).not.toThrow();
  expect(() =>
    ratelimit({ id: "large-integer-window", limit: 1, windowSecs: Number.MAX_SAFE_INTEGER + 1 }),
  ).not.toThrow();
});

test("long identifiers do not collide at a practical scale", async () => {
  const { simpleHash } = await import("../src/internal/id");
  const seen = new Set<string>();
  for (let i = 0; i < 100_000; i++) {
    seen.add(simpleHash(`user:${i}:${"x".repeat(200)}`));
  }
  // A 32-bit hash makes a birthday collision near-certain well before this.
  expect(seen.size).toBe(100_000);
});
