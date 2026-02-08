import { test, expect, beforeEach } from "bun:test";
import { redis } from "bun";
import { ratelimit, RateLimitError } from "../index";

// Clean up Redis before each test
beforeEach(async () => {
  const keys = await redis.send("KEYS", ["ratelimit:test:*"]);
  if (Array.isArray(keys) && keys.length > 0) {
    await redis.send("DEL", keys as string[]);
  }
});

test("allows requests within limit", async () => {
  const limiter = ratelimit.create({
    limit: 5,
    windowSecs: 60,
    prefix: "ratelimit:test",
  });

  for (let i = 0; i < 5; i++) {
    const result = await limiter.check("user:1");
    expect(result.limited).toBe(false);
    expect(result.remaining).toBe(5 - i - 1);
  }
});

test("blocks requests over limit", async () => {
  const limiter = ratelimit.create({
    limit: 3,
    windowSecs: 60,
    prefix: "ratelimit:test",
  });

  // Use up the limit
  for (let i = 0; i < 3; i++) {
    await limiter.check("user:2");
  }

  // Next request should be limited
  const result = await limiter.check("user:2");
  expect(result.limited).toBe(true);
  expect(result.remaining).toBe(0);
});

test("checkOrThrow throws RateLimitError when limited", async () => {
  const limiter = ratelimit.create({
    limit: 1,
    windowSecs: 60,
    prefix: "ratelimit:test",
  });

  // Use up the limit
  await limiter.check("user:3");

  // Should throw
  try {
    await limiter.checkOrThrow("user:3");
    expect(true).toBe(false); // Should not reach here
  } catch (e) {
    expect(e).toBeInstanceOf(RateLimitError);
    expect((e as RateLimitError).remaining).toBe(0);
    expect((e as RateLimitError).resetIn).toBeGreaterThan(0);
  }
});

test("different identifiers have separate limits", async () => {
  const limiter = ratelimit.create({
    limit: 2,
    windowSecs: 60,
    prefix: "ratelimit:test",
  });

  // User A uses their limit
  await limiter.check("user:a");
  await limiter.check("user:a");
  const resultA = await limiter.check("user:a");
  expect(resultA.limited).toBe(true);

  // User B should still have their limit
  const resultB = await limiter.check("user:b");
  expect(resultB.limited).toBe(false);
  expect(resultB.remaining).toBe(1);
});

test("resetIn returns time until window reset", async () => {
  const limiter = ratelimit.create({
    limit: 10,
    windowSecs: 60,
    prefix: "ratelimit:test",
  });

  const result = await limiter.check("user:4");
  expect(result.resetIn).toBeGreaterThan(0);
  expect(result.resetIn).toBeLessThanOrEqual(60000);
});

test("sliding window counts previous window", async () => {
  const limiter = ratelimit.create({
    limit: 10,
    windowSecs: 1, // 1 second window for faster testing
    prefix: "ratelimit:test",
  });

  // Make 8 requests
  for (let i = 0; i < 8; i++) {
    await limiter.check("user:5");
  }

  // Wait for half the window
  await Bun.sleep(500);

  // Previous window count is weighted, so we should still have limited capacity
  // 8 requests * 0.5 weight = 4 weighted from previous
  // So we should have ~6 remaining
  const result = await limiter.check("user:5");
  expect(result.limited).toBe(false);
  expect(result.remaining).toBeLessThan(10);
  expect(result.remaining).toBeGreaterThan(0);
});

test("long identifiers are hashed", async () => {
  const limiter = ratelimit.create({
    limit: 2,
    windowSecs: 60,
    prefix: "ratelimit:test",
  });

  const longId = "user:" + "a".repeat(200);
  const result = await limiter.check(longId);
  expect(result.limited).toBe(false);
  expect(result.remaining).toBe(1);
});
