import { test, expect, beforeEach } from "bun:test";
import { redis } from "bun";
import { ratelimit, RateLimitError } from "../index";

beforeEach(async () => {
  const keys = await redis.send("KEYS", ["test:rl:*"]);
  if (Array.isArray(keys) && keys.length > 0) {
    await redis.send("DEL", keys as string[]);
  }
});

test("allows requests within limit", async () => {
  const limiter = ratelimit({
    id: "basic",
    limit: 5,
    windowSecs: 60,
    prefix: "test:rl",
  });

  expect(limiter.id).toBe("basic");

  for (let i = 0; i < 5; i++) {
    const result = await limiter.check("user:1");
    expect(result.limited).toBe(false);
    expect(result.remaining).toBe(5 - i - 1);
  }
});

test("blocks requests over limit", async () => {
  const limiter = ratelimit({
    id: "block",
    limit: 3,
    windowSecs: 60,
    prefix: "test:rl",
  });

  for (let i = 0; i < 3; i++) {
    await limiter.check("user:2");
  }

  const result = await limiter.check("user:2");
  expect(result.limited).toBe(true);
  expect(result.remaining).toBe(0);
});

test("checkOrThrow throws RateLimitError when limited", async () => {
  const limiter = ratelimit({
    id: "throw",
    limit: 1,
    windowSecs: 60,
    prefix: "test:rl",
  });

  await limiter.check("user:3");

  let thrown: unknown = null;
  try {
    await limiter.checkOrThrow("user:3");
  } catch (e) {
    thrown = e;
  }

  expect(thrown).toBeInstanceOf(RateLimitError);
  expect((thrown as RateLimitError).remaining).toBe(0);
  expect((thrown as RateLimitError).resetIn).toBeGreaterThan(0);
});

test("different identifiers have separate limits", async () => {
  const limiter = ratelimit({
    id: "separate",
    limit: 2,
    windowSecs: 60,
    prefix: "test:rl",
  });

  await limiter.check("user:a");
  await limiter.check("user:a");
  const resultA = await limiter.check("user:a");
  expect(resultA.limited).toBe(true);

  const resultB = await limiter.check("user:b");
  expect(resultB.limited).toBe(false);
  expect(resultB.remaining).toBe(1);
});

test("resetIn returns time until window reset", async () => {
  const limiter = ratelimit({
    id: "reset",
    limit: 10,
    windowSecs: 60,
    prefix: "test:rl",
  });

  const result = await limiter.check("user:4");
  expect(result.resetIn).toBeGreaterThan(0);
  expect(result.resetIn).toBeLessThanOrEqual(60000);
});

test("sliding window applies the weighted previous-window count", async () => {
  const id = "sliding";
  const identifier = "user:5";
  const windowSecs = 10;
  const windowMs = windowSecs * 1_000;
  const limiter = ratelimit({
    id,
    limit: 10,
    windowSecs,
    prefix: "test:rl",
  });

  let now = Date.now();
  const remainingInWindow = windowMs - (now % windowMs);
  if (remainingInWindow < 1_000) {
    await Bun.sleep(remainingInWindow + 20);
    now = Date.now();
  }

  const previousWindow = Math.floor(now / windowMs) - 1;
  await redis.set(`test:rl:${id}:${identifier}:${previousWindow}`, "100");

  const result = await limiter.check(identifier);
  expect(result).toMatchObject({ limited: true, remaining: 0 });
});

test("sliding window preserves fractional carry-over from Redis Lua", async () => {
  const id = "sliding-fraction";
  const identifier = "user:fraction";
  const windowSecs = 60;
  const windowMs = windowSecs * 1_000;
  const limiter = ratelimit({ id, limit: 1, windowSecs, prefix: "test:rl" });

  let now = Date.now();
  const remainingInWindow = windowMs - (now % windowMs);
  if (remainingInWindow < 1_000) {
    await Bun.sleep(remainingInWindow + 10);
    now = Date.now();
  }
  const previousWindow = Math.floor(now / windowMs) - 1;
  await redis.set(`test:rl:${id}:${identifier}:${previousWindow}`, "1");

  // Current count 1 plus any positive carry-over exceeds the limit. Returning
  // the Lua number directly used to truncate it to 1 and fail open.
  expect(await limiter.check(identifier)).toMatchObject({ limited: true, remaining: 0 });
});

test("long identifiers are hashed", async () => {
  const limiter = ratelimit({
    id: "hash",
    limit: 2,
    windowSecs: 60,
    prefix: "test:rl",
  });

  const longId = "user:" + "a".repeat(200);
  const result = await limiter.check(longId);
  expect(result.limited).toBe(false);
  expect(result.remaining).toBe(1);
});

test("concurrent checks are atomic", async () => {
  const limiter = ratelimit({
    id: "concurrent",
    limit: 5,
    windowSecs: 60,
    prefix: "test:rl",
  });

  const results = await Promise.all(
    Array.from({ length: 10 }, () => limiter.check("user:race")),
  );

  const limited = results.filter((r) => r.limited).length;
  const notLimited = results.filter((r) => !r.limited).length;

  expect(notLimited).toBe(5);
  expect(limited).toBe(5);
});

test("limit resets after window expires", async () => {
  const limiter = ratelimit({
    id: "window-reset",
    limit: 2,
    windowSecs: 1,
    prefix: "test:rl",
  });

  await limiter.check("user:reset");
  await limiter.check("user:reset");
  const blocked = await limiter.check("user:reset");
  expect(blocked.limited).toBe(true);

  // Wait for TWO full windows so the sliding window weight is zero
  // (previous window's count * (1 - elapsedRatio) must be negligible)
  await Bun.sleep(2100);

  const fresh = await limiter.check("user:reset");
  expect(fresh.limited).toBe(false);
});

test("different limiter ids are isolated", async () => {
  const apiLimiter = ratelimit({
    id: "api",
    limit: 1,
    windowSecs: 60,
    prefix: "test:rl",
  });

  const webhookLimiter = ratelimit({
    id: "webhook",
    limit: 1,
    windowSecs: 60,
    prefix: "test:rl",
  });

  await apiLimiter.check("user:1");
  const apiBlocked = await apiLimiter.check("user:1");
  expect(apiBlocked.limited).toBe(true);

  // Same identifier but different limiter id — should not be limited
  const webhookOk = await webhookLimiter.check("user:1");
  expect(webhookOk.limited).toBe(false);
});

test("invalid limiter configuration is rejected instead of failing open", () => {
  // windowSecs: 0 collapsed both window keys, made elapsedRatio NaN, and
  // `EXPIRE key 0` deleted the counter on every first increment — so the
  // limiter silently allowed everything, the worst failure available to an
  // abuse-control primitive. The browser already threw on the same config.
  expect(() => ratelimit({ id: "bad-window", limit: 5, windowSecs: 0 })).toThrow(/windowSecs/);
  expect(() => ratelimit({ id: "frac-window", limit: 5, windowSecs: 0.1 })).toThrow(/windowSecs/);
  expect(() => ratelimit({ id: "huge-window", limit: 5, windowSecs: 1e308 })).toThrow(/windowSecs/);
  expect(() => ratelimit({ id: "bad-limit", limit: 0, windowSecs: 60 })).toThrow(/limit/);
  expect(() => ratelimit({ id: "ok", limit: 5, windowSecs: 60 })).not.toThrow();
});
