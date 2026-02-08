import { redis } from "bun";
import { createHash } from "crypto";

// ==========================
// Types
// ==========================

export type RateLimitResult = {
  limited: boolean;
  remaining: number;
  resetIn: number;
};

export type RateLimitConfig = {
  /** Maximum number of requests allowed in the window */
  limit: number;
  /** Window size in seconds (default: 1) */
  windowSecs?: number;
  /** Key prefix for Redis keys (default: "ratelimit") */
  prefix?: string;
};

export type RateLimiter = {
  /** Check rate limit for an identifier */
  check: (identifier: string) => Promise<RateLimitResult>;
  /** Check and throw if limited */
  checkOrThrow: (identifier: string) => Promise<RateLimitResult>;
};

// ==========================
// Rate Limit Error
// ==========================

export class RateLimitError extends Error {
  readonly remaining: number;
  readonly resetIn: number;

  constructor(result: RateLimitResult) {
    super("Rate limit exceeded");
    this.name = "RateLimitError";
    this.remaining = result.remaining;
    this.resetIn = result.resetIn;
  }
}

// ==========================
// Lua Script for Atomic Rate Limiting
// ==========================

// Atomically increment counter and set expiry in one operation
const RATE_LIMIT_SCRIPT = `
  local currentKey = KEYS[1]
  local previousKey = KEYS[2]
  local windowSecs = tonumber(ARGV[1])
  local limit = tonumber(ARGV[2])
  local elapsedRatio = tonumber(ARGV[3])

  -- Get previous window count
  local previousCount = tonumber(redis.call("GET", previousKey) or "0")

  -- Increment current window and set expiry atomically
  local currentCount = redis.call("INCR", currentKey)
  if currentCount == 1 then
    redis.call("EXPIRE", currentKey, windowSecs * 2)
  end

  -- Calculate weighted count
  local weightedCount = previousCount * (1 - elapsedRatio) + currentCount

  return {currentCount, previousCount, weightedCount}
`;

const MAX_IDENTIFIER_LENGTH = 128;

const normalizeIdentifier = (identifier: string): string => {
  if (identifier.length <= MAX_IDENTIFIER_LENGTH) return identifier;
  const hash = createHash("sha256").update(identifier).digest("hex");
  return `hash:${hash}`;
};

// ==========================
// Rate Limiter Factory
// ==========================

/**
 * Create a rate limiter with the given configuration.
 * Uses a sliding window algorithm for smooth rate limiting.
 *
 * @example
 * ```ts
 * const limiter = ratelimit.create({ limit: 100, windowSecs: 60 });
 *
 * // Check rate limit
 * const result = await limiter.check("user:123");
 * if (result.limited) {
 *   console.log(`Retry in ${result.resetIn}ms`);
 * }
 *
 * // Or throw on limit
 * await limiter.checkOrThrow("user:123");
 * ```
 */
const create = (config: RateLimitConfig): RateLimiter => {
  const { limit, windowSecs = 1, prefix = "ratelimit" } = config;

  const check = async (identifier: string): Promise<RateLimitResult> => {
    const safeIdentifier = normalizeIdentifier(identifier);
    const now = Date.now();
    const windowMs = windowSecs * 1000;
    const currentWindow = Math.floor(now / windowMs);
    const previousWindow = currentWindow - 1;
    const elapsedInWindow = now % windowMs;
    const elapsedRatio = elapsedInWindow / windowMs;

    const currentKey = `${prefix}:${safeIdentifier}:${currentWindow}`;
    const previousKey = `${prefix}:${safeIdentifier}:${previousWindow}`;

    // Execute atomic Lua script
    const result = (await redis.send("EVAL", [
      RATE_LIMIT_SCRIPT,
      "2",
      currentKey,
      previousKey,
      windowSecs.toString(),
      limit.toString(),
      elapsedRatio.toString(),
    ])) as [number, number, number];

    const [, , weightedCount] = result;

    const limited = weightedCount > limit;
    const remaining = Math.max(0, Math.floor(limit - weightedCount));
    const resetIn = windowMs - elapsedInWindow;

    return { limited, remaining, resetIn };
  };

  const checkOrThrow = async (identifier: string): Promise<RateLimitResult> => {
    const result = await check(identifier);
    if (result.limited) {
      throw new RateLimitError(result);
    }
    return result;
  };

  return { check, checkOrThrow };
};

// ==========================
// Export
// ==========================

export const ratelimit = { create, RateLimitError };
