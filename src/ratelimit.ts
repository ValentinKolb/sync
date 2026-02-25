import { redis } from "bun";
import { createHash } from "crypto";

const DEFAULT_PREFIX = "sync:ratelimit";
const DEFAULT_WINDOW_SECS = 1;
const MAX_IDENTIFIER_LENGTH = 128;

const RATE_LIMIT_SCRIPT = `
  local currentKey = KEYS[1]
  local previousKey = KEYS[2]
  local windowSecs = tonumber(ARGV[1])
  local limit = tonumber(ARGV[2])
  local elapsedRatio = tonumber(ARGV[3])

  local previousCount = tonumber(redis.call("GET", previousKey) or "0")

  local currentCount = redis.call("INCR", currentKey)
  if currentCount == 1 then
    redis.call("EXPIRE", currentKey, windowSecs * 2)
  end

  local weightedCount = previousCount * (1 - elapsedRatio) + currentCount

  return {currentCount, previousCount, weightedCount}
`;

const normalizeIdentifier = (identifier: string): string => {
  if (identifier.length <= MAX_IDENTIFIER_LENGTH) return identifier;
  const hash = createHash("sha256").update(identifier).digest("hex");
  return `hash:${hash}`;
};

// ==========================
// Types
// ==========================

export type RateLimitResult = {
  limited: boolean;
  remaining: number;
  resetIn: number;
};

export type RateLimitConfig = {
  id: string;
  limit: number;
  windowSecs?: number;
  prefix?: string;
};

export type RateLimiter = {
  id: string;
  check(identifier: string): Promise<RateLimitResult>;
  checkOrThrow(identifier: string): Promise<RateLimitResult>;
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
// Rate Limiter Factory
// ==========================

export const ratelimit = (config: RateLimitConfig): RateLimiter => {
  const prefix = config.prefix ?? DEFAULT_PREFIX;
  const windowSecs = config.windowSecs ?? DEFAULT_WINDOW_SECS;
  const { limit } = config;

  const check = async (identifier: string): Promise<RateLimitResult> => {
    const safeIdentifier = normalizeIdentifier(identifier);
    const now = Date.now();
    const windowMs = windowSecs * 1000;
    const currentWindow = Math.floor(now / windowMs);
    const previousWindow = currentWindow - 1;
    const elapsedInWindow = now % windowMs;
    const elapsedRatio = elapsedInWindow / windowMs;

    const currentKey = `${prefix}:${config.id}:${safeIdentifier}:${currentWindow}`;
    const previousKey = `${prefix}:${config.id}:${safeIdentifier}:${previousWindow}`;

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

  return { id: config.id, check, checkOrThrow };
};
