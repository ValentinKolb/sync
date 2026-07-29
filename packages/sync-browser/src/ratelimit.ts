import { type Store } from "./store";
import { resolveStore } from "./internal/shared-state";
import { simpleHash } from "./internal/id";

const DEFAULT_PREFIX = "sync:ratelimit";
const DEFAULT_WINDOW_SECS = 1;
const MAX_IDENTIFIER_LENGTH = 128;

const normalizeIdentifier = (identifier: string): string => {
  if (identifier.length <= MAX_IDENTIFIER_LENGTH) return identifier;
  return simpleHash(identifier);
};

const readCounter = (value: unknown): number | null => {
  if (value === undefined) return 0;
  return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : null;
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
  store?: Store;
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
  const store = resolveStore(config.store);

  if (!Number.isInteger(windowSecs) || windowSecs <= 0) {
    throw new Error("windowSecs must be a positive integer number of seconds");
  }
  if (!Number.isFinite(limit) || limit <= 0) {
    throw new Error("limit must be > 0");
  }

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

    const previousCount = readCounter(store.get(previousKey));
    const currentStoredCount = readCounter(store.get(currentKey));
    const resetIn = windowMs - elapsedInWindow;

    if (previousCount === null || currentStoredCount === null) {
      return { limited: true, remaining: 0, resetIn };
    }

    const currentCount = currentStoredCount + 1;
    store.set(currentKey, currentCount, windowSecs * 2000);

    const weightedCount = previousCount * (1 - elapsedRatio) + currentCount;

    const limited = weightedCount > limit;
    const remaining = Math.max(0, Math.floor(limit - weightedCount));

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
