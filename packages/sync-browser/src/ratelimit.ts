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

  if (
    !Number.isSafeInteger(windowSecs)
    || windowSecs <= 0
    || windowSecs > Math.floor(Number.MAX_SAFE_INTEGER / 2_000)
  ) {
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

    const keyForWindow = (window: number): string =>
      `sync:ratelimit:browser:v2:${encodeURIComponent(
        JSON.stringify([prefix, config.id, safeIdentifier, window]),
      )}`;
    const legacyKeyForWindow = (window: number): string =>
      `${prefix}:${config.id}:${safeIdentifier}:${window}`;
    const currentKey = keyForWindow(currentWindow);
    const previousKey = keyForWindow(previousWindow);
    const currentLegacyKey = legacyKeyForWindow(currentWindow);
    const previousLegacyKey = legacyKeyForWindow(previousWindow);

    const previousCounts = [
      readCounter(store.get(previousKey)),
      readCounter(store.get(previousLegacyKey)),
    ];
    const currentCounts = [
      readCounter(store.get(currentKey)),
      readCounter(store.get(currentLegacyKey)),
    ];
    const resetIn = windowMs - elapsedInWindow;

    if (previousCounts.includes(null) || currentCounts.includes(null)) {
      return { limited: true, remaining: 0, resetIn };
    }

    const previousCount = Math.max(...previousCounts as number[]);
    const currentStoredCount = Math.max(...currentCounts as number[]);
    const currentCount = currentStoredCount + 1;
    store.set(currentLegacyKey, currentCount, windowSecs * 2000);
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
