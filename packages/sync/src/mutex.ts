import { redis, sleep } from "bun";
import { createHash, randomBytes } from "crypto";

const DEFAULT_PREFIX = "sync:mutex";
const DEFAULT_RETRY_COUNT = 10;
const DEFAULT_RETRY_DELAY = 200;
const DEFAULT_TTL = 10_000;
const MAX_RESOURCE_LENGTH = 128;

const RELEASE_SCRIPT = `
  if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
  else
    return 0
  end
`;

const EXTEND_SCRIPT = `
  if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("pexpire", KEYS[1], ARGV[2])
  else
    return 0
  end
`;

const normalizeResource = (resource: string): string => {
  if (resource.length <= MAX_RESOURCE_LENGTH) return resource;
  const hash = createHash("sha256").update(resource).digest("hex");
  return `hash:${hash}`;
};

const assertTtl = (ttl: number, label = "ttl"): void => {
  if (!Number.isSafeInteger(ttl) || ttl <= 0) {
    throw new RangeError(`${label} must be a positive safe integer`);
  }
};

// ==========================
// Types
// ==========================

export type Lock = {
  resource: string;
  value: string;
  ttl: number;
  expiration: number;
};

export type MutexConfig = {
  id: string;
  prefix?: string;
  retryCount?: number;
  retryDelay?: number;
  defaultTtl?: number;
};

export type Mutex = {
  id: string;
  acquire(resource: string, ttl?: number): Promise<Lock | null>;
  release(lock: Lock): Promise<void>;
  withLock<T>(resource: string, fn: (lock: Lock) => Promise<T> | T, ttl?: number): Promise<T | null>;
  withLockOrThrow<T>(resource: string, fn: (lock: Lock) => Promise<T> | T, ttl?: number): Promise<T>;
  extend(lock: Lock, ttl?: number): Promise<boolean>;
};

// ==========================
// Lock Error
// ==========================

export class LockError extends Error {
  readonly resource: string;

  constructor(resource: string) {
    super(`Failed to acquire lock on resource: ${resource}`);
    this.name = "LockError";
    this.resource = resource;
  }
}

// ==========================
// Mutex Factory
// ==========================

export const mutex = (config: MutexConfig): Mutex => {
  const prefix = config.prefix ?? DEFAULT_PREFIX;
  const retryCount = config.retryCount ?? DEFAULT_RETRY_COUNT;
  const retryDelay = config.retryDelay ?? DEFAULT_RETRY_DELAY;
  const defaultTtl = config.defaultTtl ?? DEFAULT_TTL;
  assertTtl(defaultTtl, "defaultTtl");

  const acquire = async (resource: string, ttl: number = defaultTtl): Promise<Lock | null> => {
    assertTtl(ttl);
    const safeResource = normalizeResource(resource);
    const key = `${prefix}:${config.id}:${safeResource}`;
    const value = randomBytes(16).toString("hex");

    for (let attempt = 0; attempt <= retryCount; attempt++) {
      const result = await redis.send("SET", [key, value, "NX", "PX", ttl.toString()]);

      if (result === "OK") {
        return {
          resource: key,
          value,
          ttl,
          expiration: Date.now() + ttl,
        };
      }

      if (attempt < retryCount) {
        await sleep(retryDelay + Math.random() * 100);
      }
    }

    return null;
  };

  const release = async (lock: Lock): Promise<void> => {
    await redis.send("EVAL", [RELEASE_SCRIPT, "1", lock.resource, lock.value]);
  };

  const extend = async (lock: Lock, ttl: number = defaultTtl): Promise<boolean> => {
    assertTtl(ttl);
    const result = await redis.send("EVAL", [EXTEND_SCRIPT, "1", lock.resource, lock.value, ttl.toString()]);
    // `Number(result) > 0` is what the rest of the package uses; a strict === 1
    // silently returns false if the driver ever hands back a string.
    if (Number(result) > 0) {
      lock.ttl = ttl;
      lock.expiration = Date.now() + ttl;
      return true;
    }
    return false;
  };

  const withLock = async <T>(resource: string, fn: (lock: Lock) => Promise<T> | T, ttl?: number): Promise<T | null> => {
    const lock = await acquire(resource, ttl);
    if (!lock) return null;

    try {
      return await fn(lock);
    } finally {
      await release(lock);
    }
  };

  const withLockOrThrow = async <T>(resource: string, fn: (lock: Lock) => Promise<T> | T, ttl?: number): Promise<T> => {
    const lock = await acquire(resource, ttl);
    if (!lock) {
      throw new LockError(resource);
    }

    try {
      return await fn(lock);
    } finally {
      await release(lock);
    }
  };

  return { id: config.id, acquire, release, withLock, withLockOrThrow, extend };
};
