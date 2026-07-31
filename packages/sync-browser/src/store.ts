// ==========================
// Store Interface
// ==========================

export interface Store {
  get(key: string): unknown | undefined;
  set(key: string, value: unknown, ttlMs?: number): void;
  del(key: string): void;
  keys(prefix?: string): string[];
}

const MAX_TIMER_DELAY_MS = 2_147_483_647;

const nextTimerDelay = (expiresAt: number): number =>
  Math.min(MAX_TIMER_DELAY_MS, Math.max(0, expiresAt - Date.now()));

// ==========================
// MemoryStore
// ==========================

type Entry = {
  value: unknown;
  expiresAt: number | null;
};

/**
 * Thrown when the backing storage refuses a write — most often a quota error.
 * Distinguishable so callers do not mistake a storage failure for a user error.
 */
export class StoreWriteError extends Error {
  readonly key: string;
  override readonly cause: unknown;

  constructor(key: string, cause: unknown) {
    super(`store write failed for key "${key}": ${cause instanceof Error ? cause.message : String(cause)}`);
    this.name = "StoreWriteError";
    this.key = key;
    this.cause = cause;
  }
}

/**
 * Values are snapshotted on the way in and out. Handing back the caller's own
 * object meant `send({ data: payload })` then `payload.x = 1` changed what the
 * consumer saw, and a consumer mutating a received value mutated the stored
 * copy across redelivery. `structuredClone` preserves the public in-memory
 * store's value domain while still preventing reference aliasing.
 */
const snapshot = (value: unknown): unknown => structuredClone(value);

export class MemoryStore implements Store {
  private data = new Map<string, Entry>();
  private timers = new Map<string, ReturnType<typeof setTimeout>>();

  private scheduleExpiry(key: string, expiresAt: number): void {
    const timer = setTimeout(() => {
      if (this.timers.get(key) !== timer) return;

      const entry = this.data.get(key);
      if (!entry || entry.expiresAt !== expiresAt) {
        this.timers.delete(key);
        return;
      }
      if (Date.now() < expiresAt) {
        this.scheduleExpiry(key, expiresAt);
        return;
      }
      this.del(key);
    }, nextTimerDelay(expiresAt));
    this.timers.set(key, timer);
  }

  get(key: string): unknown | undefined {
    const entry = this.data.get(key);
    if (!entry) return undefined;

    // Lazy expiry check (safety net if timer hasn't fired yet)
    if (entry.expiresAt !== null && Date.now() >= entry.expiresAt) {
      this.del(key);
      return undefined;
    }

    return snapshot(entry.value);
  }

  set(key: string, value: unknown, ttlMs?: number): void {
    let valueSnapshot: unknown;
    try {
      valueSnapshot = snapshot(value);
    } catch (error) {
      throw new StoreWriteError(key, error);
    }

    const existingTimer = this.timers.get(key);
    if (existingTimer) {
      clearTimeout(existingTimer);
      this.timers.delete(key);
    }

    const expiresAt = ttlMs != null && ttlMs > 0 ? Date.now() + ttlMs : null;
    this.data.set(key, { value: valueSnapshot, expiresAt });

    if (expiresAt !== null) this.scheduleExpiry(key, expiresAt);
  }

  del(key: string): void {
    this.data.delete(key);
    const timer = this.timers.get(key);
    if (timer) {
      clearTimeout(timer);
      this.timers.delete(key);
    }
  }

  keys(prefix?: string): string[] {
    const now = Date.now();
    const result: string[] = [];

    for (const [key, entry] of this.data) {
      // Lazy expiry during iteration
      if (entry.expiresAt !== null && now >= entry.expiresAt) {
        this.del(key);
        continue;
      }
      if (prefix === undefined || key.startsWith(prefix)) {
        result.push(key);
      }
    }

    return result;
  }

  /** Clear all keys and timers (useful for tests). */
  clear(): void {
    for (const timer of this.timers.values()) {
      clearTimeout(timer);
    }
    this.timers.clear();
    this.data.clear();
  }
}

// ==========================
// LocalStorageStore
// ==========================

type StoredValue = {
  value: unknown;
  expiresAt: number | null;
};

const parseStoredValue = (key: string, raw: string): StoredValue => {
  try {
    const entry = JSON.parse(raw) as unknown;
    if (
      entry === null ||
      typeof entry !== "object" ||
      Array.isArray(entry) ||
      !Object.prototype.hasOwnProperty.call(entry, "value") ||
      !Object.prototype.hasOwnProperty.call(entry, "expiresAt")
    ) {
      throw new Error("invalid storage envelope");
    }
    const stored = entry as StoredValue;
    if (stored.expiresAt !== null && !Number.isFinite(stored.expiresAt)) {
      throw new Error("invalid expiry");
    }
    return stored;
  } catch (error) {
    throw new Error(`invalid stored value for key "${key}"`, { cause: error });
  }
};

export class LocalStorageStore implements Store {
  private prefix: string;
  private timers = new Map<string, ReturnType<typeof setTimeout>>();
  private initialized = false;

  constructor(prefix = "sync") {
    this.prefix = prefix;
  }

  private storageKey(key: string): string {
    return `${this.prefix}:${key}`;
  }

  private ensureInitialized(): void {
    if (this.initialized) return;
    this.initialized = true;

    const now = Date.now();
    const fullPrefix = this.storageKey("");
    const storageKeys: string[] = [];
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      if (key?.startsWith(fullPrefix)) storageKeys.push(key);
    }

    for (const storageKey of storageKeys) {
      const raw = localStorage.getItem(storageKey);
      if (raw === null) continue;

      try {
        const logicalKey = storageKey.slice(fullPrefix.length);
        const entry = parseStoredValue(logicalKey, raw);
        if (entry.expiresAt === null || !Number.isFinite(entry.expiresAt)) continue;

        if (now >= entry.expiresAt) {
          localStorage.removeItem(storageKey);
        } else {
          this.scheduleExpiry(logicalKey, entry.expiresAt);
        }
      } catch {
        // Corrupt values remain unreadable through get(); they have no TTL to restore.
      }
    }
  }

  private scheduleExpiry(key: string, expiresAt: number): void {
    const timer = setTimeout(() => {
      if (this.timers.get(key) !== timer) return;

      const raw = localStorage.getItem(this.storageKey(key));
      if (raw === null) {
        this.timers.delete(key);
        return;
      }

      try {
        const entry = parseStoredValue(key, raw);
        if (entry.expiresAt !== expiresAt) {
          this.timers.delete(key);
          return;
        }
      } catch {
        this.timers.delete(key);
        return;
      }

      if (Date.now() < expiresAt) {
        this.scheduleExpiry(key, expiresAt);
        return;
      }
      this.del(key);
    }, nextTimerDelay(expiresAt));
    this.timers.set(key, timer);
  }

  get(key: string): unknown | undefined {
    this.ensureInitialized();
    const raw = localStorage.getItem(this.storageKey(key));
    if (raw === null) return undefined;

    const entry = parseStoredValue(key, raw);

    // Lazy expiry check
    if (entry.expiresAt !== null && Date.now() >= entry.expiresAt) {
      this.del(key);
      return undefined;
    }

    return entry.value;
  }

  set(key: string, value: unknown, ttlMs?: number): void {
    this.ensureInitialized();
    const expiresAt = ttlMs != null && ttlMs > 0 ? Date.now() + ttlMs : null;
    let raw: string;
    try {
      raw = JSON.stringify({ value, expiresAt });
      parseStoredValue(key, raw);
    } catch (error) {
      throw new StoreWriteError(key, error);
    }
    try {
      localStorage.setItem(this.storageKey(key), raw);
    } catch (error) {
      // A raw QuotaExceededError used to propagate out of topic.pub(),
      // ratelimit.check() or pump's writeState mid-dispatch, where it landed
      // inside runAttempt's try and was misreported as a *user* dispatch
      // failure — incrementing failureCount and eventually marking the run
      // permanently failed. Keep its true classification.
      throw new StoreWriteError(key, error);
    }

    const existingTimer = this.timers.get(key);
    if (existingTimer) {
      clearTimeout(existingTimer);
      this.timers.delete(key);
    }
    if (expiresAt !== null) this.scheduleExpiry(key, expiresAt);
  }

  del(key: string): void {
    this.ensureInitialized();
    try {
      localStorage.removeItem(this.storageKey(key));
    } catch (error) {
      throw new StoreWriteError(key, error);
    }
    const timer = this.timers.get(key);
    if (timer) {
      clearTimeout(timer);
      this.timers.delete(key);
    }
  }

  keys(prefix?: string): string[] {
    this.ensureInitialized();
    const now = Date.now();
    const result: string[] = [];
    const fullPrefix = prefix !== undefined ? this.storageKey(prefix) : this.storageKey("");

    // Two passes, the pattern clear() below already uses. localStorage is index
    // addressed: removing item i shifts every later item down one, so deleting
    // an expired entry inside the loop skipped exactly one live key per expired
    // key — silently stalling pump's claimNext, which is the only caller.
    const storageKeys: string[] = [];
    for (let i = 0; i < localStorage.length; i++) {
      const storageKey = localStorage.key(i);
      if (storageKey && storageKey.startsWith(fullPrefix)) storageKeys.push(storageKey);
    }

    for (const storageKey of storageKeys) {
      const logicalKey = storageKey.slice(this.prefix.length + 1);

      const raw = localStorage.getItem(storageKey);
      if (raw) {
        try {
          const entry = parseStoredValue(logicalKey, raw);
          if (entry.expiresAt !== null && now >= entry.expiresAt) {
            this.del(logicalKey);
            continue;
          }
        } catch {
          // Keep corrupt keys visible so callers cannot mistake them for absent state.
        }
      }

      if (prefix === undefined || logicalKey.startsWith(prefix)) {
        result.push(logicalKey);
      }
    }

    return result;
  }

  /** Clear all keys with this store's prefix from localStorage. */
  clear(): void {
    this.ensureInitialized();
    for (const timer of this.timers.values()) {
      clearTimeout(timer);
    }
    this.timers.clear();

    const toRemove: string[] = [];
    const fullPrefix = this.storageKey("");
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      if (key && key.startsWith(fullPrefix)) {
        toRemove.push(key);
      }
    }
    for (const key of toRemove) {
      localStorage.removeItem(key);
    }
  }
}

// ==========================
// Factories
// ==========================

export const createMemoryStore = (): MemoryStore => new MemoryStore();
export const createLocalStorageStore = (prefix?: string): LocalStorageStore => new LocalStorageStore(prefix);
