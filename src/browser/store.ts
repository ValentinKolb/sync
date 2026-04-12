// ==========================
// Store Interface
// ==========================

export interface Store {
  get(key: string): unknown | undefined;
  set(key: string, value: unknown, ttlMs?: number): void;
  del(key: string): void;
  keys(prefix?: string): string[];
}

// ==========================
// MemoryStore
// ==========================

type Entry = {
  value: unknown;
  expiresAt: number | null;
};

export class MemoryStore implements Store {
  private data = new Map<string, Entry>();
  private timers = new Map<string, ReturnType<typeof setTimeout>>();

  get(key: string): unknown | undefined {
    const entry = this.data.get(key);
    if (!entry) return undefined;

    // Lazy expiry check (safety net if timer hasn't fired yet)
    if (entry.expiresAt !== null && Date.now() >= entry.expiresAt) {
      this.del(key);
      return undefined;
    }

    return entry.value;
  }

  set(key: string, value: unknown, ttlMs?: number): void {
    // Clear any existing timer
    const existingTimer = this.timers.get(key);
    if (existingTimer) {
      clearTimeout(existingTimer);
      this.timers.delete(key);
    }

    const expiresAt = ttlMs != null && ttlMs > 0 ? Date.now() + ttlMs : null;
    this.data.set(key, { value, expiresAt });

    if (ttlMs != null && ttlMs > 0) {
      this.timers.set(
        key,
        setTimeout(() => this.del(key), ttlMs),
      );
    }
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

export class LocalStorageStore implements Store {
  private prefix: string;
  private timers = new Map<string, ReturnType<typeof setTimeout>>();

  constructor(prefix = "sync") {
    this.prefix = prefix;
  }

  private storageKey(key: string): string {
    return `${this.prefix}:${key}`;
  }

  get(key: string): unknown | undefined {
    const raw = localStorage.getItem(this.storageKey(key));
    if (raw === null) return undefined;

    try {
      const entry = JSON.parse(raw) as StoredValue;

      // Lazy expiry check
      if (entry.expiresAt !== null && Date.now() >= entry.expiresAt) {
        this.del(key);
        return undefined;
      }

      return entry.value;
    } catch {
      return undefined;
    }
  }

  set(key: string, value: unknown, ttlMs?: number): void {
    // Clear any existing timer
    const existingTimer = this.timers.get(key);
    if (existingTimer) {
      clearTimeout(existingTimer);
      this.timers.delete(key);
    }

    const expiresAt = ttlMs != null && ttlMs > 0 ? Date.now() + ttlMs : null;
    localStorage.setItem(this.storageKey(key), JSON.stringify({ value, expiresAt }));

    if (ttlMs != null && ttlMs > 0) {
      this.timers.set(
        key,
        setTimeout(() => this.del(key), ttlMs),
      );
    }
  }

  del(key: string): void {
    localStorage.removeItem(this.storageKey(key));
    const timer = this.timers.get(key);
    if (timer) {
      clearTimeout(timer);
      this.timers.delete(key);
    }
  }

  keys(prefix?: string): string[] {
    const now = Date.now();
    const result: string[] = [];
    const fullPrefix = prefix !== undefined ? this.storageKey(prefix) : this.storageKey("");

    for (let i = 0; i < localStorage.length; i++) {
      const storageKey = localStorage.key(i);
      if (!storageKey || !storageKey.startsWith(fullPrefix)) continue;

      // Strip the store prefix to get the logical key
      const logicalKey = storageKey.slice(this.prefix.length + 1);

      // Lazy expiry during iteration
      const raw = localStorage.getItem(storageKey);
      if (raw) {
        try {
          const entry = JSON.parse(raw) as StoredValue;
          if (entry.expiresAt !== null && now >= entry.expiresAt) {
            this.del(logicalKey);
            continue;
          }
        } catch {
          continue;
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
