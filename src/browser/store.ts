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
// Factory
// ==========================

export const createMemoryStore = (): MemoryStore => new MemoryStore();
