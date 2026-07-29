import { Emitter } from "./emitter";

// ==========================
// Types
// ==========================

export type EventLogEntry = {
  id: string;
  ts: number;
  fields: Record<string, unknown>;
};

export type EventLogConfig = {
  maxLen?: number;
  retentionMs?: number;
  initialEntries?: EventLogEntry[];
};

type AppendHooks = {
  beforeEmit?(entry: EventLogEntry): void;
  rollback?(): void;
};

// ==========================
// EventLog (replaces Redis Streams)
// ==========================

export class EventLog {
  private entries: EventLogEntry[] = [];
  private seq = 0;
  private emitter = new Emitter<EventLogEntry>();
  private maxLen: number;
  private retentionMs: number;

  constructor(config: EventLogConfig = {}) {
    this.maxLen = config.maxLen ?? 50_000;
    this.retentionMs = config.retentionMs ?? 5 * 60 * 1000;
    this.loadEntries(config.initialEntries ?? []);
  }

  private normalizedEntries(initialEntries: EventLogEntry[]): EventLogEntry[] {
    const normalized = new Map<number, EventLogEntry>();
    for (const entry of initialEntries) {
      if (
        !entry
        || typeof entry !== "object"
        || typeof entry.id !== "string"
        || !/^[1-9]\d*$/.test(entry.id)
        || !Number.isSafeInteger(Number(entry.id))
        || !Number.isFinite(entry.ts)
        || entry.ts < 0
        || !entry.fields
        || typeof entry.fields !== "object"
        || Array.isArray(entry.fields)
      ) {
        continue;
      }
      const id = Number(entry.id);
      if (normalized.has(id)) continue;
      normalized.set(id, { id: entry.id, ts: entry.ts, fields: { ...entry.fields } });
    }
    return [...normalized.values()].sort((a, b) => Number(a.id) - Number(b.id));
  }

  private loadEntries(initialEntries: EventLogEntry[]): void {
    this.entries = this.normalizedEntries(initialEntries);
    this.seq = this.entries.reduce((max, entry) => Math.max(max, Number(entry.id) || 0), 0);
    this.trim();
  }

  /** Merge persisted contents without replacing the log identity or rewinding its cursor. */
  restore(initialEntries: EventLogEntry[]): void {
    const merged = new Map(this.entries.map((entry) => [Number(entry.id), entry]));
    for (const entry of this.normalizedEntries(initialEntries)) {
      if (!merged.has(Number(entry.id))) merged.set(Number(entry.id), entry);
    }
    this.entries = [...merged.values()].sort((a, b) => Number(a.id) - Number(b.id));
    this.seq = Math.max(
      this.seq,
      this.entries.reduce((max, entry) => Math.max(max, Number(entry.id) || 0), 0),
    );
    this.trim();
    // Wake blocked subscribers so they re-read the restored buffer from their
    // own cursor instead of treating one restored entry as the new head.
    this.emitter.emit({ id: "0", ts: Date.now(), fields: {} });
  }

  /** Append an entry and return its cursor ID. */
  append(fields: Record<string, unknown>, hooks?: AppendHooks): string {
    const previousEntries = [...this.entries];
    const id = String(++this.seq);
    const entry: EventLogEntry = { id, ts: Date.now(), fields };
    this.entries.push(entry);
    this.trim();
    try {
      hooks?.beforeEmit?.(entry);
    } catch (error) {
      this.entries = previousEntries;
      try {
        hooks?.rollback?.();
      } catch {
        // Preserve the original write error.
      }
      throw error;
    }
    this.emitter.emit(entry);
    return id;
  }

  /** Get entries after a cursor, optionally limited by count. */
  range(after: string, count?: number): EventLogEntry[] {
    this.trim();
    const afterNum = Number(after) || 0;
    const result: EventLogEntry[] = [];

    for (const entry of this.entries) {
      if (Number(entry.id) <= afterNum) continue;
      result.push(entry);
      if (count !== undefined && result.length >= count) break;
    }

    return result;
  }

  /** Get the latest cursor, or "0" if empty. */
  latest(): string {
    this.trim();
    if (this.entries.length === 0) return "0";
    return this.entries[this.entries.length - 1]!.id;
  }

  /** Get the earliest available cursor, or null if empty. */
  earliest(): string | null {
    this.trim();
    if (this.entries.length === 0) return null;
    return this.entries[0]!.id;
  }

  /** Check whether a specific cursor still exists in the log. */
  has(cursor: string): boolean {
    this.trim();
    const num = Number(cursor);
    return this.entries.some((e) => Number(e.id) === num);
  }

  /** Return one entry by cursor without exposing the mutable stored object. */
  get(cursor: string): EventLogEntry | undefined {
    this.trim();
    const num = Number(cursor);
    const entry = this.entries.find((candidate) => Number(candidate.id) === num);
    return entry ? { ...entry, fields: { ...entry.fields } } : undefined;
  }

  /** Reserve every cursor through the supplied high-water mark. */
  advanceTo(cursor: string): void {
    if (!/^[1-9]\d*$/.test(cursor) || !Number.isSafeInteger(Number(cursor))) return;
    this.seq = Math.max(this.seq, Number(cursor));
  }

  /** Subscribe to new entries after a cursor. Yields entries as they arrive. */
  async *subscribe(after: string, signal?: AbortSignal): AsyncIterable<EventLogEntry> {
    let cursor = after;

    while (!signal?.aborted) {
      // Drain any buffered entries first
      const buffered = this.range(cursor);
      if (buffered.length > 0) {
        for (const entry of buffered) {
          cursor = entry.id;
          yield entry;
        }
        // After draining, loop back to check for more before blocking.
        // This avoids the race where append() fires between range() and once().
        continue;
      }

      // No buffered entries — register listener BEFORE checking range again
      // to close the race window where an append() could slip through.
      try {
        const entry = await this.emitter.onceWithSignal(signal);
        if (Number(entry.id) > Number(cursor)) {
          cursor = entry.id;
          yield entry;
        }
        // After receiving an event, loop back to drain any that arrived
        // between the emit and our next iteration.
      } catch {
        // AbortError or similar — stop iterating
        break;
      }
    }
  }

  /** Trim old entries based on maxLen and retentionMs. */
  trim(): void {
    // Trim by maxLen
    if (this.entries.length > this.maxLen) {
      this.entries.splice(0, this.entries.length - this.maxLen);
    }

    // Trim by retention
    if (this.retentionMs > 0) {
      const cutoff = Date.now() - this.retentionMs;
      let trimCount = 0;
      for (const entry of this.entries) {
        if (entry.ts >= cutoff) break;
        trimCount++;
      }
      if (trimCount > 0) {
        this.entries.splice(0, trimCount);
      }
    }
  }

  /** Number of entries currently in the log. */
  get size(): number {
    this.trim();
    return this.entries.length;
  }

  /** Serializable snapshot used by browser primitives with a persistent Store. */
  snapshot(): EventLogEntry[] {
    this.trim();
    return this.entries.map((entry) => ({ ...entry, fields: { ...entry.fields } }));
  }
}
