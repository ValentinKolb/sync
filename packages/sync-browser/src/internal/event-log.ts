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
    // No implicit count cap. topic constructed this without a maxLen and the
    // 50 000 default applied silently, so a high-throughput browser topic
    // dropped events inside its own stated retentionMs window while the server
    // trims by MINID only. Callers that genuinely want a count bound — ephemeral
    // does, via its documented eventMaxLen — pass one.
    this.maxLen = config.maxLen ?? Number.POSITIVE_INFINITY;
    this.retentionMs = config.retentionMs ?? 5 * 60 * 1000;
    this.entries = (config.initialEntries ?? []).map((entry) => ({
      ...entry,
      fields: { ...entry.fields },
    }));
    this.seq = this.entries.reduce((max, entry) => Math.max(max, Number(entry.id) || 0), 0);
    this.trim();
  }

  /** Append an entry and return its cursor ID. */
  append(fields: Record<string, unknown>, hooks?: AppendHooks): string {
    const previousEntries = [...this.entries];
    const previousSeq = this.seq;
    const id = String(++this.seq);
    const entry: EventLogEntry = { id, ts: Date.now(), fields };
    this.entries.push(entry);
    this.trim();
    try {
      hooks?.beforeEmit?.(entry);
    } catch (error) {
      this.entries = previousEntries;
      this.seq = previousSeq;
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
