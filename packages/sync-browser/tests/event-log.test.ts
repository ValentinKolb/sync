import { test, expect, beforeEach, describe } from "bun:test";
import { EventLog } from "../src/internal/event-log";

let log: EventLog;

beforeEach(() => {
  log = new EventLog();
});

// ==========================
// append
// ==========================

describe("append", () => {
  test("returns monotonically increasing cursor IDs", () => {
    const id1 = log.append({ a: 1 });
    const id2 = log.append({ b: 2 });
    const id3 = log.append({ c: 3 });
    expect(Number(id1)).toBeLessThan(Number(id2));
    expect(Number(id2)).toBeLessThan(Number(id3));
  });

  test("first cursor is '1'", () => {
    expect(log.append({ x: 1 })).toBe("1");
  });

  test("returns string IDs", () => {
    const id = log.append({ x: 1 });
    expect(typeof id).toBe("string");
  });
});

// ==========================
// range
// ==========================

describe("range", () => {
  test("returns entries after the given cursor", () => {
    const id1 = log.append({ v: 1 });
    const id2 = log.append({ v: 2 });
    const id3 = log.append({ v: 3 });

    const entries = log.range(id1);
    expect(entries).toHaveLength(2);
    expect(entries[0]!.id).toBe(id2);
    expect(entries[1]!.id).toBe(id3);
  });

  test("returns all entries when cursor is '0'", () => {
    log.append({ v: 1 });
    log.append({ v: 2 });
    const entries = log.range("0");
    expect(entries).toHaveLength(2);
  });

  test("returns empty array when cursor is at latest", () => {
    const id = log.append({ v: 1 });
    expect(log.range(id)).toEqual([]);
  });

  test("returns empty array for an empty log", () => {
    expect(log.range("0")).toEqual([]);
  });

  test("limits results with count parameter", () => {
    log.append({ v: 1 });
    log.append({ v: 2 });
    log.append({ v: 3 });
    log.append({ v: 4 });

    const entries = log.range("0", 2);
    expect(entries).toHaveLength(2);
    expect(entries[0]!.fields).toEqual({ v: 1 });
    expect(entries[1]!.fields).toEqual({ v: 2 });
  });

  test("count larger than available entries returns all available", () => {
    log.append({ v: 1 });
    const entries = log.range("0", 100);
    expect(entries).toHaveLength(1);
  });

  test("entries contain id, ts, and fields", () => {
    const before = Date.now();
    log.append({ foo: "bar" });
    const after = Date.now();

    const entries = log.range("0");
    expect(entries).toHaveLength(1);
    const entry = entries[0]!;
    expect(entry.id).toBe("1");
    expect(entry.fields).toEqual({ foo: "bar" });
    expect(entry.ts).toBeGreaterThanOrEqual(before);
    expect(entry.ts).toBeLessThanOrEqual(after);
  });
});

// ==========================
// latest
// ==========================

describe("latest", () => {
  test("returns '0' for an empty log", () => {
    expect(log.latest()).toBe("0");
  });

  test("returns the last cursor after appends", () => {
    log.append({ v: 1 });
    const id = log.append({ v: 2 });
    expect(log.latest()).toBe(id);
  });
});

// ==========================
// earliest
// ==========================

describe("earliest", () => {
  test("returns null for an empty log", () => {
    expect(log.earliest()).toBeNull();
  });

  test("returns the first cursor", () => {
    const id = log.append({ v: 1 });
    log.append({ v: 2 });
    expect(log.earliest()).toBe(id);
  });

  test("updates when the first entry is trimmed", () => {
    const smallLog = new EventLog({ maxLen: 2 });
    smallLog.append({ v: 1 });
    const id2 = smallLog.append({ v: 2 });
    smallLog.append({ v: 3 }); // triggers trim, removes first
    expect(smallLog.earliest()).toBe(id2);
  });
});

// ==========================
// has
// ==========================

describe("has", () => {
  test("returns true for an existing cursor", () => {
    const id = log.append({ v: 1 });
    expect(log.has(id)).toBe(true);
  });

  test("returns false for a non-existent cursor", () => {
    expect(log.has("999")).toBe(false);
  });

  test("returns false for cursor '0'", () => {
    expect(log.has("0")).toBe(false);
  });

  test("returns false after the entry has been trimmed", () => {
    const smallLog = new EventLog({ maxLen: 1 });
    const id1 = smallLog.append({ v: 1 });
    smallLog.append({ v: 2 }); // trims id1
    expect(smallLog.has(id1)).toBe(false);
  });
});

// ==========================
// trim by maxLen
// ==========================

describe("trim by maxLen", () => {
  test("trims oldest entries when maxLen is exceeded", () => {
    const smallLog = new EventLog({ maxLen: 3 });
    smallLog.append({ v: 1 });
    smallLog.append({ v: 2 });
    smallLog.append({ v: 3 });
    smallLog.append({ v: 4 }); // should evict entry 1

    expect(smallLog.size).toBe(3);
    expect(smallLog.has("1")).toBe(false);
    expect(smallLog.has("2")).toBe(true);
    expect(smallLog.has("3")).toBe(true);
    expect(smallLog.has("4")).toBe(true);
  });

  test("keeps exactly maxLen entries", () => {
    const smallLog = new EventLog({ maxLen: 2 });
    for (let i = 0; i < 10; i++) {
      smallLog.append({ i });
    }
    expect(smallLog.size).toBe(2);
  });
});

// ==========================
// trim by retentionMs
// ==========================

describe("trim by retentionMs", () => {
  test("removes entries older than retentionMs on next append", async () => {
    const shortLog = new EventLog({ retentionMs: 50 });
    shortLog.append({ v: "old" });
    expect(shortLog.size).toBe(1);

    await Bun.sleep(80);

    // Appending triggers trim
    shortLog.append({ v: "new" });
    expect(shortLog.size).toBe(1);
    const entries = shortLog.range("0");
    expect(entries[0]!.fields).toEqual({ v: "new" });
  });

  test("retains entries within the retention window", async () => {
    const shortLog = new EventLog({ retentionMs: 200 });
    shortLog.append({ v: 1 });
    await Bun.sleep(30);
    shortLog.append({ v: 2 });
    // Both should still be within the 200ms window
    expect(shortLog.size).toBe(2);
  });
});

// ==========================
// subscribe
// ==========================

describe("subscribe", () => {
  test("yields new entries as they arrive", async () => {
    const received: Record<string, unknown>[] = [];
    const ac = new AbortController();

    const consuming = (async () => {
      for await (const entry of log.subscribe("0", ac.signal)) {
        received.push(entry.fields);
        if (received.length >= 3) break;
      }
    })();

    // Give the subscription a moment to start listening
    await Bun.sleep(10);
    log.append({ v: 1 });
    log.append({ v: 2 });
    log.append({ v: 3 });

    await consuming;
    ac.abort();

    expect(received).toEqual([{ v: 1 }, { v: 2 }, { v: 3 }]);
  });

  test("yields buffered entries first, then waits for new ones", async () => {
    // Append some entries before subscribing
    log.append({ v: "buffered-1" });
    log.append({ v: "buffered-2" });

    const received: Record<string, unknown>[] = [];
    const ac = new AbortController();

    const consuming = (async () => {
      for await (const entry of log.subscribe("0", ac.signal)) {
        received.push(entry.fields);
        if (received.length >= 3) break;
      }
    })();

    // The first two should be yielded immediately from the buffer
    await Bun.sleep(10);
    expect(received).toEqual([{ v: "buffered-1" }, { v: "buffered-2" }]);

    // Now append a new entry
    log.append({ v: "live" });
    await consuming;
    ac.abort();

    expect(received).toEqual([
      { v: "buffered-1" },
      { v: "buffered-2" },
      { v: "live" },
    ]);
  });

  test("stops when abort signal fires", async () => {
    const received: Record<string, unknown>[] = [];
    const ac = new AbortController();

    const consuming = (async () => {
      for await (const entry of log.subscribe("0", ac.signal)) {
        received.push(entry.fields);
      }
    })();

    await Bun.sleep(10);
    log.append({ v: 1 });
    await Bun.sleep(10);

    // Abort the subscription
    ac.abort();
    await consuming;

    // Append more after abort — should not be received
    log.append({ v: 2 });
    await Bun.sleep(10);

    expect(received).toEqual([{ v: 1 }]);
  });

  test("stops immediately if signal is already aborted", async () => {
    const ac = new AbortController();
    ac.abort();

    const received: Record<string, unknown>[] = [];
    for await (const entry of log.subscribe("0", ac.signal)) {
      received.push(entry.fields);
    }

    expect(received).toEqual([]);
  });

  test("respects the after cursor and skips earlier entries", async () => {
    const id1 = log.append({ v: 1 });
    log.append({ v: 2 });
    const id2 = log.append({ v: 3 });

    const received: Record<string, unknown>[] = [];
    const ac = new AbortController();

    const consuming = (async () => {
      for await (const entry of log.subscribe(id1, ac.signal)) {
        received.push(entry.fields);
        if (received.length >= 2) break;
      }
    })();

    await consuming;
    ac.abort();

    // Should have skipped v:1 and received v:2 and v:3
    expect(received).toEqual([{ v: 2 }, { v: 3 }]);
  });
});

// ==========================
// size
// ==========================

describe("size", () => {
  test("starts at zero", () => {
    expect(log.size).toBe(0);
  });

  test("reflects the current entry count", () => {
    log.append({ v: 1 });
    expect(log.size).toBe(1);
    log.append({ v: 2 });
    expect(log.size).toBe(2);
    log.append({ v: 3 });
    expect(log.size).toBe(3);
  });

  test("decreases after trimming", () => {
    const smallLog = new EventLog({ maxLen: 2 });
    smallLog.append({ v: 1 });
    smallLog.append({ v: 2 });
    smallLog.append({ v: 3 });
    expect(smallLog.size).toBe(2);
  });
});

// ==========================
// subscribe race condition
// ==========================

test("subscribe does not lose events during rapid appends", async () => {
  const log = new EventLog();
  const received: string[] = [];
  const ac = new AbortController();

  // Start subscriber
  const sub = (async () => {
    for await (const entry of log.subscribe("0", ac.signal)) {
      received.push(entry.id);
      if (received.length >= 5) { ac.abort(); break; }
    }
  })();

  // Rapid-fire appends with no gaps
  await Bun.sleep(5); // let subscriber start
  for (let i = 0; i < 5; i++) {
    log.append({ i });
  }

  await sub.catch(() => {});
  expect(received.length).toBe(5);
  expect(received).toEqual(["1", "2", "3", "4", "5"]);
});
