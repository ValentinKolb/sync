import { expect, test } from "bun:test";
import {
  computeRetryDelay,
  isTerminalStatus,
  parseJsonOrNull,
  withTimeout,
} from "../src/internal/job-utils";

test("isTerminalStatus matches terminal job statuses", () => {
  expect(isTerminalStatus("completed")).toBe(true);
  expect(isTerminalStatus("failed")).toBe(true);
  expect(isTerminalStatus("cancelled")).toBe(true);
  expect(isTerminalStatus("timed_out")).toBe(true);
  expect(isTerminalStatus("running")).toBe(false);
});

test("parseJsonOrNull parses valid JSON and rejects invalid JSON", () => {
  const parsed = parseJsonOrNull<{ a: number }>("{\"a\":1}");
  expect(parsed?.a).toBe(1);

  expect(parseJsonOrNull<{ a: number }>("not-json")).toBeNull();
  expect(parseJsonOrNull<{ a: number }>(null)).toBeNull();
});

test("computeRetryDelay supports fixed and exponential backoff", () => {
  expect(computeRetryDelay({ kind: "fixed", baseMs: 250 }, 1)).toBe(250);
  expect(computeRetryDelay({ kind: "exp", baseMs: 100 }, 1)).toBe(100);
  expect(computeRetryDelay({ kind: "exp", baseMs: 100 }, 3)).toBe(400);
  expect(computeRetryDelay({ kind: "exp", baseMs: 100, maxMs: 250 }, 3)).toBe(250);
  expect(computeRetryDelay(undefined, 2)).toBe(0);
  expect(computeRetryDelay({ kind: "fixed", baseMs: 0 }, 2)).toBe(0);
});

test("withTimeout resolves when promise completes in time", async () => {
  const value = await withTimeout(Promise.resolve("ok"), 200);
  expect(value).toBe("ok");
});

test("withTimeout throws timeout error when promise exceeds timeout", async () => {
  let thrown: unknown = null;
  try {
    await withTimeout(
      new Promise<string>((resolve) => {
        setTimeout(() => resolve("late"), 80);
      }),
      20,
    );
  } catch (error) {
    thrown = error;
  }

  expect(thrown).toBeInstanceOf(Error);
  expect((thrown as Error).name).toBe("JobTimeoutError");
});

