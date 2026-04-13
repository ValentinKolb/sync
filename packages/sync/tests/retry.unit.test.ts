import { expect, test } from "bun:test";
import {
  DEFAULT_RETRY_OPTIONS,
  isRetryableTransportError,
  retry,
} from "../src/retry";

test("DEFAULT_RETRY_OPTIONS exposes stable defaults", () => {
  expect(DEFAULT_RETRY_OPTIONS.attempts).toBe(8);
  expect(DEFAULT_RETRY_OPTIONS.minDelayMs).toBe(100);
  expect(DEFAULT_RETRY_OPTIONS.maxDelayMs).toBe(2000);
  expect(DEFAULT_RETRY_OPTIONS.factor).toBe(2);
  expect(DEFAULT_RETRY_OPTIONS.jitter).toBe(0.2);
});

test("retry succeeds on first attempt", async () => {
  const seen: number[] = [];
  const value = await retry(async (attempt) => {
    seen.push(attempt);
    return "ok";
  });
  expect(value).toBe("ok");
  expect(seen).toEqual([1]);
});

test("retry retries retryable errors and eventually succeeds", async () => {
  let attempts = 0;
  const value = await retry(
    async () => {
      attempts += 1;
      if (attempts < 3) {
        const err = new Error("socket closed");
        (err as Error & { code?: string }).code = "ECONNRESET";
        throw err;
      }
      return 42;
    },
    { attempts: 5, minDelayMs: 1, maxDelayMs: 5, jitter: 0 },
  );
  expect(value).toBe(42);
  expect(attempts).toBe(3);
});

test("retry does not retry non-retryable errors by default", async () => {
  let attempts = 0;
  let thrown: unknown = null;

  try {
    await retry(
      async () => {
        attempts += 1;
        throw new Error("validation failed");
      },
      { attempts: 5, minDelayMs: 1, maxDelayMs: 5, jitter: 0 },
    );
  } catch (error) {
    thrown = error;
  }

  expect(thrown).toBeInstanceOf(Error);
  expect((thrown as Error).message).toBe("validation failed");
  expect(attempts).toBe(1);
});

test("retry honors attempts cap", async () => {
  let attempts = 0;
  let thrown: unknown = null;

  try {
    await retry(
      async () => {
        attempts += 1;
        const err = new Error("connection reset");
        (err as Error & { code?: string }).code = "ECONNRESET";
        throw err;
      },
      { attempts: 3, minDelayMs: 1, maxDelayMs: 5, jitter: 0 },
    );
  } catch (error) {
    thrown = error;
  }

  expect(thrown).toBeInstanceOf(Error);
  expect(attempts).toBe(3);
});

test("retry supports custom retryIf", async () => {
  let attempts = 0;
  const value = await retry(
    async () => {
      attempts += 1;
      if (attempts < 2) throw new Error("custom");
      return "done";
    },
    {
      attempts: 3,
      minDelayMs: 1,
      maxDelayMs: 5,
      jitter: 0,
      retryIf: (error) => (error as Error).message === "custom",
    },
  );

  expect(value).toBe("done");
  expect(attempts).toBe(2);
});

test("retry aborts before first attempt when signal already aborted", async () => {
  const ac = new AbortController();
  ac.abort();

  let thrown: unknown = null;
  try {
    await retry(async () => "ok", { signal: ac.signal });
  } catch (error) {
    thrown = error;
  }

  expect(thrown).toBeInstanceOf(Error);
  expect((thrown as Error).name).toBe("AbortError");
});

test("retry aborts during backoff wait", async () => {
  const ac = new AbortController();
  let attempts = 0;
  let thrown: unknown = null;

  const p = retry(
    async () => {
      attempts += 1;
      const err = new Error("socket closed");
      (err as Error & { code?: string }).code = "ECONNRESET";
      throw err;
    },
    {
      attempts: 10,
      minDelayMs: 1_000,
      maxDelayMs: 1_000,
      jitter: 0,
      signal: ac.signal,
    },
  );

  setTimeout(() => ac.abort(), 20);

  try {
    await p;
  } catch (error) {
    thrown = error;
  }

  expect(thrown).toBeInstanceOf(Error);
  expect((thrown as Error).name).toBe("AbortError");
  expect(attempts).toBe(1);
});

test("isRetryableTransportError matches transport-like failures", () => {
  expect(isRetryableTransportError(new Error("connection closed by peer"))).toBe(true);
  const coded = new Error("x") as Error & { code?: string };
  coded.code = "ETIMEDOUT";
  expect(isRetryableTransportError(coded)).toBe(true);
  expect(isRetryableTransportError(new Error("validation failed"))).toBe(false);
});
