import { test, expect } from "bun:test";
import {
  retry,
  isRetryableTransportError,
  DEFAULT_RETRY_OPTIONS,
  type RetryOptions,
} from "../src/retry";

// ==========================
// Default options
// ==========================

test("default options are used when none provided", async () => {
  expect(DEFAULT_RETRY_OPTIONS.attempts).toBe(8);
  expect(DEFAULT_RETRY_OPTIONS.minDelayMs).toBe(100);
  expect(DEFAULT_RETRY_OPTIONS.maxDelayMs).toBe(2_000);
  expect(DEFAULT_RETRY_OPTIONS.factor).toBe(2);
  expect(DEFAULT_RETRY_OPTIONS.jitter).toBe(0.2);
  expect(DEFAULT_RETRY_OPTIONS.retryIf).toBe(isRetryableTransportError);

  // When called without options, the defaults are applied internally.
  // Verify by running a function that always succeeds — it should be
  // invoked exactly once with attempt === 1.
  const seen: number[] = [];
  await retry(async (attempt) => {
    seen.push(attempt);
    return "ok";
  });
  expect(seen).toEqual([1]);
});

// ==========================
// Succeeds on first try
// ==========================

test("succeeds on first try", async () => {
  const seen: number[] = [];
  const value = await retry(
    async (attempt) => {
      seen.push(attempt);
      return "first-try";
    },
    { attempts: 5, minDelayMs: 1, maxDelayMs: 5, jitter: 0 },
  );
  expect(value).toBe("first-try");
  expect(seen).toEqual([1]);
});

// ==========================
// Retries and succeeds on later attempt
// ==========================

test("retries and succeeds on later attempt", async () => {
  let attempts = 0;
  const value = await retry(
    async () => {
      attempts += 1;
      if (attempts < 3) {
        const err = new Error("connection reset");
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

// ==========================
// Throws last error after max attempts
// ==========================

test("throws last error after max attempts exhausted", async () => {
  let attempts = 0;
  let thrown: unknown = null;

  try {
    await retry(
      async () => {
        attempts += 1;
        const err = new Error(`fail #${attempts}`);
        (err as Error & { code?: string }).code = "ECONNRESET";
        throw err;
      },
      { attempts: 3, minDelayMs: 1, maxDelayMs: 5, jitter: 0 },
    );
  } catch (error) {
    thrown = error;
  }

  expect(thrown).toBeInstanceOf(Error);
  expect((thrown as Error).message).toBe("fail #3");
  expect(attempts).toBe(3);
});

// ==========================
// Respects retryIf predicate
// ==========================

test("respects retryIf predicate — non-retryable error thrown immediately", async () => {
  let attempts = 0;
  let thrown: unknown = null;

  try {
    await retry(
      async () => {
        attempts += 1;
        throw new Error("validation failed");
      },
      {
        attempts: 5,
        minDelayMs: 1,
        maxDelayMs: 5,
        jitter: 0,
        // Only retry errors that say "transient"
        retryIf: (err) => (err as Error).message.includes("transient"),
      },
    );
  } catch (error) {
    thrown = error;
  }

  expect(thrown).toBeInstanceOf(Error);
  expect((thrown as Error).message).toBe("validation failed");
  expect(attempts).toBe(1); // no retries
});

test("retryIf returning true allows retries", async () => {
  let attempts = 0;
  const value = await retry(
    async () => {
      attempts += 1;
      if (attempts < 3) throw new Error("transient blip");
      return "recovered";
    },
    {
      attempts: 5,
      minDelayMs: 1,
      maxDelayMs: 5,
      jitter: 0,
      retryIf: (err) => (err as Error).message.includes("transient"),
    },
  );
  expect(value).toBe("recovered");
  expect(attempts).toBe(3);
});

// ==========================
// Exponential backoff produces increasing delays
// ==========================

test("exponential backoff produces increasing delays", async () => {
  const timestamps: number[] = [];
  let attempts = 0;

  try {
    await retry(
      async () => {
        timestamps.push(performance.now());
        attempts += 1;
        const err = new Error("network error");
        (err as Error & { code?: string }).code = "ECONNRESET";
        throw err;
      },
      {
        attempts: 4,
        minDelayMs: 20,
        maxDelayMs: 500,
        factor: 2,
        jitter: 0, // no jitter so delays are deterministic
      },
    );
  } catch {
    // expected
  }

  expect(attempts).toBe(4);
  expect(timestamps.length).toBe(4);

  // Compute gaps between attempts
  const gaps: number[] = [];
  for (let i = 1; i < timestamps.length; i++) {
    gaps.push(timestamps[i] - timestamps[i - 1]);
  }

  // With factor=2 and minDelayMs=20, jitter=0:
  //   gap[0] ≈ 20ms  (delay = 20 * 2^0 = 20)
  //   gap[1] ≈ 40ms  (delay = 20 * 2^1 = 40)
  //   gap[2] ≈ 80ms  (delay = 20 * 2^2 = 80)
  // Each gap should be roughly double the previous one.
  expect(gaps[1]).toBeGreaterThan(gaps[0] * 1.5);
  expect(gaps[2]).toBeGreaterThan(gaps[1] * 1.5);
});

// ==========================
// AbortSignal cancels retry
// ==========================

test("AbortSignal already aborted cancels before first attempt", async () => {
  const ac = new AbortController();
  ac.abort();

  let thrown: unknown = null;
  let attempts = 0;

  try {
    await retry(
      async () => {
        attempts += 1;
        return "ok";
      },
      { signal: ac.signal },
    );
  } catch (error) {
    thrown = error;
  }

  expect(thrown).toBeInstanceOf(Error);
  expect((thrown as Error).name).toBe("AbortError");
  expect(attempts).toBe(0); // never even called the function
});

test("AbortSignal cancels during backoff wait", async () => {
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

  // Abort after 20ms — should cancel while sleeping between attempts
  setTimeout(() => ac.abort(), 20);

  try {
    await p;
  } catch (error) {
    thrown = error;
  }

  expect(thrown).toBeInstanceOf(Error);
  expect((thrown as Error).name).toBe("AbortError");
  expect(attempts).toBe(1); // ran once, then aborted during sleep
});

// ==========================
// Jitter adds randomness to delay
// ==========================

test("jitter adds randomness to delay", async () => {
  // Run retry multiple times and collect the gaps; with jitter they
  // should not all be identical.
  const collectGaps = async (): Promise<number[]> => {
    const timestamps: number[] = [];
    try {
      await retry(
        async () => {
          timestamps.push(performance.now());
          const err = new Error("network");
          (err as Error & { code?: string }).code = "ECONNRESET";
          throw err;
        },
        {
          attempts: 4,
          minDelayMs: 30,
          maxDelayMs: 500,
          factor: 2,
          jitter: 1.0, // max jitter
        },
      );
    } catch {
      // expected
    }
    const gaps: number[] = [];
    for (let i = 1; i < timestamps.length; i++) {
      gaps.push(timestamps[i] - timestamps[i - 1]);
    }
    return gaps;
  };

  // Collect gaps from several runs
  const allGaps: number[][] = [];
  for (let i = 0; i < 5; i++) {
    allGaps.push(await collectGaps());
  }

  // With jitter=1.0 the delays have a ±100% spread around the base.
  // It's extremely unlikely that all 5 runs produce identical first-gap
  // values (rounded to ms).
  const firstGaps = allGaps.map((g) => Math.round(g[0]));
  const unique = new Set(firstGaps);
  expect(unique.size).toBeGreaterThanOrEqual(2);
});

// ==========================
// Single attempt (attempts: 1) never retries
// ==========================

test("single attempt (attempts: 1) never retries", async () => {
  let attempts = 0;
  let thrown: unknown = null;

  try {
    await retry(
      async () => {
        attempts += 1;
        throw new Error("boom");
      },
      {
        attempts: 1,
        minDelayMs: 1,
        maxDelayMs: 5,
        jitter: 0,
        retryIf: () => true, // always retryable
      },
    );
  } catch (error) {
    thrown = error;
  }

  expect(thrown).toBeInstanceOf(Error);
  expect((thrown as Error).message).toBe("boom");
  expect(attempts).toBe(1);
});

test("single attempt succeeds without retry", async () => {
  const value = await retry(async () => "only-once", { attempts: 1 });
  expect(value).toBe("only-once");
});

// ==========================
// isRetryableTransportError
// ==========================

test("isRetryableTransportError detects transport errors by code", () => {
  const codes = [
    "ECONNRESET",
    "ETIMEDOUT",
    "ECONNREFUSED",
    "ENOTFOUND",
    "EPIPE",
    "EHOSTUNREACH",
    "ECONNABORTED",
  ];

  for (const code of codes) {
    const err = new Error("something") as Error & { code?: string };
    err.code = code;
    expect(isRetryableTransportError(err)).toBe(true);
  }

  // Lowercase code should also work (parseCode uppercases)
  const lower = new Error("x") as Error & { code?: string };
  lower.code = "econnreset";
  expect(isRetryableTransportError(lower)).toBe(true);
});

test("isRetryableTransportError detects transport errors by message", () => {
  const messages = [
    "econnreset happened",
    "etimedout on call",
    "connection lost",
    "socket hang up",
    "broken pipe detected",
    "network failure",
    "loading chunk failed",
    "tryagain later",
    "clusterdown for maintenance",
  ];

  for (const msg of messages) {
    expect(isRetryableTransportError(new Error(msg))).toBe(true);
  }
});

test("isRetryableTransportError rejects non-transport errors", () => {
  expect(isRetryableTransportError(new Error("validation failed"))).toBe(false);
  expect(isRetryableTransportError(new Error("not found"))).toBe(false);
  expect(isRetryableTransportError(new Error("permission denied"))).toBe(false);
  expect(isRetryableTransportError(null)).toBe(false);
  expect(isRetryableTransportError(undefined)).toBe(false);
  expect(isRetryableTransportError("string error")).toBe(false);
  expect(isRetryableTransportError(42)).toBe(false);
});

test("isRetryableTransportError handles non-Error objects with code", () => {
  const obj = { code: "ECONNRESET", message: "irrelevant" };
  // parseCode reads .code; asError wraps it — code path still triggers
  expect(isRetryableTransportError(obj)).toBe(true);
});
