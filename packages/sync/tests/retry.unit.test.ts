import { expect, test } from "bun:test";
import { expBackoff, isRetryableTransportError, retry } from "../src/retry";

// ==========================
// expBackoff pure function
// ==========================

test("expBackoff grows exponentially then caps at maxMs", () => {
  expect(expBackoff(1, { baseMs: 100, maxMs: 10_000, jitter: 0 })).toBe(100);
  expect(expBackoff(2, { baseMs: 100, maxMs: 10_000, jitter: 0 })).toBe(200);
  expect(expBackoff(3, { baseMs: 100, maxMs: 10_000, jitter: 0 })).toBe(400);
  expect(expBackoff(4, { baseMs: 100, maxMs: 10_000, jitter: 0 })).toBe(800);
  expect(expBackoff(10, { baseMs: 100, maxMs: 10_000, jitter: 0 })).toBe(10_000);
  expect(expBackoff(20, { baseMs: 100, maxMs: 10_000, jitter: 0 })).toBe(10_000);
});

test("expBackoff with jitter stays within bounds", () => {
  for (let i = 0; i < 20; i++) {
    const v = expBackoff(4, { baseMs: 100, maxMs: 10_000, jitter: 0.2 });
    expect(v).toBeGreaterThanOrEqual(640);
    expect(v).toBeLessThanOrEqual(960);
  }
});

test("expBackoff uses defaults", () => {
  const v1 = expBackoff(1, { jitter: 0 });
  expect(v1).toBe(100);
  const v10 = expBackoff(10, { jitter: 0 });
  expect(v10).toBe(2_000);
});

// ==========================
// retry happy path
// ==========================

test("retry succeeds on first attempt, no after needed", async () => {
  const value = await retry({ run: () => "ok" });
  expect(value).toBe("ok");
});

test("retry with after on success does not reschedule", async () => {
  let runs = 0;
  let afterCalled = false;
  const value = await retry({
    run: () => {
      runs += 1;
      return "ok";
    },
    after: ({ ctx }) => {
      afterCalled = true;
      expect(ctx.data).toBe("ok");
      expect(ctx.error).toBeUndefined();
    },
  });
  expect(value).toBe("ok");
  expect(runs).toBe(1);
  expect(afterCalled).toBe(true);
});

// ==========================
// retry failure paths
// ==========================

test("retry with no after throws the first error immediately", async () => {
  let runs = 0;
  await expect(
    retry({
      run: () => {
        runs += 1;
        throw new Error("fail");
      },
    }),
  ).rejects.toThrow("fail");
  expect(runs).toBe(1);
});

test("retry with after that reschedules retries until success", async () => {
  const seen: number[] = [];
  const value = await retry({
    run: ({ ctx }) => {
      seen.push(ctx.attempt);
      if (ctx.attempt < 3) throw new Error(`fail ${ctx.attempt}`);
      return "done";
    },
    after: ({ ctx }) => {
      if (ctx.error && ctx.attempt < 5) ctx.reschedule({ delayMs: 5 });
    },
  });
  expect(value).toBe("done");
  expect(seen).toEqual([1, 2, 3]);
});

test("retry without reschedule on failure is terminal", async () => {
  let runs = 0;
  const afterAttempts: number[] = [];
  await expect(
    retry({
      run: () => {
        runs += 1;
        throw new Error("nope");
      },
      after: ({ ctx }) => {
        afterAttempts.push(ctx.attempt);
      },
    }),
  ).rejects.toThrow("nope");
  expect(runs).toBe(1);
  expect(afterAttempts).toEqual([1]);
});

test("retry gives up when after stops rescheduling", async () => {
  let runs = 0;
  await expect(
    retry({
      run: () => {
        runs += 1;
        throw new Error("fail");
      },
      after: ({ ctx }) => {
        if (ctx.attempt < 3) ctx.reschedule({ delayMs: 1 });
      },
    }),
  ).rejects.toThrow("fail");
  expect(runs).toBe(3);
});

// ==========================
// ctx.expBackoff in after
// ==========================

test("ctx.expBackoff uses attempt internally", async () => {
  const delays: number[] = [];
  await expect(
    retry({
      run: () => {
        throw new Error("fail");
      },
      after: ({ ctx }) => {
        const delay = ctx.expBackoff({ baseMs: 10, maxMs: 1_000, jitter: 0 });
        delays.push(delay);
        if (ctx.attempt < 4) ctx.reschedule({ delayMs: delay });
      },
    }),
  ).rejects.toThrow();
  expect(delays).toEqual([10, 20, 40, 80]);
});

// ==========================
// signal
// ==========================

test("retry honors abort signal during sleep", async () => {
  const ac = new AbortController();
  const p = retry({
    run: () => {
      throw new Error("retry please");
    },
    after: ({ ctx }) => {
      ctx.reschedule({ delayMs: 10_000 });
    },
    signal: ac.signal,
  });

  await Bun.sleep(20);
  ac.abort();
  await expect(p).rejects.toThrow(/aborted/i);
});

test("retry honors abort signal before run", async () => {
  const ac = new AbortController();
  ac.abort();
  await expect(retry({ run: () => "ok", signal: ac.signal })).rejects.toThrow(/aborted/i);
});

// ==========================
// isRetryableTransportError
// ==========================

test("isRetryableTransportError catches common transport errors by code", () => {
  expect(isRetryableTransportError({ code: "ECONNRESET" })).toBe(true);
  expect(isRetryableTransportError({ code: "ETIMEDOUT" })).toBe(true);
  expect(isRetryableTransportError({ code: "ECONNREFUSED" })).toBe(true);
  expect(isRetryableTransportError({ code: "ENOTFOUND" })).toBe(true);
  expect(isRetryableTransportError({ code: "EOTHER" })).toBe(false);
});

test("isRetryableTransportError catches common transport errors by message", () => {
  expect(isRetryableTransportError(new Error("Connection reset"))).toBe(true);
  expect(isRetryableTransportError(new Error("Socket closed"))).toBe(true);
  expect(isRetryableTransportError(new Error("LOADING Redis is starting"))).toBe(true);
  expect(isRetryableTransportError(new Error("TRYAGAIN"))).toBe(true);
  expect(isRetryableTransportError(new Error("CLUSTERDOWN"))).toBe(true);
  expect(isRetryableTransportError(new Error("some logic error"))).toBe(false);
});

// ==========================
// after error swallow
// ==========================

test("errors in after are swallowed and terminal decision applies", async () => {
  let runs = 0;
  await expect(
    retry({
      run: () => {
        runs += 1;
        throw new Error("original");
      },
      after: () => {
        throw new Error("after error");
      },
    }),
  ).rejects.toThrow("original");
  expect(runs).toBe(1);
});

test("isRetryableTransportError does not misclassify application errors", () => {
  // These used to match on the bare substrings "connection" and "loading",
  // so a user error was replayed silently as a transport blip.
  expect(isRetryableTransportError(new Error("invalid connection string in config"))).toBe(false);
  expect(isRetryableTransportError(new Error("error loading user profile"))).toBe(false);
  expect(isRetryableTransportError(new Error("socket must be a string"))).toBe(false);
  expect(isRetryableTransportError(new Error("network policy denied"))).toBe(false);

  // Genuine transport failures still match.
  expect(isRetryableTransportError(new Error("Connection reset by peer"))).toBe(true);
  expect(isRetryableTransportError(new Error("LOADING Redis is loading the dataset in memory"))).toBe(true);
});
