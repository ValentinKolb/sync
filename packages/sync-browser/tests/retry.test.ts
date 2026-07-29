import { test, expect } from "bun:test";
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
});

test("expBackoff with jitter stays within bounds", () => {
  for (let i = 0; i < 20; i++) {
    const v = expBackoff(4, { baseMs: 100, maxMs: 10_000, jitter: 0.2 });
    expect(v).toBeGreaterThanOrEqual(640);
    expect(v).toBeLessThanOrEqual(960);
  }
});

test("expBackoff uses defaults", () => {
  expect(expBackoff(1, { jitter: 0 })).toBe(100);
  expect(expBackoff(10, { jitter: 0 })).toBe(2_000);
});

// ==========================
// retry happy path
// ==========================

test("retry succeeds on first attempt", async () => {
  const v = await retry({ run: () => "ok" });
  expect(v).toBe("ok");
});

test("retry with after on success does not reschedule", async () => {
  let runs = 0;
  let afterCalled = false;
  const v = await retry({
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
  expect(v).toBe("ok");
  expect(runs).toBe(1);
  expect(afterCalled).toBe(true);
});

// ==========================
// retry failure
// ==========================

test("retry with no after throws first error immediately", async () => {
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

test("retry reschedules until success", async () => {
  const seen: number[] = [];
  const v = await retry({
    run: ({ ctx }) => {
      seen.push(ctx.attempt);
      if (ctx.attempt < 3) throw new Error(`fail ${ctx.attempt}`);
      return "done";
    },
    after: ({ ctx }) => {
      if (ctx.error && ctx.attempt < 5) ctx.reschedule({ delayMs: 5 });
    },
  });
  expect(v).toBe("done");
  expect(seen).toEqual([1, 2, 3]);
});

test("after without reschedule is terminal", async () => {
  let runs = 0;
  await expect(
    retry({
      run: () => {
        runs += 1;
        throw new Error("nope");
      },
      after: () => {},
    }),
  ).rejects.toThrow("nope");
  expect(runs).toBe(1);
});

// ==========================
// ctx.expBackoff
// ==========================

test("ctx.expBackoff uses attempt internally", async () => {
  const delays: number[] = [];
  await expect(
    retry({
      run: () => {
        throw new Error("fail");
      },
      after: ({ ctx }) => {
        const d = ctx.expBackoff({ baseMs: 10, maxMs: 1_000, jitter: 0 });
        delays.push(d);
        if (ctx.attempt < 4) ctx.reschedule({ delayMs: d });
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
      throw new Error("fail");
    },
    after: ({ ctx }) => ctx.reschedule({ delayMs: 10_000 }),
    signal: ac.signal,
  });
  await Bun.sleep(20);
  ac.abort();
  await expect(p).rejects.toThrow(/aborted/i);
});

test("retry honors an already-aborted signal", async () => {
  const ac = new AbortController();
  ac.abort();
  await expect(retry({ run: () => "ok", signal: ac.signal })).rejects.toMatchObject({ name: "AbortError" });
});

test("retry honors abort while run is active", async () => {
  const ac = new AbortController();
  const pending = retry({
    signal: ac.signal,
    run: async () => {
      await Bun.sleep(20);
      return "late success";
    },
  });

  ac.abort();
  await expect(pending).rejects.toMatchObject({ name: "AbortError" });
});

test("retry honors abort while after is active", async () => {
  const ac = new AbortController();
  const pending = retry({
    signal: ac.signal,
    run: () => "ok",
    after: async () => {
      await Bun.sleep(20);
    },
  });

  await Bun.sleep(5);
  ac.abort();
  await expect(pending).rejects.toMatchObject({ name: "AbortError" });
});

// ==========================
// isRetryableTransportError
// ==========================

test("isRetryableTransportError catches common codes", () => {
  expect(isRetryableTransportError({ code: "ECONNRESET" })).toBe(true);
  expect(isRetryableTransportError({ code: "ETIMEDOUT" })).toBe(true);
  expect(isRetryableTransportError({ code: "EOTHER" })).toBe(false);
});

test("isRetryableTransportError catches common messages", () => {
  expect(isRetryableTransportError(new Error("Connection reset"))).toBe(true);
  expect(isRetryableTransportError(new Error("LOADING"))).toBe(true);
  expect(isRetryableTransportError(new Error("TRYAGAIN later"))).toBe(true);
  expect(isRetryableTransportError(new Error("CLUSTERDOWN unavailable"))).toBe(true);
  expect(isRetryableTransportError(new Error("MASTERDOWN unavailable"))).toBe(true);
  expect(isRetryableTransportError({ code: "LOADING" })).toBe(true);
  expect(isRetryableTransportError(new Error("random"))).toBe(false);
});

test("isRetryableTransportError does not match broad application vocabulary", () => {
  expect(isRetryableTransportError(new Error("invalid connection string in config"))).toBe(false);
  expect(isRetryableTransportError(new Error("error loading user profile"))).toBe(false);
  expect(isRetryableTransportError(new Error("Loading user profile failed"))).toBe(false);
  expect(isRetryableTransportError(new Error("TRYAGAINLater is an application error"))).toBe(false);
  expect(isRetryableTransportError(new Error("CLUSTERDOWNSTREAM failed"))).toBe(false);
  expect(isRetryableTransportError(new Error("MASTERDOWNSTREAM failed"))).toBe(false);
  expect(isRetryableTransportError(new Error("socket must be a string"))).toBe(false);
  expect(isRetryableTransportError(new Error("network policy denied"))).toBe(false);
});

test("zero-delay reschedule yields to timers", async () => {
  let timerFired = false;
  let attempts = 0;
  setTimeout(() => {
    timerFired = true;
  }, 0);

  await retry({
    run: () => {
      attempts += 1;
      return attempts;
    },
    after: ({ ctx }) => {
      if (!timerFired && ctx.attempt < 100) ctx.reschedule({ delayMs: 0 });
    },
  });

  expect(timerFired).toBe(true);
  expect(attempts).toBeLessThan(100);
});

// ==========================
// after error swallow
// ==========================

test("errors in after are swallowed; terminal decision applies", async () => {
  await expect(
    retry({
      run: () => {
        throw new Error("original");
      },
      after: () => {
        throw new Error("after threw");
      },
    }),
  ).rejects.toThrow("original");
});
