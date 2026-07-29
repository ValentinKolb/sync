import { sleep } from "bun";

// ==========================
// Types
// ==========================

export type BackoffOptions = {
  baseMs?: number;
  maxMs?: number;
  jitter?: number;
};

export type RetryCtx = {
  attempt: number;
};

export type RetryAfterCtx<T = unknown> = RetryCtx & {
  data?: T;
  error?: Error;
  reschedule(cfg?: { delayMs?: number }): void;
  expBackoff(cfg?: BackoffOptions): number;
};

export type RetryConfig<T = unknown> = {
  run: (cfg: { ctx: RetryCtx }) => Promise<T> | T;
  after?: (cfg: { ctx: RetryAfterCtx<T> }) => Promise<void> | void;
  signal?: AbortSignal;
};

// ==========================
// Helpers
// ==========================

const asError = (error: unknown): Error => (error instanceof Error ? error : new Error(String(error)));

const createAbortError = (): Error => {
  const error = new Error("retry aborted");
  error.name = "AbortError";
  return error;
};

const parseCode = (error: unknown): string => {
  if (!error || typeof error !== "object") return "";
  const code = (error as { code?: unknown }).code;
  return typeof code === "string" ? code.toUpperCase() : "";
};

const RETRYABLE_REDIS_CODES = new Set(["LOADING", "TRYAGAIN", "CLUSTERDOWN", "MASTERDOWN"]);

export const isRetryableTransportError = (error: unknown): boolean => {
  const code = parseCode(error);
  if (
    code === "ECONNRESET" ||
    code === "ETIMEDOUT" ||
    code === "ECONNREFUSED" ||
    code === "ENOTFOUND" ||
    code === "EPIPE" ||
    code === "EHOSTUNREACH" ||
    code === "ECONNABORTED" ||
    RETRYABLE_REDIS_CODES.has(code)
  ) {
    return true;
  }

  // Anchored on transport vocabulary rather than bare words: "connection" and
  // "loading" match plenty of application error messages, and misclassifying a
  // user error as retryable replays it silently.
  const rawMessage = asError(error).message;
  const message = rawMessage.toLowerCase();
  const responseCode = rawMessage.trimStart().split(/\s+/, 1)[0] ?? "";
  return (
    message.includes("econnreset") ||
    message.includes("econnrefused") ||
    message.includes("epipe") ||
    message.includes("etimedout") ||
    message.includes("connection closed") ||
    message.includes("connection refused") ||
    message.includes("connection reset") ||
    message.includes("connection lost") ||
    message.includes("socket closed") ||
    message.includes("socket hang up") ||
    message.includes("broken pipe") ||
    message.includes("network error") ||
    RETRYABLE_REDIS_CODES.has(responseCode)
  );
};

/**
 * Compute exponential backoff delay in ms for a given attempt (1-indexed).
 * Defaults: baseMs=100, maxMs=2_000, jitter=0.2 (±20%).
 */
export const expBackoff = (attempt: number, cfg?: BackoffOptions): number => {
  const baseMs = Math.max(0, cfg?.baseMs ?? 100);
  const maxMs = Math.max(baseMs, cfg?.maxMs ?? 2_000);
  const jitter = Math.min(1, Math.max(0, cfg?.jitter ?? 0.2));
  const raw = baseMs * 2 ** Math.max(0, attempt - 1);
  const capped = Math.min(maxMs, raw);
  const spread = capped * jitter;
  const jittered = capped + (Math.random() * 2 - 1) * spread;
  return Math.max(0, Math.floor(jittered));
};

const sleepWithSignal = async (delayMs: number, signal?: AbortSignal): Promise<void> => {
  // Yield to the macrotask queue even at zero, so a synchronous run() with an
  // unconditional reschedule cannot starve timers by draining microtasks only.
  if (delayMs <= 0) {
    await sleep(0);
    return;
  }
  if (!signal) {
    await sleep(delayMs);
    return;
  }
  if (signal.aborted) throw createAbortError();

  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, delayMs);

    const onAbort = (): void => {
      clearTimeout(timer);
      signal.removeEventListener("abort", onAbort);
      reject(createAbortError());
    };

    signal.addEventListener("abort", onAbort, { once: true });
  });
};

// ==========================
// retry()
// ==========================

export const retry = async <T>(config: RetryConfig<T>): Promise<T> => {
  let attempt = 0;

  while (true) {
    if (config.signal?.aborted) throw createAbortError();
    attempt += 1;

    const ctx: RetryCtx = { attempt };

    let result: T | undefined;
    let error: Error | undefined;
    try {
      result = await Promise.resolve(config.run({ ctx }));
    } catch (err) {
      error = asError(err);
    }
    if (config.signal?.aborted) throw createAbortError();

    let rescheduleRequested: { delayMs?: number } | null = null;
    const afterCtx: RetryAfterCtx<T> = Object.create(ctx) as RetryAfterCtx<T>;
    if (error) afterCtx.error = error;
    if (!error) afterCtx.data = result;
    afterCtx.reschedule = (rcfg?: { delayMs?: number }): void => {
      rescheduleRequested = { delayMs: rcfg?.delayMs };
    };
    afterCtx.expBackoff = (bcfg?: BackoffOptions): number => expBackoff(attempt, bcfg);

    if (config.after) {
      try {
        await Promise.resolve(config.after({ ctx: afterCtx }));
      } catch {
        // after errors swallowed — fall through to terminal decision
      }
    }
    if (config.signal?.aborted) throw createAbortError();

    if (rescheduleRequested) {
      const delayMs = Math.max(0, (rescheduleRequested as { delayMs?: number }).delayMs ?? 0);
      await sleepWithSignal(delayMs, config.signal);
      continue;
    }

    // Terminal
    if (error) throw error;
    return result as T;
  }
};
