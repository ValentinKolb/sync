import { sleep } from "bun";

export type RetryOptions = {
  attempts?: number;
  minDelayMs?: number;
  maxDelayMs?: number;
  factor?: number;
  jitter?: number;
  signal?: AbortSignal;
  retryIf?: (error: unknown) => boolean;
};

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

export const isRetryableTransportError = (error: unknown): boolean => {
  const code = parseCode(error);
  if (
    code === "ECONNRESET" ||
    code === "ETIMEDOUT" ||
    code === "ECONNREFUSED" ||
    code === "ENOTFOUND" ||
    code === "EPIPE" ||
    code === "EHOSTUNREACH" ||
    code === "ECONNABORTED"
  ) {
    return true;
  }

  const message = asError(error).message.toLowerCase();
  return (
    message.includes("econnreset") ||
    message.includes("etimedout") ||
    message.includes("connection") ||
    message.includes("socket") ||
    message.includes("broken pipe") ||
    message.includes("network") ||
    message.includes("loading") ||
    message.includes("tryagain") ||
    message.includes("clusterdown")
  );
};

export const DEFAULT_RETRY_OPTIONS = {
  attempts: 8,
  minDelayMs: 100,
  maxDelayMs: 2_000,
  factor: 2,
  jitter: 0.2,
  retryIf: isRetryableTransportError,
} as const;

const computeDelayMs = (attempt: number, opts: Required<Pick<RetryOptions, "minDelayMs" | "maxDelayMs" | "factor" | "jitter">>): number => {
  const base = opts.minDelayMs * opts.factor ** Math.max(0, attempt - 1);
  const capped = Math.min(opts.maxDelayMs, base);
  const spread = capped * opts.jitter;
  const jittered = capped + (Math.random() * 2 - 1) * spread;
  return Math.max(0, Math.floor(jittered));
};

const sleepWithSignal = async (delayMs: number, signal?: AbortSignal): Promise<void> => {
  if (delayMs <= 0) return;
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

export const retry = async <T>(
  fn: (attempt: number) => Promise<T> | T,
  opts: RetryOptions = {},
): Promise<T> => {
  const attempts = Math.max(1, opts.attempts ?? DEFAULT_RETRY_OPTIONS.attempts);
  const minDelayMs = Math.max(0, opts.minDelayMs ?? DEFAULT_RETRY_OPTIONS.minDelayMs);
  const maxDelayMs = Math.max(minDelayMs, opts.maxDelayMs ?? DEFAULT_RETRY_OPTIONS.maxDelayMs);
  const factor = Math.max(1, opts.factor ?? DEFAULT_RETRY_OPTIONS.factor);
  const jitter = Math.min(1, Math.max(0, opts.jitter ?? DEFAULT_RETRY_OPTIONS.jitter));
  const retryIf = opts.retryIf ?? DEFAULT_RETRY_OPTIONS.retryIf;

  for (let attempt = 1; attempt <= attempts; attempt++) {
    if (opts.signal?.aborted) throw createAbortError();

    try {
      return await fn(attempt);
    } catch (error) {
      if (attempt >= attempts) throw error;
      if (!retryIf(error)) throw error;

      const delayMs = computeDelayMs(attempt, { minDelayMs, maxDelayMs, factor, jitter });
      await sleepWithSignal(delayMs, opts.signal);
    }
  }

  throw new Error("unreachable retry state");
};
