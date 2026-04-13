export type JobTerminalStatus = "completed" | "failed" | "cancelled" | "timed_out";

export type RetryBackoff =
  | {
      kind: "fixed" | "exp";
      baseMs: number;
      maxMs?: number;
    }
  | undefined;

export const isTerminalStatus = (status: string): status is JobTerminalStatus => {
  return status === "completed" || status === "failed" || status === "cancelled" || status === "timed_out";
};

export const parseJsonOrNull = <T>(raw: string | null): T | null => {
  if (!raw) return null;
  try {
    return JSON.parse(raw) as T;
  } catch {
    return null;
  }
};

export const createTimeoutError = (): Error => {
  const error = new Error("job execution timed out");
  error.name = "JobTimeoutError";
  return error;
};

export const withTimeout = async <T>(promise: Promise<T>, timeoutMs: number): Promise<T> => {
  let timeoutHandle: ReturnType<typeof setTimeout> | null = null;

  try {
    const timeoutPromise = new Promise<T>((_, reject) => {
      timeoutHandle = setTimeout(() => {
        reject(createTimeoutError());
      }, timeoutMs);
    });

    return await Promise.race([promise, timeoutPromise]);
  } finally {
    if (timeoutHandle) clearTimeout(timeoutHandle);
  }
};

export const computeRetryDelay = (backoff: RetryBackoff, attempt: number): number => {
  if (!backoff || backoff.baseMs <= 0) return 0;
  if (backoff.kind === "fixed") return backoff.baseMs;

  const exp = backoff.baseMs * 2 ** Math.max(0, attempt - 1);
  if (backoff.maxMs === undefined) return exp;
  return Math.min(exp, backoff.maxMs);
};

