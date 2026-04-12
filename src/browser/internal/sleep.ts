export const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

const createAbortError = (): Error => {
  const error = new Error("sleep aborted");
  error.name = "AbortError";
  return error;
};

export const sleepWithSignal = async (delayMs: number, signal?: AbortSignal): Promise<void> => {
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
