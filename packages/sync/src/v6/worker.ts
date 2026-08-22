// ==========================
// Worker lifecycle
// ==========================

/**
 * A tracked worker handle. `concurrency` always means: maximum number of user
 * handlers started simultaneously by this handle in this process. It is never
 * a global cluster limit — that is `delivery.maxInFlight`.
 */
export type Worker = {
  readonly active: number;
  readonly capacity: number;
  stop(): void;
  drain(options?: { timeoutMs?: number }): Promise<void>;
  [Symbol.asyncDispose](): Promise<void>;
};

export type ProcessOptions = {
  /** Local handler concurrency for this worker handle. Default 1. */
  concurrency?: number;
  signal?: AbortSignal;
};

export type WorkerRuntime = {
  worker: Worker;
  readonly stopping: boolean;
  readonly active: number;
  readonly capacity: number;
  readonly completed: number;
  readonly aborted: number;
  /** Number of currently free handler slots (0 while stopping). */
  freeSlots(): number;
  /** Resolves when at least one slot is free or the worker is stopping. */
  waitForSlot(): Promise<void>;
  /** Run a handler in one slot. The signal aborts on forced drain. */
  track(fn: (signal: AbortSignal) => Promise<void>): void;
  /** Resolves when the worker is stopping (used to interrupt long pulls). */
  readonly stopped: Promise<void>;
};

export const assertConcurrency = (value: number): void => {
  if (!Number.isSafeInteger(value) || value < 1 || value > 1_024) {
    throw new RangeError("concurrency must be an integer between 1 and 1024");
  }
};

export const createWorkerRuntime = (
  options: ProcessOptions,
  hooks: {
    /** Called exactly once when the worker stops pulling (before handlers settle). */
    onStop?: () => void;
    /** Called when the worker is fully drained or force-aborted. */
    onFinished?: () => void;
  } = {},
): WorkerRuntime => {
  const capacity = options.concurrency ?? 1;
  assertConcurrency(capacity);

  let active = 0;
  let completed = 0;
  let aborted = 0;
  let stopping = false;
  let finished = false;
  const controllers = new Set<AbortController>();
  const waiters = new Set<() => void>();
  let resolveStopped: () => void;
  const stopped = new Promise<void>((resolve) => {
    resolveStopped = resolve;
  });

  const notifyWaiters = (): void => {
    for (const waiter of waiters) waiter();
    waiters.clear();
  };

  const stop = (): void => {
    if (stopping) return;
    stopping = true;
    resolveStopped!();
    notifyWaiters();
    hooks.onStop?.();
    maybeFinish();
  };

  const maybeFinish = (): void => {
    if (!stopping || active > 0 || finished) return;
    finished = true;
    hooks.onFinished?.();
  };

  options.signal?.addEventListener("abort", stop, { once: true });

  const track = (fn: (signal: AbortSignal) => Promise<void>): void => {
    const controller = new AbortController();
    controllers.add(controller);
    active += 1;
    fn(controller.signal)
      .then(
        () => {
          completed += 1;
        },
        () => {
          if (controller.signal.aborted) aborted += 1;
          else completed += 1;
        },
      )
      .finally(() => {
        controllers.delete(controller);
        active -= 1;
        notifyWaiters();
        maybeFinish();
      });
  };

  const drain = async (drainOptions: { timeoutMs?: number } = {}): Promise<void> => {
    stop();
    const timeoutMs = drainOptions.timeoutMs ?? 30_000;
    const deadline = Date.now() + timeoutMs;
    while (active > 0 && Date.now() < deadline) {
      await new Promise<void>((resolve) => {
        waiters.add(resolve);
        setTimeout(resolve, Math.min(250, Math.max(1, deadline - Date.now())));
      });
    }
    if (active > 0) {
      for (const controller of controllers) controller.abort();
      // Give aborted handlers a moment to settle (nak, cleanup).
      const settleDeadline = Date.now() + 1_000;
      while (active > 0 && Date.now() < settleDeadline) {
        await new Promise<void>((resolve) => {
          waiters.add(resolve);
          setTimeout(resolve, 50);
        });
      }
    }
  };

  const worker: Worker = {
    get active() {
      return active;
    },
    get capacity() {
      return capacity;
    },
    stop,
    drain,
    [Symbol.asyncDispose]: () => drain(),
  };

  return {
    worker,
    get stopping() {
      return stopping;
    },
    get active() {
      return active;
    },
    get capacity() {
      return capacity;
    },
    get completed() {
      return completed;
    },
    get aborted() {
      return aborted;
    },
    freeSlots: () => (stopping ? 0 : capacity - active),
    waitForSlot: () => {
      if (stopping || active < capacity) return Promise.resolve();
      return new Promise<void>((resolve) => {
        waiters.add(resolve);
      });
    },
    track,
    stopped,
  };
};
