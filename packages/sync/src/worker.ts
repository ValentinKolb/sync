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
  /**
   * Atomically reserve up to `want` slots before fetching. Several pull loops
   * can share one runtime (partitions, schedules); reservations keep the sum
   * of running handlers plus in-flight fetch requests within `capacity`.
   */
  reserve(want: number): number;
  /** Return reservations that were not turned into handlers. */
  releaseReserved(count: number): void;
  /** Resolves when at least one slot is free or the worker is stopping. */
  waitForSlot(): Promise<void>;
  /** Run a handler in one slot. The signal aborts on forced drain. */
  track(fn: (signal: AbortSignal) => Promise<void>, options?: { fromReservation?: boolean }): void;
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
    /** Called when the worker is fully drained or force-aborted. */
    onFinished?: () => void;
  } = {},
): WorkerRuntime => {
  const capacity = options.concurrency ?? 1;
  assertConcurrency(capacity);

  let active = 0;
  let reserved = 0;
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

  const maybeFinish = (): void => {
    if (!stopping || active > 0 || finished) return;
    finished = true;
    options.signal?.removeEventListener("abort", stop);
    hooks.onFinished?.();
  };

  const stop = (): void => {
    if (stopping) return;
    stopping = true;
    resolveStopped!();
    notifyWaiters();
    maybeFinish();
  };

  options.signal?.addEventListener("abort", stop, { once: true });

  const freeSlots = (): number => (stopping ? 0 : Math.max(0, capacity - active - reserved));

  const track: WorkerRuntime["track"] = (fn, trackOptions = {}) => {
    if (trackOptions.fromReservation && reserved > 0) reserved -= 1;
    const controller = new AbortController();
    controllers.add(controller);
    active += 1;
    fn(controller.signal)
      .then(
        () => {
          completed += 1;
        },
        () => {
          // Force-aborted handlers were already bulk-counted in drain().
          if (controller.signal.aborted && !finished) aborted += 1;
          else if (!controller.signal.aborted) completed += 1;
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
      if (active > 0 && !finished) {
        // A handler ignored its abort signal. Count it as aborted and finish
        // the worker so later drains do not wait for it again; its deliveries
        // are recovered by other processes after ackWait expiry.
        aborted += active;
        finished = true;
        options.signal?.removeEventListener("abort", stop);
        hooks.onFinished?.();
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
    freeSlots,
    reserve: (want) => {
      const granted = Math.min(want, freeSlots());
      if (granted <= 0) return 0;
      reserved += granted;
      return granted;
    },
    releaseReserved: (count) => {
      reserved = Math.max(0, reserved - count);
      notifyWaiters();
    },
    waitForSlot: () => {
      if (stopping || freeSlots() > 0) return Promise.resolve();
      return new Promise<void>((resolve) => {
        waiters.add(resolve);
      });
    },
    track,
    stopped,
  };
};
