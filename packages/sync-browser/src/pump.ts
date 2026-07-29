import { randomId } from "./internal/id";
import { resolveStore } from "./internal/shared-state";
import { sleep } from "./internal/sleep";
import { expBackoff, type BackoffOptions } from "./retry";
import { type Store } from "./store";
import { emitTrace, type TraceHandler } from "./trace";

const DAY_MS = 24 * 60 * 60 * 1000;
const DEFAULT_PREFIX = "sync:pump";
const DEFAULT_BATCH_SIZE = 100;
const DEFAULT_DELAY_MS = 0;
const DEFAULT_LEASE_MS = 30_000;
const DEFAULT_TERMINAL_RETENTION_MS = 7 * DAY_MS;
const DEFAULT_PAGE_BYTES = 128 * 1024;
const DEFAULT_MAX_ATTEMPTS = 10;
const DEFAULT_RETRY_BASE_MS = 1_000;
const DEFAULT_RETRY_MAX_MS = 60_000;
const DEFAULT_RETRY_JITTER = 0.2;
const WORKER_POLL_MS = 250;
const MAX_KEY_BYTES = 512;
const STALLED_PAGE_ERROR = "pull returned a page without advancing the cursor";

const textEncoder = new TextEncoder();

export type PumpItem = {
  key: string;
};

export type PumpStatus = "queued" | "running" | "waiting" | "completed" | "failed" | "canceled";

export type PumpState<Input = void, Cursor = unknown> = {
  key: string;
  input: Input;
  cursor: Cursor | null;
  state: PumpStatus;
  dispatched: number;
  failureCount: number;
  lastError?: string;
  nextRunAt?: number;
  meta?: Record<string, unknown>;
  createdAt: number;
  updatedAt: number;
};

export type PumpPullContext<Input, Cursor> = {
  input: Input;
  cursor: Cursor | null;
  limit: number;
  signal: AbortSignal;
};

export type PumpDispatchContext<Input, Item extends PumpItem> = {
  input: Input;
  item: Item;
  signal: AbortSignal;
};

export type PumpPullResult<Cursor, Item extends PumpItem> = {
  items: Item[];
  nextCursor: Cursor | null;
};

export type PumpRetryConfig = BackoffOptions & {
  maxAttempts?: number;
};

export type PumpTraceEvent<Input = void, Cursor = unknown> =
  | { type: "submitted"; key: string; input: Input; meta?: Record<string, unknown> }
  | { type: "started"; key: string; cursor: Cursor | null; failureCount: number }
  | { type: "pulled"; key: string; itemCount: number; durationMs: number }
  | { type: "dispatched"; key: string; itemKey: string; dispatched: number; durationMs: number }
  | { type: "rescheduled"; key: string; failureCount: number; delayMs: number; error: Error }
  | {
      type: "finished";
      key: string;
      status: "completed" | "failed" | "canceled";
      dispatched: number;
      durationMs: number;
      error?: Error;
    };

export type PumpConfig<Input, Cursor, Item extends PumpItem> = {
  id: string;
  prefix?: string;
  batchSize?: number;
  delayMs?: number;
  defaults?: {
    leaseMs?: number;
    heartbeatMs?: number;
    terminalRetentionMs?: number;
  };
  retry?: PumpRetryConfig;
  limits?: {
    pageBytes?: number;
  };
  trace?: TraceHandler<PumpTraceEvent<Input, Cursor>>;
  pull: (ctx: PumpPullContext<Input, Cursor>) => Promise<PumpPullResult<Cursor, Item>> | PumpPullResult<Cursor, Item>;
  dispatch: (ctx: PumpDispatchContext<Input, Item>) => Promise<void> | void;
  /**
   * Persistence backend. Defaults to the process-wide MemoryStore; use
   * LocalStorageStore to resume executions after a page reload.
   */
  store?: Store;
};

export type PumpStartConfig<Input = void> = {
  key: string;
  meta?: Record<string, unknown>;
} & (Input extends void ? { input?: Input } : { input: Input });

export type PumpHandle<Input = void, Cursor = unknown> = {
  id: string;
  start(cfg: PumpStartConfig<Input>): Promise<PumpState<Input, Cursor>>;
  get(cfg: { key: string }): Promise<PumpState<Input, Cursor> | null>;
  cancel(cfg: { key: string }): Promise<boolean>;
  stop(): void;
};

type ActivePage<Cursor, Item extends PumpItem> = {
  items: Item[];
  nextCursor: Cursor | null;
  nextIndex: number;
};

type StoredPumpState<Input, Cursor, Item extends PumpItem> = PumpState<Input, Cursor> & {
  version: 1;
  activePage?: ActivePage<Cursor, Item>;
  leaseToken?: string;
  leaseUntil?: number;
};

type ActiveAttempt = {
  stateKey: string;
  token: string;
  abort: AbortController;
};

type ActiveWorker = {
  abort: AbortController;
  current?: ActiveAttempt;
};

// Workers belong to handles rather than the shared store. This keeps stop()
// local while the lease token fences concurrent workers claiming the same run.

class LeaseLostError extends Error {
  constructor() {
    super("pump lease lost");
    this.name = "LeaseLostError";
  }
}

const asError = (error: unknown): Error => (error instanceof Error ? error : new Error(String(error)));

const assertIdentifier = (value: string, label: string): void => {
  if (!value) throw new Error(`${label} must be non-empty`);
  if (value.length > 256) throw new Error(`${label} too long (max 256 chars)`);
};

const assertKey = (key: string): void => {
  if (!key) throw new Error("key must be non-empty");
  if (textEncoder.encode(key).byteLength > MAX_KEY_BYTES) {
    throw new Error(`key exceeds max length (${MAX_KEY_BYTES} bytes)`);
  }
};

const assertJsonValue = (value: unknown, label: string, seen = new WeakSet<object>()): void => {
  if (value === null || typeof value === "string" || typeof value === "boolean") return;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new Error(`${label} must contain only finite numbers`);
    return;
  }
  if (typeof value !== "object") throw new Error(`${label} must be JSON-serializable`);
  if (seen.has(value)) throw new Error(`${label} must not contain circular references`);
  seen.add(value);

  if (Array.isArray(value)) {
    value.forEach((entry, index) => assertJsonValue(entry, `${label}[${index}]`, seen));
    seen.delete(value);
    return;
  }
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) {
    throw new Error(`${label} must contain only plain objects and arrays`);
  }
  for (const [key, entry] of Object.entries(value)) {
    assertJsonValue(entry, `${label}.${key}`, seen);
  }
  seen.delete(value);
};

const cloneState = <Input, Cursor, Item extends PumpItem>(
  value: unknown,
): StoredPumpState<Input, Cursor, Item> | null => {
  if (!value || typeof value !== "object") return null;
  try {
    return JSON.parse(JSON.stringify(value)) as StoredPumpState<Input, Cursor, Item>;
  } catch {
    return null;
  }
};

const toPublicState = <Input, Cursor, Item extends PumpItem>(
  state: StoredPumpState<Input, Cursor, Item>,
): PumpState<Input, Cursor> => ({
  key: state.key,
  input: state.input,
  cursor: state.cursor,
  state: state.state,
  dispatched: state.dispatched,
  failureCount: state.failureCount,
  ...(state.lastError ? { lastError: state.lastError } : {}),
  ...(state.nextRunAt !== undefined ? { nextRunAt: state.nextRunAt } : {}),
  ...(state.meta ? { meta: state.meta } : {}),
  createdAt: state.createdAt,
  updatedAt: state.updatedAt,
});

export const pump = <Input = void, Cursor = unknown, Item extends PumpItem = PumpItem>(
  config: PumpConfig<Input, Cursor, Item>,
): PumpHandle<Input, Cursor> => {
  assertIdentifier(config.id, "config.id");

  const prefix = config.prefix ?? DEFAULT_PREFIX;
  const baseKey = `${prefix}:${config.id}`;
  const runPrefix = `${baseKey}:run:`;
  const store = resolveStore(config.store);
  const batchSize = Math.max(1, Math.floor(config.batchSize ?? DEFAULT_BATCH_SIZE));
  const delayMs = Math.max(0, config.delayMs ?? DEFAULT_DELAY_MS);
  const leaseMs = Math.max(50, config.defaults?.leaseMs ?? DEFAULT_LEASE_MS);
  const heartbeatMs = Math.max(
    10,
    Math.min(config.defaults?.heartbeatMs ?? Math.floor(leaseMs / 3), Math.max(10, Math.floor(leaseMs / 2))),
  );
  const terminalRetentionMs = Math.max(
    1,
    config.defaults?.terminalRetentionMs ?? DEFAULT_TERMINAL_RETENTION_MS,
  );
  const pageBytes = Math.max(1, config.limits?.pageBytes ?? DEFAULT_PAGE_BYTES);
  const maxAttempts = Math.max(1, Math.floor(config.retry?.maxAttempts ?? DEFAULT_MAX_ATTEMPTS));
  const retryBackoff: BackoffOptions = {
    baseMs: config.retry?.baseMs ?? DEFAULT_RETRY_BASE_MS,
    maxMs: config.retry?.maxMs ?? DEFAULT_RETRY_MAX_MS,
    jitter: config.retry?.jitter ?? DEFAULT_RETRY_JITTER,
  };

  // The worker belongs to this handle, not to its Store or its id.
  let activeWorker: ActiveWorker | null = null;

  const stateKeyFor = (key: string): string => `${runPrefix}${encodeURIComponent(key)}`;

  const readStateByKey = (stateKey: string): StoredPumpState<Input, Cursor, Item> | null =>
    cloneState<Input, Cursor, Item>(store.get(stateKey));

  const readState = (key: string): StoredPumpState<Input, Cursor, Item> | null =>
    readStateByKey(stateKeyFor(key));

  const writeState = (
    stateKey: string,
    state: StoredPumpState<Input, Cursor, Item>,
    ttlMs?: number,
  ): StoredPumpState<Input, Cursor, Item> => {
    const cloned = cloneState<Input, Cursor, Item>(state);
    if (!cloned) throw new Error("pump state is not JSON-serializable");
    store.set(stateKey, cloned, ttlMs);
    return cloned;
  };

  const heartbeat = (stateKey: string, token: string): boolean => {
    const state = readStateByKey(stateKey);
    if (!state || state.state !== "running" || state.leaseToken !== token) return false;
    const now = Date.now();
    state.leaseUntil = now + leaseMs;
    state.updatedAt = now;
    writeState(stateKey, state);
    return true;
  };

  const release = (stateKey: string, token: string): void => {
    const state = readStateByKey(stateKey);
    if (!state || state.state !== "running" || state.leaseToken !== token) return;
    const now = Date.now();
    state.state = "waiting";
    state.nextRunAt = now;
    delete state.leaseToken;
    delete state.leaseUntil;
    state.updatedAt = now;
    writeState(stateKey, state);
  };

  const runAttempt = async (
    worker: ActiveWorker,
    stateKey: string,
    token: string,
    initialState: StoredPumpState<Input, Cursor, Item>,
  ): Promise<void> => {
    const attemptAbort = new AbortController();
    const current: ActiveAttempt = { stateKey, token, abort: attemptAbort };
    worker.current = current;

    const onWorkerAbort = (): void => attemptAbort.abort();
    worker.abort.signal.addEventListener("abort", onWorkerAbort, { once: true });

    const heartbeatTimer = setInterval(() => {
      if (attemptAbort.signal.aborted) return;
      try {
        if (heartbeat(stateKey, token)) return;
      } catch {
        // Treat persistence failures like lease loss.
      }
      if (!attemptAbort.signal.aborted) {
        attemptAbort.abort();
      }
    }, heartbeatMs);

    let state = initialState;
    try {
      await emitTrace(config.trace, {
        type: "started",
        key: state.key,
        cursor: state.cursor,
        failureCount: state.failureCount,
      });

      if (!state.activePage) {
        const pullStartedAt = Date.now();
        const result = await Promise.resolve(
          config.pull({
            input: state.input,
            cursor: state.cursor,
            limit: batchSize,
            signal: attemptAbort.signal,
          }),
        );
        if (attemptAbort.signal.aborted) throw new LeaseLostError();
        if (!result || !Array.isArray(result.items)) {
          throw new Error("pump pull must return { items, nextCursor }");
        }
        assertJsonValue(result.items, "pull.items");
        assertJsonValue(result.nextCursor, "pull.nextCursor");
        for (const item of result.items) {
          if (!item || typeof item.key !== "string" || !item.key) {
            throw new Error("every pump item must have a non-empty string key");
          }
        }

        const activePage: ActivePage<Cursor, Item> = {
          items: result.items,
          nextCursor: result.nextCursor,
          nextIndex: 0,
        };
        const pageRaw = JSON.stringify({
          items: activePage.items,
          nextCursor: activePage.nextCursor,
        });
        if (textEncoder.encode(pageRaw).byteLength > pageBytes) {
          throw new Error(`pump page exceeds limit (${pageBytes} bytes)`);
        }

        const currentState = readStateByKey(stateKey);
        if (!currentState || currentState.state !== "running" || currentState.leaseToken !== token) {
          throw new LeaseLostError();
        }
        currentState.activePage = activePage;
        const stalled =
          activePage.nextCursor !== null &&
          JSON.stringify(activePage.nextCursor) === JSON.stringify(currentState.cursor);
        if (!stalled) {
          currentState.failureCount = 0;
          delete currentState.lastError;
        }
        currentState.updatedAt = Date.now();
        state = writeState(stateKey, currentState);

        await emitTrace(config.trace, {
          type: "pulled",
          key: state.key,
          itemCount: activePage.items.length,
          durationMs: Date.now() - pullStartedAt,
        });
      }

      while (state.activePage && state.activePage.nextIndex < state.activePage.items.length) {
        if (attemptAbort.signal.aborted || !heartbeat(stateKey, token)) throw new LeaseLostError();

        const index = state.activePage.nextIndex;
        const item = state.activePage.items[index]!;
        const dispatchStartedAt = Date.now();
        await Promise.resolve(
          config.dispatch({
            input: state.input,
            item,
            signal: attemptAbort.signal,
          }),
        );
        if (attemptAbort.signal.aborted) throw new LeaseLostError();

        const currentState = readStateByKey(stateKey);
        if (
          !currentState ||
          currentState.state !== "running" ||
          currentState.leaseToken !== token ||
          !currentState.activePage ||
          currentState.activePage.nextIndex !== index
        ) {
          throw new LeaseLostError();
        }
        currentState.activePage.nextIndex = index + 1;
        currentState.dispatched += 1;
        const stalled =
          currentState.activePage.nextCursor !== null &&
          JSON.stringify(currentState.activePage.nextCursor) === JSON.stringify(currentState.cursor);
        if (!stalled) {
          currentState.failureCount = 0;
          delete currentState.lastError;
        }
        currentState.updatedAt = Date.now();
        state = writeState(stateKey, currentState);

        await emitTrace(config.trace, {
          type: "dispatched",
          key: state.key,
          itemKey: item.key,
          dispatched: state.dispatched,
          durationMs: Date.now() - dispatchStartedAt,
        });
      }

      const currentState = readStateByKey(stateKey);
      if (
        !currentState ||
        currentState.state !== "running" ||
        currentState.leaseToken !== token ||
        !currentState.activePage ||
        currentState.activePage.nextIndex < currentState.activePage.items.length
      ) {
        throw new LeaseLostError();
      }

      const nextCursor = currentState.activePage.nextCursor;
      const stalled =
        nextCursor !== null &&
        JSON.stringify(nextCursor) === JSON.stringify(currentState.cursor);
      currentState.cursor = nextCursor;
      delete currentState.activePage;
      delete currentState.leaseToken;
      delete currentState.leaseUntil;
      currentState.updatedAt = Date.now();

      if (nextCursor === null) {
        delete currentState.lastError;
        currentState.failureCount = 0;
        currentState.state = "completed";
        delete currentState.nextRunAt;
        state = writeState(stateKey, currentState, terminalRetentionMs);
        await emitTrace(config.trace, {
          type: "finished",
          key: state.key,
          status: "completed",
          dispatched: state.dispatched,
          durationMs: Date.now() - state.createdAt,
        });
      } else if (stalled) {
        currentState.failureCount += 1;
        currentState.lastError = STALLED_PAGE_ERROR;

        if (currentState.failureCount >= maxAttempts) {
          currentState.state = "failed";
          delete currentState.nextRunAt;
          state = writeState(stateKey, currentState, terminalRetentionMs);
          await emitTrace(config.trace, {
            type: "finished",
            key: state.key,
            status: "failed",
            dispatched: state.dispatched,
            durationMs: Date.now() - state.createdAt,
            error: new Error(STALLED_PAGE_ERROR),
          });
        } else {
          const retryDelayMs = Math.max(
            delayMs,
            expBackoff(currentState.failureCount, retryBackoff),
          );
          currentState.state = "waiting";
          currentState.nextRunAt = Date.now() + retryDelayMs;
          state = writeState(stateKey, currentState);
          await emitTrace(config.trace, {
            type: "rescheduled",
            key: state.key,
            failureCount: state.failureCount,
            delayMs: retryDelayMs,
            error: new Error(STALLED_PAGE_ERROR),
          });
        }
      } else {
        delete currentState.lastError;
        currentState.failureCount = 0;
        currentState.state = "waiting";
        currentState.nextRunAt = Date.now() + delayMs;
        writeState(stateKey, currentState);
      }
    } catch (caught) {
      const error = asError(caught);
      if (attemptAbort.signal.aborted || error instanceof LeaseLostError) {
        release(stateKey, token);
        return;
      }

      const currentState = readStateByKey(stateKey);
      if (!currentState || currentState.state !== "running" || currentState.leaseToken !== token) return;

      currentState.failureCount += 1;
      currentState.lastError = error.message;
      delete currentState.leaseToken;
      delete currentState.leaseUntil;
      currentState.updatedAt = Date.now();
      const retryDelayMs = expBackoff(currentState.failureCount, retryBackoff);

      if (currentState.failureCount >= maxAttempts) {
        currentState.state = "failed";
        delete currentState.nextRunAt;
        delete currentState.activePage;
        state = writeState(stateKey, currentState, terminalRetentionMs);
        await emitTrace(config.trace, {
          type: "finished",
          key: state.key,
          status: "failed",
          dispatched: state.dispatched,
          durationMs: Date.now() - state.createdAt,
          error,
        });
      } else {
        currentState.state = "waiting";
        currentState.nextRunAt = Date.now() + retryDelayMs;
        state = writeState(stateKey, currentState);
        await emitTrace(config.trace, {
          type: "rescheduled",
          key: state.key,
          failureCount: state.failureCount,
          delayMs: retryDelayMs,
          error,
        });
      }
    } finally {
      clearInterval(heartbeatTimer);
      worker.abort.signal.removeEventListener("abort", onWorkerAbort);
      if (worker.current === current) worker.current = undefined;
    }
  };

  const claimNext = (): { stateKey: string; token: string; state: StoredPumpState<Input, Cursor, Item> } | null => {
    const now = Date.now();

    // Earliest due first, as the server's ZRANGEBYSCORE over the due set does.
    // Taking the first eligible key in store order let a run with delayMs 0 —
    // which sets nextRunAt to now after every page, so it looks due on every
    // poll — starve every run that happened to sort after it.
    const candidates: Array<{ stateKey: string; state: StoredPumpState<Input, Cursor, Item>; dueAt: number }> = [];
    for (const stateKey of store.keys(runPrefix)) {
      const state = readStateByKey(stateKey);
      if (!state) continue;
      if (state.state === "completed" || state.state === "failed" || state.state === "canceled") continue;
      const dueAt = state.nextRunAt ?? 0;
      if (dueAt > now) continue;
      if ((state.leaseUntil ?? 0) > now) continue;
      candidates.push({ stateKey, state, dueAt });
    }
    candidates.sort((a, b) => a.dueAt - b.dueAt || (a.stateKey < b.stateKey ? -1 : 1));

    for (const { stateKey, state } of candidates) {
      const token = randomId();
      state.state = "running";
      delete state.nextRunAt;
      state.leaseToken = token;
      state.leaseUntil = now + leaseMs;
      state.updatedAt = now;
      return { stateKey, token, state: writeState(stateKey, state) };
    }
    return null;
  };

  const startWorker = (): void => {
    if (activeWorker) return;

    const worker: ActiveWorker = { abort: new AbortController() };
    activeWorker = worker;

    void (async () => {
      try {
        while (!worker.abort.signal.aborted) {
          try {
            const claimed = claimNext();
            if (!claimed) {
              await sleep(WORKER_POLL_MS);
              continue;
            }
            await runAttempt(worker, claimed.stateKey, claimed.token, claimed.state);
          } catch {
            if (!worker.abort.signal.aborted) await sleep(WORKER_POLL_MS);
          }
        }
      } finally {
        if (activeWorker === worker) activeWorker = null;
      }
    })();
  };

  const start = async (cfg: PumpStartConfig<Input>): Promise<PumpState<Input, Cursor>> => {
    assertKey(cfg.key);
    const input = (cfg as { input?: Input }).input as Input;
    if (input !== undefined) assertJsonValue(input, "start.input");
    if (cfg.meta !== undefined) assertJsonValue(cfg.meta, "start.meta");

    const stateKey = stateKeyFor(cfg.key);
    const existing = readStateByKey(stateKey);
    if (existing) {
      startWorker();
      return toPublicState(existing);
    }

    const now = Date.now();
    const state = writeState(stateKey, {
      version: 1,
      key: cfg.key,
      input,
      cursor: null,
      state: "queued",
      dispatched: 0,
      failureCount: 0,
      nextRunAt: now,
      ...(cfg.meta ? { meta: cfg.meta } : {}),
      createdAt: now,
      updatedAt: now,
    });
    startWorker();

    await emitTrace(config.trace, {
      type: "submitted",
      key: state.key,
      input: state.input,
      ...(state.meta ? { meta: state.meta } : {}),
    });
    return toPublicState(state);
  };

  const get = async (cfg: { key: string }): Promise<PumpState<Input, Cursor> | null> => {
    assertKey(cfg.key);
    const state = readState(cfg.key);
    return state ? toPublicState(state) : null;
  };

  const cancel = async (cfg: { key: string }): Promise<boolean> => {
    assertKey(cfg.key);
    const stateKey = stateKeyFor(cfg.key);
    const state = readStateByKey(stateKey);
    if (!state || state.state === "completed" || state.state === "failed" || state.state === "canceled") {
      return false;
    }

    state.state = "canceled";
    delete state.nextRunAt;
    delete state.leaseToken;
    delete state.leaseUntil;
    delete state.activePage;
    state.updatedAt = Date.now();
    const stored = writeState(stateKey, state, terminalRetentionMs);

    const worker = activeWorker;
    if (worker?.current?.stateKey === stateKey) worker.current.abort.abort();

    await emitTrace(config.trace, {
      type: "finished",
      key: stored.key,
      status: "canceled",
      dispatched: stored.dispatched,
      durationMs: Date.now() - stored.createdAt,
    });
    return true;
  };

  const stop = (): void => {
    const worker = activeWorker;
    if (!worker) return;
    worker.abort.abort();
    if (worker.current) {
      worker.current.abort.abort();
      release(worker.current.stateKey, worker.current.token);
    }
    activeWorker = null;
  };

  startWorker();

  return {
    id: config.id,
    start,
    get,
    cancel,
    stop,
  };
};
