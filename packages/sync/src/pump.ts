import { redis, sleep } from "bun";
import { randomUUID } from "crypto";
import { expBackoff, type BackoffOptions } from "./retry";
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

const START_SCRIPT = `
  local existing = redis.call("GET", KEYS[1])
  if existing then
    return { existing, 0 }
  end

  redis.call("SET", KEYS[1], ARGV[2])
  redis.call("ZADD", KEYS[2], ARGV[3], ARGV[1])
  return { ARGV[2], 1 }
`;

const CLAIM_SCRIPT = `
  local raw = redis.call("GET", KEYS[1])
  if not raw then
    redis.call("ZREM", KEYS[2], ARGV[1])
    return nil
  end

  local ok, state = pcall(cjson.decode, raw)
  if not ok then
    redis.call("ZREM", KEYS[2], ARGV[1])
    return nil
  end

  if state.state == "completed" or state.state == "failed" or state.state == "canceled" then
    redis.call("ZREM", KEYS[2], ARGV[1])
    return nil
  end

  local now = tonumber(ARGV[3])
  local nextRunAt = tonumber(state.nextRunAt) or 0
  local leaseUntil = tonumber(state.leaseUntil) or 0
  if nextRunAt > now then
    redis.call("ZADD", KEYS[2], tostring(nextRunAt), ARGV[1])
    return nil
  end
  if leaseUntil > now then
    redis.call("ZADD", KEYS[2], tostring(leaseUntil), ARGV[1])
    return nil
  end

  state.state = "running"
  state.nextRunAt = nil
  state.leaseToken = ARGV[2]
  state.leaseUntil = tonumber(ARGV[4])
  state.updatedAt = now
  local encoded = cjson.encode(state)
  redis.call("SET", KEYS[1], encoded)
  redis.call("ZADD", KEYS[2], ARGV[4], ARGV[1])
  return encoded
`;

const HEARTBEAT_SCRIPT = `
  local raw = redis.call("GET", KEYS[1])
  if not raw then return 0 end

  local ok, state = pcall(cjson.decode, raw)
  if not ok or state.state ~= "running" or state.leaseToken ~= ARGV[2] then
    return 0
  end

  state.leaseUntil = tonumber(ARGV[3])
  state.updatedAt = tonumber(ARGV[4])
  redis.call("SET", KEYS[1], cjson.encode(state))
  redis.call("ZADD", KEYS[2], ARGV[3], ARGV[1])
  return 1
`;

const STORE_PAGE_SCRIPT = `
  local raw = redis.call("GET", KEYS[1])
  if not raw then return nil end

  local ok, state = pcall(cjson.decode, raw)
  if not ok or state.state ~= "running" or state.leaseToken ~= ARGV[2] then
    return nil
  end

  state.pageItemsJson = ARGV[3]
  state.pageNextCursorJson = ARGV[4]
  state.pageNextIndex = 0
  state.pageItemCount = tonumber(ARGV[5])
  local stalled = ARGV[4] ~= "null" and ARGV[4] == state.cursorJson
  if not stalled then
    state.failureCount = 0
    state.lastError = nil
  end
  state.updatedAt = tonumber(ARGV[6])
  local encoded = cjson.encode(state)
  redis.call("SET", KEYS[1], encoded)
  return encoded
`;

const CHECKPOINT_SCRIPT = `
  local raw = redis.call("GET", KEYS[1])
  if not raw then return nil end

  local ok, state = pcall(cjson.decode, raw)
  if not ok or state.state ~= "running" or state.leaseToken ~= ARGV[1] or not state.pageItemsJson then
    return nil
  end

  if tonumber(state.pageNextIndex) ~= tonumber(ARGV[2]) then
    return nil
  end

  state.pageNextIndex = tonumber(ARGV[2]) + 1
  state.dispatched = (tonumber(state.dispatched) or 0) + 1
  local stalled = state.pageNextCursorJson ~= "null"
    and state.pageNextCursorJson == state.cursorJson
  if not stalled then
    state.failureCount = 0
    state.lastError = nil
  end
  state.updatedAt = tonumber(ARGV[3])
  local encoded = cjson.encode(state)
  redis.call("SET", KEYS[1], encoded)
  return encoded
`;

const COMMIT_PAGE_SCRIPT = `
  local raw = redis.call("GET", KEYS[1])
  if not raw then return nil end

  local ok, state = pcall(cjson.decode, raw)
  if not ok or state.state ~= "running" or state.leaseToken ~= ARGV[2] or not state.pageItemsJson then
    return nil
  end

  if tonumber(state.pageNextIndex) < tonumber(state.pageItemCount) then
    return nil
  end

  local nextCursorJson = state.pageNextCursorJson
  -- A page that did not move the cursor made no durable progress, even when
  -- dispatch accepted items from it.
  -- Resetting failureCount on it, and scheduling the next run immediately when
  -- delayMs is 0, let a fully-filtered source or an external API returning an
  -- empty page with a nextPageToken hammer Redis and the upstream forever, with
  -- no terminal state and no trace signal. Such a round now counts as a failure
  -- so the configured backoff and maxAttempts apply.
  local stalled = nextCursorJson ~= "null"
    and nextCursorJson == state.cursorJson
  state.cursorJson = nextCursorJson
  state.pageItemsJson = nil
  state.pageNextCursorJson = nil
  state.pageNextIndex = nil
  state.pageItemCount = nil
  state.leaseToken = nil
  state.leaseUntil = nil
  state.updatedAt = tonumber(ARGV[3])

  if nextCursorJson == "null" then
    state.failureCount = 0
    state.lastError = nil
    state.state = "completed"
    state.nextRunAt = nil
    local encoded = cjson.encode(state)
    redis.call("SET", KEYS[1], encoded, "PX", ARGV[5])
    redis.call("ZREM", KEYS[2], ARGV[1])
    return encoded
  end

  if stalled then
    state.failureCount = (tonumber(state.failureCount) or 0) + 1
    state.lastError = "${STALLED_PAGE_ERROR}"
    if state.failureCount >= tonumber(ARGV[7]) then
      state.state = "failed"
      state.nextRunAt = nil
      local encoded = cjson.encode(state)
      redis.call("SET", KEYS[1], encoded, "PX", ARGV[5])
      redis.call("ZREM", KEYS[2], ARGV[1])
      return encoded
    end
  else
    state.failureCount = 0
    state.lastError = nil
  end

  state.state = "waiting"
  state.nextRunAt = stalled and tonumber(ARGV[6]) or tonumber(ARGV[4])
  local encoded = cjson.encode(state)
  redis.call("SET", KEYS[1], encoded)
  redis.call("ZADD", KEYS[2], tostring(state.nextRunAt), ARGV[1])
  return encoded
`;

const FAIL_SCRIPT = `
  local raw = redis.call("GET", KEYS[1])
  if not raw then return nil end

  local ok, state = pcall(cjson.decode, raw)
  if not ok or state.state ~= "running" or state.leaseToken ~= ARGV[2] then
    return nil
  end

  state.failureCount = (tonumber(state.failureCount) or 0) + 1
  state.lastError = ARGV[3]
  state.leaseToken = nil
  state.leaseUntil = nil
  state.updatedAt = tonumber(ARGV[4])

  if state.failureCount >= tonumber(ARGV[6]) then
    state.state = "failed"
    state.nextRunAt = nil
    state.pageItemsJson = nil
    state.pageNextCursorJson = nil
    state.pageNextIndex = nil
    state.pageItemCount = nil
    local encoded = cjson.encode(state)
    redis.call("SET", KEYS[1], encoded, "PX", ARGV[7])
    redis.call("ZREM", KEYS[2], ARGV[1])
    return encoded
  end

  state.state = "waiting"
  state.nextRunAt = tonumber(ARGV[5])
  local encoded = cjson.encode(state)
  redis.call("SET", KEYS[1], encoded)
  redis.call("ZADD", KEYS[2], ARGV[5], ARGV[1])
  return encoded
`;

const RELEASE_SCRIPT = `
  local raw = redis.call("GET", KEYS[1])
  if not raw then return 0 end

  local ok, state = pcall(cjson.decode, raw)
  if not ok or state.state ~= "running" or state.leaseToken ~= ARGV[2] then
    return 0
  end

  state.state = "waiting"
  state.nextRunAt = tonumber(ARGV[3])
  state.leaseToken = nil
  state.leaseUntil = nil
  state.updatedAt = tonumber(ARGV[3])
  redis.call("SET", KEYS[1], cjson.encode(state))
  redis.call("ZADD", KEYS[2], ARGV[3], ARGV[1])
  return 1
`;

const CANCEL_SCRIPT = `
  local raw = redis.call("GET", KEYS[1])
  if not raw then return nil end

  local ok, state = pcall(cjson.decode, raw)
  if not ok then return nil end
  if state.state == "completed" or state.state == "failed" or state.state == "canceled" then
    return { cjson.encode(state), 0 }
  end

  state.state = "canceled"
  state.nextRunAt = nil
  state.leaseToken = nil
  state.leaseUntil = nil
  state.pageItemsJson = nil
  state.pageNextCursorJson = nil
  state.pageNextIndex = nil
  state.pageItemCount = nil
  state.updatedAt = tonumber(ARGV[2])
  local encoded = cjson.encode(state)
  redis.call("SET", KEYS[1], encoded, "PX", ARGV[3])
  redis.call("ZREM", KEYS[2], ARGV[1])
  return { encoded, 1 }
`;

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

type StoredPumpRecord = {
  version: 1;
  key: string;
  inputJson?: string;
  cursorJson: string;
  state: PumpStatus;
  dispatched: number;
  failureCount: number;
  lastError?: string;
  nextRunAt?: number;
  metaJson?: string;
  createdAt: number;
  updatedAt: number;
  pageItemsJson?: string;
  pageNextCursorJson?: string;
  pageNextIndex?: number;
  pageItemCount?: number;
  leaseToken?: string;
  leaseUntil?: number;
};

type ActiveAttempt = {
  member: string;
  token: string;
  abort: AbortController;
};

type ActiveWorker = {
  abort: AbortController;
  current?: ActiveAttempt;
};

// Deliberately not a module-level registry keyed by `${prefix}:${id}`. That
// made a second handle with the same id a silent no-op — its pull, dispatch and
// trace callbacks were never invoked, so after a hot reload stale code kept
// serving live traffic — and either handle's stop() aborted the shared worker.
// Concurrent workers on one id are already resolved by the leaseToken fence.

class LeaseLostError extends Error {
  constructor() {
    super("pump lease lost");
    this.name = "LeaseLostError";
  }
}

const asError = (error: unknown): Error => (error instanceof Error ? error : new Error(String(error)));

const evalScript = async (script: string, keys: string[], args: Array<string | number>): Promise<unknown> =>
  await redis.send("EVAL", [script, String(keys.length), ...keys, ...args.map(String)]);

const assertIdentifier = (value: string, label: string): void => {
  if (!value) throw new Error(`${label} must be non-empty`);
  if (value.length > 256) throw new Error(`${label} too long (max 256 chars)`);
};

const finiteConfig = (value: number | undefined, fallback: number, label: string): number => {
  const resolved = value ?? fallback;
  if (!Number.isFinite(resolved)) throw new Error(`${label} must be finite`);
  return resolved;
};

const positiveSafeIntegerConfig = (value: number | undefined, fallback: number, label: string): number => {
  const resolved = finiteConfig(value, fallback, label);
  if (!Number.isSafeInteger(resolved) || resolved <= 0) {
    throw new RangeError(`${label} must be a positive safe integer`);
  }
  return resolved;
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

const parseState = <Input, Cursor, Item extends PumpItem>(
  raw: unknown,
): StoredPumpState<Input, Cursor, Item> | null => {
  if (typeof raw !== "string") return null;
  try {
    const record = JSON.parse(raw) as StoredPumpRecord;
    const input = record.inputJson === undefined
      ? undefined as Input
      : JSON.parse(record.inputJson) as Input;
    const cursor = JSON.parse(record.cursorJson) as Cursor | null;
    const meta = record.metaJson === undefined
      ? undefined
      : JSON.parse(record.metaJson) as Record<string, unknown>;
    const activePage = record.pageItemsJson === undefined
      ? undefined
      : {
          items: JSON.parse(record.pageItemsJson) as Item[],
          nextCursor: JSON.parse(record.pageNextCursorJson!) as Cursor | null,
          nextIndex: record.pageNextIndex ?? 0,
        };

    return {
      version: 1,
      key: record.key,
      input,
      cursor,
      state: record.state,
      dispatched: record.dispatched,
      failureCount: record.failureCount,
      ...(record.lastError ? { lastError: record.lastError } : {}),
      ...(record.nextRunAt !== undefined ? { nextRunAt: record.nextRunAt } : {}),
      ...(meta ? { meta } : {}),
      createdAt: record.createdAt,
      updatedAt: record.updatedAt,
      ...(activePage ? { activePage } : {}),
      ...(record.leaseToken ? { leaseToken: record.leaseToken } : {}),
      ...(record.leaseUntil !== undefined ? { leaseUntil: record.leaseUntil } : {}),
    };
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
  const dueKey = `${baseKey}:due`;
  // The worker belongs to this handle, not to the id.
  let activeWorker: ActiveWorker | null = null;
  const batchSize = Math.max(1, Math.floor(finiteConfig(config.batchSize, DEFAULT_BATCH_SIZE, "batchSize")));
  const delayMs = Math.max(0, finiteConfig(config.delayMs, DEFAULT_DELAY_MS, "delayMs"));
  const leaseMs = Math.max(50, finiteConfig(config.defaults?.leaseMs, DEFAULT_LEASE_MS, "defaults.leaseMs"));
  const heartbeatMs = Math.max(
    10,
    Math.min(
      finiteConfig(config.defaults?.heartbeatMs, Math.floor(leaseMs / 3), "defaults.heartbeatMs"),
      Math.max(10, Math.floor(leaseMs / 2)),
    ),
  );
  const terminalRetentionMs = positiveSafeIntegerConfig(
    config.defaults?.terminalRetentionMs,
    DEFAULT_TERMINAL_RETENTION_MS,
    "defaults.terminalRetentionMs",
  );
  const pageBytes = Math.max(1, finiteConfig(config.limits?.pageBytes, DEFAULT_PAGE_BYTES, "limits.pageBytes"));
  const maxAttempts = Math.max(
    1,
    Math.floor(finiteConfig(config.retry?.maxAttempts, DEFAULT_MAX_ATTEMPTS, "retry.maxAttempts")),
  );
  const retryBackoff: BackoffOptions = {
    baseMs: finiteConfig(config.retry?.baseMs, DEFAULT_RETRY_BASE_MS, "retry.baseMs"),
    maxMs: finiteConfig(config.retry?.maxMs, DEFAULT_RETRY_MAX_MS, "retry.maxMs"),
    jitter: finiteConfig(config.retry?.jitter, DEFAULT_RETRY_JITTER, "retry.jitter"),
  };

  const memberFor = (key: string): string => encodeURIComponent(key);
  const stateKeyForMember = (member: string): string => `${baseKey}:run:${member}`;
  const stateKeyFor = (key: string): string => stateKeyForMember(memberFor(key));

  const readState = async (key: string): Promise<StoredPumpState<Input, Cursor, Item> | null> =>
    parseState<Input, Cursor, Item>(await redis.get(stateKeyFor(key)));

  const heartbeat = async (member: string, token: string): Promise<boolean> => {
    const now = Date.now();
    const result = await evalScript(
      HEARTBEAT_SCRIPT,
      [stateKeyForMember(member), dueKey],
      [member, token, now + leaseMs, now],
    );
    return Number(result) > 0;
  };

  const release = async (member: string, token: string): Promise<void> => {
    const now = Date.now();
    await evalScript(RELEASE_SCRIPT, [stateKeyForMember(member), dueKey], [member, token, now]);
  };

  const runAttempt = async (
    worker: ActiveWorker,
    member: string,
    token: string,
    initialState: StoredPumpState<Input, Cursor, Item>,
  ): Promise<void> => {
    const attemptAbort = new AbortController();
    const current: ActiveAttempt = { member, token, abort: attemptAbort };
    worker.current = current;

    const onWorkerAbort = (): void => attemptAbort.abort();
    worker.abort.signal.addEventListener("abort", onWorkerAbort, { once: true });

    let heartbeatBusy = false;
    const heartbeatTimer = setInterval(() => {
      if (heartbeatBusy || attemptAbort.signal.aborted) return;
      heartbeatBusy = true;
      void heartbeat(member, token)
        .then((ok) => {
          if (!ok) attemptAbort.abort();
        })
        .catch(() => attemptAbort.abort())
        .finally(() => {
          heartbeatBusy = false;
        });
    }, heartbeatMs);
    (heartbeatTimer as unknown as { unref?: () => void }).unref?.();

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
        const itemsJson = JSON.stringify(activePage.items);
        const nextCursorJson = JSON.stringify(activePage.nextCursor);
        const pageRaw = JSON.stringify({
          items: activePage.items,
          nextCursor: activePage.nextCursor,
        });
        const actualPageBytes = textEncoder.encode(pageRaw).byteLength;
        if (actualPageBytes > pageBytes) {
          throw new Error(`pump page exceeds limit (${pageBytes} bytes)`);
        }

        const stored = parseState<Input, Cursor, Item>(
          await evalScript(
            STORE_PAGE_SCRIPT,
            [stateKeyForMember(member)],
            [member, token, itemsJson, nextCursorJson, activePage.items.length, Date.now()],
          ),
        );
        if (!stored) throw new LeaseLostError();
        state = stored;

        await emitTrace(config.trace, {
          type: "pulled",
          key: state.key,
          itemCount: activePage.items.length,
          durationMs: Date.now() - pullStartedAt,
        });
      }

      while (state.activePage && state.activePage.nextIndex < state.activePage.items.length) {
        if (attemptAbort.signal.aborted) throw new LeaseLostError();
        if (!(await heartbeat(member, token))) throw new LeaseLostError();

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

        const checkpointed = parseState<Input, Cursor, Item>(
          await evalScript(
            CHECKPOINT_SCRIPT,
            [stateKeyForMember(member)],
            [token, index, Date.now()],
          ),
        );
        if (!checkpointed) throw new LeaseLostError();
        state = checkpointed;

        await emitTrace(config.trace, {
          type: "dispatched",
          key: state.key,
          itemKey: item.key,
          dispatched: state.dispatched,
          durationMs: Date.now() - dispatchStartedAt,
        });
      }

      const nextRunAt = Date.now() + delayMs;
      const stalledRunAt =
        Date.now() + Math.max(delayMs, expBackoff((state.failureCount ?? 0) + 1, retryBackoff));
      const committed = parseState<Input, Cursor, Item>(
        await evalScript(
          COMMIT_PAGE_SCRIPT,
          [stateKeyForMember(member), dueKey],
          [member, token, Date.now(), nextRunAt, terminalRetentionMs, stalledRunAt, maxAttempts],
        ),
      );
      if (!committed) throw new LeaseLostError();
      state = committed;

      if (state.state === "completed") {
        await emitTrace(config.trace, {
          type: "finished",
          key: state.key,
          status: "completed",
          dispatched: state.dispatched,
          durationMs: Date.now() - state.createdAt,
        });
      } else if (state.state === "failed") {
        await emitTrace(config.trace, {
          type: "finished",
          key: state.key,
          status: "failed",
          dispatched: state.dispatched,
          durationMs: Date.now() - state.createdAt,
          error: new Error(STALLED_PAGE_ERROR),
        });
      } else if (state.lastError === STALLED_PAGE_ERROR && state.nextRunAt !== undefined) {
        await emitTrace(config.trace, {
          type: "rescheduled",
          key: state.key,
          failureCount: state.failureCount,
          delayMs: Math.max(0, state.nextRunAt - Date.now()),
          error: new Error(STALLED_PAGE_ERROR),
        });
      }
    } catch (caught) {
      const error = asError(caught);
      if (attemptAbort.signal.aborted || error instanceof LeaseLostError) {
        await release(member, token).catch(() => undefined);
        return;
      }

      const nextFailureCount = state.failureCount + 1;
      const retryDelayMs = expBackoff(nextFailureCount, retryBackoff);
      const failed = parseState<Input, Cursor, Item>(
        await evalScript(
          FAIL_SCRIPT,
          [stateKeyForMember(member), dueKey],
          [
            member,
            token,
            error.message,
            Date.now(),
            Date.now() + retryDelayMs,
            maxAttempts,
            terminalRetentionMs,
          ],
        ),
      );
      if (!failed) return;

      if (failed.state === "failed") {
        await emitTrace(config.trace, {
          type: "finished",
          key: failed.key,
          status: "failed",
          dispatched: failed.dispatched,
          durationMs: Date.now() - failed.createdAt,
          error,
        });
      } else {
        await emitTrace(config.trace, {
          type: "rescheduled",
          key: failed.key,
          failureCount: failed.failureCount,
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

  const startWorker = (): void => {
    if (activeWorker) return;

    const worker: ActiveWorker = { abort: new AbortController() };
    activeWorker = worker;

    void (async () => {
      try {
        while (!worker.abort.signal.aborted) {
          try {
            const candidates = await redis.send("ZRANGEBYSCORE", [
              dueKey,
              "-inf",
              String(Date.now()),
              "LIMIT",
              "0",
              "1",
            ]);
            const member = Array.isArray(candidates) && candidates[0] !== undefined
              ? String(candidates[0])
              : null;
            if (!member) {
              await sleep(WORKER_POLL_MS);
              continue;
            }

            const token = randomUUID();
            const now = Date.now();
            const claimed = parseState<Input, Cursor, Item>(
              await evalScript(
                CLAIM_SCRIPT,
                [stateKeyForMember(member), dueKey],
                [member, token, now, now + leaseMs],
              ),
            );
            if (!claimed) {
              await sleep(10);
              continue;
            }

            await runAttempt(worker, member, token, claimed);
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

    const now = Date.now();
    const state: StoredPumpRecord = {
      version: 1,
      key: cfg.key,
      ...(input !== undefined ? { inputJson: JSON.stringify(input) } : {}),
      cursorJson: "null",
      state: "queued",
      dispatched: 0,
      failureCount: 0,
      nextRunAt: now,
      ...(cfg.meta ? { metaJson: JSON.stringify(cfg.meta) } : {}),
      createdAt: now,
      updatedAt: now,
    };
    const stateRaw = JSON.stringify(state);
    const member = memberFor(cfg.key);
    const result = await evalScript(
      START_SCRIPT,
      [stateKeyForMember(member), dueKey],
      [member, stateRaw, now],
    );
    if (!Array.isArray(result)) throw new Error("pump start failed");

    const stored = parseState<Input, Cursor, Item>(result[0]);
    if (!stored) throw new Error("pump state is invalid");
    const created = Number(result[1]) > 0;
    startWorker();

    if (created) {
      await emitTrace(config.trace, {
        type: "submitted",
        key: stored.key,
        input: stored.input,
        ...(stored.meta ? { meta: stored.meta } : {}),
      });
    }
    return toPublicState(stored);
  };

  const get = async (cfg: { key: string }): Promise<PumpState<Input, Cursor> | null> => {
    assertKey(cfg.key);
    const state = await readState(cfg.key);
    return state ? toPublicState(state) : null;
  };

  const cancel = async (cfg: { key: string }): Promise<boolean> => {
    assertKey(cfg.key);
    const member = memberFor(cfg.key);
    const result = await evalScript(
      CANCEL_SCRIPT,
      [stateKeyForMember(member), dueKey],
      [member, Date.now(), terminalRetentionMs],
    );
    if (!Array.isArray(result)) return false;

    const state = parseState<Input, Cursor, Item>(result[0]);
    const changed = Number(result[1]) > 0;
    if (!state || !changed) return false;

    const worker = activeWorker;
    if (worker?.current?.member === member) worker.current.abort.abort();

    await emitTrace(config.trace, {
      type: "finished",
      key: state.key,
      status: "canceled",
      dispatched: state.dispatched,
      durationMs: Date.now() - state.createdAt,
    });
    return true;
  };

  const stop = (): void => {
    const worker = activeWorker;
    if (!worker) return;
    worker.abort.abort();
    worker.current?.abort.abort();
    if (worker.current) {
      void release(worker.current.member, worker.current.token).catch(() => undefined);
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
