import { redis, sleep } from "bun";
import { mutex, type Lock } from "./mutex";
import { expBackoff, type BackoffOptions } from "./retry";
import { assertValidTimeZone, nextCronTimestamp } from "./internal/cron";
import { emitTrace, type TraceHandler } from "./trace";
import {
  markSchedulerControlAccepted,
  markSchedulerControlNotFound,
  markSchedulerControlUnavailable,
  refreshSchedulerControlRequestBinding,
  refreshSchedulerControlHandler,
  registerSchedulerControlIndex,
  removeSchedulerControlHandler,
  schedulerControlQueue,
  type SchedulerControlRequest,
} from "./scheduler-control";

const DEFAULT_PREFIX = "sync:scheduler";
const DEFAULT_LEASE_MS = 5_000;
const DEFAULT_HEARTBEAT_MS = 500;
const DEFAULT_TICK_MS = 500;
const DEFAULT_BATCH_SIZE = 200;
// How many times one manual-run request may be retried before it is reported as
// failed instead of replayed indefinitely.
const CONTROL_MAX_ATTEMPTS = 3;
const MIN_LEASE_MS = 500;
const MIN_HEARTBEAT_MS = 100;

const normalizeMs = (value: number | undefined, fallback: number, minimum: number): number =>
  Math.max(minimum, Math.floor(value !== undefined && Number.isFinite(value) ? value : fallback));

// Upsert: creates or updates the schedule record. Preserves runNumber always;
// preserves nextRunAt/failureCount iff cron/tz unchanged.
// Returns [1 = created | 2 = updated, stored nextRunAt].
const UPSERT_SCRIPT = `
  local raw = redis.call("GET", KEYS[1])
  local incomingRaw = ARGV[1]
  local firstRunAt = tonumber(ARGV[2])
  local scheduleId = ARGV[3]
  local now = tonumber(ARGV[4])

  local created = 1
  local incoming = cjson.decode(incomingRaw)

  if raw then
    created = 0
    local ok, existing = pcall(cjson.decode, raw)
    if ok then
      incoming.createdAt = tonumber(existing.createdAt) or incoming.createdAt
      incoming.runNumber = tonumber(existing.runNumber) or 0

      local shouldResetNext = tostring(existing.cron) ~= tostring(incoming.cron)
        or tostring(existing.tz) ~= tostring(incoming.tz)

      if shouldResetNext then
        incoming.nextRunAt = firstRunAt
        incoming.failureCount = 0
      else
        incoming.nextRunAt = tonumber(existing.nextRunAt) or firstRunAt
        incoming.failureCount = tonumber(existing.failureCount) or 0
      end
    else
      incoming.runNumber = 0
      incoming.nextRunAt = firstRunAt
      incoming.failureCount = 0
    end
  else
    incoming.runNumber = 0
    incoming.nextRunAt = firstRunAt
    incoming.failureCount = 0
  end

  incoming.updatedAt = now

  redis.call("SET", KEYS[1], cjson.encode(incoming))
  redis.call("ZADD", KEYS[2], tostring(incoming.nextRunAt), scheduleId)
  redis.call("SADD", KEYS[3], scheduleId)

  if created == 1 then return {1, tostring(incoming.nextRunAt)} end
  return {2, tostring(incoming.nextRunAt)}
`;

const DELETE_SCRIPT = `
  redis.call("DEL", KEYS[1])
  redis.call("ZREM", KEYS[2], ARGV[1])
  redis.call("SREM", KEYS[3], ARGV[1])
  return 1
`;

// Persist after a dispatch. Every durable scheduler mutation goes through here,
// and it refuses to write unless all four fences hold:
//
//  1. the schedule record still exists — a delete during an in-flight run must
//     not resurrect it as a record the index set no longer lists;
//  2. the leader key still holds our token — a pod whose lease lapsed during a
//     long callback must not clobber the new leader's state;
//  3. the stored runNumber is still the one this run was derived from — a
//     concurrent manual and cron dispatch must not collide or rewind nextRunAt.
//  4. the per-schedule dispatch lease still belongs to this run.
//
// A refusal returns 0 and the run's state change is dropped, which is the safe
// direction: the surviving record belongs to whoever still holds the fence.
//
// Only the counters this run owns are patched onto the stored record, so a
// field written by someone else in the meantime is preserved, and `metaJson`
// stays an opaque string.
const PERSIST_SCRIPT = `
  local raw = redis.call("GET", KEYS[1])
  if not raw then return 0 end

  if ARGV[3] ~= "" and redis.call("GET", KEYS[3]) ~= ARGV[3] then return 0 end
  if redis.call("GET", KEYS[4]) ~= ARGV[5] then return 0 end

  local ok, existing = pcall(cjson.decode, raw)
  if not ok or type(existing) ~= "table" then return 0 end
  if tostring(tonumber(existing.runNumber) or 0) ~= ARGV[2] then return 0 end
  if tostring(existing.revision or "") ~= ARGV[6] then return 0 end

  local patch = cjson.decode(ARGV[4])
  existing.runNumber = patch.runNumber
  existing.nextRunAt = patch.nextRunAt
  existing.failureCount = patch.failureCount
  existing.updatedAt = patch.updatedAt
  if patch.lastError ~= nil and patch.lastError ~= cjson.null then
    existing.lastError = patch.lastError
  else
    existing.lastError = nil
  end

  redis.call("SET", KEYS[1], cjson.encode(existing))
  redis.call("ZADD", KEYS[2], tostring(patch.nextRunAt), ARGV[1])
  return 1
`;

// ==========================
// Types
// ==========================

type StoredSchedule = {
  id: string;
  revision: string;
  cron: string;
  tz: string;
  createdAt: number;
  updatedAt: number;
  nextRunAt: number;
  runNumber: number;
  failureCount: number;
  lastError?: string;
  /**
   * Caller `meta` as an opaque pre-serialized JSON string. Lua copies it and
   * never decodes it: Redis' cjson turns an empty array into an empty object and
   * loses precision past 14 significant digits. Records written by <= 5.8.0
   * carry a decoded `meta` instead and are normalized on read.
   */
  metaJson?: string;
};

export type SchedulerMetrics = {
  isLeader: boolean;
  leaderChanges: number;
  dispatches: number;
  failures: number;
  reschedules: number;
  tickErrors: number;
  /** Durable writes a fence refused: lost lease, deleted schedule, or a concurrent dispatch. */
  staleWrites: number;
  /** Due slots this instance was leader for but had no handler to serve. */
  unservedSlots: number;
  lastTickAt: number | null;
};

export type SchedulerTraceEvent<Result = unknown> =
  | { type: "scheduled"; scheduleId: string; cron: string; tz: string; nextRunAt: number; meta?: Record<string, unknown> }
  | { type: "started"; scheduleId: string; runNumber: number; trigger: "cron" | "manual"; slotTs: number }
  | { type: "succeeded"; scheduleId: string; runNumber: number; data: Result; durationMs: number }
  | { type: "failed"; scheduleId: string; runNumber: number; error: Error; durationMs: number }
  | { type: "rescheduled"; scheduleId: string; runNumber: number; delayMs: number };

export type ScheduleCtx = {
  scheduleId: string;
  slotTs: number;
  runNumber: number;
  failureCount: number;
  /** What caused this run: "cron" for tick dispatch, "manual" for `runNow`. */
  trigger: "cron" | "manual";
  readonly duration: number;
  signal: AbortSignal;
};

export type ScheduleAfterCtx<Result = unknown> = ScheduleCtx & {
  data?: Result;
  error?: Error;
  reschedule(cfg?: { delayMs?: number }): void;
  expBackoff(cfg?: BackoffOptions): number;
  metric: SchedulerMetrics;
};

export type ScheduleConfig<Result = unknown> = {
  id: string;
  cron: string;
  tz?: string;
  meta?: Record<string, unknown>;
  trace?: TraceHandler<SchedulerTraceEvent<Result>>;
  process: (cfg: { ctx: ScheduleCtx }) => Promise<Result> | Result;
  after?: (cfg: { ctx: ScheduleAfterCtx<Result> }) => Promise<void> | void;
};

export type SchedulerConfig = {
  id: string;
  prefix?: string;
  leader?: {
    leaseMs?: number;
    heartbeatMs?: number;
  };
  dispatch?: {
    tickMs?: number;
    batchSize?: number;
  };
};

export type SchedulerInfo = {
  id: string;
  cron: string;
  tz: string;
  createdAt: number;
  updatedAt: number;
  nextRunAt: number;
  runNumber: number;
  failureCount: number;
  lastError?: string;
  meta?: Record<string, unknown>;
};

export type Scheduler = {
  id: string;
  start(): void;
  stop(): Promise<void>;
  create<R>(cfg: ScheduleConfig<R>): Promise<{ created: boolean; updated: boolean }>;
  delete(cfg: { id: string }): Promise<void>;
  runNow(cfg: { id: string }): Promise<void>;
  get(cfg: { id: string }): Promise<SchedulerInfo | null>;
  list(): Promise<SchedulerInfo[]>;
  metric(): SchedulerMetrics;
};

// ==========================
// Internal
// ==========================

type HandlerEntry = {
  process: (cfg: { ctx: ScheduleCtx }) => Promise<unknown> | unknown;
  after?: (cfg: { ctx: ScheduleAfterCtx<unknown> }) => Promise<void> | void;
  trace?: TraceHandler<SchedulerTraceEvent<unknown>>;
};

const asError = (error: unknown): Error => (error instanceof Error ? error : new Error(String(error)));

const parseSchedule = (raw: string | null): StoredSchedule | null => {
  if (!raw) return null;
  try {
    const value = JSON.parse(raw) as Record<string, unknown>;
    if (typeof value.id !== "string" || !value.id) return null;
    if (typeof value.cron !== "string" || !value.cron) return null;
    if (typeof value.tz !== "string" || !value.tz) return null;
    if (typeof value.nextRunAt !== "number" || !Number.isFinite(value.nextRunAt)) return null;
    return {
      id: value.id,
      revision: typeof value.revision === "string" ? value.revision : "",
      cron: value.cron,
      tz: value.tz,
      createdAt: Number(value.createdAt) || Date.now(),
      updatedAt: Number(value.updatedAt) || Date.now(),
      nextRunAt: Number(value.nextRunAt),
      runNumber: Number(value.runNumber) || 0,
      failureCount: Number(value.failureCount) || 0,
      ...(typeof value.lastError === "string" ? { lastError: value.lastError } : {}),
      ...(typeof value.metaJson === "string"
        ? { metaJson: value.metaJson }
        : value.meta && typeof value.meta === "object"
          ? { metaJson: JSON.stringify(value.meta) }
          : {}),
    };
  } catch {
    return null;
  }
};

const parseMeta = (metaJson: string): Record<string, unknown> | undefined => {
  try {
    const value = JSON.parse(metaJson) as unknown;
    return value && typeof value === "object" ? (value as Record<string, unknown>) : undefined;
  } catch {
    return undefined;
  }
};

const asInfo = (schedule: StoredSchedule): SchedulerInfo => ({
  id: schedule.id,
  cron: schedule.cron,
  tz: schedule.tz,
  createdAt: schedule.createdAt,
  updatedAt: schedule.updatedAt,
  nextRunAt: schedule.nextRunAt,
  runNumber: schedule.runNumber,
  failureCount: schedule.failureCount,
  ...(schedule.lastError ? { lastError: schedule.lastError } : {}),
  ...(schedule.metaJson !== undefined ? { meta: parseMeta(schedule.metaJson) } : {}),
});

// ==========================
// Factory
// ==========================

export const scheduler = (config: SchedulerConfig): Scheduler => {
  const prefix = config.prefix ?? DEFAULT_PREFIX;
  const leaseMs = normalizeMs(config.leader?.leaseMs, DEFAULT_LEASE_MS, MIN_LEASE_MS);
  const heartbeatMs = Math.min(
    normalizeMs(config.leader?.heartbeatMs, DEFAULT_HEARTBEAT_MS, MIN_HEARTBEAT_MS),
    Math.floor(leaseMs / 3),
  );
  const tickMs = normalizeMs(config.dispatch?.tickMs, DEFAULT_TICK_MS, 50);
  const batchSize = Math.max(
    1,
    Math.floor(Number.isFinite(config.dispatch?.batchSize) ? config.dispatch!.batchSize! : DEFAULT_BATCH_SIZE),
  );
  const controlHandlerTtlMs = Math.max(5_000, tickMs * 4);
  const controlHeartbeatMs = Math.max(500, Math.floor(controlHandlerTtlMs / 3));

  const scheduleKey = (id: string): string => `${prefix}:${config.id}:schedule:${id}`;
  const dueKey = `${prefix}:${config.id}:due`;
  const indexKey = `${prefix}:${config.id}:index`;
  const instanceId = crypto.randomUUID();

  const leaderMutex = mutex({
    id: `${config.id}:leader`,
    prefix: `${prefix}:leader`,
    defaultTtl: leaseMs,
    retryCount: 0,
  });
  const dispatchMutex = mutex({
    id: `${config.id}:dispatch`,
    prefix: `${prefix}:dispatch`,
    defaultTtl: leaseMs,
    retryCount: 0,
  });

  const handlers = new Map<string, HandlerEntry>();
  const controlQueues = new Map<string, ReturnType<typeof schedulerControlQueue>>();
  const metrics: SchedulerMetrics = {
    isLeader: false,
    leaderChanges: 0,
    dispatches: 0,
    failures: 0,
    reschedules: 0,
    tickErrors: 0,
    staleWrites: 0,
    unservedSlots: 0,
    lastTickAt: null,
  };

  let running = false;
  let loopPromise: Promise<void> | null = null;
  let heartbeatPromise: Promise<void> | null = null;
  let controlHeartbeatPromise: Promise<void> | null = null;
  let controlRefreshPromise: Promise<void> | null = null;
  let dispatchGeneration = 0;
  // Controllers of the callbacks currently running, so stop() can cancel them.
  const activeRuns = new Set<AbortController>();
  let currentLeaderLock: Lock | null = null;
  let lastHeartbeatAt = 0;

  const controlQueueForSchedule = (scheduleId: string): ReturnType<typeof schedulerControlQueue> => {
    const existing = controlQueues.get(scheduleId);
    if (existing) return existing;
    const created = schedulerControlQueue(prefix, config.id, scheduleId);
    controlQueues.set(scheduleId, created);
    return created;
  };

  const setLeader = (next: boolean): void => {
    if (metrics.isLeader === next) return;
    metrics.isLeader = next;
    metrics.leaderChanges += 1;
  };

  const tryAcquireLeadership = async (): Promise<void> => {
    if (currentLeaderLock) return;
    const acquired = await leaderMutex.acquire("active", leaseMs);
    if (!acquired) return;
    currentLeaderLock = acquired;
    lastHeartbeatAt = Date.now();
    setLeader(true);
  };

  const maintainLeadership = async (): Promise<void> => {
    const lock = currentLeaderLock;
    if (!lock) return;
    const nowMs = Date.now();
    if (nowMs - lastHeartbeatAt < heartbeatMs) return;
    lastHeartbeatAt = nowMs;
    const ok = await leaderMutex.extend(lock, leaseMs);
    if (!ok && currentLeaderLock === lock) {
      currentLeaderLock = null;
      setLeader(false);
    }
  };

  const relinquishLeadership = async (): Promise<void> => {
    if (!currentLeaderLock) return;
    try {
      await leaderMutex.release(currentLeaderLock);
    } catch {
      // best effort
    }
    currentLeaderLock = null;
    setLeader(false);
  };

  /**
   * @param expectedRunNumber the runNumber this run was derived from
   * @param requireLeadership cron dispatch must own the lease; a manual run,
   *   which legitimately happens on any pod holding the handler, must not
   * @returns false when a fence refused the write
   */
  const persist = async (
    schedule: StoredSchedule,
    expectedRunNumber: number,
    requireLeadership: boolean,
    dispatchLock: Lock,
  ): Promise<boolean> => {
    const leaderToken = requireLeadership ? (currentLeaderLock?.value ?? null) : "";
    if (leaderToken === null) return false;

    const result = await redis.send("EVAL", [
      PERSIST_SCRIPT,
      "4",
      scheduleKey(schedule.id),
      dueKey,
      currentLeaderLock?.resource ?? `${prefix}:leader:${config.id}:leader:active`,
      dispatchLock.resource,
      schedule.id,
      String(expectedRunNumber),
      leaderToken,
      JSON.stringify({
        runNumber: schedule.runNumber,
        nextRunAt: schedule.nextRunAt,
        failureCount: schedule.failureCount,
        updatedAt: schedule.updatedAt,
        lastError: schedule.lastError ?? null,
      }),
      dispatchLock.value,
      schedule.revision,
    ]);
    const persisted = Number(result) > 0;
    if (!persisted) metrics.staleWrites += 1;
    return persisted;
  };

  // One dispatch per schedule at a time in this process, so a manual run and a
  // cron run of the same schedule cannot interleave their read-modify-writes.
  // One entry per schedule id, replaced rather than appended, so this is bounded
  // by the number of schedules this instance knows about.
  const dispatchChains = new Map<string, Promise<unknown>>();
  const serializeDispatch = (scheduleId: string, run: () => Promise<void>): Promise<void> => {
    const previous = dispatchChains.get(scheduleId) ?? Promise.resolve();
    const next = previous.then(run, run);
    const settled = next.catch(() => {});
    dispatchChains.set(scheduleId, settled);
    void settled.then(() => {
      if (dispatchChains.get(scheduleId) === settled) {
        dispatchChains.delete(scheduleId);
      }
    });
    return next;
  };

  // Run a single schedule: increment runNumber, invoke process + after, update state, persist.
  // `trigger` records what caused this run. It also controls cron advancement:
  // "cron" advances nextRunAt to the next cron slot when user does not reschedule;
  // "manual" leaves nextRunAt unchanged (regular cron continues unaffected by runNow).
  const dispatchOne = async (
    schedule: StoredSchedule,
    handler: HandlerEntry,
    trigger: "cron" | "manual",
    dispatchLock: Lock,
  ): Promise<void> => {
    const advanceCron = trigger === "cron";
    const slotTs = schedule.nextRunAt;
    // The value the terminal write is fenced against.
    const expectedRunNumber = schedule.runNumber;
    schedule.runNumber += 1;
    const runNumber = schedule.runNumber;
    const failureCountBefore = schedule.failureCount;
    const startedAt = Date.now();
    const jobAc = new AbortController();
    activeRuns.add(jobAc);
    let dispatchLeaseLost = false;
    let heartbeatTail = Promise.resolve();
    const dispatchHeartbeat = setInterval(() => {
      heartbeatTail = heartbeatTail.then(async () => {
        if (dispatchLeaseLost || jobAc.signal.aborted) return;
        try {
          if (await dispatchMutex.extend(dispatchLock, leaseMs)) return;
        } catch {
          // A transport failure makes ownership uncertain. Fail closed.
        }
        dispatchLeaseLost = true;
        jobAc.abort();
      });
    }, Math.max(50, Math.floor(heartbeatMs / 2)));

    try {
      const makeCtx = (): ScheduleCtx => {
        const ctx = {
          scheduleId: schedule.id,
          slotTs,
          runNumber,
          failureCount: failureCountBefore,
          trigger,
          signal: jobAc.signal,
        } as ScheduleCtx;
        Object.defineProperty(ctx, "duration", {
          get: () => Date.now() - startedAt,
          enumerable: true,
        });
        return ctx;
      };

      const ctx = makeCtx();

      await emitTrace(handler.trace, {
        type: "started",
        scheduleId: schedule.id,
        runNumber,
        trigger,
        slotTs,
      });

      let result: unknown;
      let error: Error | undefined;
      try {
        result = await Promise.resolve(handler.process({ ctx }));
      } catch (err) {
        error = asError(err);
      }

      if (dispatchLeaseLost) {
        throw new Error(`scheduler dispatch lease lost for ${schedule.id}`);
      }
      if (jobAc.signal.aborted) {
        throw new Error(`scheduler dispatch stopped for ${schedule.id}`);
      }

      if (error) {
        await emitTrace(handler.trace, {
          type: "failed",
          scheduleId: schedule.id,
          runNumber,
          error,
          durationMs: Date.now() - startedAt,
        });
      } else {
        await emitTrace(handler.trace, {
          type: "succeeded",
          scheduleId: schedule.id,
          runNumber,
          data: result,
          durationMs: Date.now() - startedAt,
        });
      }

      let rescheduleRequested: { delayMs?: number } | null = null;
      const afterCtx: ScheduleAfterCtx<unknown> = Object.create(ctx) as ScheduleAfterCtx<unknown>;
      if (error) afterCtx.error = error;
      if (!error) afterCtx.data = result;
      afterCtx.reschedule = (rcfg?: { delayMs?: number }): void => {
        rescheduleRequested = { delayMs: rcfg?.delayMs };
      };
      afterCtx.expBackoff = (bcfg?: BackoffOptions): number => expBackoff(failureCountBefore + 1, bcfg);
      afterCtx.metric = metrics;

      if (handler.after) {
        try {
          await Promise.resolve(handler.after({ ctx: afterCtx }));
        } catch {
          // after errors swallowed
        }
      }

      if (dispatchLeaseLost) {
        throw new Error(`scheduler dispatch lease lost for ${schedule.id}`);
      }
      if (jobAc.signal.aborted) {
        throw new Error(`scheduler dispatch stopped for ${schedule.id}`);
      }

      if (error) {
        schedule.failureCount += 1;
        schedule.lastError = error.message;
      } else {
        schedule.failureCount = 0;
        delete schedule.lastError;
      }

      let traceRescheduled: { delayMs: number } | null = null;
      if (rescheduleRequested) {
        const delayMs = Math.max(0, (rescheduleRequested as { delayMs?: number }).delayMs ?? 0);
        schedule.nextRunAt = Date.now() + delayMs;
        traceRescheduled = { delayMs };
      } else if (advanceCron) {
        schedule.nextRunAt = nextCronTimestamp(schedule.cron, schedule.tz, Date.now());
      }
      // else: nextRunAt unchanged (runNow without reschedule — cron schedule continues as before)

      schedule.updatedAt = Date.now();

      const persisted = await persist(schedule, expectedRunNumber, advanceCron, dispatchLock);
      if (!persisted) {
        throw new Error(`scheduler dispatch state changed for ${schedule.id}`);
      }

      if (error) metrics.failures += 1;
      else metrics.dispatches += 1;
      if (traceRescheduled) metrics.reschedules += 1;

      if (traceRescheduled) {
        await emitTrace(handler.trace, {
          type: "rescheduled",
          scheduleId: schedule.id,
          runNumber,
          delayMs: traceRescheduled.delayMs,
        });
      }
    } finally {
      clearInterval(dispatchHeartbeat);
      await heartbeatTail;
      jobAc.abort();
      activeRuns.delete(jobAc);
    }
  };

  const dispatchSchedule = async (
    scheduleId: string,
    handler: HandlerEntry,
    trigger: "cron" | "manual",
    options?: {
      onAcquired?: () => Promise<void> | void;
      shouldContinue?: () => boolean;
    },
  ): Promise<void> => {
    const generation = dispatchGeneration;
    await serializeDispatch(scheduleId, async () => {
      let dispatchLock = await dispatchMutex.acquire(scheduleId, leaseMs);
      if (trigger === "cron" && !dispatchLock) return;
      while (!dispatchLock) {
        if (generation !== dispatchGeneration || (options?.shouldContinue && !options.shouldContinue())) {
          throw new Error(`scheduler dispatch stopped for ${scheduleId}`);
        }
        await sleep(Math.max(25, Math.min(100, Math.floor(heartbeatMs / 2))));
        dispatchLock = await dispatchMutex.acquire(scheduleId, leaseMs);
      }

      let preparationLeaseLost = false;
      let preparationHeartbeatTail = Promise.resolve();
      const preparationHeartbeat = setInterval(() => {
        preparationHeartbeatTail = preparationHeartbeatTail.then(async () => {
          if (preparationLeaseLost) return;
          try {
            if (await dispatchMutex.extend(dispatchLock, leaseMs)) return;
          } catch {
            // Ownership is uncertain after a transport error.
          }
          preparationLeaseLost = true;
        });
      }, Math.max(50, Math.floor(heartbeatMs / 2)));

      try {
        const schedule = parseSchedule(await redis.get(scheduleKey(scheduleId)));
        if (!schedule) throw new Error(`runNow: schedule ${scheduleId} not found`);
        if (trigger === "cron" && (!currentLeaderLock || schedule.nextRunAt > Date.now())) return;
        if (
          generation !== dispatchGeneration
          || (options?.shouldContinue && !options.shouldContinue())
        ) {
          throw new Error(`scheduler dispatch stopped for ${scheduleId}`);
        }
        if (preparationLeaseLost || !(await dispatchMutex.extend(dispatchLock, leaseMs))) {
          throw new Error(`scheduler dispatch lease lost for ${scheduleId}`);
        }
        await options?.onAcquired?.();
        clearInterval(preparationHeartbeat);
        await preparationHeartbeatTail;
        if (preparationLeaseLost || !(await dispatchMutex.extend(dispatchLock, leaseMs))) {
          throw new Error(`scheduler dispatch lease lost for ${scheduleId}`);
        }
        await dispatchOne(schedule, handler, trigger, dispatchLock);
      } finally {
        clearInterval(preparationHeartbeat);
        await preparationHeartbeatTail;
        await dispatchMutex.release(dispatchLock);
      }
    });
  };

  const dispatchDue = async (): Promise<void> => {
    const nowMs = Date.now();
    const dueIdsRaw = await redis.send("ZRANGEBYSCORE", [
      dueKey,
      "-inf",
      String(nowMs),
      "LIMIT",
      "0",
      String(batchSize),
    ]);
    if (!Array.isArray(dueIdsRaw) || dueIdsRaw.length === 0) return;

    const dueIds = dueIdsRaw.map((v) => String(v));

    for (const scheduleId of dueIds) {
      if (!currentLeaderLock) break;

      const raw = await redis.get(scheduleKey(scheduleId));
      const schedule = parseSchedule(raw);
      if (!schedule) {
        // Broken record: clean up
        await redis.send("EVAL", [DELETE_SCRIPT, "3", scheduleKey(scheduleId), dueKey, indexKey, scheduleId]);
        continue;
      }

      if (schedule.nextRunAt > nowMs) continue;

      const handler = handlers.get(scheduleId);
      if (!handler) {
        // Do not advance a slot this pod cannot serve. Advancing made leadership
        // a black hole: leases are sticky, so the same handler-less leader kept
        // winning and skipping slot after slot while `get()`/`list()` showed a
        // healthy schedule with a frozen runNumber. Step down instead, so a pod
        // that did register the handler can take over and actually run it.
        metrics.unservedSlots += 1;
        await relinquishLeadership();
        return;
      }

      await dispatchSchedule(scheduleId, handler, "cron");
    }
  };

  const refreshControlHandlers = async (): Promise<void> => {
    await Promise.all(
      Array.from(handlers.keys()).map((scheduleId) =>
        refreshSchedulerControlHandler({
          prefix,
          schedulerId: config.id,
          scheduleId,
          instanceId,
          ttlMs: controlHandlerTtlMs,
        }),
      ),
    );
  };

  const removeControlHandlers = async (): Promise<void> => {
    await Promise.all(
      Array.from(handlers.keys()).map((scheduleId) =>
        removeSchedulerControlHandler({
          prefix,
          schedulerId: config.id,
          scheduleId,
          instanceId,
        }),
      ),
    );
  };

  const dispatchControlRequests = async (): Promise<void> => {
    let dispatched = 0;
    const controlLeaseMs = 30_000;
    for (const [scheduleId, handler] of handlers) {
      if (dispatched >= batchSize) break;
      const message = await controlQueueForSchedule(scheduleId).recv({
        wait: false,
        leaseMs: controlLeaseMs,
        consumerId: instanceId,
      });
      if (!message) continue;

      const request: SchedulerControlRequest = message.data;
      if (!(await refreshSchedulerControlRequestBinding(prefix, request))) {
        await message.ack();
        dispatched += 1;
        continue;
      }
      const raw = await redis.get(scheduleKey(scheduleId));
      const schedule = parseSchedule(raw);
      if (!schedule) {
        await markSchedulerControlNotFound(
          prefix,
          request,
          `schedulerControl.runNow: schedule ${request.schedulerId}/${request.scheduleId} not found`,
        );
        await message.ack();
        dispatched += 1;
        continue;
      }

      if (!handlers.has(scheduleId)) {
        await markSchedulerControlUnavailable(
          prefix,
          request,
          `schedulerControl.runNow: no live handler for schedule ${request.schedulerId}/${request.scheduleId}`,
        );
        await message.ack();
        dispatched += 1;
        continue;
      }

      const touchTimer = setInterval(() => {
        // Without the catch a transient Redis error here becomes an unhandled
        // rejection, which terminates the process under Bun and Node defaults.
        void message.touch({ leaseMs: controlLeaseMs }).catch(() => {});
        void refreshSchedulerControlRequestBinding(prefix, request).catch(() => {});
      }, Math.floor(controlLeaseMs / 3));
      try {
        await dispatchSchedule(scheduleId, handler, "manual", {
          onAcquired: async () => {
            if (!(await markSchedulerControlAccepted(prefix, request))) {
              throw new Error(`schedulerControl.runNow: stale request ${request.requestId}`);
            }
          },
          shouldContinue: () => running,
        });
        await message.ack();
      } catch (error) {
        // The control queue has effectively unlimited deliveries, so an
        // unconditional nack replayed the user's callback every 250ms for the
        // whole message-age window. Give up after a bounded number of attempts
        // and report it, rather than storming.
        if (message.attempt >= CONTROL_MAX_ATTEMPTS) {
          await markSchedulerControlUnavailable(
            prefix,
            request,
            `schedulerControl.runNow: dispatch failed after ${message.attempt} attempts: ${asError(error).message}`,
          );
          await message.ack();
        } else {
          await message.nack({ delayMs: 250, reason: "control-dispatch-error", error: asError(error).message });
        }
      } finally {
        clearInterval(touchTimer);
      }
      dispatched += 1;
    }
  };

  const loop = async (): Promise<void> => {
    while (running) {
      try {
        await dispatchControlRequests();
        await tryAcquireLeadership();
        if (currentLeaderLock) {
          await dispatchDue();
        }
        metrics.lastTickAt = Date.now();
      } catch {
        metrics.tickErrors += 1;
      }
      await sleep(tickMs);
    }
  };

  // Handler liveness must not depend on the dispatch loop: that loop awaits
  // user callbacks, which may legitimately run longer than the presence TTL.
  const controlHeartbeatLoop = async (): Promise<void> => {
    while (running) {
      try {
        controlRefreshPromise = refreshControlHandlers();
        await controlRefreshPromise;
      } catch {
        metrics.tickErrors += 1;
      } finally {
        controlRefreshPromise = null;
      }
      await sleep(controlHeartbeatMs);
    }
  };

  // The lease heartbeat runs on its own loop, not on the tick loop. On the tick
  // loop it could not run at all while a dispatch was in flight, so any handler
  // that outlived leaseMs — an ordinary batch job — silently dropped the lease,
  // let a second pod take over and dispatch the same slot, and then wrote its
  // own stale state over the new leader's.
  const heartbeatLoop = async (): Promise<void> => {
    while (running) {
      try {
        await maintainLeadership();
      } catch {
        metrics.tickErrors += 1;
      }
      await sleep(Math.max(50, Math.floor(heartbeatMs / 2)));
    }
  };

  // ==========================
  // Public API
  // ==========================

  const create = async <R>(cfg: ScheduleConfig<R>): Promise<{ created: boolean; updated: boolean }> => {
    const tz = cfg.tz ?? "UTC";
    assertValidTimeZone(tz);
    const firstRunAt = nextCronTimestamp(cfg.cron, tz, Date.now());

    const incoming: StoredSchedule = {
      id: cfg.id,
      revision: crypto.randomUUID(),
      cron: cfg.cron,
      tz,
      createdAt: Date.now(),
      updatedAt: Date.now(),
      nextRunAt: firstRunAt,
      runNumber: 0,
      failureCount: 0,
      ...(cfg.meta === undefined ? {} : { metaJson: JSON.stringify(cfg.meta) }),
    };

    const resultRaw = await redis.send("EVAL", [
      UPSERT_SCRIPT,
      "3",
      scheduleKey(cfg.id),
      dueKey,
      indexKey,
      JSON.stringify(incoming),
      String(firstRunAt),
      cfg.id,
      String(Date.now()),
    ]);
    const resultTuple = Array.isArray(resultRaw) ? resultRaw : [resultRaw, firstRunAt];
    const result = Number(resultTuple[0]);
    const storedNextRunAt = Number(resultTuple[1] ?? firstRunAt);

    await registerSchedulerControlIndex(prefix, config.id);
    handlers.set(cfg.id, {
      process: cfg.process as HandlerEntry["process"],
      after: cfg.after as HandlerEntry["after"],
      trace: cfg.trace as HandlerEntry["trace"],
    });
    if (running) {
      await refreshSchedulerControlHandler({
        prefix,
        schedulerId: config.id,
        scheduleId: cfg.id,
        instanceId,
        ttlMs: controlHandlerTtlMs,
      });
    }

    await emitTrace(cfg.trace, {
      type: "scheduled",
      scheduleId: cfg.id,
      cron: cfg.cron,
      tz,
      nextRunAt: storedNextRunAt,
      ...(cfg.meta ? { meta: cfg.meta } : {}),
    });

    return {
      created: result === 1,
      updated: result === 2,
    };
  };

  const deleteSchedule = async (cfg: { id: string }): Promise<void> => {
    await redis.send("EVAL", [DELETE_SCRIPT, "3", scheduleKey(cfg.id), dueKey, indexKey, cfg.id]);
    handlers.delete(cfg.id);
    // A refresh may have captured this handler immediately before deletion.
    // Wait it out, then make the removal the final Redis write.
    await controlRefreshPromise?.catch(() => {});
    await removeSchedulerControlHandler({ prefix, schedulerId: config.id, scheduleId: cfg.id, instanceId });
  };

  const runNow = async (cfg: { id: string }): Promise<void> => {
    const raw = await redis.get(scheduleKey(cfg.id));
    const schedule = parseSchedule(raw);
    if (!schedule) throw new Error(`runNow: schedule ${cfg.id} not found`);
    const handler = handlers.get(cfg.id);
    if (!handler) throw new Error(`runNow: no handler registered for schedule ${cfg.id} on this pod`);

    // `runNow` does not advance cron — regular schedule continues as before,
    // unless user explicitly calls ctx.reschedule in after. Serialising it
    // against tick dispatch is what keeps that promise: an unsynchronised
    // read-modify-write let a manual run persist a stale snapshot and rewind
    // nextRunAt, making the slot immediately due again.
    await dispatchSchedule(cfg.id, handler, "manual");
  };

  const get = async (cfg: { id: string }): Promise<SchedulerInfo | null> => {
    const raw = await redis.get(scheduleKey(cfg.id));
    const parsed = parseSchedule(raw);
    return parsed ? asInfo(parsed) : null;
  };

  const list = async (): Promise<SchedulerInfo[]> => {
    const idsRaw = await redis.send("SMEMBERS", [indexKey]);
    if (!Array.isArray(idsRaw) || idsRaw.length === 0) return [];
    const ids = idsRaw.map((v) => String(v));
    const values = await redis.send("MGET", ids.map((id) => scheduleKey(id)));
    if (!Array.isArray(values)) return [];

    const out: SchedulerInfo[] = [];
    for (const raw of values) {
      const parsed = parseSchedule(typeof raw === "string" ? raw : null);
      if (parsed) out.push(asInfo(parsed));
    }
    out.sort((a, b) => a.id.localeCompare(b.id));
    return out;
  };

  const metric = (): SchedulerMetrics => ({ ...metrics });

  const start = (): void => {
    if (running) return;
    running = true;
    loopPromise = loop();
    heartbeatPromise = heartbeatLoop();
    controlHeartbeatPromise = controlHeartbeatLoop();
  };

  const stop = async (): Promise<void> => {
    const wasRunning = running;
    running = false;
    dispatchGeneration += 1;
    const pendingDispatches = Array.from(dispatchChains.values());
    // Cancel in-flight callbacks so `ctx.signal.aborted` is a usable
    // cancellation signal instead of something that is always false.
    for (const ac of activeRuns) ac.abort();
    if (wasRunning) {
      await Promise.all([loopPromise, heartbeatPromise, controlHeartbeatPromise]);
    }
    await Promise.all(pendingDispatches);
    loopPromise = null;
    heartbeatPromise = null;
    controlHeartbeatPromise = null;
    await removeControlHandlers();
    await relinquishLeadership();
  };

  return {
    id: config.id,
    start,
    stop,
    create,
    delete: deleteSchedule,
    runNow,
    get,
    list,
    metric,
  };
};
