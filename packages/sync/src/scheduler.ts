import { redis, sleep } from "bun";
import { mutex, type Lock } from "./mutex";
import { expBackoff, type BackoffOptions } from "./retry";
import { assertValidTimeZone, nextCronTimestamp } from "./internal/cron";
import { emitTrace, type TraceHandler } from "./trace";
import {
  markSchedulerControlAccepted,
  markSchedulerControlNotFound,
  markSchedulerControlUnavailable,
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

// Persist after a dispatch: updates schedule record and due zset atomically.
// (Index set is unaffected; only populated on create, removed on delete.)
const PERSIST_SCRIPT = `
  redis.call("SET", KEYS[1], ARGV[1])
  redis.call("ZADD", KEYS[2], ARGV[2], ARGV[3])
  return 1
`;

// ==========================
// Types
// ==========================

type StoredSchedule = {
  id: string;
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
  const leaseMs = Math.max(500, config.leader?.leaseMs ?? DEFAULT_LEASE_MS);
  const heartbeatMs = Math.max(100, config.leader?.heartbeatMs ?? DEFAULT_HEARTBEAT_MS);
  const tickMs = Math.max(50, config.dispatch?.tickMs ?? DEFAULT_TICK_MS);
  const batchSize = Math.max(1, config.dispatch?.batchSize ?? DEFAULT_BATCH_SIZE);

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

  const handlers = new Map<string, HandlerEntry>();
  const controlQueues = new Map<string, ReturnType<typeof schedulerControlQueue>>();
  const metrics: SchedulerMetrics = {
    isLeader: false,
    leaderChanges: 0,
    dispatches: 0,
    failures: 0,
    reschedules: 0,
    tickErrors: 0,
    lastTickAt: null,
  };

  let running = false;
  let loopPromise: Promise<void> | null = null;
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
    if (!currentLeaderLock) return;
    const nowMs = Date.now();
    if (nowMs - lastHeartbeatAt < heartbeatMs) return;
    const ok = await leaderMutex.extend(currentLeaderLock, leaseMs);
    lastHeartbeatAt = nowMs;
    if (!ok) {
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

  const persist = async (schedule: StoredSchedule): Promise<void> => {
    await redis.send("EVAL", [
      PERSIST_SCRIPT,
      "2",
      scheduleKey(schedule.id),
      dueKey,
      JSON.stringify(schedule),
      String(schedule.nextRunAt),
      schedule.id,
    ]);
  };

  // Run a single schedule: increment runNumber, invoke process + after, update state, persist.
  // `trigger` records what caused this run. It also controls cron advancement:
  // "cron" advances nextRunAt to the next cron slot when user does not reschedule;
  // "manual" leaves nextRunAt unchanged (regular cron continues unaffected by runNow).
  const dispatchOne = async (
    schedule: StoredSchedule,
    handler: HandlerEntry,
    trigger: "cron" | "manual",
  ): Promise<void> => {
    const advanceCron = trigger === "cron";
    const slotTs = schedule.nextRunAt;
    schedule.runNumber += 1;
    const runNumber = schedule.runNumber;
    const failureCountBefore = schedule.failureCount;
    const startedAt = Date.now();
    const jobAc = new AbortController();

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
      jobAc.abort();
      error = asError(err);
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

    if (error) {
      schedule.failureCount += 1;
      schedule.lastError = error.message;
      metrics.failures += 1;
    } else {
      schedule.failureCount = 0;
      delete schedule.lastError;
      metrics.dispatches += 1;
    }

    let traceRescheduled: { delayMs: number } | null = null;
    if (rescheduleRequested) {
      const delayMs = Math.max(0, (rescheduleRequested as { delayMs?: number }).delayMs ?? 0);
      schedule.nextRunAt = Date.now() + delayMs;
      metrics.reschedules += 1;
      traceRescheduled = { delayMs };
    } else if (advanceCron) {
      schedule.nextRunAt = nextCronTimestamp(schedule.cron, schedule.tz, Date.now());
    }
    // else: nextRunAt unchanged (runNow without reschedule — cron schedule continues as before)

    schedule.updatedAt = Date.now();

    await persist(schedule);
    if (traceRescheduled) {
      await emitTrace(handler.trace, {
        type: "rescheduled",
        scheduleId: schedule.id,
        runNumber,
        delayMs: traceRescheduled.delayMs,
      });
    }
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
        // Handler not registered on this pod. Advance past this slot so another
        // leader (on a pod that did register) can pick it up at the next tick.
        schedule.nextRunAt = nextCronTimestamp(schedule.cron, schedule.tz, Date.now());
        schedule.updatedAt = Date.now();
        await persist(schedule);
        continue;
      }

      await dispatchOne(schedule, handler, "cron");
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
          ttlMs: Math.max(5_000, tickMs * 4),
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
      const raw = await redis.get(scheduleKey(scheduleId));
      const schedule = parseSchedule(raw);
      if (!schedule) {
        await markSchedulerControlNotFound(
          prefix,
          request.requestId,
          `schedulerControl.runNow: schedule ${request.schedulerId}/${request.scheduleId} not found`,
        );
        await message.ack();
        dispatched += 1;
        continue;
      }

      if (!handlers.has(scheduleId)) {
        await markSchedulerControlUnavailable(
          prefix,
          request.requestId,
          `schedulerControl.runNow: no live handler for schedule ${request.schedulerId}/${request.scheduleId}`,
        );
        await message.ack();
        dispatched += 1;
        continue;
      }

      await markSchedulerControlAccepted(prefix, request.requestId);
      const touchTimer = setInterval(() => {
        void message.touch({ leaseMs: controlLeaseMs });
      }, Math.floor(controlLeaseMs / 3));
      try {
        await dispatchOne(schedule, handler, "manual");
        await message.ack();
      } catch (error) {
        await message.nack({ delayMs: 250, reason: "control-dispatch-error", error: asError(error).message });
      } finally {
        clearInterval(touchTimer);
      }
      dispatched += 1;
    }
  };

  const loop = async (): Promise<void> => {
    while (running) {
      try {
        await refreshControlHandlers();
        await dispatchControlRequests();
        await tryAcquireLeadership();
        await maintainLeadership();
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

  // ==========================
  // Public API
  // ==========================

  const create = async <R>(cfg: ScheduleConfig<R>): Promise<{ created: boolean; updated: boolean }> => {
    const tz = cfg.tz ?? "UTC";
    assertValidTimeZone(tz);
    const firstRunAt = nextCronTimestamp(cfg.cron, tz, Date.now());

    const incoming: StoredSchedule = {
      id: cfg.id,
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
        ttlMs: Math.max(5_000, tickMs * 4),
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
    await removeSchedulerControlHandler({ prefix, schedulerId: config.id, scheduleId: cfg.id, instanceId });
    handlers.delete(cfg.id);
  };

  const runNow = async (cfg: { id: string }): Promise<void> => {
    const raw = await redis.get(scheduleKey(cfg.id));
    const schedule = parseSchedule(raw);
    if (!schedule) throw new Error(`runNow: schedule ${cfg.id} not found`);
    const handler = handlers.get(cfg.id);
    if (!handler) throw new Error(`runNow: no handler registered for schedule ${cfg.id} on this pod`);

    // `runNow` does not advance cron — regular schedule continues as before,
    // unless user explicitly calls ctx.reschedule in after.
    await dispatchOne(schedule, handler, "manual");
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
  };

  const stop = async (): Promise<void> => {
    if (!running) return;
    running = false;
    await loopPromise;
    loopPromise = null;
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
