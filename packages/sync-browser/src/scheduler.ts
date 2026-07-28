import { mutex, type Lock } from "./mutex";
import { type Store, createMemoryStore } from "./store";
import { expBackoff, type BackoffOptions } from "./retry";
import { sleep } from "./internal/sleep";
import { assertValidTimeZone, nextCronTimestamp } from "./internal/cron";
import { emitTrace, type TraceHandler } from "./trace";
import {
  registerBrowserSchedulerControl,
  setBrowserSchedulerControlAvailable,
  unregisterBrowserSchedulerControl,
} from "./scheduler-control";

const DEFAULT_PREFIX = "sync:scheduler";
const DEFAULT_LEASE_MS = 5_000;
const DEFAULT_HEARTBEAT_MS = 500;
const DEFAULT_TICK_MS = 500;
const DEFAULT_BATCH_SIZE = 200;

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
  meta?: Record<string, unknown>;
};

type PersistedState = {
  version: 1;
  runNumber: number;
  nextRunAt: number;
  failureCount: number;
  updatedAt: number;
};

export type SchedulerMetrics = {
  isLeader: boolean;
  leaderChanges: number;
  dispatches: number;
  failures: number;
  reschedules: number;
  tickErrors: number;
  /** Durable writes a fence refused. Always 0 here: a tab has no competing writer. */
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
  /** Optional store to persist schedule state across tab reloads.
   *  Default: MemoryStore (state lost on refresh).
   *  Pass a localStorage-backed store to survive tab closes. */
  store?: Store;
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
// Shared state (across instances with same scheduler id)
// ==========================

type HandlerEntry = {
  process: (cfg: { ctx: ScheduleCtx }) => Promise<unknown> | unknown;
  after?: (cfg: { ctx: ScheduleAfterCtx<unknown> }) => Promise<void> | void;
  trace?: TraceHandler<SchedulerTraceEvent<unknown>>;
};

// Module-level shared maps so multiple scheduler() instances with the same id
// coordinate (mirror Redis shared namespace on server).
const sharedSchedules = new Map<string, Map<string, StoredSchedule>>();
const sharedMutexStores = new Map<string, Store>();

const asError = (error: unknown): Error => (error instanceof Error ? error : new Error(String(error)));

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
  ...(schedule.meta ? { meta: schedule.meta } : {}),
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
  const store = config.store ?? createMemoryStore();
  const instanceId = crypto.randomUUID();

  const schedulesKey = `${prefix}:${config.id}:schedules`;
  if (!sharedSchedules.has(schedulesKey)) {
    sharedSchedules.set(schedulesKey, new Map());
  }
  const schedules = sharedSchedules.get(schedulesKey)!;

  // Shared mutex store so leader election coordinates across instances
  const sharedMutexKey = `${prefix}:${config.id}:leader:store`;
  if (!sharedMutexStores.has(sharedMutexKey)) {
    sharedMutexStores.set(sharedMutexKey, createMemoryStore());
  }
  const sharedMutexStore = sharedMutexStores.get(sharedMutexKey)!;

  const leaderMutex = mutex({
    id: `${config.id}:leader`,
    prefix: `${prefix}:leader`,
    defaultTtl: leaseMs,
    retryCount: 0,
    store: sharedMutexStore,
  });

  const persistedStateKey = (scheduleId: string): string =>
    `${prefix}:${config.id}:state:${scheduleId}`;

  const readPersistedState = (scheduleId: string): PersistedState | null => {
    const raw = store.get(persistedStateKey(scheduleId));
    if (!raw || typeof raw !== "object") return null;
    const value = raw as Record<string, unknown>;
    const runNumber = Number(value.runNumber);
    const nextRunAt = Number(value.nextRunAt);
    const failureCount = Number(value.failureCount);
    if (!Number.isFinite(runNumber) || !Number.isFinite(nextRunAt) || !Number.isFinite(failureCount)) {
      return null;
    }
    return {
      version: 1,
      runNumber,
      nextRunAt,
      failureCount,
      updatedAt: Number(value.updatedAt) || Date.now(),
    };
  };

  const writePersistedState = (schedule: StoredSchedule): void => {
    const state: PersistedState = {
      version: 1,
      runNumber: schedule.runNumber,
      nextRunAt: schedule.nextRunAt,
      failureCount: schedule.failureCount,
      updatedAt: schedule.updatedAt,
    };
    store.set(persistedStateKey(schedule.id), state);
  };

  const deletePersistedState = (scheduleId: string): void => {
    store.del(persistedStateKey(scheduleId));
  };

  // Per-instance: handlers are local (functions can't be shared across scopes in a useful way)
  const handlers = new Map<string, HandlerEntry>();

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
  // Controllers of the callbacks currently running, so stop() can cancel them.
  const activeRuns = new Set<AbortController>();
  let currentLeaderLock: Lock | null = null;
  let lastHeartbeatAt = 0;

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
    activeRuns.add(jobAc);

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
    } finally {
      jobAc.abort();
      activeRuns.delete(jobAc);
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

    schedule.updatedAt = Date.now();
    writePersistedState(schedule);
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
    const due: StoredSchedule[] = [];
    for (const schedule of schedules.values()) {
      if (schedule.nextRunAt <= nowMs) due.push(schedule);
    }
    due.sort((a, b) => a.nextRunAt - b.nextRunAt);
    const batch = due.slice(0, batchSize);

    for (const schedule of batch) {
      if (!currentLeaderLock) break;
      if (schedule.nextRunAt > Date.now()) continue;

      const handler = handlers.get(schedule.id);
      if (!handler) {
        // Mirror the server: do not advance a slot this instance cannot serve,
        // or the schedule silently never runs while its record looks healthy.
        metrics.unservedSlots += 1;
        await relinquishLeadership();
        return;
      }

      await dispatchOne(schedule, handler, "cron");
    }
  };

  const loop = async (): Promise<void> => {
    while (running) {
      try {
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
    const nowMs = Date.now();
    const firstRunAt = nextCronTimestamp(cfg.cron, tz, nowMs);

    const existing = schedules.get(cfg.id);
    const persisted = readPersistedState(cfg.id);

    const stored: StoredSchedule = {
      id: cfg.id,
      cron: cfg.cron,
      tz,
      createdAt: existing?.createdAt ?? persisted?.updatedAt ?? nowMs,
      updatedAt: nowMs,
      nextRunAt: firstRunAt,
      runNumber: 0,
      failureCount: 0,
      meta: cfg.meta,
    };

    // Preserve runNumber always (from existing in-memory or persisted store)
    if (existing) {
      stored.runNumber = existing.runNumber;
    } else if (persisted) {
      stored.runNumber = persisted.runNumber;
    }

    // Preserve nextRunAt/failureCount only if cron/tz unchanged
    const cronUnchanged =
      (existing && existing.cron === cfg.cron && existing.tz === tz) ||
      (!existing && persisted !== null);

    if (cronUnchanged) {
      if (existing && existing.cron === cfg.cron && existing.tz === tz) {
        stored.nextRunAt = existing.nextRunAt;
        stored.failureCount = existing.failureCount;
      } else if (persisted) {
        // Resume from persisted state on tab reopen
        stored.nextRunAt = persisted.nextRunAt <= nowMs ? firstRunAt : persisted.nextRunAt;
        stored.failureCount = persisted.failureCount;
      }
    }

    const created = !existing;
    schedules.set(cfg.id, stored);

    handlers.set(cfg.id, {
      process: cfg.process as HandlerEntry["process"],
      after: cfg.after as HandlerEntry["after"],
      trace: cfg.trace as HandlerEntry["trace"],
    });
    registerBrowserSchedulerControl({
      prefix,
      schedulerId: config.id,
      scheduleId: cfg.id,
      instanceId,
      getInfo: () => {
        const current = schedules.get(cfg.id);
        return current ? asInfo(current) : null;
      },
      runNow: () => runNow({ id: cfg.id }),
    });
    if (running) {
      setBrowserSchedulerControlAvailable({ prefix, schedulerId: config.id, instanceId, available: true });
    }

    writePersistedState(stored);
    await emitTrace(cfg.trace, {
      type: "scheduled",
      scheduleId: cfg.id,
      cron: cfg.cron,
      tz,
      nextRunAt: stored.nextRunAt,
      ...(cfg.meta ? { meta: cfg.meta } : {}),
    });

    return {
      created,
      updated: !!existing,
    };
  };

  const deleteSchedule = async (cfg: { id: string }): Promise<void> => {
    schedules.delete(cfg.id);
    handlers.delete(cfg.id);
    unregisterBrowserSchedulerControl({ prefix, schedulerId: config.id, scheduleId: cfg.id, instanceId });
    deletePersistedState(cfg.id);
  };

  const runNow = async (cfg: { id: string }): Promise<void> => {
    const schedule = schedules.get(cfg.id);
    if (!schedule) throw new Error(`runNow: schedule ${cfg.id} not found`);
    const handler = handlers.get(cfg.id);
    if (!handler) throw new Error(`runNow: no handler registered for schedule ${cfg.id}`);

    await dispatchOne(schedule, handler, "manual");
  };

  const get = async (cfg: { id: string }): Promise<SchedulerInfo | null> => {
    const schedule = schedules.get(cfg.id);
    return schedule ? asInfo(schedule) : null;
  };

  const listSchedules = async (): Promise<SchedulerInfo[]> => {
    return Array.from(schedules.values())
      .map(asInfo)
      .sort((a, b) => a.id.localeCompare(b.id));
  };

  const metric = (): SchedulerMetrics => ({ ...metrics });

  const start = (): void => {
    if (running) return;
    running = true;
    setBrowserSchedulerControlAvailable({ prefix, schedulerId: config.id, instanceId, available: true });
    loopPromise = loop();
  };

  const stop = async (): Promise<void> => {
    if (!running) return;
    running = false;
    // Cancel in-flight callbacks so `ctx.signal.aborted` is a usable
    // cancellation signal instead of something that is always false.
    for (const ac of activeRuns) ac.abort();
    setBrowserSchedulerControlAvailable({ prefix, schedulerId: config.id, instanceId, available: false });
    if (loopPromise) {
      await loopPromise;
      loopPromise = null;
    }
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
    list: listSchedules,
    metric,
  };
};
