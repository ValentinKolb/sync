import { mutex, type Lock } from "./mutex";
import { type Store } from "./store";
import { expBackoff, type BackoffOptions } from "./retry";
import { sleep } from "./internal/sleep";
import { resolveStore, sharedState } from "./internal/shared-state";
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
const MIN_LEASE_MS = 500;
const MIN_HEARTBEAT_MS = 100;

const normalizeMs = (value: number | undefined, fallback: number, minimum: number): number =>
  Math.max(minimum, Math.floor(value !== undefined && Number.isFinite(value) ? value : fallback));

const assertPositiveMs = (name: string, value: number | undefined): void => {
  if (value !== undefined && (!Number.isFinite(value) || value <= 0)) {
    throw new Error(`${name} must be a finite number greater than 0`);
  }
};

const assertNonNegativeMs = (name: string, value: number | undefined): void => {
  if (value !== undefined && (!Number.isFinite(value) || value < 0)) {
    throw new Error(`${name} must be a finite non-negative number`);
  }
};

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
  meta?: Record<string, unknown>;
};

type PersistedState = {
  version: 1;
  runNumber: number;
  nextRunAt: number;
  failureCount: number;
  updatedAt: number;
  /**
   * The cron and tz this state belongs to. Without them, "persisted state
   * exists" was read as "cron unchanged", so a schedule persisted under an old
   * expression kept its old nextRunAt after the app shipped a new one — for up
   * to a day — and carried a stale failureCount with it. Absent on records
   * written by <= 5.8.0, which are then treated as belonging to a different
   * expression and reset, matching the server.
   */
  cron?: string;
  tz?: string;
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
  /**
   * Optional store to persist schedule state across tab reloads, and the store
   * leader election coordinates through. Default: a MemoryStore shared by all
   * handles with this id in this tab, so state is lost on refresh.
   *
   * Pass a localStorage-backed store to survive tab closes. Note that it buys
   * durability, not cross-tab mutual exclusion: `mutex.acquire` is a
   * check-then-set, which is atomic only because a tab is single-threaded, so
   * two tabs racing the same localStorage lock can both win. Treat multi-tab
   * dispatch as at-least-once.
   */
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
  assertPositiveMs("leader.leaseMs", config.leader?.leaseMs);
  assertPositiveMs("leader.heartbeatMs", config.leader?.heartbeatMs);
  assertPositiveMs("dispatch.tickMs", config.dispatch?.tickMs);
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
  const store = resolveStore(config.store);
  const instanceId = crypto.randomUUID();

  const schedulesKey = `${prefix}:${config.id}:schedules`;
  const schedules = sharedState(schedulesKey, store, () => new Map<string, StoredSchedule>());

  const leaderMutex = mutex({
    id: `${config.id}:leader`,
    prefix: `${prefix}:leader`,
    defaultTtl: leaseMs,
    retryCount: 0,
    store,
  });
  const dispatchMutex = mutex({
    id: `${config.id}:dispatch`,
    prefix: `${prefix}:dispatch`,
    defaultTtl: leaseMs,
    retryCount: 0,
    store,
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
      ...(typeof value.cron === "string" ? { cron: value.cron } : {}),
      ...(typeof value.tz === "string" ? { tz: value.tz } : {}),
    };
  };

  const writePersistedState = (schedule: StoredSchedule): void => {
    const state: PersistedState = {
      version: 1,
      runNumber: schedule.runNumber,
      nextRunAt: schedule.nextRunAt,
      failureCount: schedule.failureCount,
      updatedAt: schedule.updatedAt,
      cron: schedule.cron,
      tz: schedule.tz,
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
  let heartbeatPromise: Promise<void> | null = null;
  let dispatchGeneration = 0;
  // Controllers of the callbacks currently running, so stop() can cancel them.
  const activeRuns = new Set<AbortController>();
  let currentLeaderLock: Lock | null = null;
  let lastHeartbeatAt = 0;
  let nextLeadershipAttemptAt = 0;

  const setLeader = (next: boolean): void => {
    if (metrics.isLeader === next) return;
    metrics.isLeader = next;
    metrics.leaderChanges += 1;
  };

  const tryAcquireLeadership = async (): Promise<void> => {
    if (currentLeaderLock) return;
    if (Date.now() < nextLeadershipAttemptAt) return;
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

  // One dispatch per schedule at a time in this instance. In particular, a
  // manual run cannot overlap a cron run and mutate the same schedule record.
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
    source: StoredSchedule,
    handler: HandlerEntry,
    trigger: "cron" | "manual",
    dispatchLock: Lock,
  ): Promise<void> => {
    const schedule = { ...source };
    const advanceCron = trigger === "cron";
    const slotTs = schedule.nextRunAt;
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
      heartbeatTail = heartbeatTail
        .then(async () => {
          if (dispatchLeaseLost || jobAc.signal.aborted) return;
          if (await dispatchMutex.extend(dispatchLock, leaseMs)) return;
          dispatchLeaseLost = true;
          jobAc.abort();
        })
        .catch(() => {
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
        assertNonNegativeMs("reschedule.delayMs", rcfg?.delayMs);
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
        const delayMs = (rescheduleRequested as { delayMs?: number }).delayMs ?? 0;
        schedule.nextRunAt = Date.now() + delayMs;
        traceRescheduled = { delayMs };
      } else if (advanceCron) {
        schedule.nextRunAt = nextCronTimestamp(schedule.cron, schedule.tz, Date.now());
      }

      schedule.updatedAt = Date.now();
      const current = schedules.get(schedule.id);
      if (
        !current
        || current.runNumber !== expectedRunNumber
        || current.revision !== schedule.revision
        || !(await dispatchMutex.extend(dispatchLock, leaseMs))
      ) {
        metrics.staleWrites += 1;
        throw new Error(`scheduler dispatch state changed for ${schedule.id}`);
      }
      schedules.set(schedule.id, schedule);
      writePersistedState(schedule);
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
        preparationHeartbeatTail = preparationHeartbeatTail
          .then(async () => {
            if (preparationLeaseLost) return;
            if (await dispatchMutex.extend(dispatchLock, leaseMs)) return;
            preparationLeaseLost = true;
          })
          .catch(() => {
            preparationLeaseLost = true;
          });
      }, Math.max(50, Math.floor(heartbeatMs / 2)));

      try {
        const schedule = schedules.get(scheduleId);
        if (!schedule) throw new Error(`runNow: schedule ${scheduleId} not found`);
        const handler = handlers.get(scheduleId);
        if (!handler) throw new Error(`runNow: no current handler registered for schedule ${scheduleId}`);
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
        if (
          generation !== dispatchGeneration
          || (options?.shouldContinue && !options.shouldContinue())
        ) {
          throw new Error(`scheduler dispatch stopped for ${scheduleId}`);
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
        // Avoid immediately reacquiring the released lock on the same tick
        // cadence and starving an instance that does own the handler.
        metrics.unservedSlots += 1;
        nextLeadershipAttemptAt = Date.now() + leaseMs;
        await relinquishLeadership();
        return;
      }

      await dispatchSchedule(schedule.id, "cron");
    }
  };

  const loop = async (): Promise<void> => {
    while (running) {
      try {
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
    const nowMs = Date.now();
    const firstRunAt = nextCronTimestamp(cfg.cron, tz, nowMs);

    const existing = schedules.get(cfg.id);
    const persisted = readPersistedState(cfg.id);

    const stored: StoredSchedule = {
      id: cfg.id,
      revision: crypto.randomUUID(),
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

    // Preserve nextRunAt/failureCount only if cron/tz are genuinely unchanged.
    if (existing && existing.cron === cfg.cron && existing.tz === tz) {
      stored.nextRunAt = existing.nextRunAt;
      stored.failureCount = existing.failureCount;
    } else if (!existing && persisted && persisted.cron === cfg.cron && persisted.tz === tz) {
      // Resume from persisted state on tab reopen.
      stored.nextRunAt = persisted.nextRunAt <= nowMs ? firstRunAt : persisted.nextRunAt;
      stored.failureCount = persisted.failureCount;
    }

    const created = !existing;
    writePersistedState(stored);
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
      runNow: (onAccepted) => runNow({ id: cfg.id }, onAccepted),
    });
    if (running) {
      setBrowserSchedulerControlAvailable({ prefix, schedulerId: config.id, instanceId, available: true });
    }

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

  const runNow = async (cfg: { id: string }, onAccepted?: () => void): Promise<void> => {
    const schedule = schedules.get(cfg.id);
    if (!schedule) throw new Error(`runNow: schedule ${cfg.id} not found`);
    if (!handlers.has(cfg.id)) throw new Error(`runNow: no handler registered for schedule ${cfg.id}`);

    await dispatchSchedule(cfg.id, "manual", {
      onAcquired: onAccepted,
      ...(onAccepted ? { shouldContinue: () => running } : {}),
    });
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
    heartbeatPromise = heartbeatLoop();
  };

  const stop = async (): Promise<void> => {
    const wasRunning = running;
    running = false;
    dispatchGeneration += 1;
    const pendingDispatches = Array.from(dispatchChains.values());
    // Cancel in-flight callbacks so `ctx.signal.aborted` is a usable
    // cancellation signal instead of something that is always false.
    for (const ac of activeRuns) ac.abort();
    setBrowserSchedulerControlAvailable({ prefix, schedulerId: config.id, instanceId, available: false });
    if (wasRunning) await Promise.all([loopPromise, heartbeatPromise]);
    await Promise.all(pendingDispatches);
    loopPromise = null;
    heartbeatPromise = null;
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
