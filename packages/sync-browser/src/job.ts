import { queue, type Queue } from "./queue";
import { expBackoff, type BackoffOptions } from "./retry";
import { sleep } from "./internal/sleep";
import { emitTrace, type TraceHandler } from "./trace";

const DEFAULT_PREFIX = "sync:job";
const DAY_MS = 24 * 60 * 60 * 1000;
const DEFAULT_LEASE_MS = 30_000;
const DEFAULT_WORKER_RECV_TIMEOUT_MS = 1_000;
const DEFAULT_KEY_TTL_MS = 24 * 60 * 60 * 1000;
const MIN_KEY_TTL_MS = 1_000;
const MAX_KEY_TTL_MS = 30 * DAY_MS;
const MAX_JOB_DELAY_MS = MAX_KEY_TTL_MS - MIN_KEY_TTL_MS;
const MAX_JOB_MESSAGE_AGE_MS = Number.MAX_SAFE_INTEGER;

// ==========================
// Types
// ==========================

export type JobId = string;

export type JobMetrics = {
  dispatches: number;
  failures: number;
  reschedules: number;
};

export type JobTraceEvent<Input = void, Result = unknown> =
  | { type: "submitted"; jobId: string; key: string; input?: Input; meta?: Record<string, unknown> }
  | { type: "started"; jobId: string; key: string; input?: Input; attempt: number }
  | { type: "succeeded"; jobId: string; key: string; input?: Input; data: Result; durationMs: number }
  | { type: "failed"; jobId: string; key: string; input?: Input; error: Error; durationMs: number }
  | { type: "rescheduled"; jobId: string; key: string; attempt: number; delayMs: number }
  | { type: "finished"; jobId: string; key: string; status: "succeeded" | "failed"; durationMs: number };

export type JobCtx<Input = void> = {
  jobId: JobId;
  key: string;
  input: Input;
  failureCount: number;
  readonly duration: number;
  signal: AbortSignal;
  heartbeat(cfg?: { leaseMs?: number }): Promise<void>;
};

export type JobAfterCtx<Input = void, Result = unknown> = JobCtx<Input> & {
  data?: Result;
  error?: Error;
  reschedule(cfg?: { delayMs?: number }): void;
  expBackoff(cfg?: BackoffOptions): number;
  metric: JobMetrics;
};

export type SubmitConfig<Input = void> = {
  key: string;
  keyTtlMs?: number;
  delayMs?: number;
  at?: number;
  leaseMs?: number;
  meta?: Record<string, unknown>;
} & (Input extends void ? { input?: Input } : { input: Input });

export type JobConfig<Input = void, Result = unknown> = {
  id: string;
  prefix?: string;
  defaults?: {
    leaseMs?: number;
    keyTtlMs?: number;
  };
  trace?: TraceHandler<JobTraceEvent<Input, Result>>;
  process: (cfg: { ctx: JobCtx<Input> }) => Promise<Result> | Result;
  after?: (cfg: { ctx: JobAfterCtx<Input, Result> }) => Promise<void> | void;
};

export type JobHandle<Input = void> = {
  id: string;
  submit(cfg: SubmitConfig<Input>): Promise<JobId>;
  metric(): JobMetrics;
  stop(): void;
};

// ==========================
// Internal
// ==========================

type WorkPayload = {
  jobId: JobId;
  key: string;
  input?: unknown;
  keyTtlMs: number;
  leaseMs: number;
  meta?: Record<string, unknown>;
};

type IdempotencyEntry = {
  jobId: JobId;
  expiresAt: number;
};

type SharedJobState = {
  idempotency: Map<string, IdempotencyEntry>;
  workQueue: Queue<WorkPayload>;
  seq: number;
};

const sharedStates = new Map<string, SharedJobState>();
const asError = (error: unknown): Error => (error instanceof Error ? error : new Error(String(error)));
const requireSafeInteger = (name: string, value: number, min: number, max = Number.MAX_SAFE_INTEGER): number => {
  if (!Number.isSafeInteger(value) || value < min || value > max) {
    throw new RangeError(`${name} must be a safe integer between ${min} and ${max}`);
  }
  return value;
};

const requireFutureDuration = (name: string, value: number, min: number): number =>
  requireSafeInteger(name, value, min, Number.MAX_SAFE_INTEGER - Date.now());

const resolveDelayMs = (name: string, value = 0): number =>
  requireSafeInteger(name, value, 0, MAX_JOB_DELAY_MS);

/**
 * The queue and the idempotency map are genuinely shared by every handle with
 * this id, exactly as they are through Redis on the server. Metrics are not:
 * the server keeps them closure-local, so a dashboard constructing
 * `job({id:"emails"})` for observability reports zeros there and reported the
 * worker's cumulative counts here. The lease is not either — it was baked into
 * the shared work queue by whichever handle happened to construct it first, so
 * a later `job({id, defaults:{leaseMs}})` silently inherited the first one's.
 * It is applied per recv instead.
 */
const getSharedState = (id: string, prefix: string): SharedJobState => {
  const key = `${prefix}:${id}`;
  let shared = sharedStates.get(key);
  if (!shared) {
    shared = {
      idempotency: new Map(),
      workQueue: queue<WorkPayload>({
        id: `${id}:work`,
        prefix: `${prefix}:queue`,
        delivery: {
          maxDeliveries: Number.MAX_SAFE_INTEGER,
        },
        limits: {
          maxMessageAgeMs: MAX_JOB_MESSAGE_AGE_MS,
          maxNackDelayMs: MAX_JOB_DELAY_MS,
        },
      }),
      seq: 0,
    };
    sharedStates.set(key, shared);
  }
  return shared;
};

// ==========================
// Factory
// ==========================

export const job = <Input = void, Result = unknown>(
  config: JobConfig<Input, Result>,
): JobHandle<Input> => {
  const prefix = config.prefix ?? DEFAULT_PREFIX;
  // The worker belongs to this handle, not to the id.
  let workerAcRef: AbortController | null = null;
  let activeJobAc: AbortController | null = null;
  let restartRequested = false;
  const defaultLeaseMs = requireFutureDuration(
    "defaults.leaseMs",
    config.defaults?.leaseMs ?? DEFAULT_LEASE_MS,
    1,
  );
  const defaultKeyTtlMs = requireSafeInteger(
    "defaults.keyTtlMs",
    config.defaults?.keyTtlMs ?? DEFAULT_KEY_TTL_MS,
    MIN_KEY_TTL_MS,
    MAX_KEY_TTL_MS,
  );

  const shared = getSharedState(config.id, prefix);
  // Per handle, matching the server.
  const metrics: JobMetrics = { dispatches: 0, failures: 0, reschedules: 0 };

  const sweepExpiredKeys = (): void => {
    const now = Date.now();
    for (const [k, v] of shared.idempotency) {
      if (now >= v.expiresAt) shared.idempotency.delete(k);
    }
  };

  const claimKey = (key: string, keyTtlMs: number): { jobId: JobId; isNew: boolean } => {
    sweepExpiredKeys();
    const existing = shared.idempotency.get(key);
    if (existing && Date.now() < existing.expiresAt) {
      return { jobId: existing.jobId, isNew: false };
    }
    const jobId = String(++shared.seq);
    shared.idempotency.set(key, { jobId, expiresAt: Date.now() + keyTtlMs });
    return { jobId, isNew: true };
  };

  const refreshKey = (key: string, jobId: JobId, keyTtlMs: number): boolean => {
    const entry = shared.idempotency.get(key);
    if (entry?.jobId !== jobId) return false;
    entry.expiresAt = Date.now() + keyTtlMs;
    return true;
  };

  const releaseKey = (key: string, jobId: JobId): void => {
    if (shared.idempotency.get(key)?.jobId === jobId) {
      shared.idempotency.delete(key);
    }
  };

  const startWorker = (): void => {
    if (workerAcRef) {
      if (workerAcRef.signal.aborted) restartRequested = true;
      return;
    }
    restartRequested = false;
    const workerAc = new AbortController();
    workerAcRef = workerAc;

    void (async () => {
      try {
        while (!workerAc.signal.aborted) {
          try {
            const message = await shared.workQueue.recv({
              wait: true,
              timeoutMs: DEFAULT_WORKER_RECV_TIMEOUT_MS,
              leaseMs: defaultLeaseMs,
              signal: workerAc.signal,
            });

            if (!message) continue;

            const payload = message.data;
            const attempt = message.attempt;
            const failureCount = attempt - 1;
            const startedAt = Date.now();
            const jobAc = new AbortController();
            let leaseLost = false;
            activeJobAc = jobAc;

            try {
              const keepLease = async (leaseMs = payload.leaseMs): Promise<boolean> => {
                requireFutureDuration("heartbeat.leaseMs", leaseMs, 1);
                const held = await message.touch({ leaseMs });
                if (!held) {
                  leaseLost = true;
                  jobAc.abort();
                  return false;
                }
                refreshKey(payload.key, payload.jobId, payload.keyTtlMs);
                return true;
              };
              const canceled = (): boolean => workerAc.signal.aborted || leaseLost || jobAc.signal.aborted;
              const renewIfActive = async (leaseMs = payload.leaseMs): Promise<boolean> => {
                if (canceled() || !(await keepLease(leaseMs))) return false;
                return !canceled();
              };

              const ctx = {
                jobId: payload.jobId,
                key: payload.key,
                input: payload.input as Input,
                failureCount,
                signal: jobAc.signal,
                heartbeat: async (cfg?: { leaseMs?: number }): Promise<void> => {
                  await keepLease(cfg?.leaseMs);
                },
              } as JobCtx<Input>;
              Object.defineProperty(ctx, "duration", {
                get: () => Date.now() - startedAt,
                enumerable: true,
              });

              // Queue recv uses the handle default. Apply this submission's
              // lease before trace or user code can outlive the wrong lease.
              if (!(await renewIfActive(payload.leaseMs))) continue;
              const traceInput = payload.input === undefined ? {} : { input: payload.input as Input };

              await emitTrace(config.trace, {
                type: "started",
                jobId: payload.jobId,
                key: payload.key,
                ...traceInput,
                attempt,
              });
              if (!(await renewIfActive())) continue;

              let result: Result | undefined;
              let error: Error | undefined;
              try {
                result = await Promise.resolve(config.process({ ctx }));
              } catch (err) {
                error = asError(err);
              }
              if (!(await renewIfActive())) continue;

              if (error) {
                await emitTrace(config.trace, {
                  type: "failed",
                  jobId: payload.jobId,
                  key: payload.key,
                  ...traceInput,
                  error,
                  durationMs: Date.now() - startedAt,
                });
              } else {
                await emitTrace(config.trace, {
                  type: "succeeded",
                  jobId: payload.jobId,
                  key: payload.key,
                  ...traceInput,
                  data: result as Result,
                  durationMs: Date.now() - startedAt,
                });
              }
              if (!(await renewIfActive())) continue;

              let rescheduleRequested: { delayMs: number } | null = null;
              const afterCtx: JobAfterCtx<Input, Result> = Object.create(ctx) as JobAfterCtx<Input, Result>;
              if (error) afterCtx.error = error;
              if (!error) afterCtx.data = result;
              afterCtx.reschedule = (rcfg?: { delayMs?: number }): void => {
                rescheduleRequested = {
                  delayMs: resolveDelayMs("reschedule.delayMs", rcfg?.delayMs),
                };
              };
              afterCtx.expBackoff = (bcfg?: BackoffOptions): number => expBackoff(failureCount + 1, bcfg);
              afterCtx.metric = metrics;

              if (config.after) {
                try {
                  await Promise.resolve(config.after({ ctx: afterCtx }));
                } catch {
                  // after errors are swallowed
                }
              }
              if (!(await renewIfActive())) continue;

              if (rescheduleRequested) {
                const delayMs = (rescheduleRequested as { delayMs: number }).delayMs;
                const extended = refreshKey(
                  payload.key,
                  payload.jobId,
                  Math.min(MAX_KEY_TTL_MS, delayMs + payload.keyTtlMs),
                );
                if (!extended || canceled()) continue;
                const nacked = await message.nack({
                  delayMs,
                  reason: "reschedule",
                  error: error?.message,
                });
                if (!nacked) continue;
                metrics.reschedules += 1;
                await emitTrace(config.trace, {
                  type: "rescheduled",
                  jobId: payload.jobId,
                  key: payload.key,
                  attempt,
                  delayMs,
                });
                continue;
              }

              // Terminal: ack + release key
              const acked = await message.ack();
              if (!acked) continue;
              releaseKey(payload.key, payload.jobId);

              if (error) {
                metrics.failures += 1;
              } else {
                metrics.dispatches += 1;
              }
              await emitTrace(config.trace, {
                type: "finished",
                jobId: payload.jobId,
                key: payload.key,
                status: error ? "failed" : "succeeded",
                durationMs: Date.now() - startedAt,
              });
            } finally {
              // Ordinary completion keeps the signal live through after().
              // Only clear the controller if this attempt still owns the slot.
              if (activeJobAc === jobAc) activeJobAc = null;
            }
          } catch {
            if (workerAc.signal.aborted) break;
            await sleep(25);
          }
        }
      } finally {
        const current = workerAcRef;
        if (current === workerAc) {
          workerAcRef = null;
        }
        if (restartRequested) startWorker();
      }
    })();
  };

  const submit = async (cfg: SubmitConfig<Input>): Promise<JobId> => {
    if (!cfg.key) throw new Error("submit: key is required");

    const leaseMs = requireFutureDuration("submit.leaseMs", cfg.leaseMs ?? defaultLeaseMs, 1);
    const keyTtlMs = requireSafeInteger(
      "submit.keyTtlMs",
      cfg.keyTtlMs ?? defaultKeyTtlMs,
      MIN_KEY_TTL_MS,
      MAX_KEY_TTL_MS,
    );
    const now = Date.now();
    const delayMs =
      cfg.at !== undefined
        ? resolveDelayMs("submit.at", Math.max(0, requireSafeInteger("submit.at", cfg.at, 0) - now))
        : resolveDelayMs("submit.delayMs", cfg.delayMs);
    const initialKeyTtlMs = Math.min(MAX_KEY_TTL_MS, delayMs + keyTtlMs);

    const { jobId, isNew } = claimKey(cfg.key, initialKeyTtlMs);
    if (!isNew) {
      startWorker();
      return jobId;
    }

    const payload: WorkPayload = {
      jobId,
      key: cfg.key,
      input: (cfg as { input?: Input }).input,
      keyTtlMs,
      leaseMs,
      meta: cfg.meta,
    };

    try {
      await shared.workQueue.send({
        data: payload,
        delayMs,
        meta: cfg.meta,
      });
    } catch (error) {
      releaseKey(cfg.key, jobId);
      throw error;
    }

    const traceInput = payload.input === undefined ? {} : { input: payload.input as Input };
    await emitTrace(config.trace, {
      type: "submitted",
      jobId,
      key: cfg.key,
      ...traceInput,
      ...(cfg.meta ? { meta: cfg.meta } : {}),
    });
    startWorker();

    return jobId;
  };

  const metric = (): JobMetrics => ({ ...metrics });

  const stop = (): void => {
    restartRequested = false;
    workerAcRef?.abort();
    activeJobAc?.abort();
  };

  return {
    id: config.id,
    submit,
    metric,
    stop,
  };
};
