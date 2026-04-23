import { queue, type Queue } from "./queue";
import { expBackoff, type BackoffOptions } from "./retry";
import { sleep } from "./internal/sleep";

const DEFAULT_PREFIX = "sync:job";
const DAY_MS = 24 * 60 * 60 * 1000;
const DEFAULT_LEASE_MS = 30_000;
const DEFAULT_WORKER_RECV_TIMEOUT_MS = 1_000;
const DEFAULT_KEY_TTL_MS = 24 * 60 * 60 * 1000;
const MAX_KEY_TTL_MS = 30 * DAY_MS;

// ==========================
// Types
// ==========================

export type JobId = string;

export type JobMetrics = {
  dispatches: number;
  failures: number;
  reschedules: number;
};

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
  metrics: JobMetrics;
  seq: number;
};

const sharedStates = new Map<string, SharedJobState>();
const activeWorkers = new Map<string, AbortController>();

const asError = (error: unknown): Error => (error instanceof Error ? error : new Error(String(error)));

const getSharedState = (id: string, prefix: string, defaultLeaseMs: number): SharedJobState => {
  const key = `${prefix}:${id}`;
  let shared = sharedStates.get(key);
  if (!shared) {
    shared = {
      idempotency: new Map(),
      workQueue: queue<WorkPayload>({
        id: `${id}:work`,
        prefix: `${prefix}:queue`,
        delivery: {
          defaultLeaseMs,
          maxDeliveries: Number.MAX_SAFE_INTEGER,
        },
      }),
      metrics: {
        dispatches: 0,
        failures: 0,
        reschedules: 0,
      },
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
  const workerId = `${prefix}:${config.id}`;
  const defaultLeaseMs = Math.max(1, config.defaults?.leaseMs ?? DEFAULT_LEASE_MS);
  const defaultKeyTtlMs = Math.min(
    MAX_KEY_TTL_MS,
    Math.max(1_000, config.defaults?.keyTtlMs ?? DEFAULT_KEY_TTL_MS),
  );

  const shared = getSharedState(config.id, prefix, defaultLeaseMs);
  const metrics = shared.metrics;

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

  const releaseKey = (key: string): void => {
    shared.idempotency.delete(key);
  };

  const startWorker = (): void => {
    if (activeWorkers.has(workerId)) return;
    const workerAc = new AbortController();
    activeWorkers.set(workerId, workerAc);

    void (async () => {
      try {
        while (!workerAc.signal.aborted) {
          try {
            const message = await shared.workQueue.recv({
              wait: true,
              timeoutMs: DEFAULT_WORKER_RECV_TIMEOUT_MS,
              leaseMs: defaultLeaseMs,
            });

            if (!message) continue;

            const payload = message.data;
            const attempt = message.attempt;
            const failureCount = attempt - 1;
            const startedAt = Date.now();
            const jobAc = new AbortController();

            const makeCtx = (): JobCtx<Input> => {
              const ctx = {
                jobId: payload.jobId,
                key: payload.key,
                input: payload.input as Input,
                failureCount,
                signal: jobAc.signal,
                heartbeat: async (cfg?: { leaseMs?: number }): Promise<void> => {
                  await message.touch({ leaseMs: cfg?.leaseMs ?? payload.leaseMs });
                },
              } as JobCtx<Input>;
              Object.defineProperty(ctx, "duration", {
                get: () => Date.now() - startedAt,
                enumerable: true,
              });
              return ctx;
            };

            const ctx = makeCtx();

            let result: Result | undefined;
            let error: Error | undefined;
            try {
              result = await Promise.resolve(config.process({ ctx }));
            } catch (err) {
              jobAc.abort();
              error = asError(err);
            }

            // Build after ctx
            let rescheduleRequested: { delayMs?: number } | null = null;
            const afterCtx: JobAfterCtx<Input, Result> = Object.create(ctx) as JobAfterCtx<Input, Result>;
            if (error) afterCtx.error = error;
            if (!error) afterCtx.data = result;
            afterCtx.reschedule = (rcfg?: { delayMs?: number }): void => {
              rescheduleRequested = { delayMs: rcfg?.delayMs };
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

            if (rescheduleRequested) {
              const nacked = await message.nack({
                delayMs: (rescheduleRequested as { delayMs?: number }).delayMs ?? 0,
                reason: "reschedule",
                error: error?.message,
              });
              metrics.reschedules += 1;
              if (!nacked) continue;
              continue;
            }

            // Terminal: ack + release key
            const acked = await message.ack();
            if (!acked) continue;
            releaseKey(payload.key);

            if (error) {
              metrics.failures += 1;
            } else {
              metrics.dispatches += 1;
            }
          } catch {
            if (workerAc.signal.aborted) break;
            await sleep(25);
          }
        }
      } finally {
        const current = activeWorkers.get(workerId);
        if (current === workerAc) {
          activeWorkers.delete(workerId);
        }
      }
    })();
  };

  const submit = async (cfg: SubmitConfig<Input>): Promise<JobId> => {
    if (!cfg.key) throw new Error("submit: key is required");
    startWorker();

    const leaseMs = Math.max(1, cfg.leaseMs ?? defaultLeaseMs);
    const keyTtlMs = Math.min(MAX_KEY_TTL_MS, Math.max(1_000, cfg.keyTtlMs ?? defaultKeyTtlMs));
    const delayMs = cfg.at !== undefined ? Math.max(0, cfg.at - Date.now()) : Math.max(0, cfg.delayMs ?? 0);

    const { jobId, isNew } = claimKey(cfg.key, keyTtlMs);
    if (!isNew) return jobId;

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
      releaseKey(cfg.key);
      throw error;
    }

    return jobId;
  };

  const metric = (): JobMetrics => ({ ...metrics });

  const stop = (): void => {
    const ac = activeWorkers.get(workerId);
    if (ac) {
      ac.abort();
      activeWorkers.delete(workerId);
    }
  };

  return {
    id: config.id,
    submit,
    metric,
    stop,
  };
};
