import { redis, sleep } from "bun";
import { queue } from "./queue";
import { expBackoff, isRetryableTransportError, retry, type BackoffOptions } from "./retry";

const DEFAULT_PREFIX = "sync:job";
const DAY_MS = 24 * 60 * 60 * 1000;
const DEFAULT_LEASE_MS = 30_000;
const DEFAULT_WORKER_RECV_TIMEOUT_MS = 1_000;
const DEFAULT_KEY_TTL_MS = 24 * 60 * 60 * 1000;
const MAX_KEY_TTL_MS = 30 * DAY_MS;

// Claim idempotency key atomically. Returns { jobId, isNew }.
// If key already exists, returns the existing jobId and isNew=0.
const CLAIM_KEY_SCRIPT = `
  local idemKey = KEYS[1]
  local seqKey = KEYS[2]
  local ttlMs = tonumber(ARGV[1])

  local existing = redis.call("GET", idemKey)
  if existing then
    return { existing, 0 }
  end

  local nextId = redis.call("INCR", seqKey)
  redis.call("SET", idemKey, tostring(nextId), "PX", tostring(ttlMs))
  return { tostring(nextId), 1 }
`;

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

const activeWorkers = new Map<string, AbortController>();

const asError = (error: unknown): Error => (error instanceof Error ? error : new Error(String(error)));

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

  const keys = {
    seq: `${prefix}:${config.id}:seq`,
    idempotencyPrefix: `${prefix}:${config.id}:idempotency`,
  };

  const idempotencyKey = (key: string): string => `${keys.idempotencyPrefix}:${key}`;

  const workQueue = queue<WorkPayload>({
    id: `${config.id}:work`,
    prefix: `${prefix}:queue`,
    delivery: {
      defaultLeaseMs,
      maxDeliveries: Number.MAX_SAFE_INTEGER, // reschedule is user-controlled via ctx.reschedule
    },
  });

  const metrics: JobMetrics = {
    dispatches: 0,
    failures: 0,
    reschedules: 0,
  };

  const releaseKey = async (key: string): Promise<void> => {
    try {
      await redis.del(idempotencyKey(key));
    } catch {
      // best effort — if release fails, TTL will reclaim eventually
    }
  };

  const claimKey = async (key: string, keyTtlMs: number): Promise<{ jobId: JobId; isNew: boolean }> => {
    const result = await redis.send("EVAL", [
      CLAIM_KEY_SCRIPT,
      "2",
      idempotencyKey(key),
      keys.seq,
      String(keyTtlMs),
    ]);
    const arr = result as [string, number];
    return { jobId: String(arr[0]), isNew: Number(arr[1]) === 1 };
  };

  const startWorker = (): void => {
    if (activeWorkers.has(workerId)) return;
    const workerAc = new AbortController();
    activeWorkers.set(workerId, workerAc);

    void (async () => {
      try {
        while (!workerAc.signal.aborted) {
          try {
            const message = await retry({
              run: () =>
                workQueue.recv({
                  wait: true,
                  timeoutMs: DEFAULT_WORKER_RECV_TIMEOUT_MS,
                  leaseMs: defaultLeaseMs,
                }),
              after: ({ ctx }) => {
                if (ctx.error && isRetryableTransportError(ctx.error)) {
                  ctx.reschedule({ delayMs: ctx.expBackoff({ baseMs: 50, maxMs: 1_000 }) });
                }
              },
              signal: workerAc.signal,
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
                // after errors are swallowed — transport decision is made by ctx.reschedule flag
              }
            }

            if (rescheduleRequested) {
              const nacked = await message.nack({
                delayMs: (rescheduleRequested as { delayMs?: number }).delayMs ?? 0,
                reason: "reschedule",
                error: error?.message,
              });
              metrics.reschedules += 1;
              if (!nacked) {
                // Lease expired; message will be redelivered. Key stays claimed.
                continue;
              }
              continue;
            }

            // Terminal: ack + release key
            const acked = await message.ack();
            if (!acked) {
              // Lease expired; message will be redelivered. Key stays claimed.
              continue;
            }
            await releaseKey(payload.key);

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

    const { jobId, isNew } = await claimKey(cfg.key, keyTtlMs);
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
      await workQueue.send({
        data: payload,
        delayMs,
        meta: cfg.meta,
      });
    } catch (error) {
      // Enqueue failed after claiming key — release the key so resubmit works.
      await releaseKey(cfg.key);
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
