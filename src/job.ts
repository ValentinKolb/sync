import { redis, sleep } from "bun";
import { z, type ZodTypeAny } from "zod";
import { queue } from "./queue";
import { topic, type Topic } from "./topic";
import { isRetryableTransportError, retry } from "./retry";
import {
  computeRetryDelay,
  isTerminalStatus,
  parseJsonOrNull,
  withTimeout,
} from "./internal/job-utils";

const DEFAULT_PREFIX = "sync:job";
const DAY_MS = 24 * 60 * 60 * 1000;
const DEFAULT_LEASE_MS = 30_000;
const DEFAULT_WORKER_RECV_TIMEOUT_MS = 1_000;
const DEFAULT_MAX_ATTEMPTS = 1;
const DEFAULT_STATE_RETENTION_MS = 7 * DAY_MS;
const FINALIZE_STATE_SCRIPT = `
  local key = KEYS[1]
  local payload = ARGV[1]
  local ttlMs = tonumber(ARGV[2])

  local raw = redis.call("GET", key)
  if not raw then return 0 end

  local ok, state = pcall(cjson.decode, raw)
  if not ok then return 0 end

  local status = tostring(state.status or "")
  if status == "cancelled" then return -1 end
  if status == "completed" or status == "failed" or status == "timed_out" then return 0 end

  redis.call("SET", key, payload, "PX", tostring(ttlMs))
  return 1
`;

const CANCEL_STATE_SCRIPT = `
  local key = KEYS[1]
  local payload = ARGV[1]
  local ttlMs = tonumber(ARGV[2])

  local raw = redis.call("GET", key)
  if not raw then return 0 end

  local ok, state = pcall(cjson.decode, raw)
  if not ok then return 0 end

  local status = tostring(state.status or "")
  if status == "completed" or status == "failed" or status == "timed_out" or status == "cancelled" then
    return 0
  end

  redis.call("SET", key, payload, "PX", tostring(ttlMs))
  return 1
`;

const activeWorkers = new Map<string, AbortController>();
const activeJobRuns = new Map<string, AbortController>();

export type JobId = string;

export type JobStatus = "completed" | "failed" | "cancelled" | "timed_out";

export type JobTerminal<Result = unknown> = {
  id: JobId;
  status: JobStatus;
  result?: Result;
  error?: {
    message: string;
    code?: string;
  };
  finishedAt: number;
};

export type SubmitOptions = {
  key?: string;
  keyTtlMs?: number;
  delayMs?: number;
  at?: number;
  maxAttempts?: number;
  backoff?: {
    kind: "fixed" | "exp";
    baseMs: number;
    maxMs?: number;
  };
  leaseMs?: number;
  meta?: Record<string, unknown>;
};

export type JoinOptions = {
  timeoutMs?: number;
};

export type CancelOptions = {
  reason?: string;
};

export type JobEvent =
  | { type: "submitted"; id: JobId; ts: number }
  | { type: "started"; id: JobId; runId: string; attempt: number; ts: number }
  | { type: "heartbeat"; id: JobId; runId: string; ts: number }
  | { type: "retry"; id: JobId; runId: string; nextAt: number; reason?: string; ts: number }
  | { type: "completed"; id: JobId; ts: number }
  | { type: "failed"; id: JobId; reason?: string; ts: number }
  | { type: "cancelled"; id: JobId; reason?: string; ts: number };

export type JobEvents = Pick<Topic<JobEvent>, "reader" | "live">;

export type JobContext = {
  step<T>(cfg: { id: string; run: () => Promise<T> | T }): Promise<T>;
  heartbeat(cfg?: { leaseMs?: number }): Promise<void>;
  signal: AbortSignal;
};

export type JobHandle<Input, Result = unknown> = {
  id: string;
  submit(cfg: { input: Input } & SubmitOptions): Promise<JobId>;
  validateInput(input: unknown): void;
  join(cfg: { id: JobId } & JoinOptions): Promise<JobTerminal<Result>>;
  cancel(cfg: { id: JobId } & CancelOptions): Promise<void>;
  events(id: JobId): JobEvents;
  stop(): void;
};

export type JobDefinition<TSchema extends ZodTypeAny, Result = unknown> = {
  id: string;
  schema: TSchema;
  defaults?: Omit<SubmitOptions, "key" | "delayMs" | "at" | "meta">;
  process: (cfg: { ctx: JobContext; input: z.infer<TSchema> }) => Promise<Result> | Result;
};

type WorkPayload = {
  id: JobId;
  input: unknown;
  maxAttempts: number;
  backoff?: {
    kind: "fixed" | "exp";
    baseMs: number;
    maxMs?: number;
  };
  leaseMs: number;
  meta?: Record<string, unknown>;
};

type InternalState<Result> = {
  id: JobId;
  status: "submitted" | "running" | JobStatus;
  attempts: number;
  updatedAt: number;
  finishedAt?: number;
  result?: Result;
  error?: {
    message: string;
    code?: string;
  };
};

const workPayloadSchema = z.object({
  id: z.string(),
  input: z.unknown(),
  maxAttempts: z.number().int().min(1),
  backoff: z
    .object({
      kind: z.enum(["fixed", "exp"]),
      baseMs: z.number().int().min(0),
      maxMs: z.number().int().min(0).optional(),
    })
    .optional(),
  leaseMs: z.number().int().min(1),
  meta: z.record(z.string(), z.unknown()).optional(),
});

const now = (): number => Date.now();

const parseState = <Result>(raw: string | null): InternalState<Result> | null => {
  return parseJsonOrNull<InternalState<Result>>(raw);
};

export const job = <TSchema extends ZodTypeAny, Result = unknown>(
  definition: JobDefinition<TSchema, Result>,
): JobHandle<z.infer<TSchema>, Result> => {
  type Input = z.infer<TSchema>;

  const prefix = DEFAULT_PREFIX;
  const workerId = `${prefix}:${definition.id}`;
  const runningJobKey = (jobId: JobId): string => `${workerId}:run:${jobId}`;
  const keys = {
    seq: `${prefix}:${definition.id}:seq`,
    statePrefix: `${prefix}:${definition.id}:state`,
    idempotencyPrefix: `${prefix}:${definition.id}:idempotency`,
  };

  const workQueue = queue({
    id: `${definition.id}:work`,
    prefix: `${prefix}:queue`,
    schema: workPayloadSchema,
    delivery: {
      defaultLeaseMs: DEFAULT_LEASE_MS,
      maxDeliveries: Number.MAX_SAFE_INTEGER,
    },
  });

  const topicCache = new Map<string, Topic<JobEvent>>();

  const eventsTopicFor = (jobId: JobId): Topic<JobEvent> => {
    let cached = topicCache.get(jobId);
    if (cached) return cached;
    cached = topic({
      id: `${definition.id}:${jobId}:events`,
      prefix: `${prefix}:events`,
      schema: z.any(),
      retentionMs: 7 * 24 * 60 * 60 * 1000,
    }) as unknown as Topic<JobEvent>;
    topicCache.set(jobId, cached);
    return cached;
  };

  const emitEvent = async (jobId: JobId, event: JobEvent): Promise<void> => {
    await eventsTopicFor(jobId).pub({ data: event });
  };

  const stateKey = (jobId: JobId): string => `${keys.statePrefix}:${jobId}`;

  const readState = async (jobId: JobId): Promise<InternalState<Result> | null> => {
    const raw = await redis.get(stateKey(jobId));
    return parseState<Result>(raw);
  };

  const writeState = async (state: InternalState<Result>, ttlMs: number = DEFAULT_STATE_RETENTION_MS): Promise<void> => {
    await redis.send("SET", [stateKey(state.id), JSON.stringify(state), "PX", ttlMs.toString()]);
  };

  const writeStateIfAbsent = async (state: InternalState<Result>, ttlMs: number = DEFAULT_STATE_RETENTION_MS): Promise<boolean> => {
    const result = await redis.send("SET", [stateKey(state.id), JSON.stringify(state), "PX", ttlMs.toString(), "NX"]);
    return result === "OK";
  };

  const writeFinalState = async (state: InternalState<Result>, ttlMs: number = DEFAULT_STATE_RETENTION_MS): Promise<"ok" | "cancelled" | "missing"> => {
    const result = Number(
      await redis.send("EVAL", [
        FINALIZE_STATE_SCRIPT,
        "1",
        stateKey(state.id),
        JSON.stringify(state),
        String(ttlMs),
      ]),
    );
    if (result === 1) return "ok";
    if (result === -1) return "cancelled";
    return "missing";
  };

  const writeCancelledState = async (state: InternalState<Result>, ttlMs: number = DEFAULT_STATE_RETENTION_MS): Promise<boolean> => {
    const result = Number(
      await redis.send("EVAL", [
        CANCEL_STATE_SCRIPT,
        "1",
        stateKey(state.id),
        JSON.stringify(state),
        String(ttlMs),
      ]),
    );
    return result === 1;
  };

  const startWorker = (): void => {
    if (activeWorkers.has(workerId)) return;
    const workerAc = new AbortController();
    activeWorkers.set(workerId, workerAc);

    void (async () => {
      try {
        while (!workerAc.signal.aborted) {
          try {
            const message = await retry(
              async () =>
                await workQueue.recv({
                  wait: true,
                  timeoutMs: DEFAULT_WORKER_RECV_TIMEOUT_MS,
                  leaseMs: DEFAULT_LEASE_MS,
                }),
              {
                attempts: Number.POSITIVE_INFINITY,
                signal: workerAc.signal,
                retryIf: isRetryableTransportError,
              },
            );

            if (!message) continue;

            const payload = message.data as WorkPayload;
            let state = await readState(payload.id);

            if (!state) {
              // Recover from submit crash windows where the queue message exists
              // but the durable state key was never written.
              await writeStateIfAbsent({
                id: payload.id,
                status: "submitted",
                attempts: 0,
                updatedAt: now(),
              });
              state = await readState(payload.id);
              if (!state) {
                await message.nack({ delayMs: 250, reason: "state_missing_recover_failed" });
                continue;
              }
            }

            if (state.status === "cancelled") {
              await message.ack();
              continue;
            }

            if (isTerminalStatus(state.status)) {
              await message.ack();
              continue;
            }

            const attempt = message.attempt;
            const runId = message.deliveryId;
            const startedAt = now();

            await writeState({
              ...state,
              status: "running",
              attempts: attempt,
              updatedAt: startedAt,
            });

            await emitEvent(payload.id, {
              type: "started",
              id: payload.id,
              runId,
              attempt,
              ts: startedAt,
            });

            await message.touch({ leaseMs: payload.leaseMs });

            const jobAc = new AbortController();
            const jobRunKey = runningJobKey(payload.id);
            activeJobRuns.set(jobRunKey, jobAc);

            try {
              const ctx: JobContext = {
                signal: jobAc.signal,
                step: async <T>(cfg: { id: string; run: () => Promise<T> | T }): Promise<T> => {
                  return await Promise.resolve(cfg.run());
                },
                heartbeat: async (cfg?: { leaseMs?: number }): Promise<void> => {
                  await message.touch({ leaseMs: cfg?.leaseMs ?? payload.leaseMs });
                  await emitEvent(payload.id, {
                    type: "heartbeat",
                    id: payload.id,
                    runId,
                    ts: now(),
                  });
                },
              };

              const inputParsed = definition.schema.safeParse(payload.input);
              if (!inputParsed.success) {
                throw inputParsed.error;
              }

              const processPromise = Promise.resolve(definition.process({ ctx, input: inputParsed.data as Input }));
              const result = await withTimeout(processPromise, payload.leaseMs);

              const latest = await readState(payload.id);
              if (latest?.status === "cancelled") {
                jobAc.abort();
                await message.ack();
                continue;
              }

              const acked = await message.ack();
              if (!acked) continue;

              const finishedAt = now();
              const writeResult = await writeFinalState({
                id: payload.id,
                status: "completed",
                attempts: attempt,
                updatedAt: finishedAt,
                finishedAt,
                result: result as Result,
              });
              if (writeResult !== "ok") continue;

              await emitEvent(payload.id, {
                type: "completed",
                id: payload.id,
                ts: finishedAt,
              });
            } catch (error) {
              jobAc.abort();
              const err = error instanceof Error ? error : new Error(String(error));
              const timedOut = err.name === "JobTimeoutError";
              const canRetry = attempt < payload.maxAttempts;

              if (canRetry) {
                const delayMs = computeRetryDelay(payload.backoff, attempt);
                const nextAt = now() + delayMs;

                const nacked = await message.nack({
                  delayMs,
                  reason: timedOut ? "timed_out" : "error",
                  error: err.message,
                });

                if (!nacked) continue;

                const latestState = await readState(payload.id);
                if (latestState?.status === "cancelled") continue;

                await writeState({
                  id: payload.id,
                  status: "submitted",
                  attempts: attempt,
                  updatedAt: now(),
                });

                await emitEvent(payload.id, {
                  type: "retry",
                  id: payload.id,
                  runId,
                  nextAt,
                  reason: err.message,
                  ts: now(),
                });

                continue;
              }

              const acked = await message.ack();
              if (!acked) continue;

              const finishedAt = now();
              const status: JobStatus = timedOut ? "timed_out" : "failed";

              const writeResult = await writeFinalState({
                id: payload.id,
                status,
                attempts: attempt,
                updatedAt: finishedAt,
                finishedAt,
                error: {
                  message: err.message,
                  code: timedOut ? "TIMEOUT" : undefined,
                },
              });
              if (writeResult !== "ok") continue;

              await emitEvent(payload.id, {
                type: "failed",
                id: payload.id,
                reason: err.message,
                ts: finishedAt,
              });
            } finally {
              const active = activeJobRuns.get(jobRunKey);
              if (active === jobAc) {
                activeJobRuns.delete(jobRunKey);
              }
            }
          } catch {
            // Keep worker alive on transient infrastructure errors.
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

  const SUBMIT_IDEMPOTENCY_SCRIPT = `
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

  const submit = async (cfg: { input: Input } & SubmitOptions): Promise<JobId> => {
    startWorker();

    const parsed = definition.schema.safeParse(cfg.input);
    if (!parsed.success) throw parsed.error;

    const maxAttempts = Math.max(1, cfg.maxAttempts ?? definition.defaults?.maxAttempts ?? DEFAULT_MAX_ATTEMPTS);
    const leaseMs = Math.max(1, cfg.leaseMs ?? definition.defaults?.leaseMs ?? DEFAULT_LEASE_MS);
    const backoff = cfg.backoff ?? definition.defaults?.backoff;
    const delayMs = cfg.at !== undefined ? Math.max(0, cfg.at - now()) : Math.max(0, cfg.delayMs ?? 0);
    const keyTtlMs = Math.max(1_000, cfg.keyTtlMs ?? DEFAULT_STATE_RETENTION_MS);

    let jobId: string;
    let isNewSubmission = true;

    if (cfg.key) {
      const idemKey = `${keys.idempotencyPrefix}:${cfg.key}`;
      const result = await redis.send("EVAL", [
        SUBMIT_IDEMPOTENCY_SCRIPT,
        "2",
        idemKey,
        keys.seq,
        String(keyTtlMs),
      ]);
      const arr = result as [string, number];
      jobId = String(arr[0]);
      isNewSubmission = Number(arr[1]) === 1;
    } else {
      const nextId = await redis.incr(keys.seq);
      jobId = String(nextId);
    }

    const submittedState: InternalState<Result> = {
      id: jobId,
      status: "submitted",
      attempts: 0,
      updatedAt: now(),
    };

    const payload: WorkPayload = {
      id: jobId,
      input: parsed.data,
      maxAttempts,
      backoff,
      leaseMs,
      meta: cfg.meta,
    };

    if (!isNewSubmission) {
      const existingState = await readState(jobId);
      if (existingState) return jobId;
      // Recover from partial failures where idempotency key was written
      // but state/queue write did not complete.
      await workQueue.send({
        data: payload,
        delayMs,
        idempotencyKey: cfg.key,
        idempotencyTtlMs: keyTtlMs,
        meta: cfg.meta,
      });

      const wrote = await writeStateIfAbsent(submittedState);
      if (wrote) {
        await emitEvent(jobId, {
          type: "submitted",
          id: jobId,
          ts: now(),
        });
      }
      return jobId;
    } else {
      await workQueue.send({
        data: payload,
        delayMs,
        idempotencyKey: cfg.key,
        idempotencyTtlMs: keyTtlMs,
        meta: cfg.meta,
      });

      const wrote = await writeStateIfAbsent(submittedState);
      if (wrote) {
        await emitEvent(jobId, {
          type: "submitted",
          id: jobId,
          ts: now(),
        });
      }

      return jobId;
    }
  };

  const join = async (cfg: { id: JobId } & JoinOptions): Promise<JobTerminal<Result>> => {
    // Check immediately — may already be done
    const state = await readState(cfg.id);
    if (state && isTerminalStatus(state.status)) {
      return {
        id: cfg.id,
        status: state.status,
        result: state.result,
        error: state.error,
        finishedAt: state.finishedAt ?? state.updatedAt,
      };
    }

    const ac = new AbortController();
    let timeout: ReturnType<typeof setTimeout> | null = null;

    if (cfg.timeoutMs !== undefined) {
      timeout = setTimeout(() => ac.abort(), cfg.timeoutMs);
    }

    try {
      for await (const event of eventsTopicFor(cfg.id).live({
        after: "0-0",
        signal: ac.signal,
        timeoutMs: cfg.timeoutMs ?? 30_000,
      })) {
        if (
          event.data.type === "completed" ||
          event.data.type === "failed" ||
          event.data.type === "cancelled"
        ) {
          const finalState = await readState(cfg.id);
          if (finalState && isTerminalStatus(finalState.status)) {
            return {
              id: cfg.id,
              status: finalState.status,
              result: finalState.result,
              error: finalState.error,
              finishedAt: finalState.finishedAt ?? finalState.updatedAt,
            };
          }
        }
      }
    } catch {
      // aborted or timed out — fall through
    } finally {
      if (timeout) clearTimeout(timeout);
    }

    // One final check after the stream ends
    const finalState = await readState(cfg.id);
    if (finalState && isTerminalStatus(finalState.status)) {
      return {
        id: cfg.id,
        status: finalState.status,
        result: finalState.result,
        error: finalState.error,
        finishedAt: finalState.finishedAt ?? finalState.updatedAt,
      };
    }

    return {
      id: cfg.id,
      status: "timed_out",
      error: {
        message: "join timed out",
        code: "JOIN_TIMEOUT",
      },
      finishedAt: now(),
    };
  };

  const cancel = async (cfg: { id: JobId } & CancelOptions): Promise<void> => {
    const existing = await readState(cfg.id);
    if (!existing) return;
    if (isTerminalStatus(existing.status)) return;

    const finishedAt = now();

    const wrote = await writeCancelledState({
      id: cfg.id,
      status: "cancelled",
      attempts: existing.attempts,
      updatedAt: finishedAt,
      finishedAt,
      error: cfg.reason
        ? {
            message: cfg.reason,
            code: "CANCELLED",
          }
        : undefined,
    });
    if (!wrote) return;

    activeJobRuns.get(runningJobKey(cfg.id))?.abort();

    await emitEvent(cfg.id, {
      type: "cancelled",
      id: cfg.id,
      reason: cfg.reason,
      ts: finishedAt,
    });
  };

  const events = (id: JobId): JobEvents => {
    const t = eventsTopicFor(id);
    return {
      reader: t.reader,
      live: t.live,
    };
  };

  const stop = (): void => {
    const ac = activeWorkers.get(workerId);
    if (ac) {
      ac.abort();
      activeWorkers.delete(workerId);
    }
  };

  return {
    id: definition.id,
    submit,
    validateInput: (input: unknown): void => {
      const parsed = definition.schema.safeParse(input);
      if (!parsed.success) throw parsed.error;
    },
    join,
    cancel,
    events,
    stop,
  };
};
