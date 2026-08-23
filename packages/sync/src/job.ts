import type { KV } from "@nats-io/kv";
import { decodeJson, encodeJson, extString } from "./codec.ts";
import type { JsonValue } from "./codec.ts";
import { BatchSubmitError, asError } from "./errors.ts";
import { assertName, kvBucketName, resourceIdentity, subjectToken } from "./naming.ts";
import { ensureKv, toStorageType } from "./resources.ts";
import { createQueueCore } from "./queue.ts";
import type { BatchReceipt, DeadLetterStore, PauseInfo, QueueConfig } from "./queue.ts";
import type { SyncRuntime } from "./runtime.ts";
import type { MessageMeta, PublishReceipt } from "./types.ts";
import type { ProcessOptions, Worker } from "./worker.ts";

// ==========================
// Types
// ==========================

export type JobConfig = QueueConfig & {
  /** Retention for diagnostics and dead letters. Default 7 days. */
  terminalRetentionMs?: number;
};

export type JobSubmit<Input> = {
  /** Required idempotent submission key (the NATS message ID within the dedupe window). */
  key: string;
  input: Input;
  /**
   * Coalescing dedupe: at most one queued-or-running job per key; the key is
   * released when the job settles (success or dead letter) — not after a time
   * window. Coalesced submissions skip the windowed NATS dedupe, so a key can
   * be resubmitted immediately after completion.
   */
  coalesce?: boolean;
  tenantId?: string;
  delayMs?: number;
  at?: Date;
  orderingKey?: string;
  meta?: MessageMeta;
};

export type JobContext<Input> = {
  jobId: string;
  key: string;
  input: Input;
  attempt: number;
  failureCount: number;
  signal: AbortSignal;
  heartbeat(): Promise<void>;
};

export type JobFailureDecision = { action: "retry"; delayMs?: number } | { action: "dead_letter"; reason: string };

export type JobProcessOptions<Input> = ProcessOptions & {
  onError?: (input: { context: JobContext<Input>; error: Error }) => JobFailureDecision | Promise<JobFailureDecision>;
};

export type JobSubmitManyOptions = {
  /** Bounded number of in-flight publish promises. Default 16. */
  publishConcurrency?: number;
  /** Bounded bytes of in-flight publishes — local backpressure. Default 8 MiB. */
  maxPendingBytes?: number;
  signal?: AbortSignal;
};

export type Job<Input> = {
  ready(): Promise<void>;
  submit(job: JobSubmit<Input>): Promise<PublishReceipt & { jobId: string }>;
  submitMany(
    jobs: Iterable<JobSubmit<Input>> | AsyncIterable<JobSubmit<Input>>,
    options?: JobSubmitManyOptions,
  ): Promise<{ accepted: number; duplicates: number }>;
  /**
   * Atomic all-or-nothing submission of up to 1000 jobs (no delays). Unlike
   * submit(), keys are NOT deduplicated (NATS batches carry no message ids) —
   * resubmitting a committed batch duplicates its jobs.
   */
  submitBatch(jobs: JobSubmit<Input>[]): Promise<BatchReceipt>;
  /** Pause delivery on the durable consumer — global, all pods. Submissions continue. */
  pause(options?: { untilMs?: number }): Promise<PauseInfo>;
  resume(): Promise<PauseInfo>;
  process(options: JobProcessOptions<Input>, handler: (context: JobContext<Input>) => Promise<void>): Promise<Worker>;
  deadLetters: DeadLetterStore<{ key: string; input: Input }>;
};

// ==========================
// Job factory
// ==========================

export const createJob = <Input>(runtime: SyncRuntime, config: JobConfig): Job<Input> => {
  const terminalRetentionMs = config.terminalRetentionMs ?? 7 * 24 * 60 * 60 * 1_000;
  const identity = resourceIdentity(runtime.namespace, "job", config.id);
  const claimsBucket = kvBucketName(identity);
  let claims: KV | null = null;

  const claimKey = (tenantId: string, key: string): string =>
    `c.${subjectToken(tenantId, "tenantId")}.${subjectToken(key, "job key")}`;

  const getClaims = async (): Promise<KV> => {
    await core.declarationReady();
    if (claims === null) {
      const ctx = await runtime.context();
      claims = await ctx.kvm.open(claimsBucket);
    }
    return claims;
  };

  /**
   * Release-on-completion: delete the claim BEFORE the settlement ack, so a
   * crash retries the release together with the redelivery.
   */
  const releaseClaim = async (tenantId: string, key: string): Promise<void> => {
    const store = await getClaims();
    await store.purge(claimKey(tenantId, key)).catch(() => {});
  };

  const core = createQueueCore<Input, { key: string; input: Input }>(
    runtime,
    {
      ...config,
      dlqMaxAgeMs: terminalRetentionMs,
      extraNatsNames: [`KV_${claimsBucket}`],
      provisionExtra: async (ctx) => {
        claims = await ensureKv(ctx, identity, config.owner ?? runtime.application, claimsBucket, {
          history: 1,
          replicas: config.replicas ?? runtime.defaults.replicas,
          storage: toStorageType(runtime.defaults.storage),
          markerTTL: 1_000,
        });
      },
      onDeadLetter: async (envelope) => {
        const key = extString(envelope, "key");
        if (envelope !== null && key !== undefined && extString(envelope, "coalesce") === "1") {
          await releaseClaim(envelope.tenantId, key);
        }
      },
    },
    "job",
    (envelope) => ({ key: extString(envelope, "key") ?? "", input: envelope.data as Input }),
  );

  const toSend = (job: JobSubmit<Input>) => {
    assertName(job.key, "job key");
    return {
      message: {
        data: job.input,
        tenantId: job.tenantId,
        // Coalesced submissions dedupe via the claim, not the window — a
        // windowed message id would swallow the resubmit after completion.
        idempotencyKey: job.coalesce === true ? undefined : job.key,
        delayMs: job.delayMs,
        at: job.at,
        orderingKey: job.orderingKey,
        meta: job.meta,
      },
      ext: {
        key: job.key,
        ...(job.coalesce === true ? { coalesce: "1" } : {}),
      } satisfies Record<string, JsonValue>,
    };
  };

  const submit: Job<Input>["submit"] = async (job) => {
    const { message, ext } = toSend(job);
    if (job.coalesce === true) {
      const store = await getClaims();
      const tenantId = job.tenantId ?? "default";
      const ttl = `${Math.max(1, Math.ceil((config.retention?.maxAgeMs ?? 7 * 24 * 60 * 60 * 1_000) / 1_000))}s`;
      const raw = claimKey(tenantId, job.key);
      let claimed: number | null = null;
      try {
        claimed = await store.create(raw, encodeJson(`job ${config.id} claim`, { pending: true }, 4_096), ttl);
      } catch {
        claimed = null;
      }
      if (claimed === null) {
        const existing = await store.get(raw);
        if (existing !== null && existing.operation === "PUT") {
          const stored = decodeJson<{ jobId?: string; seq?: number }>(existing.value);
          return {
            messageId: stored.jobId ?? "",
            streamSequence: stored.seq ?? 0,
            duplicate: true,
            jobId: stored.jobId ?? "",
          };
        }
        // Claim raced away (released between create and get) — retry once.
        return submit(job);
      }
      try {
        const receipt = await core.send(message, ext);
        await store
          .put(raw, encodeJson(`job ${config.id} claim`, { jobId: receipt.messageId, seq: receipt.streamSequence }, 4_096))
          .catch(() => {});
        return { ...receipt, jobId: receipt.messageId };
      } catch (error) {
        // Publish failed: free the claim instead of blocking the key until TTL.
        await store.purge(raw).catch(() => {});
        throw error;
      }
    }
    const receipt = await core.send(message, ext);
    return { ...receipt, jobId: receipt.messageId };
  };

  const submitMany: Job<Input>["submitMany"] = async (jobs, options = {}) => {
    const publishConcurrency = options.publishConcurrency ?? 16;
    const maxPendingBytes = options.maxPendingBytes ?? 8 * 1024 * 1024;
    if (!Number.isSafeInteger(publishConcurrency) || publishConcurrency < 1) {
      throw new RangeError("publishConcurrency must be a positive integer");
    }
    if (!Number.isSafeInteger(maxPendingBytes) || maxPendingBytes < 1) {
      throw new RangeError("maxPendingBytes must be a positive integer");
    }

    let accepted = 0;
    let duplicates = 0;
    let pendingBytes = 0;
    let firstError: Error | null = null;
    const inflight = new Set<Promise<void>>();

    const waitForCapacity = async (nextBytes: number): Promise<void> => {
      // Byte backpressure never deadlocks: a single oversized item may fly alone.
      while (
        inflight.size >= publishConcurrency ||
        (inflight.size > 0 && pendingBytes + nextBytes > maxPendingBytes)
      ) {
        await Promise.race(inflight);
        if (firstError !== null) return;
      }
    };

    try {
      for await (const job of jobs) {
        if (options.signal?.aborted) throw new Error("submitMany aborted");
        const { message, ext } = toSend(job);
        const prepared = await core.prepareSend(message, ext);
        await waitForCapacity(prepared.byteLength);
        if (firstError !== null) break;
        pendingBytes += prepared.byteLength;
        const task = prepared
          .publish()
          .then((receipt) => {
            if (receipt.duplicate) duplicates += 1;
            else accepted += 1;
          })
          .catch((error) => {
            firstError ??= asError(error);
          })
          .finally(() => {
            pendingBytes -= prepared.byteLength;
            inflight.delete(task);
          });
        inflight.add(task);
      }
    } catch (error) {
      firstError ??= asError(error);
    }
    await Promise.all(inflight);
    if (firstError !== null) {
      // Not atomic by design: prior accepted items stay accepted.
      throw new BatchSubmitError("submitMany failed", accepted, duplicates, firstError);
    }
    return { accepted, duplicates };
  };

  const process: Job<Input>["process"] = (options, handler) => {
    const { onError, ...processOptions } = options;
    const toContext = (message: {
      messageId: string;
      data: Input;
      attempt: number;
      signal: AbortSignal;
      heartbeat(): Promise<void>;
    }, key: string): JobContext<Input> => ({
      jobId: message.messageId,
      key,
      input: message.data,
      attempt: message.attempt,
      failureCount: message.attempt - 1,
      signal: message.signal,
      heartbeat: message.heartbeat,
    });
    return core.process(
      processOptions,
      async (message, envelope) => {
        await handler(toContext(message, extString(envelope, "key") ?? ""));
        if (extString(envelope, "coalesce") === "1") {
          // Release-on-completion happens before the ack (delete-then-ack).
          await releaseClaim(envelope.tenantId, extString(envelope, "key") ?? "");
        }
      },
      onError === undefined
        ? undefined
        : async ({ message, envelope, error }) => {
            const decision = await onError({
              context: toContext(message, extString(envelope, "key") ?? ""),
              error,
            });
            return decision.action === "retry"
              ? { action: "retry", ...(decision.delayMs !== undefined ? { delayMs: decision.delayMs } : {}) }
              : { action: "dead_letter", reason: decision.reason };
          },
    );
  };

  const submitBatch: Job<Input>["submitBatch"] = (jobs) => {
    for (const job of jobs) assertName(job.key, "job key");
    return core.sendBatch(
      jobs.map((job) => ({
        data: job.input,
        tenantId: job.tenantId,
        delayMs: job.delayMs,
        at: job.at,
        orderingKey: job.orderingKey,
        meta: job.meta,
      })),
      (index) => ({ key: jobs[index]!.key }),
    );
  };

  return {
    ready: () => core.declarationReady(),
    submit,
    submitMany,
    submitBatch,
    pause: core.pause,
    resume: core.resume,
    process,
    deadLetters: core.deadLetters,
  };
};
