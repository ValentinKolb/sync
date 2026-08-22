import { createHash } from "node:crypto";
import { AckPolicy, DeliverPolicy, DiscardPolicy, RetentionPolicy, StorageType } from "@nats-io/jetstream";
import type { Consumer, JsMsg } from "@nats-io/jetstream";
import { decodeEnvelope, encodeEnvelope } from "./codec.ts";
import type { Envelope, JsonValue } from "./codec.ts";
import { confirmedAck, runPullLoop } from "./consume.ts";
import { InvalidNameError, asError } from "./errors.ts";
import { assertName, consumerName, dlqStreamName, resourceIdentity, streamName, subjectRoot, subjectToken, assertSubjectLength } from "./naming.ts";
import type { ResourceIdentity, SyncResourceKind } from "./naming.ts";
import { ensureConsumer, ensureStream } from "./resources.ts";
import type { ProvisionContext } from "./resources.ts";
import type { SyncRuntime } from "./runtime.ts";
import {
  DEFAULT_DEDUPE_WINDOW_MS,
  DEFAULT_MESSAGE_PAYLOAD_BYTES,
  DEFAULT_TENANT,
  assertRetention,
  backoffDelayMs,
  nanos,
  resolveDelivery,
} from "./types.ts";
import type { DeliveryConfig, MessageMeta, OrderingConfig, PublishReceipt, RetentionConfig } from "./types.ts";
import { createWorkerRuntime } from "./worker.ts";
import type { ProcessOptions, Worker } from "./worker.ts";

// ==========================
// Types
// ==========================

export type QueueConfig = {
  id: string;
  owner?: string;
  delivery?: DeliveryConfig;
  retention?: RetentionConfig;
  /** One stream-level NATS duplicate window. Default 120_000. */
  dedupeWindowMs?: number;
  ordering?: OrderingConfig;
  maxPayloadBytes?: number;
  replicas?: number;
};

export type QueueSend<T> = {
  data: T;
  tenantId?: string;
  idempotencyKey?: string;
  orderingKey?: string;
  delayMs?: number;
  at?: Date;
  meta?: MessageMeta;
};

export type QueueMessage<T> = {
  data: T;
  messageId: string;
  attempt: number;
  publishedAt: Date;
  tenantId: string;
  orderingKey?: string;
  meta?: MessageMeta;
  signal: AbortSignal;
  /** In-progress acknowledgement: resets ackWaitMs for this delivery. */
  heartbeat(): Promise<void>;
};

export type QueueDelivery<T> = QueueMessage<T> & {
  ack(): Promise<void>;
  retry(options?: { delayMs?: number; reason?: string }): Promise<void>;
  deadLetter(options: { reason: string; error?: string }): Promise<void>;
};

export type QueueReader<T> = {
  receive(options?: { waitMs?: number; signal?: AbortSignal }): Promise<QueueDelivery<T> | null>;
  stream(options?: { signal?: AbortSignal }): AsyncIterable<QueueDelivery<T>>;
  close(): Promise<void>;
  [Symbol.asyncDispose](): Promise<void>;
};

export type DeadLetter<T> = {
  messageId: string;
  data: T;
  tenantId: string;
  attempts: number;
  failedAt: Date;
  reason: string;
  error?: string;
};

export type DeadLetterStore<T> = {
  list(options?: { limit?: number; after?: string }): Promise<DeadLetter<T>[]>;
  requeue(input: { messageId: string; idempotencyKey: string }): Promise<PublishReceipt>;
  delete(input: { messageId: string }): Promise<boolean>;
};

export type Queue<T> = {
  ready(): Promise<void>;
  send(message: QueueSend<T>): Promise<PublishReceipt>;
  process(options: ProcessOptions, handler: (message: QueueMessage<T>) => Promise<void>): Promise<Worker>;
  reader(options?: { signal?: AbortSignal }): Promise<QueueReader<T>>;
  deadLetters: DeadLetterStore<T>;
};

// ==========================
// Internal queue core (shared with job)
// ==========================

export type PreparedSend = {
  messageId: string;
  bytes: Uint8Array;
  byteLength: number;
};

export type QueueCore<T, D = T> = {
  identity: ResourceIdentity;
  declarationReady(): Promise<void>;
  send(message: QueueSend<T>, ext?: Record<string, JsonValue>): Promise<PublishReceipt>;
  /** Encode a message now (byte size known), publish it later — for bounded fan-out. */
  prepareSend(message: QueueSend<T>, ext?: Record<string, JsonValue>): Promise<PreparedSend & { publish(): Promise<PublishReceipt> }>;
  process(
    options: ProcessOptions,
    handler: (message: QueueMessage<T>, envelope: Envelope, msg: JsMsg) => Promise<void>,
    onFailure?: (input: {
      message: QueueMessage<T>;
      envelope: Envelope;
      error: Error;
    }) => Promise<{ action: "retry"; delayMs?: number } | { action: "dead_letter"; reason: string }>,
  ): Promise<Worker>;
  reader(options?: { signal?: AbortSignal }): Promise<QueueReader<T>>;
  deadLetters: DeadLetterStore<D>;
  maxPayloadBytes: number;
  dedupeWindowMs: number;
};

const partitionOf = (orderingKey: string, partitions: number): number => {
  const digest = createHash("sha256").update(orderingKey, "utf8").digest();
  return digest.readUInt32BE(0) % partitions;
};

export const scopedMessageId = (tenantId: string, key: string): string =>
  `k.${subjectToken(tenantId, "tenantId")}.${subjectToken(key, "idempotencyKey")}`;

export const createQueueCore = <T, D = T>(
  runtime: SyncRuntime,
  config: QueueConfig & { dlqMaxAgeMs?: number },
  kind: SyncResourceKind,
  deadLetterData: (envelope: Envelope) => D = (envelope) => envelope.data as D,
): QueueCore<T, D> => {
  const identity = resourceIdentity(runtime.namespace, kind, config.id);
  const owner = config.owner ?? runtime.application;
  const delivery = resolveDelivery(config.delivery);
  const retention = assertRetention(config.retention ?? { maxAgeMs: 7 * 24 * 60 * 60 * 1_000, maxBytes: 1024 ** 3 });
  const dedupeWindowMs = config.dedupeWindowMs ?? DEFAULT_DEDUPE_WINDOW_MS;
  const maxPayloadBytes = config.maxPayloadBytes ?? DEFAULT_MESSAGE_PAYLOAD_BYTES;
  const replicas = config.replicas ?? runtime.defaults.replicas;
  const storage = runtime.defaults.storage === "memory" ? StorageType.Memory : StorageType.File;
  const ordering: OrderingConfig = config.ordering ?? { mode: "none" };
  if (ordering.mode === "partitioned") {
    if (!Number.isSafeInteger(ordering.partitions) || ordering.partitions < 1 || ordering.partitions > 1_024) {
      throw new RangeError("ordering.partitions must be an integer between 1 and 1024");
    }
  }

  const stream = streamName(identity);
  const dlqStream = dlqStreamName(identity);
  const root = subjectRoot(identity);
  const label = `${kind} ${config.id}`;

  const workSubject = (tenantId: string, partition: number | null): string => {
    const base = `${root}.t.${subjectToken(tenantId, "tenantId")}.work`;
    const subject = partition === null ? base : `${base}.p${partition}`;
    assertSubjectLength(subject);
    return subject;
  };
  const dlqSubject = (tenantId: string): string => `${root}.t.${subjectToken(tenantId, "tenantId")}.dlq`;

  const workFilter = ordering.mode === "partitioned" ? `${root}.t.*.work.*` : `${root}.t.*.work`;
  const partitionFilter = (partition: number): string => `${root}.t.*.work.p${partition}`;
  const durableFor = (partition: number | null): string =>
    partition === null ? `S6_QC_${identity.hash}` : `S6_QC_${identity.hash}_p${partition}`;

  const declaration = runtime.declare({
    identity,
    owner,
    configKey: JSON.stringify([
      kind,
      config.id,
      owner,
      delivery,
      retention,
      dedupeWindowMs,
      ordering,
      maxPayloadBytes,
      replicas,
      config.dlqMaxAgeMs ?? null,
    ]),
    natsNames: [stream, dlqStream],
    provision: async (ctx: ProvisionContext) => {
      await ensureStream(ctx, identity, owner, {
        name: stream,
        subjects: [workFilter, `${root}.t.*.delay.>`],
        retention: RetentionPolicy.Workqueue,
        discard: DiscardPolicy.Old,
        storage,
        num_replicas: replicas,
        max_age: nanos(retention.maxAgeMs),
        max_bytes: retention.maxBytes,
        max_msgs: retention.maxMessages ?? -1,
        max_msg_size: -1,
        duplicate_window: nanos(dedupeWindowMs),
        allow_msg_schedules: true,
        allow_msg_ttl: true,
      });
      await ensureStream(ctx, identity, owner, {
        name: dlqStream,
        subjects: [`${root}.t.*.dlq`],
        retention: RetentionPolicy.Limits,
        discard: DiscardPolicy.Old,
        storage,
        num_replicas: replicas,
        max_age: nanos(config.dlqMaxAgeMs ?? retention.maxAgeMs),
        max_bytes: retention.maxBytes,
        max_msgs: -1,
        max_msg_size: -1,
        duplicate_window: nanos(dedupeWindowMs),
      });
    },
    summary: async (ctx: ProvisionContext) => {
      const info = await ctx.jsm.streams.info(stream);
      const dlq = await ctx.jsm.streams.info(dlqStream);
      const consumers: Record<string, JsonValue> = {};
      const partitions = ordering.mode === "partitioned" ? ordering.partitions : 1;
      for (let p = 0; p < partitions; p++) {
        const durable = durableFor(ordering.mode === "partitioned" ? p : null);
        const consumerInfo = await ctx.jsm.consumers.info(stream, durable).catch(() => null);
        if (consumerInfo) {
          consumers[durable] = {
            pending: consumerInfo.num_pending,
            ackPending: consumerInfo.num_ack_pending,
            redelivered: consumerInfo.num_redelivered,
          };
        }
      }
      return {
        messages: info.state.messages,
        bytes: info.state.bytes,
        deadLetters: dlq.state.messages,
        consumers,
      } satisfies Record<string, JsonValue>;
    },
  });

  const ensureWorkConsumers = async (ctx: ProvisionContext): Promise<string[]> => {
    if (ordering.mode === "partitioned") {
      const durables: string[] = [];
      for (let p = 0; p < ordering.partitions; p++) {
        const durable = durableFor(p);
        await ensureConsumer(ctx, stream, {
          durable_name: durable,
          ack_policy: AckPolicy.Explicit,
          filter_subject: partitionFilter(p),
          ack_wait: nanos(delivery.ackWaitMs),
          max_deliver: delivery.maxAttempts + 1,
          // Serial per-partition delivery is the whole point of partitioning.
          max_ack_pending: 1,
          deliver_policy: DeliverPolicy.All,
        });
        durables.push(durable);
      }
      return durables;
    }
    const durable = durableFor(null);
    await ensureConsumer(ctx, stream, {
      durable_name: durable,
      ack_policy: AckPolicy.Explicit,
      filter_subject: workFilter,
      ack_wait: nanos(delivery.ackWaitMs),
      max_deliver: delivery.maxAttempts + 1,
      max_ack_pending: delivery.maxInFlight,
      deliver_policy: DeliverPolicy.All,
    });
    return [durable];
  };

  // ==========================
  // Send
  // ==========================

  const prepareSend: QueueCore<T, D>["prepareSend"] = async (message, ext) => {
    runtime.assertActive();
    await declaration.ready();
    const tenantId = message.tenantId ?? DEFAULT_TENANT;
    if (message.delayMs !== undefined && message.at !== undefined) {
      throw new InvalidNameError("delayMs and at are mutually exclusive");
    }
    if (ordering.mode === "partitioned" && message.orderingKey === undefined) {
      throw new InvalidNameError(`${label} is partitioned; send() requires an orderingKey`);
    }
    const messageId = message.idempotencyKey
      ? scopedMessageId(tenantId, message.idempotencyKey)
      : crypto.randomUUID();
    const envelope: Envelope = {
      v: 6,
      data: message.data,
      tenantId,
      publishedAt: new Date().toISOString(),
      ...(message.orderingKey !== undefined ? { orderingKey: message.orderingKey } : {}),
      ...(message.meta !== undefined ? { meta: message.meta } : {}),
      ext: { ...ext, messageId },
    };
    const bytes = encodeEnvelope(label, envelope, maxPayloadBytes);
    const partition = ordering.mode === "partitioned" ? partitionOf(message.orderingKey!, ordering.partitions) : null;
    const target = workSubject(tenantId, partition);
    const fireAt = message.at ?? (message.delayMs !== undefined ? new Date(Date.now() + message.delayMs) : null);

    const publish = async (): Promise<PublishReceipt> => {
      const ctx = await runtime.context();
      if (fireAt !== null && fireAt.getTime() > Date.now()) {
        const delaySubject = `${root}.t.${subjectToken(tenantId, "tenantId")}.delay.${crypto.randomUUID()}`;
        const ack = await ctx.js.publish(delaySubject, bytes, {
          msgID: messageId,
          schedule: { specification: { at: fireAt }, target, ttl: "never" },
        });
        return { messageId, streamSequence: ack.seq, duplicate: ack.duplicate };
      }
      const ack = await ctx.js.publish(target, bytes, { msgID: messageId });
      return { messageId, streamSequence: ack.seq, duplicate: ack.duplicate };
    };

    return { messageId, bytes, byteLength: bytes.byteLength, publish };
  };

  const send: QueueCore<T, D>["send"] = async (message, ext) => {
    const prepared = await prepareSend(message, ext);
    return prepared.publish();
  };

  // ==========================
  // Delivery handling
  // ==========================

  const toMessage = (envelope: Envelope, msg: JsMsg, signal: AbortSignal): QueueMessage<T> => ({
    data: envelope.data as T,
    messageId: (envelope.ext?.messageId as string) ?? `seq-${msg.seq}`,
    attempt: msg.info.deliveryCount,
    publishedAt: new Date(envelope.publishedAt),
    tenantId: envelope.tenantId,
    orderingKey: envelope.orderingKey,
    meta: envelope.meta,
    signal,
    heartbeat: async () => {
      msg.working();
    },
  });

  const deadLetterTransfer = async (
    ctx: ProvisionContext,
    msg: JsMsg,
    envelope: Envelope | null,
    reason: string,
    error?: string,
  ): Promise<void> => {
    const messageId = (envelope?.ext?.messageId as string) ?? `seq-${msg.seq}`;
    const tenantId = envelope?.tenantId ?? DEFAULT_TENANT;
    const dlqEnvelope: Envelope = {
      v: 6,
      data: envelope?.data ?? null,
      tenantId,
      publishedAt: new Date().toISOString(),
      ext: {
        ...envelope?.ext,
        messageId,
        attempts: msg.info.deliveryCount,
        reason,
        failedAt: new Date().toISOString(),
        ...(error !== undefined ? { error } : {}),
      },
    };
    const bytes = encodeEnvelope(`${label} dlq`, dlqEnvelope, maxPayloadBytes);
    // The original message ID keys the DLQ dedupe window, so a crash between
    // DLQ publish and source ack repeats the transfer without duplicating it.
    await ctx.js.publish(dlqSubject(tenantId), bytes, { msgID: `dlq.${messageId}` });
    await confirmedAck(msg, label).catch(() => {});
    runtime.events.emit({
      type: "dead_letter",
      resource: config.id,
      kind,
      detail: { messageId, reason },
    });
  };

  const process: QueueCore<T>["process"] = async (options, handler, onFailure) => {
    runtime.assertActive();
    await declaration.ready();
    const ctx = await runtime.context();
    const durables = await ensureWorkConsumers(ctx);

    const wr = createWorkerRuntime(options, { onFinished: () => runtime.unregisterWorker(wr) });
    runtime.registerWorker(wr);
    runtime.events.emit({ type: "worker_started", resource: config.id, kind });

    const onMessage = async (msg: JsMsg, signal: AbortSignal): Promise<void> => {
      let envelope: Envelope;
      try {
        envelope = decodeEnvelope(msg.data);
      } catch (error) {
        await deadLetterTransfer(ctx, msg, null, "invalid envelope", asError(error).message);
        return;
      }
      const attempt = msg.info.deliveryCount;
      if (attempt > delivery.maxAttempts) {
        await deadLetterTransfer(ctx, msg, envelope, "max attempts exhausted");
        return;
      }
      const message = toMessage(envelope, msg, signal);
      try {
        await handler(message, envelope, msg);
        await confirmedAck(msg, label);
      } catch (error) {
        if (signal.aborted) {
          msg.nak();
          return;
        }
        const err = asError(error);
        runtime.events.emit({ type: "handler_error", resource: config.id, kind, error: err.message });
        let decision: { action: "retry"; delayMs?: number } | { action: "dead_letter"; reason: string } = {
          action: "retry",
        };
        if (onFailure) {
          try {
            decision = await onFailure({ message, envelope, error: err });
          } catch {
            // A throwing failure policy must never settle work accidentally.
            decision = { action: "retry" };
          }
        }
        if (decision.action === "dead_letter") {
          await deadLetterTransfer(ctx, msg, envelope, decision.reason, err.message);
        } else if (attempt >= delivery.maxAttempts) {
          await deadLetterTransfer(ctx, msg, envelope, "max attempts exhausted", err.message);
        } else {
          msg.nak(decision.delayMs ?? backoffDelayMs(delivery, attempt));
          runtime.events.emit({ type: "redelivery", resource: config.id, kind, detail: { attempt } });
        }
      }
    };

    const loops = durables.map((durable) =>
      runPullLoop(wr, () => ctx.js.consumers.get(stream, durable), onMessage, {
        events: runtime.events,
        resource: config.id,
      }),
    );
    Promise.all(loops).finally(() => {
      runtime.events.emit({ type: "worker_stopped", resource: config.id, kind });
    });
    return wr.worker;
  };

  // ==========================
  // Manual reader
  // ==========================

  const reader: QueueCore<T>["reader"] = async (options = {}) => {
    runtime.assertActive();
    if (ordering.mode === "partitioned") {
      throw new InvalidNameError(`${label} is partitioned; use process() — reader() supports unpartitioned queues`);
    }
    await declaration.ready();
    const ctx = await runtime.context();
    const [durable] = await ensureWorkConsumers(ctx);
    let closed = false;
    const controller = new AbortController();
    options.signal?.addEventListener("abort", () => controller.abort(), { once: true });

    const toDelivery = (msg: JsMsg): QueueDelivery<T> | null => {
      let envelope: Envelope;
      try {
        envelope = decodeEnvelope(msg.data);
      } catch (error) {
        void deadLetterTransfer(ctx, msg, null, "invalid envelope", asError(error).message);
        return null;
      }
      const message = toMessage(envelope, msg, controller.signal);
      return {
        ...message,
        ack: () => confirmedAck(msg, label),
        retry: async (retryOptions = {}) => {
          if (message.attempt >= delivery.maxAttempts) {
            await deadLetterTransfer(ctx, msg, envelope, retryOptions.reason ?? "retry requested on final attempt");
            return;
          }
          msg.nak(retryOptions.delayMs ?? backoffDelayMs(delivery, message.attempt));
        },
        deadLetter: (dlOptions) => deadLetterTransfer(ctx, msg, envelope, dlOptions.reason, dlOptions.error),
      };
    };

    const receive = async (receiveOptions: { waitMs?: number; signal?: AbortSignal } = {}) => {
      if (closed || controller.signal.aborted) return null;
      const consumer = await ctx.js.consumers.get(stream, durable!);
      // NATS requires a fetch expiry of at least one second.
      const batch = await consumer.fetch({ max_messages: 1, expires: Math.max(receiveOptions.waitMs ?? 5_000, 1_000) });
      const stop = (): void => batch.stop();
      receiveOptions.signal?.addEventListener("abort", stop, { once: true });
      controller.signal.addEventListener("abort", stop, { once: true });
      try {
        for await (const msg of batch) {
          const queueDelivery = toDelivery(msg);
          if (queueDelivery !== null) return queueDelivery;
        }
      } finally {
        receiveOptions.signal?.removeEventListener("abort", stop);
        controller.signal.removeEventListener("abort", stop);
      }
      return null;
    };

    const queueReader: QueueReader<T> = {
      receive,
      stream: (streamOptions = {}) => ({
        async *[Symbol.asyncIterator]() {
          while (!closed && !controller.signal.aborted && !streamOptions.signal?.aborted) {
            const queueDelivery = await receive({ signal: streamOptions.signal });
            if (queueDelivery !== null) yield queueDelivery;
          }
        },
      }),
      close: async () => {
        closed = true;
        controller.abort();
      },
      [Symbol.asyncDispose]: async () => {
        closed = true;
        controller.abort();
      },
    };
    return queueReader;
  };

  // ==========================
  // Dead letter store
  // ==========================

  const scanDeadLetters = async function* (): AsyncGenerator<{ seq: number; envelope: Envelope }> {
    const ctx = await runtime.context();
    const info = await ctx.jsm.streams.info(dlqStream).catch(() => null);
    if (info === null || info.state.messages === 0) return;
    const lastSeq = info.state.last_seq;
    const consumer = await ctx.js.consumers.get(dlqStream);
    const messages = await consumer.consume({ max_messages: 256 });
    try {
      for await (const msg of messages) {
        try {
          yield { seq: msg.seq, envelope: decodeEnvelope(msg.data) };
        } catch {
          // Skip foreign messages.
        }
        if (msg.seq >= lastSeq) return;
      }
    } finally {
      messages.stop();
    }
  };

  const findDeadLetter = async (messageId: string): Promise<{ seq: number; envelope: Envelope } | null> => {
    for await (const entry of scanDeadLetters()) {
      if (entry.envelope.ext?.messageId === messageId) return entry;
    }
    return null;
  };

  const toDeadLetter = (envelope: Envelope): DeadLetter<D> => ({
    messageId: (envelope.ext?.messageId as string) ?? "",
    data: deadLetterData(envelope),
    tenantId: envelope.tenantId,
    attempts: (envelope.ext?.attempts as number) ?? 0,
    failedAt: new Date((envelope.ext?.failedAt as string) ?? envelope.publishedAt),
    reason: (envelope.ext?.reason as string) ?? "unknown",
    ...(envelope.ext?.error !== undefined ? { error: envelope.ext.error as string } : {}),
  });

  const deadLetters: DeadLetterStore<D> = {
    list: async (options = {}) => {
      await declaration.ready();
      const limit = options.limit ?? 100;
      const entries: DeadLetter<D>[] = [];
      let skipping = options.after !== undefined;
      for await (const entry of scanDeadLetters()) {
        if (skipping) {
          if (entry.envelope.ext?.messageId === options.after) skipping = false;
          continue;
        }
        entries.push(toDeadLetter(entry.envelope));
        if (entries.length >= limit) break;
      }
      return entries;
    },
    requeue: async (input) => {
      await declaration.ready();
      assertName(input.idempotencyKey, "idempotencyKey");
      const entry = await findDeadLetter(input.messageId);
      if (entry === null) throw new InvalidNameError(`dead letter ${input.messageId} not found`);
      const ctx = await runtime.context();
      // Preserve primitive-specific ext fields (e.g. a job key) on requeue.
      const { attempts: _a, reason: _r, failedAt: _f, error: _e, messageId: _m, ...restExt } = entry.envelope.ext ?? {};
      const receipt = await send(
        {
          data: entry.envelope.data as T,
          tenantId: entry.envelope.tenantId,
          idempotencyKey: input.idempotencyKey,
          ...(entry.envelope.orderingKey !== undefined ? { orderingKey: entry.envelope.orderingKey } : {}),
          ...(entry.envelope.meta !== undefined ? { meta: entry.envelope.meta } : {}),
        },
        restExt,
      );
      await ctx.jsm.streams.deleteMessage(dlqStream, entry.seq).catch(() => {});
      return receipt;
    },
    delete: async (input) => {
      await declaration.ready();
      const entry = await findDeadLetter(input.messageId);
      if (entry === null) return false;
      const ctx = await runtime.context();
      return ctx.jsm.streams.deleteMessage(dlqStream, entry.seq).catch(() => false);
    },
  };

  return {
    identity,
    declarationReady: () => declaration.ready(),
    send,
    prepareSend,
    process,
    reader,
    deadLetters,
    maxPayloadBytes,
    dedupeWindowMs,
  };
};

// ==========================
// Public queue factory
// ==========================

export const createQueue = <T>(runtime: SyncRuntime, config: QueueConfig): Queue<T> => {
  const core = createQueueCore<T>(runtime, config, "queue");
  return {
    ready: () => core.declarationReady(),
    send: (message) => core.send(message),
    process: (options, handler) => core.process(options, (message) => handler(message)),
    reader: core.reader,
    deadLetters: core.deadLetters,
  };
};
