import { AckPolicy, DeliverPolicy, DiscardPolicy, RetentionPolicy, StorageType } from "@nats-io/jetstream";
import type { Consumer, JsMsg } from "@nats-io/jetstream";
import { decodeEnvelope, encodeEnvelope } from "./codec.ts";
import type { Envelope, JsonValue } from "./codec.ts";
import { confirmedAck, runPullLoop } from "./consume.ts";
import { CursorMismatchError, RetentionGapError, asError } from "./errors.ts";
import { assertName, resourceIdentity, streamName, dlqStreamName, consumerName, subjectRoot, subjectToken, assertSubjectLength } from "./naming.ts";
import type { ResourceIdentity } from "./naming.ts";
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
import type { DeliveryConfig, MessageMeta, PublishReceipt, RetentionConfig } from "./types.ts";
import { createWorkerRuntime } from "./worker.ts";
import type { ProcessOptions, Worker } from "./worker.ts";

// ==========================
// Types
// ==========================

export type TopicConfig = {
  id: string;
  owner?: string;
  retention: RetentionConfig;
  dedupeWindowMs?: number;
  maxPayloadBytes?: number;
  replicas?: number;
};

/** Opaque, resource-bound cursor: identifies this topic and a stream sequence. */
export type TopicCursor = string;

export type TopicEvent<T> = {
  data: T;
  eventId: string;
  cursor: TopicCursor;
  tenantId: string;
  orderingKey?: string;
  publishedAt: Date;
  meta?: MessageMeta;
};

export type TopicLiveEvent<T> = Omit<TopicEvent<T>, "cursor">;

export type TopicPublish<T> = {
  data: T;
  tenantId?: string;
  idempotencyKey?: string;
  orderingKey?: string;
  meta?: MessageMeta;
};

export type TopicProcessOptions = ProcessOptions & {
  consumer: string;
  tenantId?: string;
  delivery?: DeliveryConfig;
  start?: "earliest" | "latest" | { after: TopicCursor };
};

export type Topic<T> = {
  ready(): Promise<void>;
  publish(input: TopicPublish<T>): Promise<PublishReceipt & { eventId: string; cursor: TopicCursor }>;
  latestCursor(options?: { tenantId?: string }): Promise<TopicCursor | null>;
  live(options?: { tenantId?: string; signal?: AbortSignal }): AsyncIterable<TopicLiveEvent<T>>;
  replay(options?: {
    tenantId?: string;
    after?: TopicCursor;
    until?: TopicCursor;
    signal?: AbortSignal;
  }): AsyncIterable<TopicEvent<T>>;
  follow(options?: { tenantId?: string; after?: TopicCursor; signal?: AbortSignal }): AsyncIterable<TopicEvent<T>>;
  process(
    options: TopicProcessOptions,
    handler: (event: TopicEvent<T> & { attempt: number; signal: AbortSignal }) => Promise<void>,
  ): Promise<Worker>;
};

// ==========================
// Cursor encoding
// ==========================

const cursorOf = (identity: ResourceIdentity, seq: number): TopicCursor => `s6t.${identity.subjectHash}.${seq}`;

const parseCursor = (identity: ResourceIdentity, cursor: TopicCursor, label: string): number => {
  const parts = cursor.split(".");
  const seq = Number(parts[2]);
  if (parts.length !== 3 || parts[0] !== "s6t" || !Number.isSafeInteger(seq) || seq < 0) {
    throw new CursorMismatchError(`${label} is not a valid topic cursor: ${cursor}`);
  }
  if (parts[1] !== identity.subjectHash) {
    throw new CursorMismatchError(`${label} was issued by a different topic resource`);
  }
  return seq;
};

// ==========================
// Topic factory
// ==========================

export const createTopic = <T>(runtime: SyncRuntime, config: TopicConfig): Topic<T> => {
  const identity = resourceIdentity(runtime.namespace, "topic", config.id);
  const owner = config.owner ?? runtime.application;
  const retention = assertRetention(config.retention);
  const dedupeWindowMs = config.dedupeWindowMs ?? DEFAULT_DEDUPE_WINDOW_MS;
  const maxPayloadBytes = config.maxPayloadBytes ?? DEFAULT_MESSAGE_PAYLOAD_BYTES;
  const replicas = config.replicas ?? runtime.defaults.replicas;
  const storage = runtime.defaults.storage === "memory" ? StorageType.Memory : StorageType.File;

  const stream = streamName(identity);
  const dlqStream = dlqStreamName(identity);
  const root = subjectRoot(identity);
  const eventSubject = (tenantId: string): string => {
    const subject = `${root}.t.${subjectToken(tenantId, "tenantId")}.event`;
    assertSubjectLength(subject);
    return subject;
  };
  const dlqSubject = (consumer: string): string => `${root}.dlq.${subjectToken(consumer, "consumer")}`;

  const declaration = runtime.declare({
    identity,
    owner,
    configKey: JSON.stringify(["topic", config.id, owner, retention, dedupeWindowMs, maxPayloadBytes, replicas]),
    natsNames: [stream, dlqStream],
    provision: async (ctx: ProvisionContext) => {
      await ensureStream(ctx, identity, owner, {
        name: stream,
        subjects: [`${root}.t.*.event`],
        retention: RetentionPolicy.Limits,
        discard: DiscardPolicy.Old,
        storage,
        num_replicas: replicas,
        max_age: nanos(retention.maxAgeMs),
        max_bytes: retention.maxBytes,
        max_msgs: retention.maxMessages ?? -1,
        max_msg_size: -1,
        duplicate_window: nanos(dedupeWindowMs),
      });
      await ensureStream(ctx, identity, owner, {
        name: dlqStream,
        subjects: [`${root}.dlq.>`],
        retention: RetentionPolicy.Limits,
        discard: DiscardPolicy.Old,
        storage,
        num_replicas: replicas,
        max_age: nanos(retention.maxAgeMs),
        max_bytes: retention.maxBytes,
        max_msgs: -1,
        max_msg_size: -1,
        duplicate_window: nanos(dedupeWindowMs),
      });
    },
    summary: async (ctx: ProvisionContext) => {
      const info = await ctx.jsm.streams.info(stream);
      const dlq = await ctx.jsm.streams.info(dlqStream);
      return {
        messages: info.state.messages,
        bytes: info.state.bytes,
        firstSequence: info.state.first_seq,
        lastSequence: info.state.last_seq,
        consumers: info.state.consumer_count,
        deadLetters: dlq.state.messages,
      } satisfies Record<string, JsonValue>;
    },
  });

  const toEvent = (envelope: Envelope, seq: number): TopicEvent<T> => ({
    data: envelope.data as T,
    eventId: (envelope.ext?.eventId as string) ?? String(seq),
    cursor: cursorOf(identity, seq),
    tenantId: envelope.tenantId,
    orderingKey: envelope.orderingKey,
    publishedAt: new Date(envelope.publishedAt),
    meta: envelope.meta,
  });

  const publish: Topic<T>["publish"] = async (input) => {
    runtime.assertActive();
    await declaration.ready();
    const tenantId = input.tenantId ?? DEFAULT_TENANT;
    const messageId = input.idempotencyKey
      ? `k.${subjectToken(tenantId, "tenantId")}.${subjectToken(input.idempotencyKey, "idempotencyKey")}`
      : crypto.randomUUID();
    const envelope: Envelope = {
      v: 6,
      data: input.data,
      tenantId,
      publishedAt: new Date().toISOString(),
      ...(input.orderingKey !== undefined ? { orderingKey: input.orderingKey } : {}),
      ...(input.meta !== undefined ? { meta: input.meta } : {}),
      ext: { eventId: messageId },
    };
    const bytes = encodeEnvelope(`topic ${config.id}`, envelope, maxPayloadBytes);
    const ctx = await runtime.context();
    const ack = await ctx.js.publish(eventSubject(tenantId), bytes, { msgID: messageId });
    return {
      messageId,
      streamSequence: ack.seq,
      duplicate: ack.duplicate,
      eventId: messageId,
      cursor: cursorOf(identity, ack.seq),
    };
  };

  const latestCursor: Topic<T>["latestCursor"] = async (options = {}) => {
    await declaration.ready();
    const ctx = await runtime.context();
    try {
      const msg = await ctx.jsm.streams.getMessage(stream, {
        last_by_subj: eventSubject(options.tenantId ?? DEFAULT_TENANT),
      });
      return msg === null ? null : cursorOf(identity, msg.seq);
    } catch (error) {
      if (/no message found/i.test(asError(error).message)) return null;
      throw error;
    }
  };

  const live: Topic<T>["live"] = (options = {}) => {
    const tenantId = options.tenantId ?? DEFAULT_TENANT;
    return {
      [Symbol.asyncIterator]: () => liveIterator(tenantId, options.signal),
    };
  };

  async function* liveIterator(tenantId: string, signal?: AbortSignal): AsyncGenerator<TopicLiveEvent<T>> {
    runtime.assertActive();
    await declaration.ready();
    const sub = runtime.nc.subscribe(eventSubject(tenantId));
    runtime.registerLiveSubscription(sub);
    const onAbort = (): void => {
      sub.unsubscribe();
    };
    signal?.addEventListener("abort", onAbort, { once: true });
    try {
      for await (const msg of sub) {
        let envelope: Envelope;
        try {
          envelope = decodeEnvelope(msg.data);
        } catch {
          continue;
        }
        yield {
          data: envelope.data as T,
          eventId: (envelope.ext?.eventId as string) ?? crypto.randomUUID(),
          tenantId: envelope.tenantId,
          orderingKey: envelope.orderingKey,
          publishedAt: new Date(envelope.publishedAt),
          meta: envelope.meta,
        };
      }
    } finally {
      signal?.removeEventListener("abort", onAbort);
      runtime.unregisterLiveSubscription(sub);
      if (!sub.isClosed()) sub.unsubscribe();
    }
  }

  /**
   * Durable-log read shared by replay() and follow(). Reads the stream without
   * a server-side subject filter so stream sequences stay contiguous: any gap
   * proves messages were removed by retention and is reported explicitly.
   */
  async function* logIterator(options: {
    tenantId: string;
    after?: TopicCursor;
    until?: TopicCursor;
    follow: boolean;
    signal?: AbortSignal;
  }): AsyncGenerator<TopicEvent<T>> {
    runtime.assertActive();
    await declaration.ready();
    const ctx = await runtime.context();
    const info = await ctx.jsm.streams.info(stream);
    const firstSeq = info.state.first_seq;
    const lastSeq = info.state.last_seq;

    const afterSeq = options.after === undefined ? null : parseCursor(identity, options.after, "after");
    let startSeq = afterSeq === null ? firstSeq : afterSeq + 1;
    if (afterSeq !== null && startSeq < firstSeq) {
      throw new RetentionGapError(cursorOf(identity, startSeq), cursorOf(identity, firstSeq));
    }
    const untilSeq = options.until === undefined ? lastSeq : parseCursor(identity, options.until, "until");
    if (!options.follow && (untilSeq < startSeq || lastSeq === 0)) return;

    const consumer = await ctx.js.consumers.get(stream, {
      deliver_policy: startSeq <= 1 ? DeliverPolicy.All : DeliverPolicy.StartSequence,
      ...(startSeq > 1 ? { opt_start_seq: startSeq } : {}),
    });
    const messages = await consumer.consume({ max_messages: 256 });
    const onAbort = (): void => messages.stop();
    options.signal?.addEventListener("abort", onAbort, { once: true });
    let expectedSeq = Math.max(startSeq, firstSeq);
    try {
      for await (const msg of messages) {
        if (msg.seq > expectedSeq) {
          // Contiguity broken: retention removed messages while reading.
          throw new RetentionGapError(cursorOf(identity, expectedSeq), cursorOf(identity, msg.seq));
        }
        expectedSeq = msg.seq + 1;
        let envelope: Envelope | null = null;
        try {
          envelope = decodeEnvelope(msg.data);
        } catch {
          envelope = null;
        }
        if (envelope !== null && envelope.tenantId === options.tenantId) {
          yield toEvent(envelope, msg.seq);
        }
        if (!options.follow && msg.seq >= untilSeq) return;
      }
    } finally {
      options.signal?.removeEventListener("abort", onAbort);
      messages.stop();
    }
  }

  const replay: Topic<T>["replay"] = (options = {}) => ({
    [Symbol.asyncIterator]: () =>
      logIterator({
        tenantId: options.tenantId ?? DEFAULT_TENANT,
        after: options.after,
        until: options.until,
        follow: false,
        signal: options.signal,
      }),
  });

  const follow: Topic<T>["follow"] = (options = {}) => ({
    [Symbol.asyncIterator]: () =>
      logIterator({
        tenantId: options.tenantId ?? DEFAULT_TENANT,
        after: options.after,
        follow: true,
        signal: options.signal,
      }),
  });

  const process: Topic<T>["process"] = async (options, handler) => {
    runtime.assertActive();
    assertName(options.consumer, "consumer");
    await declaration.ready();
    const ctx = await runtime.context();
    const delivery = resolveDelivery(options.delivery);
    const tenantId = options.tenantId ?? DEFAULT_TENANT;
    const durable = consumerName(identity, `${options.consumer}.${tenantId}`);
    const start = options.start ?? "earliest";
    const startSeq = typeof start === "object" ? parseCursor(identity, start.after, "start.after") + 1 : null;

    await ensureConsumer(ctx, stream, {
      durable_name: durable,
      ack_policy: AckPolicy.Explicit,
      filter_subject: eventSubject(tenantId),
      ack_wait: nanos(delivery.ackWaitMs),
      // One extra delivery beyond the handler attempts guarantees the DLQ
      // transfer happens even when the process dies on the final attempt.
      max_deliver: delivery.maxAttempts + 1,
      max_ack_pending: delivery.maxInFlight,
      deliver_policy:
        startSeq !== null ? DeliverPolicy.StartSequence : start === "latest" ? DeliverPolicy.New : DeliverPolicy.All,
      ...(startSeq !== null ? { opt_start_seq: startSeq } : {}),
    });

    const wr = createWorkerRuntime(options, { onFinished: () => runtime.unregisterWorker(wr) });
    runtime.registerWorker(wr);
    runtime.events.emit({ type: "worker_started", resource: config.id, kind: "topic" });

    const deadLetter = async (msg: JsMsg, envelope: Envelope | null, reason: string, error?: string): Promise<void> => {
      const eventId = (envelope?.ext?.eventId as string) ?? `seq-${msg.seq}`;
      const dlqEnvelope: Envelope = {
        v: 6,
        data: envelope?.data ?? null,
        tenantId: envelope?.tenantId ?? tenantId,
        publishedAt: new Date().toISOString(),
        ext: {
          eventId,
          consumer: options.consumer,
          attempts: msg.info.deliveryCount,
          reason,
          ...(error !== undefined ? { error } : {}),
        },
      };
      const bytes = encodeEnvelope(`topic ${config.id} dlq`, dlqEnvelope, maxPayloadBytes);
      await ctx.js.publish(dlqSubject(options.consumer), bytes, {
        msgID: `dlq.${subjectToken(options.consumer, "consumer")}.${eventId}`,
      });
      await confirmedAck(msg, config.id).catch(() => {});
      runtime.events.emit({
        type: "dead_letter",
        resource: config.id,
        kind: "topic",
        detail: { consumer: options.consumer, eventId, reason },
      });
    };

    const onMessage = async (msg: JsMsg, signal: AbortSignal): Promise<void> => {
      let envelope: Envelope;
      try {
        envelope = decodeEnvelope(msg.data);
      } catch (error) {
        await deadLetter(msg, null, "invalid envelope", asError(error).message);
        return;
      }
      const attempt = msg.info.deliveryCount;
      if (attempt > delivery.maxAttempts) {
        await deadLetter(msg, envelope, "max attempts exhausted");
        return;
      }
      try {
        await handler({ ...toEvent(envelope, msg.seq), attempt, signal });
        await confirmedAck(msg, config.id);
      } catch (error) {
        if (signal.aborted) {
          msg.nak();
          return;
        }
        const err = asError(error);
        runtime.events.emit({ type: "handler_error", resource: config.id, kind: "topic", error: err.message });
        if (attempt >= delivery.maxAttempts) {
          await deadLetter(msg, envelope, "max attempts exhausted", err.message);
        } else {
          msg.nak(backoffDelayMs(delivery, attempt));
          runtime.events.emit({
            type: "redelivery",
            resource: config.id,
            kind: "topic",
            detail: { consumer: options.consumer, attempt },
          });
        }
      }
    };

    const getConsumer = (): Promise<Consumer> => ctx.js.consumers.get(stream, durable);
    runPullLoop(wr, getConsumer, onMessage, { events: runtime.events, resource: config.id }).finally(() => {
      runtime.events.emit({ type: "worker_stopped", resource: config.id, kind: "topic" });
    });
    return wr.worker;
  };

  return {
    ready: () => declaration.ready(),
    publish,
    latestCursor,
    live,
    replay,
    follow,
    process,
  };
};
