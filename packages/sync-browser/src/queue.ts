import { Emitter } from "./internal/emitter";
import { randomId } from "./internal/id";
import { sleep } from "./internal/sleep";

const DAY_MS = 24 * 60 * 60 * 1000;
const DEFAULT_PREFIX = "sync:queue";
const DEFAULT_TENANT = "default";
const DEFAULT_LEASE_MS = 30_000;
const DEFAULT_WAIT_TIMEOUT_MS = 30_000;
const DEFAULT_MAX_DELIVERIES = 10;
const DEFAULT_MAX_NACK_DELAY_MS = 7 * DAY_MS;
const DEFAULT_MAX_MESSAGE_AGE_MS = 7 * DAY_MS;
const DEFAULT_DLQ_RETENTION_MS = 7 * DAY_MS;
const DEFAULT_IDEMPOTENCY_TTL_MS = 7 * DAY_MS;
const DEFAULT_PAYLOAD_BYTES = 128 * 1024;
const DEFAULT_MAINTENANCE_INTERVAL_MS = 1_000;

const textEncoder = new TextEncoder();

// ==========================
// Types
// ==========================

export type QueueConfig<T = unknown> = {
  id: string;
  tenantId?: string;
  prefix?: string;
  ordering?: {
    mode?: "best_effort" | "ordering_key_partitioned";
    partitions?: number;
  };
  limits?: {
    payloadBytes?: number;
    maxMessageAgeMs?: number;
    maxNackDelayMs?: number;
    dlqRetentionMs?: number;
  };
  delivery?: {
    defaultLeaseMs?: number;
    maxDeliveries?: number;
  };
};

export type QueueSendConfig<T> = {
  data: T;
  delayMs?: number;
  orderingKey?: string;
  idempotencyKey?: string;
  idempotencyTtlMs?: number;
  meta?: Record<string, unknown>;
  tenantId?: string;
};

export type QueueRecvConfig = {
  wait?: boolean;
  timeoutMs?: number;
  leaseMs?: number;
  signal?: AbortSignal;
  consumerId?: string;
  tenantId?: string;
};

export type QueueReceived<T> = {
  data: T;
  messageId: string;
  deliveryId: string;
  attempt: number;
  leaseUntil: number;
  orderingKey?: string;
  meta?: Record<string, unknown>;
  ack(): Promise<boolean>;
  nack(cfg?: { delayMs?: number; reason?: string; error?: string }): Promise<boolean>;
  touch(cfg?: { leaseMs?: number }): Promise<boolean>;
};

export type QueueReader<T> = {
  recv(cfg?: QueueRecvConfig): Promise<QueueReceived<T> | null>;
  stream(cfg?: QueueRecvConfig): AsyncIterable<QueueReceived<T>>;
};

export type QueueDeadLetter<T> = {
  messageId: string;
  data: T;
  attempts: number;
  movedAt: number;
  reason: string;
  orderingKey?: string;
  meta?: Record<string, unknown>;
  lastError?: string;
};

export type Queue<T> = QueueReader<T> & {
  send(cfg: QueueSendConfig<T>): Promise<{ messageId: string }>;
  reader(): QueueReader<T>;
  /** Oldest dead letters first. Read-only; use `dlqRemove` to drain. */
  dlq(cfg?: { tenantId?: string; limit?: number }): Promise<Array<QueueDeadLetter<T>>>;
  dlqRemove(cfg: { messageId: string; tenantId?: string }): Promise<boolean>;
};

// ==========================
// Internal Types
// ==========================

type StoredMessage<T = unknown> = {
  data: T;
  orderingKey?: string;
  meta?: Record<string, unknown>;
  enqueuedAt: number;
  attempt: number;
};

type DeliveryMeta = {
  messageId: string;
  deliveryId: string;
  leaseUntil: number;
  attempt: number;
};

type DlqEntry = {
  messageId: string;
  data: unknown;
  orderingKey?: string;
  meta?: Record<string, unknown>;
  attempts: number;
  movedAt: number;
  reason: string;
  lastError?: string;
};

type QueueState = {
  seq: number;
  ready: string[];
  delayed: Map<string, number>;
  messages: Map<string, StoredMessage>;
  deliveries: Map<string, DeliveryMeta>;
  leases: Map<string, number>;
  dlq: Map<string, DlqEntry>;
  idempotency: Map<string, { messageId: string; expiresAt: number }>;
  emitter: Emitter<void>;
  lastMaintenance: number;
};

// ==========================
// Queue Factory
// ==========================

export const queue = <T>(config: QueueConfig<T>): Queue<T> => {
  type TData = T;

  if (config.ordering?.mode === "ordering_key_partitioned") {
    // Mirrors the server: the mode is declared but nothing implements it, so it
    // is rejected rather than silently ignored.
    throw new Error("ordering.mode 'ordering_key_partitioned' is not implemented; use 'best_effort'");
  }

  const prefix = config.prefix ?? DEFAULT_PREFIX;
  const defaultTenant = config.tenantId ?? DEFAULT_TENANT;
  const defaultLeaseMs = config.delivery?.defaultLeaseMs ?? DEFAULT_LEASE_MS;
  const maxDeliveries = config.delivery?.maxDeliveries ?? DEFAULT_MAX_DELIVERIES;
  const maxPayloadBytes = config.limits?.payloadBytes ?? DEFAULT_PAYLOAD_BYTES;
  const maxNackDelayMs = config.limits?.maxNackDelayMs ?? DEFAULT_MAX_NACK_DELAY_MS;
  const maxMessageAgeMs = config.limits?.maxMessageAgeMs ?? DEFAULT_MAX_MESSAGE_AGE_MS;
  const dlqRetentionMs = config.limits?.dlqRetentionMs ?? DEFAULT_DLQ_RETENTION_MS;
  const resolveTenant = (tenantId?: string): string => tenantId ?? defaultTenant;

  // Per-tenant state
  const states = new Map<string, QueueState>();
  const getState = (tenantId: string): QueueState => {
    const key = `${prefix}:${tenantId}:${config.id}`;
    let state = states.get(key);
    if (!state) {
      state = {
        seq: 0,
        ready: [],
        delayed: new Map(),
        messages: new Map(),
        deliveries: new Map(),
        leases: new Map(),
        dlq: new Map(),
        idempotency: new Map(),
        emitter: new Emitter(),
        lastMaintenance: 0,
      };
      states.set(key, state);
    }
    return state;
  };

  // ==========================
  // Maintenance
  // ==========================

  const runMaintenance = (state: QueueState): void => {
    const now = Date.now();

    // Rate-limit maintenance
    if (now - state.lastMaintenance < DEFAULT_MAINTENANCE_INTERVAL_MS) return;
    state.lastMaintenance = now;

    // Promote delayed messages
    for (const [messageId, deliverAt] of state.delayed) {
      if (deliverAt <= now) {
        state.delayed.delete(messageId);
        const msg = state.messages.get(messageId);
        if (msg) {
          // Check message age
          if (now - msg.enqueuedAt > maxMessageAgeMs) {
            moveToDlq(state, messageId, msg, "expired");
          } else {
            state.ready.push(messageId);
          }
        }
      }
    }

    // Expire leases
    for (const [deliveryId, leaseUntil] of state.leases) {
      if (leaseUntil <= now) {
        const delivery = state.deliveries.get(deliveryId);
        state.leases.delete(deliveryId);
        state.deliveries.delete(deliveryId);

        if (delivery) {
          const msg = state.messages.get(delivery.messageId);
          if (msg) {
            if (msg.attempt >= maxDeliveries) {
              moveToDlq(state, delivery.messageId, msg, "max_deliveries");
            } else {
              state.ready.push(delivery.messageId);
              state.emitter.emit();
            }
          }
        }
      }
    }

    // Clean expired DLQ entries
    for (const [messageId, dlq] of state.dlq) {
      if (now - dlq.movedAt > dlqRetentionMs) {
        state.dlq.delete(messageId);
      }
    }

    // Clean expired idempotency keys
    for (const [key, entry] of state.idempotency) {
      if (now >= entry.expiresAt) {
        state.idempotency.delete(key);
      }
    }
  };

  const moveToDlq = (
    state: QueueState,
    messageId: string,
    msg: StoredMessage,
    reason: string,
    lastError?: string,
  ): void => {
    state.dlq.set(messageId, {
      messageId,
      data: msg.data,
      orderingKey: msg.orderingKey,
      meta: msg.meta,
      attempts: msg.attempt,
      movedAt: Date.now(),
      reason,
      lastError,
    });
    state.messages.delete(messageId);
  };

  // ==========================
  // send
  // ==========================

  const send = async (sendCfg: QueueSendConfig<TData>): Promise<{ messageId: string }> => {
    const tenantId = resolveTenant(sendCfg.tenantId);
    const state = getState(tenantId);

    const payloadRaw = JSON.stringify(sendCfg.data);
    const payloadBytes = textEncoder.encode(payloadRaw).byteLength;
    if (payloadBytes > maxPayloadBytes) {
      throw new Error(`payload exceeds limit (${maxPayloadBytes} bytes)`);
    }

    // Idempotency check
    if (sendCfg.idempotencyKey) {
      const existing = state.idempotency.get(sendCfg.idempotencyKey);
      if (existing && Date.now() < existing.expiresAt) {
        return { messageId: existing.messageId };
      }
    }

    const messageId = String(++state.seq);
    const msg: StoredMessage<TData> = {
      data: sendCfg.data,
      orderingKey: sendCfg.orderingKey,
      meta: sendCfg.meta,
      enqueuedAt: Date.now(),
      attempt: 0,
    };

    state.messages.set(messageId, msg);

    if (sendCfg.idempotencyKey) {
      state.idempotency.set(sendCfg.idempotencyKey, {
        messageId,
        expiresAt: Date.now() + (sendCfg.idempotencyTtlMs ?? DEFAULT_IDEMPOTENCY_TTL_MS),
      });
    }

    const delayMs = sendCfg.delayMs ?? 0;
    if (delayMs > 0) {
      state.delayed.set(messageId, Date.now() + delayMs);
    } else {
      state.ready.push(messageId);
      state.emitter.emit();
    }

    return { messageId };
  };

  // ==========================
  // createReader
  // ==========================

  const createReader = (): QueueReader<TData> => {
    const recv = async (recvCfg: QueueRecvConfig = {}): Promise<QueueReceived<TData> | null> => {
      const tenantId = resolveTenant(recvCfg.tenantId);
      const state = getState(tenantId);
      const wait = recvCfg.wait ?? true;
      const timeoutMs = recvCfg.timeoutMs ?? DEFAULT_WAIT_TIMEOUT_MS;
      const leaseMs = recvCfg.leaseMs ?? defaultLeaseMs;

      runMaintenance(state);

      // Try to claim a message
      const claimed = claimNext(state, leaseMs);
      if (claimed) return claimed;

      if (!wait) return null;

      // Wait for a message with timeout
      const deadline = Date.now() + timeoutMs;

      while (!recvCfg.signal?.aborted) {
        const remaining = deadline - Date.now();
        if (remaining <= 0) break;

        // Wait for emitter or timeout
        const raceTimeout = Math.min(remaining, 1000);
        await Promise.race([
          state.emitter.onceWithSignal(recvCfg.signal),
          sleep(raceTimeout),
        ]).catch(() => {});

        runMaintenance(state);
        const claimed = claimNext(state, leaseMs);
        if (claimed) return claimed;
      }

      return null;
    };

    const claimNext = (state: QueueState, leaseMs: number): QueueReceived<TData> | null => {
      // FIFO: take first from ready
      while (state.ready.length > 0) {
        const messageId = state.ready.shift()!;
        const msg = state.messages.get(messageId);
        if (!msg) continue;

        // Check message age
        if (Date.now() - msg.enqueuedAt > maxMessageAgeMs) {
          moveToDlq(state, messageId, msg, "expired");
          continue;
        }

        msg.attempt++;

        const deliveryId = `${messageId}:${randomId()}`;
        const leaseUntil = Date.now() + leaseMs;

        const delivery: DeliveryMeta = {
          messageId,
          deliveryId,
          leaseUntil,
          attempt: msg.attempt,
        };

        state.deliveries.set(deliveryId, delivery);
        state.leases.set(deliveryId, leaseUntil);

        return buildReceived(state, messageId, deliveryId, msg, delivery);
      }
      return null;
    };

    const buildReceived = (
      state: QueueState,
      messageId: string,
      deliveryId: string,
      msg: StoredMessage,
      delivery: DeliveryMeta,
    ): QueueReceived<TData> | null => {
      let settled = false;

      const ack = async (): Promise<boolean> => {
        if (settled) return false;
        if (!state.deliveries.has(deliveryId)) return false;
        settled = true;
        state.deliveries.delete(deliveryId);
        state.leases.delete(deliveryId);
        state.messages.delete(messageId);
        return true;
      };

      const nack = async (cfg?: { delayMs?: number; reason?: string; error?: string }): Promise<boolean> => {
        if (settled) return false;
        if (!state.deliveries.has(deliveryId)) return false;

        // Validate BEFORE settling to avoid orphaning the message
        const delayMs = cfg?.delayMs ?? 0;
        if (delayMs > maxNackDelayMs) {
          throw new Error(`nack delayMs (${delayMs}) exceeds maxNackDelayMs (${maxNackDelayMs})`);
        }

        settled = true;
        state.deliveries.delete(deliveryId);
        state.leases.delete(deliveryId);

        if (msg.attempt >= maxDeliveries) {
          moveToDlq(state, messageId, msg, cfg?.reason ?? "max_deliveries", cfg?.error);
          return true;
        }

        if (delayMs > 0) {
          state.delayed.set(messageId, Date.now() + delayMs);
        } else {
          state.ready.push(messageId);
          state.emitter.emit();
        }

        return true;
      };

      const touch = async (cfg?: { leaseMs?: number }): Promise<boolean> => {
        if (settled) return false;
        if (!state.deliveries.has(deliveryId)) return false;
        const newLeaseMs = cfg?.leaseMs ?? defaultLeaseMs;
        const newLeaseUntil = Date.now() + newLeaseMs;
        delivery.leaseUntil = newLeaseUntil;
        state.leases.set(deliveryId, newLeaseUntil);
        return true;
      };

      return {
        data: msg.data as TData,
        messageId,
        deliveryId,
        attempt: msg.attempt,
        leaseUntil: delivery.leaseUntil,
        orderingKey: msg.orderingKey,
        meta: msg.meta,
        ack,
        nack,
        touch,
      };
    };

    const stream = async function* (streamCfg: QueueRecvConfig = {}): AsyncIterable<QueueReceived<TData>> {
      const wait = streamCfg.wait ?? true;

      while (!streamCfg.signal?.aborted) {
        const message = await recv(streamCfg);
        if (message) {
          yield message;
          continue;
        }
        if (!wait) break;
      }
    };

    return { recv, stream };
  };

  const dlq = async (cfg: { tenantId?: string; limit?: number } = {}): Promise<Array<QueueDeadLetter<TData>>> => {
    const state = getState(resolveTenant(cfg.tenantId));
    runMaintenance(state);
    const limit = Math.max(1, cfg.limit ?? 100);
    return [...state.dlq.values()]
      .sort((a, b) => a.movedAt - b.movedAt)
      .slice(0, limit)
      .map((entry) => ({ ...entry, data: entry.data as TData }));
  };

  const dlqRemove = async (cfg: { messageId: string; tenantId?: string }): Promise<boolean> => {
    const state = getState(resolveTenant(cfg.tenantId));
    return state.dlq.delete(cfg.messageId);
  };

  const defaultReader = createReader();

  return {
    send,
    recv: defaultReader.recv,
    stream: defaultReader.stream,
    reader: () => createReader(),
    dlq,
    dlqRemove,
  };
};
