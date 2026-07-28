# queue

Durable work queue with at-least-once delivery, lease-based visibility, delayed messages, idempotency, and DLQ. Same API on server and browser.

## Factory

```ts
import { queue } from "@k2b/sync";

const q = queue<{ to: string; subject: string }>({
  id: "mail.send",
  // prefix?: string,                        // default: "sync:queue"
  // tenantId?: string,                      // default: "default"
  // delivery?: { defaultLeaseMs: 30_000, maxDeliveries: 10 },
  // limits?: { payloadBytes, maxMessageAgeMs, maxNackDelayMs, dlqRetentionMs },
  // ordering?: { mode: "best_effort" },   // "ordering_key_partitioned" is not implemented and throws
});
```

**Note: v5 has no `schema` config field.** Use the `<T>` generic. Validate yourself if you need runtime checks.

## API

```ts
type QueueSendConfig<T> = {
  data: T;
  tenantId?: string;
  delayMs?: number;           // deliver after N ms
  orderingKey?: string;       // stored and delivered back; not an ordering guarantee (see Gotchas)
  idempotencyKey?: string;    // dedupe against this key
  idempotencyTtlMs?: number;
  meta?: Record<string, unknown>;
};

type QueueReceived<T> = {
  data: T;
  messageId: string;
  deliveryId: string;
  attempt: number;            // 1-indexed
  leaseUntil: number;
  orderingKey?: string;
  meta?: Record<string, unknown>;
  ack(): Promise<boolean>;
  nack(cfg?: { delayMs?: number; reason?: string; error?: string }): Promise<boolean>;
  touch(cfg?: { leaseMs?: number }): Promise<boolean>;
};

type Queue<T> = {
  send(cfg: QueueSendConfig<T>): Promise<{ messageId: string }>;
  recv(cfg?: QueueRecvConfig): Promise<QueueReceived<T> | null>;
  stream(cfg?: QueueRecvConfig): AsyncIterable<QueueReceived<T>>;
  reader(): QueueReader<T>;
};
```

## Usage

```ts
// Send + receive
await q.send({
  data: { to: "u@x.com", subject: "Welcome" },
  idempotencyKey: "welcome:u@x.com",
  delayMs: 5_000,
  meta: { traceId: "abc" },
});

const msg = await q.recv({ wait: true, timeoutMs: 30_000 });
if (msg) {
  try {
    await sendMail(msg.data);
    await msg.ack();
  } catch (err) {
    await msg.nack({ delayMs: 5_000, error: String(err) });
  }
}

// Stream processing
for await (const m of q.stream({ signal: abortCtrl.signal })) {
  await handle(m.data);
  await m.ack();
}

// Multiple independent readers (blocking connections)
const reader = q.reader();
const msg2 = await reader.recv({ signal });
```

## Gotchas

- **Lease**: every received message has a lease. If you don't `ack`/`nack` within `leaseMs`, maintenance redelivers it with `attempt++`. Long-running handlers should `touch({ leaseMs })` periodically.
- **DLQ**: after `maxDeliveries` failed attempts, message moves to DLQ. Retention is per entry via `dlqRetentionMs` (default 7d), enforced against a `movedAt`-scored index, so a steady trickle of failures cannot keep old entries alive and a pause cannot drop fresh ones. Read them with `q.dlq({ limit })` (oldest first) and drain with `q.dlqRemove({ messageId })`.
- **`ack` / `nack` return bool**: `false` means lease expired before settle (message already redelivered to someone else). Your handler's work is in an ambiguous state — design for at-least-once.
- **Idempotency key**: dedupes `send` within `idempotencyTtlMs`. Same key returns the same messageId without enqueuing a new message.
- **Ordering**: only `best_effort` exists. `orderingKey` is stored and delivered back with the message, but nothing partitions or serialises by it, so concurrent consumers can reorder same-key messages. Constructing a queue with `ordering: { mode: "ordering_key_partitioned" }` throws rather than silently ignoring the guarantee.
- **`tenantId`**: isolates queue state (separate namespace). Presence in config sets the default; can be overridden per-call.

## Redis keys (server)

- `{prefix}:{tenantId}:{id}:seq` — messageId counter
- `{prefix}:{tenantId}:{id}:dlq:index` — ZSET messageId → movedAt (DLQ retention index)
- `{prefix}:{tenantId}:{id}:messages` — hash messageId → payload
- `{prefix}:{tenantId}:{id}:ready` — list (FIFO ready queue)
- `{prefix}:{tenantId}:{id}:delayed` — sorted set of delayed messages
- `{prefix}:{tenantId}:{id}:deliveries` + `:leases` + `:active`
- `{prefix}:{tenantId}:{id}:dlq` — dead-letter list
- `{prefix}:{tenantId}:{id}:idempotency:{key}` — messageId with TTL
