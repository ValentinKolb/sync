# API

## Browser

```ts
import { queue } from "@valentinkolb/sync-browser";

const q = queue({
  id: "mail.send",
  schema: z.object({ to: z.string().email(), subject: z.string() }),
  // All server options work: tenantId, prefix, limits, delivery
});
```

Same types and API. State is in-memory (arrays, Maps) instead of Redis data structures. Blocking reads use an event emitter. No `store` config needed — queue manages its own internal state.

---

## Factory

```ts
import { z } from "zod";
import { queue } from "@valentinkolb/sync";

const q = queue({
  id: "mail.send",
  schema: z.object({ to: z.string().email(), subject: z.string() }),
  // tenantId: "default",
  // prefix: "sync:queue",
  // limits: { payloadBytes, maxMessageAgeMs, maxNackDelayMs, dlqRetentionMs },
  // delivery: { defaultLeaseMs, maxDeliveries },
});
```

## Types

```ts
type QueueConfig<TSchema extends z.ZodTypeAny> = {
  id: string;
  schema: TSchema;
  tenantId?: string;
  prefix?: string;
  ordering?: {
    mode?: "best_effort" | "ordering_key_partitioned";
    partitions?: number;
  }; // currently not enforced by runtime behavior
  limits?: {
    payloadBytes?: number; // default: 131072
    maxMessageAgeMs?: number; // default: 604800000 (7d)
    maxNackDelayMs?: number; // default: 604800000 (7d)
    dlqRetentionMs?: number; // default: 604800000 (7d)
  };
  delivery?: {
    defaultLeaseMs?: number; // default: 30000
    maxDeliveries?: number; // default: 10
  };
};

type QueueSendConfig<T> = {
  tenantId?: string;
  data: T;
  delayMs?: number;
  orderingKey?: string;
  idempotencyKey?: string;
  idempotencyTtlMs?: number; // default: 604800000 (7d)
  meta?: Record<string, unknown>;
};

type QueueRecvConfig = {
  tenantId?: string;
  wait?: boolean; // default: true
  timeoutMs?: number; // default: 30000
  leaseMs?: number; // default from config.delivery.defaultLeaseMs
  consumerId?: string; // currently unused
  signal?: AbortSignal;
};

type QueueReceived<T> = {
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

type QueueReader<T> = {
  recv(cfg?: QueueRecvConfig): Promise<QueueReceived<T> | null>;
  stream(cfg?: QueueRecvConfig): AsyncIterable<QueueReceived<T>>;
};

type Queue<T> = QueueReader<T> & {
  send(cfg: QueueSendConfig<T>): Promise<{ messageId: string }>;
  reader(): QueueReader<T>;
};
```

## Config Options

- `id`: required queue namespace.
- `schema`: required Zod schema for payload validation.
- `tenantId`: default tenant keyspace. Default `default`.
- `prefix`: Redis key prefix. Default `sync:queue`.
- `limits.payloadBytes`: hard max for JSON payload bytes. Default `128KB`.
- `limits.maxMessageAgeMs`: max age before maintenance sends to DLQ with reason `expired`.
- `limits.maxNackDelayMs`: upper bound for `nack({ delayMs })`.
- `limits.dlqRetentionMs`: TTL applied to DLQ hash.
- `delivery.defaultLeaseMs`: visibility timeout for each receive.
- `delivery.maxDeliveries`: after this many attempts, maintenance/DLQ path moves message to DLQ.
- `ordering.*`: accepted but currently only stored metadata (`orderingKey`), no partitioned dispatch enforcement.

## Semantics

- Delivery is at-least-once.
- `ack()` can return `false` if lease/delivery record is gone.
- `nack()` can return `false` if delivery already expired/acknowledged.
- `touch()` extends current delivery lease and returns success/failure.
- Blocking reads use independent Redis clients; aborting signal closes client safely.
- Maintenance runs on non-blocking recv every call and on blocking recv at interval (~1s).
- `stream({ wait: true })` auto-retries transient transport errors (using library retry defaults).
- `stream({ wait: false })` and direct `recv()` keep explicit one-shot error behavior.

## Usage Patterns

### Producer

```ts
await q.send({
  data: { to: "u@example.com", subject: "Welcome" },
  idempotencyKey: "welcome:u@example.com",
  idempotencyTtlMs: 24 * 60 * 60 * 1000,
  delayMs: 5000,
  meta: { traceId: "abc-123" },
});
```

### Consumer loop

```ts
for await (const msg of q.stream({ signal: ac.signal })) {
  try {
    await handle(msg.data);
    await msg.ack();
  } catch (error) {
    await msg.nack({
      delayMs: 3000,
      reason: "handler_error",
      error: error instanceof Error ? error.message : String(error),
    });
  }
}
```

### Long-running task

```ts
const msg = await q.recv({ wait: true, leaseMs: 15_000 });
if (msg) {
  await doPart1();
  await msg.touch({ leaseMs: 15_000 });
  await doPart2();
  await msg.ack();
}
```

## Redis Keys

Pattern: `{prefix}:{tenantId}:{id}:{suffix}`

- `seq`: message sequence
- `ready`: list of ready message IDs
- `delayed`: zset of delayed message IDs (score = due timestamp)
- `leases`: zset of delivery IDs (score = lease until)
- `deliveries`: hash deliveryId -> lease metadata
- `messages`: hash messageId -> payload
- `active`: list of claimed message IDs
- `dlq`: hash messageId -> DLQ payload
- `idempotency:{key}`: message dedupe key

## Failure Guidance

- Make handlers idempotent; duplicates are normal under at-least-once semantics.
- Bound retry delays; avoid unbounded nack loops.
- Monitor DLQ size and reasons (`expired`, `max_deliveries_exceeded`, `nacked`).
