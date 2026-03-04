# API

## Factory

```ts
import { z } from "zod";
import { topic } from "@valentinkolb/sync";

const t = topic({
  id: "order.events",
  schema: z.object({ type: z.string(), orderId: z.string() }),
  // tenantId: "default",
  // prefix: "sync:topic",
  // limits: { payloadBytes: 131072 },
  // retentionMs: 604800000,
});
```

## Types

```ts
type TopicConfig<TSchema extends z.ZodTypeAny> = {
  id: string;
  schema: TSchema;
  tenantId?: string;
  prefix?: string;
  limits?: { payloadBytes?: number }; // default: 131072
  retentionMs?: number; // default: 604800000 (7d)
};

type TopicPubConfig<T> = {
  tenantId?: string;
  data: T;
  orderingKey?: string;
  idempotencyKey?: string;
  idempotencyTtlMs?: number; // default: 604800000 (7d)
  meta?: Record<string, unknown>;
};

type TopicRecvConfig = {
  tenantId?: string;
  timeoutMs?: number; // default: 30000
  wait?: boolean; // default: true
  signal?: AbortSignal;
};

type TopicDelivery<T> = {
  data: T;
  eventId: string;
  deliveryId: string; // "{group}:{eventId}"
  cursor: string;
  orderingKey?: string;
  publishedAt: number;
  meta?: Record<string, unknown>;
  commit(): Promise<boolean>;
};

type TopicLiveConfig = {
  tenantId?: string;
  after?: string; // default: "$"
  signal?: AbortSignal;
  timeoutMs?: number; // default: 30000
};

type TopicLiveEvent<T> = {
  data: T;
  eventId: string;
  cursor: string;
  orderingKey?: string;
  publishedAt: number;
  meta?: Record<string, unknown>;
};

type TopicReader<T> = {
  group: string;
  recv(cfg?: TopicRecvConfig): Promise<TopicDelivery<T> | null>;
  stream(cfg?: TopicRecvConfig): AsyncIterable<TopicDelivery<T>>;
};

type Topic<T> = {
  pub(cfg: TopicPubConfig<T>): Promise<{ eventId: string; cursor: string }>;
  reader(group?: string): TopicReader<T>; // default group: "default"
  live(cfg?: TopicLiveConfig): AsyncIterable<TopicLiveEvent<T>>;
};
```

## Config Options

- `id`: required stream namespace.
- `schema`: required Zod schema.
- `tenantId`: default tenant. Default `default`.
- `prefix`: default `sync:topic`.
- `limits.payloadBytes`: max serialized event bytes (default `128KB`).
- `retentionMs`: stream retention window for trim-min-id (default `7d`).

## Semantics

- `reader(group)` uses Redis consumer groups (`XREADGROUP`).
- Messages are acknowledged only when `commit()` succeeds.
- Invalid payload entries are acknowledged and skipped.
- `live()` uses `XREAD`; no commit, no group state.
- `live({ after: "0-0" })` replays whole retained stream.
- `reader().stream({ wait: true })` auto-retries transient transport errors.
- `live()` auto-retries transient transport errors while preserving cursor progression.
- `reader().recv()` and `stream({ wait: false })` keep explicit one-shot error behavior.

## Usage Patterns

### Publish

```ts
await t.pub({
  data: { type: "order.confirmed", orderId },
  idempotencyKey: `order-confirmed:${orderId}`,
  meta: { source: "checkout" },
});
```

### Group consumer

```ts
const reader = t.reader("mailer");
for await (const event of reader.stream({ signal: ac.signal })) {
  await sendMail(event.data);
  await event.commit();
}
```

### Live replay

```ts
for await (const event of t.live({ after: "0-0", signal: ac.signal })) {
  await project(event.data);
}
```

## Redis Keys

- Stream key: `{prefix}:{tenantId}:{id}:stream`
- Idempotency key: `{prefix}:{tenantId}:{id}:idempotency:{key}`

## Failure Guidance

- Make consumers idempotent; re-delivery is expected.
- Use separate groups per independent downstream system.
- Keep retention long enough for replay/recovery requirements.
