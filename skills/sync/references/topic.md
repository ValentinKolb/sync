# topic

Pub/sub with cursor-based replay. Consumer groups for at-least-once delivery, `live()` for best-effort streaming to all listeners.

## Factory

```ts
import { topic } from "@valentinkolb/sync";

const t = topic<{ type: string; orderId: string }>({
  id: "order.events",
  // prefix?: string,                   // default: "sync:topic"
  // tenantId?: string,                 // default: "default"
  // retentionMs?: number,              // default: 7d
  // limits?: { payloadBytes },
  // store?: Store,                     // browser only (additive)
});
```

**No `schema` config in v5.** Use the `<T>` generic.

## API

```ts
type Topic<T> = {
  pub(cfg: TopicPubConfig<T>): Promise<{ eventId: string; cursor: string }>;
  latestCursor(cfg?: TopicCursorConfig): Promise<string | null>;
  reader(group?: string): TopicReader<T>;
  live(cfg?: TopicLiveConfig): AsyncIterable<TopicLiveEvent<T>>;
};

type TopicCursorConfig = {
  tenantId?: string;
};

type TopicReader<T> = {
  group: string;
  recv(cfg?: TopicRecvConfig): Promise<TopicDelivery<T> | null>;
  stream(cfg?: TopicRecvConfig): AsyncIterable<TopicDelivery<T>>;
};

type TopicDelivery<T> = {
  data: T;
  eventId: string;
  cursor: string;
  deliveryId: string;
  orderingKey?: string;
  publishedAt: number;
  meta?: Record<string, unknown>;
  commit(): Promise<boolean>;   // ack
};

type TopicLiveEvent<T> = {
  data: T;
  eventId: string;
  cursor: string;
  orderingKey?: string;
  publishedAt: number;
  meta?: Record<string, unknown>;
};
```

## Usage

```ts
// Publish
await t.pub({
  data: { type: "order.confirmed", orderId: "o1" },
  idempotencyKey: "order:o1:confirmed",
});

// Consumer group — at-least-once, acked
const reader = t.reader("analytics");
for await (const msg of reader.stream()) {
  await sendToAnalytics(msg.data);
  await msg.commit();
}

// Live — best-effort, all listeners receive
for await (const event of t.live({ after: "0-0", signal })) {
  console.log(event.data);
}

// Live with replay from cursor
for await (const event of t.live({ after: lastCursor, timeoutMs: 2_000 })) {
  // ...
}

// Start from now when the client has no cursor yet
const startCursor = (await t.latestCursor({ tenantId })) ?? "0-0";
for await (const event of t.live({ tenantId, after: startCursor })) {
  // ...
}
```

## Gotchas

- **Consumer group (`reader("group-name")`)**: each message is delivered to exactly one consumer in the group. Other groups get their own copy. Use for at-least-once work distribution.
- **`live()`**: best-effort fan-out to every listener. Not acked. Missed events by slow/disconnected listeners may trigger an `overflow` signal (browser) or cursor reset (server). Use for ephemeral updates (presence, UI sync).
- **`latestCursor()`**: reads the current head cursor for a tenant without consuming or acknowledging anything. Returns `null` when the tenant stream has no entries.
- **`after: "0-0"`**: start from the earliest retained event. Useful for replay.
- **`idempotencyKey` on pub**: dedupes within `idempotencyTtlMs` (default 7d). Same key returns the same eventId.
- **`retentionMs`**: events older than this are trimmed during publish. Set carefully for replay requirements.
- **`tenantId`**: isolates the stream — separate event log per tenant. Browser: `tenantId` also isolates `maxEntries`.

## Redis keys (server)

- `{prefix}:{tenantId}:{id}:stream` — Redis Stream
- `{prefix}:{tenantId}:{id}:idempotency:{key}` — eventId with TTL

Consumer groups use native Redis Streams consumer groups (`XREADGROUP`).
