# topic

Pub/sub with cursor-based replay. Consumer groups for at-least-once delivery, `live()` for best-effort streaming to all listeners.

## Factory

```ts
import { topic } from "@k2b/sync";

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

// topic() returns this concrete extension of Topic<T>.
type RecoverableTopic<T> = Omit<Topic<T>, "reader"> & {
  reader(group?: string): RecoverableTopicReader<T>;
};

type TopicCursorConfig = {
  tenantId?: string;
};

type TopicReader<T> = {
  group: string;
  recv(cfg?: TopicRecvConfig): Promise<TopicDelivery<T> | null>;
  reclaim?(cfg?: TopicReclaimConfig): Promise<TopicReclaimResult<T>>;
  stream(cfg?: TopicRecvConfig): AsyncIterable<TopicDelivery<T>>;
  close(): Promise<void>;
  [Symbol.asyncDispose](): Promise<void>;
};

type RecoverableTopicReader<T> = TopicReader<T> & {
  reclaim(cfg?: TopicReclaimConfig): Promise<TopicReclaimResult<T>>;
};

type TopicRecvConfig = {
  tenantId?: string;
  timeoutMs?: number;
  wait?: boolean;
  signal?: AbortSignal;
  invalidPayload?: "ack" | "throw";
};

type TopicReclaimConfig = {
  tenantId?: string;
  minIdleMs?: number; // default: 60_000
  cursor?: string;    // default: "0-0"
  count?: number;     // default: 25, range: 1..1000
};

type TopicReclaimResult<T> = {
  nextCursor: string;
  entries: TopicReclaimedDelivery<T>[];
};

type TopicReclaimedDelivery<T> =
  | { kind: "delivery"; delivery: TopicDelivery<T> }
  | TopicInvalidDelivery;

type TopicInvalidDelivery = {
  kind: "invalid";
  eventId: string;
  cursor: string;
  deliveryId: string;
  error: string;
  rawPayload: string | null;
  commit(): Promise<boolean>;
};

class TopicPayloadError extends Error {
  readonly eventId: string;
  readonly rawPayload: string | null;
}

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

// Recover deliveries abandoned by a crashed consumer before starting the
// long-lived consumer loop. Keep nextCursor between calls so a poison prefix
// cannot starve later pending entries.
let recoveryCursor = "0-0";
do {
  const batch = await reader.reclaim({ minIdleMs: 60_000, cursor: recoveryCursor });
  for (const entry of batch.entries) {
    if (entry.kind === "invalid") {
      await recordPoisonMessage(entry.eventId, entry.error);
      await entry.commit();
      continue;
    }
    await sendToAnalytics(entry.delivery.data);
    await entry.delivery.commit();
  }
  recoveryCursor = batch.nextCursor;
} while (recoveryCursor !== "0-0");

for await (const msg of reader.stream()) {
  await sendToAnalytics(msg.data);
  await msg.commit();
}
await reader.close();

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
- **`reclaim()`**: claims pending deliveries that have been idle for at least `minIdleMs` (default 60s). Run it before starting a long-lived reader or periodically. Continue with `nextCursor` until it returns `"0-0"`; this advances past pending entries that are not idle long enough.
- **`close()`**: permanently closes that reader handle, stops its active blocking read, and releases its resources. It is idempotent; create a new reader handle instead of calling `recv()` or `reclaim()` after closing. On the server it removes the consumer record from every tenant used by that reader only when the consumer has no pending deliveries; pending deliveries remain reclaimable.
- **Malformed payloads**: by default, `recv()` and `stream()` acknowledge malformed transport envelopes and continue. Pass `invalidPayload: "throw"` to receive a `TopicPayloadError` and leave the entry pending. `reclaim()` reports those entries as `{ kind: "invalid" }` so the application can record or dead-letter them before `commit()`.
- **`live()`**: best-effort fan-out to every listener. Not acked. Missed events by slow/disconnected listeners may trigger an `overflow` signal (browser) or cursor reset (server). Use for ephemeral updates (presence, UI sync).
- **`latestCursor()`**: reads the current head cursor for a tenant without consuming or acknowledging anything. Returns `null` when the tenant stream has no entries.
- **`after: "0-0"`**: start from the earliest retained event. Useful for replay.
- **`idempotencyKey` on pub**: dedupes within `idempotencyTtlMs` (default 7d). Same key returns the same eventId.
- **`retentionMs`**: events older than this are trimmed during publish. Set carefully for replay requirements.
- **`tenantId`**: isolates the stream — separate event log per tenant. Browser: `tenantId` also isolates `maxEntries`.
- **Browser runtime**: consumer groups behave as documented above. The group's cursor advances on `commit()`, not on delivery, so an uncommitted delivery stays recoverable via `reclaim({ minIdleMs, cursor, count })`; pass each returned cursor into the next call until it returns `"0-0"`. Readers of one group distribute rather than broadcast; a recreated reader resumes at the group's committed position; and `commit()` is refused once another reader has reclaimed the delivery. Leader-free and tab-local: state is shared per `{prefix}:{id}` within the tab (or per `store` when one is passed), not across tabs.

## Redis keys (server)

- `{prefix}:{tenantId}:{id}:stream` — Redis Stream
- `{prefix}:{tenantId}:{id}:idempotency:{key}` — eventId with TTL

Consumer groups use native Redis Streams consumer groups (`XREADGROUP`).
