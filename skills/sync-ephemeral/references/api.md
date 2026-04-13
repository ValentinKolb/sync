# API

## Browser

```ts
import { ephemeral, EphemeralCapacityError, EphemeralPayloadTooLargeError } from "@valentinkolb/sync-browser";

const store = ephemeral({
  id: "presence",
  schema: z.object({ status: z.enum(["online", "away"]) }),
  ttlMs: 30_000,
  // All server options work: tenantId, limits
});
```

Same types and API. Entries and events are held in-memory. TTL expiration uses `setTimeout` (no Redis sentinel keys or reconciliation). The `overflow` event still works — it fires when a reader's cursor falls behind the event log's retention window.

---

## Factory

```ts
import { z } from "zod";
import { ephemeral } from "@valentinkolb/sync";

const store = ephemeral({
  id: "presence",
  schema: z.object({ nodeId: z.string(), status: z.enum(["up", "down"]) }),
  ttlMs: 30_000,
  // tenantId: "default",
  // limits: { maxEntries, maxPayloadBytes, eventRetentionMs, eventMaxLen },
});
```

## Types

```ts
class EphemeralCapacityError extends Error {}
class EphemeralPayloadTooLargeError extends Error {}

type EphemeralConfig<TSchema extends z.ZodTypeAny> = {
  id: string;
  schema: TSchema;
  ttlMs: number;
  tenantId?: string;
  limits?: {
    maxEntries?: number; // default: 10000
    maxPayloadBytes?: number; // default: 4096
    eventRetentionMs?: number; // default: 300000
    eventMaxLen?: number; // default: 50000
  };
};

type EphemeralUpsertConfig<T> = {
  key: string;
  value: T;
  ttlMs?: number;
  tenantId?: string;
};

type EphemeralTouchConfig = {
  key: string;
  ttlMs?: number;
  tenantId?: string;
};

type EphemeralRemoveConfig = {
  key: string;
  reason?: string;
  tenantId?: string;
};

type EphemeralEntry<T> = {
  key: string;
  value: T;
  version: string;
  updatedAt: number;
  expiresAt: number;
};

type EphemeralSnapshot<T> = {
  entries: EphemeralEntry<T>[];
  cursor: string;
};

type EphemeralRecvConfig = {
  wait?: boolean; // default: true
  timeoutMs?: number; // default: 30000
  signal?: AbortSignal;
};

type EphemeralEvent<T> =
  | { type: "upsert"; cursor: string; entry: EphemeralEntry<T> }
  | { type: "touch"; cursor: string; key: string; version: string; expiresAt: number }
  | { type: "delete"; cursor: string; key: string; version: string; deletedAt: number; reason?: string }
  | { type: "expire"; cursor: string; key: string; version: string; expiredAt: number }
  | { type: "overflow"; cursor: string; after: string; firstAvailable: string };

type EphemeralReader<T> = {
  recv(cfg?: EphemeralRecvConfig): Promise<EphemeralEvent<T> | null>;
  stream(cfg?: EphemeralRecvConfig): AsyncIterable<EphemeralEvent<T>>;
};

type EphemeralStore<T> = {
  upsert(cfg: EphemeralUpsertConfig<T>): Promise<EphemeralEntry<T>>;
  touch(cfg: EphemeralTouchConfig): Promise<{ ok: boolean; version?: string; expiresAt?: number }>;
  remove(cfg: EphemeralRemoveConfig): Promise<boolean>;
  snapshot(cfg?: { tenantId?: string }): Promise<EphemeralSnapshot<T>>;
  reader(cfg?: { after?: string; tenantId?: string }): EphemeralReader<T>;
};
```

## Config and Limits

- `id`: required identifier.
- `schema`: required Zod schema.
- `ttlMs`: required default TTL (`> 0`).
- `tenantId`: default tenant keyspace. Default `default`.
- `limits.maxEntries`: cap on active entries, default `10000`.
- `limits.maxPayloadBytes`: serialized payload max bytes, default `4096`.
- `limits.eventRetentionMs`: event stream retention window, default `5m`.
- `limits.eventMaxLen`: stream length cap, default `50000`.

## Validation and Constraints

- Logical key must be non-empty and <= 512 bytes.
- `id` and `tenantId` must be non-empty and <= 256 chars.
- `ttlMs` must be finite and > 0.
- `upsert()` validates value against schema; throws Zod error on invalid value.
- `upsert()` throws:
  - `EphemeralCapacityError` when `maxEntries` reached.
  - `EphemeralPayloadTooLargeError` when payload exceeds `maxPayloadBytes`.

## Semantics

- `upsert()` replaces existing value and resets TTL.
- `touch()` only extends TTL; returns `{ ok: false }` if key missing.
- `remove()` deletes key and emits delete event (optional reason).
- Reconcile loop checks TTL marker keys and emits `expire` events.
- Reader may emit `overflow` when requested replay cursor is trimmed away.
- `reader().stream({ wait: true })` auto-retries transient transport errors.
- `reader().recv()` and `stream({ wait: false })` keep explicit one-shot error behavior.

## Usage Patterns

### Presence state

```ts
await store.upsert({ key: `worker:${id}`, value: { nodeId: id, status: "up" } });
await store.touch({ key: `worker:${id}` });
```

### Snapshot + stream

```ts
const snap = await store.snapshot();
hydrateFromSnapshot(snap.entries);

for await (const ev of store.reader({ after: snap.cursor, signal: ac.signal }).stream()) {
  if (ev.type === "overflow") {
    const full = await store.snapshot();
    replaceState(full.entries);
    continue;
  }
  applyEvent(ev);
}
```

## Redis Keys

Base: `sync:e:{tenant}:{id}`

- `:seq`
- `:state` (hash)
- `:exp` (zset)
- `:ttl:{len}:{logicalKey}` (marker key with PX)
- `:events` (stream)
