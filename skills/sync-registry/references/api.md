# API

## Factory

```ts
import { z } from "zod";
import { registry } from "@valentinkolb/sync";

const services = registry({
  id: "services",
  schema: z.object({
    appId: z.string(),
    kind: z.enum(["instance", "setting", "flag"]),
    url: z.string().url().optional(),
  }),
  // tenantId: "default",
  // limits: { maxEntries, maxPayloadBytes, eventRetentionMs, eventMaxLen, tombstoneRetentionMs, reconcileBatchSize },
});
```

## Types

```ts
class RegistryCapacityError extends Error {}
class RegistryPayloadTooLargeError extends Error {}

type RegistryConfig<TSchema extends z.ZodTypeAny> = {
  id: string;
  schema: TSchema;
  tenantId?: string;
  prefix?: string; // default: "sync:registry"
  limits?: {
    maxEntries?: number; // default: 10000
    maxPayloadBytes?: number; // default: 131072
    eventRetentionMs?: number; // default: 300000
    eventMaxLen?: number; // default: 50000
    tombstoneRetentionMs?: number; // default: 300000
    reconcileBatchSize?: number; // default: 200
  };
};

type RegistryUpsertConfig<T> = {
  key: string;
  value: T;
  ttlMs?: number;
  tenantId?: string;
};

type RegistryTouchConfig = {
  key: string;
  tenantId?: string;
};

type RegistryRemoveConfig = {
  key: string;
  reason?: string;
  tenantId?: string;
};

type RegistryGetConfig = {
  key: string;
  tenantId?: string;
  includeExpired?: boolean;
};

type RegistryListConfig = {
  prefix?: string;
  status?: "active" | "expired";
  tenantId?: string;
  limit?: number;
  afterKey?: string;
};

type RegistryCasConfig<T> = {
  key: string;
  version: string;
  value: T;
  tenantId?: string;
};

type RegistryEntry<T> = {
  key: string;
  value: T;
  version: string;
  status: "active" | "expired";
  createdAt: number;
  updatedAt: number;
  ttlMs: number | null;
  expiresAt: number | null;
};

type RegistrySnapshot<T> = {
  entries: RegistryEntry<T>[];
  cursor: string;
  nextKey?: string;
};

type RegistryRecvConfig = {
  wait?: boolean; // default: true
  timeoutMs?: number; // default: 30000
  signal?: AbortSignal;
};

type RegistryEvent<T> =
  | { type: "upsert"; cursor: string; entry: RegistryEntry<T> }
  | { type: "touch"; cursor: string; key: string; version: string; updatedAt: number; expiresAt: number }
  | { type: "delete"; cursor: string; key: string; version: string; removedAt: number; reason?: string }
  | { type: "expire"; cursor: string; key: string; version: string; removedAt: number }
  | { type: "overflow"; cursor: string; after: string; firstAvailable: string };

type RegistryReader<T> = {
  recv(cfg?: RegistryRecvConfig): Promise<RegistryEvent<T> | null>;
  stream(cfg?: RegistryRecvConfig): AsyncIterable<RegistryEvent<T>>;
};

type Registry<T> = {
  upsert(cfg: RegistryUpsertConfig<T>): Promise<RegistryEntry<T>>;
  touch(cfg: RegistryTouchConfig): Promise<{ ok: boolean; version?: string; expiresAt?: number }>;
  remove(cfg: RegistryRemoveConfig): Promise<boolean>;
  get(cfg: RegistryGetConfig): Promise<RegistryEntry<T> | null>;
  list(cfg?: RegistryListConfig): Promise<RegistrySnapshot<T>>;
  cas(cfg: RegistryCasConfig<T>): Promise<{ ok: boolean; entry?: RegistryEntry<T> }>;
  reader(cfg?: { key?: string; prefix?: string; after?: string; tenantId?: string }): RegistryReader<T>;
};
```

## Config and Limits

- `id`: required identifier.
- `schema`: required Zod schema for payload validation and typed read-path parsing.
- `tenantId`: default tenant keyspace. Default `default`.
- `prefix`: Redis key prefix. Default `sync:registry`.
- `limits.maxEntries`: cap on active entries, default `10000`.
- `limits.maxPayloadBytes`: serialized payload max bytes, default `131072`.
- `limits.eventRetentionMs`: root-stream replay retention window, default `5m`.
- `limits.eventMaxLen`: per-stream length cap for root, key, and prefix streams, default `50000`.
- `limits.tombstoneRetentionMs`: retention for expired/deleted tombstones, default `5m`.
- `limits.reconcileBatchSize`: max expirations/tombstones processed per reconcile batch, default `200`.

## Validation and Constraints

- Logical key must be non-empty, must not end with `/`, must not contain `//`, and must be <= 512 bytes.
- Prefix values passed to `list()` and `reader()` must end with `/` when provided.
- `id` and `tenantId` must be non-empty and <= 256 chars.
- Keys and prefixes must not contain null bytes or `{` / `}`.
- Key depth is capped at 8 slash-delimited segments.
- `upsert()` validates value against schema; throws Zod error on invalid value.
- `cas()` validates value against schema before sending to Redis.
- `ttlMs`, when provided to `upsert()`, must be finite and > 0.
- `upsert()` and `cas()` throw:
  - `RegistryCapacityError` when `maxEntries` is reached for a new active key.
  - `RegistryPayloadTooLargeError` when payload exceeds `maxPayloadBytes`.

## Semantics

- `upsert()` creates or replaces the current payload.
- `ttlMs` is set only by `upsert()`. Records without `ttlMs` are static.
- `touch()` extends existing TTL for live entries; it returns `{ ok: false }` for missing or static entries.
- `touch()` updates liveness metadata but does not bump `version`.
- `cas()` guards payload writes with `entry.version` and returns `{ ok: false }` on mismatch or missing entry.
- `cas()` refreshes liveness for TTL-backed entries by resetting `expiresAt` and the TTL sentinel.
- `remove()` deletes the active record and emits `delete`.
- Expired records are moved into tombstones and may be queried with `status: "expired"` or `get({ includeExpired: true })` until tombstone retention elapses.
- `list()` returns a cursor for replay-safe handoff into `reader()`.
- `reader({ key })` watches one exact key, `reader({ prefix })` watches one namespace prefix, and `reader()` with neither watches the whole registry.
- `reader().stream({ wait: true })` auto-retries transient transport errors.
- `reader().recv()` and `stream({ wait: false })` keep explicit one-shot error behavior.

## Usage Patterns

### Static settings

```ts
await services.upsert({
  key: "settings/smtp",
  value: {
    appId: "platform",
    kind: "setting",
  },
});

const current = await services.get({ key: "settings/smtp" });
```

### Live instances

```ts
await services.upsert({
  key: "apps/contacts/instances/i-1",
  value: {
    appId: "contacts",
    kind: "instance",
    url: "https://contacts-1.internal",
  },
  ttlMs: 15_000,
});

await services.touch({ key: "apps/contacts/instances/i-1" });
```

### Prefix snapshot + stream

```ts
const snap = await services.list({
  prefix: "apps/contacts/instances/",
  status: "active",
});

hydrateFromSnapshot(snap.entries);

for await (const ev of services.reader({ prefix: "apps/contacts/instances/", after: snap.cursor }).stream({ signal: ac.signal })) {
  if (ev.type === "overflow") {
    const full = await services.list({
      prefix: "apps/contacts/instances/",
      status: "active",
    });
    replaceState(full.entries);
    continue;
  }
  applyEvent(ev);
}
```

### CAS update

```ts
const current = await services.get({ key: "flags/contacts/new-navbar" });

if (current) {
  const result = await services.cas({
    key: current.key,
    version: current.version,
    value: {
      ...current.value,
      kind: "flag",
    },
  });

  if (!result.ok) {
    // re-read and retry if needed
  }
}
```

## Redis Keys

Base: `sync:registry:{tenant}:{id}`

- `:state` (hash)
- `:keys` (zset lex index for active entries)
- `:exp` (zset by expiresAt)
- `:ttl:{len}:{logicalKey}` (TTL sentinel key with PX)
- `:dead` (hash of tombstones)
- `:deadkeys` (zset lex index for tombstones)
- `:deadexp` (zset by tombstone cleanup time)
- `:pref` (hash of namespace prefix refcounts)
- `:seq` (payload-version counter)
- `:ev:root` (full-registry stream)
- `:ev:key:{logicalKey}` (exact-key stream)
- `:ev:px:{prefix}` (namespace-prefix stream)

Notes:

- `eventRetentionMs` is enforced on the root stream replay window.
- `eventMaxLen` bounds the root, key, and prefix streams.
