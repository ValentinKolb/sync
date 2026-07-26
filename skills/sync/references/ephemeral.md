# ephemeral

TTL-based key/value store with tenant isolation, snapshots with optional prefix filter, and change-stream reader. **Replaces the old `registry` module in v5** via the `prefix` filter.

## Factory

```ts
import { ephemeral, EphemeralCapacityError, EphemeralPayloadTooLargeError } from "@k2b/sync";

const presence = ephemeral<{ userId: string; displayName: string }>({
  id: "notebook.presence",
  ttlMs: 30_000,
  // prefix?: string,                                     // default: "sync:e"
  // tenantId?: string,                                   // default: "default"
  // limits?: { maxEntries, maxPayloadBytes, eventRetentionMs, eventMaxLen },
});
```

**No `schema` config in v5.** Use the `<T>` generic.

## API

```ts
type EphemeralStore<T> = {
  upsert(cfg: EphemeralUpsertConfig<T>): Promise<EphemeralEntry<T>>;
  touch(cfg: EphemeralTouchConfig): Promise<{ ok: boolean; version?: string; expiresAt?: number }>;
  remove(cfg: EphemeralRemoveConfig): Promise<boolean>;
  snapshot(cfg?: { tenantId?: string; prefix?: string }): Promise<EphemeralSnapshot<T>>;
  reader(cfg?: { after?: string; tenantId?: string; prefix?: string }): EphemeralReader<T>;
};

type EphemeralEntry<T> = {
  key: string;
  value: T;
  version: string;
  createdAt: number;       // first upsert of this key (preserved across touch/upsert)
  updatedAt: number;       // most recent touch or upsert
  expiresAt: number;
};

type EphemeralEvent<T> =
  | { type: "upsert"; cursor: string; entry: EphemeralEntry<T> }
  | { type: "touch"; cursor: string; key: string; version: string; expiresAt: number }
  | { type: "delete"; cursor: string; key: string; version: string; deletedAt: number; reason?: string }
  | { type: "expire"; cursor: string; key: string; version: string; expiredAt: number }
  | { type: "overflow"; cursor: string; after: string; firstAvailable: string };
```

## Usage

### Presence (per-notebook, tenant-isolated)

```ts
// Join
await presence.upsert({
  tenantId: noteId,              // each notebook has its own event stream + TTL zone
  key: peerId,
  value: { userId, displayName },
});

// Heartbeat (extends TTL)
await presence.touch({ tenantId: noteId, key: peerId });

// Snapshot
const snap = await presence.snapshot({ tenantId: noteId });
// snap.entries — live (TTL-valid) entries, sorted by key
// snap.cursor — use with reader({ after }) to get events since snapshot

// Live updates
for await (const event of presence.reader({ tenantId: noteId, after: snap.cursor }).stream()) {
  // upsert / touch / delete / expire / overflow
}
```

### Registry replacement (prefix filter within a tenant)

```ts
const apps = ephemeral<{ version: string; endpoints: string[] }>({
  id: "app-registry",
  ttlMs: 90_000, // heartbeat every 30s × 3
});

await apps.upsert({ key: "apps/backend", value: { ... } });
await apps.upsert({ key: "services/cache", value: { ... } });

// Snapshot just the "apps/" prefix (server: ZRANGEBYLEX; browser: startsWith)
const allApps = await apps.snapshot({ prefix: "apps/" });

// Admin-UI: show uptime per app
for (const entry of allApps.entries) {
  const uptimeMs = Date.now() - entry.createdAt;
  const lastHeartbeatMs = Date.now() - entry.updatedAt;
  console.log(`${entry.key} up for ${uptimeMs}ms, last heartbeat ${lastHeartbeatMs}ms ago`);
}

// Reader filtered by prefix (only events for matching keys flow)
for await (const event of apps.reader({ prefix: "apps/" }).stream()) {
  // ...
}
```

## Gotchas

- **`tenantId` vs `prefix`**: `tenantId` is **hard isolation** (separate Redis stream, separate TTL zone, separate `maxEntries` quota). `prefix` is **soft filter** within a tenant. Use `tenantId` for multi-tenant apps where streams must be separate; use `prefix` for organizing keys inside one namespace.
- **`reader` prefix filter**: server Redis Streams can't do server-side filtering, so `reader({ prefix })` discards non-matching events in the client. Still useful but doesn't save bandwidth server-side.
- **`overflow` events** fire when the reader's cursor fell behind the retention window. Handle by re-snapshotting.
- **`maxEntries` per tenant**: once reached, `upsert` throws `EphemeralCapacityError`. Scale via multiple tenants or raise the limit.
- **Touch returns `{ ok: false }`** if the key already expired between check and touch.
- **`createdAt` vs `updatedAt`**: `createdAt` is set once when the entry is first upserted and **preserved** across subsequent `upsert` (even with new values) and `touch` calls on the same key. It only resets after `remove` or TTL expiry followed by a fresh upsert. Use `Date.now() - entry.createdAt` for "how long has this been registered" displays (admin UI uptime). `updatedAt` tracks the most recent touch/upsert — use for "last heartbeat" semantics.

## Redis keys (server)

- `{prefix}:{tenantId}:{id}:state` — hash, fields = keys, values = stored entry JSON
- `{prefix}:{tenantId}:{id}:exp` — sorted set of expirations (reconciled during reads)
- `{prefix}:{tenantId}:{id}:events` — Redis Stream
- `{prefix}:{tenantId}:{id}:seq` — version counter
