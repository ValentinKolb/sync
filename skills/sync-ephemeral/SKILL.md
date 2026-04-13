---
name: sync-ephemeral
description: "Use this skill when implementing short-lived typed state with @valentinkolb/sync ephemeral: TTL-based key/value with upsert/touch/remove, snapshot-plus-cursor reconciliation for cache hydration, streaming upsert/touch/delete/expire events, capacity/payload limits, and presence-style use cases where entries should naturally expire. Also works in the browser via `@valentinkolb/sync-browser` with in-memory state — same API, no Redis needed."
---

# Sync Ephemeral

TTL-scoped typed key/value store where all entries must have a time-to-live. Server version uses Redis, browser version runs entirely in-memory. Ideal for presence, sessions, and temporary state.

## Typical Pattern: Snapshot + Stream

```
snapshot() → hydrate local state → reader({ after: cursor }).stream() → apply incremental events
```

On `overflow` event (cursor fell behind retention window), re-snapshot and restart the stream.

## Decision Guide

- **Create/update entry**: `upsert({ key, value })` — resets TTL.
- **Extend TTL without changing value**: `touch({ key })` — returns `{ ok: false }` if key missing.
- **Explicit removal**: `remove({ key, reason? })` — emits `delete` event.
- **Multi-tenant**: pass `tenantId` on factory or per-operation override.

## Gotchas

- Every entry MUST have a TTL (`ttlMs` on factory is required, > 0). This is not optional storage.
- `maxEntries` default is 10,000. `upsert()` throws `EphemeralCapacityError` when full.
- `maxPayloadBytes` default is 4,096. `upsert()` throws `EphemeralPayloadTooLargeError` on oversized JSON.
- Logical key must be non-empty and <= 512 bytes.
- `snapshot()` runs internal reconciliation (expiry cleanup) before returning — may be slow on first call with many stale entries.
- `stream({ wait: true })` auto-retries transient transport errors internally. Do not wrap with `retry()`.
- Event stream retention default is 5 minutes (`eventRetentionMs`). Slow consumers will get `overflow`.

## Browser

```ts
import { ephemeral, EphemeralCapacityError } from "@valentinkolb/sync-browser";
```

Same API. Browser-specific notes:
- TTL expiration uses `setTimeout` callbacks instead of Redis key expiry + reconciliation.
- No reconciliation loop needed — `setTimeout` fires the expiry directly and emits the `expire` event.
- Events use an in-memory `EventLog` instead of Redis Streams.
- `snapshot()` is instant (no reconciliation pass) since expired entries are removed eagerly by timers.
- State is single-tab, lost on page refresh.
- `setTimeout` may be throttled in background tabs — TTL precision is best-effort.

## API Reference

Read full API/types/config/defaults in [references/api.md](references/api.md).
