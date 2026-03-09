---
name: sync-registry
description: "Use this skill when implementing typed service/config registries with @valentinkolb/sync registry: exact-key lookups, Redis-native prefix listing, compare-and-swap updates, optional TTL-backed liveness via touch(), snapshot-plus-cursor cache hydration, scoped change-stream readers for keys/prefixes/whole registry, and tombstone retention for expired entries. Also use when choosing between registry (name-addressable, CAS, prefix queries) vs ephemeral (TTL-only, simpler)."
---

# Sync Registry

Name-addressable typed records with optional TTL-backed liveness, CAS updates, and Redis-native prefix queries. Combines the change-streaming of ephemeral with durable key/value semantics.

## Decision Guide: registry vs ephemeral

- **registry**: entries can be static (no TTL) or live (with TTL). Supports `get()`, `list()` with prefix filtering, `cas()` for optimistic concurrency. Tombstone retention for expired entries. Best for service discovery, config, feature flags.
- **ephemeral**: all entries MUST have TTL. Simpler API, no CAS, no prefix queries. Best for pure presence/session state. See `sync-ephemeral` skill.

## Typical Pattern: Prefix Snapshot + Stream

```
list({ prefix: "apps/contacts/" }) → hydrate → reader({ prefix: "apps/contacts/", after: cursor }).stream() → apply events
```

On `overflow`, re-list and restart.

## Key Design Rules

- Keys use `/` as namespace separator. Prefix queries require trailing `/` (e.g. `"apps/contacts/"`).
- Keys must not end with `/`, must not contain `//`, max 8 slash-delimited segments, max 512 bytes.
- `touch()` extends TTL without bumping `version` — only for TTL-backed records.
- `cas()` compares `entry.version` atomically. Returns `{ ok: false }` on mismatch or missing entry.
- Expired entries become tombstones queryable via `get({ includeExpired: true })` or `list({ status: "expired" })` for `tombstoneRetentionMs` (default 5 min).

## Gotchas

- Records without `ttlMs` in `upsert()` are static — `touch()` will return `{ ok: false }` on them.
- `maxEntries` default 10,000. Throws `RegistryCapacityError` when full (new active keys only).
- `maxPayloadBytes` default 128KB. Throws `RegistryPayloadTooLargeError`.
- Reader scoping: `reader({ key })` = exact key, `reader({ prefix })` = namespace, `reader()` = whole registry. Each scope has its own Redis stream.
- `stream({ wait: true })` auto-retries transient errors. Do not wrap with `retry()`.
- `eventRetentionMs` default 5 min. Slow consumers get `overflow`.

## API Reference

Read full API/types/config/defaults in [references/api.md](references/api.md).
