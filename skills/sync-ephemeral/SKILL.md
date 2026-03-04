---
name: sync-ephemeral
description: Use this skill when implementing short-lived typed state with @valentinkolb/sync ephemeral: TTL-based upsert/touch/remove, snapshot-plus-cursor reconciliation, stream consumers for upsert/touch/delete/expire events, and capacity/payload safety limits.
---

# Sync Ephemeral

Use this skill for TTL-scoped key/value state that should naturally expire.

## Workflow

1. Define strict value schema.
2. Instantiate `ephemeral()` with default TTL.
3. Use `upsert()` for create/update, `touch()` for lease extension, `remove()` for explicit delete.
4. Use `snapshot()` to load current state and cursor.
5. Use `reader({ after: snapshot.cursor })` for incremental updates.
6. Handle `overflow` events by re-snapshotting.

## Behavioral Guarantees

- Write operations are atomic Lua scripts.
- Each mutation emits stream event (`upsert`, `touch`, `delete`, `expire`).
- Expiration reconciliation removes stale entries and emits `expire`.
- Snapshot runs full reconcile before returning entries.
- Payloads are validated by schema and size-limited.

## Non-Guarantees

- Do not provide durable historical event logs beyond retention window.
- Do not preserve data after TTL expiration by design.
- Do not guarantee replay if consumer cursor falls behind retention window (overflow signal instead).

## API Reference

Read full API/types/config/defaults in [references/api.md](references/api.md).
