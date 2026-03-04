---
name: sync-topic
description: Use this skill when implementing event streams with @valentinkolb/sync topic: publishing typed events, consumer-group processing with commit, live replay, retention tuning, idempotent publish, and multi-tenant stream isolation.
---

# Sync Topic

Use this skill for typed pub/sub and event-stream workflows with `topic()`.

## Workflow

1. Define a strict event schema.
2. Instantiate topic with retention and payload limits.
3. Publish with optional idempotency and metadata.
4. Consume via `reader(group)` for at-least-once processing.
5. Call `commit()` after durable downstream handling.
6. Use `live({ after })` for replay or fan-out listeners.

## Behavioral Guarantees

- Publish atomically (`XADD + optional idempotency SET + XTRIM`) via Lua.
- Consumer groups provide at-least-once semantics.
- Group creation is idempotent and auto-managed.
- Live consumers can replay from any stream cursor.
- Payloads are schema-validated on publish and on read path.

## Non-Guarantees

- Do not provide exactly-once delivery.
- Do not auto-recover pending entries of dead consumers (no explicit XAUTOCLAIM flow).
- Do not guarantee strict total ordering across independent consumer groups.

## API Reference

Read full API/types/config/defaults in [references/api.md](references/api.md).
