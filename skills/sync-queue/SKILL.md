---
name: sync-queue
description: "Use this skill when implementing durable queue processing with @valentinkolb/sync queue: send/recv/stream loops, at-least-once delivery, lease management, retries via nack delays, DLQ behavior, multi-tenant keyspaces, and idempotent enqueueing."
---

# Sync Queue

Use this skill to implement durable work distribution with `queue()`.

## Workflow

1. Define a strict Zod schema for payloads.
2. Instantiate one queue per logical workload.
3. Send messages with optional `idempotencyKey` and `delayMs`.
4. Consume with `recv()` or `stream()`.
5. `ack()` on success, `nack()` on retryable failure, `touch()` for long handlers.
6. Use `reader()` to run multiple independent blocking consumers per process.

## Behavioral Guarantees

- Provide at-least-once delivery semantics.
- Perform send/claim/ack/nack/touch/maintenance atomically via Lua scripts.
- Use lease-based visibility timeout; expired leases are reclaimed.
- Move aged-out or over-delivered messages to DLQ.
- Validate payloads at send boundary and parse again at receive boundary.

## Non-Guarantees

- Do not provide exactly-once processing.
- Do not preserve strict global ordering across retries/failures.
- Do not guarantee immediate redelivery timing under Redis/backpressure delays.

## API Reference

Read full API/types/config/defaults in [references/api.md](references/api.md).
