---
name: sync-queue
description: "Use this skill when implementing durable queue processing with @valentinkolb/sync queue: send/recv/stream consumer loops, at-least-once delivery with ack/nack/touch, lease-based visibility timeout, delayed messages, DLQ behavior, idempotent enqueueing, and multi-tenant keyspaces. Also use when choosing between queue (work distribution) vs topic (event fan-out) vs job (durable execution with state). Also works in the browser via `@valentinkolb/sync-browser` with in-memory state — same API, no Redis needed."
---

# Sync Queue

At-least-once work queue. Server version is Redis-backed and durable, browser version uses in-memory state. Messages are delivered to exactly one consumer per delivery attempt.

## Consumer Pattern

```ts
for await (const msg of q.stream({ signal: ac.signal })) {
  try {
    await handle(msg.data);
    await msg.ack();
  } catch (error) {
    await msg.nack({ delayMs: 3000, reason: "handler_error" });
  }
}
```

## Decision Guide

- `ack()`: processing succeeded. Message removed.
- `nack({ delayMs? })`: processing failed, re-queue after delay. After `maxDeliveries` (default 10), moves to DLQ.
- `touch({ leaseMs? })`: extend visibility timeout for long-running handlers. Call before lease expires.
- `reader()`: create additional independent blocking consumers within the same process.

## Gotchas

- `stream({ wait: true })` auto-retries transient transport errors. Do not wrap with `retry()`.
- `ack()` and `nack()` can return `false` if lease already expired (another consumer may have reclaimed).
- `maxDeliveries` default is 10. After that, message goes to DLQ with reason `max_deliveries_exceeded`.
- `defaultLeaseMs` is 30s. If handler takes longer, call `touch()` or it will be reclaimed.
- `maxMessageAgeMs` default 7 days. Old unprocessed messages go to DLQ with reason `expired`.
- `ordering` config is accepted but **not enforced at runtime** — `orderingKey` is stored as metadata only.
- Maintenance (lease reclaim, delayed→ready promotion, DLQ moves) runs automatically during recv calls.
- Payload max 128KB. Validated at send and re-parsed at receive.
- `idempotencyKey` with `idempotencyTtlMs` prevents duplicate sends (default TTL 7 days).

## Browser

```ts
import { queue } from "@valentinkolb/sync-browser";
```

Same API (send/recv/stream/ack/nack/touch). Browser-specific notes:
- Queue state (ready list, delayed messages, deliveries, DLQ) lives in JS heap — lost on page refresh.
- Blocking `recv({ wait: true })` uses a Promise-based event emitter instead of Redis `BLMOVE`.
- Maintenance (lease reclaim, delayed→ready promotion) runs lazily on each `recv()` call, same as server.
- No transport error retries wrapping — no network errors possible in-memory.
- `stream({ wait: true })` works identically but without the retry wrapper.

## API Reference

Read full API/types/config/defaults in [references/api.md](references/api.md).
