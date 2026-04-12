---
name: sync-job
description: "Use this skill when implementing durable background jobs with @valentinkolb/sync job: defining typed process handlers with ctx.step/ctx.heartbeat/ctx.signal, submit/join/cancel flows, idempotent submission via key, retries with exponential backoff, lease timeouts, and per-job event streams for audit. Also use when choosing between job (durable execution with state) vs queue (simple work distribution). Also works in the browser via `@valentinkolb/sync/browser` with in-memory state — same API, no Redis needed."
---

# Sync Job

Async execution with lifecycle: submit, process (with retries), join for result, cancel cooperatively. Server version is durable (Redis-backed), browser version runs in-memory. Built on top of `queue` and `topic` internally.

## Decision Guide: job vs queue

- **job**: full lifecycle (submitted → running → completed/failed/cancelled). Has state, retries, backoff, `join()`, `cancel()`, event audit stream. Best for background tasks where you need to track outcome.
- **queue**: raw work distribution with ack/nack. No state machine, no join. Best for fire-and-forget pipelines. See `sync-queue` skill.

## Handler Pattern

```ts
process: async ({ ctx, input }) => {
  // 1. Check cancellation
  if (ctx.signal.aborted) return;

  // 2. Use step() for resumable sub-tasks
  await ctx.step({ id: "fetch", run: () => fetchData(input) });

  // 3. Heartbeat for long work (extends queue lease)
  await ctx.heartbeat();

  // 4. Return result (stored in terminal state)
  return { success: true };
}
```

## Gotchas

- `submit()` auto-starts the worker loop on the calling process. No separate `start()` needed.
- `key` on submit is the idempotency key. Same key returns same job ID. Use for retry-safe submissions.
- `maxAttempts` default is `1` (no retry). Set explicitly for retryable jobs.
- `join()` with no `timeoutMs` still returns eventually — as `{ status: "timed_out", error.code: "JOIN_TIMEOUT" }`.
- `cancel()` is cooperative: it sets state to `cancelled` only if non-terminal. The handler must check `ctx.signal.aborted`.
- `ctx.heartbeat()` extends the underlying queue lease. Call it in long-running handlers to prevent lease expiry and re-delivery.
- State is retained for 7 days (`DEFAULT_STATE_RETENTION_MS`). After that, `join()` cannot find the result.
- Worker receive loop auto-retries transient transport errors — survives brief Redis outages.

## Browser

```ts
import { job } from "@valentinkolb/sync/browser";
```

Same API (submit/join/cancel/events/stop). Browser-specific notes:
- Job state, the work queue, and event topics all run in-memory.
- State transitions (finalize, cancel CAS) are synchronous — safe because JS is single-threaded.
- `stop()` is still important to halt the worker loop and prevent memory leaks.
- Jobs are lost on page refresh — not durable. Best for in-session background tasks.
- `ctx.step()`, `ctx.heartbeat()`, and `ctx.signal` all work identically.

## API Reference

Read full API/types/config/defaults in [references/api.md](references/api.md).
