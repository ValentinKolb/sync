---
name: sync-job
description: "Use this skill when implementing durable background jobs with @valentinkolb/sync job: typed submit/join/cancel flows, retries and backoff, lease timeouts, heartbeat-based lease extension, event-stream integration, and idempotent submission semantics."
---

# Sync Job

Use this skill for durable asynchronous execution with `job()`.

## Workflow

1. Define job schema and `process` handler.
2. Create one `job()` handle per job type.
3. Submit with optional idempotency key and retry settings.
4. Inside handler: check `ctx.signal`, call `ctx.heartbeat()` for long work.
5. Use `join()` for request-response orchestration.
6. Use `cancel()` for cooperative cancellation.
7. Use `events(id)` for audit stream readers.

## Behavioral Guarantees

- Persist queue work + state + event stream (Redis-backed durability).
- Process with at-least-once semantics.
- Guard terminal state transitions with Lua CAS scripts.
- Recover partial submit windows where idempotency key exists but state is missing.
- Recover worker windows where queue message exists but state key is missing.

## Non-Guarantees

- Do not provide exactly-once execution.
- Do not preserve in-process work on hard termination without retry path.
- Do not guarantee `join()` success without timeout (returns `timed_out` terminal result).

## API Reference

Read full API/types/config/defaults in [references/api.md](references/api.md).
