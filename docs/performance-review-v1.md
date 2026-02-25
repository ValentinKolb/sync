# Performance Review v1

Date: 2026-02-20
Scope: `ratelimit`, `mutex`, `queue`, `topic`, `job`

## What was optimized now

1. Persistent blocking clients for queue reads:
- `queue.recv/stream` no longer opens/closes a `RedisClient` on every blocking call.
- Implemented in `/Users/valentinkolb/Git/sync/src/queue.ts`.

2. Persistent blocking clients for topic reads:
- `topic.reader(...).recv/stream` keeps a dedicated blocking client per reader.
- `topic.live(...)` reuses a dedicated client for the lifetime of the iterator (when no abort signal is used).
- Implemented in `/Users/valentinkolb/Git/sync/src/topic.ts`.

3. Group creation caching for topic:
- `XGROUP CREATE` is cached per `streamKey + group`, avoiding repeated BUSYGROUP roundtrips.
- Implemented in `/Users/valentinkolb/Git/sync/src/topic.ts`.

4. Queue maintenance throttling:
- `runMaintenance` is no longer executed on every single recv call.
- Throttled per tenant with a short interval.
- Implemented in `/Users/valentinkolb/Git/sync/src/queue.ts`.

## Current bottlenecks and tradeoffs

## P1 (high impact)

1. Lua script dispatch uses `EVAL` every time:
- Modules repeatedly send script source text (`queue`, `topic`, `ratelimit`, `mutex`).
- Cost: larger payloads + repeated server-side script parsing.
- Recommendation: load scripts once (`SCRIPT LOAD`) and call via `EVALSHA` with fallback to reload.

2. Queue maintenance is still consumer-driven:
- Even with throttling, each process may run overlapping maintenance cycles.
- Cost: duplicated work under high horizontal scale.
- Recommendation: leader/lease-based maintenance worker per queue (or randomized staggered maintenance).

3. Job join polling:
- `job.join` currently loops with `HGET` + sleep.
- Cost: Redis read amplification for many concurrent waiters.
- Recommendation: switch join waiting to blocking primitive (e.g. queue/topic signal) or exponential polling backoff.

## P2 (medium impact)

1. High key cardinality for job event streams:
- `job.events(id)` maps to per-job topic stream key.
- Cost: large number of Redis keys for high job throughput.
- Recommendation: optional partitioned event stream (`job.events:<partition>`) with `jobId` in payload + group filtering.

2. Queue active list cleanup uses `LREM`:
- Several scripts perform `LREM` on active list.
- Cost: O(n) behavior can degrade for large active queues.
- Recommendation: use structures with cheaper removal (e.g. hash/set + sorted leases, avoid list scans).

3. Mutex retry path logs on each acquire error:
- Under connection issues this can produce heavy stderr volume.
- Recommendation: use structured/lower-frequency logging (or hook-based logger with rate limiting).

## P3 (lower impact / ergonomics)

1. Reader lifecycle and connection cleanup:
- Readers keep dedicated clients; this is intended for hot paths.
- Tradeoff: one-off readers may leave idle connections until GC/lifecycle end.
- Recommendation: optional explicit `.close()` on reader handles for manual lifecycle control.

2. Topic live with abort signal:
- Abort-enabled live reads use temporary clients per blocking call for safe cancellation.
- Recommendation: optional advanced mode with persistent client + abort hook if needed for very high-frequency live streams.

## Quick next-step backlog

1. Implement shared script cache (`EVALSHA`) across all modules.
2. Add single-maintainer mode for queue delayed/lease promotion.
3. Add `join` wait optimization (signal-based or adaptive backoff).
4. Add optional `reader.close()` APIs for queue/topic.

