# Migrating @k2b/sync v5 → v6

v6 replaces the Redis server runtime completely with NATS 2.14+ and JetStream. This is a **hard cut**:

- No Redis compatibility layer, no generic transport interface.
- **No migration of v5 Redis state.** Durable Redis queue/topic/job/scheduler state is not readable by v6.
- `@k2b/sync/browser` is removed. Only the local `retry` helper remains browser-safe, via `@k2b/sync/retry`.
- `ratelimit` is removed from Sync. Keep rate limiting where your Redis lives (it left Sync precisely because Redis stays application-owned for sessions/KV).
- Internal APIs, wire formats, and resource layouts are all new.

## Before you upgrade

1. **Provision NATS.** A 2.14+ cluster with JetStream and persistent storage (three nodes recommended). Sync will not create or configure servers.
2. **Drain v5 work.** Stop producers, let workers finish or explicitly disposition accepted Redis work (finish it, export the few items that matter, or accept their loss after checking inactivity). v6 cannot see it.
3. **Audit stored references.** Rows in your database holding v5 cursors, schedule ids, job ids, or resource names must be migrated or reset deliberately — v6 cursors and ids use new formats.

## Construction: modules → one Sync instance

v5 modules were standalone factories bound to a global Redis connection. v6 has one explicit entry point on a caller-owned connection:

```ts
// v5
import { queue } from "@k2b/sync";
const emails = queue<Email>({ id: "emails" });

// v6
import { connect } from "@nats-io/transport-node";
import { createSync } from "@k2b/sync";

const connection = await connect({ servers: [...] });
const sync = createSync({ connection, namespace: "prod", application: "mailer" });
const emails = sync.queue<Email>({ id: "emails" });
await sync.ready();
```

- `namespace` isolates deployments; `application` is ownership/diagnostics metadata.
- Sync reads no environment variables and loads no credentials.
- Create the instance once per process; shut down with `await sync.drain()` **before** closing the connection.

## Per module

| v5 | v6 | Notes |
| --- | --- | --- |
| `ratelimit(...)` | — | Removed. Keep it next to your Redis. |
| `mutex({ id })` | `sync.mutex({ id })` | `Lock.value` → `Lock.ownerToken`; new monotonic `Lock.fence` (bigint) for fencing stale writers. `withLockOrThrow` is gone — check `withLock`'s `null`. `acquire`/`withLock` take a single input object; `retry.maxAttempts` counts total tries. TTLs round up to whole seconds. |
| `queue({ id })` | `sync.queue({ id })` | `recv`/lease API → `process()` (auto-settling) or `reader()` (manual `ack`/`retry`/`deadLetter`). `delayMs` now uses broker-side message schedules. Partitioned per-key ordering is new (`ordering: { mode: "partitioned" }`). |
| `topic({ id })` | `sync.topic({ id, retention })` | Retention is required and explicit. Reads split into `live()` / `replay()` / `follow()` / `process({ consumer })`. Consumer groups are named durable consumers; pending recovery is NATS redelivery. Cursors are new and resource-bound; retention gaps throw `RetentionGapError`. |
| `job({ id })` | `sync.job({ id })` | `submit({ key, input })` — the key is now the idempotency key (deduped within `dedupeWindowMs`). Lifecycle `after` callback → `onError` decision (`retry` / `dead_letter`). No stored results; keep run state in your database. `submitMany` adds bounded fan-out. |
| `pump({ id, pull, dispatch })` | `sync.pump({ id, pull, dispatch })` | Same shape. State/checkpoints now live in NATS KV; `reconcile()` repairs lost wake-ups; `leaseMs` tunes crash takeover. Sinks must stay idempotent by `item.key`. |
| `scheduler({ id })` | `sync.scheduler({ id })` | Same 5-field cron and timezone model. The broker (NATS message schedules) is now the clock: ticks are produced and retained even when no app process runs. Leader election is gone — per-schedule serial consumers replace it; `process()` (not `start()`) serves them. `misfire: "latest" \| "all"` replaces catch-up options. `runNow({ id, requestId })` deduplicates per request id within the 120 s duplicate window. Tick retention is `{ maxAgeMs, maxTicksPerSchedule }` (per schedule, not global bytes). |
| `ephemeral({ id, ttlMs })` | `sync.ephemeral({ id, ttlMs })` | Same role. `watch({ after })` replaces the change-stream reader; a too-old revision yields one `resync_required` event and ends. TTLs round up to whole seconds (min 1s). |
| — | `sync.objectStore({ id, retention, maxObjectBytes })` | New: explicit streamed artifacts with `ObjectRef` for queue/job payloads. Sync never auto-offloads oversized payloads. |
| `retry(...)` | `retry(...)` from `@k2b/sync` or `@k2b/sync/retry` | Unchanged callback model; Redis-specific error codes removed from `isRetryableTransportError()`. |

## Browser consumers

There is no v6 browser runtime. What to do instead:

- **retry** → `@k2b/sync/retry` (no NATS, no Bun).
- Local queues/topics/presence emulation → application-local code or your state library; the v5 browser package's semantics were parity shims, not a contract worth carrying.

## Semantics that changed underneath you

- **At-least-once, everywhere durable.** Redelivery is intentional; handlers must be idempotent. There is no exactly-once mode.
- **Dedupe is windowed.** An `idempotencyKey`/job key deduplicates only within `dedupeWindowMs` (default 2 min). Permanent uniqueness belongs in your database.
- **Late acks are not detectable.** After redelivery, NATS accepts the superseded ack idempotently. `StaleDeliveryError` appears only when an ack cannot be confirmed at all.
- **Global concurrency is `delivery.maxInFlight`** (NATS MaxAckPending) shared across pods; `concurrency` is per-process-per-handle. Neither is a fair semaphore.
- **Resources drift-check.** Changing delivery/retention/partitions of an existing resource throws `ResourceDriftError` on start instead of silently reconfiguring. Apply intentional changes with operational tooling, then roll pods.

## Verification checklist

- [ ] `sync.ready()` succeeds against your real cluster from every app.
- [ ] Workers drain cleanly (`sync.drain()` before `connection.drain()`).
- [ ] Kill -9 a worker pod: its in-flight work redelivers after `ackWaitMs`.
- [ ] Restart everything: accepted jobs, pump runs, and schedule ticks resume.
- [ ] DLQ inspection/requeue wired into your ops tooling.
