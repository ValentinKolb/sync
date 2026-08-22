# Sync v6: NATS-native hard cut

## TL;DR

Sync v6 replaces the Redis server runtime completely with NATS 2.14+ and
JetStream. There is no Redis compatibility layer, no generic transport
abstraction, and no migration of v5 Redis state. `ratelimit` leaves Sync;
`mutex` remains a Sync primitive. Cloud is the primary acceptance consumer,
but Cloud code, deployment, data cutover, and its NATS admin UI are explicitly
owned by a separate Cloud epic and must not be implemented by the Sync agent.

Internal APIs and schemas may break. After the separate Cloud migration,
end-user-visible behavior must still just work, including after a complete
Cloud restart.

## Direction and boundaries

### Owned by this Sync epic

- Replace the server package's Redis implementation with NATS/JetStream.
- Require a caller-provided, already connected NATS client. Sync does not read
  environment variables, load credentials, or create infrastructure.
- Keep `mutex`, `queue`, `topic`, `ephemeral`, `job`, `pump`, `scheduler`, and
  `retry`, with explicit v6 contracts and recovery semantics.
- Remove `ratelimit` and all Redis code, configuration, tests, documentation,
  migrations, and dependencies from Sync.
- Add first-class diagnostics and resource metadata that a consumer can use to
  build operational tooling.
- Publish a hard-cut migration guide and a Cloud consumer contract/checklist.
- Verify Bun compatibility against a real NATS cluster and failure scenarios.

### Explicitly not owned by this Sync epic

- No edits in `/Users/valentinkolb/Git/stuve/cloud`.
- No Cloud consumer migration.
- No Cloud deployment, Kubernetes, secrets, TLS, account provisioning, or
  production cutover.
- No Cloud NATS admin page, API, CLI, dashboards, or alerts.
- No migration of durable Redis data into NATS.
- No Redis session, KV, or rate-limit replacement in Cloud.

These are retained below as external meta-TODOs because Cloud compatibility is
the primary acceptance boundary for Sync v6.

## Proposed v6 runtime API

Sync accepts infrastructure through dependency injection:

```ts
const sync = createSync({
  connection: natsConnection,
  namespace: "cloud-prod",
  application: "notebooks",
});

await sync.ready();

const events = sync.topic<NotebookEvent>({ id: "workspace-events" });
```

Rules:

- One NATS connection is shared by all Sync primitives in one app process.
- `namespace` isolates deployments such as development, staging, and
  production.
- `application` is client/ownership metadata, not automatic isolation: Cloud
  has cross-app resources, so resource IDs remain explicit.
- Sync never reads `NATS_*` or Cloud-specific environment variables.
- Sync owns graceful draining of its workers/readers; the application owns the
  underlying connection lifecycle.
- Creating a primitive must not silently create incompatible server resources.
  Resource configuration drift fails clearly and is visible in diagnostics.

## Primitive direction

### Topic

- JetStream stream-based durable events.
- Independent cursor readers remain possible; multiple cursors can replay the
  same stream without affecting each other.
- Durable consumer groups provide at-least-once delivery, acknowledgement,
  pending recovery, and automatic load balancing across group members.
- A live/broadcast mode remains available for listeners that should all see an
  event.
- Retention gaps must be explicit rather than silently skipping data.

### Queue

- JetStream work-queue semantics with at-least-once delivery.
- Competing consumers are automatically load-balanced.
- Preserve delayed delivery, lease/ack/nack behavior, idempotency, bounded
  retries, and DLQ inspection through a clear v6 contract.
- Application handlers remain idempotent because redelivery is intentional.

### Job

- Compose queue delivery, idempotent submission, processing, retry decisions,
  lifecycle callbacks, and traces.
- Accepted work survives process and complete Cloud restarts.
- No promise of exactly-once execution.

### Pump

- Keep the durable cursor/checkpoint abstraction for imports, backfills,
  reconciliation, and reindexing.
- Persist run/page/checkpoint state in JetStream/KV where the contract requires
  it.
- Preserve at-least-once dispatch and explicit idempotent sinks.

### Scheduler

- Keep Sync's higher-level cron model; NATS does not replace the complete
  scheduler domain contract.
- Persist schedule definitions and next-run state in NATS KV/JetStream.
- Use NATS coordination for distributed ownership/fencing.
- Define restart/misfire behavior explicitly and test schedules becoming due
  while every Cloud process is offline.
- Preserve remote control and traceability where useful; do not pretend an
  accepted manual trigger is already completed.

### Mutex

- Remains in Sync.
- Implement using NATS KV compare-and-set plus TTL/lease ownership and fencing.
- Owner-only release and extension remain mandatory.
- Document that a lock lease alone cannot prevent stale external writes;
  consumers needing strict correctness must use fencing/idempotency.

### Ephemeral

- NATS KV with TTL/watch for presence, service registry, and transient state.
- Preserve tenant/prefix isolation, bounded snapshots, and explicit overflow or
  resync behavior.

### Retry

- Remains transport-independent and local.
- Keep abort-aware exponential backoff and explicit retry decisions.

### Rate limit

- Remove from Sync v6.
- Cloud keeps rate limiting as Cloud-owned Redis logic because Redis remains in
  Cloud for sessions and other KV state.

## Observability contract owned by Sync

Sync exposes enough structured information for consumers without implementing
their UI:

- connection and readiness state;
- reconnect, consumer, redelivery, ack/nack, DLQ, lease, lock, schedule, pump,
  and resource-drift events;
- stable resource names and ownership metadata;
- sanitized stream, consumer, KV, and scheduler summaries;
- trace hooks that cannot alter transport state;
- health/readiness diagnostics with no credentials or payload leakage;
- documented mapping from Sync resource IDs to NATS subjects/resources.

## Verification gates for Sync v6

- Bun can connect, authenticate, publish, consume, drain, and reconnect using
  the selected official NATS JavaScript client.
- Tests run against a real three-node NATS 2.14+ cluster with JetStream and
  persistent storage.
- Node loss, process death, reconnect, duplicate delivery, stale consumer,
  retention gap, complete client outage, and restart recovery are covered.
- Multiple independent topic cursors and competing group consumers are tested.
- Queue/job/pump/scheduler accepted state survives complete client restarts.
- Mutex ownership, expiry, extension, stale release, and fencing semantics are
  tested.
- Resource configuration drift fails visibly.
- Package/typecheck/build/packed-consumer checks pass under Bun.
- No Redis runtime code, dependency, config, or documentation remains.

## External Cloud meta-epic

The following items are requirements for the later Cloud migration, not tasks
for the Sync agent.

### Cloud framework integration

- Create one NATS connection and one Sync instance per app process through the
  Cloud app lifecycle.
- Make the same app-scoped Sync instance available outside HTTP requests. A
  request context alone is insufficient for schedulers, workers, WebSockets,
  and lifecycle services.
- Intended access shape: `app.sync` for general app/service code and the same
  instance as `lifecycleContext.sync` for startup/shutdown work.
- Do not make Sync user- or request-scoped. Tenant/user authorization remains
  explicit application logic.
- Start serving or report ready only after NATS/Sync readiness; drain app
  workers and Sync before closing the NATS connection.

### `defineApp` lifecycle decision

Two Cloud-owned designs are valid and must be evaluated against build and test
imports:

1. **Recommended default:** keep `defineApp()` declarative and synchronous;
   `await app.start()` connects NATS and binds a ready `app.sync`. Code must not
   perform Sync I/O during module evaluation.
2. **Allowed breaking alternative:** make `defineApp()` async and use
   `const app = await defineApp(...)`, so `app.sync` can be fully constructed
   immediately. This is only acceptable if importing app config for SSR builds,
   tests, metadata, or tooling does not unexpectedly require live NATS.

Do not introduce a magical unbounded promise queue merely to hide pre-start
access. Whichever design Cloud chooses, use a clear readiness contract and fail
clearly on lifecycle misuse.

### Cloud configuration and authentication

Suggested application configuration:

```env
NATS_SERVERS=nats://nats-0.nats:4222,nats://nats-1.nats:4222,nats://nats-2.nats:4222
NATS_CREDS_FILE=/run/secrets/cloud.nats.creds
NATS_TLS_CA_FILE=/run/secrets/nats-ca.crt
SYNC_NAMESPACE=cloud-prod
```

- `NATS_SERVERS` contains bootstrap/seed endpoints; Cloud does not create the
  cluster.
- Mount credentials as secret files and put only their paths in environment
  variables.
- Generate a useful connection name from app and instance identity.
- Recommended production shape: one NATS account per Cloud environment and one
  user credential per app/workload, using JWT/NKey `.creds` plus TLS.
- Do not create a NATS user for every Cloud end user.
- Use a separate privileged diagnostics/system credential for the NATS admin
  surface. Normal app credentials must not access system subjects.
- Development may use simpler local authentication.

### Cloud consumer migration

- Inventory every `@k2b/sync` and `@k2b/sync/browser` consumer.
- Migrate top-level primitive construction to the chosen ready app-scoped Sync
  lifecycle.
- Replace Sync rate-limit consumers with Cloud-owned Redis logic.
- Preserve external APIs and end-user behavior; internal schemas and APIs may
  break.
- Inspect PostgreSQL rows containing Sync resource IDs, cursors, schedules,
  workflow state, or runtime configuration and migrate/reset them deliberately.
- Do not claim compatibility from compile success alone; exercise real durable
  workflows.

### Cloud cutover and data safety

- Schedule maintenance/downtime; downtime is acceptable.
- Stop producers and drain or explicitly disposition Redis-backed accepted
  work before deploying v6.
- Preserve application-owned durable data such as notebook content/snapshots,
  workflow definitions, and audit data.
- Old Redis queue/topic/job/scheduler state is not automatically readable by
  v6. Either finish it, export the small set that matters, or explicitly accept
  its loss after checking inactivity.
- Back up PostgreSQL and relevant Redis/NATS state before cutover and document
  the rollback boundary.
- Perform complete Cloud restart and recovery tests before declaring success.

### Cloud NATS administration

- Add a NATS admin page alongside existing Redis and PostgreSQL pages.
- Show cluster/node/JetStream health, streams, consumers, lag, pending and
  redelivery counts, DLQs, KV buckets, schedules, and Sync resource ownership.
- Provide safe inspection first. Destructive purge/delete/replay actions require
  separate explicit design and authorization.
- Add operational metrics, alerting, capacity views, retention/storage usage,
  and restart/degraded-node visibility.

### End-user acceptance invariant

After the Cloud meta-epic is complete, the following must still just work:

- AI durable event streams and task recovery;
- notebook collaboration, replay, presence, snapshots, and reindexing;
- workflows and record events;
- mail jobs, commands, schedules, and notifications;
- independent durable topic cursors and load-balanced consumer groups;
- jobs, pumps, and schedules after a complete Cloud restart;
- temporary NATS disconnect and loss of one cluster node;
- Redis-backed sessions, general Cloud KV, and rate limiting.

No 1:1 internal behavior is required, but accepted important work must not be
silently lost and ordinary end users must not need to understand the migration.

## Suggested implementation tasks

1. Freeze v6 public contracts and NATS resource mapping.
2. Build the shared NATS runtime, naming, lifecycle, and diagnostics foundation.
3. Implement topic, queue, and ephemeral on JetStream/KV.
4. Implement mutex and compose job, pump, and scheduler recovery semantics.
5. Remove Redis, rate limiting, and browser-parity commitments; update package,
   tests, docs, skill, and migration guidance.
6. Run cluster fault verification, Bun packed-consumer validation, and produce
   the Cloud handoff checklist.

## Done when

- All Sync-owned implementation and verification gates above pass.
- The Sync repository contains no Redis server runtime or compatibility path.
- The Cloud meta-epic is documented and handed off without modifying Cloud.
- Sync v6 is not described as production-ready for Cloud until the separate
  Cloud migration and end-user acceptance suite are complete.
