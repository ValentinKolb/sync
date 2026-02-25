# @valentinkolb/sync

Distributed synchronization primitives for Bun and TypeScript, backed by Redis.

## Philosophy

- **Bun-native** - Built for [Bun](https://bun.sh). Uses `Bun.redis`, `Bun.sleep`, `RedisClient` directly. No Node.js compatibility layers.
- **Minimal dependencies** - Only `zod` as a peer dependency. Everything else is Bun built-ins and Redis Lua scripts.
- **Composable building blocks** - Five focused primitives that work independently or together. `job` composes `queue` + `topic` internally.
- **Consistent API** - Every module follows the same pattern: `moduleName({ id, ...config })` returns an instance. No classes, no `.create()`, no `new`.
- **Atomic by default** - All Redis operations use Lua scripts for atomicity. No multi-step race conditions at the Redis level.
- **Schema-validated** - Queue, topic, and job payloads are validated with Zod at the boundary. Invalid data never enters Redis.

## Features

- **Rate limiting** - Sliding window algorithm with atomic Lua scripts
- **Distributed mutex** - SET NX-based locking with retry, extend, and auto-expiry
- **Queue** - Durable work queue with leases, DLQ, delayed messages, and idempotency
- **Topic** - Pub/sub with consumer groups, at-least-once delivery, and live streaming
- **Job** - Durable job processing built on queue + topic with retries, cancellation, and event sourcing

For a complete API reference (types, config options, Redis key patterns, internals), see [`llms.txt`](./llms.txt).

## Installation

```bash
bun add @valentinkolb/sync zod
```

Requires [Bun](https://bun.sh) and a Redis-compatible server (Redis 6.2+, Valkey, Dragonfly).

## Rate Limit

Sliding window rate limiter. Atomic via Lua script.

```ts
import { ratelimit, RateLimitError } from "@valentinkolb/sync";

const limiter = ratelimit({
  id: "api",
  limit: 100,
  windowSecs: 60,
});

const result = await limiter.check("user:123");
// { limited: false, remaining: 99, resetIn: 58432 }

try {
  await limiter.checkOrThrow("user:123");
} catch (error) {
  if (error instanceof RateLimitError) {
    console.log(`Retry in ${error.resetIn}ms`);
  }
}
```

## Mutex

Distributed lock with retry + jitter, TTL auto-expiry, and Lua-based owner-only release.

```ts
import { mutex, LockError } from "@valentinkolb/sync";

const m = mutex({ id: "checkout", defaultTtl: 5000 });

// Automatic acquire + release
const result = await m.withLock("order:123", async (lock) => {
  await m.extend(lock, 10_000); // extend if needed
  return await processOrder();
});

// Throws LockError if lock cannot be acquired
await m.withLockOrThrow("order:123", async () => {
  await doExclusiveWork();
});

// Manual acquire/release
const lock = await m.acquire("order:123");
if (lock) {
  try { /* work */ } finally { await m.release(lock); }
}
```

## Queue

Durable work queue with at-least-once delivery, lease-based visibility, delayed messages, idempotency, and dead-letter queue.

```ts
import { z } from "zod";
import { queue } from "@valentinkolb/sync";

const q = queue({
  id: "mail.send",
  schema: z.object({ to: z.string().email(), subject: z.string() }),
  delivery: { defaultLeaseMs: 60_000, maxDeliveries: 5 },
  limits: { maxMessageAgeMs: 7 * 24 * 60 * 60 * 1000 },
});

// Send
await q.send({
  data: { to: "user@example.com", subject: "Welcome" },
  idempotencyKey: "welcome:user@example.com",
  delayMs: 5_000,               // optional: deliver after 5s
  meta: { traceId: "abc-123" }, // optional metadata
});

// Receive + process
const msg = await q.recv({ wait: true, timeoutMs: 30_000 });
if (msg) {
  try {
    await sendMail(msg.data);
    await msg.ack();
  } catch (error) {
    await msg.nack({ delayMs: 5_000, error: String(error) });
  }
}

// Stream processing
for await (const m of q.stream()) {
  await handle(m.data);
  await m.ack();
}

// Multiple readers (independent blocking clients)
const reader = q.reader();
const msg2 = await reader.recv({ signal: abortController.signal });
```

### Queue features

- **Lease-based delivery**: Messages are invisible to other consumers while leased. Call `msg.touch()` to extend.
- **Dead-letter queue**: After `maxDeliveries` failed attempts, messages move to DLQ.
- **Delayed messages**: `send({ delayMs })` or `nack({ delayMs })` for retry delays.
- **Idempotency**: `send({ idempotencyKey })` deduplicates within a configurable TTL.
- **Multi-tenant**: Pass `tenantId` to `send()` and `recv()` for isolated queues.
- **AbortSignal**: Pass `signal` to `recv()` for graceful shutdown.

## Topic

Pub/sub with Redis Streams. Supports consumer groups (at-least-once, load-balanced) and live streaming (best-effort, all events).

```ts
import { z } from "zod";
import { topic } from "@valentinkolb/sync";

const t = topic({
  id: "order.events",
  schema: z.object({ type: z.string(), orderId: z.string() }),
  retentionMs: 7 * 24 * 60 * 60 * 1000,
});

// Publish
await t.pub({
  data: { type: "order.confirmed", orderId: "o1" },
  idempotencyKey: "confirm:o1",
  meta: { source: "checkout" },
});

// Consumer group reader (at-least-once, load-balanced across consumers)
const reader = t.reader("mailer");
for await (const event of reader.stream()) {
  await sendConfirmationEmail(event.data);
  await event.commit();
}

// Multiple groups receive the same events independently
const analytics = t.reader("analytics");
const billing = t.reader("billing");

// Live stream (best-effort, no consumer group, no commit needed)
for await (const event of t.live({ signal: ac.signal })) {
  console.log(event.data);
}

// Replay from a specific cursor
for await (const event of t.live({ after: "0-0" })) {
  // receives all stored events from the beginning
}
```

### Topic features

- **Consumer groups**: Each group tracks its own position. Multiple consumers in the same group load-balance.
- **Live streaming**: `t.live()` uses XREAD for real-time, best-effort delivery to all listeners.
- **Replay**: Pass `after: "0-0"` to `live()` to replay all stored events.
- **Retention**: Automatic XTRIM based on `retentionMs`.
- **Multi-tenant**: Pass `tenantId` to `pub()` and `recv()` for isolated streams.
- **AbortSignal**: Pass `signal` to `recv()`, `stream()`, and `live()`.

## Job

Durable job processing built on queue + topic. Supports retries with backoff, cancellation, event sourcing, and graceful shutdown.

```ts
import { z } from "zod";
import { job } from "@valentinkolb/sync";

const sendOrderMail = job({
  id: "mail.send-order",
  schema: z.object({ orderId: z.string(), to: z.string().email() }),
  defaults: { maxAttempts: 3, backoff: { kind: "exp", baseMs: 1000 } },
  process: async ({ ctx, input }) => {
    // ctx.signal is aborted on timeout or error
    if (ctx.signal.aborted) return;

    await ctx.heartbeat(); // extend lease
    await ctx.step({ id: "send", run: () => mailProvider.send(input) });
    return { ok: true };
  },
});

// Submit
const id = await sendOrderMail.submit({
  input: { orderId: "o1", to: "user@example.com" },
  key: "mail:o1",         // idempotency key
  delayMs: 5_000,         // schedule for later
  maxAttempts: 3,
  backoff: { kind: "exp", baseMs: 1000, maxMs: 30_000 },
});

// Wait for completion
const terminal = await sendOrderMail.join({ id, timeoutMs: 60_000 });
// terminal.status: "completed" | "failed" | "cancelled" | "timed_out"

// Cancel
await sendOrderMail.cancel({ id, reason: "user-request" });

// Event stream
const events = sendOrderMail.events(id);
for await (const e of events.reader("orchestrator").stream({ wait: false })) {
  console.log(e.data.type); // "submitted" | "started" | "heartbeat" | "retry" | "completed" | "failed" | "cancelled"
  await e.commit();
}

// Live events
for await (const e of events.live({ signal: ac.signal })) {
  console.log(e.data.type);
}

// Graceful shutdown
sendOrderMail.stop();
```

### Job features

- **Automatic retries**: Fixed or exponential backoff with configurable max attempts.
- **Lease timeout**: Jobs that exceed `leaseMs` are automatically timed out.
- **Cancellation**: Cancel in-flight or queued jobs. Workers detect cancellation between steps.
- **Event sourcing**: Every state transition emits a typed event to a per-job topic.
- **Idempotent submit**: Pass `key` to deduplicate submissions atomically.
- **AbortSignal**: `ctx.signal` is aborted on timeout, error, or cancellation.
- **Graceful shutdown**: `stop()` signals the worker loop to exit.
- **Per-job state TTL**: Each job's state has its own Redis TTL (7 days default).

## Testing

```bash
bun test --preload ./tests/preload.ts
```

Requires a Redis-compatible server on `localhost:6399` (configured in `tests/preload.ts`).

## Contributing

### Setup

```bash
git clone https://github.com/valentinkolb/sync.git
cd sync
bun install
```

### Running tests

You need a Redis-compatible server on port 6399. The easiest way is Docker/Podman:

```bash
docker run -d --name valkey -p 6399:6379 valkey/valkey:latest
bun test --preload ./tests/preload.ts
```

### Project structure

```
src/
  ratelimit.ts       # Sliding window rate limiter
  mutex.ts           # Distributed lock
  queue.ts           # Durable work queue
  topic.ts           # Pub/sub with consumer groups
  job.ts             # Job processing (composes queue + topic)
  internal/
    job-utils.ts     # Job helper functions (retry, timeout, parsing)
    topic-utils.ts   # Stream entry parsing helpers
tests/
  *.test.ts          # Integration tests (require Redis)
  *-utils.unit.test.ts  # Pure unit tests
  preload.ts         # Sets REDIS_URL for test environment
index.ts             # Public API exports
llms.txt             # Complete API reference for LLMs
```

### Guidelines

- Keep it minimal. No abstractions for one-time operations.
- Every Redis mutation must be in a Lua script for atomicity.
- Validate at boundaries (user input), trust internal data.
- All modules follow the `moduleName({ id, ...config })` factory pattern.
- Tests go in `tests/`. Use `test:q`, `test:t`, etc. as prefix in tests to avoid collisions. Each test file has a `beforeEach` that cleans up its own keys.
- Run `bun test --preload ./tests/preload.ts` before submitting a PR. All tests must pass.

## License

MIT
