# @valentinkolb/sync

Distributed synchronization primitives for Bun and TypeScript. Built on Redis for horizontal scaling.

**Minimal dependencies** - uses only Bun's native Redis client (`Bun.redis`) and Zod for schema validation.

## Features

- **Rate Limiting** - Sliding window algorithm for smooth rate limiting
- **Distributed Mutex** - Acquire locks across multiple processes with automatic expiry
- **Background Jobs** - Delayed, scheduled, and periodic jobs with retries
- **Horizontal Scaling** - All state in Redis, run multiple instances safely
- **Type Safe** - Full TypeScript support with Zod schema validation for jobs

## Installation

```bash
bun add @valentinkolb/sync zod
```

## Quick Start

### Rate Limiting

```typescript
import { ratelimit } from "@valentinkolb/sync";

const limiter = ratelimit.create({
  limit: 100,      // 100 requests
  windowSecs: 60,  // per minute
});

const result = await limiter.check("user:123");
if (result.limited) {
  console.log(`Rate limited. Retry in ${result.resetIn}ms`);
}

// Or throw on limit
await limiter.checkOrThrow("user:123");
```

### Distributed Mutex

```typescript
import { mutex } from "@valentinkolb/sync";

const m = mutex.create();

// Automatic acquire/release
const result = await m.withLock("resource:123", async () => {
  // Only one process can execute this at a time
  return await doExclusiveWork();
});

// Or throw if lock cannot be acquired
const result = await m.withLockOrThrow("resource:123", async () => {
  return await doExclusiveWork();
});
```

### Background Jobs

```typescript
import { jobs } from "@valentinkolb/sync";
import { z } from "zod";

// Create a typed job queue
const emailJobs = jobs.create({
  name: "emails",
  schema: z.object({
    to: z.string().email(),
    subject: z.string(),
    body: z.string(),
  }),
});

// Send jobs
await emailJobs.send({ to: "user@example.com", subject: "Hello", body: "World" });

// Process jobs
const stop = emailJobs.process(async (job) => {
  await sendEmail(job.data);
}, { concurrency: 10 });
```

## Rate Limiting

Uses a sliding window algorithm that provides smoother rate limiting than fixed windows.

```typescript
import { ratelimit, RateLimitError } from "@valentinkolb/sync";

const limiter = ratelimit.create({
  limit: 100,        // Max requests in window
  windowSecs: 60,    // Window size in seconds (default: 1)
  prefix: "rl",      // Redis key prefix (default: "ratelimit")
});

// Check without throwing
const result = await limiter.check("user:123");
// { limited: boolean, remaining: number, resetIn: number }

// Check and throw RateLimitError if limited
try {
  await limiter.checkOrThrow("user:123");
} catch (e) {
  if (e instanceof RateLimitError) {
    console.log(`Retry in ${e.resetIn}ms`);
  }
}
```

## Distributed Mutex

Uses Redis SET NX for atomic lock acquisition with automatic expiry to prevent deadlocks.

```typescript
import { mutex, LockError } from "@valentinkolb/sync";

const m = mutex.create({
  prefix: "lock",       // Redis key prefix (default: "mutex")
  retryCount: 10,       // Retry attempts (default: 10)
  retryDelay: 200,      // Delay between retries in ms (default: 200)
  defaultTtl: 10000,    // Lock TTL in ms (default: 10000)
});

// Automatic release with withLock
const result = await m.withLock("resource", async (lock) => {
  // Extend lock if operation takes longer than expected
  await m.extend(lock, 30000);
  return await longOperation();
});

// Returns null if lock cannot be acquired
if (result === null) {
  console.log("Could not acquire lock");
}

// Or throw LockError
try {
  await m.withLockOrThrow("resource", async () => {
    return await doWork();
  });
} catch (e) {
  if (e instanceof LockError) {
    console.log(`Failed to lock: ${e.resource}`);
  }
}

// Manual acquire/release
const lock = await m.acquire("resource");
if (lock) {
  try {
    await doWork();
  } finally {
    await m.release(lock);
  }
}
```

## Background Jobs

A Redis-backed job queue with support for delayed jobs, periodic scheduling, retries, and timeouts.

### Creating a Queue

```typescript
import { jobs } from "@valentinkolb/sync";
import { z } from "zod";

const emailJobs = jobs.create({
  name: "emails",                        // Queue name
  schema: z.object({                     // Zod schema for validation
    to: z.string().email(),
    subject: z.string(),
    body: z.string(),
  }),
  prefix: "jobs",                        // Redis key prefix (default: "jobs")
});
```

### Sending Jobs

```typescript
// Immediate execution
await emailJobs.send({ to: "...", subject: "...", body: "..." });

// Delayed execution
await emailJobs.send(data, { delay: 5000 });        // In 5 seconds
await emailJobs.send(data, { at: Date.now() + 60000 }); // At specific time

// Periodic jobs
await emailJobs.send(data, { interval: 3600000 });  // Every hour
await emailJobs.send(data, { interval: 3600000, startImmediately: true });

// With retries and timeout
await emailJobs.send(data, {
  retries: 3,       // 1 initial + 3 retries = 4 attempts
  timeout: 60000,   // Fail if processing takes > 60s
});
```

| Option | Description | Default |
|--------|-------------|---------|
| `delay` | Delay execution by ms | - |
| `at` | Execute at timestamp | - |
| `interval` | Repeat every ms | - |
| `startImmediately` | Start interval job immediately | `false` |
| `retries` | Number of retries on failure | `0` |
| `timeout` | Max processing time before hard fail | `30000` |

> **Note:** `delay`, `at`, and `interval` are mutually exclusive.

### Processing Jobs

```typescript
const stop = emailJobs.process(async (job) => {
  await sendEmail(job.data);
}, { concurrency: 10, pollInterval: 1000 });

stop();
```

Processing uses blocking reads by default. `pollInterval` now controls the maintenance loop (delayed/timeout checks) and serves as a fallback for the blocking timeout if `blockingTimeoutSecs` is not set.

| Process Option | Description | Default |
|---------------|-------------|---------|
| `concurrency` | Number of concurrent workers | `1` |
| `blockingTimeoutSecs` | Blocking read timeout in seconds | derived from `pollInterval` |
| `pollInterval` | Maintenance interval (ms) and blocking timeout fallback | `1000` |
| `maintenanceIntervalMs` | Maintenance loop interval override (ms) | `pollInterval` |

### Multiple Queues (Agents)

Each queue creates its own Redis keys. You can safely run many queues side by side.

```typescript
import { jobs } from "@valentinkolb/sync";
import { z } from "zod";

const schema = z.object({ payload: z.string() });

const agents = ["alpha", "beta"].map((name) => jobs.create({ name: `agent:${name}`, schema }));

for (const queue of agents) {
  queue.process(async (job) => {
    await handleAgentJob(job);
  });
}
```

## Queue

### Processing Messages

```typescript
const stop = events.process(async (data) => {
  console.log(data.type, data.payload);
}, { concurrency: 5, pollInterval: 1000 });

stop();
```

Processing uses blocking reads by default. `pollInterval` is the fallback for the blocking timeout when `blockingTimeoutSecs` is not set.

| Queue Option | Description | Default |
|--------------|-------------|---------|
| `concurrency` | Number of concurrent consumers | `1` |
| `blockingTimeoutSecs` | Blocking read timeout in seconds | derived from `pollInterval` |
| `pollInterval` | Blocking timeout fallback (ms) | `1000` |

### Job Object

```typescript
type Job<T> = {
  id: string;
  data: T;
  status: "waiting" | "delayed" | "active" | "completed" | "failed";
  attempts: number;
  maxRetries: number;
  timeout: number;
  interval?: number;
  createdAt: number;
  scheduledAt?: number;
  startedAt?: number;
  completedAt?: number;
  error?: string;
};
```

### Horizontal Scaling

Jobs are safe to process across multiple instances. Each job is claimed atomically using Redis RPOPLPUSH/BRPOPLPUSH, ensuring exactly-once processing.

```
                    ┌─────────────┐
                    │   Redis     │
                    │  (waiting)  │
                    └──────┬──────┘
                           │
         ┌─────────────────┼─────────────────┐
         │                 │                 │
         ▼                 ▼                 ▼
    ┌─────────┐       ┌─────────┐       ┌─────────┐
    │ Worker  │       │ Worker  │       │ Worker  │
    │ (Pod 1) │       │ (Pod 2) │       │ (Pod 3) │
    └─────────┘       └─────────┘       └─────────┘
```

## Configuration

The library reads Redis connection from environment variables:

| Variable | Default |
|----------|---------|
| `REDIS_URL` | `redis://localhost:6379` |
| `VALKEY_URL` | (fallback) |

## License

MIT
