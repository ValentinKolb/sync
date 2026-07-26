# job

Durable background tasks with callback-based lifecycle. Same API on server and browser.

## Factory

```ts
import { job, isRetryableTransportError } from "@k2b/sync";

// Simple — no input, no typed result
const sync = job({
  id: "sync-ipa",
  process: async ({ ctx }) => { await doSync(); },
});

// Typed input + result
const sendMail = job<{ userId: string }, { sent: boolean }>({
  id: "send-mail",
  defaults: { leaseMs: 30_000, keyTtlMs: 24 * 60 * 60 * 1000 },
  trace: async (event) => {
    await cloudTrace({ source: "send-mail", event });
  },
  process: async ({ ctx }) => {
    // ctx.input: { userId: string }
    return { sent: true };
  },
  after: async ({ ctx }) => {
    // ctx.data?: { sent: boolean } — if process returned
    // ctx.error?: Error              — if process threw
    if (ctx.error && ctx.failureCount < 3) {
      ctx.reschedule({ delayMs: ctx.expBackoff({ baseMs: 1000 }) });
    }
  },
});
```

## Types

```ts
type JobCtx<Input = void> = {
  jobId: string;
  key: string;                    // idempotency key from submit
  input: Input;                   // from submit, undefined if Input = void
  failureCount: number;           // prior failures of THIS jobId (0 on first run)
  readonly duration: number;      // ms since process started
  signal: AbortSignal;
  heartbeat(cfg?: { leaseMs?: number }): Promise<void>;
};

type JobAfterCtx<Input, Result> = JobCtx<Input> & {
  data?: Result;                  // set iff process returned
  error?: Error;                  // set iff process threw
  reschedule(cfg?: { delayMs?: number }): void;
  expBackoff(cfg?: BackoffOptions): number;
  metric: JobMetrics;             // live reference
};

type JobMetrics = {
  dispatches: number;             // successful process returns
  failures: number;               // terminal failures (no reschedule)
  reschedules: number;            // ctx.reschedule() calls
};

type TraceHandler<Event> = (event: Event) => void | Promise<void>;

type JobTraceEvent<Input = void, Result = unknown> =
  | { type: "submitted"; jobId: string; key: string; input?: Input; meta?: Record<string, unknown> }
  | { type: "started"; jobId: string; key: string; input?: Input; attempt: number }
  | { type: "succeeded"; jobId: string; key: string; input?: Input; data: Result; durationMs: number }
  | { type: "failed"; jobId: string; key: string; input?: Input; error: Error; durationMs: number }
  | { type: "rescheduled"; jobId: string; key: string; attempt: number; delayMs: number }
  | { type: "finished"; jobId: string; key: string; status: "succeeded" | "failed"; durationMs: number };

type JobConfig<Input = void, Result = unknown> = {
  id: string;
  prefix?: string;
  defaults?: {
    leaseMs?: number;
    keyTtlMs?: number;
  };
  trace?: TraceHandler<JobTraceEvent<Input, Result>>;
  process: (cfg: { ctx: JobCtx<Input> }) => Promise<Result> | Result;
  after?: (cfg: { ctx: JobAfterCtx<Input, Result> }) => Promise<void> | void;
};

type SubmitConfig<Input> = {
  key: string;                    // required; idempotency scope
  keyTtlMs?: number;
  delayMs?: number;               // or `at` absolute timestamp
  at?: number;
  leaseMs?: number;
  meta?: Record<string, unknown>;
} & (Input extends void ? { input?: Input } : { input: Input });

type JobHandle<Input> = {
  id: string;
  submit(cfg: SubmitConfig<Input>): Promise<JobId>;
  metric(): JobMetrics;           // snapshot copy
  stop(): void;
};
```

## Lifecycle

```
submit({ key })
  → claim idempotency key atomically
  → if key already held → return existing jobId (dedupe)
  → enqueue work message
  → trace "submitted" (new enqueue only)
  → auto-start worker loop (first submit only)

worker picks up message
  → trace "started"
  → process({ ctx })
     if process returns: result in ctx.data
     if process throws:  error in ctx.error
     if process returns: trace "succeeded"
     if process throws:  trace "failed"
  → after({ ctx })  (if defined)

if ctx.reschedule called in after:
  → nack message with delayMs
  → trace "rescheduled"
  → key stays claimed
  → eventually re-delivered as attempt = failureCount + 2
else (terminal):
  → ack message
  → DEL idempotency key
  → metric counter updated (dispatches or failures)
  → trace "finished"
```

## Usage patterns

### Typed input

```ts
const processChat = job<{ chatId: string; priority: "high" | "low" }>({
  id: "process-chat",
  process: async ({ ctx }) => {
    // ctx.input.chatId  — typed
    // ctx.input.priority — typed
    await work(ctx.input);
  },
});

await processChat.submit({
  key: `chat:${chatId}`,
  input: { chatId, priority: "high" },
});
```

### Retry with exponential backoff

```ts
after: async ({ ctx }) => {
  if (!ctx.error) return;
  if (ctx.failureCount >= 5) {
    // Give up — write audit row
    await db.jobFailures.insert({ jobId: ctx.jobId, key: ctx.key, error: ctx.error.message });
    return;
  }
  ctx.reschedule({ delayMs: ctx.expBackoff({ baseMs: 1000, maxMs: 5 * 60_000 }) });
}
```

### Polling pattern (reschedule on success)

```ts
process: async ({ ctx }) => {
  const batch = await fetchNextBatch();
  await process(batch);
  return { hasMore: batch.length === MAX_BATCH };
},
after: async ({ ctx }) => {
  if (ctx.data?.hasMore) ctx.reschedule({ delayMs: 0 });  // drain immediately
}
```

### Long-running with heartbeat

```ts
process: async ({ ctx }) => {
  for (const item of items) {
    if (ctx.signal.aborted) return;
    await processItem(item);
    await ctx.heartbeat({ leaseMs: 30_000 });
  }
}
```

### Trace to your own observability sink

```ts
const summarize = job<{ docId: string }, { tokens: number }>({
  id: "summarize",
  trace: async (event) => {
    await cloudTrace({ source: "summarize", event });
  },
  process: async ({ ctx }) => summarizeDoc(ctx.input.docId),
});
```

Trace is a callback, not a storage layer. Use it to log, publish, count, or map events to OpenTelemetry. The library does not redact or serialize `input`, `data`, or `error`; do that in your handler.

## Gotchas

- **`key` is required** — jobs can't be submitted anonymously. This is your idempotency scope.
- **`trace` is observability-only**: trace handler errors are logged with `[sync trace]` and swallowed. They never fail submit, process, ack, nack, or key release.
- **Trace order is deterministic for one job attempt**: handlers are awaited. If you want fire-and-forget behavior, do that inside your trace handler.
- **`submitted` means new enqueue**: duplicate submits that return an existing `jobId` do not emit `submitted`.
- **`succeeded` / `failed` describe the process attempt**: `after` can still call `ctx.reschedule()` after either event.
- **`finished` means terminal**: it fires only after a successful ack and idempotency key release. Rescheduled jobs emit `rescheduled`, then a later attempt emits its own `started` event.
- **Crash recovery**: if the worker dies mid-process, the queue lease expires and another worker (or the same one on restart) receives the message with `attempt++`. `ctx.failureCount` = `attempt - 1`.
- **At-least-once**: `process` might run multiple times if crashes happen between side-effects and `ack`. Make your side effects idempotent or use `ctx.key` to detect re-runs.
- **`after` errors are swallowed**: don't throw inside `after` — decide via `ctx.reschedule` instead.
- **`data` is passed in-memory**: the result of `process` is handed to `after` without JSON round-trip. `Date`, `Map`, class instances all work. Only `input` is marshalled (through the queue).
- **Multiple workers with same `id`**: coordinate automatically via the internal queue. Each message is delivered to one worker.
- **`stop()` halts the receive loop** but lets in-flight process run to completion.

## Redis keys (server)

- `sync:job:{id}:seq` — jobId counter
- `sync:job:{id}:idempotency:{key}` — key → jobId with TTL
- `sync:job:queue:default:{id}:work:*` — internal queue state

**What's NOT in Redis (removed in v5):**
- Per-job state key (`state:{jobId}`) — gone
- Per-job event topic/stream — gone
- DLQ (internal queue uses `maxDeliveries: Number.MAX_SAFE_INTEGER`)

## Browser differences

- State is in-memory Maps (shared across `job()` instances with the same id via module-level `sharedStates`).
- `stop()` still important — prevents worker-loop memory leak.
- Jobs lost on page refresh — not durable.
