# API

## Factory

```ts
import { z } from "zod";
import { job } from "@valentinkolb/sync";

const sendOrderMail = job({
  id: "mail.send-order",
  schema: z.object({ orderId: z.string(), to: z.string().email() }),
  defaults: {
    maxAttempts: 3,
    backoff: { kind: "exp", baseMs: 1000, maxMs: 30_000 },
    leaseMs: 30_000,
    keyTtlMs: 7 * 24 * 60 * 60 * 1000,
  },
  process: async ({ ctx, input }) => {
    if (ctx.signal.aborted) return;
    await ctx.step({ id: "send", run: () => provider.send(input) });
    await ctx.heartbeat();
    return { ok: true };
  },
});
```

## Types

```ts
type SubmitOptions = {
  key?: string;
  keyTtlMs?: number;
  delayMs?: number;
  at?: number;
  maxAttempts?: number;
  backoff?: { kind: "fixed" | "exp"; baseMs: number; maxMs?: number };
  leaseMs?: number;
  meta?: Record<string, unknown>;
};

type JoinOptions = { timeoutMs?: number };

type CancelOptions = { reason?: string };

type JobStatus = "completed" | "failed" | "cancelled" | "timed_out";

type JobTerminal<Result = unknown> = {
  id: string;
  status: JobStatus;
  result?: Result;
  error?: { message: string; code?: string };
  finishedAt: number;
};

type JobContext = {
  step<T>(cfg: { id: string; run: () => Promise<T> | T }): Promise<T>;
  heartbeat(cfg?: { leaseMs?: number }): Promise<void>;
  signal: AbortSignal;
};

type JobEvent =
  | { type: "submitted"; id: string; ts: number }
  | { type: "started"; id: string; runId: string; attempt: number; ts: number }
  | { type: "heartbeat"; id: string; runId: string; ts: number }
  | { type: "retry"; id: string; runId: string; nextAt: number; reason?: string; ts: number }
  | { type: "completed"; id: string; ts: number }
  | { type: "failed"; id: string; reason?: string; ts: number }
  | { type: "cancelled"; id: string; reason?: string; ts: number };

type JobHandle<Input, Result = unknown> = {
  id: string;
  submit(cfg: { input: Input } & SubmitOptions): Promise<string>;
  validateInput(input: unknown): void;
  join(cfg: { id: string } & JoinOptions): Promise<JobTerminal<Result>>;
  cancel(cfg: { id: string } & CancelOptions): Promise<void>;
  events(id: string): {
    reader: (group?: string) => import("@valentinkolb/sync").TopicReader<JobEvent>;
    live: (cfg?: import("@valentinkolb/sync").TopicLiveConfig) => AsyncIterable<import("@valentinkolb/sync").TopicLiveEvent<JobEvent>>;
  };
  stop(): void;
};
```

## Config and Defaults

- `id`: required job namespace.
- `schema`: required input schema.
- `defaults.maxAttempts`: default `1`.
- `defaults.leaseMs`: default `30000`.
- `defaults.backoff`: optional.
- `defaults.keyTtlMs`: default effectively `7d` (`DEFAULT_STATE_RETENTION_MS`) when submit key used.
- state TTL: 7 days.
- worker recv timeout: 1000ms polling block.

## Submit Semantics

- `submit()` auto-starts worker loop for this job definition in current process.
- `at` overrides `delayMs` (`delay = max(0, at - now)`).
- `key` deduplicates via atomic Lua script against idempotency key.
- Existing idempotent key returns same job id.
- If key existed but state missing, implementation recovers by re-enqueue + write missing state.

## Execution Semantics

- Worker reads from internal queue (`sync:job:queue:...`).
- Worker receive loop auto-retries transient transport errors and keeps running after short Redis outages.
- Before process, state moves to `running` and `started` event emitted.
- `ctx.heartbeat()` extends queue lease and emits heartbeat event.
- On handler error/timeout and attempts left: message nacked with computed delay and `retry` event emitted.
- On terminal failure: state set to `failed` or `timed_out`.
- On cancellation race: final writes are CAS-guarded and cancelled wins.

## join/cancel

- `join()` first checks state key, then streams events from cursor `0-0`, then final re-check.
- On join timeout, returns `{ status: "timed_out", error.code: "JOIN_TIMEOUT" }`.
- `cancel()` writes cancelled state only if current state is non-terminal.

## Usage Patterns

### Submit + join

```ts
const id = await sendOrderMail.submit({
  input: { orderId: "o1", to: "user@example.com" },
  key: "mail:o1",
  maxAttempts: 3,
  backoff: { kind: "exp", baseMs: 500, maxMs: 10_000 },
});

const terminal = await sendOrderMail.join({ id, timeoutMs: 60_000 });
```

### Cancel

```ts
await sendOrderMail.cancel({ id, reason: "user-request" });
```

### Events

```ts
for await (const ev of sendOrderMail.events(id).reader("orchestrator").stream()) {
  await persistAudit(ev.data);
  await ev.commit();
}
```

## Redis Keys

- Sequence: `sync:job:{id}:seq`
- State: `sync:job:{id}:state:{jobId}`
- Idempotency: `sync:job:{id}:idempotency:{key}`
- Work queue namespace: `sync:job:queue:default:{id}:work:*`
- Event stream namespace: `sync:job:events:default:{id}:{jobId}:events:stream`
