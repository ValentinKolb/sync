# retry

General-purpose retry wrapper with the same callback pattern as `job` and `scheduler`.

## API

```ts
import { retry, isRetryableTransportError, expBackoff } from "@valentinkolb/sync";

type RetryCtx = {
  attempt: number;          // 1-indexed
};

type RetryAfterCtx<T> = RetryCtx & {
  data?: T;
  error?: Error;
  reschedule(cfg?: { delayMs?: number }): void;
  expBackoff(cfg?: BackoffOptions): number;
};

type RetryConfig<T> = {
  run: (cfg: { ctx: RetryCtx }) => Promise<T> | T;
  after?: (cfg: { ctx: RetryAfterCtx<T> }) => Promise<void> | void;
  signal?: AbortSignal;
};

type BackoffOptions = {
  baseMs?: number;     // default: 100
  maxMs?: number;      // default: 2_000
  jitter?: number;     // 0..1, default: 0.2
};

retry<T>(cfg: RetryConfig<T>): Promise<T>
expBackoff(attempt: number, cfg?: BackoffOptions): number
isRetryableTransportError(error: unknown): boolean
```

## Usage

### Simple retry on transport errors

```ts
const user = await retry({
  run: () => fetchUser(id),
  after: ({ ctx }) => {
    if (ctx.error && isRetryableTransportError(ctx.error) && ctx.attempt < 5) {
      ctx.reschedule({ delayMs: ctx.expBackoff() });
    }
  },
});
```

### Fixed-count retry

```ts
await retry({
  run: () => callApi(),
  after: ({ ctx }) => {
    if (ctx.error && ctx.attempt < 3) ctx.reschedule({ delayMs: 1000 });
  },
});
```

### Retry with abort signal

```ts
const ac = new AbortController();
setTimeout(() => ac.abort(), 30_000);

const result = await retry({
  run: () => longRunningCall(),
  after: ({ ctx }) => {
    if (ctx.error) ctx.reschedule({ delayMs: ctx.expBackoff({ baseMs: 500 }) });
  },
  signal: ac.signal,
});
```

### Polling until condition

```ts
const { status } = await retry<{ status: "pending" | "done" }>({
  run: () => checkJobStatus(id),
  after: ({ ctx }) => {
    if (ctx.data?.status === "pending") ctx.reschedule({ delayMs: 1000 });
  },
});
```

## Semantics

- **No `after` defined** → first error throws immediately (fail-fast).
- **`after` without `ctx.reschedule()`** → terminal. Throws the error (if any) or returns the value.
- **`after` with `ctx.reschedule({ delayMs })`** → sleeps delayMs (respecting signal), then re-invokes `run` with `ctx.attempt + 1`.
- **`after` errors are swallowed** — decide via `ctx.reschedule` instead.
- **`signal.aborted` at any point** → throws AbortError (including during sleep).

## `ctx.expBackoff`

Helper that uses `ctx.attempt` internally. Computes:
```
delayMs = min(maxMs, baseMs * 2^(attempt-1))
       ± (delayMs * jitter)  // random in [-jitter, +jitter] of value
```

Equivalent to calling the free function `expBackoff(ctx.attempt, cfg)`.

## `isRetryableTransportError`

Returns true for common transport/connection errors (useful default for `after` predicates):

- Error codes: `ECONNRESET`, `ETIMEDOUT`, `ECONNREFUSED`, `ENOTFOUND`, `EPIPE`, `EHOSTUNREACH`, `ECONNABORTED`
- Message includes: `"econnreset"`, `"etimedout"`, `"connection"`, `"socket"`, `"broken pipe"`, `"network"`, `"loading"`, `"tryagain"`, `"clusterdown"`

## Gotchas

- Use the config-object form: `retry({ run, after, signal })`. The v4 `retry(fn, opts)` form is gone.
- `ctx.expBackoff()` uses `attempt` internally. From inside `after`, `ctx.attempt` is the attempt that just failed/returned (1 for first run, 2 for second, etc.).
- Pair with `isRetryableTransportError` for transport-layer robustness: `if (ctx.error && isRetryableTransportError(ctx.error))`.
- `delayMs: 0` in `reschedule` still yields the event loop — use for immediate re-runs.
