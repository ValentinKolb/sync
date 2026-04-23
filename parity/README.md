# Parity

This directory enforces **API parity** between `@valentinkolb/sync` (Redis-backed, server) and `@valentinkolb/sync-browser` (in-memory, browser).

## Type parity

`parity/types.ts` imports the public types from both packages and asserts they are structurally equivalent. If any shared public type diverges, `bunx tsc --noEmit` in this directory fails.

Run manually:
```bash
cd parity && bunx tsc --noEmit
```

Or via the root script:
```bash
bun run typecheck:parity
```

### What's enforced

For every shared public type exported from both packages, `Equal<A, B>` asserts mutual subtyping. This catches:

- missing or renamed fields
- changed field types
- changed function signatures
- changed variance on generics

### What's explicitly additive (not equality)

Some browser configs accept an optional `store?: Store` field that the server does not need (Redis handles persistence). These are flagged with a one-way assignability check — server config must be assignable to browser config, not the reverse. This is *additive*: browser-specific extras don't break the shared contract.

Types that currently have additive fields:
- `MutexConfig`
- `QueueConfig` *(not currently — check code)*
- `TopicConfig`
- `EphemeralConfig` *(not currently — check code)*
- `SchedulerConfig`
- `RateLimitConfig`

## Behavior parity

Behavior parity is enforced implicitly: both packages have parallel test suites covering the same scenarios. See:

- `packages/sync/tests/*.test.ts` (server, requires Valkey on :6399)
- `packages/sync-browser/tests/*.test.ts` (browser, in-memory)

Each shared module has a corresponding test file in both. When adding a new scenario to one side, add the mirror to the other.

### Crash recovery guarantee

The key behavioral guarantee the lib provides:

> If a worker (`job` or `scheduler`) crashes while `process` is running, another worker (on a different node / browser tab scope) picks up the message via queue lease expiry and re-runs `process`. `after` runs on the node that completed the run.

This is covered by `packages/sync/tests/fault-*.test.ts` on the server and matching tests on the browser.
