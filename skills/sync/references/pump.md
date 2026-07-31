# Pump

Use `pump()` to incrementally drain a finite cursor-based source into a durable,
idempotent sink. Typical uses are backfills, imports, reindexing,
reconciliation, and paginated external APIs.

## Core API

```ts
import { pump } from "@k2b/sync";

const records = pump<
  { accountId: string },
  { createdAt: number; id: string },
  { key: string; recordId: string }
>({
  id: "records.backfill",

  pull: async ({ input, cursor, limit, signal }) => {
    const rows = await loadRecords({
      accountId: input.accountId,
      after: cursor,
      limit,
      signal,
    });
    return {
      items: rows.map((row) => ({ key: row.id, recordId: row.id })),
      nextCursor: rows.length === limit
        ? { createdAt: rows.at(-1)!.createdAt, id: rows.at(-1)!.id }
        : null,
    };
  },

  dispatch: async ({ item }) => {
    await acceptItemIdempotently(item);
  },
});

await records.start({ key: "account:a1:v3", input: { accountId: "a1" } });
const state = await records.get({ key: "account:a1:v3" });
await records.cancel({ key: "account:a1:v3" });
records.stop();
```

Only `id`, `pull`, and `dispatch` are required. Optional configuration:

```ts
{
  batchSize: 100,
  delayMs: 0,
  defaults: {
    leaseMs: 30_000,
    heartbeatMs: 10_000,
    terminalRetentionMs: 7 * 24 * 60 * 60 * 1000,
  },
  retry: {
    maxAttempts: 10,
    baseMs: 1_000,
    maxMs: 60_000,
    jitter: 0.2,
  },
  limits: {
    pageBytes: 128 * 1024,
  },
  trace: async (event) => observe(event),
}
```

All options shown above are their defaults. Heartbeats are automatic; callback
code only receives an `AbortSignal`.

`limits.pageBytes` applies to the UTF-8 JSON size of `{ items, nextCursor }`.
The internal dispatch checkpoint is persisted separately and does not count
toward this source-page limit.

Public progress and trace types:

```ts
type PumpStatus =
  | "queued" | "running" | "waiting"
  | "completed" | "failed" | "canceled";

type PumpState<Input, Cursor> = {
  key: string;
  input: Input;
  cursor: Cursor | null;
  state: PumpStatus;
  dispatched: number;
  failureCount: number;
  lastError?: string;
  nextRunAt?: number;
  meta?: Record<string, unknown>;
  createdAt: number;
  updatedAt: number;
};

type PumpTraceEvent<Input, Cursor> =
  | { type: "submitted"; key: string; input: Input; meta?: Record<string, unknown> }
  | { type: "started"; key: string; cursor: Cursor | null; failureCount: number }
  | { type: "pulled"; key: string; itemCount: number; durationMs: number }
  | { type: "dispatched"; key: string; itemKey: string; dispatched: number; durationMs: number }
  | { type: "rescheduled"; key: string; failureCount: number; delayMs: number; error: Error }
  | { type: "finished"; key: string; status: "completed" | "failed" | "canceled"; dispatched: number; durationMs: number; error?: Error };
```

Trace callbacks are awaited to preserve event order, but trace errors are logged
and swallowed. Observability cannot change pump state or delivery decisions.

## Delivery and recovery

The server stores `input`, the committed cursor, and the complete active page
in Redis/Valkey/Dragonfly before the first dispatch. It checkpoints
`nextIndex` after each successful dispatch. Another node with the same pump
definition can therefore continue a crashed run without calling `pull()` again.

The dispatch and Redis checkpoint cannot be one transaction. A crash after the
sink accepted an item but before `nextIndex` was persisted repeats that item.
This is at-least-once delivery, so `item.key` must be stable and the sink must
deduplicate it. The cursor advances only after all page items were accepted.

`input`, cursor values, items, and `meta` must contain only JSON values.
Functions, `Date`, `BigInt`, `Error`, typed arrays, class instances, circular
references, `undefined`, and non-finite numbers are rejected.

`cancel({ key })` durably marks the run canceled and prevents later pages from
being dispatched. `stop()` only stops this local worker; it does not cancel a
durable run, and another live handle or node may resume it. Automatic lease
heartbeats protect long `pull`/`dispatch` callbacks. Lease loss or a failed
heartbeat aborts the local attempt so another worker can recover it under the
same at-least-once guarantee.

## Server namespace upgrades

Server pumps use an encoded `(prefix, id)` identity. Versions `<=5.8.0` used an
ambiguous colon-concatenated namespace. Stop every old worker and finish,
export, or remove legacy runs before upgrading. If an exact legacy run remains,
`start()`, `get()`, and `cancel()` fail with a `namespace migration required`
error and leave it untouched; new workers never poll the legacy due set.

## Queue sink

```ts
const reindexQueue = queue<{ recordId: string }>({ id: "search.reindex" });

dispatch: async ({ item }) => {
  await reindexQueue.send({
    data: { recordId: item.recordId },
    idempotencyKey: item.key,
  });
}
```

The pump waits for queue acceptance, not downstream indexing.

## Job sink

```ts
const importRecord = job<{ recordId: string }>({
  id: "records.import-one",
  process: async ({ ctx }) => importOne(ctx.input.recordId),
});

dispatch: async ({ item }) => {
  await importRecord.submit({
    key: item.key,
    input: { recordId: item.recordId },
  });
}
```

Each submitted job owns its own retry lifecycle.

## Workflow-event sink

```ts
dispatch: async ({ input, item, signal }) => {
  await emitWorkflowEvent({
    scopeId: input.mailboxId,
    targetWorkflowId: input.workflowId,
    type: "mail.messageReceived",
    dedupeKey: item.key,
    data: { messageId: item.messageId },
    signal,
  });
}
```

One failed workflow run does not stop the pump after the event was durably
accepted.

## Reindexing

```ts
const searchReindex = pump<
  { index: string },
  { updatedAt: number; id: string },
  { key: string; document: SearchDocument }
>({
  id: "search.reindex",
  pull: ({ input, cursor, limit, signal }) =>
    loadSearchDocuments({ index: input.index, after: cursor, limit, signal }),
  dispatch: async ({ input, item }) => {
    await search.upsert({
      index: input.index,
      id: item.key,
      document: item.document,
    });
  },
});
```

The search upsert is naturally idempotent by document id.

## External API pagination

```ts
const externalImport = pump<
  { connectionId: string },
  string,
  { key: string; externalId: string }
>({
  id: "crm.import",
  delayMs: 500,
  pull: async ({ input, cursor, limit, signal }) => {
    const page = await crm.listContacts({
      connectionId: input.connectionId,
      pageToken: cursor,
      pageSize: limit,
      signal,
    });
    return {
      items: page.contacts.map((contact) => ({
        key: contact.id,
        externalId: contact.id,
      })),
      nextCursor: page.nextPageToken ?? null,
    };
  },
  dispatch: ({ item }) =>
    contactJobs.submit({
      key: `crm:${item.key}`,
      input: { externalId: item.externalId },
    }),
});
```

`delayMs` paces successful pages. Provider request budgets still belong in the
API client or a shared rate limiter; pump pacing is not a rate limiter.

A page that returns the current non-terminal cursor has made no durable progress.
Pump retries it with the configured retry backoff and marks the run failed after
`maxAttempts`. Return `nextCursor: null` when the source is exhausted.

## Browser

Import from `@k2b/sync/browser`. State is in a process-wide memory store by
default, so handles with the same `id` share runs. Pass the same explicit store
to coordinate a separate scope, or `store: createLocalStorageStore()` to resume
after reloads. Multi-tab ownership is best-effort because browser storage has
no Redis-style atomic fencing.

Persisted runs from the old concatenated browser namespace are not guessed or
silently restarted. Close old tabs and call
`migrateLegacyPumpState({ id, prefix, key, store })` once per run before
constructing the new pump. The explicit identity and run key are required
because an old key cannot prove how its prefix and id were split. If two old
identities collided, the operator must choose the intended owner; never migrate
the same old run into both new identities.

Browser pump persistence uses an encoded `(prefix, id)` identity. State written
under the old concatenated key is not auto-imported because colliding old keys
cannot prove which pump owned the run.
