# API (Current)

This file is the canonical API summary for `queue`, `topic`, and `job`.
The older spec/draft plan files were removed.

## Queue

- `const q = queue(config)`
- `await q.send({ data, ...opts }) -> { messageId }`
- `await q.recv({ wait, timeoutMs, leaseMs, signal, ... }) -> QueueReceived | null`
- `for await (const m of q.stream({ ... })) { ... }`
- `const r = q.reader()` for read-only access (`recv`, `stream`)

`QueueReceived`:

- `data`, `messageId`, `deliveryId`, `attempt`, `leaseUntil`, `orderingKey?`, `meta?`
- `await ack()`
- `await nack({ delayMs?, reason?, error? })`
- `await touch({ leaseMs? })`

## Topic

- `const t = topic(config)`
- `await t.pub({ data, ...opts }) -> { eventId, cursor }`
- `const r = t.reader(group?)` (default group: `"default"`)
- `await r.recv({ wait, timeoutMs, signal, ... }) -> TopicDelivery | null`
- `for await (const m of r.stream({ ... })) { ... }` (durable group stream)
- `for await (const e of t.live({ after?, signal?, timeoutMs? })) { ... }` (ephemeral live)

`TopicDelivery`:

- `data`, `eventId`, `deliveryId`, `cursor`, `orderingKey?`, `publishedAt`, `meta?`
- `await commit()`

## Job

- `const j = job({ id, schema, process, defaults? })`
- `await j.submit({ input, ...opts }) -> jobId`
- `await j.join({ id, timeoutMs? }) -> { id, status, ... }`
- `await j.cancel({ id, reason? })`
- `const ev = j.events(jobId)` gives topic-style read APIs:
- `const r = ev.reader(group?)`
- `await r.recv(...)` / `r.stream(...)`
- `ev.live(...)`
