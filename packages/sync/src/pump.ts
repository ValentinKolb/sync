import { AckPolicy, DeliverPolicy, DiscardPolicy, RetentionPolicy, StorageType } from "@nats-io/jetstream";
import type { JsMsg } from "@nats-io/jetstream";
import type { KV } from "@nats-io/kv";
import { decodeJson, encodeJson } from "./codec.ts";
import type { JsonValue } from "./codec.ts";
import { runPullLoop } from "./consume.ts";
import { asError } from "./errors.ts";
import { assertName, resourceIdentity, subjectRoot, subjectToken, decodeSubjectToken } from "./naming.ts";
import { ensureConsumer, ensureKv, ensureStream } from "./resources.ts";
import type { ProvisionContext } from "./resources.ts";
import type { SyncRuntime } from "./runtime.ts";
import { DEFAULT_MESSAGE_PAYLOAD_BYTES, nanos } from "./types.ts";
import type { MessageMeta } from "./types.ts";
import { createWorkerRuntime } from "./worker.ts";
import type { ProcessOptions, Worker } from "./worker.ts";

// ==========================
// Types
// ==========================

export type PumpItem = { key: string };

export type PumpConfig<Input, Cursor, Item extends PumpItem> = {
  id: string;
  owner?: string;
  /** Items requested per pull(). Default 100. */
  batchSize?: number;
  /** Simultaneously dispatched items inside each active run. Default 1. */
  dispatchConcurrency?: number;
  /** Global ceiling of concurrently leased runs across all pods. Default 100. */
  maxActiveRuns?: number;
  retention?: {
    /** How long terminal run state stays readable. Default 7 days. */
    terminalMs?: number;
    /** Maximum encoded bytes of one persisted run record incl. its page. Default 128 KiB. */
    maxPageBytes?: number;
  };
  retry?: { maxAttempts?: number; backoffMs?: number[] };
  /** Run lease duration: crash takeover latency vs. duplicate-claim safety. Default 60_000, min 2_000. */
  leaseMs?: number;
  pull(context: {
    input: Input;
    cursor: Cursor | null;
    limit: number;
    signal: AbortSignal;
  }): Promise<{ items: Item[]; nextCursor: Cursor | null }>;
  dispatch(context: { input: Input; item: Item; signal: AbortSignal }): Promise<void>;
};

export type PumpStatus = "queued" | "running" | "waiting" | "completed" | "failed" | "canceled";

export type PumpState<Input, Cursor> = {
  key: string;
  input: Input;
  cursor: Cursor | null;
  status: PumpStatus;
  dispatched: number;
  failureCount: number;
  lastError?: string;
  createdAt: Date;
  updatedAt: Date;
};

export type Pump<Input, Cursor> = {
  ready(): Promise<void>;
  start(input: { key: string; input: Input; meta?: MessageMeta }): Promise<void>;
  process(options?: ProcessOptions): Promise<Worker>;
  get(input: { key: string }): Promise<PumpState<Input, Cursor> | null>;
  cancel(input: { key: string }): Promise<boolean>;
  /** Re-enqueue wake-ups for every non-terminal run. Runs automatically on process(). */
  reconcile(): Promise<{ requeued: number }>;
};

/** The KV record is the truth for a run; wake-up messages are repairable hints. */
type PumpRecord<Input, Cursor, Item extends PumpItem> = {
  key: string;
  input: Input;
  cursor: Cursor | null;
  status: PumpStatus;
  dispatched: number;
  failureCount: number;
  lastError?: string;
  meta?: MessageMeta;
  createdAt: string;
  updatedAt: string;
  lease?: { token: string; until: string };
  page?: { items: Item[]; done: string[]; nextCursor: Cursor | null };
};

const TERMINAL: readonly PumpStatus[] = ["completed", "failed", "canceled"];

// ==========================
// Pump factory
// ==========================

export const createPump = <Input, Cursor, Item extends PumpItem>(
  runtime: SyncRuntime,
  config: PumpConfig<Input, Cursor, Item>,
): Pump<Input, Cursor> => {
  const identity = resourceIdentity(runtime.namespace, "pump", config.id);
  const owner = config.owner ?? runtime.application;
  const batchSize = config.batchSize ?? 100;
  const dispatchConcurrency = config.dispatchConcurrency ?? 1;
  const maxActiveRuns = config.maxActiveRuns ?? 100;
  const maxPageBytes = config.retention?.maxPageBytes ?? DEFAULT_MESSAGE_PAYLOAD_BYTES;
  const terminalMs = config.retention?.terminalMs ?? 7 * 24 * 60 * 60 * 1_000;
  const maxAttempts = config.retry?.maxAttempts ?? 5;
  const backoffMs = config.retry?.backoffMs ?? [1_000, 5_000, 30_000];
  const leaseMs = config.leaseMs ?? 60_000;
  if (!Number.isSafeInteger(leaseMs) || leaseMs < 2_000) {
    throw new RangeError("leaseMs must be an integer of at least 2000");
  }
  for (const [label, value] of [
    ["batchSize", batchSize],
    ["dispatchConcurrency", dispatchConcurrency],
    ["maxActiveRuns", maxActiveRuns],
    ["retry.maxAttempts", maxAttempts],
  ] as const) {
    if (!Number.isSafeInteger(value) || value < 1) throw new RangeError(`${label} must be a positive integer`);
  }

  const replicas = runtime.defaults.replicas;
  const storage = runtime.defaults.storage === "memory" ? StorageType.Memory : StorageType.File;
  const bucket = `S6_P_${identity.hash}`;
  const wakeStream = `S6_PQ_${identity.hash}`;
  const wakeDurable = `S6_PQC_${identity.hash}`;
  const root = subjectRoot(identity);
  const wakeSubject = `${root}.wake`;
  const label = `pump ${config.id}`;

  let kv: KV | null = null;

  const declaration = runtime.declare({
    identity,
    owner,
    configKey: JSON.stringify([
      "pump",
      config.id,
      owner,
      batchSize,
      dispatchConcurrency,
      maxActiveRuns,
      maxPageBytes,
      terminalMs,
      maxAttempts,
      backoffMs,
      leaseMs,
    ]),
    natsNames: [`KV_${bucket}`, wakeStream],
    provision: async (ctx: ProvisionContext) => {
      kv = await ensureKv(ctx, identity, owner, bucket, {
        history: 1,
        replicas,
        storage,
        markerTTL: 1_000,
      });
      await ensureStream(ctx, identity, owner, {
        name: wakeStream,
        subjects: [wakeSubject],
        retention: RetentionPolicy.Workqueue,
        discard: DiscardPolicy.Old,
        storage,
        num_replicas: replicas,
        max_age: nanos(terminalMs),
        max_bytes: 64 * 1024 * 1024,
        max_msgs: -1,
        max_msg_size: -1,
        duplicate_window: nanos(120_000),
      });
    },
    summary: async (ctx: ProvisionContext) => {
      const status = await (kv ?? (await ctx.kvm.open(bucket))).status();
      const wake = await ctx.jsm.streams.info(wakeStream);
      return {
        runs: status.values,
        pendingWakeups: wake.state.messages,
      } satisfies Record<string, JsonValue>;
    },
  });

  const getKv = async (): Promise<KV> => {
    await declaration.ready();
    if (kv === null) {
      const ctx = await runtime.context();
      kv = await ctx.kvm.open(bucket);
    }
    return kv;
  };

  const runKey = (key: string): string => {
    assertName(key, "pump key");
    return `run.${subjectToken(key, "pump key")}`;
  };

  type Loaded = { record: PumpRecord<Input, Cursor, Item>; revision: number } | null;

  const load = async (key: string): Promise<Loaded> => {
    const store = await getKv();
    const entry = await store.get(runKey(key));
    if (entry === null || entry.operation !== "PUT") return null;
    try {
      return { record: decodeJson<PumpRecord<Input, Cursor, Item>>(entry.value), revision: entry.revision };
    } catch {
      return null;
    }
  };

  /** CAS write; returns the new revision or null when the record moved underneath us. */
  const casWrite = async (
    key: string,
    record: PumpRecord<Input, Cursor, Item>,
    previousRevision: number,
  ): Promise<number | null> => {
    const store = await getKv();
    record.updatedAt = new Date().toISOString();
    const bytes = encodeJson(label, record, maxPageBytes);
    try {
      if (previousRevision === 0) {
        return await store.create(runKey(key), bytes);
      }
      return await store.update(runKey(key), bytes, previousRevision);
    } catch (error) {
      if (/wrong last sequence|already exists/i.test(asError(error).message)) return null;
      throw error;
    }
  };

  const publishWake = async (key: string, revision: number): Promise<void> => {
    const ctx = await runtime.context();
    // The stable message ID makes wake-ups repairable: re-enqueueing the same
    // run revision dedupes, a new revision wakes again.
    await ctx.js.publish(wakeSubject, encodeJson(label, { key }, 4_096), {
      msgID: `wake.${subjectToken(key, "pump key")}.${revision}`,
    });
  };

  const toState = (record: PumpRecord<Input, Cursor, Item>): PumpState<Input, Cursor> => ({
    key: record.key,
    input: record.input,
    cursor: record.cursor,
    status: record.status,
    dispatched: record.dispatched,
    failureCount: record.failureCount,
    ...(record.lastError !== undefined ? { lastError: record.lastError } : {}),
    createdAt: new Date(record.createdAt),
    updatedAt: new Date(record.updatedAt),
  });

  // ==========================
  // Public operations
  // ==========================

  const start: Pump<Input, Cursor>["start"] = async (input) => {
    runtime.assertActive();
    await declaration.ready();
    const existing = await load(input.key);
    if (existing !== null && !TERMINAL.includes(existing.record.status)) return; // idempotent
    const now = new Date().toISOString();
    const record: PumpRecord<Input, Cursor, Item> = {
      key: input.key,
      input: input.input,
      cursor: null,
      status: "queued",
      dispatched: 0,
      failureCount: 0,
      ...(input.meta !== undefined ? { meta: input.meta } : {}),
      createdAt: now,
      updatedAt: now,
    };
    const revision = await casWrite(input.key, record, existing?.revision ?? 0);
    if (revision === null) return; // lost a concurrent start — that start enqueued the wake-up
    await publishWake(input.key, revision);
  };

  const get: Pump<Input, Cursor>["get"] = async (input) => {
    runtime.assertActive();
    const loaded = await load(input.key);
    return loaded === null ? null : toState(loaded.record);
  };

  const cancel: Pump<Input, Cursor>["cancel"] = async (input) => {
    runtime.assertActive();
    for (let attempt = 0; attempt < 5; attempt++) {
      const loaded = await load(input.key);
      if (loaded === null || TERMINAL.includes(loaded.record.status)) return false;
      const record = { ...loaded.record, status: "canceled" as const, lease: undefined };
      if ((await casWrite(input.key, record, loaded.revision)) !== null) return true;
    }
    return false;
  };

  const reconcile: Pump<Input, Cursor>["reconcile"] = async () => {
    runtime.assertActive();
    const store = await getKv();
    let requeued = 0;
    const keys = await store.keys("run.>");
    for await (const rawKey of keys) {
      const key = decodeSubjectToken(rawKey.slice("run.".length));
      const loaded = await load(key);
      if (loaded === null || TERMINAL.includes(loaded.record.status)) continue;
      await publishWake(key, loaded.revision);
      requeued += 1;
    }
    if (requeued > 0) {
      runtime.events.emit({ type: "pump_recovered", resource: config.id, kind: "pump", detail: { requeued } });
    }
    return { requeued };
  };

  // ==========================
  // Run execution
  // ==========================

  /** Executes one run to a terminal, waiting, or lease-blocked outcome. */
  const executeRun = async (key: string, msg: JsMsg, signal: AbortSignal): Promise<void> => {
    const leaseToken = crypto.randomUUID();
    let loaded = await load(key);

    // Claim or bail out.
    while (true) {
      if (loaded === null || TERMINAL.includes(loaded.record.status)) {
        msg.ack();
        return;
      }
      const lease = loaded.record.lease;
      if (lease !== undefined && new Date(lease.until).getTime() > Date.now() && lease.token !== leaseToken) {
        // Another process owns this run; check again after the lease window.
        msg.nak(Math.max(1_000, new Date(lease.until).getTime() - Date.now()));
        return;
      }
      const claimed: PumpRecord<Input, Cursor, Item> = {
        ...loaded.record,
        status: "running",
        lease: { token: leaseToken, until: new Date(Date.now() + leaseMs).toISOString() },
      };
      const revision = await casWrite(key, claimed, loaded.revision);
      if (revision !== null) {
        loaded = { record: claimed, revision };
        break;
      }
      loaded = await load(key);
    }

    let { record, revision } = loaded;

    /** CAS with lease refresh; on external change reloads and returns false. */
    const checkpoint = async (mutate: (r: PumpRecord<Input, Cursor, Item>) => void): Promise<boolean> => {
      const next = { ...record, lease: { token: leaseToken, until: new Date(Date.now() + leaseMs).toISOString() } };
      mutate(next);
      const newRevision = await casWrite(key, next, revision);
      msg.working();
      if (newRevision === null) {
        const reloaded = await load(key);
        if (reloaded !== null) ({ record, revision } = reloaded);
        return false;
      }
      record = next;
      revision = newRevision;
      return true;
    };

    const fail = async (error: Error): Promise<void> => {
      const failureCount = record.failureCount + 1;
      if (failureCount >= maxAttempts) {
        await checkpoint((r) => {
          r.status = "failed";
          r.failureCount = failureCount;
          r.lastError = error.message;
          r.lease = undefined;
        });
        runtime.events.emit({ type: "handler_error", resource: config.id, kind: "pump", error: error.message });
        msg.ack();
        return;
      }
      await checkpoint((r) => {
        r.status = "waiting";
        r.failureCount = failureCount;
        r.lastError = error.message;
        r.lease = undefined;
      });
      msg.nak(backoffMs[Math.min(failureCount - 1, backoffMs.length - 1)]!);
    };

    while (true) {
      if (signal.aborted || TERMINAL.includes(record.status)) {
        // Canceled externally or shutting down: release the lease, keep state.
        if (TERMINAL.includes(record.status)) msg.ack();
        else {
          await checkpoint((r) => {
            r.lease = undefined;
          });
          msg.nak();
        }
        return;
      }

      // 1. Ensure there is a current page.
      if (record.page === undefined) {
        let pulled: { items: Item[]; nextCursor: Cursor | null };
        try {
          pulled = await config.pull({ input: record.input, cursor: record.cursor, limit: batchSize, signal });
        } catch (error) {
          await fail(asError(error));
          return;
        }
        const keys = new Set(pulled.items.map((item) => item.key));
        if (keys.size !== pulled.items.length) {
          await fail(new Error("pull() returned items with duplicate keys"));
          return;
        }
        if (pulled.items.length === 0 && pulled.nextCursor === null) {
          await checkpoint((r) => {
            r.status = "completed";
            r.lease = undefined;
          });
          msg.ack();
          return;
        }
        if (!(await checkpoint((r) => {
          r.page = { items: pulled.items, done: [], nextCursor: pulled.nextCursor };
        }))) continue; // externally changed (e.g. canceled) — re-evaluate
      }

      // 2. Dispatch remaining page items with bounded concurrency, checkpointing
      //    each completed item key individually. A crash repeats only items
      //    whose sink succeeded after their last confirmed checkpoint.
      const page = record.page!;
      const remaining = page.items.filter((item) => !page.done.includes(item.key));
      let dispatchError: Error | null = null;
      let index = 0;
      const workers = Array.from({ length: Math.min(dispatchConcurrency, remaining.length) }, async () => {
        while (dispatchError === null && !signal.aborted) {
          const item = remaining[index++];
          if (item === undefined) return;
          try {
            await config.dispatch({ input: record.input, item, signal });
          } catch (error) {
            dispatchError = asError(error);
            return;
          }
          await checkpoint((r) => {
            r.page?.done.push(item.key);
          });
        }
      });
      await Promise.all(workers);
      if (dispatchError !== null) {
        await fail(dispatchError);
        return;
      }
      if (signal.aborted || TERMINAL.includes(record.status)) continue; // handled at loop head

      // 3. Page complete: advance the committed cursor, clear the page.
      const finished = record.page!;
      const advanced = await checkpoint((r) => {
        r.cursor = finished.nextCursor;
        r.dispatched += finished.items.length;
        r.failureCount = 0;
        delete r.lastError;
        delete r.page;
        if (finished.nextCursor === null) {
          r.status = "completed";
          r.lease = undefined;
        }
      });
      if (!advanced) continue;
      if (record.status === "completed") {
        msg.ack();
        return;
      }
    }
  };

  const process: Pump<Input, Cursor>["process"] = async (options = {}) => {
    runtime.assertActive();
    await declaration.ready();
    const ctx = await runtime.context();
    await ensureConsumer(ctx, wakeStream, {
      durable_name: wakeDurable,
      ack_policy: AckPolicy.Explicit,
      filter_subject: wakeSubject,
      ack_wait: nanos(leaseMs),
      max_ack_pending: maxActiveRuns,
      deliver_policy: DeliverPolicy.All,
    });
    await reconcile();

    const wr = createWorkerRuntime(options, { onFinished: () => runtime.unregisterWorker(wr) });
    runtime.registerWorker(wr);
    runtime.events.emit({ type: "worker_started", resource: config.id, kind: "pump" });

    const onMessage = async (msg: JsMsg, signal: AbortSignal): Promise<void> => {
      let wake: { key: string };
      try {
        wake = decodeJson<{ key: string }>(msg.data);
        if (typeof wake.key !== "string") throw new Error("invalid wake-up");
      } catch {
        msg.term();
        return;
      }
      try {
        await executeRun(wake.key, msg, signal);
      } catch (error) {
        runtime.events.emit({ type: "handler_error", resource: config.id, kind: "pump", error: asError(error).message });
        msg.nak(5_000);
      }
    };

    runPullLoop(wr, () => ctx.js.consumers.get(wakeStream, wakeDurable), onMessage, {
      events: runtime.events,
      resource: config.id,
    }).finally(() => {
      runtime.events.emit({ type: "worker_stopped", resource: config.id, kind: "pump" });
    });
    return wr.worker;
  };

  return {
    ready: () => declaration.ready(),
    start,
    process,
    get,
    cancel,
    reconcile,
  };
};
