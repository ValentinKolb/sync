import { AckPolicy, DeliverPolicy, DiscardPolicy, RetentionPolicy, StorageType } from "@nats-io/jetstream";
import type { JsMsg } from "@nats-io/jetstream";
import { headers as natsHeaders } from "@nats-io/nats-core";
import type { KV } from "@nats-io/kv";
import { assertValidTimeZone, nextCronTimestamp } from "./cron.ts";
import { decodeJson, encodeJson } from "./codec.ts";
import type { JsonValue } from "./codec.ts";
import { confirmedAck, runPullLoop } from "./consume.ts";
import { InvalidNameError, asError } from "./errors.ts";
import { assertName, consumerName, resourceIdentity, streamName, subjectRoot, subjectToken, decodeSubjectToken } from "./naming.ts";
import { ensureConsumer, ensureKv, ensureStream } from "./resources.ts";
import type { ProvisionContext } from "./resources.ts";
import type { SyncRuntime } from "./runtime.ts";
import { assertRetention, backoffDelayMs, millis, nanos, resolveDelivery } from "./types.ts";
import type { DeliveryConfig, MessageMeta, RetentionConfig } from "./types.ts";
import { createWorkerRuntime } from "./worker.ts";
import type { ProcessOptions, Worker, WorkerRuntime } from "./worker.ts";

// ==========================
// Types
// ==========================

export type SchedulerConfig = {
  id: string;
  owner?: string;
  delivery?: DeliveryConfig;
  /** Retention of accepted ticks (and run history) in the schedule stream. Default 7 days / 64 MiB. */
  retention?: RetentionConfig;
};

export type ScheduleContext = {
  scheduleId: string;
  runId: string;
  runNumber: number;
  slot: Date;
  trigger: "schedule" | "manual";
  attempt: number;
  signal: AbortSignal;
  heartbeat(): Promise<void>;
};

export type ScheduleDefinition = {
  id: string;
  /** Standard five-field cron: minute hour day month weekday. */
  cron: string;
  timezone?: string;
  misfire?: "latest" | "all";
  meta?: MessageMeta;
  process(context: ScheduleContext): Promise<void>;
};

export type ScheduleInfo = {
  id: string;
  cron: string;
  timezone: string;
  misfire: "latest" | "all";
  nextRunAt: Date;
  runNumber: number;
  failureCount: number;
  handlerAvailable: boolean;
  meta?: MessageMeta;
};

export type Scheduler = {
  ready(): Promise<void>;
  create(config: ScheduleDefinition): Promise<{ created: boolean; updated: boolean }>;
  start(options?: ProcessOptions): Promise<Worker>;
  delete(input: { id: string }): Promise<boolean>;
  /** Durably accept a manual run. Repeating the same requestId returns the same run. */
  runNow(input: { id: string; requestId: string }): Promise<{ runId: string }>;
  get(input: { id: string }): Promise<ScheduleInfo | null>;
  list(): Promise<ScheduleInfo[]>;
};

type DefRecord = {
  id: string;
  cron: string;
  timezone: string;
  misfire: "latest" | "all";
  meta?: MessageMeta;
  createdAt: string;
  updatedAt: string;
};

type RunRecord = {
  runNumber: number;
  lastRun: string;
  failureCount: number;
};

// ==========================
// Scheduler factory
// ==========================

export const createScheduler = (runtime: SyncRuntime, config: SchedulerConfig): Scheduler => {
  const identity = resourceIdentity(runtime.namespace, "scheduler", config.id);
  const owner = config.owner ?? runtime.application;
  const delivery = resolveDelivery(config.delivery);
  const retention = assertRetention(config.retention ?? { maxAgeMs: 7 * 24 * 60 * 60 * 1_000, maxBytes: 64 * 1024 * 1024 });
  const replicas = runtime.defaults.replicas;
  const storage = runtime.defaults.storage === "memory" ? StorageType.Memory : StorageType.File;
  const stream = streamName(identity);
  const bucket = `S6_SK_${identity.hash}`;
  const root = subjectRoot(identity);
  const label = `scheduler ${config.id}`;

  const schedSubject = (id: string): string => `${root}.sched.${subjectToken(id, "schedule id")}`;
  const tickSubject = (id: string): string => `${root}.tick.${subjectToken(id, "schedule id")}`;
  const manualSubject = (id: string): string => `${root}.run.${subjectToken(id, "schedule id")}`;

  /** Locally registered schedules (handler + definition). */
  const registry = new Map<string, ScheduleDefinition & { timezone: string; misfire: "latest" | "all" }>();
  /** Active workers so create() can attach loops to already started workers. */
  const activeWorkers = new Set<{ wr: WorkerRuntime; served: Set<string> }>();

  let kv: KV | null = null;

  const declaration = runtime.declare({
    identity,
    owner,
    configKey: JSON.stringify(["scheduler", config.id, owner, delivery, retention]),
    natsNames: [stream, `KV_${bucket}`],
    provision: async (ctx: ProvisionContext) => {
      await ensureStream(ctx, identity, owner, {
        name: stream,
        subjects: [`${root}.sched.*`, `${root}.tick.*`, `${root}.run.*`, `${root}.cancel`],
        retention: RetentionPolicy.Limits,
        discard: DiscardPolicy.Old,
        storage,
        num_replicas: replicas,
        max_age: nanos(retention.maxAgeMs),
        max_bytes: retention.maxBytes,
        max_msgs: retention.maxMessages ?? -1,
        max_msg_size: -1,
        duplicate_window: nanos(120_000),
        allow_msg_schedules: true,
        allow_msg_ttl: true,
        allow_rollup_hdrs: true,
      });
      kv = await ensureKv(ctx, identity, owner, bucket, {
        history: 1,
        replicas,
        storage,
        markerTTL: 1_000,
      });
    },
    summary: async (ctx: ProvisionContext) => {
      const info = await ctx.jsm.streams.info(stream, { subjects_filter: `${root}.sched.*` });
      const status = await (kv ?? (await ctx.kvm.open(bucket))).status();
      return {
        schedules: Object.keys(info.state.subjects ?? {}).length,
        messages: info.state.messages,
        kvEntries: status.values,
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

  const defKey = (id: string): string => `def.${subjectToken(id, "schedule id")}`;
  const runKey = (id: string): string => `run.${subjectToken(id, "schedule id")}`;

  const loadDef = async (id: string): Promise<{ record: DefRecord; revision: number } | null> => {
    const store = await getKv();
    const entry = await store.get(defKey(id));
    if (entry === null || entry.operation !== "PUT") return null;
    return { record: decodeJson<DefRecord>(entry.value), revision: entry.revision };
  };

  const loadRun = async (id: string): Promise<RunRecord> => {
    const store = await getKv();
    const entry = await store.get(runKey(id));
    if (entry === null || entry.operation !== "PUT") return { runNumber: 0, lastRun: "", failureCount: 0 };
    return decodeJson<RunRecord>(entry.value);
  };

  const saveRun = async (id: string, mutate: (record: RunRecord) => void): Promise<RunRecord> => {
    const store = await getKv();
    for (let attempt = 0; attempt < 10; attempt++) {
      const entry = await store.get(runKey(id));
      const record =
        entry === null || entry.operation !== "PUT"
          ? { runNumber: 0, lastRun: "", failureCount: 0 }
          : decodeJson<RunRecord>(entry.value);
      mutate(record);
      const bytes = encodeJson(label, record, 4_096);
      try {
        if (entry === null || entry.operation !== "PUT") await store.create(runKey(id), bytes);
        else await store.update(runKey(id), bytes, entry.revision);
        return record;
      } catch (error) {
        if (!/wrong last sequence|already exists/i.test(asError(error).message)) throw error;
      }
    }
    throw new Error(`${label}: run record for ${id} is contended`);
  };

  /** Publish (or replace, via per-subject rollup) the broker-side schedule message. */
  const publishSchedule = async (record: DefRecord, revision: number): Promise<void> => {
    const ctx = await runtime.context();
    const hdrs = natsHeaders();
    // The schedule message must outlive the stream's tick retention.
    hdrs.set("Nats-TTL", "never");
    await ctx.js.publish(
      schedSubject(record.id),
      encodeJson(label, { scheduleId: record.id, trigger: "schedule" }, 16 * 1024),
      {
        headers: hdrs,
        msgID: `sched.${subjectToken(record.id, "schedule id")}.${revision}`,
        schedule: {
          // NATS uses six-field cron; Sync's public contract stays five-field.
          specification: { cron: `0 ${record.cron}` },
          target: tickSubject(record.id),
          timezone: record.timezone,
          rollup: "sub",
        },
      },
    );
  };

  // ==========================
  // Public operations
  // ==========================

  const create: Scheduler["create"] = async (definition) => {
    runtime.assertActive();
    assertName(definition.id, "schedule id");
    const timezone = definition.timezone ?? "UTC";
    assertValidTimeZone(timezone);
    const misfire = definition.misfire ?? "latest";
    try {
      nextCronTimestamp(definition.cron, timezone, Date.now());
    } catch (error) {
      throw new InvalidNameError(`invalid cron for schedule ${definition.id}: ${asError(error).message}`);
    }
    await declaration.ready();

    const store = await getKv();
    const existing = await loadDef(definition.id);
    const now = new Date().toISOString();
    const record: DefRecord = {
      id: definition.id,
      cron: definition.cron,
      timezone,
      misfire,
      ...(definition.meta !== undefined ? { meta: definition.meta } : {}),
      createdAt: existing?.record.createdAt ?? now,
      updatedAt: now,
    };
    const unchanged =
      existing !== null &&
      existing.record.cron === record.cron &&
      existing.record.timezone === record.timezone &&
      existing.record.misfire === record.misfire &&
      JSON.stringify(existing.record.meta ?? null) === JSON.stringify(record.meta ?? null);

    let revision: number;
    if (existing === null) {
      revision = await store.create(defKey(definition.id), encodeJson(label, record, 16 * 1024));
    } else if (unchanged) {
      revision = existing.revision;
    } else {
      revision = await store.update(defKey(definition.id), encodeJson(label, record, 16 * 1024), existing.revision);
    }
    // Always republish: self-heals a lost schedule message; the stable message
    // ID plus per-subject rollup make this idempotent.
    await publishSchedule(record, revision);

    registry.set(definition.id, { ...definition, timezone, misfire });
    for (const active of activeWorkers) attachLoop(active, definition.id);
    return { created: existing === null, updated: existing !== null && !unchanged };
  };

  const deleteSchedule: Scheduler["delete"] = async (input) => {
    runtime.assertActive();
    await declaration.ready();
    const ctx = await runtime.context();
    const store = await getKv();
    const existing = await loadDef(input.id);
    registry.delete(input.id);
    if (existing === null) return false;
    // Cancel the broker schedule, then remove definition and run state. The
    // cancel must be published on a different subject than the schedule itself.
    await ctx.js.publish(`${root}.cancel`, "", {
      cancelSchedule: { scheduleSubject: schedSubject(input.id) },
    });
    await store.purge(defKey(input.id)).catch(() => {});
    await store.purge(runKey(input.id)).catch(() => {});
    await ctx.jsm.consumers.delete(stream, consumerName(identity, input.id)).catch(() => {});
    return true;
  };

  const runNow: Scheduler["runNow"] = async (input) => {
    runtime.assertActive();
    assertName(input.requestId, "requestId");
    await declaration.ready();
    const existing = await loadDef(input.id);
    if (existing === null) throw new InvalidNameError(`schedule ${input.id} does not exist`);
    const ctx = await runtime.context();
    const runId = `${input.id}:manual:${input.requestId}`;
    await ctx.js.publish(
      manualSubject(input.id),
      encodeJson(label, { scheduleId: input.id, trigger: "manual", requestId: input.requestId }, 16 * 1024),
      { msgID: `manual.${subjectToken(input.id, "schedule id")}.${subjectToken(input.requestId, "requestId")}` },
    );
    // Durably accepted — not started, not completed.
    return { runId };
  };

  const toInfo = async (record: DefRecord): Promise<ScheduleInfo> => {
    const run = await loadRun(record.id);
    return {
      id: record.id,
      cron: record.cron,
      timezone: record.timezone,
      misfire: record.misfire,
      nextRunAt: new Date(nextCronTimestamp(record.cron, record.timezone, Date.now())),
      runNumber: run.runNumber,
      failureCount: run.failureCount,
      handlerAvailable: registry.has(record.id),
      ...(record.meta !== undefined ? { meta: record.meta } : {}),
    };
  };

  const get: Scheduler["get"] = async (input) => {
    runtime.assertActive();
    const existing = await loadDef(input.id);
    return existing === null ? null : toInfo(existing.record);
  };

  const list: Scheduler["list"] = async () => {
    runtime.assertActive();
    const store = await getKv();
    const infos: ScheduleInfo[] = [];
    const keys = await store.keys("def.>");
    for await (const rawKey of keys) {
      const id = decodeSubjectToken(rawKey.slice("def.".length));
      const existing = await loadDef(id);
      if (existing !== null) infos.push(await toInfo(existing.record));
    }
    return infos.toSorted((a, b) => a.id.localeCompare(b.id));
  };

  // ==========================
  // Tick execution
  // ==========================

  const handleTick = async (scheduleId: string, msg: JsMsg, signal: AbortSignal): Promise<void> => {
    const definition = registry.get(scheduleId);
    const existing = await loadDef(scheduleId);
    if (definition === undefined || existing === null) {
      // Deleted or foreign schedule: drop the tick permanently.
      msg.term();
      return;
    }
    let payload: { trigger?: string; requestId?: string };
    try {
      payload = decodeJson(msg.data);
    } catch {
      msg.term();
      return;
    }
    const trigger: "schedule" | "manual" = payload.trigger === "manual" ? "manual" : "schedule";
    const slot = new Date(millis(msg.info.timestampNanos));

    // Misfire "latest": a scheduled tick with a newer tick behind it on the
    // same subject is a stale slot — skip it explicitly. The newest retained
    // tick always executes, so accepted latest work is never lost.
    if (trigger === "schedule" && existing.record.misfire === "latest") {
      const ctx = await runtime.context();
      const last = await ctx.jsm.streams.getMessage(stream, { last_by_subj: tickSubject(scheduleId) }).catch(() => null);
      if (last !== null && last.seq > msg.seq) {
        runtime.events.emit({
          type: "schedule_misfire",
          resource: config.id,
          kind: "scheduler",
          detail: { scheduleId, skippedSlot: slot.toISOString() },
        });
        await confirmedAck(msg, label).catch(() => {});
        return;
      }
    }

    const runIdentity = trigger === "manual" ? `m:${payload.requestId}` : `s:${slot.toISOString()}`;
    const runId = trigger === "manual" ? `${scheduleId}:manual:${payload.requestId}` : `${scheduleId}:${slot.toISOString()}`;
    // Duplicate delivery of the same slot reuses its run number.
    const run = await loadRun(scheduleId);
    const runNumber =
      run.lastRun === runIdentity
        ? run.runNumber
        : (await saveRun(scheduleId, (r) => {
            if (r.lastRun !== runIdentity) {
              r.runNumber += 1;
              r.lastRun = runIdentity;
            }
          })).runNumber;

    const attempt = msg.info.deliveryCount;
    runtime.events.emit({
      type: "schedule_tick",
      resource: config.id,
      kind: "scheduler",
      detail: { scheduleId, runId, runNumber, trigger, attempt },
    });
    try {
      await definition.process({
        scheduleId,
        runId,
        runNumber,
        slot,
        trigger,
        attempt,
        signal,
        heartbeat: async () => {
          msg.working();
        },
      });
      await confirmedAck(msg, label);
    } catch (error) {
      if (signal.aborted) {
        msg.nak();
        return;
      }
      const err = asError(error);
      runtime.events.emit({ type: "handler_error", resource: config.id, kind: "scheduler", error: err.message });
      if (attempt >= delivery.maxAttempts) {
        await saveRun(scheduleId, (r) => {
          r.failureCount += 1;
        });
        await confirmedAck(msg, label).catch(() => {});
      } else {
        msg.nak(backoffDelayMs(delivery, attempt));
      }
    }
  };

  const attachLoop = (active: { wr: WorkerRuntime; served: Set<string> }, scheduleId: string): void => {
    if (active.served.has(scheduleId) || active.wr.stopping) return;
    active.served.add(scheduleId);
    const durable = consumerName(identity, scheduleId);
    const setup = async (): Promise<void> => {
      const ctx = await runtime.context();
      await ensureConsumer(ctx, stream, {
        durable_name: durable,
        ack_policy: AckPolicy.Explicit,
        filter_subjects: [tickSubject(scheduleId), manualSubject(scheduleId)],
        ack_wait: nanos(delivery.ackWaitMs),
        max_deliver: delivery.maxAttempts + 1,
        // Serial execution per schedule: no overlapping runs.
        max_ack_pending: 1,
        deliver_policy: DeliverPolicy.All,
      });
      await runPullLoop(
        active.wr,
        () => ctx.js.consumers.get(stream, durable),
        (msg, signal) => handleTick(scheduleId, msg, signal),
        { events: runtime.events, resource: config.id },
      );
    };
    setup().catch((error) => {
      runtime.events.emit({ type: "handler_error", resource: config.id, kind: "scheduler", error: asError(error).message });
    });
  };

  const start: Scheduler["start"] = async (options = {}) => {
    runtime.assertActive();
    await declaration.ready();
    const wr = createWorkerRuntime(options, {
      onFinished: () => {
        runtime.unregisterWorker(wr);
        activeWorkers.delete(active);
      },
    });
    const active = { wr, served: new Set<string>() };
    runtime.registerWorker(wr);
    activeWorkers.add(active);
    runtime.events.emit({ type: "worker_started", resource: config.id, kind: "scheduler" });
    for (const scheduleId of registry.keys()) attachLoop(active, scheduleId);
    return wr.worker;
  };

  return {
    ready: () => declaration.ready(),
    create,
    start,
    delete: deleteSchedule,
    runNow,
    get,
    list,
  };
};
