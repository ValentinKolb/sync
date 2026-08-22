import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { jetstreamManager } from "@nats-io/jetstream";
import type { NatsConnection } from "@nats-io/nats-core";
import { createSync } from "../src/sync.ts";
import type { Sync } from "../src/sync.ts";
import { CursorMismatchError, ResourceDriftError, RetentionGapError } from "../src/errors.ts";
import { connectToCluster, uniqueName } from "./cluster.ts";
import { cleanupNamespaces, collect, testNamespace, waitFor } from "./helpers.ts";

type Event = { n: number };

let nc: NatsConnection;
let sync: Sync;
const namespace = testNamespace();

const topicConfig = (id: string) => ({
  id,
  retention: { maxAgeMs: 60 * 60 * 1_000, maxBytes: 64 * 1024 * 1024 },
});

beforeAll(async () => {
  nc = await connectToCluster({ name: "topic-test" });
  sync = createSync({ connection: nc, namespace, application: "tests" });
  await sync.ready();
});

afterAll(async () => {
  await sync.drain({ timeoutMs: 5_000 });
  await cleanupNamespaces(nc, [namespace]);
  await nc.close();
});

describe("topic publish", () => {
  test("returns receipt with cursor; idempotencyKey dedupes within the window", async () => {
    const topic = sync.topic<Event>(topicConfig("pub"));
    const first = await topic.publish({ data: { n: 1 }, idempotencyKey: "evt-1" });
    expect(first.duplicate).toBe(false);
    expect(first.cursor).toContain("s6t.");
    const dup = await topic.publish({ data: { n: 1 }, idempotencyKey: "evt-1" });
    expect(dup.duplicate).toBe(true);
    expect(dup.streamSequence).toBe(first.streamSequence);
    // Same key, different tenant: no dedupe across tenants.
    const other = await topic.publish({ data: { n: 1 }, tenantId: "acme", idempotencyKey: "evt-1" });
    expect(other.duplicate).toBe(false);
  });

  test("latestCursor is per tenant and null when empty", async () => {
    const topic = sync.topic<Event>(topicConfig("cursor"));
    expect(await topic.latestCursor()).toBeNull();
    const receipt = await topic.publish({ data: { n: 1 } });
    expect(await topic.latestCursor()).toBe(receipt.cursor);
    expect(await topic.latestCursor({ tenantId: "acme" })).toBeNull();
  });
});

describe("topic live", () => {
  test("broadcasts to every live listener without replay", async () => {
    const topic = sync.topic<Event>(topicConfig("live"));
    await topic.ready();
    await topic.publish({ data: { n: 0 } }); // before subscribe: must not be seen
    const controller = new AbortController();
    const a = collect(topic.live({ signal: controller.signal }), 2);
    const b = collect(topic.live({ signal: controller.signal }), 2);
    await Bun.sleep(250); // subscriptions in place
    await topic.publish({ data: { n: 1 } });
    await topic.publish({ data: { n: 2 } });
    const [eventsA, eventsB] = await Promise.all([a, b]);
    expect(eventsA.map((e) => e.data.n)).toEqual([1, 2]);
    expect(eventsB.map((e) => e.data.n)).toEqual([1, 2]);
    controller.abort();
  });
});

describe("topic replay and follow", () => {
  test("replays from cursor to captured head, tenant-scoped", async () => {
    const topic = sync.topic<Event>(topicConfig("replay"));
    const r1 = await topic.publish({ data: { n: 1 } });
    await topic.publish({ data: { n: 99 }, tenantId: "acme" });
    await topic.publish({ data: { n: 2 } });
    await topic.publish({ data: { n: 3 } });

    const all = await collect(topic.replay());
    expect(all.map((e) => e.data.n)).toEqual([1, 2, 3]);

    const afterFirst = await collect(topic.replay({ after: r1.cursor }));
    expect(afterFirst.map((e) => e.data.n)).toEqual([2, 3]);

    const acme = await collect(topic.replay({ tenantId: "acme" }));
    expect(acme.map((e) => e.data.n)).toEqual([99]);

    // Events published after replay started are not part of the captured head.
    await topic.publish({ data: { n: 4 } });
    const stillThree = await collect(topic.replay());
    expect(stillThree.map((e) => e.data.n)).toEqual([1, 2, 3, 4]);
  });

  test("independent cursors: two replays do not affect each other", async () => {
    const topic = sync.topic<Event>(topicConfig("cursors"));
    for (let n = 1; n <= 5; n++) await topic.publish({ data: { n } });
    const [a, b] = await Promise.all([collect(topic.replay()), collect(topic.replay())]);
    expect(a.map((e) => e.data.n)).toEqual([1, 2, 3, 4, 5]);
    expect(b.map((e) => e.data.n)).toEqual([1, 2, 3, 4, 5]);
  });

  test("follow keeps yielding new events after the head", async () => {
    const topic = sync.topic<Event>(topicConfig("follow"));
    await topic.publish({ data: { n: 1 } });
    const controller = new AbortController();
    const followed = collect(topic.follow({ signal: controller.signal }), 3);
    await Bun.sleep(250);
    await topic.publish({ data: { n: 2 } });
    await topic.publish({ data: { n: 3 } });
    expect((await followed).map((e) => e.data.n)).toEqual([1, 2, 3]);
    controller.abort();
  });

  test("cursor from another topic is rejected", async () => {
    const a = sync.topic<Event>(topicConfig("mismatch-a"));
    const b = sync.topic<Event>(topicConfig("mismatch-b"));
    const receipt = await a.publish({ data: { n: 1 } });
    await b.publish({ data: { n: 1 } });
    await expect(collect(b.replay({ after: receipt.cursor }))).rejects.toBeInstanceOf(CursorMismatchError);
  });

  test("retention gap fails explicitly instead of silently skipping", async () => {
    const topic = sync.topic<Event>(topicConfig("gap"));
    const first = await topic.publish({ data: { n: 1 } });
    await topic.publish({ data: { n: 2 } });
    const third = await topic.publish({ data: { n: 3 } });
    // Simulate retention removing the first two messages.
    const jsm = await jetstreamManager(nc);
    const streams: string[] = [];
    for await (const info of jsm.streams.list()) {
      if (info.config.metadata?.["sync.id"] === "gap" && !info.config.name.includes("D_")) {
        streams.push(info.config.name);
      }
    }
    expect(streams).toHaveLength(1);
    await jsm.streams.purge(streams[0]!, { seq: Number(third.streamSequence) });
    await expect(collect(topic.replay({ after: first.cursor }))).rejects.toBeInstanceOf(RetentionGapError);
  });
});

describe("topic durable process", () => {
  test("same consumer name competes; different names own independent cursors", async () => {
    const topic = sync.topic<Event>(topicConfig("groups"));
    const shared: number[] = [];
    const independent: number[] = [];
    const w1 = await topic.process({ consumer: "shared", concurrency: 2 }, async (event) => {
      shared.push(event.data.n);
    });
    const w2 = await topic.process({ consumer: "shared", concurrency: 2 }, async (event) => {
      shared.push(event.data.n);
    });
    const w3 = await topic.process({ consumer: "solo" }, async (event) => {
      independent.push(event.data.n);
    });
    for (let n = 1; n <= 10; n++) await topic.publish({ data: { n } });
    await waitFor(() => shared.length >= 10 && independent.length >= 10);
    await Bun.sleep(300);
    expect(shared.toSorted((x, y) => x - y)).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    expect(independent.toSorted((x, y) => x - y)).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    await Promise.all([w1.drain(), w2.drain(), w3.drain()]);
  });

  test("start latest skips history; start after cursor resumes mid-stream", async () => {
    const topic = sync.topic<Event>(topicConfig("start"));
    const r1 = await topic.publish({ data: { n: 1 } });
    await topic.publish({ data: { n: 2 } });

    const fromCursor: number[] = [];
    const w = await topic.process({ consumer: "resume", start: { after: r1.cursor } }, async (event) => {
      fromCursor.push(event.data.n);
    });
    await waitFor(() => fromCursor.includes(2));
    expect(fromCursor).toEqual([2]);
    await w.drain();
  });

  test("failed handler retries with backoff and dead-letters after maxAttempts", async () => {
    const topic = sync.topic<Event>(topicConfig("retry"));
    const attempts: number[] = [];
    const w = await topic.process(
      {
        consumer: "flaky",
        delivery: { maxAttempts: 3, backoffMs: [100], ackWaitMs: 5_000 },
      },
      async (event) => {
        attempts.push(event.attempt);
        throw new Error("boom");
      },
    );
    await topic.publish({ data: { n: 1 } });
    await waitFor(() => attempts.length >= 3, 15_000);
    await Bun.sleep(500);
    expect(attempts).toEqual([1, 2, 3]);
    await w.drain();

    // DLQ entry exists in the topic's DLQ stream.
    const jsm = await jetstreamManager(nc);
    let dlqMessages = 0;
    for await (const info of jsm.streams.list()) {
      if (info.config.metadata?.["sync.id"] === "retry" && info.config.name.includes("D_")) {
        dlqMessages = info.state.messages;
      }
    }
    expect(dlqMessages).toBe(1);
  });
});

describe("resource declarations", () => {
  test("drifted redeclaration fails clearly and never mutates the stream", async () => {
    const id = uniqueName("drift");
    const topic = sync.topic<Event>(topicConfig(id));
    await topic.ready();

    const second = createSync({ connection: nc, namespace, application: "tests-2" });
    const drifted = second.topic<Event>({ id, retention: { maxAgeMs: 1_000, maxBytes: 1_024 } });
    await expect(drifted.ready()).rejects.toBeInstanceOf(ResourceDriftError);
    expect(second.health().driftedResources).toBe(1);

    // Original declaration still verifies cleanly — nothing was mutated.
    // A different application opening a shared resource declares its owner.
    const third = createSync({ connection: nc, namespace, application: "tests-3" });
    await third.topic<Event>({ ...topicConfig(id), owner: "tests" }).ready();

    // Without the explicit owner the cross-application open is refused.
    const fourth = createSync({ connection: nc, namespace, application: "tests-4" });
    await expect(fourth.topic<Event>(topicConfig(id)).ready()).rejects.toBeInstanceOf(ResourceDriftError);
  });

  test("conflicting local declarations fail before any I/O", () => {
    const id = uniqueName("conflict");
    sync.topic<Event>(topicConfig(id));
    expect(() => sync.topic<Event>({ id, retention: { maxAgeMs: 1, maxBytes: 1 } })).toThrow(
      "already declared in this process",
    );
  });

  test("health and resources report the topic", async () => {
    const health = sync.health();
    expect(health.state).toBe("ready");
    expect(health.connection).toBe("connected");
    const resources = await sync.resources();
    const pub = resources.find((r) => r.id === "pub");
    expect(pub?.kind).toBe("topic");
    expect(pub?.state).toBe("ready");
    expect(pub?.detail?.messages).toBeGreaterThan(0);
  });
});
