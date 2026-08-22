import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import type { NatsConnection } from "@nats-io/nats-core";
import { createSync } from "../src/sync.ts";
import type { Sync } from "../src/sync.ts";
import { BatchSubmitError } from "../src/errors.ts";
import { connectToCluster } from "./cluster.ts";
import { cleanupNamespaces, testNamespace, waitFor } from "./helpers.ts";

type RunInput = { runId: string };

let nc: NatsConnection;
let sync: Sync;
const namespace = testNamespace();

beforeAll(async () => {
  nc = await connectToCluster({ name: "job-test" });
  sync = createSync({ connection: nc, namespace, application: "tests" });
  await sync.ready();
});

afterAll(async () => {
  await sync.drain({ timeoutMs: 2_000 });
  await cleanupNamespaces(nc, [namespace]);
  await nc.close();
});

describe("job submission", () => {
  test("submit is idempotent by key within the dedupe window, per tenant", async () => {
    const jobs = sync.job<RunInput>({ id: "idem" });
    const first = await jobs.submit({ key: "run:1", input: { runId: "1" } });
    expect(first.duplicate).toBe(false);
    expect(first.jobId).toBe(first.messageId);
    const dup = await jobs.submit({ key: "run:1", input: { runId: "1" } });
    expect(dup.duplicate).toBe(true);
    expect(dup.streamSequence).toBe(first.streamSequence);
    const other = await jobs.submit({ key: "run:1", tenantId: "acme", input: { runId: "1" } });
    expect(other.duplicate).toBe(false);
  });

  test("submitMany fans out with bounded publishers and reports duplicates", async () => {
    const jobs = sync.job<RunInput>({ id: "fanout" });
    const inputs = Array.from({ length: 100 }, (_, i) => ({ key: `run:${i}`, input: { runId: String(i) } }));
    const result = await jobs.submitMany(inputs, { publishConcurrency: 16 });
    expect(result).toEqual({ accepted: 100, duplicates: 0 });

    // Resubmitting the same keys dedupes everything.
    const again = await jobs.submitMany(inputs, { publishConcurrency: 8, maxPendingBytes: 64 * 1024 });
    expect(again).toEqual({ accepted: 0, duplicates: 100 });

    const done: string[] = [];
    const worker = await jobs.process({ concurrency: 16 }, async (context) => {
      done.push(context.input.runId);
    });
    await waitFor(() => done.length >= 100, 20_000);
    await Bun.sleep(300);
    expect(new Set(done).size).toBe(100);
    await worker.drain();
  }, 45_000);

  test("submitMany failure keeps prior accepted items and reports counts", async () => {
    const jobs = sync.job<RunInput>({ id: "batchfail", maxPayloadBytes: 1_024 });
    const inputs = [
      { key: "ok-1", input: { runId: "1" } },
      { key: "ok-2", input: { runId: "2" } },
      { key: "huge", input: { runId: "x".repeat(4_000) } },
      { key: "never", input: { runId: "3" } },
    ];
    try {
      await jobs.submitMany(inputs, { publishConcurrency: 1 });
      throw new Error("expected BatchSubmitError");
    } catch (error) {
      expect(error).toBeInstanceOf(BatchSubmitError);
      expect((error as BatchSubmitError).accepted).toBe(2);
      expect((error as BatchSubmitError).duplicates).toBe(0);
    }
  });
});

describe("job processing", () => {
  test("onError controls retry delay and dead-lettering; DLQ entries carry the key", async () => {
    const jobs = sync.job<RunInput>({
      id: "policy",
      delivery: { maxAttempts: 5, backoffMs: [60_000], ackWaitMs: 5_000 },
    });
    const attempts: number[] = [];
    const worker = await jobs.process(
      {
        onError: async ({ context }) => {
          // Fast custom retry once, then explicit dead-letter with domain reason.
          if (context.failureCount === 0) return { action: "retry", delayMs: 100 };
          return { action: "dead_letter", reason: "gave up deliberately" };
        },
      },
      async (context) => {
        attempts.push(context.attempt);
        expect(context.key).toBe("run:x");
        throw new Error("workflow failed");
      },
    );
    await jobs.submit({ key: "run:x", input: { runId: "x" } });
    await waitFor(() => attempts.length >= 2, 15_000);
    await Bun.sleep(400);
    expect(attempts).toEqual([1, 2]);

    const dead = await jobs.deadLetters.list();
    expect(dead).toHaveLength(1);
    expect(dead[0]!.data.key).toBe("run:x");
    expect(dead[0]!.data.input.runId).toBe("x");
    expect(dead[0]!.reason).toBe("gave up deliberately");

    // Requeue keeps the original job key visible to the handler.
    const seenKeys: string[] = [];
    const worker2 = await jobs.process({}, async (context) => {
      seenKeys.push(context.key);
    });
    await jobs.deadLetters.requeue({ messageId: dead[0]!.messageId, idempotencyKey: "run:x-retry" });
    await waitFor(() => seenKeys.length >= 1, 15_000);
    expect(seenKeys[0]).toBe("run:x");
    await Promise.all([worker.drain(), worker2.drain()]);
  }, 45_000);

  test("delayed jobs execute after the delay and survive without a running worker", async () => {
    const jobs = sync.job<RunInput>({ id: "delayed" });
    const start = Date.now();
    await jobs.submit({ key: "later", input: { runId: "later" }, delayMs: 1_500 });
    const executedAt: number[] = [];
    const worker = await jobs.process({}, async () => {
      executedAt.push(Date.now() - start);
    });
    await waitFor(() => executedAt.length === 1, 15_000);
    expect(executedAt[0]!).toBeGreaterThanOrEqual(1_200);
    await worker.drain();
  }, 30_000);
});

describe("hardening regressions", () => {
  test("an unconfirmable ack after a successful handler is not a failure", async () => {
    const jobs = sync.job<RunInput>({
      id: "stale-ack",
      delivery: { maxAttempts: 3, backoffMs: [200], ackWaitMs: 5_000 },
    });
    const { jetstreamManager } = await import("@nats-io/jetstream");
    const jsm = await jetstreamManager(nc);
    const onErrorCalls: string[] = [];
    let ran = 0;
    const worker = await jobs.process(
      {
        onError: async ({ error }) => {
          onErrorCalls.push(error.message);
          return { action: "dead_letter", reason: "should never happen" };
        },
      },
      async () => {
        ran += 1;
        // Simulate the delivery being superseded: the consumer disappears
        // before the ack, so the confirmed ack cannot succeed.
        for await (const info of jsm.streams.list()) {
          const meta = info.config.metadata;
          if (meta?.["sync.kind"] === "job" && meta["sync.namespace"] === namespace && meta["sync.id"] === "stale-ack" && !info.config.name.includes("D_")) {
            for await (const consumer of jsm.consumers.list(info.config.name)) {
              await jsm.consumers.delete(info.config.name, consumer.name).catch(() => {});
            }
          }
        }
      },
    );
    await jobs.submit({ key: "job-1", input: { runId: "1" } });
    await waitFor(() => ran >= 1, 15_000);
    await Bun.sleep(1_000);
    // Success stays success: no failure policy, no dead letter.
    expect(onErrorCalls).toEqual([]);
    expect(await jobs.deadLetters.list()).toHaveLength(0);
    worker.stop();
    await worker.drain({ timeoutMs: 1_000 });
  }, 30_000);
});
