/**
 * Standalone queue worker process used by fault tests. Reads its wiring from
 * environment variables, reports handler start/end over core NATS, and runs
 * until killed.
 *
 * Env: SYNC_NAMESPACE, SYNC_QUEUE_CONFIG (JSON), SYNC_REPORT_SUBJECT,
 *      SYNC_CONCURRENCY, SYNC_HANDLER_MS
 */
import { connect } from "@nats-io/transport-node";
import { createSync } from "../../../src/v6/sync.ts";

const nc = await connect({
  servers: ["nats://127.0.0.1:14222", "nats://127.0.0.1:14223", "nats://127.0.0.1:14224"],
  name: `fault-worker-${process.pid}`,
  ignoreClusterUpdates: true,
  maxReconnectAttempts: -1,
  reconnectTimeWait: 250,
});

const sync = createSync({
  connection: nc,
  namespace: process.env.SYNC_NAMESPACE!,
  application: `worker-${process.pid}`,
});

const queue = sync.queue<{ n: number }>({ owner: "tests", ...JSON.parse(process.env.SYNC_QUEUE_CONFIG!) });
const reportSubject = process.env.SYNC_REPORT_SUBJECT!;
const handlerMs = Number(process.env.SYNC_HANDLER_MS ?? 300);

await queue.process({ concurrency: Number(process.env.SYNC_CONCURRENCY ?? 4) }, async (message) => {
  nc.publish(reportSubject, JSON.stringify({ type: "start", pid: process.pid, n: message.data.n }));
  await Bun.sleep(handlerMs);
  nc.publish(reportSubject, JSON.stringify({ type: "end", pid: process.pid, n: message.data.n }));
  await nc.flush();
});

nc.publish(reportSubject, JSON.stringify({ type: "ready", pid: process.pid }));
await nc.flush();

// Run until the parent kills this process.
await new Promise(() => {});
