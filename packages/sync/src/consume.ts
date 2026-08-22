import type { Consumer, JsMsg } from "@nats-io/jetstream";
import type { EventHub } from "./events.ts";
import { StaleDeliveryError, asError } from "./errors.ts";
import type { WorkerRuntime } from "./worker.ts";

// ==========================
// Shared pull loop
// ==========================

/**
 * Pulls messages for exactly the currently free local handler slots. Sync
 * never fetches a hidden batch beyond free capacity, so unclaimed work stays
 * on the server for other pods.
 */
export const runPullLoop = async (
  wr: WorkerRuntime,
  getConsumer: () => Promise<Consumer>,
  onMessage: (msg: JsMsg, signal: AbortSignal) => Promise<void>,
  options: { events: EventHub; resource: string },
): Promise<void> => {
  while (!wr.stopping) {
    const free = wr.freeSlots();
    if (free === 0) {
      await wr.waitForSlot();
      continue;
    }
    try {
      const consumer = await getConsumer();
      const batch = await consumer.fetch({ max_messages: free, expires: 5_000 });
      const stopBatch = (): void => batch.stop();
      wr.stopped.then(stopBatch);
      for await (const msg of batch) {
        wr.track((signal) => onMessage(msg, signal));
      }
    } catch (error) {
      if (wr.stopping) return;
      options.events.emit({
        type: "handler_error",
        resource: options.resource,
        error: `pull loop: ${asError(error).message}`,
      });
      // Transient transport/consumer failure: back off briefly and retry.
      await Promise.race([Bun.sleep(1_000), wr.stopped]);
    }
  }
};

// ==========================
// Confirmed settlement helpers
// ==========================

/** Confirmed ack; throws StaleDeliveryError when NATS no longer accepts the token. */
export const confirmedAck = async (msg: JsMsg, resource: string): Promise<void> => {
  try {
    const accepted = await msg.ackAck();
    if (!accepted) throw new StaleDeliveryError(`${resource}: delivery ${msg.seq} was already settled or expired`);
  } catch (error) {
    if (error instanceof StaleDeliveryError) throw error;
    throw new StaleDeliveryError(`${resource}: ack for delivery ${msg.seq} failed: ${asError(error).message}`);
  }
};
