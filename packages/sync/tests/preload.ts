/**
 * Test preload: block until the three-node cluster can place R3 streams.
 * A previous run (or an aborted one) may have restarted a node moments ago.
 */
import { waitForPlacementReady } from "./cluster.ts";

await waitForPlacementReady();
