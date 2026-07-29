import { createMemoryStore, type Store } from "../store";

/**
 * Coordination scope for the browser primitives.
 *
 * On the server every module is keyed purely by `{prefix}:{id}` in Redis, so two
 * factory calls with the same id always share state. In the browser each factory
 * used to allocate its own map, or its own MemoryStore, so `mutex({id:"x"})` in
 * two modules produced two independent locks — mutual exclusion silently gone,
 * failing open — and `queue({id:"emails"}).send()` in a producer module was
 * invisible to `queue({id:"emails"}).recv()` in a consumer module.
 *
 * The rule now matches what README.md always claimed: handles that do not name a
 * Store share one process-wide namespace, and a handle given an explicit Store
 * coordinates only with other handles given that same Store. Keys passed here
 * are already fully qualified with prefix, tenant and id, so distinct
 * primitives cannot collide inside one bucket.
 */
const defaultStore = createMemoryStore();

const defaultBucket = new Map<string, unknown>();
const bucketsByStore = new WeakMap<Store, Map<string, unknown>>();

/** The Store a handle should use when it did not bring its own. */
export const resolveStore = (store?: Store): Store => store ?? defaultStore;

const bucketFor = (store?: Store): Map<string, unknown> => {
  if (!store || store === defaultStore) return defaultBucket;
  let bucket = bucketsByStore.get(store);
  if (!bucket) {
    bucket = new Map<string, unknown>();
    bucketsByStore.set(store, bucket);
  }
  return bucket;
};

/**
 * Fetch the state shared by every handle in this coordination scope, creating it
 * on first use. Entries are keyed by a caller-supplied fully-qualified string
 * and live as long as their scope: the default bucket for the process, a
 * per-Store bucket only as long as that Store is reachable.
 */
export const sharedState = <S>(key: string, store: Store | undefined, create: () => S): S => {
  const bucket = bucketFor(store);
  const existing = bucket.get(key);
  if (existing !== undefined) return existing as S;
  const created = create();
  bucket.set(key, created);
  return created;
};
