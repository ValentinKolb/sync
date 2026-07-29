/** Generate a random UUID. */
export const randomId = (): string => crypto.randomUUID();

/** Generate a random hex string of the given byte length. */
export const randomHex = (bytes: number): string => {
  const buf = new Uint8Array(bytes);
  crypto.getRandomValues(buf);
  return Array.from(buf, (b) => b.toString(16).padStart(2, "0")).join("");
};

/** Module-scoped monotonic counter. */
let _seq = 0;
export const nextSeq = (): number => ++_seq;

/** Reset the sequence counter (for tests). */
export const resetSeq = (): void => {
  _seq = 0;
};

/**
 * Non-cryptographic 128-bit hash for long identifiers.
 *
 * This used to be a 32-bit djb2, where roughly 65 000 distinct long identifiers
 * make a collision likely — two users sharing one rate-limit bucket, or two
 * distinct resources sharing one mutex key. 128 bits puts that far out of
 * reach. It still differs from the server's SHA-256 digest, so the two runtimes
 * produce different *keys* for the same over-long identifier; that only matters
 * if both runtimes address one shared store, which they never do.
 */
export const simpleHash = (str: string): string => {
  // Four independently seeded FNV-1a lanes, each consuming the string with a
  // different rotation, concatenated into 32 hex characters.
  const seeds = [0x811c9dc5, 0x01000193, 0x9e3779b9, 0x85ebca6b];
  const lanes = seeds.map((seed, lane) => {
    let hash = seed >>> 0;
    for (let i = 0; i < str.length; i++) {
      hash ^= str.charCodeAt((i + lane) % str.length) + i;
      // FNV prime, via shifts to stay in 32-bit integer arithmetic.
      hash = (hash + ((hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24))) >>> 0;
    }
    return hash >>> 0;
  });
  return `hash:${lanes.map((lane) => lane.toString(16).padStart(8, "0")).join("")}`;
};
