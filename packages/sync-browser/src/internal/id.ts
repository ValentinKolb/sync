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

/** Simple non-cryptographic hash for long strings (djb2). */
export const simpleHash = (str: string): string => {
  let hash = 5381;
  for (let i = 0; i < str.length; i++) {
    hash = ((hash << 5) + hash + str.charCodeAt(i)) | 0;
  }
  return `hash:${(hash >>> 0).toString(16)}`;
};
