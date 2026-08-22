import { PayloadTooLargeError, asError } from "./errors.ts";

// ==========================
// JSON envelope codec
// ==========================

export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

/**
 * The wire envelope shared by queue, job, topic, pump, and scheduler payloads.
 * Payload limits apply to the complete encoded envelope, not only `data`.
 */
export type Envelope = {
  v: 6;
  data: unknown;
  tenantId: string;
  publishedAt: string;
  orderingKey?: string;
  meta?: Record<string, JsonValue>;
  /** Primitive-specific extras (job key, schedule slot, ...). */
  ext?: Record<string, JsonValue>;
};

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export const encodeEnvelope = (label: string, envelope: Envelope, maxBytes: number): Uint8Array => {
  let json: string;
  try {
    json = JSON.stringify(envelope);
  } catch (error) {
    throw new TypeError(`${label} payload is not JSON-serializable: ${asError(error).message}`);
  }
  if (json === undefined) throw new TypeError(`${label} payload is not JSON-serializable`);
  const bytes = encoder.encode(json);
  if (bytes.byteLength > maxBytes) throw new PayloadTooLargeError(label, bytes.byteLength, maxBytes);
  return bytes;
};

export const decodeEnvelope = (bytes: Uint8Array): Envelope => {
  const parsed = JSON.parse(decoder.decode(bytes)) as Envelope;
  if (
    parsed === null ||
    typeof parsed !== "object" ||
    parsed.v !== 6 ||
    typeof parsed.tenantId !== "string" ||
    typeof parsed.publishedAt !== "string"
  ) {
    throw new TypeError("message is not a Sync v6 envelope");
  }
  return parsed;
};

/** Encode a plain JSON value with a byte limit (KV values, control records). */
export const encodeJson = (label: string, value: unknown, maxBytes: number): Uint8Array => {
  const json = JSON.stringify(value);
  if (json === undefined) throw new TypeError(`${label} is not JSON-serializable`);
  const bytes = encoder.encode(json);
  if (bytes.byteLength > maxBytes) throw new PayloadTooLargeError(label, bytes.byteLength, maxBytes);
  return bytes;
};

export const decodeJson = <T>(bytes: Uint8Array): T => JSON.parse(decoder.decode(bytes)) as T;

/** Typed reads of primitive-specific ext fields; wrong-typed values read as absent. */
export const extString = (envelope: Envelope | null, key: string): string | undefined => {
  const value = envelope?.ext?.[key];
  return typeof value === "string" ? value : undefined;
};

export const extNumber = (envelope: Envelope | null, key: string): number | undefined => {
  const value = envelope?.ext?.[key];
  return typeof value === "number" ? value : undefined;
};
