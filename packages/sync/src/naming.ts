import { createHash } from "node:crypto";
import { InvalidNameError } from "./errors.ts";

// ==========================
// Constants
// ==========================

/** Maximum bytes for user-supplied ids, tenant ids, keys, and consumer names. */
const MAX_NAME_BYTES = 96;
/** Maximum characters for a complete encoded NATS subject or KV key. */
const MAX_SUBJECT_CHARS = 255;

const BASE32_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
const encoder = new TextEncoder();

// ==========================
// Encoding helpers
// ==========================

const base32 = (bytes: Uint8Array): string => {
  let bits = 0;
  let value = 0;
  let out = "";
  for (const byte of bytes) {
    value = (value << 8) | byte;
    bits += 8;
    while (bits >= 5) {
      out += BASE32_ALPHABET[(value >>> (bits - 5)) & 31];
      bits -= 5;
    }
  }
  if (bits > 0) out += BASE32_ALPHABET[(value << (5 - bits)) & 31];
  return out;
};

const sha256 = (input: string): Uint8Array => new Uint8Array(createHash("sha256").update(input, "utf8").digest());

const sha256Hex = (input: string): string => createHash("sha256").update(input, "utf8").digest("hex");

/**
 * Injective base64url (no padding) encoding for user-supplied values used as
 * NATS subject tokens or KV key tokens. The result contains only [A-Za-z0-9_-].
 */
export const subjectToken = (value: string, label: string): string => {
  assertName(value, label);
  return Buffer.from(value, "utf8").toString("base64url");
};

export const decodeSubjectToken = (token: string): string => Buffer.from(token, "base64url").toString("utf8");

export const assertName = (value: string, label: string): void => {
  if (typeof value !== "string" || value.length === 0) {
    throw new InvalidNameError(`${label} must be a non-empty string`);
  }
  const bytes = encoder.encode(value).length;
  if (bytes > MAX_NAME_BYTES) {
    throw new InvalidNameError(`${label} is ${bytes} bytes; the maximum is ${MAX_NAME_BYTES} bytes`);
  }
};

export const assertSubjectLength = (subject: string): void => {
  if (subject.length > MAX_SUBJECT_CHARS) {
    throw new InvalidNameError(
      `encoded subject or key is ${subject.length} characters; the safe maximum is ${MAX_SUBJECT_CHARS}`,
    );
  }
};

// ==========================
// Resource identity
// ==========================

export type SyncResourceKind =
  | "topic"
  | "queue"
  | "job"
  | "pump"
  | "scheduler"
  | "mutex"
  | "ephemeral"
  | "object-store";

export type ResourceIdentity = {
  namespace: string;
  kind: SyncResourceKind;
  id: string;
  /** Full SHA-256 hex of the identity — stored in resource metadata. */
  identitySha256: string;
  /** First 20 base32 characters — used in NATS resource names. */
  hash: string;
  /** Lower-case hash for subject tokens. */
  subjectHash: string;
  /** Namespace hash (12 base32 characters). */
  nsHash: string;
  subjectNsHash: string;
};

export const resourceIdentity = (namespace: string, kind: SyncResourceKind, id: string): ResourceIdentity => {
  assertName(namespace, "namespace");
  assertName(id, `${kind} id`);
  const identity = JSON.stringify([namespace, kind, id]);
  const digest = sha256(identity);
  const hash = base32(digest).slice(0, 20);
  const nsHash = base32(sha256(namespace)).slice(0, 12);
  return {
    namespace,
    kind,
    id,
    identitySha256: sha256Hex(identity),
    hash,
    subjectHash: hash.toLowerCase(),
    nsHash,
    subjectNsHash: nsHash.toLowerCase(),
  };
};

// ==========================
// Physical names
// ==========================

const KIND_PREFIX: Record<SyncResourceKind, string> = {
  topic: "T",
  queue: "Q",
  job: "J",
  pump: "P",
  scheduler: "S",
  mutex: "M",
  ephemeral: "E",
  "object-store": "O",
};

export const streamName = (identity: ResourceIdentity): string => `S6_${KIND_PREFIX[identity.kind]}_${identity.hash}`;

export const dlqStreamName = (identity: ResourceIdentity): string =>
  `S6_${KIND_PREFIX[identity.kind]}D_${identity.hash}`;

/** KV bucket name (without the NATS `KV_` prefix, which Kvm adds). */
export const kvBucketName = (identity: ResourceIdentity): string => `S6_${KIND_PREFIX[identity.kind]}_${identity.hash}`;

/** Object store bucket name (without the NATS `OBJ_` prefix, which Objm adds). */
export const objBucketName = (identity: ResourceIdentity): string => `S6_O_${identity.hash}`;

export const consumerName = (identity: ResourceIdentity, consumer: string): string => {
  const consumerHash = base32(sha256(consumer)).slice(0, 12);
  return `S6_${KIND_PREFIX[identity.kind]}C_${identity.hash}_${consumerHash}`;
};

/** Subject root for a resource: `sync.v6.<nsHash>.<kind>.<hash>`. */
export const subjectRoot = (identity: ResourceIdentity): string => {
  const kindToken = identity.kind === "object-store" ? "obj" : identity.kind;
  return `sync.v6.${identity.subjectNsHash}.${kindToken}.${identity.subjectHash}`;
};

// ==========================
// Resource metadata
// ==========================

// fallow-ignore-next-line unused-type -- part of exported signatures; required for declaration emit
export type ResourceMetadata = {
  "sync.api": "6";
  "sync.namespace": string;
  "sync.kind": SyncResourceKind;
  "sync.id": string;
  "sync.owner": string;
  "sync.identity_sha256": string;
  "sync.managed": "true";
};

export const resourceMetadata = (identity: ResourceIdentity, owner: string): ResourceMetadata => ({
  "sync.api": "6",
  "sync.namespace": identity.namespace,
  "sync.kind": identity.kind,
  "sync.id": identity.id,
  "sync.owner": owner,
  "sync.identity_sha256": identity.identitySha256,
  "sync.managed": "true",
});
