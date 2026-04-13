export type RawFields = Record<string, string>;

export type ParsedEntry = {
  id: string;
  fields: RawFields;
};

export const fieldArrayToObject = (value: unknown): RawFields => {
  if (!Array.isArray(value)) return {};
  const out: RawFields = {};
  for (let i = 0; i < value.length; i += 2) {
    const key = value[i];
    const raw = value[i + 1];
    if (typeof key !== "string") continue;
    out[key] = typeof raw === "string" ? raw : String(raw ?? "");
  }
  return out;
};

export const parseFirstStreamEntry = (raw: unknown): ParsedEntry | null => {
  if (!raw) return null;

  const objectResponse =
    typeof raw === "object" && !Array.isArray(raw)
      ? (raw as Record<string, Array<[string, unknown]>>)
      : null;

  if (objectResponse) {
    const firstStream = Object.values(objectResponse)[0];
    if (!Array.isArray(firstStream) || firstStream.length === 0) return null;
    const firstEntry = firstStream[0];
    if (!Array.isArray(firstEntry) || firstEntry.length < 2) return null;

    const id = firstEntry[0];
    const fieldArray = firstEntry[1];
    if (typeof id !== "string") return null;

    return {
      id,
      fields: fieldArrayToObject(fieldArray),
    };
  }

  if (!Array.isArray(raw) || raw.length === 0) return null;

  const firstStream = raw[0];
  if (!Array.isArray(firstStream) || firstStream.length < 2) return null;

  const entries = firstStream[1];
  if (!Array.isArray(entries) || entries.length === 0) return null;

  const firstEntry = entries[0];
  if (!Array.isArray(firstEntry) || firstEntry.length < 2) return null;

  const id = firstEntry[0];
  const fieldArray = firstEntry[1];
  if (typeof id !== "string") return null;

  return {
    id,
    fields: fieldArrayToObject(fieldArray),
  };
};

