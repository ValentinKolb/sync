import { expect, test } from "bun:test";
import { fieldArrayToObject, parseFirstRangeEntry, parseFirstStreamEntry } from "../src/internal/topic-utils";

test("fieldArrayToObject converts alternating key/value arrays", () => {
  const fields = fieldArrayToObject(["payload", "{\"x\":1}", "n", 42]);
  expect(fields.payload).toBe("{\"x\":1}");
  expect(fields.n).toBe("42");
});

test("fieldArrayToObject skips non-string keys", () => {
  const fields = fieldArrayToObject([1, "x", "ok", "y"]);
  expect(fields.ok).toBe("y");
  expect(fields["1"]).toBeUndefined();
});

test("parseFirstStreamEntry parses Bun object-shaped stream responses", () => {
  const raw = {
    "topic:key": [["123-0", ["payload", "{\"value\":1}"]]],
  };

  const entry = parseFirstStreamEntry(raw);
  expect(entry?.id).toBe("123-0");
  expect(entry?.fields.payload).toBe("{\"value\":1}");
});

test("parseFirstStreamEntry parses array-shaped stream responses", () => {
  const raw = [["topic:key", [["999-1", ["payload", "{\"ok\":true}"]]]]];
  const entry = parseFirstStreamEntry(raw);

  expect(entry?.id).toBe("999-1");
  expect(entry?.fields.payload).toBe("{\"ok\":true}");
});

test("parseFirstStreamEntry returns null for invalid shapes", () => {
  expect(parseFirstStreamEntry(null)).toBeNull();
  expect(parseFirstStreamEntry({})).toBeNull();
  expect(parseFirstStreamEntry([])).toBeNull();
  expect(parseFirstStreamEntry([["k", []]])).toBeNull();
});

test("parseFirstRangeEntry parses Redis range responses", () => {
  const raw = [["123-0", ["payload", "{\"value\":1}"]]];
  const entry = parseFirstRangeEntry(raw);

  expect(entry?.id).toBe("123-0");
  expect(entry?.fields.payload).toBe("{\"value\":1}");
});

test("parseFirstRangeEntry returns null for invalid shapes", () => {
  expect(parseFirstRangeEntry(null)).toBeNull();
  expect(parseFirstRangeEntry({})).toBeNull();
  expect(parseFirstRangeEntry([])).toBeNull();
  expect(parseFirstRangeEntry([["k"]])).toBeNull();
});
