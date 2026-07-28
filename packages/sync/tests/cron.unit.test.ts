import { expect, test } from "bun:test";
import { assertValidTimeZone, nextCronTimestamp } from "../src/internal/cron";

// `cron.ts` is byte-identical in both packages, so these cover the browser too.

const at = (iso: string): number => Date.parse(iso);

const localParts = (ts: number, tz: string): string =>
  new Intl.DateTimeFormat("en-CA", {
    timeZone: tz,
    hour12: false,
    hourCycle: "h23",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(ts));

// ==========================
// Basic advancement
// ==========================

test("advances to the next matching minute", () => {
  const next = nextCronTimestamp("*/15 * * * *", "UTC", at("2026-03-10T10:02:00Z"));
  expect(next).toBe(at("2026-03-10T10:15:00Z"));
});

test("advances to the next matching day", () => {
  const next = nextCronTimestamp("30 1 * * *", "UTC", at("2026-03-10T10:02:00Z"));
  expect(next).toBe(at("2026-03-11T01:30:00Z"));
});

test("honours the timezone rather than UTC", () => {
  const next = nextCronTimestamp("30 1 * * *", "America/New_York", at("2026-03-10T10:02:00Z"));
  expect(localParts(next, "America/New_York")).toBe("2026-03-11, 01:30");
});

// ==========================
// DST
// ==========================

test("a fall-back repeated local hour fires once, not twice", () => {
  const tz = "America/New_York";
  // 2026-11-01: 01:00-02:00 local occurs twice (05:00Z and 06:00Z).
  const firstRun = at("2026-11-01T05:30:00Z");
  expect(localParts(firstRun, tz)).toBe("2026-11-01, 01:30");

  // The repeated 01:30 local at 06:30Z must not be selected again.
  const next = nextCronTimestamp("30 1 * * *", tz, firstRun);
  expect(next).not.toBe(at("2026-11-01T06:30:00Z"));
  expect(localParts(next, tz)).toBe("2026-11-02, 01:30");
});

test("a fall-back does not double-fire a frequent schedule either", () => {
  const tz = "America/New_York";
  let cursor = at("2026-11-01T04:45:00Z");
  const seen: string[] = [];

  // Walk through the whole ambiguous window the way the scheduler does.
  for (let i = 0; i < 12; i++) {
    cursor = nextCronTimestamp("*/15 * * * *", tz, cursor);
    seen.push(localParts(cursor, tz));
  }

  expect(new Set(seen).size).toBe(seen.length);
});

test("a spring-forward slot that does not exist locally is skipped, not doubled", () => {
  const tz = "America/New_York";
  // 2026-03-08: local 02:00-03:00 does not exist.
  const next = nextCronTimestamp("30 2 * * *", tz, at("2026-03-07T12:00:00Z"));
  expect(localParts(next, tz)).toBe("2026-03-09, 02:30");
});

// ==========================
// Impossible and expensive expressions
// ==========================

test("a day/month pair no calendar produces is rejected at parse time", () => {
  expect(() => nextCronTimestamp("0 0 30 2 *", "UTC", Date.now())).toThrow(/never occurs/);
  expect(() => nextCronTimestamp("0 0 31 4 *", "UTC", Date.now())).toThrow(/never occurs/);
});

test("a rejected expression is rejected promptly, without scanning the window", () => {
  const startedAt = Date.now();
  expect(() => nextCronTimestamp("0 0 31 2,4,6 *", "UTC", Date.now())).toThrow(/never occurs/);
  // The old scan ran ~2.6M Intl.formatToParts calls before giving up.
  expect(Date.now() - startedAt).toBeLessThan(250);
});

test("day-of-week rescues an otherwise impossible day-of-month", () => {
  // day-of-month and day-of-week combine with OR, so this is genuinely valid.
  const next = nextCronTimestamp("0 0 30 2 1", "UTC", at("2026-02-01T00:00:00Z"));
  expect(new Date(next).getUTCDay()).toBe(1);
});

test("a rare but reachable slot is still found", () => {
  const next = nextCronTimestamp("0 0 29 2 *", "UTC", at("2026-03-01T00:00:00Z"));
  expect(new Date(next).toISOString()).toBe("2028-02-29T00:00:00.000Z");
});

test("a leap-day schedule is found promptly", () => {
  const startedAt = Date.now();
  nextCronTimestamp("0 0 29 2 *", "UTC", at("2026-03-01T00:00:00Z"));
  expect(Date.now() - startedAt).toBeLessThan(500);
});

// ==========================
// Validation
// ==========================

test("invalid fields and timezones throw", () => {
  expect(() => nextCronTimestamp("bad", "UTC", Date.now())).toThrow();
  expect(() => nextCronTimestamp("60 * * * *", "UTC", Date.now())).toThrow();
  expect(() => assertValidTimeZone("Not/AZone")).toThrow();
  expect(() => assertValidTimeZone("Europe/Berlin")).not.toThrow();
});
