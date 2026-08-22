import { describe, expect, test } from "bun:test";
import { assertValidTimeZone, nextCronTimestamp } from "../src/cron.ts";

const at = (iso: string): number => new Date(iso).getTime();

describe("cron parsing", () => {
  test("accepts standard five-field expressions", () => {
    expect(nextCronTimestamp("* * * * *", "UTC", at("2026-08-22T10:00:30Z"))).toBe(at("2026-08-22T10:01:00Z"));
    expect(nextCronTimestamp("0 3 * * *", "UTC", at("2026-08-22T10:00:00Z"))).toBe(at("2026-08-23T03:00:00Z"));
    expect(nextCronTimestamp("*/15 * * * *", "UTC", at("2026-08-22T10:07:00Z"))).toBe(at("2026-08-22T10:15:00Z"));
    expect(nextCronTimestamp("30 4 1 * *", "UTC", at("2026-08-22T10:00:00Z"))).toBe(at("2026-09-01T04:30:00Z"));
    expect(nextCronTimestamp("0 0 * * 1-5", "UTC", at("2026-08-22T10:00:00Z"))).toBe(at("2026-08-24T00:00:00Z"));
  });

  test("day-of-week 7 means Sunday and ranges may wrap", () => {
    // 2026-08-22 is a Saturday; next Sunday is the 23rd.
    expect(nextCronTimestamp("0 0 * * 7", "UTC", at("2026-08-22T10:00:00Z"))).toBe(at("2026-08-23T00:00:00Z"));
    expect(nextCronTimestamp("0 0 * * 5-7", "UTC", at("2026-08-24T10:00:00Z"))).toBe(at("2026-08-28T00:00:00Z"));
  });

  test("rejects malformed numerics instead of coercing them", () => {
    // Number("") === 0 would silently turn "-5" into the range 0-5.
    expect(() => nextCronTimestamp("-5 * * * *", "UTC", 0)).toThrow();
    expect(() => nextCronTimestamp("5- * * * *", "UTC", 0)).toThrow();
    expect(() => nextCronTimestamp("1e1 * * * *", "UTC", 0)).toThrow();
    expect(() => nextCronTimestamp("0x10 * * * *", "UTC", 0)).toThrow();
    expect(() => nextCronTimestamp("*/0 * * * *", "UTC", 0)).toThrow();
    expect(() => nextCronTimestamp("60 * * * *", "UTC", 0)).toThrow();
    expect(() => nextCronTimestamp("* 24 * * *", "UTC", 0)).toThrow();
  });

  test("rejects wrong field counts and garbage", () => {
    expect(() => nextCronTimestamp("not a cron", "UTC", 0)).toThrow();
    expect(() => nextCronTimestamp("", "UTC", 0)).toThrow();
    expect(() => nextCronTimestamp("* * * *", "UTC", 0)).toThrow();
    expect(() => nextCronTimestamp("0 * * * * *", "UTC", 0)).toThrow();
  });

  test("timezone handling: wall-clock schedules follow the zone", () => {
    // 03:00 Berlin in August is 01:00 UTC (CEST).
    expect(nextCronTimestamp("0 3 * * *", "Europe/Berlin", at("2026-08-22T10:00:00Z"))).toBe(
      at("2026-08-23T01:00:00Z"),
    );
    assertValidTimeZone("Europe/Berlin");
    expect(() => assertValidTimeZone("Mars/Olympus")).toThrow();
  });

  test("DST spring-forward skips the nonexistent hour", () => {
    // Europe/Berlin 2026: clocks jump 02:00 -> 03:00 on March 29.
    const next = nextCronTimestamp("30 2 * * *", "Europe/Berlin", at("2026-03-28T10:00:00Z"));
    // 02:30 does not exist on the 29th; the next real 02:30 wall time is March 30.
    expect(next).toBe(at("2026-03-30T00:30:00Z"));
  });
});
