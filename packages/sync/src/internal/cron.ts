export type MisfirePolicy = "skip" | "catch_up_one" | "catch_up_all";

type CronField = {
  any: boolean;
  values: Set<number>;
};

type CronSpec = {
  minute: CronField;
  hour: CronField;
  dayOfMonth: CronField;
  month: CronField;
  dayOfWeek: CronField;
};

// Eight years covers the worst legitimate gap, "0 0 29 2 *" across a skipped
// leap year (2096 -> 2104). Reachable cheaply because non-matching days are
// skipped a day at a time rather than a minute at a time.
const MAX_LOOKAHEAD_MS = 8 * 366 * 24 * 60 * 60_000;
const MAX_LOOKAHEAD_STEPS = 200_000;
const DAYS_IN_MONTH = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
const WEEKDAY_TO_NUM: Record<string, number> = {
  Sun: 0,
  Mon: 1,
  Tue: 2,
  Wed: 3,
  Thu: 4,
  Fri: 5,
  Sat: 6,
};

const zonedFormatterCache = new Map<string, Intl.DateTimeFormat>();
const parsedCronCache = new Map<string, CronSpec>();

const getZonedFormatter = (tz: string): Intl.DateTimeFormat => {
  const cached = zonedFormatterCache.get(tz);
  if (cached) return cached;
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone: tz,
    hour12: false,
    hourCycle: "h23",
    minute: "2-digit",
    hour: "2-digit",
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    weekday: "short",
  });
  zonedFormatterCache.set(tz, formatter);
  return formatter;
};

const asInt = (value: string, fieldName: string): number => {
  const parsed = Number(value);
  if (!Number.isInteger(parsed)) {
    throw new Error(`invalid ${fieldName} value: ${value}`);
  }
  return parsed;
};

const normalizeDow = (value: number): number => {
  if (value === 7) return 0;
  return value;
};

const parseSegment = (segment: string, min: number, max: number, fieldName: string): number[] => {
  const [base, stepRaw] = segment.split("/");
  const step = stepRaw === undefined ? 1 : asInt(stepRaw, `${fieldName} step`);
  if (step <= 0) throw new Error(`invalid ${fieldName} step: ${segment}`);

  const resolvedBase = base ?? "*";

  let start = min;
  let end = max;

  if (resolvedBase !== "*") {
    if (resolvedBase.includes("-")) {
      const [a, b] = resolvedBase.split("-");
      if (a === undefined || b === undefined) throw new Error(`invalid ${fieldName} range: ${segment}`);
      start = asInt(a, fieldName);
      end = asInt(b, fieldName);
    } else {
      start = asInt(resolvedBase, fieldName);
      end = start;
    }
  }

  if (fieldName === "day-of-week") {
    const rawStart = start;
    const rawEnd = end;
    start = normalizeDow(start);
    end = normalizeDow(end);

    // Allow common cron notation like 1-7 (Mon-Sun), which wraps after normalization to 1-0.
    if (rawStart !== rawEnd && start > end) {
      const wrappedValues: number[] = [];
      for (let v = start; v <= max; v += step) wrappedValues.push(v);
      for (let v = min; v <= end; v += step) wrappedValues.push(v);
      return wrappedValues;
    }
  }

  if (start < min || start > max || end < min || end > max || start > end) {
    throw new Error(`out-of-range ${fieldName}: ${segment}`);
  }

  const values: number[] = [];
  for (let v = start; v <= end; v += step) values.push(v);
  return values;
};

const parseField = (raw: string, min: number, max: number, fieldName: string): CronField => {
  if (raw === "*") return { any: true, values: new Set<number>() };
  const values = new Set<number>();
  for (const segment of raw.split(",")) {
    const trimmed = segment.trim();
    if (!trimmed) throw new Error(`invalid ${fieldName} field: "${raw}"`);
    for (const v of parseSegment(trimmed, min, max, fieldName)) {
      values.add(fieldName === "day-of-week" ? normalizeDow(v) : v);
    }
  }
  if (values.size === 0) throw new Error(`invalid ${fieldName} field: "${raw}"`);
  return { any: false, values };
};

/**
 * Reject a day/month pair no calendar can produce, such as "0 0 30 2 *". Both
 * fields parse cleanly on their own, so the expression used to be accepted and
 * then scanned minute by minute across the whole lookahead window before
 * failing. Only checked when day-of-week is unrestricted, because otherwise
 * day-of-month and day-of-week combine with OR and the month is still reachable.
 */
const assertReachableDay = (spec: CronSpec, raw: string): void => {
  if (spec.dayOfMonth.any || spec.month.any || !spec.dayOfWeek.any) return;
  for (const month of spec.month.values) {
    const maxDay = DAYS_IN_MONTH[month - 1] ?? 31;
    for (const day of spec.dayOfMonth.values) {
      if (day <= maxDay) return;
    }
  }
  throw new Error(`cron day-of-month never occurs in the selected month(s): "${raw}"`);
};

const parseCron = (raw: string): CronSpec => {
  const cached = parsedCronCache.get(raw);
  if (cached) return cached;

  const parts = raw.trim().split(/\s+/);
  if (parts.length !== 5) {
    throw new Error(`cron must have 5 fields (minute hour day-of-month month day-of-week), got: "${raw}"`);
  }
  const [minuteRaw, hourRaw, dayOfMonthRaw, monthRaw, dayOfWeekRaw] = parts as [string, string, string, string, string];
  const spec: CronSpec = {
    minute: parseField(minuteRaw, 0, 59, "minute"),
    hour: parseField(hourRaw, 0, 23, "hour"),
    dayOfMonth: parseField(dayOfMonthRaw, 1, 31, "day-of-month"),
    month: parseField(monthRaw, 1, 12, "month"),
    dayOfWeek: parseField(dayOfWeekRaw, 0, 7, "day-of-week"),
  };
  assertReachableDay(spec, raw);
  parsedCronCache.set(raw, spec);
  // Small bound to avoid unbounded growth if callers use many dynamic cron strings.
  if (parsedCronCache.size > 1_000) {
    const firstKey = parsedCronCache.keys().next().value;
    if (firstKey) parsedCronCache.delete(firstKey);
  }
  return spec;
};

const floorToMinute = (timestampMs: number): number => {
  return Math.floor(timestampMs / 60_000) * 60_000;
};

type ZonedParts = { minute: number; hour: number; dayOfMonth: number; month: number; year: number; dayOfWeek: number };

/**
 * A comparable key for a local wall-clock minute. Two distinct UTC instants can
 * render to the same key during a DST fall-back; that is exactly what makes it
 * useful.
 */
const localSlotKey = (p: ZonedParts): number =>
  ((p.year * 100 + p.month) * 100 + p.dayOfMonth) * 10000 + p.hour * 100 + p.minute;

const toZonedParts = (timestampMs: number, tz: string): ZonedParts => {
  const parts = getZonedFormatter(tz).formatToParts(new Date(timestampMs));
  let minute = -1;
  let hour = -1;
  let dayOfMonth = -1;
  let month = -1;
  let year = -1;
  let dayOfWeek = -1;

  for (const part of parts) {
    if (part.type === "minute") minute = Number(part.value);
    else if (part.type === "hour") hour = Number(part.value);
    else if (part.type === "day") dayOfMonth = Number(part.value);
    else if (part.type === "month") month = Number(part.value);
    else if (part.type === "year") year = Number(part.value);
    else if (part.type === "weekday") dayOfWeek = WEEKDAY_TO_NUM[part.value] ?? -1;
  }

  if (minute < 0 || hour < 0 || dayOfMonth < 0 || month < 0 || year < 0 || dayOfWeek < 0) {
    throw new Error(`failed to resolve zoned time parts for timezone "${tz}"`);
  }

  return { minute, hour, dayOfMonth, month, year, dayOfWeek };
};

const matchesField = (field: CronField, value: number): boolean => field.any || field.values.has(value);

const matchesDay = (spec: CronSpec, dayOfMonth: number, dayOfWeek: number): boolean => {
  const domAny = spec.dayOfMonth.any;
  const dowAny = spec.dayOfWeek.any;
  const domMatches = domAny || spec.dayOfMonth.values.has(dayOfMonth);
  const dowMatches = dowAny || spec.dayOfWeek.values.has(dayOfWeek);

  if (domAny && dowAny) return true;
  if (domAny) return dowMatches;
  if (dowAny) return domMatches;
  return domMatches || dowMatches;
};

const matchesDayParts = (spec: CronSpec, p: ZonedParts): boolean =>
  matchesField(spec.month, p.month) && matchesDay(spec, p.dayOfMonth, p.dayOfWeek);

const matchesCron = (spec: CronSpec, timestampMs: number, tz: string): boolean => {
  const p = toZonedParts(timestampMs, tz);
  return matchesField(spec.minute, p.minute) && matchesField(spec.hour, p.hour) && matchesDayParts(spec, p);
};

/**
 * The next instant matching `cron`, strictly after `afterTimestampMs`.
 *
 * A candidate is required to fall on a *later local wall-clock minute* than the
 * one `afterTimestampMs` sits in, so each local slot fires at most once. Without
 * that, a DST fall-back made two distinct UTC minutes render to the same local
 * minute and every affected schedule fired twice, once a year, deterministically.
 * The symmetric case still applies: a local slot that a spring-forward skips
 * does not exist that day and does not run.
 */
export const nextCronTimestamp = (cron: string, tz: string, afterTimestampMs: number): number => {
  const spec = parseCron(cron);
  const startMs = floorToMinute(afterTimestampMs);
  const afterSlot = localSlotKey(toZonedParts(startMs, tz));
  const horizonMs = startMs + MAX_LOOKAHEAD_MS;

  let candidate = startMs + 60_000;

  for (let step = 0; step < MAX_LOOKAHEAD_STEPS && candidate <= horizonMs; step++) {
    const p = toZonedParts(candidate, tz);

    if (!matchesDayParts(spec, p)) {
      // No minute of this local day can match, so skip the rest of it in one
      // jump. Stop an hour short of local midnight so a DST shift cannot carry
      // the jump past the start of the next day.
      const minutesLeftInDay = (24 - p.hour) * 60 - p.minute;
      candidate += Math.max(1, minutesLeftInDay - 60) * 60_000;
      continue;
    }

    if (matchesField(spec.minute, p.minute) && matchesField(spec.hour, p.hour) && localSlotKey(p) > afterSlot) {
      return candidate;
    }
    candidate += 60_000;
  }

  throw new Error(`unable to find next cron execution within lookahead window for "${cron}" (${tz})`);
};

export const assertValidTimeZone = (tz: string): void => {
  // Throws RangeError for invalid tz names.
  void getZonedFormatter(tz);
};
