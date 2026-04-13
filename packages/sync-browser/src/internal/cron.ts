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

const MAX_LOOKAHEAD_MINUTES = 5 * 366 * 24 * 60;
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

const toZonedParts = (timestampMs: number, tz: string): { minute: number; hour: number; dayOfMonth: number; month: number; dayOfWeek: number } => {
  const parts = getZonedFormatter(tz).formatToParts(new Date(timestampMs));
  let minute = -1;
  let hour = -1;
  let dayOfMonth = -1;
  let month = -1;
  let dayOfWeek = -1;

  for (const part of parts) {
    if (part.type === "minute") minute = Number(part.value);
    else if (part.type === "hour") hour = Number(part.value);
    else if (part.type === "day") dayOfMonth = Number(part.value);
    else if (part.type === "month") month = Number(part.value);
    else if (part.type === "weekday") dayOfWeek = WEEKDAY_TO_NUM[part.value] ?? -1;
  }

  if (minute < 0 || hour < 0 || dayOfMonth < 0 || month < 0 || dayOfWeek < 0) {
    throw new Error(`failed to resolve zoned time parts for timezone "${tz}"`);
  }

  return { minute, hour, dayOfMonth, month, dayOfWeek };
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

const matchesCron = (spec: CronSpec, timestampMs: number, tz: string): boolean => {
  const p = toZonedParts(timestampMs, tz);
  return (
    matchesField(spec.minute, p.minute) &&
    matchesField(spec.hour, p.hour) &&
    matchesField(spec.month, p.month) &&
    matchesDay(spec, p.dayOfMonth, p.dayOfWeek)
  );
};

export const nextCronTimestamp = (cron: string, tz: string, afterTimestampMs: number): number => {
  const spec = parseCron(cron);
  let candidate = floorToMinute(afterTimestampMs) + 60_000;

  for (let i = 0; i < MAX_LOOKAHEAD_MINUTES; i++) {
    if (matchesCron(spec, candidate, tz)) return candidate;
    candidate += 60_000;
  }

  throw new Error(`unable to find next cron execution within lookahead window for "${cron}" (${tz})`);
};

export const assertValidTimeZone = (tz: string): void => {
  // Throws RangeError for invalid tz names.
  void getZonedFormatter(tz);
};
