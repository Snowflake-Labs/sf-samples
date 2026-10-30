/**
 * Display formatters for Snowflake query results.
 *
 * The Node.js driver returns NUMBER as number (or string for large values),
 * TIMESTAMP as Date, and NULL as null. These helpers accept any of those and
 * always return a display string, using "—" for null/empty/invalid input.
 *
 * Numeric strings go through Number(), so integers above
 * Number.MAX_SAFE_INTEGER round. formatNumber keeps them exact; the compact,
 * currency and percent formatters are approximate by design.
 */

export const EMPTY = "—"

function toNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null
  const n = typeof value === "number" ? value : Number(value)
  return Number.isFinite(n) ? n : null
}

/** 1234567.891 → "1,234,567.89". Large integer strings stay exact. */
export function formatNumber(value: unknown, maximumFractionDigits = 2): string {
  if (typeof value === "string" && /^-?\d+$/.test(value.trim()) && !Number.isSafeInteger(Number(value))) {
    return BigInt(value.trim()).toLocaleString("en-US")
  }
  const n = toNumber(value)
  return n === null ? EMPTY : n.toLocaleString("en-US", { maximumFractionDigits })
}

/** 1234567 → "1.2M", 1500 → "1.5K" */
export function formatCompact(value: unknown): string {
  const n = toNumber(value)
  return n === null
    ? EMPTY
    : n.toLocaleString("en-US", { notation: "compact", maximumFractionDigits: 1 })
}

/** 1234.5 → "$1,234.50" */
export function formatCurrency(value: unknown, currency = "USD"): string {
  const n = toNumber(value)
  return n === null ? EMPTY : n.toLocaleString("en-US", { style: "currency", currency })
}

/** Ratio in, percent out: 0.1234 → "12.3%" */
export function formatPercent(value: unknown, maximumFractionDigits = 1): string {
  const n = toNumber(value)
  return n === null
    ? EMPTY
    : n.toLocaleString("en-US", { style: "percent", maximumFractionDigits })
}

function toDate(value: unknown): Date | null {
  if (value === null || value === undefined || value === "") return null
  const d = value instanceof Date ? value : new Date(value as string | number)
  return Number.isNaN(d.getTime()) ? null : d
}

/** → "Oct 23, 2026" */
export function formatDate(value: unknown): string {
  const d = toDate(value)
  return d === null ? EMPTY : d.toLocaleDateString("en-US", { dateStyle: "medium" })
}

/** → "Oct 23, 2026, 10:50:46 AM" */
export function formatDateTime(value: unknown): string {
  const d = toDate(value)
  return d === null
    ? EMPTY
    : d.toLocaleString("en-US", { dateStyle: "medium", timeStyle: "medium" })
}

/** Generic cell formatter for rows of unknown shape. */
export function formatValue(value: unknown): string {
  if (value === null || value === undefined || value === "") return EMPTY
  if (value instanceof Date) return formatDateTime(value)
  if (typeof value === "number") return formatNumber(value)
  if (typeof value === "boolean") return value ? "true" : "false"
  if (typeof value === "object") return JSON.stringify(value)
  return String(value)
}

/** True for values that should be right-aligned in a table column. */
export function isNumeric(value: unknown): boolean {
  return typeof value === "number" || (typeof value === "string" && value.trim() !== "" && Number.isFinite(Number(value)))
}

/** Milliseconds in, short duration out: 850 → "850 ms", 4200 → "4.2 s", 125000 → "2m 5s" */
export function formatDuration(ms: unknown): string {
  const n = toNumber(ms)
  if (n === null || n < 0) return EMPTY
  if (n < 1000) return `${Math.round(n)} ms`
  if (n < 60_000) return `${(n / 1000).toFixed(1)} s`
  const minutes = Math.floor(n / 60_000)
  if (minutes < 60) return `${minutes}m ${Math.round((n % 60_000) / 1000)}s`
  return `${Math.floor(minutes / 60)}h ${minutes % 60}m`
}

/** 1536 → "1.5 KB" */
export function formatBytes(bytes: unknown): string {
  const n = toNumber(bytes)
  if (n === null || n < 0) return EMPTY
  const units = ["B", "KB", "MB", "GB", "TB", "PB"]
  let v = n
  let i = 0
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024
    i++
  }
  return `${v.toLocaleString("en-US", { maximumFractionDigits: i === 0 ? 0 : 1 })} ${units[i]}`
}

/** Time since a date: "just now", "5 min ago", "3 h ago", "2 d ago". Future dates: "in 5 min", then a date. */
export function formatRelative(value: unknown, now: Date = new Date()): string {
  const d = toDate(value)
  if (d === null) return EMPTY
  const s = Math.round((now.getTime() - d.getTime()) / 1000)
  if (s < -45) {
    if (s > -3600) return `in ${Math.round(-s / 60)} min`
    if (s > -86_400) return `in ${Math.round(-s / 3600)} h`
    return formatDate(d)
  }
  if (s < 45) return "just now"
  if (s < 3600) return `${Math.round(s / 60)} min ago`
  if (s < 86_400) return `${Math.round(s / 3600)} h ago`
  return `${Math.round(s / 86_400)} d ago`
}
