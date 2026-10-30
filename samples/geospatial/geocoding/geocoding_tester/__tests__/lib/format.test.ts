import { describe, expect, it } from "vitest"
import {
  EMPTY,
  formatBytes,
  formatCompact,
  formatCurrency,
  formatDate,
  formatDateTime,
  formatDuration,
  formatNumber,
  formatPercent,
  formatRelative,
  formatValue,
  isNumeric,
} from "@/lib/format"

describe("format", () => {
  it("formats numbers and numeric strings", () => {
    expect(formatNumber(1234567.891)).toBe("1,234,567.89")
    expect(formatNumber("42")).toBe("42")
    expect(formatNumber(null)).toBe(EMPTY)
    expect(formatNumber("abc")).toBe(EMPTY)
  })

  it("keeps large integer strings exact", () => {
    expect(formatNumber("9007199254740993")).toBe("9,007,199,254,740,993")
    expect(formatNumber("-9007199254740993")).toBe("-9,007,199,254,740,993")
  })

  it("formats compact, currency, percent", () => {
    expect(formatCompact(1_234_567)).toBe("1.2M")
    expect(formatCompact(1500)).toBe("1.5K")
    expect(formatCurrency(1234.5)).toBe("$1,234.50")
    expect(formatPercent(0.1234)).toBe("12.3%")
    expect(formatPercent(undefined)).toBe(EMPTY)
  })

  it("formats dates from Date, ISO string, and bad input", () => {
    const d = new Date(2026, 9, 23, 10, 50, 46)
    expect(formatDate(d)).toBe("Oct 23, 2026")
    expect(formatDateTime(d)).toContain("Oct 23, 2026")
    expect(formatDate(d.toISOString())).toBe("Oct 23, 2026")
    expect(formatDate("not a date")).toBe(EMPTY)
  })

  it("formats unknown values", () => {
    expect(formatValue(null)).toBe(EMPTY)
    expect(formatValue("")).toBe(EMPTY)
    expect(formatValue(1000)).toBe("1,000")
    expect(formatValue(true)).toBe("true")
    expect(formatValue({ a: 1 })).toBe('{"a":1}')
    expect(formatValue("text")).toBe("text")
  })

  it("detects numeric values", () => {
    expect(isNumeric(1)).toBe(true)
    expect(isNumeric("3.5")).toBe(true)
    expect(isNumeric(" ")).toBe(false)
    expect(isNumeric("abc")).toBe(false)
    expect(isNumeric(null)).toBe(false)
  })
})

describe("format durations, bytes, relative time", () => {
  it("formats durations", () => {
    expect(formatDuration(850)).toBe("850 ms")
    expect(formatDuration(4200)).toBe("4.2 s")
    expect(formatDuration(125_000)).toBe("2m 5s")
    expect(formatDuration(3_900_000)).toBe("1h 5m")
    expect(formatDuration(null)).toBe(EMPTY)
  })

  it("formats bytes", () => {
    expect(formatBytes(0)).toBe("0 B")
    expect(formatBytes(1536)).toBe("1.5 KB")
    expect(formatBytes(5 * 1024 ** 3)).toBe("5 GB")
  })

  it("formats relative time", () => {
    const now = new Date("2026-10-23T12:00:00Z")
    expect(formatRelative("2026-10-23T11:59:50Z", now)).toBe("just now")
    expect(formatRelative("2026-10-23T11:55:00Z", now)).toBe("5 min ago")
    expect(formatRelative("2026-10-23T09:00:00Z", now)).toBe("3 h ago")
    expect(formatRelative("2026-10-21T12:00:00Z", now)).toBe("2 d ago")
  })

  it("formats future relative time", () => {
    const now = new Date("2026-10-23T12:00:00Z")
    expect(formatRelative("2026-10-23T12:00:30Z", now)).toBe("just now")
    expect(formatRelative("2026-10-23T12:05:00Z", now)).toBe("in 5 min")
    expect(formatRelative("2026-10-23T15:00:00Z", now)).toBe("in 3 h")
    expect(formatRelative("2026-10-30T12:00:00Z", now)).toBe("Oct 30, 2026")
  })
})
