/**
 * Read one property from a DESCRIBE result.
 *
 * DESCRIBE USER currently returns `property` / `property_value` /
 * `property_default`. Older sessions returned `property` / `value`. The Node
 * driver also uppercases unquoted column names, so match keys case-insensitively
 * and prefer `property_value` when that column is present.
 */
export function describeProperty(rows: Record<string, unknown>[], key: string): unknown {
  const row = rows.find((r) => String(field(r, "property") ?? "").toUpperCase() === key.toUpperCase())
  if (!row) return undefined
  if (hasField(row, "property_value")) return field(row, "property_value")
  return field(row, "value")
}

function hasField(row: Record<string, unknown>, name: string): boolean {
  const target = name.toLowerCase()
  return Object.keys(row).some((k) => k.toLowerCase() === target)
}

function field(row: Record<string, unknown>, name: string): unknown {
  const target = name.toLowerCase()
  for (const [k, v] of Object.entries(row)) {
    if (k.toLowerCase() === target) return v
  }
  return undefined
}
