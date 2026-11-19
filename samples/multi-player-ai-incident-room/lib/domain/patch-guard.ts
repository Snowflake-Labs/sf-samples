// Guards for a proposed task fix. Mirrored in sql/tools.sql (PROPOSE_AND_TEST) and
// sql/ship.sql (SHIP_FIX), which enforce them server-side; this copy gives the UI early feedback.

import type { Check } from "./incident"

// A whole dotted name (any number of parts). Quoted identifiers are rejected outright below.
const DOTTED = /\b[A-Z_][A-Z0-9_$]*(?:\.[A-Z_][A-Z0-9_$]*)+\b/gi

function strip(sql: string): string {
  return sql.trim().replace(/;\s*$/, "").trim()
}

/** One INSERT statement whose every qualified name is inside `schema` ("DB.SCHEMA"). */
export function checkTaskSql(sql: string, schema: string): Check {
  const body = strip(sql)
  if (!/^INSERT\s/i.test(body)) return { ok: false, reason: "A task fix must be a single INSERT statement" }
  if (body.includes(";")) return { ok: false, reason: "Only one statement is allowed" }
  if (body.includes('"')) return { ok: false, reason: "Quoted identifiers aren't allowed in a task fix" }
  const want = schema.toUpperCase()
  for (const m of body.matchAll(DOTTED)) {
    const parts = m[0].toUpperCase().split(".")
    const s = parts.slice(0, 2).join(".")
    if (parts.length !== 3 || s !== want) return { ok: false, reason: `References ${m[0]}, outside ${want}` }
  }
  return { ok: true }
}

export function cloneSchemaName(incidentId: string): string {
  if (!/^[A-Za-z0-9-]{1,64}$/.test(incidentId)) throw new Error(`Invalid incident id: ${incidentId}`)
  return `INCIDENT_ROOM_CLONES.${incidentId.replace(/-/g, "_").toUpperCase()}`
}

export function rewriteForClone(sql: string, schema: string, clone: string): string {
  const [db, sch] = schema.toUpperCase().split(".")
  return strip(sql).replace(new RegExp(`\\b${db}\\.${sch}\\.`, "gi"), `${clone}.`)
}
