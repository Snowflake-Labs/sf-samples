// Server-side helpers shared by the /api/{forward,reverse,normalize} routes.
// Each request stages its input in a uniquely named transient table in
// GEOCODING.TESTER, calls the procedure, reads the output and drops both.
// User input only ever reaches Snowflake through bind variables.

import { randomUUID } from "crypto"
import { querySnowflakeLongRunning } from "@/lib/snowflake"

export const SCRATCH = "GEOCODING.TESTER"

export type RunResult = {
  rows: Record<string, unknown>[]
  sql: string[]
  status?: string
  ms: number
}

export function scratchName(prefix: string) {
  return `${SCRATCH}.${prefix}_${randomUUID().replace(/-/g, "").slice(0, 16).toUpperCase()}`
}

export async function withScratch<T>(
  tables: string[],
  fn: () => Promise<T>,
): Promise<T> {
  try {
    return await fn()
  } finally {
    for (const t of tables) {
      await querySnowflakeLongRunning(`DROP TABLE IF EXISTS ${t}`).catch(() => {})
    }
  }
}

export const q = (sql: string, binds?: (string | number | null)[]) =>
  querySnowflakeLongRunning(sql, binds ? { binds } : {})

export function errorResponse(e: unknown) {
  const message = e instanceof Error ? e.message : String(e)
  return Response.json({ error: message }, { status: 500 })
}

export function firstValue(rows: Record<string, unknown>[]): string | undefined {
  const r = rows[0]
  return r ? String(Object.values(r)[0]) : undefined
}
