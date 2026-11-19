// Tool results -> evidence cards pinned to the room's board. Pure.

export type Evidence =
  | { kind: "failure"; task: string; at: string; error: string }
  | { kind: "schema"; task: string; taskSql: string; columns: Record<string, string[]> }
  | { kind: "blast"; objects: { object: string; hops: number }[] }
  | {
      kind: "patch"
      patchId: string
      ok: boolean
      cloneSchema: string
      verdict?: string
      error?: string
      clone?: { rows: number; revenue: number }
      prod?: { rows: number; revenue: number }
    }

type Rec = Record<string, unknown>
const num = (v: unknown) => Number(v ?? 0)

export function evidenceFrom(tool: string, result: unknown): Evidence | null {
  if (tool === "recent_failures" && Array.isArray(result)) {
    const f = (result as Rec[]).find((r) => r.state === "FAILED")
    return f ? { kind: "failure", task: String(f.task), at: String(f.scheduled_time), error: String(f.error_message) } : null
  }
  if (tool === "inspect_pipeline" && result && typeof result === "object") {
    const r = result as Rec
    return { kind: "schema", task: String(r.task), taskSql: String(r.task_sql ?? ""), columns: (r.columns as Record<string, string[]>) ?? {} }
  }
  if (tool === "downstream" && Array.isArray(result)) {
    return { kind: "blast", objects: (result as Rec[]).map((o) => ({ object: String(o.object), hops: num(o.hops) })) }
  }
  if (tool === "propose_and_test" && result && typeof result === "object") {
    const r = result as Rec
    if (!r.patch_id) return null
    return {
      kind: "patch",
      patchId: String(r.patch_id),
      ok: r.ok === true,
      cloneSchema: String(r.clone_schema ?? ""),
      verdict: r.verdict ? String(r.verdict) : undefined,
      error: r.error ? String(r.error) : undefined,
      clone: r.clone_rows === undefined ? undefined : { rows: num(r.clone_rows), revenue: num(r.clone_revenue) },
      prod: r.prod_last_good_rows === undefined ? undefined : { rows: num(r.prod_last_good_rows), revenue: num(r.prod_last_good_revenue) },
    }
  }
  return null
}

/** Pin a card: one per kind, the newest replaces the old (patches are keyed by id). */
export function pin(board: Evidence[], e: Evidence): Evidence[] {
  const same = (x: Evidence) => (e.kind === "patch" && x.kind === "patch" ? x.patchId === e.patchId : x.kind === e.kind)
  return [...board.filter((x) => !same(x)), e]
}
