import { describe, expect, it } from "vitest"
import { evidenceFrom, pin, type Evidence } from "@/lib/domain/evidence"

describe("evidenceFrom", () => {
  it("pins the latest failed run, not a success", () => {
    const runs = [
      { task: "D.S.T", state: "SUCCEEDED", scheduled_time: "t2" },
      { task: "D.S.T", state: "FAILED", scheduled_time: "t1", error_message: "invalid identifier 'AMOUNT'" },
    ]
    expect(evidenceFrom("recent_failures", runs)).toEqual({ kind: "failure", task: "D.S.T", at: "t1", error: "invalid identifier 'AMOUNT'" })
    expect(evidenceFrom("recent_failures", [runs[0]])).toBeNull()
  })
  it("pins the blast radius", () => {
    expect(evidenceFrom("downstream", [{ object: "D.S.V", hops: 1, kind: "VIEW" }])).toEqual({ kind: "blast", objects: [{ object: "D.S.V", hops: 1 }] })
  })
  it("pins a clone-tested patch with its proof", () => {
    const r = {
      ok: true, patch_id: "P-1", clone_schema: "INCIDENT_ROOM_CLONES.INC_1", verdict: "matches",
      clone_rows: 60, clone_revenue: 519000, prod_last_good_rows: 60, prod_last_good_revenue: 519000,
    }
    expect(evidenceFrom("propose_and_test", r)).toEqual({
      kind: "patch", patchId: "P-1", ok: true, cloneSchema: "INCIDENT_ROOM_CLONES.INC_1", verdict: "matches",
      error: undefined, clone: { rows: 60, revenue: 519000 }, prod: { rows: 60, revenue: 519000 },
    })
  })
  it("pins a refused patch with the guard's reason", () => {
    expect(evidenceFrom("propose_and_test", { ok: false, patch_id: "P-2", clone_schema: "C", error: "outside" })).toMatchObject({
      kind: "patch", ok: false, error: "outside", clone: undefined,
    })
  })
  it("ignores tools it doesn't know", () => {
    expect(evidenceFrom("room_log", [])).toBeNull()
  })
})

describe("pin", () => {
  it("replaces a card of the same kind, but keeps every patch", () => {
    const a: Evidence = { kind: "blast", objects: [] }
    const b: Evidence = { kind: "blast", objects: [{ object: "X", hops: 1 }] }
    const p1: Evidence = { kind: "patch", patchId: "P1", ok: false, cloneSchema: "C" }
    const p2: Evidence = { kind: "patch", patchId: "P2", ok: true, cloneSchema: "C" }
    expect(pin(pin(pin(pin([], a), p1), b), p2)).toEqual([p1, b, p2])
  })
})
