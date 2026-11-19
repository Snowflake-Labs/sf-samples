import { describe, expect, it } from "vitest"
import { approverCheck } from "@/lib/domain/approver"

describe("approverCheck", () => {
  it("passes when the session matches and no role is required", () => {
    expect(approverCheck("CLAIRE", undefined, { sessionUser: "claire" })).toEqual({ ok: true })
  })
  it("does not block when caller's rights couldn't run and no role is required", () => {
    expect(approverCheck("CLAIRE", undefined, { sessionUser: null }).ok).toBe(true)
  })
  it("blocks when a role is required but the session couldn't be checked", () => {
    expect(approverCheck("CLAIRE", "PIPELINE_OWNER", { sessionUser: null }).ok).toBe(false)
  })
  it("blocks a mismatched session", () => {
    expect(approverCheck("CLAIRE", undefined, { sessionUser: "EVA" }).ok).toBe(false)
  })
  it("requires the approver role when one is set", () => {
    expect(approverCheck("DASH", "PIPELINE_OWNER", { sessionUser: "DASH", hasRole: false }).ok).toBe(false)
    expect(approverCheck("DASH", "PIPELINE_OWNER", { sessionUser: "DASH", hasRole: true }).ok).toBe(true)
  })
})
