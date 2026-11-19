import { describe, expect, it } from "vitest"
import { flashFor, lineFor, lineFromLatestRun, typingNow } from "@/lib/domain/line"

describe("lineFor", () => {
  it("maps the incident to a line state", () => {
    expect(lineFor("TRIAGE", false)).toBe("stopped")
    expect(lineFor("FIXING", false)).toBe("fixing")
    expect(lineFor("VOTING", false)).toBe("gated")
    expect(lineFor("SHIPPED", false)).toBe("running")
    expect(lineFor("CLOSED", false)).toBe("running")
  })

  it("the andon always stops the line", () => {
    expect(lineFor("VOTING", true)).toBe("stopped")
    expect(lineFor("SHIPPED", true)).toBe("stopped")
  })
})

describe("lineFromLatestRun", () => {
  it("is stopped only when the latest run failed", () => {
    expect(lineFromLatestRun("FAILED")).toBe("stopped")
    expect(lineFromLatestRun("SUCCEEDED")).toBe("running")
    expect(lineFromLatestRun(undefined)).toBe("running")
  })
})

describe("typingNow", () => {
  it("lists other people with a fresh signal, sorted", () => {
    const now = 10_000
    expect(typingNow({ EVA: 9_000, CLAIRE: 8_000, DASH: 9_500, OLD: 1_000 }, now, "DASH")).toEqual(["CLAIRE", "EVA"])
  })
})

describe("flashFor", () => {
  const e = (by: string, kind: string, payload?: unknown) => ({ by, kind, at: 5, payload })
  it("points at what someone else changed", () => {
    expect(flashFor(e("CLAIRE", "vote", { vote: "reject" }), "DASH")).toEqual({ target: "vote", text: "CLAIRE rejected", at: 5 })
    expect(flashFor(e("EVA", "andon"), "DASH")).toEqual({ target: "line", text: "EVA pulled the andon", at: 5 })
  })
  it("ignores your own actions and agent chatter", () => {
    expect(flashFor(e("DASH", "vote", { vote: "approve" }), "dash")).toBeNull()
    expect(flashFor(e("FIXER", "agent.say"), "DASH")).toBeNull()
  })
})
