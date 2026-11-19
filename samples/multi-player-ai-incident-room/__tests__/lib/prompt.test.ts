import { describe, expect, it } from "vitest"
import { buildPrompt, sinceLastTurn, transcript } from "@/lib/domain/prompt"
import type { RoomEvent } from "@/lib/room/bus"

const ev = (by: string, kind: string, payload: unknown, at = 0): RoomEvent => ({ id: `${at}`, incidentId: "INC-1", at, by, kind, payload })

describe("transcript", () => {
  it("keeps human messages, agent replies, votes and andon; drops tool chatter", () => {
    expect(
      transcript([
        ev("DASH", "message", { text: "@triage what broke?" }),
        ev("TRIAGE", "agent.tool", { name: "recent_failures" }),
        ev("TRIAGE", "agent.say", { text: "AMOUNT was renamed." }),
        ev("CLAIRE", "vote", { vote: "approve", patchId: "P-1" }),
        ev("EVA", "andon", { reason: "wait" }),
      ]),
    ).toEqual([
      "DASH: @triage what broke?",
      "@triage (agent): AMOUNT was renamed.",
      "CLAIRE voted approve on P-1",
      "EVA pulled the andon cord: wait",
    ])
  })
  it("keeps only the most recent lines", () => {
    const many = Array.from({ length: 40 }, (_, i) => ev("DASH", "message", { text: `m${i}` }, i))
    const t = transcript(many)
    expect(t).toHaveLength(30)
    expect(t[0]).toBe("DASH: m10")
  })
})

describe("buildPrompt", () => {
  it("starts with the incident tag the tools need, then context, then the ask", () => {
    const p = buildPrompt({
      incidentId: "INC-1",
      title: "LOAD_DAILY_REVENUE failed",
      agent: "FIXER",
      from: "DASH",
      text: "@fixer patch it",
      history: [ev("TRIAGE", "agent.say", { text: "AMOUNT was renamed." })],
    })
    expect(p.split("\n")[0]).toBe("[incident: INC-1]")
    expect(p).toContain("@triage (agent): AMOUNT was renamed.")
    expect(p.trim().endsWith("[DASH is asking you, @fixer]\n@fixer patch it")).toBe(true)
  })
})

describe("sinceLastTurn", () => {
  const events = [
    ev("DASH", "message", { text: "@triage look" }, 1),
    ev("TRIAGE", "agent.say", { text: "column renamed" }, 2),
    ev("FIXER", "agent.say", { text: "patched" }, 3),
    ev("CLAIRE", "message", { text: "@triage again?" }, 4),
  ]
  it("keeps only what happened after the agent's last reply", () => {
    expect(sinceLastTurn(events, "TRIAGE").map((e) => e.at)).toEqual([3, 4])
  })
  it("returns everything when the agent hasn't spoken", () => {
    expect(sinceLastTurn(events, "SCRIBE")).toHaveLength(4)
  })
})

describe("decisions in the transcript", () => {
  it("shows filed decisions to the agents", () => {
    expect(transcript([ev("EVA", "decision", { text: "AMOUNT_USD is permanent" })])).toEqual([
      "EVA filed a decision: AMOUNT_USD is permanent",
    ])
  })
  it("labels a resumed thread as new since the agent last spoke", () => {
    const p = buildPrompt({ incidentId: "INC-1", title: "t", agent: "FIXER", from: "DASH", text: "go", history: [ev("DASH", "message", { text: "hi" })], resumed: true })
    expect(p).toContain("[New in the room since you last spoke")
  })
})
