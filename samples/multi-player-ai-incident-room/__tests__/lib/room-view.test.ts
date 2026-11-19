import { describe, expect, it } from "vitest"
import { initialView, reduce, type View } from "@/lib/domain/room-view"
import type { RoomEvent } from "@/lib/room/bus"

let n = 0
const ev = (by: string, kind: string, payload?: unknown): RoomEvent => ({ id: `e${++n}`, incidentId: "INC-1", at: n, by, kind, payload })
const run = (events: RoomEvent[], v: View = initialView()) => events.reduce(reduce, v)

describe("room view", () => {
  it("streams an agent reply into one bubble and finalizes it", () => {
    const v = run([
      ev("TRIAGE", "agent.start", { from: "DASH" }),
      ev("TRIAGE", "agent.delta", { text: "AMOUNT " }),
      ev("TRIAGE", "agent.tool", { name: "recent_failures", id: "t1" }),
      ev("TRIAGE", "agent.delta", { text: "was renamed." }),
      ev("TRIAGE", "agent.tool_result", { name: "recent_failures", id: "t1", ok: true }),
      ev("TRIAGE", "agent.say", { text: "AMOUNT was renamed." }),
    ])
    expect(v.items).toHaveLength(1)
    expect(v.items[0]).toMatchObject({
      kind: "agent",
      agent: "TRIAGE",
      text: "AMOUNT was renamed.",
      done: true,
      tools: [{ name: "recent_failures", status: "ok" }],
    })
    expect(v.working).toEqual([])
  })

  it("keeps two agents' parallel replies apart", () => {
    const v = run([
      ev("TRIAGE", "agent.start"),
      ev("FIXER", "agent.start"),
      ev("FIXER", "agent.delta", { text: "fix" }),
      ev("TRIAGE", "agent.delta", { text: "cause" }),
    ])
    expect(v.items.map((i) => (i.kind === "agent" ? [i.agent, i.text] : null))).toEqual([
      ["TRIAGE", "cause"],
      ["FIXER", "fix"],
    ])
    expect(v.working.sort()).toEqual(["FIXER", "TRIAGE"])
  })

  it("persisted history replays to the same result without deltas", () => {
    const v = run([ev("FIXER", "agent.start"), ev("FIXER", "agent.say", { text: "Ready for the vote." })])
    expect(v.items[0]).toMatchObject({ kind: "agent", text: "Ready for the vote.", done: true })
  })

  it("pins evidence and tracks status, andon and votes", () => {
    const v = run([
      ev("TRIAGE", "evidence", { kind: "blast", objects: [{ object: "V", hops: 1 }] }),
      ev("INCIDENT_ROOM", "status", { status: "VOTING", patchId: "P-1" }),
      ev("DASH", "vote", { patchId: "P-1", vote: "approve", approvals: ["DASH"], rejections: [], ready: false }),
      ev("CLAIRE", "andon", { reason: "wrong table", stopped: ["FIXER"] }),
    ])
    expect(v.board).toEqual([{ kind: "blast", objects: [{ object: "V", hops: 1 }] }])
    expect(v.status).toBe("VOTING")
    expect(v.tally).toEqual({ approvals: ["DASH"], rejections: [], patchId: "P-1" })
    expect(v.frozen).toEqual({ by: "CLAIRE", reason: "wrong table" })
    expect(v.items.at(-1)).toMatchObject({ kind: "system", tone: "danger" })
  })

  it("an andon pull marks interrupted agents as stopped", () => {
    const v = run([ev("FIXER", "agent.start"), ev("FIXER", "agent.delta", { text: "half" }), ev("FIXER", "agent.stopped", { text: "half" })])
    expect(v.items[0]).toMatchObject({ kind: "agent", stopped: true, done: true })
    expect(v.working).toEqual([])
  })

  it("ignores an event it has already seen (history then live)", () => {
    const e = ev("DASH", "message", { text: "hi" })
    expect(run([e, e]).items).toHaveLength(1)
  })
})

describe("typing", () => {
  it("tracks who is typing and clears it when they send", () => {
    let v = reduce(initialView(), ev("CLAIRE", "typing", { on: true }))
    expect(Object.keys(v.typing)).toEqual(["CLAIRE"])
    v = reduce(v, ev("CLAIRE", "message", { text: "hi" }))
    expect(v.typing).toEqual({})
    v = reduce(v, ev("EVA", "typing", { on: true }))
    v = reduce(v, ev("EVA", "typing", { on: false }))
    expect(v.typing).toEqual({})
  })
})

describe("vote tally", () => {
  it("remembers which patch the live tally counts for", () => {
    const v = reduce(initialView(), ev("CLAIRE", "vote", { patchId: "P-1", vote: "reject", approvals: [], rejections: ["CLAIRE"] }))
    expect(v.tally).toEqual({ approvals: [], rejections: ["CLAIRE"], patchId: "P-1" })
  })
})

describe("filed decisions", () => {
  it("adds an attributed decision card", () => {
    const v = reduce(initialView(), ev("CLAIRE", "decision", { text: "Keep AMOUNT_USD", participants: ["CLAIRE", "DASH"] }))
    expect(v.items.at(-1)).toMatchObject({ kind: "decision", by: "CLAIRE", text: "Keep AMOUNT_USD", participants: ["CLAIRE", "DASH"] })
  })
})
