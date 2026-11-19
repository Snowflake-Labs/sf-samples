import { describe, expect, it } from "vitest"
import {
  AGENTS,
  applyVote,
  canDispatch,
  canShip,
  canVote,
  freeze,
  isAgentName,
  routeMentions,
  at,
  statusAfterPatch,
  tally,
  unfreeze,
  type Incident,
  type Patch,
  type Vote,
} from "@/lib/domain/incident"

const room = (over: Partial<Incident> = {}): Incident => ({
  id: "INC-1",
  status: "TRIAGE",
  frozen: false,
  requiredApprovals: 2,
  ...over,
})
const patch = (over: Partial<Patch> = {}): Patch => ({ id: "P1", incidentId: "INC-1", testedOk: true, ...over })
const v = (voter: string, vote: Vote["vote"]): Vote => ({ patchId: "P1", voter, vote })

describe("routeMentions", () => {
  it("routes role names and the old nicknames", () => {
    expect(routeMentions("@sherlog look, @Patchy fix, @quill log it")).toEqual(["TRIAGE", "FIXER", "SCRIBE"])
    expect(at("TRIAGE")).toBe("@triage")
    expect(isAgentName("Patchy")).toBe(true)
  })
  it("routes each @mention to its agent, case-insensitively, in order, once", () => {
    expect(routeMentions("@triage what broke? then @FIXER patch it, thanks @triage")).toEqual(["TRIAGE", "FIXER"])
  })
  it("@all dispatches every agent in parallel", () => {
    expect(routeMentions("@all go")).toEqual([...AGENTS])
  })
  it("plain chat dispatches nobody", () => {
    expect(routeMentions("I think it's the rename")).toEqual([])
  })
  it("ignores look-alikes such as emails", () => {
    expect(routeMentions("ping fixer@corp.com")).toEqual([])
  })
})

describe("andon", () => {
  it("freeze stops agents and shipping; anyone can unfreeze", () => {
    const frozen = freeze(room(), "DASH", "wrong table")
    expect(frozen).toMatchObject({ frozen: true, frozenBy: "DASH", frozenReason: "wrong table" })
    expect(canDispatch(frozen)).toEqual({ ok: false, reason: expect.stringMatching(/andon/i) })
    expect(canShip(frozen, patch(), [v("A", "approve"), v("B", "approve")]).ok).toBe(false)
    expect(unfreeze(frozen)).toMatchObject({ frozen: false, frozenBy: undefined, frozenReason: undefined })
  })
  it("a closed room accepts no dispatch", () => {
    expect(canDispatch(room({ status: "CLOSED" })).ok).toBe(false)
    expect(canDispatch(room()).ok).toBe(true)
  })
  it("after a ship only @scribe works (to write the postmortem)", () => {
    expect(canDispatch(room({ status: "SHIPPED" }), "SCRIBE").ok).toBe(true)
    expect(canDispatch(room({ status: "SHIPPED" }), "FIXER").ok).toBe(false)
    expect(canDispatch(room({ status: "CLOSED" }), "SCRIBE").ok).toBe(false)
  })
})

describe("statusAfterPatch", () => {
  it("a patch that passed on the clone opens the vote; a failed one keeps fixing", () => {
    expect(statusAfterPatch(room({ status: "TRIAGE" }), patch({ testedOk: true }))).toBe("VOTING")
    expect(statusAfterPatch(room({ status: "TRIAGE" }), patch({ testedOk: false }))).toBe("FIXING")
  })
  it("never reopens a shipped or closed room", () => {
    expect(statusAfterPatch(room({ status: "SHIPPED" }), patch())).toBe("SHIPPED")
  })
})

describe("votes", () => {
  it("agents cannot vote", () => {
    expect(isAgentName("Fixer")).toBe(true)
    expect(canVote(room({ status: "VOTING" }), patch(), "FIXER")).toEqual({ ok: false, reason: expect.stringMatching(/agent/i) })
  })
  it("no vote on an untested patch, a frozen room, or outside voting", () => {
    expect(canVote(room({ status: "VOTING" }), patch({ testedOk: false }), "DASH").ok).toBe(false)
    expect(canVote(room({ status: "VOTING", frozen: true }), patch(), "DASH").ok).toBe(false)
    expect(canVote(room({ status: "TRIAGE" }), patch(), "DASH").ok).toBe(false)
    expect(canVote(room({ status: "VOTING" }), patch(), "DASH").ok).toBe(true)
  })
  it("one vote per person; the latest wins; names are case-insensitive", () => {
    const votes = applyVote(applyVote([], v("dash", "reject")), v("DASH", "approve"))
    expect(votes).toEqual([{ patchId: "P1", voter: "DASH", vote: "approve" }])
  })
  it("tally counts distinct approvals and any rejection", () => {
    expect(tally([v("A", "approve"), v("B", "reject")])).toEqual({ approvals: ["A"], rejections: ["B"] })
  })
})

describe("canShip", () => {
  const voting = room({ status: "VOTING" })
  it("needs the required number of distinct human approvals", () => {
    expect(canShip(voting, patch(), [v("A", "approve")])).toEqual({ ok: false, reason: "1 of 2 approvals" })
    expect(canShip(voting, patch(), [v("A", "approve"), v("B", "approve")])).toEqual({ ok: true })
  })
  it("any rejection blocks shipping", () => {
    expect(canShip(voting, patch(), [v("A", "approve"), v("B", "approve"), v("C", "reject")])).toEqual({
      ok: false,
      reason: "Rejected by C",
    })
  })
  it("agent 'approvals' never count", () => {
    expect(canShip(voting, patch(), [v("A", "approve"), { patchId: "P1", voter: "TRIAGE", vote: "approve" }]).ok).toBe(false)
  })
  it("only a clone-tested patch, only while voting", () => {
    const two = [v("A", "approve"), v("B", "approve")]
    expect(canShip(voting, patch({ testedOk: false }), two).ok).toBe(false)
    expect(canShip(room({ status: "SHIPPED" }), patch(), two).ok).toBe(false)
  })
  it("votes for another patch don't count", () => {
    expect(canShip(voting, patch({ id: "P2" }), [v("A", "approve"), v("B", "approve")]).ok).toBe(false)
  })
})
