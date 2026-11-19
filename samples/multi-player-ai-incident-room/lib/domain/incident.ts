// Incident rules for the Multi-player AI Incident Room. Pure: no I/O, no clock.

export const AGENTS = ["TRIAGE", "FIXER", "SCRIBE"] as const
export type AgentName = (typeof AGENTS)[number]

export type Status = "TRIAGE" | "FIXING" | "VOTING" | "SHIPPED" | "CLOSED"

export interface Incident {
  id: string
  status: Status
  frozen: boolean
  frozenBy?: string
  frozenReason?: string
  requiredApprovals: number
}

export interface Patch {
  id: string
  incidentId: string
  testedOk: boolean
}

export interface Vote {
  patchId: string
  voter: string
  vote: "approve" | "reject"
}

export type Check = { ok: true } | { ok: false; reason: string }

/** What people type and see. Snowflake object names (TRIAGE, FIXER, SCRIBE) stay as they are. */
export const HANDLE: Record<AgentName, string> = { TRIAGE: "triage", FIXER: "fixer", SCRIBE: "scribe" }

export const at = (agent: string) => `@${HANDLE[agent as AgentName] ?? agent.toLowerCase()}`

// Role names first; the earlier nicknames still work as aliases.
const ALIAS: Record<string, AgentName> = {
  SHERLOG: "TRIAGE", PATCHY: "FIXER", QUILL: "SCRIBE",
  TRIAGE: "TRIAGE", FIXER: "FIXER", SCRIBE: "SCRIBE",
}

const MENTION = /(?:^|[^\w@.])@(sherlog|patchy|quill|triage|fixer|scribe|all)\b/gi

/** Agents a message addresses, in order of first mention. `@all` means every agent. */
export function routeMentions(text: string): AgentName[] {
  const out: AgentName[] = []
  for (const m of text.matchAll(MENTION)) {
    const word = m[1].toUpperCase()
    if (word === "ALL") return [...AGENTS]
    const name = ALIAS[word]
    if (!out.includes(name)) out.push(name)
  }
  return out
}

export function isAgentName(name: string): boolean {
  return name.trim().toUpperCase() in ALIAS
}

const terminal = (s: Status) => s === "SHIPPED" || s === "CLOSED"

export function canDispatch(inc: Incident, agent?: AgentName): Check {
  if (inc.frozen) return { ok: false, reason: `Line stopped (andon) by ${inc.frozenBy ?? "a teammate"}` }
  if (inc.status === "CLOSED") return { ok: false, reason: "Incident is closed" }
  // After a ship the only work left is the record.
  if (inc.status === "SHIPPED" && agent !== "SCRIBE") return { ok: false, reason: "Incident is shipped; only @scribe can still help" }
  return { ok: true }
}

/** Pull the andon cord: stops every agent and blocks shipping until someone clears it. */
export function freeze(inc: Incident, by: string, reason?: string): Incident {
  return { ...inc, frozen: true, frozenBy: by, frozenReason: reason?.trim() || undefined }
}

export function unfreeze(inc: Incident): Incident {
  return { ...inc, frozen: false, frozenBy: undefined, frozenReason: undefined }
}

/** A patch proven on the clone opens the vote; a failed test keeps the room fixing. */
export function statusAfterPatch(inc: Incident, p: Patch): Status {
  if (terminal(inc.status)) return inc.status
  return p.testedOk ? "VOTING" : "FIXING"
}

export function canVote(inc: Incident, p: Patch, voter: string): Check {
  if (isAgentName(voter)) return { ok: false, reason: "Agents can't vote" }
  if (inc.frozen) return { ok: false, reason: "Line stopped (andon)" }
  if (inc.status !== "VOTING") return { ok: false, reason: "Nothing is up for a vote" }
  if (!p.testedOk) return { ok: false, reason: "This patch hasn't passed on the clone" }
  return { ok: true }
}

/** Record a vote: one per person, the latest wins. */
export function applyVote(votes: Vote[], vote: Vote): Vote[] {
  const voter = vote.voter.trim().toUpperCase()
  return [...votes.filter((x) => !(x.patchId === vote.patchId && x.voter.toUpperCase() === voter)), { ...vote, voter }]
}

export function tally(votes: Vote[]): { approvals: string[]; rejections: string[] } {
  const humans = votes.filter((x) => !isAgentName(x.voter))
  const names = (k: Vote["vote"]) => [...new Set(humans.filter((x) => x.vote === k).map((x) => x.voter.toUpperCase()))]
  return { approvals: names("approve"), rejections: names("reject") }
}

export function canShip(inc: Incident, p: Patch, votes: Vote[]): Check {
  if (inc.frozen) return { ok: false, reason: "Line stopped (andon)" }
  if (inc.status !== "VOTING") return { ok: false, reason: "Nothing is up for a vote" }
  if (!p.testedOk) return { ok: false, reason: "This patch hasn't passed on the clone" }
  const { approvals, rejections } = tally(votes.filter((x) => x.patchId === p.id))
  if (rejections.length) return { ok: false, reason: `Rejected by ${rejections.join(", ")}` }
  if (approvals.length < inc.requiredApprovals) {
    return { ok: false, reason: `${approvals.length} of ${inc.requiredApprovals} approvals` }
  }
  return { ok: true }
}
