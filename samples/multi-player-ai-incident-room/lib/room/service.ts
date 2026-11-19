// Room orchestration: every action checks the domain rules, persists an event, and
// publishes it to everyone in the room.

import { randomUUID } from "node:crypto"
import {
  at,
  applyVote,
  canDispatch,
  canShip,
  canVote,
  freeze,
  routeMentions,
  statusAfterPatch,
  tally,
  unfreeze,
  type AgentName,
  type Check,
} from "@/lib/domain/incident"
import { evidenceFrom } from "@/lib/domain/evidence"
import { buildPrompt, sinceLastTurn } from "@/lib/domain/prompt"
import { runAgent, usesThreads } from "@/lib/agents/run"
import { claimAgent, present, publish, releaseAgent, runningAgents, stopAll, type RoomEvent } from "./bus"
import * as store from "./store"
import { checkApprover } from "./caller"

// At least two people must approve; IR_REQUIRED_APPROVALS can raise it, never lower it.
export const REQUIRED_APPROVALS = Math.max(2, Math.floor(Number(process.env.IR_REQUIRED_APPROVALS)) || 2)

async function emit(incidentId: string, by: string, kind: string, payload?: unknown): Promise<RoomEvent> {
  const e: RoomEvent = { id: randomUUID(), incidentId, at: Date.now(), by, kind, payload }
  publish(e)
  try {
    await store.appendEvent(e)
  } catch (err) {
    console.error(new Date().toISOString(), "[room] failed to persist event", kind, err)
  }
  return e
}

export class RoomError extends Error {
  constructor(message: string, readonly status = 409) {
    super(message)
  }
}

async function load(incidentId: string) {
  const inc = await store.getIncident(incidentId)
  if (!inc) throw new RoomError("No such incident", 404)
  return inc
}

const refuse = (c: Check) => {
  if (!c.ok) throw new RoomError(c.reason)
}

export async function openIncident(by: string, i: { title: string; objectFqn?: string; errorMessage?: string }) {
  const id = await store.createIncident({ ...i, openedBy: by, requiredApprovals: REQUIRED_APPROVALS })
  await emit(id, by, "opened", i)
  // The line starts itself: Triage is paged the moment a room opens.
  void dispatch(id, "TRIAGE", by, `Incident opened: ${i.title}.${i.errorMessage ? ` Error: ${i.errorMessage}` : ""} What broke, and what is affected?`)
  return id
}

export async function postMessage(incidentId: string, by: string, text: string) {
  const body = text.trim().slice(0, 4000)
  if (!body) throw new RoomError("Empty message", 400)
  const inc = await load(incidentId)
  await emit(incidentId, by, "message", { text: body })
  const agents = routeMentions(body)
  if (!agents.length) return { dispatched: [] as AgentName[] }
  const allowed: AgentName[] = []
  for (const a of agents) {
    const gate = canDispatch(inc, a)
    if (gate.ok) allowed.push(a)
    else await emit(incidentId, "INCIDENT_ROOM", "notice", { text: `${gate.reason}. ${at(a)} was not called.` })
  }
  // Agents work in parallel; each streams into the room on its own.
  for (const a of allowed) void dispatch(incidentId, a, by, body)
  return { dispatched: allowed }
}

/** Run one agent turn in the room. Resolves to what the agent said ("" if it was stopped or failed). */
async function dispatch(incidentId: string, agent: AgentName, from: string, text: string): Promise<string> {
  const ctl = claimAgent(incidentId, agent)
  if (!ctl) {
    await emit(incidentId, "INCIDENT_ROOM", "notice", { text: `${at(agent)} is already working on this.` })
    return ""
  }
  let said = ""
  try {
    const inc = await load(incidentId)
    refuse(canDispatch(inc, agent))
    const [events, thread] = await Promise.all([
      store.listEvents(incidentId),
      usesThreads() ? store.getThread(incidentId, agent) : undefined,
    ])
    // With a thread the agent already remembers earlier turns; send only what's new.
    const resumed = !!thread
    const history = resumed ? sinceLastTurn(events, agent) : events
    const prompt = buildPrompt({ incidentId, title: inc.title, agent, from, text, history, resumed })
    await emit(incidentId, agent, "agent.start", { from })
    const run = {
      agent,
      prompt,
      signal: ctl.signal,
      thread,
      onThread: (t: { thread: string; parent: string }) => store.saveThread(incidentId, agent, t),
    }
    for await (const e of runAgent(run)) {
      if (ctl.signal.aborted) break
      if (e.type === "text") {
        said += e.text
        publish({ id: randomUUID(), incidentId, at: Date.now(), by: agent, kind: "agent.delta", payload: { text: e.text } })
      } else if (e.type === "tool") {
        await emit(incidentId, agent, "agent.tool", { name: e.name, input: e.input, id: e.id })
      } else if (e.type === "tool_result") {
        await emit(incidentId, agent, "agent.tool_result", { name: e.name, id: e.id, ok: e.ok })
        const card = evidenceFrom(e.name, e.result)
        if (card) await emit(incidentId, agent, "evidence", card)
        if (card?.kind === "patch") await onPatch(incidentId, card.patchId, card.ok)
      } else if (e.type === "error") {
        await emit(incidentId, agent, "agent.error", { message: e.message })
      }
    }
  } catch (err) {
    if (!ctl.signal.aborted) {
      console.error(new Date().toISOString(), `[room] ${agent} failed`, err)
      await emit(incidentId, agent, "agent.error", {
        message: err instanceof RoomError ? err.message : `${at(agent)} hit an error. Try again.`,
      })
    }
  } finally {
    releaseAgent(incidentId, agent, ctl)
    if (ctl.signal.aborted) {
      await emit(incidentId, agent, "agent.stopped", { text: said })
    } else {
      await emit(incidentId, agent, "agent.say", { text: said.trim() })
    }
  }
  return ctl.signal.aborted ? "" : said.trim()
}

async function onPatch(incidentId: string, patchId: string, testedOk: boolean) {
  const inc = await load(incidentId)
  const next = statusAfterPatch(inc, { id: patchId, incidentId, testedOk })
  if (next !== inc.status) {
    await store.saveIncidentState({ ...inc, status: next })
    await emit(incidentId, "INCIDENT_ROOM", "status", { status: next, patchId })
  }
}

/**
 * `/file`: record a decision the team made. Append-only and attributed, with who was
 * in the room. @scribe cites these in the postmortem; @triage and @fixer read them in
 * later incidents through PAST_INCIDENTS.
 */
export async function fileDecision(incidentId: string, by: string, text: string) {
  const inc = await load(incidentId)
  const typed = text.trim().slice(0, 2000)
  if (typed) return recordDecision(incidentId, by, typed, by)
  // A bare /file: @scribe drafts the decisions made since the last filing.
  refuse(canDispatch(inc, "SCRIBE"))
  void (async () => {
    // @scribe may be mid-postmortem (it starts on ship); queue behind it rather than drop the ask.
    if (runningAgents(incidentId).includes("SCRIBE")) {
      await emit(incidentId, "INCIDENT_ROOM", "notice", { text: `${at("SCRIBE")} will draft the decision once it finishes.` })
      for (let i = 0; i < 360 && runningAgents(incidentId).includes("SCRIBE"); i++) await new Promise((r) => setTimeout(r, 1000))
    }
    const draft = await dispatch(incidentId, "SCRIBE", by, DRAFT_DECISION)
    if (draft && !/^nothing new\b/i.test(draft)) await recordDecision(incidentId, "SCRIBE", draft.slice(0, 2000), by)
  })()
}

const DRAFT_DECISION =
  "Draft a decision record for /file: what the team decided since the last filed decision (who decided what, and why), " +
  "in at most 500 characters of plain text. Return only the record: no greeting, no heading, and don't call file_postmortem. " +
  "If nothing new was decided, reply exactly: Nothing new to file."

async function recordDecision(incidentId: string, filedBy: string, body: string, requestedBy: string) {
  const id = randomUUID()
  const participants = [...new Set([requestedBy, ...present(incidentId)])]
  await store.insertDecision({ id, incidentId, filedBy, participants, body })
  await emit(incidentId, filedBy, "decision", { id, text: body, participants, requestedBy })
}

export async function pullAndon(incidentId: string, by: string, reason?: string) {
  const inc = await load(incidentId)
  const frozen = freeze(inc, by, reason)
  await store.saveIncidentState(frozen)
  const stopped = stopAll(incidentId)
  await emit(incidentId, by, "andon", { reason: frozen.frozenReason, stopped })
}

export async function clearAndon(incidentId: string, by: string) {
  const inc = await load(incidentId)
  await store.saveIncidentState(unfreeze(inc))
  await emit(incidentId, by, "andon.clear", {})
}

export async function vote(incidentId: string, by: string, patchId: string, choice: "approve" | "reject") {
  const inc = await load(incidentId)
  const patch = await store.getPatch(patchId)
  if (!patch || patch.incidentId !== incidentId) throw new RoomError("No such patch", 404)
  refuse(canVote(inc, patch, by))
  // Anyone can block; approving is checked against the voter's own Snowflake session.
  if (choice === "approve") refuse(await checkApprover(by))
  await store.insertVote({ patchId, voter: by, vote: choice })
  const votes = applyVote(await store.listVotes(patchId), { patchId, voter: by, vote: choice })
  await emit(incidentId, by, "vote", { patchId, vote: choice, ...tally(votes), ready: canShip(inc, patch, votes).ok })
}

export async function ship(incidentId: string, by: string, patchId: string) {
  const inc = await load(incidentId)
  const patch = await store.getPatch(patchId)
  if (!patch || patch.incidentId !== incidentId) throw new RoomError("No such patch", 404)
  refuse(canShip(inc, patch, await store.listVotes(patchId)))
  refuse(await checkApprover(by))
  // SHIP_FIX re-checks the votes in the database and is the real gate.
  const r = await store.shipFix(patchId, by)
  if (!r.ok) throw new RoomError(r.error ?? "Ship refused")
  await store.saveIncidentState({ ...inc, status: "SHIPPED" })
  await emit(incidentId, by, "ship", { patchId, ...r })
  void dispatch(incidentId, "SCRIBE", by, "The fix shipped. Write the postmortem and file it.")
}

export async function closeIncident(incidentId: string, by: string) {
  const inc = await load(incidentId)
  stopAll(incidentId)
  const patch = await store.currentPatch(incidentId)
  if (patch?.cloneSchema) {
    try {
      await store.dropClone(patch.cloneSchema)
    } catch (err) {
      console.error(new Date().toISOString(), "[room] clone drop failed", err)
    }
  }
  await store.saveIncidentState({ ...inc, status: "CLOSED" })
  await emit(incidentId, by, "closed", { droppedClone: patch?.cloneSchema ?? null })
}

export async function snapshot(incidentId: string) {
  const inc = await load(incidentId)
  const [events, patch, postmortem] = await Promise.all([
    store.listEvents(incidentId),
    store.currentPatch(incidentId),
    store.latestPostmortem(incidentId),
  ])
  const votes = patch ? await store.listVotes(patch.id) : []
  return {
    incident: inc,
    events,
    patch,
    votes,
    tally: tally(votes),
    shipCheck: patch ? canShip(inc, patch, votes) : { ok: false, reason: "No tested patch yet" },
    running: runningAgents(incidentId),
    postmortem,
  }
}

/** Typing indicator: broadcast only. Never persisted, so a reload doesn't replay stale dots. */
export function signalTyping(incidentId: string, by: string, on: boolean): void {
  publish({ id: randomUUID(), incidentId, at: Date.now(), by, kind: "typing", payload: { on } })
}
