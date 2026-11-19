// What an agent is told when it's called into the room. Pure.

import type { RoomEvent } from "@/lib/room/bus"
import { at, type AgentName } from "./incident"

const MAX_LINES = 30
const MAX_CHARS = 1200

/** One transcript line per human message or agent reply; tool chatter is left out. */
export function transcript(events: RoomEvent[]): string[] {
  const lines: string[] = []
  for (const e of events) {
    const p = (e.payload ?? {}) as { text?: string; reason?: string; vote?: string; patchId?: string }
    if (e.kind === "message" && p.text) lines.push(`${e.by}: ${p.text}`)
    else if (e.kind === "agent.say" && p.text) lines.push(`${at(e.by)} (agent): ${p.text}`)
    else if (e.kind === "vote") lines.push(`${e.by} voted ${p.vote} on ${p.patchId}`)
    else if (e.kind === "andon") lines.push(`${e.by} pulled the andon cord${p.reason ? `: ${p.reason}` : ""}`)
    else if (e.kind === "decision" && p.text) lines.push(`${e.by} filed a decision: ${p.text}`)
  }
  return lines.slice(-MAX_LINES).map((l) => (l.length > MAX_CHARS ? `${l.slice(0, MAX_CHARS)}…` : l))
}

/**
 * Events after the agent's last finished reply. An agent with a thread already
 * remembers everything up to then, so it only needs what's new.
 */
export function sinceLastTurn(events: RoomEvent[], agent: AgentName): RoomEvent[] {
  const who = agent.toLowerCase()
  for (let i = events.length - 1; i >= 0; i--) {
    const e = events[i]
    if ((e.kind === "agent.say" || e.kind === "agent.stopped") && e.by.toLowerCase() === who) return events.slice(i + 1)
  }
  return events
}

export function buildPrompt(args: {
  incidentId: string
  title: string
  agent: AgentName
  from: string
  text: string
  history: RoomEvent[]
  /** True when the agent's thread already holds the earlier conversation. */
  resumed?: boolean
}): string {
  const lines = transcript(args.history)
  const heading = args.resumed ? "[New in the room since you last spoke, oldest first]" : "[Room so far, oldest first]"
  return [
    `[incident: ${args.incidentId}]`,
    `Incident: ${args.title}`,
    ...(lines.length ? ["", heading, ...lines] : []),
    "",
    `[${args.from} is asking you, ${at(args.agent)}]`,
    args.text,
  ].join("\n")
}
