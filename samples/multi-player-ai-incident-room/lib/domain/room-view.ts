// The room as the UI shows it, folded from events. Pure: the same reducer replays
// persisted history and applies live events, so a reload looks identical.

import type { RoomEvent } from "@/lib/room/bus"
import { pin, type Evidence } from "./evidence"
import { at, type AgentName, type Status } from "./incident"

export type Tool = { id?: string; name: string; status: "running" | "ok" | "error" }

export type Item =
  | { kind: "human"; id: string; by: string; at: number; text: string }
  | { kind: "agent"; id: string; agent: AgentName; at: number; text: string; tools: Tool[]; done: boolean; stopped?: boolean; error?: string }
  | { kind: "system"; id: string; at: number; text: string; tone: "info" | "success" | "danger" }
  | { kind: "decision"; id: string; by: string; at: number; text: string; participants: string[]; requestedBy?: string }

export interface View {
  seen: Set<string>
  items: Item[]
  board: Evidence[]
  status?: Status
  frozen: { by: string; reason?: string } | null
  /** Live tally from the latest vote event, and the patch it counts for. */
  tally: { approvals: string[]; rejections: string[]; patchId?: string }
  working: AgentName[]
  present: string[]
  /** Latest typing signal per person (ms). Ephemeral: never persisted. */
  typing: Record<string, number>
  shipped?: { by: string; approvedBy: string[] }
}

export const initialView = (): View => ({
  seen: new Set(),
  items: [],
  board: [],
  frozen: null,
  tally: { approvals: [], rejections: [] },
  working: [],
  present: [],
  typing: {},
})

type P = Record<string, unknown> & { text?: string; reason?: string; name?: string; id?: string; message?: string }

const agentOf = (by: string) => by.toUpperCase() as AgentName

/** Index of the agent's in-progress bubble, or -1. */
function openBubble(items: Item[], agent: AgentName): number {
  for (let i = items.length - 1; i >= 0; i--) {
    const it = items[i]
    if (it.kind === "agent" && it.agent === agent && !it.done) return i
  }
  return -1
}

function ensureBubble(v: View, e: RoomEvent): [Item[], number] {
  const agent = agentOf(e.by)
  const i = openBubble(v.items, agent)
  if (i >= 0) return [v.items.slice(), i]
  const items = [...v.items, { kind: "agent" as const, id: e.id, agent, at: e.at, text: "", tools: [], done: false }]
  return [items, items.length - 1]
}

function patchBubble(v: View, e: RoomEvent, f: (b: Extract<Item, { kind: "agent" }>) => Partial<Extract<Item, { kind: "agent" }>>): Item[] {
  const [items, i] = ensureBubble(v, e)
  const b = items[i] as Extract<Item, { kind: "agent" }>
  items[i] = { ...b, ...f(b) }
  return items
}

const working = (v: View, agent: AgentName, on: boolean) =>
  on ? [...new Set([...v.working, agent])] : v.working.filter((a) => a !== agent)

const sys = (e: RoomEvent, text: string, tone: "info" | "success" | "danger" = "info"): Item => ({ kind: "system", id: e.id, at: e.at, text, tone })

export function reduce(v: View, e: RoomEvent): View {
  // Deltas aren't persisted and have fresh ids, so only de-duplicate durable events.
  if (e.kind !== "agent.delta" && e.kind !== "presence" && e.kind !== "typing") {
    if (v.seen.has(e.id)) return v
    v = { ...v, seen: new Set(v.seen).add(e.id) }
  }
  const p = (e.payload ?? {}) as P
  switch (e.kind) {
    case "opened":
      return { ...v, status: v.status ?? "TRIAGE", items: [...v.items, sys(e, `${e.by} opened the room.`)] }
    case "message": {
      const { [e.by]: _done, ...typing } = v.typing
      return { ...v, typing, items: [...v.items, { kind: "human", id: e.id, by: e.by, at: e.at, text: String(p.text ?? "") }] }
    }
    case "typing": {
      const { [e.by]: _was, ...rest } = v.typing
      return { ...v, typing: p.on === false ? rest : { ...rest, [e.by]: e.at } }
    }
    case "agent.start":
      return { ...v, items: ensureBubble(v, e)[0], working: working(v, agentOf(e.by), true) }
    case "agent.delta":
      return { ...v, items: patchBubble(v, e, (b) => ({ text: b.text + (p.text ?? "") })) }
    case "agent.tool":
      return { ...v, items: patchBubble(v, e, (b) => ({ tools: [...b.tools, { id: p.id, name: String(p.name), status: "running" }] })) }
    case "agent.tool_result":
      return {
        ...v,
        items: patchBubble(v, e, (b) => {
          const tools = b.tools.slice()
          const k = tools.findIndex((t) => (p.id ? t.id === p.id : t.name === p.name && t.status === "running"))
          const status = p.ok === false ? "error" : "ok"
          if (k >= 0) tools[k] = { ...tools[k], status }
          else tools.push({ id: p.id, name: String(p.name), status })
          return { tools }
        }),
      }
    case "agent.say":
      return {
        ...v,
        items: patchBubble(v, e, (b) => ({ text: p.text || b.text, done: true, tools: b.tools.map((t) => (t.status === "running" ? { ...t, status: "ok" } : t)) })),
        working: working(v, agentOf(e.by), false),
      }
    case "agent.stopped":
      return { ...v, items: patchBubble(v, e, (b) => ({ text: p.text || b.text, done: true, stopped: true })), working: working(v, agentOf(e.by), false) }
    case "agent.error":
      return { ...v, items: patchBubble(v, e, () => ({ error: String(p.message ?? "Error") })) }
    case "evidence":
      return { ...v, board: pin(v.board, e.payload as Evidence) }
    case "status": {
      const s = p.status as Status
      const note = s === "VOTING" ? sys(e, "A fix passed on the clone. It's up for a vote.", "success") : sys(e, `Status: ${s.toLowerCase()}`)
      return { ...v, status: s, items: [...v.items, note] }
    }
    case "vote":
      return {
        ...v,
        tally: { approvals: (p.approvals as string[]) ?? [], rejections: (p.rejections as string[]) ?? [], patchId: p.patchId as string | undefined },
        items: [...v.items, sys(e, `${e.by} voted to ${p.vote === "approve" ? "ship" : "reject"} ${p.patchId}.`, p.vote === "approve" ? "info" : "danger")],
      }
    case "andon": {
      const stopped = (p.stopped as string[]) ?? []
      return {
        ...v,
        frozen: { by: e.by, reason: p.reason },
        working: [],
        items: [
          ...v.items,
          sys(e, `${e.by} pulled the andon cord${p.reason ? `: "${p.reason}"` : ""}. Line stopped${stopped.length ? `, ${stopped.map((s) => at(s)).join(", ")} halted` : ""}.`, "danger"),
        ],
      }
    }
    case "andon.clear":
      return { ...v, frozen: null, items: [...v.items, sys(e, `${e.by} cleared the andon. Line running.`, "success")] }
    case "ship":
      return {
        ...v,
        status: "SHIPPED",
        shipped: { by: e.by, approvedBy: (p.approved_by as string[]) ?? [] },
        items: [...v.items, sys(e, `${e.by} shipped ${p.patchId} to prod (approved by ${((p.approved_by as string[]) ?? []).join(", ")}).`, "success")],
      }
    case "closed":
      return { ...v, status: "CLOSED", items: [...v.items, sys(e, `${e.by} closed the incident${p.droppedClone ? ` and dropped ${p.droppedClone}` : ""}.`)] }
    case "decision":
      return {
        ...v,
        items: [...v.items, { kind: "decision", id: e.id, by: e.by, at: e.at, text: String(p.text ?? ""), participants: (p.participants as string[]) ?? [], requestedBy: p.requestedBy as string | undefined }],
      }
    case "notice":
      return { ...v, items: [...v.items, sys(e, String(p.text ?? ""))] }
    case "presence":
      return { ...v, present: (p.users as string[]) ?? [] }
    default:
      return v
  }
}
