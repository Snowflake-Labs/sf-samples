// The factory line: the demo pipeline as three stations, and the state the room puts it in.
// Pure: no I/O.

import type { Status } from "./incident"

export type LineState = "running" | "stopped" | "fixing" | "gated"

export const STATIONS = [
  { key: "ingest", label: "Ingest", object: "RAW_ORDERS" },
  { key: "transform", label: "Transform", object: "LOAD_DAILY_REVENUE" },
  { key: "publish", label: "Publish", object: "EXEC_SCORECARD_V" },
] as const

/** The station that broke in this demo: the task between raw orders and the views. */
export const BROKEN_STATION = "transform"

/** Line state for a room. The andon always wins: a stopped line is stopped. */
export function lineFor(status: Status | undefined, frozen: boolean): LineState {
  if (frozen) return "stopped"
  switch (status) {
    case "SHIPPED":
    case "CLOSED":
      return "running"
    case "VOTING":
      return "gated"
    case "FIXING":
      return "fixing"
    default:
      return "stopped"
  }
}

/** Line state for the home page: from the latest run of the watched task. */
export function lineFromLatestRun(state: string | undefined): LineState {
  return state === "FAILED" ? "stopped" : "running"
}

export const LINE_COPY: Record<LineState, { title: string; detail: string }> = {
  stopped: { title: "Pipeline down", detail: "Data is stale downstream." },
  fixing: { title: "Fixing on a clone", detail: "Prod is untouched while agents work." },
  gated: { title: "Waiting on 2 approvals", detail: "A tested fix needs two people to say yes." },
  running: { title: "Pipeline healthy", detail: "Fresh data is flowing again." },
}

/** Who is typing right now, given the latest typing signal per person. Signals expire. */
export function typingNow(signals: Record<string, number>, now: number, you: string, ttlMs = 4000): string[] {
  return Object.entries(signals)
    .filter(([u, t]) => u !== you && now - t < ttlMs)
    .map(([u]) => u)
    .sort()
}

export type Flash = { target: "vote" | "line"; text: string; at: number }

/** Someone else's action that changed what you're looking at, so the UI can point at it. */
export function flashFor(e: { by: string; kind: string; at: number; payload?: unknown }, you: string): Flash | null {
  if (e.by.toUpperCase() === you.toUpperCase()) return null
  const p = (e.payload ?? {}) as { vote?: string }
  switch (e.kind) {
    case "vote":
      return { target: "vote", text: `${e.by} ${p.vote === "reject" ? "rejected" : "approved"}`, at: e.at }
    case "ship":
      return { target: "line", text: `${e.by} shipped it`, at: e.at }
    case "andon":
      return { target: "line", text: `${e.by} pulled the andon`, at: e.at }
    case "andon.clear":
      return { target: "line", text: `${e.by} cleared the andon`, at: e.at }
    default:
      return null
  }
}
