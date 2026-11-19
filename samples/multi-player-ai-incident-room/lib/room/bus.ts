// In-process pub/sub and presence for incident rooms. The app runs as a single
// instance (app.yml: max_instances 1), so one in-memory bus is enough; every event
// is also persisted to INCIDENT_ROOM.APP.EVENTS so a reload can replay the room.

export interface RoomEvent {
  id: string
  incidentId: string
  at: number
  by: string
  kind: string
  payload?: unknown
}

type Sub = (e: RoomEvent) => void

interface State {
  subs: Map<string, Set<Sub>>
  presence: Map<string, Map<string, number>>
  running: Map<string, AbortController> // `${incidentId}:${agent}`
}

const KEY = "__INCIDENT_ROOM_BUS__"
function state(): State {
  const g = globalThis as unknown as Record<string, State | undefined>
  return (g[KEY] ??= { subs: new Map(), presence: new Map(), running: new Map() })
}

export function subscribe(incidentId: string, fn: Sub): () => void {
  const s = state()
  if (!s.subs.has(incidentId)) s.subs.set(incidentId, new Set())
  s.subs.get(incidentId)!.add(fn)
  return () => {
    s.subs.get(incidentId)?.delete(fn)
    if (!s.subs.get(incidentId)?.size) s.subs.delete(incidentId)
  }
}

export function publish(e: RoomEvent): void {
  for (const fn of state().subs.get(e.incidentId) ?? []) {
    try {
      fn(e)
    } catch {
      /* a broken subscriber must not stop the others */
    }
  }
}

export function markPresent(incidentId: string, user: string): void {
  const s = state()
  if (!s.presence.has(incidentId)) s.presence.set(incidentId, new Map())
  s.presence.get(incidentId)!.set(user, Date.now())
}

export function markAbsent(incidentId: string, user: string): void {
  state().presence.get(incidentId)?.delete(user)
}

export function present(incidentId: string): string[] {
  const m = state().presence.get(incidentId)
  if (!m) return []
  const now = Date.now()
  for (const [u, t] of m) if (now - t > 90_000) m.delete(u)
  return [...m.keys()].sort()
}

/** Claim an agent's slot in a room. Returns null if that agent is already working there. */
export function claimAgent(incidentId: string, agent: string): AbortController | null {
  const s = state()
  const k = `${incidentId}:${agent}`
  if (s.running.has(k)) return null
  const c = new AbortController()
  s.running.set(k, c)
  return c
}

export function releaseAgent(incidentId: string, agent: string, c: AbortController): void {
  const s = state()
  const k = `${incidentId}:${agent}`
  if (s.running.get(k) === c) s.running.delete(k)
}

export function runningAgents(incidentId: string): string[] {
  return [...state().running.keys()].filter((k) => k.startsWith(`${incidentId}:`)).map((k) => k.split(":")[1])
}

/** Andon: abort every agent working in the room. */
export function stopAll(incidentId: string): string[] {
  const s = state()
  const stopped: string[] = []
  for (const [k, c] of s.running) {
    if (k.startsWith(`${incidentId}:`)) {
      c.abort()
      s.running.delete(k)
      stopped.push(k.split(":")[1])
    }
  }
  return stopped
}
