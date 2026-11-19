// Room state in INCIDENT_ROOM.APP. Every value is bound, never interpolated.

import { randomUUID } from "node:crypto"
import { querySnowflake } from "@/lib/snowflake"
import type { Incident, Patch, Status, Vote } from "@/lib/domain/incident"
import type { RoomEvent } from "./bus"

export interface IncidentRow extends Incident {
  title: string
  objectFqn: string | null
  errorMessage: string | null
  openedBy: string
  openedAt: string
}

export interface PatchRow extends Patch {
  taskSql: string
  cloneSchema: string
  testResult: unknown
  shippedAt: string | null
  shippedBy: string | null
}

const iso = (v: unknown) => (v instanceof Date ? v.toISOString() : v == null ? null : String(v))

function toIncident(r: Record<string, unknown>): IncidentRow {
  return {
    id: String(r.ID),
    title: String(r.TITLE ?? ""),
    objectFqn: (r.OBJECT_FQN as string) ?? null,
    errorMessage: (r.ERROR_MESSAGE as string) ?? null,
    status: String(r.STATUS) as Status,
    frozen: r.FROZEN === true,
    frozenBy: (r.FROZEN_BY as string) ?? undefined,
    frozenReason: (r.FROZEN_REASON as string) ?? undefined,
    requiredApprovals: Number(r.REQUIRED_APPROVALS ?? 2),
    openedBy: String(r.OPENED_BY ?? ""),
    openedAt: iso(r.OPENED_AT) ?? "",
  }
}

const json = (v: unknown) => (typeof v === "string" ? JSON.parse(v) : v)

export function newIncidentId(now = new Date()): string {
  const d = now.toISOString().slice(0, 10).replace(/-/g, "")
  return `INC-${d}-${randomUUID().slice(0, 4).toUpperCase()}`
}

export async function createIncident(i: { title: string; objectFqn?: string; errorMessage?: string; openedBy: string; requiredApprovals: number }) {
  const id = newIncidentId()
  await querySnowflake(
    `INSERT INTO INCIDENT_ROOM.APP.INCIDENTS (ID, TITLE, OBJECT_FQN, ERROR_MESSAGE, STATUS, REQUIRED_APPROVALS, OPENED_BY)
     SELECT ?, ?, ?, ?, 'TRIAGE', ?, ?`,
    { binds: [id, i.title, i.objectFqn ?? null, i.errorMessage ?? null, i.requiredApprovals, i.openedBy] },
  )
  return id
}

export async function listIncidents(): Promise<IncidentRow[]> {
  const rows = await querySnowflake(`SELECT * FROM INCIDENT_ROOM.APP.INCIDENTS ORDER BY OPENED_AT DESC LIMIT 50`)
  return rows.map(toIncident)
}

export async function getIncident(id: string): Promise<IncidentRow | null> {
  const [r] = await querySnowflake(`SELECT * FROM INCIDENT_ROOM.APP.INCIDENTS WHERE ID = ?`, { binds: [id] })
  return r ? toIncident(r) : null
}

export async function saveIncidentState(i: Incident & { closed?: boolean }) {
  await querySnowflake(
    `UPDATE INCIDENT_ROOM.APP.INCIDENTS SET STATUS = ?, FROZEN = ?, FROZEN_BY = ?, FROZEN_REASON = ?,
       CLOSED_AT = IFF(? , CURRENT_TIMESTAMP(), CLOSED_AT) WHERE ID = ?`,
    { binds: [i.status, i.frozen, i.frozenBy ?? null, i.frozenReason ?? null, i.status === "CLOSED", i.id] },
  )
}

export async function appendEvent(e: RoomEvent) {
  await querySnowflake(
    `INSERT INTO INCIDENT_ROOM.APP.EVENTS (ID, INCIDENT_ID, AT, BY_WHO, KIND, PAYLOAD)
     SELECT ?, ?, TO_TIMESTAMP_LTZ(?, 3), ?, ?, PARSE_JSON(?)`,
    { binds: [e.id, e.incidentId, e.at, e.by, e.kind, JSON.stringify(e.payload ?? null)] },
  )
}

export async function listEvents(incidentId: string): Promise<RoomEvent[]> {
  const rows = await querySnowflake(
    `SELECT ID, INCIDENT_ID, DATE_PART(epoch_millisecond, AT) AS AT_MS, BY_WHO, KIND, PAYLOAD
     FROM INCIDENT_ROOM.APP.EVENTS WHERE INCIDENT_ID = ? ORDER BY AT, ID LIMIT 2000`,
    { binds: [incidentId] },
  )
  return rows.map((r) => ({
    id: String(r.ID),
    incidentId: String(r.INCIDENT_ID),
    at: Number(r.AT_MS),
    by: String(r.BY_WHO),
    kind: String(r.KIND),
    payload: json(r.PAYLOAD),
  }))
}

function toPatch(r: Record<string, unknown>): PatchRow {
  return {
    id: String(r.ID),
    incidentId: String(r.INCIDENT_ID),
    testedOk: r.TESTED_OK === true,
    taskSql: String(r.TASK_SQL ?? ""),
    cloneSchema: String(r.CLONE_SCHEMA ?? ""),
    testResult: json(r.TEST_RESULT),
    shippedAt: iso(r.SHIPPED_AT),
    shippedBy: (r.SHIPPED_BY as string) ?? null,
  }
}

export async function getPatch(id: string): Promise<PatchRow | null> {
  const [r] = await querySnowflake(`SELECT * FROM INCIDENT_ROOM.APP.PATCHES WHERE ID = ?`, { binds: [id] })
  return r ? toPatch(r) : null
}

/** The newest clone-tested patch for an incident: the one up for a vote. */
export async function currentPatch(incidentId: string): Promise<PatchRow | null> {
  const [r] = await querySnowflake(
    `SELECT * FROM INCIDENT_ROOM.APP.PATCHES WHERE INCIDENT_ID = ? AND TESTED_OK ORDER BY PROPOSED_AT DESC LIMIT 1`,
    { binds: [incidentId] },
  )
  return r ? toPatch(r) : null
}

export async function listVotes(patchId: string): Promise<Vote[]> {
  const rows = await querySnowflake(
    `SELECT PATCH_ID, UPPER(VOTER) AS VOTER, VOTE FROM INCIDENT_ROOM.APP.VOTES WHERE PATCH_ID = ?
     QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(VOTER) ORDER BY AT DESC) = 1`,
    { binds: [patchId] },
  )
  return rows.map((r) => ({ patchId: String(r.PATCH_ID), voter: String(r.VOTER), vote: r.VOTE as Vote["vote"] }))
}

export async function insertVote(v: Vote) {
  await querySnowflake(`INSERT INTO INCIDENT_ROOM.APP.VOTES (PATCH_ID, VOTER, VOTE) SELECT ?, ?, ?`, {
    binds: [v.patchId, v.voter.toUpperCase(), v.vote],
  })
}

export async function shipFix(patchId: string, shippedBy: string): Promise<{ ok: boolean; error?: string; [k: string]: unknown }> {
  const [r] = await querySnowflake(`CALL INCIDENT_ROOM.AGENTS.SHIP_FIX(?, ?)`, { binds: [patchId, shippedBy] })
  return json(Object.values(r ?? {})[0]) as { ok: boolean; error?: string }
}

export async function dropClone(cloneSchema: string) {
  if (!/^INCIDENT_ROOM_CLONES\.[A-Z0-9_]{1,64}$/.test(cloneSchema)) throw new Error(`Refusing to drop ${cloneSchema}`)
  await querySnowflake(`DROP SCHEMA IF EXISTS IDENTIFIER(?)`, { binds: [cloneSchema] })
}

export async function recentFailures(): Promise<{ task: string; state: string; scheduled_time: string; error_message: string | null }[]> {
  const [r] = await querySnowflake(`CALL INCIDENT_ROOM.AGENTS.RECENT_FAILURES()`)
  return (json(Object.values(r ?? {})[0]) as never[]) ?? []
}

export async function latestPostmortem(incidentId: string): Promise<string | null> {
  const [r] = await querySnowflake(
    `SELECT BODY FROM INCIDENT_ROOM.APP.POSTMORTEMS WHERE INCIDENT_ID = ? ORDER BY WRITTEN_AT DESC LIMIT 1`,
    { binds: [incidentId] },
  )
  return r ? String(r.BODY) : null
}

/** The agent's Cortex thread for this incident and the assistant message to continue from. */
export async function getThread(incidentId: string, agent: string): Promise<{ thread: string; parent: string } | undefined> {
  // Ids are stored as strings: message ids exceed 2^53 and would round as numbers.
  const [r] = await querySnowflake(
    `SELECT THREADS[?]:thread::STRING AS T, THREADS[?]:parent::STRING AS P FROM INCIDENT_ROOM.APP.INCIDENTS WHERE ID = ?`,
    { binds: [agent, agent, incidentId] },
  )
  return r?.T ? { thread: String(r.T), parent: String(r.P ?? "0") } : undefined
}

export async function saveThread(incidentId: string, agent: string, t: { thread: string; parent: string }) {
  await querySnowflake(
    `UPDATE INCIDENT_ROOM.APP.INCIDENTS SET THREADS = OBJECT_INSERT(COALESCE(THREADS, OBJECT_CONSTRUCT()), ?, PARSE_JSON(?), TRUE) WHERE ID = ?`,
    { binds: [agent, JSON.stringify(t), incidentId] },
  )
}

/** Append-only: a decision the team filed during the incident. */
export async function insertDecision(d: { id: string; incidentId: string; filedBy: string; participants: string[]; body: string }) {
  await querySnowflake(
    `INSERT INTO INCIDENT_ROOM.APP.DECISIONS (ID, INCIDENT_ID, FILED_BY, PARTICIPANTS, BODY)
     SELECT ?, ?, ?, PARSE_JSON(?), ?`,
    { binds: [d.id, d.incidentId, d.filedBy, JSON.stringify(d.participants), d.body] },
  )
}
