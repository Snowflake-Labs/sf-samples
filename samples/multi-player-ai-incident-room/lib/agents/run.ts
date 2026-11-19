// Run one Multi-player AI Incident Room agent and yield its events as they happen.
//  - Deployed: Cortex Agents REST run API, streamed, with the service token pinned to
//    INCIDENT_ROOM_AGENT (least privilege: the agents and their tools, nothing else). Each
//    agent keeps one Cortex thread per incident, so it remembers its own earlier tool
//    calls and replies; each turn continues from its last assistant message.
//  - Local dev: SNOWFLAKE.CORTEX.DATA_AGENT_RUN over the dev SQL connection (one reply,
//    no streaming, no thread), so no REST token is needed on a laptop.

import { getServiceToken, getSnowflakeBaseUrl, querySnowflake } from "@/lib/snowflake"
import type { AgentName } from "@/lib/domain/incident"
import { fromRunResponse, normalize, SseParser, type AgentEvent } from "./stream"

const DB = "INCIDENT_ROOM"
const SCHEMA = "AGENTS"
const AGENT_ROLE = "INCIDENT_ROOM_AGENT"

/**
 * Where an agent's thread stands: the thread, and the assistant message to continue from.
 * Both are digit strings: message ids exceed 2^53, so a JS number would round them.
 */
export interface ThreadState {
  thread: string
  parent: string
}

const DIGITS = /^\d+$/

export interface AgentRun {
  agent: AgentName
  prompt: string
  signal: AbortSignal
  /** The agent's thread for this incident, if it has one. */
  thread?: ThreadState
  /** Called after each turn that stored an assistant message, so the caller can keep it. */
  onThread?: (t: ThreadState) => Promise<void>
}

/** True when agent runs keep a thread (deployed), so prompts can send only what's new. */
export const usesThreads = () => !!getServiceToken()

export async function* runAgent(run: AgentRun): AsyncGenerator<AgentEvent> {
  const token = getServiceToken()
  if (!token) {
    yield* runLocal(run)
    return
  }
  const base = getSnowflakeBaseUrl()
  if (!base) throw new Error("No Snowflake host")
  yield* runRest(base, {
    Authorization: `Bearer ${token}`,
    "X-Snowflake-Authorization-Token-Type": "OAUTH",
    "X-Snowflake-Role": AGENT_ROLE,
  }, run)
}

/** The REST runner, given base URL and auth headers. Exported so it can be exercised directly. */
export async function* runRest(base: string, auth: Record<string, string>, run: AgentRun): AsyncGenerator<AgentEvent> {
  const thread = run.thread ?? { thread: await createThread(base, auth, run.signal), parent: "0" }
  const res = await fetch(`${base}/api/v2/databases/${DB}/schemas/${SCHEMA}/agents/${run.agent}:run`, {
    method: "POST",
    signal: run.signal,
    headers: { ...auth, "Content-Type": "application/json", Accept: "text/event-stream" },
    body: runBody(thread, run.prompt),
  })
  if (!res.ok || !res.body) throw new Error(`Agent ${run.agent} run failed: HTTP ${res.status} ${(await res.text()).slice(0, 300)}`)
  const parser = new SseParser()
  const decoder = new TextDecoder()
  const reader = res.body.getReader()
  let assistant: string | undefined
  for (;;) {
    const { done, value } = await reader.read()
    if (done) break
    for (const raw of parser.push(decoder.decode(value, { stream: true }))) {
      if (raw.event === "metadata") assistant = assistantMessageId(raw.data) ?? assistant
      const e = normalize(raw)
      if (e) yield e
    }
  }
  // Continue from this turn next time. If the assistant message wasn't stored, keep the
  // last good parent (per the threads guide) so the next turn still has a valid anchor.
  const next = { thread: thread.thread, parent: assistant ?? thread.parent }
  if (!run.thread || next.parent !== run.thread.parent) await run.onThread?.(next)
}

/** The run request body, with the thread ids written as exact JSON integers. */
export function runBody(t: ThreadState, prompt: string): string {
  if (!DIGITS.test(t.thread) || !DIGITS.test(t.parent)) throw new Error("Bad thread state")
  const json = JSON.stringify({
    thread_id: "@T",
    parent_message_id: "@P",
    messages: [{ role: "user", content: [{ type: "text", text: prompt }] }],
    stream: true,
  })
  return json.replace('"thread_id":"@T"', `"thread_id":${t.thread}`).replace('"parent_message_id":"@P"', `"parent_message_id":${t.parent}`)
}

/** The assistant message id from a `metadata` event's raw JSON, read as text so it isn't rounded. */
export function assistantMessageId(data: string): string | undefined {
  if (!/"role"\s*:\s*"assistant"/.test(data)) return undefined
  return data.match(/"message_id"\s*:\s*(\d+)/)?.[1]
}

async function createThread(base: string, auth: Record<string, string>, signal: AbortSignal): Promise<string> {
  const res = await fetch(`${base}/api/v2/cortex/threads`, {
    method: "POST",
    signal,
    headers: { ...auth, "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify({ origin_application: "incident_room" }),
  })
  const text = await res.text()
  const id = text.match(/"thread_id"\s*:\s*"?(\d+)/)?.[1]
  if (!res.ok || !id) throw new Error(`Create thread failed: HTTP ${res.status} ${text.slice(0, 300)}`)
  return id
}

async function* runLocal(run: AgentRun): AsyncGenerator<AgentEvent> {
  const body = { messages: [{ role: "user", content: [{ type: "text", text: run.prompt }] }] }
  const [row] = await querySnowflake(`SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN(?, ?) AS R`, {
    binds: [`${DB}.${SCHEMA}.${run.agent}`, JSON.stringify(body)],
  })
  const reply = JSON.parse(String(row?.R ?? "{}"))
  for (const e of fromRunResponse(reply)) yield e
}
