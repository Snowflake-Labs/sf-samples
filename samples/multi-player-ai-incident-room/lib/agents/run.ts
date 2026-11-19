// Run one Multi-player AI Incident Room agent and yield its events as they happen.
//  - Deployed: the Cortex Agent SDK, streamed, with the service token pinned to
//    INCIDENT_ROOM_AGENT (least privilege: the agents and their tools, nothing else). Each
//    agent keeps one Cortex thread per incident; attaching to it continues from the last
//    assistant message, so the agent remembers its own earlier tool calls and replies.
//  - Local dev: SNOWFLAKE.CORTEX.DATA_AGENT_RUN over the dev SQL connection (one reply,
//    no streaming, no thread), so no REST token is needed on a laptop.

import { CortexAgentClient, OAuthAuth, type Authenticator, type Conversation } from "@snowflake/cortex-agent-sdk"
import { getServiceToken, getSnowflakeBaseUrl, querySnowflake } from "@/lib/snowflake"
import type { AgentName } from "@/lib/domain/incident"
import { fromRunResponse, normalize, type AgentEvent } from "./stream"

const DB = "INCIDENT_ROOM"
const SCHEMA = "AGENTS"
const AGENT_ROLE = "INCIDENT_ROOM_AGENT"

/** An agent's Cortex thread for an incident. `parent` is kept for the store's shape; the SDK finds it. */
export interface ThreadState {
  thread: string
  parent: string
}

export interface AgentRun {
  agent: AgentName
  prompt: string
  signal: AbortSignal
  /** The agent's thread for this incident, if it has one. */
  thread?: ThreadState
  /** Called when a new thread is created, so the caller can keep it. */
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
  const baseUrl = getSnowflakeBaseUrl()
  if (!baseUrl) throw new Error("No Snowflake host")
  const oauth = new OAuthAuth(token)
  const auth: Authenticator = {
    getAuthHeaders: async () => ({ ...(await oauth.getAuthHeaders()), "X-Snowflake-Role": AGENT_ROLE }),
  }
  const client = new CortexAgentClient({ baseUrl, auth })
  yield* runWith(client, run)
}

/** The SDK runner, given a client. Exported so it can be exercised directly. */
export async function* runWith(client: CortexAgentClient, run: AgentRun): AsyncGenerator<AgentEvent> {
  const agent = { type: "data" as const, spec: { database: DB, schema: SCHEMA, name: run.agent } }
  const opts = { signal: run.signal }
  let convo: Conversation
  if (run.thread) {
    convo = await client.conversations.attach(run.thread.thread, { agent }, opts)
  } else {
    convo = await client.conversations.create({ thread: { origin_application: "incident_room" }, agent }, opts)
    await run.onThread?.({ thread: convo.id, parent: "0" })
  }
  const turn = await convo.send(run.prompt, { requestOpts: opts })
  for await (const ev of turn.events({ signal: run.signal })) {
    const { event, ...rest } = ev as { event: string } & Record<string, unknown>
    const e = normalize({ event, data: JSON.stringify(rest) })
    if (e) yield e
  }
}

async function* runLocal(run: AgentRun): AsyncGenerator<AgentEvent> {
  const body = { messages: [{ role: "user", content: [{ type: "text", text: run.prompt }] }] }
  const [row] = await querySnowflake(`SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN(?, ?) AS R`, {
    binds: [`${DB}.${SCHEMA}.${run.agent}`, JSON.stringify(body)],
  })
  const reply = JSON.parse(String(row?.R ?? "{}"))
  for (const e of fromRunResponse(reply)) yield e
}
