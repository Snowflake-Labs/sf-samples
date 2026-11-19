// Cortex Agent run events -> the room's agent events. Pure; shared by the REST (streaming)
// and SQL (DATA_AGENT_RUN) runners so the UI sees one shape.

export type AgentEvent =
  | { type: "text"; text: string }
  | { type: "tool"; name: string; input: unknown; id?: string }
  | { type: "tool_result"; name: string; id?: string; ok: boolean; result: unknown }
  | { type: "error"; message: string }

export interface RawSse {
  event: string
  data: string
}

/** Incremental text/event-stream parser. */
export class SseParser {
  private buf = ""

  push(chunk: string): RawSse[] {
    this.buf += chunk.replace(/\r\n/g, "\n")
    const out: RawSse[] = []
    let i: number
    while ((i = this.buf.indexOf("\n\n")) >= 0) {
      const block = this.buf.slice(0, i)
      this.buf = this.buf.slice(i + 2)
      let event = "message"
      const data: string[] = []
      for (const line of block.split("\n")) {
        if (!line || line.startsWith(":")) continue
        const c = line.indexOf(":")
        const field = c < 0 ? line : line.slice(0, c)
        const value = c < 0 ? "" : line.slice(c + 1).replace(/^ /, "")
        if (field === "event") event = value
        else if (field === "data") data.push(value)
      }
      if (data.length) out.push({ event, data: data.join("\n") })
    }
    return out
  }
}

function parse(s: string): unknown {
  try {
    return JSON.parse(s)
  } catch {
    return s
  }
}

/** A tool result's payload: the procedure's return value, JSON-decoded when it is JSON. */
function resultOf(content: unknown): unknown {
  if (!Array.isArray(content) || !content.length) return null
  const first = content[0] as { json?: { result?: unknown }; text?: string }
  if (first.json && "result" in first.json) {
    const r = first.json.result
    return typeof r === "string" ? parse(r) : r
  }
  if (first.json) return first.json
  return first.text ?? null
}

type ToolUse = { name?: string; input?: unknown; tool_use_id?: string }
type ToolResult = { name?: string; tool_use_id?: string; status?: string; content?: unknown }

const toolEvent = (t: ToolUse): AgentEvent => ({ type: "tool", name: String(t.name), input: t.input ?? {}, id: t.tool_use_id })
const resultEvent = (t: ToolResult): AgentEvent => ({
  type: "tool_result",
  name: String(t.name),
  id: t.tool_use_id,
  ok: t.status !== "error",
  result: resultOf(t.content),
})

export function normalize(raw: RawSse): AgentEvent | null {
  const d = parse(raw.data) as Record<string, unknown>
  switch (raw.event) {
    case "response.text.delta":
      return typeof d?.text === "string" ? { type: "text", text: d.text } : null
    case "response.tool_use":
      return toolEvent(d as ToolUse)
    case "response.tool_result":
      return resultEvent(d as ToolResult)
    case "error":
      return { type: "error", message: String(d?.message ?? raw.data) }
    default:
      return null
  }
}

/** Events from a whole (non-streaming) run response, as returned by DATA_AGENT_RUN. */
export function fromRunResponse(reply: { content?: unknown[] }): AgentEvent[] {
  const out: AgentEvent[] = []
  for (const c of (reply.content ?? []) as Record<string, unknown>[]) {
    if (c.type === "text" && typeof c.text === "string") out.push({ type: "text", text: c.text })
    else if (c.type === "tool_use") out.push(toolEvent(c.tool_use as ToolUse))
    else if (c.type === "tool_result") out.push(resultEvent(c.tool_result as ToolResult))
  }
  return out
}
