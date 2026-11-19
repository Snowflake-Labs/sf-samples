import { describe, expect, it } from "vitest"
import { fromRunResponse, normalize, SseParser } from "@/lib/agents/stream"

describe("SseParser", () => {
  it("joins events split across chunks and multi-line data", () => {
    const p = new SseParser()
    const got = [
      ...p.push('event: response.text.delta\ndata: {"text":"Hel'),
      ...p.push('lo"}\n\nevent: response.tool_use\ndata: {"name":"inspect_pipeline",\ndata: "input":{}}\n\n'),
    ]
    expect(got).toEqual([
      { event: "response.text.delta", data: '{"text":"Hello"}' },
      { event: "response.tool_use", data: '{"name":"inspect_pipeline",\n"input":{}}' },
    ])
  })
  it("ignores comments and keeps an unterminated tail for the next chunk", () => {
    const p = new SseParser()
    expect(p.push(": keepalive\n\nevent: x\ndata: 1")).toEqual([])
    expect(p.push("\n\n")).toEqual([{ event: "x", data: "1" }])
  })
})

describe("normalize", () => {
  it("maps text deltas, tool calls, and tool results; drops the rest", () => {
    expect(normalize({ event: "response.text.delta", data: '{"text":"hi"}' })).toEqual({ type: "text", text: "hi" })
    expect(normalize({ event: "response.tool_use", data: '{"name":"downstream","input":{"table_name":"X"},"tool_use_id":"t1"}' })).toEqual({
      type: "tool",
      name: "downstream",
      input: { table_name: "X" },
      id: "t1",
    })
    const res = JSON.stringify({
      name: "downstream",
      tool_use_id: "t1",
      status: "success",
      content: [{ type: "json", json: { result: '[{"object":"A.B.V","hops":1}]' } }],
    })
    expect(normalize({ event: "response.tool_result", data: res })).toEqual({
      type: "tool_result",
      name: "downstream",
      id: "t1",
      ok: true,
      result: [{ object: "A.B.V", hops: 1 }],
    })
    expect(normalize({ event: "response.status", data: '{"status":"planning"}' })).toBeNull()
    expect(normalize({ event: "error", data: '{"message":"boom"}' })).toEqual({ type: "error", message: "boom" })
  })
})

describe("fromRunResponse", () => {
  it("turns a non-streaming DATA_AGENT_RUN reply into the same events, in order", () => {
    const reply = {
      content: [
        { type: "thinking", thinking: { text: "..." } },
        { type: "tool_use", tool_use: { name: "recent_failures", input: {}, tool_use_id: "a" } },
        { type: "tool_result", tool_result: { name: "recent_failures", tool_use_id: "a", status: "success", content: [{ json: { result: "[]" } }] } },
        { type: "text", text: "Nothing failed." },
      ],
    }
    expect(fromRunResponse(reply)).toEqual([
      { type: "tool", name: "recent_failures", input: {}, id: "a" },
      { type: "tool_result", name: "recent_failures", id: "a", ok: true, result: [] },
      { type: "text", text: "Nothing failed." },
    ])
  })
})
