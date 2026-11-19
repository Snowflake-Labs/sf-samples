import { describe, expect, it } from "vitest"
import { assistantMessageId, runBody } from "@/lib/agents/run"

describe("thread ids beyond 2^53", () => {
  it("reads the assistant message id without rounding", () => {
    expect(assistantMessageId('{"metadata":{"role":"assistant","message_id":18047963679850017}}')).toBe("18047963679850017")
  })
  it("ignores the user message id", () => {
    expect(assistantMessageId('{"metadata":{"role":"user","message_id":5}}')).toBeUndefined()
  })
  it("writes exact integers into the run body", () => {
    const body = runBody({ thread: "275390082713", parent: "18047963679850017" }, "hi")
    expect(body).toContain('"thread_id":275390082713')
    expect(body).toContain('"parent_message_id":18047963679850017')
    expect(JSON.parse(body).messages[0].content[0].text).toBe("hi")
  })
  it("rejects non-numeric thread state", () => {
    expect(() => runBody({ thread: "1,2", parent: "0" }, "x")).toThrow()
  })
})
