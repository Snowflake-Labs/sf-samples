import { describe, it, expect, vi, beforeEach } from "vitest"
import { GET } from "../../app/api/query/route"

vi.mock("../../lib/snowflake", () => ({
  querySnowflake: vi.fn(),
}))

import { querySnowflake } from "../../lib/snowflake"

const QUERY = `SELECT CURRENT_USER() AS "USER", CURRENT_ROLE() AS ROLE`

describe("GET /api/query", () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it("returns service and caller results on success", async () => {
    vi.mocked(querySnowflake)
      .mockResolvedValueOnce([{ USER: "SERVICE_USER", ROLE: "SERVICE_ROLE" }])
      .mockResolvedValueOnce([{ USER: "CALLER_USER", ROLE: "CALLER_ROLE" }])

    const response = await GET()
    const json = await response.json()

    expect(response.status).toBe(200)
    expect(json.service).toEqual({ mode: "service", result: { USER: "SERVICE_USER", ROLE: "SERVICE_ROLE" } })
    expect(json.caller).toEqual({ mode: "caller", result: { USER: "CALLER_USER", ROLE: "CALLER_ROLE" } })
    expect(querySnowflake).toHaveBeenCalledWith(QUERY)
    expect(querySnowflake).toHaveBeenCalledWith(QUERY, { callersRights: true })
  })

  it("includes caller error when callersRights query throws", async () => {
    vi.mocked(querySnowflake)
      .mockResolvedValueOnce([{ USER: "SERVICE_USER", ROLE: "SERVICE_ROLE" }])
      .mockRejectedValueOnce(new Error("No sf-context-current-user-token header"))

    const response = await GET()
    const json = await response.json()

    expect(response.status).toBe(200)
    expect(json.service.result).toEqual({ USER: "SERVICE_USER", ROLE: "SERVICE_ROLE" })
    expect(json.caller.mode).toBe("caller")
    expect(json.caller.result).toBeNull()
    expect(json.caller.error).toContain("No sf-context-current-user-token")
  })

  it("returns null results when queries return empty rows", async () => {
    vi.mocked(querySnowflake).mockResolvedValue([])

    const response = await GET()
    const json = await response.json()

    expect(response.status).toBe(200)
    expect(json.service.result).toBeNull()
    expect(json.caller.result).toBeNull()
  })

  it("returns 500 when service query throws", async () => {
    vi.mocked(querySnowflake).mockRejectedValue(new Error("DB connection failed"))
    vi.spyOn(console, "error").mockImplementation(() => {})

    const response = await GET()
    const json = await response.json()

    expect(response.status).toBe(500)
    expect(json.error).toBe("Query failed")
  })
})
