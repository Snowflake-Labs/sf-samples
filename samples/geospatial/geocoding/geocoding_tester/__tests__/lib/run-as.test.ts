import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"

vi.mock("../../lib/snowflake", () => ({
  querySnowflake: vi.fn(),
}))

import { querySnowflake } from "../../lib/snowflake"
import { isCallerAccessError, quoteIdent, resolveDatabase, withCallerFallback } from "../../lib/run-as"

describe("withCallerFallback", () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.stubEnv("NODE_ENV", "production")
  })
  afterEach(() => {
    vi.unstubAllEnvs()
    vi.restoreAllMocks()
  })

  it("labels a successful caller's-rights call as caller", async () => {
    const load = vi.fn().mockResolvedValue({ n: 1 })
    expect(await withCallerFallback(load)).toEqual({ n: 1, identity: "caller" })
    expect(load).toHaveBeenCalledWith({ callersRights: true })
  })

  it("falls back to the service role on a privilege error and logs it", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {})
    const err = new Error("SQL access control error: Insufficient privileges to operate on warehouse")
    const load = vi.fn().mockRejectedValueOnce(err).mockResolvedValueOnce({ n: 2 })
    expect(await withCallerFallback(load)).toEqual({ n: 2, identity: "service" })
    expect(load).toHaveBeenLastCalledWith({})
    expect(warn).toHaveBeenCalledWith(expect.any(String), err)
  })

  it("rethrows errors that are not about access", async () => {
    const err = new Error("SQL compilation error: syntax error line 1")
    const load = vi.fn().mockRejectedValue(err)
    await expect(withCallerFallback(load)).rejects.toBe(err)
    expect(load).toHaveBeenCalledTimes(1)
  })

  it("never labels dev or preview results as caller", async () => {
    vi.stubEnv("NODE_ENV", "development")
    const load = vi.fn().mockResolvedValue({ n: 3 })
    expect(await withCallerFallback(load)).toEqual({ n: 3, identity: "dev" })
    expect(load).toHaveBeenCalledWith({})
  })
})

describe("run-as helpers", () => {
  beforeEach(() => vi.clearAllMocks())

  it("recognizes access errors", () => {
    expect(isCallerAccessError(new Error("Object does not exist or not authorized."))).toBe(true)
    expect(isCallerAccessError(new Error("No sf-context-current-user-token header."))).toBe(true)
    expect(isCallerAccessError(new Error("Statement reached its statement timeout"))).toBe(false)
  })

  it("quotes identifiers", () => {
    expect(quoteIdent("My Db")).toBe('"My Db"')
    expect(quoteIdent('a"b')).toBe('"a""b"')
  })

  it("resolves a quoted database name", async () => {
    vi.mocked(querySnowflake)
      .mockResolvedValueOnce([{ DB: null }])
      .mockResolvedValueOnce([{ name: "Mixed_Case" }])
    expect(await resolveDatabase()).toBe('"Mixed_Case"')
  })
})
