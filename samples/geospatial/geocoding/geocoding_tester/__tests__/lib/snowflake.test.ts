import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"
import fs from "fs"
import os from "os"
import path from "path"

import {
  buildCallerRightsToken,
  readTomlDefaultConnection,
  resetTomlConfigCache,
  getSecret,
  SecretType,
} from "../../lib/snowflake"

// ---------------------------------------------------------------------------
// Module-level mocks (hoisted)
// ---------------------------------------------------------------------------

// Automock fs and os for readTomlDefaultConnection tests (setupFiles helper below).
// The new querySnowflake / REST-helper tests use vi.resetModules() + vi.doMock()
// inside their own beforeEach so they get fresh module instances — these top-level
// mocks don't interfere with those tests.
vi.mock("fs")
vi.mock("os")

// smol-toml is loaded via require() inside the function; provide it via mock
vi.mock("smol-toml", () => {
  const { parse } = require("smol-toml") as { parse: (s: string) => Record<string, any> }
  return { parse }
})

vi.mock("next/headers", () => ({
  headers: vi.fn(() => new Headers()),
}))

// createPool mock used by querySnowflake retry tests.
// mockExecuteImpl is set per-test inside the describe block below.
let mockExecuteImpl: (opts: {
  sqlText: string
  parameters?: Record<string, unknown>
  binds?: unknown[]
  complete: (err: Error | null, stmt: Record<string, unknown> | null, rows: Record<string, unknown>[] | null) => void
}) => void

vi.mock("snowflake-sdk", () => ({
  default: {
    configure: vi.fn(),
    createPool: vi.fn(() => ({
      use: (fn: (conn: { execute: typeof mockExecuteImpl; destroy: (cb?: () => void) => void }) => Promise<unknown>) => {
        const conn = {
          execute: (opts: Parameters<typeof mockExecuteImpl>[0]) => mockExecuteImpl(opts),
          destroy: vi.fn((cb?: () => void) => cb?.()),
        }
        return fn(conn)
      },
      drain: vi.fn(),
    })),
  },
}))

// ---------------------------------------------------------------------------
// readTomlDefaultConnection helpers
// ---------------------------------------------------------------------------

const HOME = "/mock-home"

function setupFiles(files: Record<string, string>) {
  vi.mocked(os.homedir).mockReturnValue(HOME)
  vi.mocked(fs.existsSync).mockImplementation((p) => {
    return typeof p === "string" && p in files
  })
  vi.mocked(fs.readFileSync).mockImplementation((p, _enc) => {
    const content = files[String(p)]
    if (content === undefined) throw new Error(`ENOENT: ${p}`)
    return content
  })
}

const ENV_KEYS_TO_CLEAR = [
  "SNOWFLAKE_CONNECTION_NAME",
  "SNOWFLAKE_HOME",
  "SNOWFLAKE_ACCOUNT",
  "SNOWFLAKE_USER",
  "SNOWFLAKE_PASSWORD",
  "SNOWFLAKE_DATABASE",
  "SNOWFLAKE_SCHEMA",
  "SNOWFLAKE_ROLE",
  "SNOWFLAKE_WAREHOUSE",
]

// fs mock factory that blocks toml config reads (used by URL / auth header tests)
function makeNoTomlFsMock() {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const actual = require("fs") as typeof import("fs")
  const overrides = {
    existsSync: (p: string) => {
      if (p.endsWith("connections.toml") || p.endsWith("config.toml")) return false
      return actual.existsSync(p)
    },
    readFileSync: actual.readFileSync,
  }
  return { default: { ...actual, ...overrides }, ...overrides }
}

// ---------------------------------------------------------------------------
// readTomlDefaultConnection
// ---------------------------------------------------------------------------

describe("readTomlDefaultConnection", () => {
  beforeEach(() => {
    resetTomlConfigCache()
    for (const key of ENV_KEYS_TO_CLEAR) {
      delete process.env[key]
    }
  })

  afterEach(() => {
    vi.restoreAllMocks()
    delete process.env.SNOWFLAKE_RANDOM_KEY
  })

  it("returns null when neither file exists", () => {
    setupFiles({})
    expect(readTomlDefaultConnection()).toBeNull()
  })

  it("reads connections from connections.toml (legacy top-level format)", () => {
    setupFiles({
      [path.join(HOME, ".snowflake", "connections.toml")]: `
        default_connection_name = "prod"
        [prod]
        account = "myaccount"
        user = "myuser"
        password = "mypass"
        warehouse = "mywh"
      `,
    })

    const conn = readTomlDefaultConnection()
    expect(conn).not.toBeNull()
    expect(conn!.account).toBe("myaccount")
    expect(conn!.user).toBe("myuser")
    expect(conn!.warehouse).toBe("mywh")
  })

  it("reads connections from config.toml legacy layout when connections.toml is absent", () => {
    setupFiles({
      [path.join(HOME, ".snowflake", "config.toml")]: `
        default_connection_name = "legacy"
        [connections.legacy]
        account = "legacy-acct"
        user = "legacy-user"
        password = "legacy-pass"
      `,
    })

    const conn = readTomlDefaultConnection()
    expect(conn).not.toBeNull()
    expect(conn!.account).toBe("legacy-acct")
    expect(conn!.user).toBe("legacy-user")
  })

  it("prefers connections.toml over config.toml when both exist", () => {
    setupFiles({
      [path.join(HOME, ".snowflake", "connections.toml")]: `
        [modern]
        account = "modern-acct"
        user = "modern-user"
        password = "modern-pass"
      `,
      [path.join(HOME, ".snowflake", "config.toml")]: `
        [connections.legacy]
        account = "legacy-acct"
        user = "legacy-user"
        password = "legacy-pass"
      `,
    })

    const conn = readTomlDefaultConnection()
    expect(conn).not.toBeNull()
    expect(conn!.account).toBe("modern-acct")
  })

  it("picks up default_connection_name from config.toml when connections.toml has none", () => {
    setupFiles({
      [path.join(HOME, ".snowflake", "connections.toml")]: `
        [alpha]
        account = "alpha-acct"
        user = "alpha-user"
        password = "alpha-pass"
        [beta]
        account = "beta-acct"
        user = "beta-user"
        password = "beta-pass"
      `,
      [path.join(HOME, ".snowflake", "config.toml")]: `
        default_connection_name = "beta"
      `,
    })

    const conn = readTomlDefaultConnection()
    expect(conn).not.toBeNull()
    expect(conn!.account).toBe("beta-acct")
  })

  it("SNOWFLAKE_CONNECTION_NAME env var overrides file defaults", () => {
    process.env.SNOWFLAKE_CONNECTION_NAME = "second"

    setupFiles({
      [path.join(HOME, ".snowflake", "connections.toml")]: `
        default_connection_name = "first"
        [first]
        account = "first-acct"
        user = "first-user"
        password = "first-pass"
        [second]
        account = "second-acct"
        user = "second-user"
        password = "second-pass"
      `,
    })

    const conn = readTomlDefaultConnection()
    expect(conn).not.toBeNull()
    expect(conn!.account).toBe("second-acct")
  })

  it("falls back to first connection when default name does not match any", () => {
    setupFiles({
      [path.join(HOME, ".snowflake", "connections.toml")]: `
        default_connection_name = "nonexistent"
        [myconn]
        account = "fallback-acct"
        user = "fallback-user"
        password = "fallback-pass"
      `,
    })

    const conn = readTomlDefaultConnection()
    expect(conn).not.toBeNull()
    expect(conn!.account).toBe("fallback-acct")
  })

  it("returns null when connections.toml exists but has no connection sections", () => {
    setupFiles({
      [path.join(HOME, ".snowflake", "connections.toml")]: `
        default_connection_name = "foo"
      `,
    })

    expect(readTomlDefaultConnection()).toBeNull()
  })

  it("reads connections from connections.toml nested format [connections.name]", () => {
    setupFiles({
      [path.join(HOME, ".snowflake", "connections.toml")]: `
        [connections.prod]
        account = "nested-acct"
        user = "nested-user"
        password = "nested-pass"
      `,
    })

    process.env.SNOWFLAKE_CONNECTION_NAME = "prod"
    const conn = readTomlDefaultConnection()
    expect(conn).not.toBeNull()
    expect(conn!.account).toBe("nested-acct")
  })

  it("nested [connections.*] format wins over legacy top-level on same name", () => {
    setupFiles({
      [path.join(HOME, ".snowflake", "connections.toml")]: `
        [test]
        account = "legacy-acct"
        user = "legacy-user"
        password = "legacy-pass"
        [connections.test]
        account = "nested-acct"
        user = "nested-user"
        password = "nested-pass"
      `,
    })

    process.env.SNOWFLAKE_CONNECTION_NAME = "test"
    const conn = readTomlDefaultConnection()
    expect(conn).not.toBeNull()
    expect(conn!.account).toBe("nested-acct")
  })

  it("resolves legacy and nested connections coexisting in connections.toml", () => {
    setupFiles({
      [path.join(HOME, ".snowflake", "connections.toml")]: `
        [legacy_conn]
        account = "legacy-acct"
        user = "legacy-user"
        password = "legacy-pass"
        [connections.new_conn]
        account = "new-acct"
        user = "new-user"
        password = "new-pass"
      `,
    })

    process.env.SNOWFLAKE_CONNECTION_NAME = "new_conn"
    const conn = readTomlDefaultConnection()
    expect(conn).not.toBeNull()
    expect(conn!.account).toBe("new-acct")
  })

  it("SNOWFLAKE_HOME changes the config file lookup directory", () => {
    process.env.SNOWFLAKE_HOME = "/custom-snowflake-dir"
    setupFiles({
      ["/custom-snowflake-dir/connections.toml"]: `
        [myconn]
        account = "custom-home-acct"
        user = "custom-home-user"
        password = "custom-home-pass"
      `,
    })

    const conn = readTomlDefaultConnection()
    expect(conn).not.toBeNull()
    expect(conn!.account).toBe("custom-home-acct")
  })

  it("caches the result across calls", () => {
    setupFiles({
      [path.join(HOME, ".snowflake", "connections.toml")]: `
        [cached]
        account = "cached-acct"
        user = "cached-user"
        password = "cached-pass"
      `,
    })

    const first = readTomlDefaultConnection()
    // Change the mock -- should NOT affect result due to caching
    vi.mocked(fs.existsSync).mockReturnValue(false)
    const second = readTomlDefaultConnection()

    expect(first).toEqual(second)
    expect(second!.account).toBe("cached-acct")
  })

  it("cache is cleared by resetTomlConfigCache", () => {
    setupFiles({
      [path.join(HOME, ".snowflake", "connections.toml")]: `
        [v1]
        account = "v1-acct"
        user = "v1-user"
        password = "v1-pass"
      `,
    })

    const first = readTomlDefaultConnection()
    expect(first!.account).toBe("v1-acct")

    resetTomlConfigCache()

    setupFiles({
      [path.join(HOME, ".snowflake", "connections.toml")]: `
        [v2]
        account = "v2-acct"
        user = "v2-user"
        password = "v2-pass"
      `,
    })

    const second = readTomlDefaultConnection()
    expect(second!.account).toBe("v2-acct")
  })

  // --- Env var overlay tests (spec §3.B) ---

  it("SNOWFLAKE_ACCOUNT env var overlays on toml connection", () => {
    process.env.SNOWFLAKE_ACCOUNT = "env-override-acct"

    setupFiles({
      [path.join(HOME, ".snowflake", "connections.toml")]: `
        [myconn]
        account = "file-acct"
        user = "file-user"
        password = "file-pass"
      `,
    })

    const conn = readTomlDefaultConnection()
    expect(conn).not.toBeNull()
    expect(conn!.account).toBe("env-override-acct")
    expect(conn!.user).toBe("file-user") // non-overridden field preserved
  })

  it("multiple SNOWFLAKE_<KEY> env vars overlay on toml connection", () => {
    process.env.SNOWFLAKE_DATABASE = "env-db"
    process.env.SNOWFLAKE_SCHEMA = "env-schema"
    process.env.SNOWFLAKE_ROLE = "env-role"

    setupFiles({
      [path.join(HOME, ".snowflake", "connections.toml")]: `
        [myconn]
        account = "file-acct"
        user = "file-user"
        database = "file-db"
      `,
    })

    const conn = readTomlDefaultConnection()
    expect(conn).not.toBeNull()
    expect(conn!.account).toBe("file-acct") // not overridden
    expect(conn!.database).toBe("env-db")
    expect(conn!.schema).toBe("env-schema")
    expect(conn!.role).toBe("env-role")
  })

  it("env var overlay applies to all connections in cache", () => {
    process.env.SNOWFLAKE_WAREHOUSE = "env-warehouse"

    setupFiles({
      [path.join(HOME, ".snowflake", "connections.toml")]: `
        [conn1]
        account = "acct1"
        user = "user1"
        [conn2]
        account = "acct2"
        user = "user2"
      `,
    })

    // First call caches both connections with overlay applied
    process.env.SNOWFLAKE_CONNECTION_NAME = "conn1"
    const conn1 = readTomlDefaultConnection()
    expect(conn1!.warehouse).toBe("env-warehouse")

    // Switch to conn2 (still uses cache)
    process.env.SNOWFLAKE_CONNECTION_NAME = "conn2"
    resetTomlConfigCache()

    setupFiles({
      [path.join(HOME, ".snowflake", "connections.toml")]: `
        [conn1]
        account = "acct1"
        user = "user1"
        [conn2]
        account = "acct2"
        user = "user2"
      `,
    })

    const conn2 = readTomlDefaultConnection()
    expect(conn2!.warehouse).toBe("env-warehouse")
    expect(conn2!.account).toBe("acct2")
  })

  it("ignores non-connection SNOWFLAKE_* env vars", () => {
    process.env.SNOWFLAKE_CONNECTION_NAME = "myconn"
    process.env.SNOWFLAKE_RANDOM_KEY = "ignored"

    setupFiles({
      [path.join(HOME, ".snowflake", "connections.toml")]: `
        [myconn]
        account = "file-acct"
        user = "file-user"
      `,
    })

    const conn = readTomlDefaultConnection()
    expect(conn).not.toBeNull()
    expect(conn!.account).toBe("file-acct")
    expect((conn as Record<string, unknown>).random_key).toBeUndefined()
  })
})

// ---------------------------------------------------------------------------
// buildCallerRightsToken: runtime-specific token format
// ---------------------------------------------------------------------------

describe("buildCallerRightsToken", () => {
  it("returns the bare caller token for an opaque CNG service token", () => {
    setupFiles({ "/snowflake/session/token": "opaque-cng-service-token\n" })

    expect(buildCallerRightsToken("caller.jwt.token")).toBe("caller.jwt.token")
  })

  it("returns the compound token for a JWT-shaped SPCS service token", () => {
    setupFiles({ "/snowflake/session/token": "service.jwt.token\n" })

    expect(buildCallerRightsToken("caller-token")).toBe(
      "service.jwt.token.caller-token",
    )
  })

  it("throws when no deployed service token is available", () => {
    setupFiles({})

    expect(() => buildCallerRightsToken("caller-token")).toThrow(
      /No Snowflake App service token/,
    )
  })
})

describe("querySnowflake caller token format", () => {
  const callerToken = "caller.jwt.token"

  async function loadQuerySnowflake(serviceToken: string) {
    vi.resetModules()
    vi.stubEnv("NODE_ENV", "production")
    delete process.env.SNOWFLAKE_USER
    delete process.env.SNOWFLAKE_PASSWORD

    vi.doMock("fs", () => {
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      const actual = require("fs") as typeof import("fs")
      const read = (p: string, ...args: unknown[]) =>
        p === "/snowflake/session/token"
          ? serviceToken
          : (actual.readFileSync as (...a: unknown[]) => unknown)(p, ...args)
      const exists = (p: string) =>
        p === "/snowflake/session/token" || actual.existsSync(p)
      return {
        default: { ...actual, existsSync: exists, readFileSync: read },
        existsSync: exists,
        readFileSync: read,
      }
    })

    vi.doMock("next/headers", () => ({
      headers: vi.fn(() => new Headers({ "sf-context-current-user-token": callerToken })),
    }))

    mockExecuteImpl = ({ complete }) => {
      complete(null, { getQueryId: () => "qid-rcr" }, [{ ok: true }])
    }

    const { default: snowflakeSdk } = await import("snowflake-sdk")
    const createPool = vi.mocked(snowflakeSdk.createPool)
    createPool.mockClear()

    const mod = await import("@/lib/snowflake")
    return { querySnowflake: mod.querySnowflake, createPool }
  }

  afterEach(() => {
    vi.doUnmock("fs")
    vi.doUnmock("next/headers")
    vi.clearAllMocks()
    vi.unstubAllEnvs()
  })

  it("passes the bare caller token to the SDK on CNG", async () => {
    const { querySnowflake, createPool } =
      await loadQuerySnowflake("opaque-cng-service-token")

    await expect(
      querySnowflake("SELECT CURRENT_USER()", { callersRights: true }),
    ).resolves.toEqual([{ ok: true }])

    expect(createPool).toHaveBeenCalledWith(
      expect.objectContaining({ authenticator: "OAUTH", token: callerToken }),
      expect.any(Object),
    )
  })

  it("passes the compound service-plus-caller token to the SDK on SPCS", async () => {
    const { querySnowflake, createPool } =
      await loadQuerySnowflake("service.jwt.token")

    await querySnowflake("SELECT CURRENT_USER()", { callersRights: true })

    expect(createPool).toHaveBeenCalledWith(
      expect.objectContaining({
        authenticator: "OAUTH",
        token: `service.jwt.token.${callerToken}`,
      }),
      expect.any(Object),
    )
  })
})

// ---------------------------------------------------------------------------
// querySnowflake: retry on terminated connection
// ---------------------------------------------------------------------------

describe("querySnowflake: retry on terminated connection", () => {
  let querySnowflake: typeof import("../../lib/snowflake").querySnowflake

  beforeEach(async () => {
    vi.resetModules()
    delete process.env.SNOWFLAKE_USER
    delete process.env.SNOWFLAKE_PASSWORD

    vi.doMock("fs", () => {
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      const actual = require("fs") as typeof import("fs")
      return {
        default: {
          ...actual,
          existsSync: (p: string) => p === "/snowflake/session/token" || actual.existsSync(p),
          readFileSync: (p: string, ...args: unknown[]) => {
            if (p === "/snowflake/session/token") return "fake.spcs.token"
            return (actual.readFileSync as (...a: unknown[]) => unknown)(p, ...args)
          },
        },
        existsSync: (p: string) => p === "/snowflake/session/token" || actual.existsSync(p),
        readFileSync: (p: string, ...args: unknown[]) => {
          if (p === "/snowflake/session/token") return "fake.spcs.token"
          return (actual.readFileSync as (...a: unknown[]) => unknown)(p, ...args)
        },
      }
    })

    const mod = await import("@/lib/snowflake")
    querySnowflake = mod.querySnowflake
  })

  afterEach(() => {
    vi.doUnmock("fs")
  })

  it("retries once when execute fails with 'terminated connection' and succeeds on retry", async () => {
    let callCount = 0
    mockExecuteImpl = ({ complete }) => {
      callCount++
      if (callCount === 1) {
        complete(new Error("terminated connection"), null, null)
      } else {
        complete(null, { getQueryId: () => "qid-retry-ok" }, [{ result: 1 }])
      }
    }

    const rows = await querySnowflake("SELECT 1")
    expect(rows).toEqual([{ result: 1 }])
    expect(callCount).toBe(2)
  })

  it("does NOT retry on errors that do not include 'terminated connection'", async () => {
    let callCount = 0
    mockExecuteImpl = ({ complete }) => {
      callCount++
      complete(new Error("SQL compilation error: unexpected 'SELECX'"), null, null)
    }

    await expect(querySnowflake("SELECX 1")).rejects.toThrow()
    expect(callCount).toBe(1)
  })

  it("does NOT retry more than once — re-throws after a single retry also fails", async () => {
    let callCount = 0
    mockExecuteImpl = ({ complete }) => {
      callCount++
      complete(new Error("terminated connection"), null, null)
    }

    await expect(querySnowflake("SELECT 1")).rejects.toThrow()
    expect(callCount).toBe(2)
  })

  it("ownersPool is cleared before the retry — createPool is called twice", async () => {
    const { default: snowflakeSdk } = await import("snowflake-sdk")
    const createPool = vi.mocked(snowflakeSdk.createPool)
    createPool.mockClear()

    let callCount = 0
    mockExecuteImpl = ({ complete }) => {
      callCount++
      if (callCount === 1) {
        complete(new Error("terminated connection"), null, null)
      } else {
        complete(null, { getQueryId: () => "qid-fresh" }, [{ ok: true }])
      }
    }

    await querySnowflake("SELECT 1")
    expect(createPool.mock.calls.length).toBeGreaterThanOrEqual(2)
  })

  it("passes warehouse as the QUERY_WAREHOUSE_NAME statement parameter", async () => {
    let capturedParams: Record<string, unknown> | undefined
    mockExecuteImpl = ({ parameters, complete }) => {
      capturedParams = parameters
      complete(null, { getQueryId: () => "qid-wh" }, [{ ok: true }])
    }

    await querySnowflake("SELECT 1", { warehouse: "MY_WH" })
    expect(capturedParams).toEqual({ QUERY_WAREHOUSE_NAME: "MY_WH" })
  })

  it("omits statement parameters when no warehouse is given", async () => {
    let capturedParams: Record<string, unknown> | undefined = { sentinel: true }
    mockExecuteImpl = ({ parameters, complete }) => {
      capturedParams = parameters
      complete(null, { getQueryId: () => "qid-no-wh" }, [{ ok: true }])
    }

    await querySnowflake("SELECT 1")
    expect(capturedParams).toBeUndefined()
  })

  it("passes binds through to conn.execute", async () => {
    let capturedBinds: unknown[] | undefined
    mockExecuteImpl = ({ binds, complete }) => {
      capturedBinds = binds
      complete(null, { getQueryId: () => "qid-binds" }, [{ ok: true }])
    }

    await querySnowflake("SELECT * FROM t WHERE id = ? AND status = ?", { binds: [42, "active"] })
    expect(capturedBinds).toEqual([42, "active"])
  })

  it("omits binds when none are given", async () => {
    let capturedBinds: unknown[] | undefined = ["sentinel"]
    mockExecuteImpl = ({ binds, complete }) => {
      capturedBinds = binds
      complete(null, { getQueryId: () => "qid-no-binds" }, [{ ok: true }])
    }

    await querySnowflake("SELECT 1")
    expect(capturedBinds).toBeUndefined()
  })

  it("omits binds when an empty array is given", async () => {
    let capturedBinds: unknown[] | undefined = ["sentinel"]
    mockExecuteImpl = ({ binds, complete }) => {
      capturedBinds = binds
      complete(null, { getQueryId: () => "qid-empty-binds" }, [{ ok: true }])
    }

    await querySnowflake("SELECT 1", { binds: [] })
    expect(capturedBinds).toBeUndefined()
  })
})

// ---------------------------------------------------------------------------
// querySnowflake: caller's rights is ignored in preview
// ---------------------------------------------------------------------------

describe("querySnowflake: dev/preview ignores callersRights", () => {
  let querySnowflake: (
    query: string,
    options?: { callersRights?: boolean },
  ) => Promise<Record<string, unknown>[]>
  let headersMock: ReturnType<typeof vi.fn>

  beforeEach(async () => {
    vi.resetModules()
    delete process.env.SNOWFLAKE_USER
    delete process.env.SNOWFLAKE_PASSWORD

    // SPCS service token present — exercises the owner's-rights vs caller's-rights branch.
    vi.doMock("fs", () => {
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      const actual = require("fs") as typeof import("fs")
      const read = (p: string, ...args: unknown[]) =>
        p === "/snowflake/session/token"
          ? "fake.spcs.token"
          : (actual.readFileSync as (...a: unknown[]) => unknown)(p, ...args)
      const exists = (p: string) => p === "/snowflake/session/token" || actual.existsSync(p)
      return {
        default: { ...actual, existsSync: exists, readFileSync: read },
        existsSync: exists,
        readFileSync: read,
      }
    })

    headersMock = vi.fn(() => new Headers())
    vi.doMock("next/headers", () => ({ headers: headersMock }))

    mockExecuteImpl = ({ complete }) => {
      complete(null, { getQueryId: () => "qid-preview" }, [{ ok: true }])
    }

    const mod = await import("@/lib/snowflake")
    querySnowflake = mod.querySnowflake
  })

  afterEach(() => {
    vi.doUnmock("fs")
    vi.doUnmock("next/headers")
    vi.unstubAllEnvs()
  })

  it("uses the owner's-rights token for a callersRights query in preview and logs it", async () => {
    // Preview runs the app with NODE_ENV=development (forced by the preview host's dev
    // server) and an SPCS service token present, so callersRights is downgraded to owner's
    // rights and a note is logged to the app output (visible in the preview log pane).
    vi.stubEnv("NODE_ENV", "development")
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {})

    const rows = await querySnowflake("SELECT 1", { callersRights: true })

    expect(rows).toEqual([{ ok: true }])
    // The caller token header must never be consulted in dev/preview.
    expect(headersMock).not.toHaveBeenCalled()
    expect(logSpy).toHaveBeenCalledWith(expect.stringContaining("ignored in preview"))
    logSpy.mockRestore()
  })

  it("in a real deployment (production), a callersRights query still requires the caller token header", async () => {
    vi.stubEnv("NODE_ENV", "production")

    // No caller token header, so caller's rights is attempted and throws as before.
    await expect(querySnowflake("SELECT 1", { callersRights: true })).rejects.toThrow(
      /sf-context-current-user-token/,
    )
    expect(headersMock).toHaveBeenCalled()
  })
})

// ---------------------------------------------------------------------------
// querySnowflake: local-dev warning when callersRights is requested
// ---------------------------------------------------------------------------

describe("querySnowflake: warns in local dev when callersRights requested", () => {
  let querySnowflake: (
    query: string,
    options?: { callersRights?: boolean },
  ) => Promise<Record<string, unknown>[]>
  let warnSpy: ReturnType<typeof vi.spyOn>

  beforeEach(async () => {
    vi.resetModules()
    // Local dev: dev mode, password creds, and no Snowflake App service token.
    vi.stubEnv("NODE_ENV", "development")
    process.env.SNOWFLAKE_USER = "dev-user"
    process.env.SNOWFLAKE_PASSWORD = "dev-pass"

    vi.doMock("fs", () => {
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      const actual = require("fs") as typeof import("fs")
      const read = (p: string, ...args: unknown[]) => {
        if (p === "/snowflake/session/token") throw new Error("ENOENT")
        return (actual.readFileSync as (...a: unknown[]) => unknown)(p, ...args)
      }
      const exists = (p: string) => (p === "/snowflake/session/token" ? false : actual.existsSync(p))
      return {
        default: { ...actual, existsSync: exists, readFileSync: read },
        existsSync: exists,
        readFileSync: read,
      }
    })

    warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {})
    mockExecuteImpl = ({ complete }) => {
      complete(null, { getQueryId: () => "qid-localdev" }, [{ ok: true }])
    }

    const mod = await import("@/lib/snowflake")
    querySnowflake = mod.querySnowflake
  })

  afterEach(() => {
    vi.doUnmock("fs")
    warnSpy.mockRestore()
    vi.unstubAllEnvs()
    delete process.env.SNOWFLAKE_USER
    delete process.env.SNOWFLAKE_PASSWORD
  })

  it("warns that callersRights has no effect (still using local dev credentials)", async () => {
    const rows = await querySnowflake("SELECT 1", { callersRights: true })

    expect(rows).toEqual([{ ok: true }])
    expect(warnSpy).toHaveBeenCalledWith(
      expect.stringContaining("has no effect outside a deployed Snowflake App"),
    )
  })

  it("does not warn when callersRights is not requested", async () => {
    await querySnowflake("SELECT 1")
    expect(warnSpy).not.toHaveBeenCalled()
  })
})

// ---------------------------------------------------------------------------
// getSnowflakeBaseUrl
// ---------------------------------------------------------------------------

describe("getSnowflakeBaseUrl", () => {
  afterEach(() => {
    vi.doUnmock("fs")
    delete process.env.SNOWFLAKE_ACCOUNT_URL
    delete process.env.SNOWFLAKE_HOST
  })

  it("returns SNOWFLAKE_ACCOUNT_URL env var when set", async () => {
    process.env.SNOWFLAKE_ACCOUNT_URL = "https://myaccount.snowflakecomputing.com"
    vi.resetModules()
    vi.doMock("fs", makeNoTomlFsMock)
    const mod = await import("@/lib/snowflake")
    expect(mod.getSnowflakeBaseUrl()).toBe("https://myaccount.snowflakecomputing.com")
  })

  it("returns https://HOST when SNOWFLAKE_HOST is set", async () => {
    process.env.SNOWFLAKE_HOST = "myaccount.snowflakecomputing.com"
    vi.resetModules()
    vi.doMock("fs", makeNoTomlFsMock)
    const mod = await import("@/lib/snowflake")
    expect(mod.getSnowflakeBaseUrl()).toBe("https://myaccount.snowflakecomputing.com")
  })

  it("returns null when no env vars or toml config are available", async () => {
    vi.resetModules()
    vi.doMock("fs", makeNoTomlFsMock)
    const mod = await import("@/lib/snowflake")
    expect(mod.getSnowflakeBaseUrl()).toBeNull()
  })

  it("normalises account identifier to lowercase hyphenated URL", async () => {
    vi.resetModules()
    vi.doMock("fs", () => {
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      const actual = require("fs") as typeof import("fs")
      const tomlContent = "[myconn]\naccount = \"MYORG-MY_ACCOUNT\""
      return {
        default: {
          ...actual,
          existsSync: (p: string) => p.endsWith("connections.toml"),
          readFileSync: (p: string, ...args: unknown[]) =>
            p.endsWith("connections.toml") ? tomlContent : (actual.readFileSync as (...a: unknown[]) => unknown)(p, ...args),
        },
        existsSync: (p: string) => p.endsWith("connections.toml"),
        readFileSync: (p: string, ...args: unknown[]) =>
          p.endsWith("connections.toml") ? tomlContent : (actual.readFileSync as (...a: unknown[]) => unknown)(p, ...args),
      }
    })
    const mod = await import("@/lib/snowflake")
    expect(mod.getSnowflakeBaseUrl()).toBe("https://myorg-my-account.snowflakecomputing.com")
  })
})

// ---------------------------------------------------------------------------
// getRestApiAuthHeader
// ---------------------------------------------------------------------------

describe("getRestApiAuthHeader", () => {
  beforeEach(() => {
    vi.resetModules()
    delete process.env.SNOWFLAKE_USER
    delete process.env.SNOWFLAKE_PASSWORD
  })

  afterEach(() => {
    vi.doUnmock("fs")
  })

  it("returns Bearer <token> when a Snowflake App service token is present", async () => {
    vi.doMock("fs", () => {
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      const actual = require("fs") as typeof import("fs")
      return {
        default: {
          ...actual,
          existsSync: (p: string) => p === "/snowflake/session/token",
          readFileSync: (p: string, ...args: unknown[]) =>
            p === "/snowflake/session/token" ? "spcs-token-abc" : (actual.readFileSync as (...a: unknown[]) => unknown)(p, ...args),
        },
        existsSync: (p: string) => p === "/snowflake/session/token",
        readFileSync: (p: string, ...args: unknown[]) =>
          p === "/snowflake/session/token" ? "spcs-token-abc" : (actual.readFileSync as (...a: unknown[]) => unknown)(p, ...args),
      }
    })
    const mod = await import("@/lib/snowflake")
    expect(mod.getRestApiAuthHeader()).toBe("Bearer spcs-token-abc")
  })

  it("returns Bearer <empty> (will 401) when no token or OAuth config is available", async () => {
    vi.doMock("fs", () => {
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      const actual = require("fs") as typeof import("fs")
      return {
        default: { ...actual, existsSync: () => false },
        existsSync: () => false,
        readFileSync: actual.readFileSync,
      }
    })
    const mod = await import("@/lib/snowflake")
    expect(mod.getRestApiAuthHeader()).toBe("Bearer ")
  })
})

describe("getSecret", () => {
  const SECRET_ENV_KEYS = [
    "SNOWFLAKE_SECRET_API_KEY_SECRET_STRING",
    "SNOWFLAKE_SECRET_DB_CREDS_USERNAME",
    "SNOWFLAKE_SECRET_DB_CREDS_PASSWORD",
  ]

  beforeEach(() => {
    for (const key of SECRET_ENV_KEYS) delete process.env[key]
  })

  afterEach(() => {
    for (const key of SECRET_ENV_KEYS) delete process.env[key]
  })

  describe("GENERIC_STRING", () => {
    it("reads the mounted file /secrets/<name>/secret_string (SPCS)", () => {
      setupFiles({ "/secrets/API_KEY/secret_string": "sk-from-file\n" })
      expect(getSecret("API_KEY", SecretType.GENERIC_STRING)).toBe("sk-from-file")
    })

    it("falls back to SNOWFLAKE_SECRET_<NAME>_SECRET_STRING when no file is mounted", () => {
      setupFiles({})
      process.env.SNOWFLAKE_SECRET_API_KEY_SECRET_STRING = "sk-from-env"
      expect(getSecret("API_KEY", SecretType.GENERIC_STRING)).toBe("sk-from-env")
    })

    it("prefers the mounted file over the env var", () => {
      setupFiles({ "/secrets/API_KEY/secret_string": "sk-from-file" })
      process.env.SNOWFLAKE_SECRET_API_KEY_SECRET_STRING = "sk-from-env"
      expect(getSecret("API_KEY", SecretType.GENERIC_STRING)).toBe("sk-from-file")
    })

    it("lowercase names still map to the upper-case env var fallback", () => {
      setupFiles({})
      process.env.SNOWFLAKE_SECRET_API_KEY_SECRET_STRING = "sk-from-env"
      expect(getSecret("api_key", SecretType.GENERIC_STRING)).toBe("sk-from-env")
    })

    it("throws a helpful error when neither file nor env var is present", () => {
      setupFiles({})
      expect(() => getSecret("API_KEY", SecretType.GENERIC_STRING)).toThrow(
        /SNOWFLAKE_SECRET_API_KEY_SECRET_STRING/,
      )
    })
  })

  describe("PASSWORD", () => {
    it("reads mounted /secrets/<name>/username and /password (SPCS)", () => {
      setupFiles({
        "/secrets/DB_CREDS/username": "svc_user\n",
        "/secrets/DB_CREDS/password": "hunter2\n",
      })
      expect(getSecret("DB_CREDS", SecretType.PASSWORD)).toEqual({
        username: "svc_user",
        password: "hunter2",
      })
    })

    it("falls back to USERNAME and PASSWORD env vars when no files are mounted", () => {
      setupFiles({})
      process.env.SNOWFLAKE_SECRET_DB_CREDS_USERNAME = "svc_user"
      process.env.SNOWFLAKE_SECRET_DB_CREDS_PASSWORD = "hunter2"
      expect(getSecret("DB_CREDS", SecretType.PASSWORD)).toEqual({
        username: "svc_user",
        password: "hunter2",
      })
    })

    it("mixes a mounted username with an env-var password fallback", () => {
      setupFiles({ "/secrets/DB_CREDS/username": "file_user" })
      process.env.SNOWFLAKE_SECRET_DB_CREDS_PASSWORD = "env_pass"
      expect(getSecret("DB_CREDS", SecretType.PASSWORD)).toEqual({
        username: "file_user",
        password: "env_pass",
      })
    })

    it("throws when the password field is missing entirely", () => {
      setupFiles({})
      process.env.SNOWFLAKE_SECRET_DB_CREDS_USERNAME = "svc_user"
      expect(() => getSecret("DB_CREDS", SecretType.PASSWORD)).toThrow(
        /SNOWFLAKE_SECRET_DB_CREDS_PASSWORD/,
      )
    })
  })

  describe("OAUTH2", () => {
    const OAUTH_ENV_KEYS = [
      "SNOWFLAKE_SECRET_OAUTH_CC_OAUTH_TOKEN_URL",
      "SNOWFLAKE_SECRET_OAUTH_CC_OAUTH_CLIENT_ID",
      "SNOWFLAKE_SECRET_OAUTH_CC_OAUTH_CLIENT_SECRET",
      "SNOWFLAKE_SECRET_OAUTH_CC_OAUTH_SCOPE",
      "SNOWFLAKE_SECRET_OAUTH_CACHE_OAUTH_TOKEN_URL",
      "SNOWFLAKE_SECRET_OAUTH_CACHE_OAUTH_CLIENT_ID",
      "SNOWFLAKE_SECRET_OAUTH_CACHE_OAUTH_CLIENT_SECRET",
      "SNOWFLAKE_SECRET_OAUTH_STATIC_ACCESS_TOKEN",
    ]

    beforeEach(() => {
      for (const key of OAUTH_ENV_KEYS) delete process.env[key]
    })

    afterEach(() => {
      for (const key of OAUTH_ENV_KEYS) delete process.env[key]
      vi.unstubAllGlobals()
    })

    it("reads the mounted /secrets/<name>/access_token in SPCS (no token fetch)", async () => {
      setupFiles({ "/secrets/OAUTH_SPCS/access_token": "spcs-oauth-token\n" })
      const fetchMock = vi.fn()
      vi.stubGlobal("fetch", fetchMock)

      await expect(getSecret("OAUTH_SPCS", SecretType.OAUTH2)).resolves.toBe("spcs-oauth-token")
      expect(fetchMock).not.toHaveBeenCalled()
    })

    it("mints a token via the client-credentials grant in local dev", async () => {
      setupFiles({})
      process.env.SNOWFLAKE_SECRET_OAUTH_CC_OAUTH_TOKEN_URL = "https://provider.example.com/token"
      process.env.SNOWFLAKE_SECRET_OAUTH_CC_OAUTH_CLIENT_ID = "cid"
      process.env.SNOWFLAKE_SECRET_OAUTH_CC_OAUTH_CLIENT_SECRET = "csecret"
      process.env.SNOWFLAKE_SECRET_OAUTH_CC_OAUTH_SCOPE = "api.read"

      const fetchMock = vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        statusText: "OK",
        json: async () => ({ access_token: "cc-token", expires_in: 3600 }),
        text: async () => "",
      })
      vi.stubGlobal("fetch", fetchMock)

      await expect(getSecret("OAUTH_CC", SecretType.OAUTH2)).resolves.toBe("cc-token")
      expect(fetchMock).toHaveBeenCalledTimes(1)

      // The POST body carries the client-credentials grant + configured client/scope.
      const [url, init] = fetchMock.mock.calls[0]
      expect(url).toBe("https://provider.example.com/token")
      expect(init.method).toBe("POST")
      const body = init.body as URLSearchParams
      expect(body.get("grant_type")).toBe("client_credentials")
      expect(body.get("client_id")).toBe("cid")
      expect(body.get("scope")).toBe("api.read")
    })

    it("caches the token and reuses it before expiry (no second fetch)", async () => {
      setupFiles({})
      process.env.SNOWFLAKE_SECRET_OAUTH_CACHE_OAUTH_TOKEN_URL = "https://provider.example.com/token"
      process.env.SNOWFLAKE_SECRET_OAUTH_CACHE_OAUTH_CLIENT_ID = "cid"
      process.env.SNOWFLAKE_SECRET_OAUTH_CACHE_OAUTH_CLIENT_SECRET = "csecret"

      const fetchMock = vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        statusText: "OK",
        json: async () => ({ access_token: "cached-token", expires_in: 3600 }),
        text: async () => "",
      })
      vi.stubGlobal("fetch", fetchMock)

      const first = await getSecret("OAUTH_CACHE", SecretType.OAUTH2)
      const second = await getSecret("OAUTH_CACHE", SecretType.OAUTH2)
      expect(first).toBe("cached-token")
      expect(second).toBe("cached-token")
      expect(fetchMock).toHaveBeenCalledTimes(1)
    })

    it("falls back to a static SNOWFLAKE_SECRET_<NAME>_ACCESS_TOKEN when no token URL is set", async () => {
      setupFiles({})
      process.env.SNOWFLAKE_SECRET_OAUTH_STATIC_ACCESS_TOKEN = "static-token"
      const fetchMock = vi.fn()
      vi.stubGlobal("fetch", fetchMock)

      await expect(getSecret("OAUTH_STATIC", SecretType.OAUTH2)).resolves.toBe("static-token")
      expect(fetchMock).not.toHaveBeenCalled()
    })

    it("throws a helpful error when neither a mount, token URL, nor static token exists", async () => {
      setupFiles({})
      await expect(getSecret("OAUTH_MISSING", SecretType.OAUTH2)).rejects.toThrow(
        /is not available locally/,
      )
    })
  })
})
