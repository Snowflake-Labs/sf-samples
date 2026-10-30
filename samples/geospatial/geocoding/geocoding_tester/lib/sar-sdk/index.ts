/**
 * Platform-agnostic SDK for Snowflake App Runtime apps (server-side Node.js): queries, auth,
 * connection pooling, secrets, and REST helpers. It must not import a web framework. A
 * framework wrapper (lib/snowflake.ts for Next.js) supplies the request headers that
 * caller's-rights queries read, and app code imports from that wrapper.
 *
 * Logging: by default prints concise lines (auth path, timing, truncated SQL, queryId on
 * success). Set SNOWFLAKE_SDK_QUIET=1 to disable. SDK internal logs stay at ERROR unless
 * you change snowflake.configure below.
 *
 * Auth is auto-detected (in priority order):
 *   1. Snowflake App service token file (/snowflake/session/token) — read fresh on every call
 *   2. SNOWFLAKE_USER + SNOWFLAKE_PASSWORD env vars — password auth (local dev)
 *   3. ~/.snowflake/config.toml default connection — zero-config local dev
 *
 * Caller's rights applies only to a real (production) deployment. In local dev and in
 * a Workspaces preview the app runs in dev mode (NODE_ENV=development), where there is
 * no caller identity — so callersRights is ignored and queries fall back to the
 * owner's-rights service token.
 *
 * Connection pooling:
 *   Owner's rights: single pool per process, recreated when the service token rotates.
 *   Caller's rights: one pool per user+role, keyed by the runtime-specific OAuth token
 *     (SPCS compound token or CNG bare caller token), all drained when the service
 *     token rotates.
 *   Local dev pools (password / toml) are shared and never rotated.
 *
 * Caller token handling differs by runtime:
 *   SPCS: the service token is a JWT (contains dots), so caller's rights uses
 *     serviceToken + "." + callerToken.
 *   CNG/serverless: the service token is opaque (no dots), so caller's rights uses the
 *     bare sf-context-current-user-token. Combining it with the owner token is invalid.
 *
 * Snowflake App caller's-rights helpers:
 *   queryWithToken(query, token) — runs a query with an explicit OAuth token.
 *   getServiceToken() — reads the service token from /snowflake/session/token.
 *   buildCallerRightsToken(callerUserToken) — returns the runtime-correct OAuth token.
 *
 * Secrets:
 *   getSecret(name, type) — reads a Snowflake SECRET value at runtime. Use this instead of
 *     reading SNOWFLAKE_SECRET_* env vars or querying the SECRET object directly (see the
 *     getSecret docstring below). The secret must be declared under `secrets:` in app.yml to
 *     be available in SPCS.
 *
 * Cleanup:
 *   closePool() — drains and destroys all active pools on shutdown.
 */

import fs from "fs"
import path from "path"
import os from "os"
import snowflake from "snowflake-sdk"

snowflake.configure({
  logLevel: "ERROR",
  ...(process.env.SNOWFLAKE_SDK_DISABLE_OCSP === "true" && { disableOCSPChecks: true, ocspFailOpen: true }),
})

const SERVICE_TOKEN_PATH = "/snowflake/session/token"

const LOG_PREFIX = "[snowflake]"

/**
 * True when the app is NOT running as a real (production) deployment — i.e. local dev
 * (`next dev`) or a Workspaces preview, both of which run in dev mode with
 * NODE_ENV=development. Caller's rights only applies to a real deployment, so in these
 * modes there is no caller identity and queries fall back to the owner's-rights token.
 */
function isDevOrPreview(): boolean {
  return process.env.NODE_ENV === "development"
}

/**
 * Whether a query should run with caller's rights. Caller's rights applies only to a real
 * deployment; in local dev and in a Workspaces preview the app runs in dev mode
 * (NODE_ENV=development) with no caller identity, so caller's rights is not used.
 */
function shouldUseCallersRights(callersRights: boolean): boolean {
  return callersRights && !isDevOrPreview()
}

/**
 * Note — for the preview log pane — that a requested caller's-rights query ran with the
 * owner's-rights service token instead. Call this only inside the service-token branch after
 * caller's rights was declined: there a declined-but-requested query is by construction a
 * Workspaces preview (service token present + dev mode). Local dev (no service token) never
 * reaches it and keeps its own "no effect outside a deployed Snowflake App" warning.
 */
function noteCallersRightsIgnoredInPreview(callersRights: boolean): void {
  if (callersRights) {
    sfLog("caller's rights ignored in preview — using the owner's-rights service token")
  }
}

/** Single-line SQL preview for logs (keeps log volume small). */
function previewSql(sql: string, maxLen = 200): string {
  const s = sql.replace(/\s+/g, " ").trim()
  if (s.length <= maxLen) return s
  return `${s.slice(0, maxLen)}…`
}

function sfLogQuiet(): boolean {
  const v = process.env.SNOWFLAKE_SDK_QUIET
  return v === "1" || v === "true"
}

function sfLog(message: string): void {
  if (sfLogQuiet()) return
  console.log(`${LOG_PREFIX} ${message}`)
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

/**
 * Build the `parameters` fragment for `conn.execute` when a per-query warehouse
 * override is requested. Sets the QUERY_WAREHOUSE_NAME statement parameter so the
 * query runs on the given warehouse without a separate `USE WAREHOUSE` round-trip.
 */
function warehouseParams(warehouse?: string): { parameters?: Record<string, unknown> } {
  return warehouse ? { parameters: { QUERY_WAREHOUSE_NAME: warehouse } } : {}
}

/** SDK bind values for `?` placeholders. Kept as its own type since @types may lag. */
type Binds = snowflake.Binds

/** Build the `binds` fragment for `conn.execute` when bind values are provided. */
function bindParams(binds?: Binds): { binds?: Binds } {
  return binds && binds.length > 0 ? { binds } : {}
}

/** The SDK supports these; @types/snowflake-sdk may lag — keep cast at boundary. */
type PollingConnection = snowflake.Connection & {
  getQueryStatus: (queryId: string) => Promise<string>
  getResultsFromQueryId: (opts: { queryId: string }) => Promise<{ streamRows: () => NodeJS.ReadableStream }>
  isStillRunning: (status: string) => boolean
  isAnError: (status: string) => boolean
  getQueryStatusThrowIfError: (queryId: string) => Promise<string>
}

type ExecuteOptions = Parameters<snowflake.Connection["execute"]>[0] & { asyncExec?: boolean }

function streamRowsToArray(statement: { streamRows: () => NodeJS.ReadableStream }): Promise<Record<string, any>[]> {
  return new Promise((resolve, reject) => {
    const rows: Record<string, any>[] = []
    statement
      .streamRows()
      .on("error", (err: Error) => reject(err))
      .on("data", (row: Record<string, any>) => rows.push(row))
      .on("end", () => resolve(rows))
  })
}

/**
 * Submit `asyncExec`, poll `getQueryStatus` until the warehouse finishes, then fetch rows.
 * Keeps status logs sparse (default: at most once per minute while running).
 */
async function runLongRunningOnConnection(
  conn: PollingConnection,
  query: string,
  options: {
    pollIntervalMs: number
    statusLogIntervalMs: number
    maxWaitMs?: number
  },
  warehouse?: string,
  binds?: Binds,
): Promise<Record<string, any>[]> {
  const queryId = await new Promise<string>((resolve, reject) => {
    conn.execute({
      sqlText: query,
      asyncExec: true,
      ...warehouseParams(warehouse),
      ...bindParams(binds),
      complete: (err, stmt) => {
        if (err) reject(new Error(`Async submit failed: ${err.message}`))
        else resolve(stmt!.getQueryId())
      },
    } as ExecuteOptions)
  })

  const t0 = Date.now()
  sfLog(`long-running submitted queryId=${queryId} sql=${JSON.stringify(previewSql(query))}`)

  let lastStatusLog = t0
  while (true) {
    if (options.maxWaitMs !== undefined && Date.now() - t0 > options.maxWaitMs) {
      throw new Error(
        `Long-running query timed out after ${options.maxWaitMs}ms (queryId=${queryId})`,
      )
    }

    const status = await conn.getQueryStatus(queryId)
    if (conn.isAnError(status)) {
      try {
        await conn.getQueryStatusThrowIfError(queryId)
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e)
        throw new Error(`Query failed (queryId=${queryId}): ${msg}`)
      }
      throw new Error(`Query failed (queryId=${queryId}): ${status}`)
    }
    if (!conn.isStillRunning(status)) break

    const now = Date.now()
    if (now - lastStatusLog >= options.statusLogIntervalMs) {
      sfLog(
        `long-running poll queryId=${queryId} status=${status} elapsedMs=${now - t0}`,
      )
      lastStatusLog = now
    }
    await sleep(options.pollIntervalMs)
  }

  const statement = await conn.getResultsFromQueryId({ queryId })
  const rows = await streamRowsToArray(statement)
  sfLog(
    `long-running complete queryId=${queryId} rows=${rows.length} totalMs=${Date.now() - t0}`,
  )
  return rows
}

// --- Connection pool configuration ---

const POOL_CONFIG = {
  min: 0, // Start with no connections (lazy init)
  max: 10, // Scale up to 10 concurrent connections
}

// Owner's rights: single pool, recreated when the service token rotates.
let ownersPool: ReturnType<typeof snowflake.createPool> | null = null
let ownersPoolToken = ""

// Caller's rights: one pool per runtime-specific OAuth token (user+role).
// SPCS uses a compound service+caller token; CNG uses the bare caller token.
// All pools are drained when the service token rotates.
const callersPool = new Map<string, ReturnType<typeof snowflake.createPool>>()
let callersServiceToken = ""

// Local dev pools.
let passwordPool: ReturnType<typeof snowflake.createPool> | null = null
let tomlPool: ReturnType<typeof snowflake.createPool> | null = null

function baseConfig(): snowflake.ConnectionOptions {
  const application = "SnowflakeAppRuntime"
  const base: snowflake.ConnectionOptions = { application }
  if (process.env.SNOWFLAKE_ACCOUNT) base.account = process.env.SNOWFLAKE_ACCOUNT
  if (process.env.SNOWFLAKE_WAREHOUSE) base.warehouse = process.env.SNOWFLAKE_WAREHOUSE
  if (process.env.SNOWFLAKE_ACCOUNT_URL) base.accessUrl = process.env.SNOWFLAKE_ACCOUNT_URL
  // SNOWFLAKE_HOST is commonly injected by eval/CI frameworks
  if (!base.accessUrl && process.env.SNOWFLAKE_HOST) {
    base.accessUrl = `https://${process.env.SNOWFLAKE_HOST}`
  }
  if (process.env.SNOWFLAKE_ROLE) base.role = process.env.SNOWFLAKE_ROLE
  if (process.env.SNOWFLAKE_DATABASE) base.database = process.env.SNOWFLAKE_DATABASE
  if (process.env.SNOWFLAKE_SCHEMA) base.schema = process.env.SNOWFLAKE_SCHEMA
  return base
}

// --- ~/.snowflake/connections.toml + config.toml reader ---

interface TomlConnection {
  account?: string
  user?: string
  password?: string
  host?: string
  port?: string | number
  warehouse?: string
  database?: string
  schema?: string
  region?: string
  role?: string
  authenticator?: string
  protocol?: string
  token_file_path?: string
  [key: string]: unknown
}

let _tomlConfigCache: { defaultName: string; connections: Record<string, TomlConnection> } | null | undefined

function normalizeConnectionsToml(doc: Record<string, any>): Record<string, TomlConnection> {
  const result: Record<string, TomlConnection> = {}
  for (const [key, val] of Object.entries(doc)) {
    if (key === "default_connection_name" || key === "connections") continue
    if (typeof val === "object" && val !== null && !Array.isArray(val)) {
      result[key] = val as TomlConnection
    }
  }
  if (typeof doc.connections === "object" && doc.connections !== null && !Array.isArray(doc.connections)) {
    Object.assign(result, doc.connections as Record<string, TomlConnection>)
  }
  return result
}

/**
 * Resolve a Snowflake connection from local TOML config files.
 *
 * Tries two file layouts (in order):
 *   1. ~/.snowflake/connections.toml — supports both nested ([connections.name])
 *      and legacy (top-level [name]) formats; nested wins on conflict.
 *   2. ~/.snowflake/config.toml — connections nested under [connections.*] sections.
 *
 * Both files may contain a `default_connection_name` key. The env var
 * SNOWFLAKE_CONNECTION_NAME takes priority, then SNOWFLAKE_DEFAULT_CONNECTION_NAME
 * (set by Cortex Code for SnowCLI compatibility), then the file-level default.
 *
 * Result is cached for the lifetime of the process.
 */
export function readTomlDefaultConnection(): TomlConnection | null {
  if (_tomlConfigCache === undefined) {
    _tomlConfigCache = null // mark as attempted
    try {
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      const { parse } = require("smol-toml") as { parse: (s: string) => Record<string, any> }
      const snowDir = process.env.SNOWFLAKE_HOME ?? path.join(os.homedir(), ".snowflake")

      let defaultName = ""
      let connections: Record<string, TomlConnection> = {}

      // connections.toml: top-level sections are connections
      const connPath = path.join(snowDir, "connections.toml")
      if (fs.existsSync(connPath)) {
        const doc = parse(fs.readFileSync(connPath, "utf8"))
        if (typeof doc.default_connection_name === "string") defaultName = doc.default_connection_name
        connections = normalizeConnectionsToml(doc)
      }

      // config.toml: connections nested under [connections.*], also has default_connection_name
      const configPath = path.join(snowDir, "config.toml")
      if (fs.existsSync(configPath)) {
        const doc = parse(fs.readFileSync(configPath, "utf8"))
        if (!defaultName && typeof doc.default_connection_name === "string") {
          defaultName = doc.default_connection_name
        }
        // Only use config.toml connections if connections.toml had none
        if (Object.keys(connections).length === 0) {
          connections = (doc.connections ?? {}) as Record<string, TomlConnection>
        }
      }

      // General SNOWFLAKE_<KEY> env vars overlay on every toml connection (spec §3.B)
      const envOverlay: Partial<TomlConnection> = {}
      const CONN_KEYS = new Set(["account","user","password","database","schema","role","warehouse","protocol","host","port","region","authenticator"])
      for (const [k, v] of Object.entries(process.env)) {
        if (!k.startsWith("SNOWFLAKE_") || k === "SNOWFLAKE_CONNECTION_NAME" || k === "SNOWFLAKE_DEFAULT_CONNECTION_NAME" || k === "SNOWFLAKE_HOME") continue
        const field = k.slice("SNOWFLAKE_".length).toLowerCase()
        if (CONN_KEYS.has(field)) (envOverlay as Record<string, string>)[field] = v!
      }
      if (Object.keys(envOverlay).length > 0) {
        for (const name of Object.keys(connections)) {
          connections[name] = { ...connections[name], ...envOverlay }
        }
      }

      // SNOWFLAKE_CONNECTION_NAME overrides file-level default
      if (process.env.SNOWFLAKE_CONNECTION_NAME) {
        defaultName = process.env.SNOWFLAKE_CONNECTION_NAME
      } else if (process.env.SNOWFLAKE_DEFAULT_CONNECTION_NAME) {
        defaultName = process.env.SNOWFLAKE_DEFAULT_CONNECTION_NAME
      }

      _tomlConfigCache = { defaultName, connections }
    } catch {
      // parse error or smol-toml not installed — silently skip
      return null
    }
  }

  if (!_tomlConfigCache) return null

  const { defaultName, connections } = _tomlConfigCache
  const names = Object.keys(connections)
  if (names.length === 0) return null

  const conn = (defaultName && connections[defaultName]) || connections[names[0]]
  return conn ?? null
}

/** Reset the connection config cache. Exported for testing only. */
export function resetTomlConfigCache() {
  _tomlConfigCache = undefined
}

function tomlConnectionConfig(conn: TomlConnection): snowflake.ConnectionOptions {
  // Use SDK's normalizeConnectionOptions for snake_case -> camelCase + key aliases
  // Type assertion needed until @types/snowflake-sdk includes normalizeConnectionOptions
  const normalize = (snowflake as unknown as { normalizeConnectionOptions: (o: Record<string, unknown>) => snowflake.ConnectionOptions }).normalizeConnectionOptions
  const normalized = normalize(conn as Record<string, unknown>)
  // SDK doesn't handle host/port/protocol -> accessUrl, so we do it here
  if (conn.host && !normalized.accessUrl) {
    const protocol = conn.protocol ?? "https"
    const port = conn.port ? `:${conn.port}` : ""
    normalized.accessUrl = `${protocol}://${conn.host}${port}`
  }
  // The SDK only reads token_file_path when it loads the TOML itself
  // (createConnection with no options). Because we build the options object and
  // pass it to createConnection/createPool, the SDK ignores token_file_path and
  // sends an empty OAuth token, which the server rejects with error 390303.
  // So read the token file ourselves for plain "oauth" auth.
  if (
    !normalized.token &&
    typeof normalized.authenticator === "string" &&
    normalized.authenticator.toUpperCase() === "OAUTH" &&
    conn.token_file_path
  ) {
    normalized.token = fs.readFileSync(conn.token_file_path, "utf8").trim()
  }
  return normalized
}

// --- Pool helpers ---

/** SPCS service tokens are JWTs; CNG service tokens are opaque. */
function isSpcsServiceToken(serviceToken: string): boolean {
  return serviceToken.includes(".")
}

/** Build the OAuth token expected by each runtime's caller's-rights login path. */
function callerRightsOAuthToken(serviceToken: string, callerUserToken: string): string {
  return isSpcsServiceToken(serviceToken)
    ? `${serviceToken}.${callerUserToken}`
    : callerUserToken
}

function runtimeName(serviceToken: string): "spcs" | "cng" {
  return isSpcsServiceToken(serviceToken) ? "spcs" : "cng"
}

function getOwnersPool(serviceToken: string): ReturnType<typeof snowflake.createPool> {
  if (ownersPool && ownersPoolToken !== serviceToken) {
    sfLog("pool: draining owner's-rights pool (service token rotated)")
    ownersPool.drain()
    ownersPool = null
  }
  if (!ownersPool) {
    sfLog(`pool: creating owner's-rights OAuth pool (${runtimeName(serviceToken)})`)
    ownersPool = snowflake.createPool(
      { ...baseConfig(), authenticator: "OAUTH", token: serviceToken },
      POOL_CONFIG,
    )
    ownersPoolToken = serviceToken
  }
  return ownersPool
}

function getCallersPool(oauthToken: string, serviceToken: string): ReturnType<typeof snowflake.createPool> {
  // Service token rotated — drain all runtime caller pools.
  if (callersServiceToken !== serviceToken) {
    if (callersPool.size > 0) {
      sfLog(`pool: draining ${callersPool.size} caller's-rights pool(s) (service token rotated)`)
    }
    for (const pool of callersPool.values()) pool.drain()
    callersPool.clear()
    callersServiceToken = serviceToken
  }
  if (!callersPool.has(oauthToken)) {
    sfLog(`pool: creating caller's-rights OAuth pool (${runtimeName(serviceToken)})`)
    callersPool.set(
      oauthToken,
      snowflake.createPool(
        { ...baseConfig(), authenticator: "OAUTH", token: oauthToken },
        POOL_CONFIG,
      ),
    )
  }
  return callersPool.get(oauthToken)!
}

function getPasswordPool(): ReturnType<typeof snowflake.createPool> {
  if (!passwordPool) {
    sfLog("pool: creating password-auth pool (SNOWFLAKE_USER)")
    passwordPool = snowflake.createPool(
      {
        ...baseConfig(),
        username: process.env.SNOWFLAKE_USER,
        password: process.env.SNOWFLAKE_PASSWORD,
      },
      POOL_CONFIG,
    )
  }
  return passwordPool
}

function getTomlPool(conn: TomlConnection): ReturnType<typeof snowflake.createPool> {
  if (!tomlPool) {
    sfLog("pool: creating TOML default-connection pool (~/.snowflake)")
    tomlPool = snowflake.createPool({ ...tomlConnectionConfig(conn), ...baseConfig() }, POOL_CONFIG)
  }
  return tomlPool
}

function queryWithPool(
  pool: ReturnType<typeof snowflake.createPool>,
  query: string,
  authTag: string,
  warehouse?: string,
  binds?: Binds,
): Promise<Record<string, any>[]> {
  return pool.use(async (conn) => {
    const t0 = Date.now()
    sfLog(`query start mode=${authTag} sql=${JSON.stringify(previewSql(query))}`)
    return new Promise<Record<string, any>[]>((res, rej) => {
      conn.execute({
        sqlText: query,
        ...warehouseParams(warehouse),
        ...bindParams(binds),
        complete: (err, stmt, rows) => {
          const ms = Date.now() - t0
          const qid =
            stmt && typeof stmt.getQueryId === "function" ? ` queryId=${stmt.getQueryId()}` : ""
          if (err) {
            sfLog(`query error mode=${authTag} afterMs=${ms}${qid}: ${err.message}`)
            rej(new Error(`Query failed: ${err.message}`))
          } else {
            const out = (rows ?? []) as Record<string, any>[]
            sfLog(`query ok mode=${authTag} rows=${out.length} afterMs=${ms}${qid}`)
            res(out)
          }
        },
      })
    })
  })
}

function queryWithPoolLongRunning(
  pool: ReturnType<typeof snowflake.createPool>,
  query: string,
  authTag: string,
  longOpts: { pollIntervalMs: number; statusLogIntervalMs: number; maxWaitMs?: number },
  warehouse?: string,
  binds?: Binds,
): Promise<Record<string, any>[]> {
  return pool.use(async (conn) => {
    sfLog(`long-running start mode=${authTag} sql=${JSON.stringify(previewSql(query))}`)
    return runLongRunningOnConnection(conn as unknown as PollingConnection, query, longOpts, warehouse, binds)
  })
}

// --- One-shot connection helper (used for per-request tokens) ---

function connectAndQuery(
  config: snowflake.ConnectionOptions,
  query: string,
  authTag: string,
  warehouse?: string,
  binds?: Binds,
): Promise<Record<string, any>[]> {
  const conn = snowflake.createConnection(config)
  const t0 = Date.now()
  sfLog(`query start mode=${authTag} (one-shot) sql=${JSON.stringify(previewSql(query))}`)
  return new Promise((resolve, reject) => {
    conn.connect((err) => {
      if (err) {
        sfLog(`connect failed mode=${authTag}: ${err.message}`)
        return reject(new Error(`Snowflake connection failed: ${err.message}`))
      }
      conn.execute({
        sqlText: query,
        ...warehouseParams(warehouse),
        ...bindParams(binds),
        complete: (err, stmt, rows) => {
          conn.destroy(() => {})
          const ms = Date.now() - t0
          const qid =
            stmt && typeof stmt.getQueryId === "function" ? ` queryId=${stmt.getQueryId()}` : ""
          if (err) {
            sfLog(`query error mode=${authTag} afterMs=${ms}${qid}: ${err.message}`)
            reject(new Error(`Query failed: ${err.message}`))
          } else {
            const out = (rows ?? []) as Record<string, any>[]
            sfLog(`query ok mode=${authTag} rows=${out.length} afterMs=${ms}${qid}`)
            resolve(out)
          }
        },
      })
    })
  })
}

function connectAndQueryLongRunning(
  config: snowflake.ConnectionOptions,
  query: string,
  authTag: string,
  longOpts: { pollIntervalMs: number; statusLogIntervalMs: number; maxWaitMs?: number },
  warehouse?: string,
  binds?: Binds,
): Promise<Record<string, any>[]> {
  const conn = snowflake.createConnection(config)
  const t0 = Date.now()
  sfLog(`long-running start mode=${authTag} (one-shot) sql=${JSON.stringify(previewSql(query))}`)
  return new Promise((resolve, reject) => {
    conn.connect((err) => {
      if (err) {
        sfLog(`connect failed mode=${authTag}: ${err.message}`)
        return reject(new Error(`Snowflake connection failed: ${err.message}`))
      }
      runLongRunningOnConnection(conn as unknown as PollingConnection, query, longOpts, warehouse, binds)
        .then((rows) => {
          conn.destroy(() => {})
          sfLog(`long-running one-shot done mode=${authTag} totalMs=${Date.now() - t0}`)
          resolve(rows)
        })
        .catch((e) => {
          conn.destroy(() => {})
          reject(e)
        })
    })
  })
}

export interface QueryOptions {
  callersRights?: boolean
  /**
   * Run this query on a specific warehouse. Sets the QUERY_WAREHOUSE_NAME
   * statement parameter for the execution. When omitted, the connection's
   * default warehouse is used.
   */
  warehouse?: string
  /**
   * Bind values for `?` placeholders in the SQL. Use this for any user-supplied
   * values to avoid SQL injection, e.g.
   *   querySnowflake("SELECT * FROM t WHERE id = ?", { binds: [id] })
   */
  binds?: Binds
}

/** Extra options for `querySnowflakeLongRunning` / `queryWithTokenLongRunning`. */
export interface LongRunningQueryOptions extends QueryOptions {
  /**
   * How often to call GS while the query is running (default 5000).
   * Kept conservative to limit log volume and API chatter.
   */
  pollIntervalMs?: number
  /**
   * While running, emit at most one `[snowflake] long-running poll …` line per this many ms (default 60000).
   */
  statusLogIntervalMs?: number
  /** Fail if the query is still not finished after this many ms (optional). */
  maxWaitMs?: number
}

function resolveLongRunningOpts(
  o: LongRunningQueryOptions,
): { pollIntervalMs: number; statusLogIntervalMs: number; maxWaitMs?: number } {
  return {
    pollIntervalMs: o.pollIntervalMs ?? 5000,
    statusLogIntervalMs: o.statusLogIntervalMs ?? 60_000,
    maxWaitMs: o.maxWaitMs,
  }
}

/**
 * The current request's headers, keyed by lowercase header name. Query functions take a function
 * that returns them and only call it when a query actually runs with caller's rights, because
 * reading request headers can have side effects (Next.js `headers()` requires a request scope).
 */
export type RequestHeaders = Record<string, string | null | undefined>

export async function querySnowflake(
  query: string,
  options: QueryOptions = {},
  getRequestHeaders: () => RequestHeaders | Promise<RequestHeaders> = () => ({}),
): Promise<Record<string, any>[]> {
  const { callersRights = false, warehouse, binds } = options
  const serviceToken = getServiceToken()

  const useCallersRights = shouldUseCallersRights(callersRights)

  if (serviceToken) {
    const runtime = runtimeName(serviceToken)
    // Case 1 — real deployment with caller's rights: run the query as the calling user.
    if (useCallersRights) {
      const callerToken = (await getRequestHeaders())["sf-context-current-user-token"] ?? ""
      if (!callerToken) {
        throw new Error(
          "No sf-context-current-user-token header. Ensure this is an authenticated request to a deployed Snowflake App.",
        )
      }
      const oauthToken = callerRightsOAuthToken(serviceToken, callerToken)
      return queryWithPool(
        getCallersPool(oauthToken, serviceToken),
        query,
        `${runtime}-caller`,
        warehouse,
        binds,
      )
    }
    // Case 2 — preview (or owner's rights): run with the owner's-rights service token.
    noteCallersRightsIgnoredInPreview(callersRights)
    // Retry once on terminated connection (service token rotated mid-flight).
    try {
      return await queryWithPool(
        getOwnersPool(serviceToken),
        query,
        `${runtime}-owner`,
        warehouse,
        binds,
      )
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err)
      if (msg.includes("terminated connection")) {
        sfLog("query retry: terminated connection — clearing owner pool and retrying once")
        ownersPool?.drain()
        ownersPool = null
        return queryWithPool(
          getOwnersPool(serviceToken),
          query,
          `${runtime}-owner`,
          warehouse,
          binds,
        )
      }
      throw err
    }
  }

  // Case 3 — local dev: no service token, so caller's rights is not possible; use local credentials.
  if (callersRights) {
    console.warn(
      "[snowflake] useCallersRights=true has no effect outside a deployed Snowflake App — using local dev credentials",
    )
  }

  // Explicit env vars: password auth via pooled connections
  if (process.env.SNOWFLAKE_USER && process.env.SNOWFLAKE_PASSWORD) {
    return queryWithPool(getPasswordPool(), query, "password", warehouse, binds)
  }

  // ~/.snowflake/connections.toml or config.toml: use the default connection (local dev)
  const tomlConn = readTomlDefaultConnection()
  if (tomlConn) {
    return queryWithPool(getTomlPool(tomlConn), query, "toml", warehouse, binds)
  }

  throw new Error(
    "No Snowflake credentials found. Provide one of:\n" +
    "  1. Snowflake App service token file at /snowflake/session/token\n" +
    "  2. SNOWFLAKE_USER + SNOWFLAKE_PASSWORD env vars\n" +
      "  3. ~/.snowflake/config.toml with a default connection"
  )
}

/**
 * Same auth and pooling as {@link querySnowflake}, but submits with `asyncExec`, then polls
 * by `queryId` until the statement finishes (intended for queries that often exceed ~1 minute).
 */
export async function querySnowflakeLongRunning(
  query: string,
  options: LongRunningQueryOptions = {},
  getRequestHeaders: () => RequestHeaders | Promise<RequestHeaders> = () => ({}),
): Promise<Record<string, any>[]> {
  const { callersRights = false, warehouse, binds, ...longRest } = options
  const longOpts = resolveLongRunningOpts({ callersRights, ...longRest })
  const serviceToken = getServiceToken()

  const useCallersRights = shouldUseCallersRights(callersRights)

  if (serviceToken) {
    const runtime = runtimeName(serviceToken)
    if (useCallersRights) {
      const callerToken = (await getRequestHeaders())["sf-context-current-user-token"] ?? ""
      if (!callerToken) {
        throw new Error(
          "No sf-context-current-user-token header. Ensure this is an authenticated request to a deployed Snowflake App.",
        )
      }
      const oauthToken = callerRightsOAuthToken(serviceToken, callerToken)
      return queryWithPoolLongRunning(
        getCallersPool(oauthToken, serviceToken),
        query,
        `${runtime}-caller`,
        longOpts,
        warehouse,
        binds,
      )
    }
    noteCallersRightsIgnoredInPreview(callersRights)
    return queryWithPoolLongRunning(
      getOwnersPool(serviceToken),
      query,
      `${runtime}-owner`,
      longOpts,
      warehouse,
      binds,
    )
  }

  if (callersRights) {
    console.warn(
      "[snowflake] useCallersRights=true has no effect outside a deployed Snowflake App — using local dev credentials",
    )
  }

  if (process.env.SNOWFLAKE_USER && process.env.SNOWFLAKE_PASSWORD) {
    return queryWithPoolLongRunning(getPasswordPool(), query, "password", longOpts, warehouse, binds)
  }

  const tomlConn = readTomlDefaultConnection()
  if (tomlConn) {
    return queryWithPoolLongRunning(getTomlPool(tomlConn), query, "toml", longOpts, warehouse, binds)
  }

  throw new Error(
    "No Snowflake credentials found. Provide one of:\n" +
    "  1. Snowflake App service token file at /snowflake/session/token\n" +
    "  2. SNOWFLAKE_USER + SNOWFLAKE_PASSWORD env vars\n" +
    "  3. ~/.snowflake/config.toml with a default connection"
  )
}

// --- Snowflake App caller's rights helpers ---

export function getServiceToken(): string {
  try {
    return fs.readFileSync(SERVICE_TOKEN_PATH, "utf8").trim()
  } catch {
    return ""
  }
}

/**
 * Build the OAuth token for caller's rights.
 *
 * SPCS service tokens are JWTs, and the login path requires the compound
 * serviceToken.callerToken form. CNG service tokens are opaque, and its login path
 * requires the bare caller token; compounding it with the owner token is invalid.
 */
export function buildCallerRightsToken(callerUserToken: string): string {
  const serviceToken = getServiceToken()
  if (!serviceToken) {
    throw new Error("No Snowflake App service token available at " + SERVICE_TOKEN_PATH)
  }
  return callerRightsOAuthToken(serviceToken, callerUserToken)
}

/**
 * Run a query using an explicit OAuth token (for service-rights or caller's-rights).
 * Creates a fresh connection each time since the token may differ per request.
 * Pass `warehouse` to run the query on a specific warehouse (QUERY_WAREHOUSE_NAME).
 * Pass `binds` for `?` placeholder values (recommended for user-supplied input).
 */
export async function queryWithToken(
  query: string,
  token: string,
  warehouse?: string,
  binds?: Binds,
): Promise<Record<string, any>[]> {
  return connectAndQuery({ ...baseConfig(), authenticator: "OAUTH", token }, query, "oauth-token", warehouse, binds)
}

/**
 * One-shot OAuth connection: async submit + poll + fetch (see {@link querySnowflakeLongRunning}).
 */
export async function queryWithTokenLongRunning(
  query: string,
  token: string,
  options: Omit<LongRunningQueryOptions, "callersRights"> = {},
): Promise<Record<string, any>[]> {
  const { warehouse, binds, ...longRest } = options
  const longOpts = resolveLongRunningOpts({ callersRights: false, ...longRest })
  return connectAndQueryLongRunning(
    { ...baseConfig(), authenticator: "OAUTH", token },
    query,
    "oauth-token",
    longOpts,
    warehouse,
    binds,
  )
}

// --- Secrets ---

/**
 * Snowflake secret types supported by {@link getSecret}. Mirrors the `TYPE` of a
 * Snowflake `SECRET` object. (`GENERIC_STRING`, `PASSWORD`, and `OAUTH2` are supported.)
 */
export enum SecretType {
  /** Single opaque string (e.g. an API key). `getSecret` returns the string. */
  GENERIC_STRING = "GENERIC_STRING",
  /** Username + password pair. `getSecret` returns `{ username, password }`. */
  PASSWORD = "PASSWORD",
  /** OAuth2 access token. `getSecret` returns a `Promise<string>` — see the docstring. */
  OAUTH2 = "OAUTH2",
}

/** Value returned for a {@link SecretType.PASSWORD} secret. */
export interface PasswordSecret {
  username: string
  password: string
}

/** Directory where SPCS mounts declared secrets (one subdirectory per secret). */
const SPCS_SECRETS_DIR = "/secrets"

/** `SNOWFLAKE_SECRET_<NAME>_<FIELD>` — the env var the runtime injects (name upper-cased). */
function secretEnvVarName(name: string, field: string): string {
  return `SNOWFLAKE_SECRET_${name.toUpperCase()}_${field}`
}

/** Read a mounted secret file (SPCS) if present and non-empty; otherwise null. */
function readMountedSecretFile(name: string, file: string): string | null {
  const filePath = path.join(SPCS_SECRETS_DIR, name, file)
  try {
    const contents = fs.readFileSync(filePath, "utf8")
    if (typeof contents === "string" && contents.trim() !== "") {
      return contents.trim()
    }
  } catch {
    // File not mounted (e.g. local dev).
  }
  return null
}

/**
 * Read one field of a secret.
 *
 * In SPCS the runtime mounts each declared secret as files under
 * `/secrets/<name>/<field>`. We read that file first, and fall back to the
 * `SNOWFLAKE_SECRET_<NAME>_<FIELD>` env var (upper-case) for local development
 * where no files are mounted.
 *
 * @param name  Logical secret name (matches app.yml `secrets[].name`).
 * @param file  Mounted file name (lower-case), e.g. `secret_string`, `username`, `password`.
 * @param env   Env var field suffix (upper-case), e.g. `SECRET_STRING`, `USERNAME`, `PASSWORD`.
 */
function readSecretField(name: string, file: string, env: string): string {
  // 1. Mounted secret file (SPCS): /secrets/<name>/<file>
  const mounted = readMountedSecretFile(name, file)
  if (mounted !== null) return mounted

  // 2. Env var fallback (local dev): SNOWFLAKE_SECRET_<NAME>_<FIELD>
  const envVar = secretEnvVarName(name, env)
  const value = process.env[envVar]
  if (value) return value

  throw new Error(
    `Secret "${name}" field "${file}" is not available. Expected the mounted file ` +
      `${path.join(SPCS_SECRETS_DIR, name, file)} (SPCS) or the env var ${envVar} (local dev). ` +
      `Declare the secret under the top-level "secrets:" block in app.yml (see README → Secrets).`,
  )
}

// --- OAUTH2 tokens ---
//
// In SPCS the runtime mounts an OAUTH2 secret's (auto-rotated) access token at
// /secrets/<name>/access_token, so we read that file fresh on every call. Locally
// there is no mount and the token expires after a few minutes, so the app mints and
// refreshes tokens itself via the OAuth2 client-credentials grant, caching them in
// memory until shortly before they expire.

interface CachedOAuthToken {
  token: string
  /** Epoch ms after which the cached token must be refreshed. */
  expiresAtMs: number
}

/** Refresh a cached token this many ms before it actually expires. */
const OAUTH_REFRESH_MARGIN_MS = 60_000
/** Fallback lifetime (ms) when the token endpoint omits `expires_in`. */
const OAUTH_DEFAULT_TTL_MS = 300_000

const oauthTokenCache = new Map<string, CachedOAuthToken>()
// De-dupe concurrent refreshes for the same secret so we mint one token, not N.
const oauthInflight = new Map<string, Promise<string>>()

/** Mint a fresh access token via the OAuth2 client-credentials grant and cache it. */
async function fetchClientCredentialsToken(name: string, tokenUrl: string): Promise<string> {
  const clientId = process.env[secretEnvVarName(name, "OAUTH_CLIENT_ID")]
  const clientSecret = process.env[secretEnvVarName(name, "OAUTH_CLIENT_SECRET")]
  const scope = process.env[secretEnvVarName(name, "OAUTH_SCOPE")]

  const body = new URLSearchParams({ grant_type: "client_credentials" })
  if (clientId) body.set("client_id", clientId)
  if (clientSecret) body.set("client_secret", clientSecret)
  if (scope) body.set("scope", scope)

  const res = await fetch(tokenUrl, {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body,
  })
  if (!res.ok) {
    const detail = await res.text().catch(() => "")
    throw new Error(
      `OAuth2 secret "${name}": token endpoint ${tokenUrl} returned ${res.status} ${res.statusText}. ${detail.slice(0, 300)}`,
    )
  }
  const json = (await res.json()) as { access_token?: string; expires_in?: number }
  if (!json.access_token) {
    throw new Error(`OAuth2 secret "${name}": token endpoint response contained no access_token.`)
  }
  const ttlMs = typeof json.expires_in === "number" ? json.expires_in * 1000 : OAUTH_DEFAULT_TTL_MS
  oauthTokenCache.set(name, { token: json.access_token, expiresAtMs: Date.now() + ttlMs })
  return json.access_token
}

/** Local-dev OAuth token: cached client-credentials token (auto-refreshed) or a static env token. */
async function getLocalOAuthToken(name: string): Promise<string> {
  const cached = oauthTokenCache.get(name)
  if (cached && Date.now() < cached.expiresAtMs - OAUTH_REFRESH_MARGIN_MS) {
    return cached.token
  }

  const tokenUrl = process.env[secretEnvVarName(name, "OAUTH_TOKEN_URL")]
  if (tokenUrl) {
    // De-dupe concurrent refreshes so a burst of requests mints a single token.
    let inflight = oauthInflight.get(name)
    if (!inflight) {
      inflight = fetchClientCredentialsToken(name, tokenUrl).finally(() => oauthInflight.delete(name))
      oauthInflight.set(name, inflight)
    }
    return inflight
  }

  // Escape hatch for quick local testing: a static token (NOT auto-refreshed — it will expire).
  const staticToken = process.env[secretEnvVarName(name, "ACCESS_TOKEN")]
  if (staticToken) return staticToken

  throw new Error(
    `OAuth2 secret "${name}" is not available locally. Set ` +
      `${secretEnvVarName(name, "OAUTH_TOKEN_URL")} + ${secretEnvVarName(name, "OAUTH_CLIENT_ID")} + ` +
      `${secretEnvVarName(name, "OAUTH_CLIENT_SECRET")} for the client-credentials refresh flow ` +
      `(recommended — auto-refreshes), or ${secretEnvVarName(name, "ACCESS_TOKEN")} for a short-lived ` +
      `static token (see README → Secrets).`,
  )
}

/** Resolve an OAuth2 access token: SPCS mounted file (auto-rotated) first, else app-level flow. */
async function readOAuth2Token(name: string): Promise<string> {
  const mounted = readMountedSecretFile(name, "access_token")
  if (mounted !== null) return mounted
  return getLocalOAuthToken(name)
}

/**
 * Read a Snowflake SECRET value at runtime.
 *
 * Use this helper rather than reading `SNOWFLAKE_SECRET_*` env vars yourself or
 * querying the SECRET object directly (e.g. `SYSTEM$GET_SECRET`) — a direct
 * runtime query is insecure and is not supported.
 *
 * The secret must be declared under a `secrets:` block in `app.yml` so the runtime
 * mounts it. `secrets:` is a TOP-LEVEL key (a sibling of `install:`/`run:`/
 * `profile:`) — do NOT nest it under `run:`, or it is ignored:
 *
 *   secrets:                  # top-level, NOT under run:
 *     - name: API_KEY
 *       secret: db.schema.api_key_secret
 *
 * Resolution (mounted file first, then local-dev fallback):
 *   - GENERIC_STRING → /secrets/<name>/secret_string   | SNOWFLAKE_SECRET_<NAME>_SECRET_STRING
 *   - PASSWORD       → /secrets/<name>/username         | SNOWFLAKE_SECRET_<NAME>_USERNAME
 *                      /secrets/<name>/password         | SNOWFLAKE_SECRET_<NAME>_PASSWORD
 *   - OAUTH2 (async) → /secrets/<name>/access_token in SPCS (mounted, auto-rotated). Locally the
 *                      app mints + refreshes tokens via the OAuth2 client-credentials grant using
 *                      SNOWFLAKE_SECRET_<NAME>_OAUTH_TOKEN_URL / _OAUTH_CLIENT_ID / _OAUTH_CLIENT_SECRET
 *                      (+ optional _OAUTH_SCOPE), caching until just before expiry. `getSecret`
 *                      returns a `Promise<string>` for OAUTH2 — `await` it.
 *
 * @example
 *   const apiKey = getSecret("API_KEY", SecretType.GENERIC_STRING)
 *   const { username, password } = getSecret("DB_CREDS", SecretType.PASSWORD)
 *   const token = await getSecret("MY_OAUTH", SecretType.OAUTH2)
 */
export function getSecret(name: string, type: SecretType.GENERIC_STRING): string
export function getSecret(name: string, type: SecretType.PASSWORD): PasswordSecret
export function getSecret(name: string, type: SecretType.OAUTH2): Promise<string>
export function getSecret(name: string, type: SecretType): string | PasswordSecret | Promise<string>
export function getSecret(
  name: string,
  type: SecretType,
): string | PasswordSecret | Promise<string> {
  switch (type) {
    case SecretType.GENERIC_STRING:
      return readSecretField(name, "secret_string", "SECRET_STRING")
    case SecretType.PASSWORD:
      return {
        username: readSecretField(name, "username", "USERNAME"),
        password: readSecretField(name, "password", "PASSWORD"),
      }
    case SecretType.OAUTH2:
      return readOAuth2Token(name)
    default:
      throw new Error(`Unsupported secret type: ${String(type)}`)
  }
}

// --- Pool lifecycle ---

/**
 * Gracefully drain and destroy all active connection pools.
 * Call during server shutdown for clean resource cleanup.
 */
export async function closePool(): Promise<void> {
  const drainPromises: Promise<void>[] = []

  if (ownersPool) {
    const p = ownersPool
    ownersPool = null
    ownersPoolToken = ""
    drainPromises.push(p.drain())
  }

  for (const pool of callersPool.values()) {
    drainPromises.push(pool.drain())
  }
  callersPool.clear()

  if (passwordPool) {
    const p = passwordPool
    passwordPool = null
    drainPromises.push(p.drain())
  }

  if (tomlPool) {
    const p = tomlPool
    tomlPool = null
    drainPromises.push(p.drain())
  }

  await Promise.all(drainPromises)
}

/** Returns the Snowflake account base URL for REST API calls (Cortex Analyst, etc.). */
export function getSnowflakeBaseUrl(): string | null {
  if (process.env.SNOWFLAKE_ACCOUNT_URL) return process.env.SNOWFLAKE_ACCOUNT_URL
  if (process.env.SNOWFLAKE_HOST) return `https://${process.env.SNOWFLAKE_HOST}`
  const conn = readTomlDefaultConnection()
  if (!conn) return null
  if (conn.host) {
    const protocol = (conn.protocol as string | undefined) ?? "https"
    const port = conn.port ? `:${conn.port}` : ""
    return `${protocol}://${conn.host}${port}`
  }
  if (conn.account) {
    // "MYORG-MY_ACCOUNT" → "myorg-my-account.snowflakecomputing.com"
    return `https://${(conn.account as string).toLowerCase().replace(/_/g, "-")}.snowflakecomputing.com`
  }
  return null
}

/**
 * Returns the Authorization header value for Snowflake REST API calls.
 * Both Snowflake App service tokens and external OAuth tokens use "Bearer ..." format.
 * Callers must also send `X-Snowflake-Authorization-Token-Type: OAUTH`.
 * (The "Snowflake Token=..." form is rejected with 401/390104 by the Cortex
 * inference endpoint when using a Snowflake App service token.)
 */
export function getRestApiAuthHeader(): string {
  const serviceToken = getServiceToken()
  if (serviceToken) return `Bearer ${serviceToken}`
  const conn = readTomlDefaultConnection()
  if (conn) {
    const auth = (typeof conn.authenticator === "string" ? conn.authenticator : "").toUpperCase()
    if (auth === "OAUTH" && conn.token_file_path) {
      try {
        return `Bearer ${fs.readFileSync(conn.token_file_path as string, "utf8").trim()}`
      } catch { /* token file missing or unreadable */ }
    }
  }
  return `Bearer ` // will 401 — no valid local auth available
}
