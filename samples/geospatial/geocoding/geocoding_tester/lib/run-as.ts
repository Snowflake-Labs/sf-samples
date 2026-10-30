import { querySnowflake } from "@/lib/snowflake"

type QueryOptions = NonNullable<Parameters<typeof querySnowflake>[1]>

/**
 * Who answered a query:
 * - "caller": the person viewing the deployed app (caller's rights ran).
 * - "service": the app's own role, after caller's rights failed on grants.
 * - "dev": local dev or a Workspaces preview, where caller's rights does not
 *   apply and every query runs as the developer's or owner's credentials.
 */
export type Identity = "caller" | "service" | "dev"

/** Caller's rights only applies to a deployed app. The SDK ignores it in dev mode. */
export function callersRightsActive(): boolean {
  return process.env.NODE_ENV !== "development"
}

/**
 * True for errors that mean the caller cannot run the query as themselves:
 * missing grants, or no caller token on the request. Other errors (syntax,
 * timeouts, outages) are not a reason to retry as the app.
 */
export function isCallerAccessError(e: unknown): boolean {
  const message = e instanceof Error ? e.message : String(e)
  return /insufficient privileges|does not exist or not authorized|sf-context-current-user-token/i.test(message)
}

/**
 * Run `load` with caller's rights, falling back to owner's rights only when
 * the caller lacks access. The result says which identity answered so the UI
 * can say whose data it shows.
 *
 *   const { identity, ...data } = await withCallerFallback((opts) => load(opts))
 */
export async function withCallerFallback<T extends object>(
  load: (opts: QueryOptions) => Promise<T>,
): Promise<T & { identity: Identity }> {
  if (!callersRightsActive()) return { ...(await load({})), identity: "dev" }
  try {
    return { ...(await load({ callersRights: true })), identity: "caller" }
  } catch (e) {
    if (!isCallerAccessError(e)) throw e
    console.warn("Caller's rights query failed, retrying as the app's role:", e)
    return { ...(await load({})), identity: "service" }
  }
}

/** Single-query shorthand for withCallerFallback. */
export async function queryAsCallerOrService(sql: string) {
  return withCallerFallback(async (opts) => ({ rows: await querySnowflake(sql, opts) }))
}

/**
 * A database to resolve INFORMATION_SCHEMA table functions against: the
 * session's current database, else the first one the role can see. Returns a
 * quoted identifier, ready to interpolate into SQL.
 */
export async function resolveDatabase(opts: QueryOptions = {}): Promise<string | null> {
  const [current] = await querySnowflake("SELECT CURRENT_DATABASE() AS DB", opts)
  if (current?.DB) return quoteIdent(current.DB as string)
  const dbs = await querySnowflake("SHOW TERSE DATABASES", opts)
  const name = dbs[0]?.name as string | undefined
  return name ? quoteIdent(name) : null
}

/** Quote a Snowflake identifier, e.g. a user name from CURRENT_USER(). */
export function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`
}
