/**
 * Snowflake query helper. Call from any server component or route handler:
 *   const rows = await querySnowflake("SELECT ...")
 *   const rows = await querySnowflake("SELECT ...", { callersRights: true })
 *   const rows = await querySnowflake("SELECT ...", { warehouse: "MY_WH" })
 *
 * Variable binding (recommended for any user-supplied values — prevents SQL injection):
 *   const rows = await querySnowflake(
 *     "SELECT * FROM t WHERE id = ? AND status = ?",
 *     { binds: [id, status] },
 *   )
 *   Use `?` placeholders in the SQL and pass values via the `binds` option. Binds are
 *   supported on both helpers and on queryWithToken(...). See the Snowflake SDK docs for
 *   supported bind value types.
 *
 * Which helper to use:
 *   - querySnowflake — default for simple, fast queries (small lookups, tight filters,
 *     typically well under ~10 seconds end-to-end).
 *   - querySnowflakeLongRunning — use when the work may exceed ~10 seconds or is
 *     warehouse-heavy (large scans, big aggregations, COPY/EXPORT, long-running
 *     procedures, resuming warehouse, etc.). Submits async, polls by query id, then
 *     fetches rows:
 *       const rows = await querySnowflakeLongRunning("CALL MY_LONG_JOB(...)")
 *   Same auth options (callersRights, pools) apply to both.
 *
 * This file is the Next.js wrapper around the platform-agnostic SDK in ./sar-sdk: it supplies
 * the request headers (from next/headers) that caller's-rights queries read, and re-exports
 * the rest of the SDK unchanged. Keep Next.js-specific code here. Auth detection, logging,
 * connection pooling, secrets, and cleanup are documented in ./sar-sdk.
 */

import { headers } from "next/headers"
import * as sdk from "./sar-sdk"

export {
  readTomlDefaultConnection,
  resetTomlConfigCache,
  type LongRunningQueryOptions,
  getServiceToken,
  buildCallerRightsToken,
  queryWithToken,
  queryWithTokenLongRunning,
  SecretType,
  type PasswordSecret,
  getSecret,
  closePool,
  getSnowflakeBaseUrl,
  getRestApiAuthHeader,
} from "./sar-sdk"

async function requestHeaders(): Promise<sdk.RequestHeaders> {
  return Object.fromEntries(await headers())
}

export async function querySnowflake(query: string, options: sdk.QueryOptions = {}): Promise<Record<string, any>[]> {
  return sdk.querySnowflake(query, options, requestHeaders)
}

/**
 * Same auth and pooling as {@link querySnowflake}, but submits with `asyncExec`, then polls
 * by `queryId` until the statement finishes (intended for queries that often exceed ~1 minute).
 */
export async function querySnowflakeLongRunning(
  query: string,
  options: sdk.LongRunningQueryOptions = {},
): Promise<Record<string, any>[]> {
  return sdk.querySnowflakeLongRunning(query, options, requestHeaders)
}
