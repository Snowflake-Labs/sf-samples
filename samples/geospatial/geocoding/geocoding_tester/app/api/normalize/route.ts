import { errorResponse, q } from "@/lib/geocode"

export const dynamic = "force-dynamic"

type Item = { address: string; country?: string }

// POST { address, country }            -> { result }   every UDF on one address
// POST { items: [{address, country?}], country } -> { results: [{ID, ADDRESS, COUNTRY, ...}] }
// The libpostal functions are queried separately so a --no-intl install still
// returns the US parser output. Inputs go in as one bound JSON array.
const MAX_ITEMS = 200

const SRC = `WITH i AS (
  SELECT f.index + 1 AS ID, f.value:address::STRING AS ADDRESS,
         UPPER(LEFT(COALESCE(NULLIF(f.value:country::STRING, ''), ?), 2)) AS COUNTRY
  FROM TABLE(FLATTEN(PARSE_JSON(?))) f)`
const US_SQL = `${SRC}
SELECT ID, GEOCODING.PUBLIC.PARSE_ADDRESS(ADDRESS) AS PARSE_ADDRESS,
  GEOCODING.PUBLIC.STANDARDIZE_STREET(PARSE_ADDRESS:street::STRING) AS STANDARDIZE_STREET
FROM i ORDER BY ID`
const INTL_SQL = `${SRC}
SELECT ID, GEOCODING.PUBLIC.PARSE_ADDRESS_INTL(ADDRESS) AS PARSE_ADDRESS_INTL,
  GEOCODING.PUBLIC.EXPAND_STREET_INTL(ADDRESS) AS EXPAND_STREET_INTL,
  GEOCODING.PUBLIC.NORMALIZE_STREET_EU(PARSE_ADDRESS_INTL:road::STRING, COUNTRY) AS NORMALIZE_STREET_EU
FROM i ORDER BY ID`

export async function POST(req: Request) {
  try {
    const body = (await req.json()) as { address?: string; items?: Item[]; country?: string }
    const single = !Array.isArray(body.items)
    const items = (single ? [{ address: String(body.address ?? "") }] : body.items!)
      .map((i) => ({ address: String(i?.address ?? "").trim(), country: i?.country ? String(i.country) : undefined }))
      .filter((i) => i.address)
    if (!items.length) return Response.json({ error: "Enter an address" }, { status: 400 })
    if (items.length > MAX_ITEMS) return Response.json({ error: `At most ${MAX_ITEMS} addresses per run` }, { status: 400 })
    const cc = String(body.country || "US").toUpperCase().slice(0, 2)
    const binds = [cc, JSON.stringify(items)]
    const t0 = Date.now()

    const [us, intl] = await Promise.allSettled([q(US_SQL, binds), q(INTL_SQL, binds)])
    const results: Record<string, unknown>[] = items.map((it, n) => ({
      ID: n + 1,
      ADDRESS: it.address,
      COUNTRY: (it.country || cc).toUpperCase().slice(0, 2),
    }))
    const merge = (r: PromiseSettledResult<Record<string, unknown>[]>, errKey: string) => {
      if (r.status === "rejected") {
        const msg = String((r.reason as Error)?.message ?? r.reason)
        for (const row of results) row[errKey] = { error: msg }
        return
      }
      for (const row of r.value) {
        const target = results[Number(row.ID) - 1]
        if (!target) continue
        for (const [k, v] of Object.entries(row)) if (k !== "ID") target[k] = parse(v)
      }
    }
    merge(us, "PARSE_ADDRESS")
    merge(intl, "PARSE_ADDRESS_INTL")

    const sql = [US_SQL, INTL_SQL]
    const ms = Date.now() - t0
    if (single) {
      const { ID: _id, ADDRESS: _a, COUNTRY: _c, ...result } = results[0]
      return Response.json({ result, sql, ms })
    }
    return Response.json({ results, sql, ms })
  } catch (e) {
    return errorResponse(e)
  }
}

const parse = (v: unknown) => (typeof v === "string" ? safeJson(v) : v)

function safeJson(s: string) {
  try {
    return JSON.parse(s)
  } catch {
    return s
  }
}
