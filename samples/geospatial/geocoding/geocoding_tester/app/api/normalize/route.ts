import { errorResponse, q } from "@/lib/geocode"

export const dynamic = "force-dynamic"

// POST { address, country } -> every normalisation UDF applied to one address.
// The libpostal functions are queried separately so a --no-intl install still
// returns the US parser output.
export async function POST(req: Request) {
  try {
    const { address, country } = (await req.json()) as { address: string; country?: string }
    const a = String(address ?? "").trim()
    if (!a) return Response.json({ error: "Enter an address" }, { status: 400 })
    const cc = (country || "US").toUpperCase().slice(0, 2)
    const t0 = Date.now()

    const usSql = `SELECT GEOCODING.PUBLIC.PARSE_ADDRESS(?) AS PARSE_ADDRESS,
  GEOCODING.PUBLIC.STANDARDIZE_STREET(PARSE_ADDRESS:street::STRING) AS STANDARDIZE_STREET`
    const intlSql = `SELECT GEOCODING.PUBLIC.PARSE_ADDRESS_INTL(?) AS PARSE_ADDRESS_INTL,
  GEOCODING.PUBLIC.EXPAND_STREET_INTL(?) AS EXPAND_STREET_INTL,
  GEOCODING.PUBLIC.NORMALIZE_STREET_EU(PARSE_ADDRESS_INTL:road::STRING, ?) AS NORMALIZE_STREET_EU`

    const [us, intl] = await Promise.allSettled([q(usSql, [a]), q(intlSql, [a, a, cc])])
    const parse = (v: unknown) => (typeof v === "string" ? safeJson(v) : v)
    const result: Record<string, unknown> = {}
    if (us.status === "fulfilled") {
      for (const [k, v] of Object.entries(us.value[0] ?? {})) result[k] = parse(v)
    } else result.PARSE_ADDRESS = { error: String(us.reason?.message ?? us.reason) }
    if (intl.status === "fulfilled") {
      for (const [k, v] of Object.entries(intl.value[0] ?? {})) result[k] = parse(v)
    } else result.PARSE_ADDRESS_INTL = { error: String(intl.reason?.message ?? intl.reason) }

    return Response.json({ result, sql: [usSql, intlSql], ms: Date.now() - t0 })
  } catch (e) {
    return errorResponse(e)
  }
}

function safeJson(s: string) {
  try {
    return JSON.parse(s)
  } catch {
    return s
  }
}
