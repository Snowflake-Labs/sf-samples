import { errorResponse, firstValue, q, scratchName, withScratch } from "@/lib/geocode"

export const dynamic = "force-dynamic"

type Item = { address: string; country?: string }

// POST { items: [{address, country?}], country } -> forward geocode results.
// A per-item country overrides the default, so one batch can mix countries.
export async function POST(req: Request) {
  try {
    const body = (await req.json()) as { items?: Item[]; addresses?: string[]; country?: string }
    const cc = (body.country || "US").toUpperCase().slice(0, 2)
    const items = (body.items ?? (body.addresses ?? []).map((address): Item => ({ address })))
      .map((i) => ({ address: String(i.address ?? "").trim(), country: (i.country || cc).toUpperCase().slice(0, 2) }))
      .filter((i) => i.address)
      .slice(0, 200)
    if (!items.length) return Response.json({ error: "Enter at least one address" }, { status: 400 })

    const src = scratchName("FWD_IN")
    const out = scratchName("FWD_OUT")
    const t0 = Date.now()
    return await withScratch([src, out], async () => {
      const sql = [
        `CREATE TRANSIENT TABLE ${src} (ID INT, ADDRESS STRING, COUNTRY STRING)`,
        `INSERT INTO ${src} SELECT INDEX + 1, VALUE:address::STRING, VALUE:country::STRING FROM TABLE(FLATTEN(PARSE_JSON(?)))`,
        `CALL GEOCODING.PUBLIC.FORWARD_GEOCODE_TABLE('${src}', 'ADDRESS', 'ID', '${out}', ?, 'COUNTRY')`,
        `SELECT * EXCLUDE (RESULT_GEOG), ST_X(RESULT_GEOG) AS LON, ST_Y(RESULT_GEOG) AS LAT FROM ${out} ORDER BY INPUT_ID`,
      ]
      await q(sql[0])
      await q(sql[1], [JSON.stringify(items)])
      const status = firstValue(await q(sql[2], [cc]))
      if (status?.startsWith("ERROR")) return Response.json({ error: status, sql }, { status: 400 })
      const rows = await q(sql[3])
      return Response.json({ rows, sql, status, ms: Date.now() - t0 })
    })
  } catch (e) {
    return errorResponse(e)
  }
}
