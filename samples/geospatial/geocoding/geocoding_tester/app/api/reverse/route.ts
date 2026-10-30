import { errorResponse, firstValue, q, scratchName, withScratch } from "@/lib/geocode"

export const dynamic = "force-dynamic"

type Point = { lat: number; lon: number }

// POST { points: [{lat, lon}], radius, country? } -> nearest address per point.
// The single-point shape { lat, lon } is still accepted.
export async function POST(req: Request) {
  try {
    const body = (await req.json()) as { points?: Point[]; lat?: number; lon?: number; radius?: number; country?: string }
    const raw = body.points ?? [{ lat: Number(body.lat), lon: Number(body.lon) }]
    const points = raw
      .map((p) => ({ lat: Number(p.lat), lon: Number(p.lon) }))
      .filter((p) => Number.isFinite(p.lat) && Number.isFinite(p.lon) && Math.abs(p.lat) <= 90 && Math.abs(p.lon) <= 180)
      .slice(0, 200)
    if (!points.length) return Response.json({ error: "Enter at least one valid lat, lon" }, { status: 400 })
    const radius = Math.min(Math.max(Number(body.radius) || 200, 10), 5000)
    const cc = body.country ? body.country.toUpperCase().slice(0, 2) : null

    const src = scratchName("REV_IN")
    const out = scratchName("REV_OUT")
    const t0 = Date.now()
    return await withScratch([src, out], async () => {
      const sql = [
        `CREATE TRANSIENT TABLE ${src} AS SELECT INDEX + 1 AS ID, VALUE:lat::FLOAT AS LAT, VALUE:lon::FLOAT AS LON FROM TABLE(FLATTEN(PARSE_JSON(?)))`,
        `CALL GEOCODING.PUBLIC.REVERSE_GEOCODE_TABLE('${src}', 'LAT', 'LON', 'ID', ?, '${out}', ?)`,
        `SELECT * EXCLUDE (INPUT_GEOG, RESULT_GEOG), ST_X(RESULT_GEOG) AS LON, ST_Y(RESULT_GEOG) AS LAT FROM ${out} ORDER BY INPUT_ID`,
      ]
      await q(sql[0], [JSON.stringify(points)])
      const status = firstValue(await q(sql[1], [radius, cc]))
      const rows = await q(sql[2])
      return Response.json({ rows, sql, status, ms: Date.now() - t0, input: { points, radius } })
    })
  } catch (e) {
    return errorResponse(e)
  }
}
