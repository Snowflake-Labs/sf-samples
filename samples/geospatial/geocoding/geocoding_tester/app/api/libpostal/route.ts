import { querySnowflake } from "@/lib/snowflake"

export const dynamic = "force-dynamic"

// Status and start/stop for the libpostal SPCS service and its compute pool.
// Runs as the app owner (owner's rights). The only input is the action name,
// which picks one of the fixed statements below.
const SERVICE = "GEOCODING.PUBLIC.LIBPOSTAL_SVC"
const POOL = "GEOCODING_LIBPOSTAL_POOL"

const ACTIONS: Record<string, string[]> = {
  start: [`ALTER COMPUTE POOL ${POOL} RESUME`, `ALTER SERVICE ${SERVICE} RESUME`],
  stop: [`ALTER SERVICE ${SERVICE} SUSPEND`, `ALTER COMPUTE POOL ${POOL} SUSPEND`],
}

async function status() {
  const svc = await querySnowflake(`SHOW SERVICES LIKE 'LIBPOSTAL_SVC' IN SCHEMA GEOCODING.PUBLIC`).catch(() => [])
  if (!svc.length) return { installed: false }
  const pool = await querySnowflake(`SHOW COMPUTE POOLS LIKE '${POOL}'`).catch(() => [])
  let container: string | null = null
  try {
    const r = await querySnowflake(`SELECT SYSTEM$GET_SERVICE_STATUS('${SERVICE}') AS S`)
    container = JSON.parse(String(r[0]?.S ?? "[]"))[0]?.status ?? null
  } catch {
    container = null
  }
  return {
    installed: true,
    service: String(svc[0].status),
    container,
    autoSuspendSecs: Number(svc[0].auto_suspend_secs ?? 0),
    pool: pool[0] ? String(pool[0].state) : null,
    instanceFamily: pool[0] ? String(pool[0].instance_family) : null,
  }
}

export async function GET() {
  try {
    return Response.json(await status())
  } catch (e) {
    return Response.json({ error: e instanceof Error ? e.message : String(e) }, { status: 500 })
  }
}

export async function POST(req: Request) {
  const { action } = (await req.json().catch(() => ({}))) as { action?: string }
  const statements = action ? ACTIONS[action] : undefined
  if (!statements) return Response.json({ error: "action must be start or stop" }, { status: 400 })
  const errors: string[] = []
  for (const sql of statements) {
    // RESUME on a running object and SUSPEND on a suspended one raise errors
    // that only mean "already in that state".
    await querySnowflake(sql).catch((e) => {
      const m = e instanceof Error ? e.message : String(e)
      if (!/already|not in .* state|cannot be (resumed|suspended)/i.test(m)) errors.push(m)
    })
  }
  return Response.json({ ...(await status()), errors })
}
