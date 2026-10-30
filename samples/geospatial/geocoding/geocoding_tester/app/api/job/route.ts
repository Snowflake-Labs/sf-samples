import { randomUUID } from "crypto"
import { POST as forward } from "../forward/route"
import { POST as reverse } from "../reverse/route"
import { POST as normalize } from "../normalize/route"

export const dynamic = "force-dynamic"

// Snowflake App Runtime's ingress closes requests after about a minute, but a
// 200-row batch can take longer. POST starts the work and returns a job id at
// once; the page polls GET ?id= until it is done. Jobs live in memory for an
// hour (the app runs as a single instance).
type Job = { status: "running" | "done"; httpStatus?: number; result?: unknown; at: number }
const jobs = new Map<string, Job>()
const HANDLERS: Record<string, (r: Request) => Promise<Response>> = { forward, reverse, normalize }

export async function POST(req: Request) {
  const { kind, body } = (await req.json()) as { kind: string; body: unknown }
  const handler = HANDLERS[kind]
  if (!handler) return Response.json({ error: `unknown kind ${kind}` }, { status: 400 })

  for (const [k, j] of jobs) if (Date.now() - j.at > 3_600_000) jobs.delete(k)
  const id = randomUUID()
  jobs.set(id, { status: "running", at: Date.now() })
  const inner = new Request(req.url, { method: "POST", headers: req.headers, body: JSON.stringify(body) })
  handler(inner)
    .then(async (r) => jobs.set(id, { status: "done", httpStatus: r.status, result: await r.json(), at: Date.now() }))
    .catch((e) => jobs.set(id, { status: "done", httpStatus: 500, result: { error: String(e) }, at: Date.now() }))
  return Response.json({ id })
}

export async function GET(req: Request) {
  const id = new URL(req.url).searchParams.get("id") ?? ""
  const job = jobs.get(id)
  if (!job) return Response.json({ error: "unknown job" }, { status: 404 })
  return Response.json(job)
}
