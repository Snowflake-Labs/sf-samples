import { currentUser } from "@/lib/room/identity"
import { markAbsent, markPresent, present, publish, subscribe } from "@/lib/room/bus"

export const runtime = "nodejs"
export const dynamic = "force-dynamic"

// Live room events (Server-Sent Events). History comes from GET /api/incidents/:id.
export async function GET(req: Request, ctx: { params: Promise<{ id: string }> }) {
  const { id } = await ctx.params
  const user = await currentUser()
  const enc = new TextEncoder()
  const presence = () =>
    publish({ id: `p-${Date.now()}`, incidentId: id, at: Date.now(), by: "INCIDENT_ROOM", kind: "presence", payload: { users: present(id) } })

  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      let closed = false
      const write = (s: string) => {
        if (closed) return
        try {
          controller.enqueue(enc.encode(s))
        } catch {
          closed = true
        }
      }
      const unsubscribe = subscribe(id, (e) => write(`data: ${JSON.stringify(e)}\n\n`))
      markPresent(id, user)
      presence()
      const hb = setInterval(() => {
        write(`: hb\n\n`)
        markPresent(id, user)
      }, 25_000)
      req.signal.addEventListener("abort", () => {
        closed = true
        clearInterval(hb)
        unsubscribe()
        markAbsent(id, user)
        presence()
        try {
          controller.close()
        } catch {
          /* already closed */
        }
      })
    },
  })
  return new Response(stream, {
    headers: {
      "content-type": "text/event-stream",
      "cache-control": "no-cache, no-transform",
      "x-accel-buffering": "no",
      connection: "keep-alive",
    },
  })
}
