import { NextResponse } from "next/server"
import { currentUser } from "@/lib/room/identity"
import { clearAndon, closeIncident, fileDecision, postMessage, pullAndon, RoomError, ship, signalTyping, vote } from "@/lib/room/service"

export const dynamic = "force-dynamic"

type Action =
  | { type: "message"; text: string }
  | { type: "andon"; reason?: string }
  | { type: "andon.clear" }
  | { type: "vote"; patchId: string; vote: "approve" | "reject" }
  | { type: "ship"; patchId: string }
  | { type: "close" }
  | { type: "typing"; on: boolean }
  | { type: "file"; text: string }

export async function POST(req: Request, ctx: { params: Promise<{ id: string }> }) {
  const { id } = await ctx.params
  const by = await currentUser()
  const a = (await req.json().catch(() => null)) as Action | null
  try {
    switch (a?.type) {
      case "message":
        return NextResponse.json(await postMessage(id, by, String(a.text ?? "")))
      case "andon":
        await pullAndon(id, by, a.reason?.slice(0, 300))
        break
      case "andon.clear":
        await clearAndon(id, by)
        break
      case "vote":
        if (a.vote !== "approve" && a.vote !== "reject") return NextResponse.json({ error: "Bad vote" }, { status: 400 })
        await vote(id, by, String(a.patchId), a.vote)
        break
      case "ship":
        await ship(id, by, String(a.patchId))
        break
      case "file":
        await fileDecision(id, by, String(a.text ?? ""))
        break
      case "typing":
        signalTyping(id, by, a.on !== false)
        break
      case "close":
        await closeIncident(id, by)
        break
      default:
        return NextResponse.json({ error: "Unknown action" }, { status: 400 })
    }
    return NextResponse.json({ ok: true })
  } catch (e) {
    if (e instanceof RoomError) return NextResponse.json({ error: e.message }, { status: e.status })
    console.error(new Date().toISOString(), "[action] failed", a?.type, e)
    return NextResponse.json({ error: (e as Error).message }, { status: 500 })
  }
}
