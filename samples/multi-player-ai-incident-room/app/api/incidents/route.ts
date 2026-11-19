import { NextResponse } from "next/server"
import { currentUser } from "@/lib/room/identity"
import { listIncidents } from "@/lib/room/store"
import { openIncident } from "@/lib/room/service"

export const dynamic = "force-dynamic"

export async function GET() {
  try {
    return NextResponse.json({ incidents: await listIncidents() })
  } catch (e) {
    console.error(new Date().toISOString(), "[incidents] list failed", e)
    return NextResponse.json({ error: (e as Error).message }, { status: 500 })
  }
}

export async function POST(req: Request) {
  const by = await currentUser()
  const b = (await req.json().catch(() => ({}))) as { title?: string; objectFqn?: string; errorMessage?: string }
  const title = (b.title ?? "").trim().slice(0, 200)
  if (!title) return NextResponse.json({ error: "A title is required" }, { status: 400 })
  try {
    const id = await openIncident(by, { title, objectFqn: b.objectFqn?.slice(0, 300), errorMessage: b.errorMessage?.slice(0, 2000) })
    return NextResponse.json({ id }, { status: 201 })
  } catch (e) {
    console.error(new Date().toISOString(), "[incidents] open failed", e)
    return NextResponse.json({ error: (e as Error).message }, { status: 500 })
  }
}
