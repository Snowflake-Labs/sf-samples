import { NextResponse } from "next/server"
import { currentUser } from "@/lib/room/identity"
import { RoomError, snapshot } from "@/lib/room/service"

export const dynamic = "force-dynamic"

export async function GET(_req: Request, ctx: { params: Promise<{ id: string }> }) {
  const { id } = await ctx.params
  try {
    return NextResponse.json({ ...(await snapshot(id)), you: await currentUser() })
  } catch (e) {
    if (e instanceof RoomError) return NextResponse.json({ error: e.message }, { status: e.status })
    console.error(new Date().toISOString(), "[incident] snapshot failed", e)
    return NextResponse.json({ error: (e as Error).message }, { status: 500 })
  }
}
