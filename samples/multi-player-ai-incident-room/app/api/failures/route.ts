import { NextResponse } from "next/server"
import { recentFailures } from "@/lib/room/store"

export const dynamic = "force-dynamic"

export async function GET() {
  try {
    const runs = await recentFailures()
    // `latest` is the newest run of the watched task, so the line view can go green after a ship.
    return NextResponse.json({ failures: runs.filter((r) => r.state === "FAILED"), latest: runs[0]?.state ?? null })
  } catch (e) {
    console.error(new Date().toISOString(), "[failures] failed", e)
    return NextResponse.json({ error: (e as Error).message }, { status: 500 })
  }
}
