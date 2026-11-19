import { NextResponse } from "next/server"

// Local dev only: become a different teammate so two browsers can play two people.
//   /api/dev/as?name=DASH  (redirects back to the referring page)
export async function GET(req: Request) {
  if (process.env.NODE_ENV === "production") return NextResponse.json({ error: "Not available" }, { status: 404 })
  const url = new URL(req.url)
  const name = (url.searchParams.get("name") ?? "").toUpperCase()
  if (!/^[A-Z0-9_.-]{1,40}$/.test(name)) return NextResponse.json({ error: "Bad name" }, { status: 400 })
  const back = url.searchParams.get("back")
  const res = NextResponse.redirect(new URL(back && back.startsWith("/") ? back : "/", url))
  res.cookies.set("ir_user", name, { path: "/", sameSite: "lax" })
  return res
}
