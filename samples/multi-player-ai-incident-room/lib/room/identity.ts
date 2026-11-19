// Who is in the room. Deployed: Snowflake's ingress sets Sf-Context-Current-User.
// Local dev: a name from the `ir_user` cookie (set by /api/dev/as?name=NAME, local dev only) so two
// browsers can play two teammates.

import { cookies, headers } from "next/headers"

export async function currentUser(): Promise<string> {
  const h = await headers()
  const sf = h.get("sf-context-current-user")
  if (sf) return sf.toUpperCase()
  if (process.env.NODE_ENV === "production") return "UNKNOWN"
  const c = (await cookies()).get("ir_user")?.value
  const name = (c ?? process.env.IR_LOCAL_USER ?? "DEV").toUpperCase()
  return /^[A-Z0-9_.-]{1,40}$/.test(name) ? name : "DEV"
}
