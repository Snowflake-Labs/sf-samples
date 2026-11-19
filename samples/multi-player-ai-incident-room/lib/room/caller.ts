// Facts from the voter's own Snowflake session, via caller's rights. Deployed only:
// local dev has no caller token, so the room identity is taken as given.

import { querySnowflake } from "@/lib/snowflake"
import { callersRightsActive } from "@/lib/run-as"
import { approverCheck } from "@/lib/domain/approver"

export const APPROVER_ROLE = process.env.IR_APPROVER_ROLE?.trim().toUpperCase() || undefined

export async function checkApprover(roomUser: string) {
  if (!callersRightsActive()) return approverCheck(roomUser, undefined, { sessionUser: roomUser })
  try {
    const [r] = APPROVER_ROLE
      ? await querySnowflake(`SELECT CURRENT_USER() AS U, IS_ROLE_IN_SESSION(?) AS R`, { binds: [APPROVER_ROLE], callersRights: true })
      : await querySnowflake(`SELECT CURRENT_USER() AS U`, { callersRights: true })
    return approverCheck(roomUser, APPROVER_ROLE, { sessionUser: r?.U ? String(r.U) : null, hasRole: r?.R === true })
  } catch (err) {
    console.warn(new Date().toISOString(), "[room] caller's-rights check failed", err)
    return approverCheck(roomUser, APPROVER_ROLE, { sessionUser: null })
  }
}
