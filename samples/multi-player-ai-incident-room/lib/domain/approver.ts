// Who may vote or ship, judged from the voter's own Snowflake session (caller's rights). Pure.

import type { Check } from "./incident"

export interface CallerFacts {
  /** CURRENT_USER() in the voter's own session, or null if caller's rights didn't run. */
  sessionUser: string | null
  /** IS_ROLE_IN_SESSION(requiredRole) in that session, when a role is required. */
  hasRole?: boolean
}

/**
 * The voter must be the person the ingress says they are and, when IR_APPROVER_ROLE is
 * set, hold that role. With no required role, a session we couldn't open isn't a reason
 * to block; with one, it is.
 */
export function approverCheck(roomUser: string, requiredRole: string | undefined, f: CallerFacts): Check {
  if (f.sessionUser === null) {
    return requiredRole ? { ok: false, reason: `Couldn't confirm you hold ${requiredRole}` } : { ok: true }
  }
  if (f.sessionUser.toUpperCase() !== roomUser.toUpperCase()) {
    return { ok: false, reason: "Your Snowflake session doesn't match your room identity" }
  }
  if (requiredRole && !f.hasRole) return { ok: false, reason: `Only holders of ${requiredRole} can approve or ship` }
  return { ok: true }
}
