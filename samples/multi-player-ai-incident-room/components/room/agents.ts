import type { AgentName } from "@/lib/domain/incident"

export const AGENT_META: Record<AgentName, { label: string; role: string; dot: string; ring: string }> = {
  TRIAGE: { label: "@triage", role: "Finds what broke", dot: "bg-amber-500", ring: "ring-amber-500/40" },
  FIXER: { label: "@fixer", role: "Proves a fix on a clone", dot: "bg-emerald-500", ring: "ring-emerald-500/40" },
  SCRIBE: { label: "@scribe", role: "Keeps the record", dot: "bg-violet-500", ring: "ring-violet-500/40" },
}
