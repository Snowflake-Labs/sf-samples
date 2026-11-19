import { Badge } from "@/components/ui/badge"
import type { Status } from "@/lib/domain/incident"

const LABEL: Record<Status, string> = {
  TRIAGE: "Triage",
  FIXING: "Fixing",
  VOTING: "Up for a vote",
  SHIPPED: "Shipped",
  CLOSED: "Closed",
}
const VARIANT: Record<Status, "warning" | "default" | "success" | "secondary" | "outline"> = {
  TRIAGE: "warning",
  FIXING: "warning",
  VOTING: "default",
  SHIPPED: "success",
  CLOSED: "secondary",
}

export function StatusBadge({ status, frozen, className }: { status: Status; frozen?: boolean; className?: string }) {
  if (frozen) {
    return (
      <Badge variant="destructive" className={className}>
        Line stopped
      </Badge>
    )
  }
  return (
    <Badge variant={VARIANT[status]} className={className}>
      {LABEL[status]}
    </Badge>
  )
}
