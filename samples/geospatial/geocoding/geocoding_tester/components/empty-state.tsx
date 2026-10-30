import type React from "react"
import { AlertTriangle, Inbox, type LucideIcon } from "lucide-react"
import { cn } from "@/lib/utils"

/**
 * EmptyState — shown when a query returns no rows (or fails, with
 * `variant="error"`). Use inside a Card in place of a table or chart.
 *   {rows.length === 0 && <EmptyState title="No orders yet" description="Try a wider date range." />}
 */
export function EmptyState({
  title,
  description,
  icon,
  action,
  variant = "empty",
  className,
}: {
  title: React.ReactNode
  description?: React.ReactNode
  icon?: LucideIcon
  action?: React.ReactNode
  variant?: "empty" | "error"
  className?: string
}) {
  const Icon = icon ?? (variant === "error" ? AlertTriangle : Inbox)
  return (
    <div
      role={variant === "error" ? "alert" : undefined}
      className={cn(
        "flex flex-col items-center justify-center gap-3 rounded-lg border border-dashed px-6 py-10 text-center",
        variant === "error" && "border-destructive/40 bg-destructive/5",
        className
      )}
    >
      <span
        className={cn(
          "flex h-10 w-10 items-center justify-center rounded-full bg-muted text-muted-foreground",
          variant === "error" && "bg-destructive/10 text-destructive"
        )}
      >
        <Icon className="h-5 w-5" />
      </span>
      <div className="space-y-1">
        <p className="text-sm font-medium">{title}</p>
        {description && (
          <p className="mx-auto max-w-sm break-words text-sm text-muted-foreground">{description}</p>
        )}
      </div>
      {action}
    </div>
  )
}
