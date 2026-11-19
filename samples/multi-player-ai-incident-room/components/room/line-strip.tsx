"use client"

import { Check, ChevronRight, OctagonX, ShieldCheck, Wrench } from "lucide-react"
import { BROKEN_STATION, LINE_COPY, STATIONS, type LineState } from "@/lib/domain/line"
import { cn } from "@/lib/utils"

// What the broken station looks like in each line state. Healthy stations stay green.
const BROKEN: Record<LineState, { box: string; icon: typeof Check; tag: string }> = {
  stopped: { box: "border-destructive bg-destructive/10 text-destructive animate-pulse", icon: OctagonX, tag: "down" },
  fixing: { box: "border-warning bg-warning/10 text-warning", icon: Wrench, tag: "fixing on a clone" },
  gated: { box: "border-primary bg-primary/10 text-primary", icon: ShieldCheck, tag: "waiting on 2 approvals" },
  running: { box: "border-success bg-success/10 text-success", icon: Check, tag: "fixed" },
}

// The whole strip glows with the line's state.
const BANNER: Record<LineState, string> = {
  stopped: "glow-red",
  fixing: "glow-amber",
  gated: "glow-cyan",
  running: "glow-emerald",
}

/** The pipeline as a factory line. `compact` drops the copy for tight headers. */
export function LineStrip({ state, compact, flash, className }: { state: LineState; compact?: boolean; flash?: string; className?: string }) {
  const copy = LINE_COPY[state]
  return (
    <div
      className={cn("glass relative flex flex-col gap-3 rounded-2xl p-4 sm:flex-row sm:items-center", BANNER[state], flash && "ring-4 ring-primary/60", className)}
      role="status"
      aria-label={`${copy.title}. ${copy.detail}`}
    >
      {!compact && (
        <div className="min-w-0 sm:w-56">
          <p className="eyebrow text-[11px]">Pipeline</p>
          <p className="mt-1 text-lg font-extrabold tracking-tight text-white">{copy.title}</p>
          <p className="text-xs text-muted-foreground">{copy.detail}</p>
        </div>
      )}
      {flash && (
        <span className="absolute -top-2.5 right-3 rounded-full bg-primary px-2 py-0.5 text-[11px] font-semibold text-primary-foreground shadow">{flash}</span>
      )}
      <ol className="flex flex-1 items-center gap-1.5">
        {STATIONS.map((s, i) => {
          const broken = s.key === BROKEN_STATION
          const look = broken ? BROKEN[state] : null
          const Icon = look?.icon ?? Check
          const flowing = state === "running" || i < STATIONS.findIndex((x) => x.key === BROKEN_STATION)
          return (
            <li key={s.key} className="flex flex-1 items-center gap-1.5">
              <div
                className={cn(
                  "flex min-w-0 flex-1 items-center gap-2 rounded-xl border px-3 py-2 transition-colors duration-500",
                  look?.box ?? (state === "stopped" && i > 0 ? "border-border bg-muted/40 text-muted-foreground" : "border-success/50 bg-success/5 text-success"),
                )}
              >
                <Icon className="size-4 shrink-0" aria-hidden />
                <div className="min-w-0">
                  <p className="text-xs font-semibold leading-tight">{s.label}</p>
                  <p className="truncate font-mono text-[10px] leading-tight opacity-80">{broken ? look!.tag : s.object}</p>
                </div>
              </div>
              {i < STATIONS.length - 1 && (
                <ChevronRight className={cn("size-4 shrink-0", flowing ? "text-success" : "text-muted-foreground/50")} aria-hidden />
              )}
            </li>
          )
        })}
      </ol>
    </div>
  )
}
