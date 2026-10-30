"use client"

import { useEffect, useState } from "react"
import { Button } from "@/components/ui/button"

export type LibpostalStatus = {
  installed?: boolean
  service?: string
  container?: string | null
  autoSuspendSecs?: number
  pool?: string | null
  instanceFamily?: string | null
  errors?: string[]
  error?: string
}

// Coarse state shown in the chip.
export function libpostalState(s: LibpostalStatus | null): "loading" | "absent" | "running" | "starting" | "suspended" | "error" {
  if (!s) return "loading"
  if (s.error) return "error"
  if (!s.installed) return "absent"
  if (s.service === "SUSPENDED" || s.service === "SUSPENDING") return "suspended"
  if (s.service === "RUNNING" && s.container === "READY") return "running"
  return "starting"
}

const CHIP: Record<string, { label: string; cls: string }> = {
  loading: { label: "checking", cls: "bg-muted text-muted-foreground" },
  absent: { label: "not installed", cls: "bg-muted text-muted-foreground" },
  running: { label: "running", cls: "bg-success/15 text-success" },
  starting: { label: "starting", cls: "bg-warning/15 text-warning" },
  suspended: { label: "suspended", cls: "bg-muted text-muted-foreground" },
  error: { label: "status unavailable", cls: "bg-destructive/15 text-destructive" },
}

/** Status chip plus Start / Stop for the libpostal SPCS service and pool. */
export function LibpostalControl({ onChange }: { onChange?: (s: LibpostalStatus) => void }) {
  const [status, setStatus] = useState<LibpostalStatus | null>(null)
  const [busy, setBusy] = useState(false)
  const [confirmStop, setConfirmStop] = useState(false)
  const state = libpostalState(status)

  async function refresh() {
    const s = (await fetch("/api/libpostal").then((r) => r.json()).catch((e) => ({ error: String(e) }))) as LibpostalStatus
    setStatus(s)
    onChange?.(s)
  }

  async function act(action: "start" | "stop") {
    setBusy(true)
    setConfirmStop(false)
    try {
      const s = (await fetch("/api/libpostal", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ action }),
      }).then((r) => r.json())) as LibpostalStatus
      setStatus(s)
      onChange?.(s)
    } finally {
      setBusy(false)
    }
  }

  useEffect(() => {
    refresh()
  }, [])

  // Poll while it is coming up or going down.
  useEffect(() => {
    if (state !== "starting" && !(state === "suspended" && status?.pool && status.pool !== "SUSPENDED")) return
    const t = setInterval(refresh, 5000)
    return () => clearInterval(t)
  }, [state, status?.pool])

  const chip = CHIP[state]
  const idleMin = status?.autoSuspendSecs ? Math.round(status.autoSuspendSecs / 60) : null

  return (
    <div className="flex flex-wrap items-center gap-2 rounded-lg border px-3 py-2 text-sm">
      <span className="font-medium">libpostal</span>
      <span className={`rounded-full px-2 py-0.5 text-xs font-medium ${chip.cls}`}>{chip.label}</span>
      {status?.installed && (
        <span className="text-xs text-muted-foreground">
          {status.instanceFamily} pool {status.pool?.toLowerCase()}
          {idleMin ? `, auto-suspends after ${idleMin} min idle` : ", no idle timer"}
        </span>
      )}
      {status?.installed && (
        <span className="ml-auto flex gap-2">
          {state === "suspended" && (
            <Button size="sm" disabled={busy} onClick={() => act("start")}>
              {busy ? "Starting..." : "Start"}
            </Button>
          )}
          {(state === "running" || state === "starting") &&
            (confirmStop ? (
              <>
                <Button size="sm" variant="destructive" disabled={busy} onClick={() => act("stop")}>
                  Stop service and pool
                </Button>
                <Button size="sm" variant="outline" onClick={() => setConfirmStop(false)}>
                  Cancel
                </Button>
              </>
            ) : (
              <Button size="sm" variant="outline" disabled={busy} onClick={() => setConfirmStop(true)}>
                Stop
              </Button>
            ))}
        </span>
      )}
      {status?.errors && status.errors.length > 0 && (
        <span className="w-full text-xs text-destructive">{status.errors.join("; ")}</span>
      )}
    </div>
  )
}
