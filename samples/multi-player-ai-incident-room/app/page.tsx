"use client"

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { AlertTriangle, Flame, Radio } from "lucide-react"
import Link from "next/link"
import { useRouter } from "next/navigation"
import { useState } from "react"
import { EmptyState } from "@/components/empty-state"
import { PageHeader } from "@/components/page-header"
import { LineStrip } from "@/components/room/line-strip"
import { StatusBadge } from "@/components/room/status-badge"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Skeleton } from "@/components/ui/skeleton"
import { formatDateTime } from "@/lib/format"
import type { Status } from "@/lib/domain/incident"
import { lineFromLatestRun } from "@/lib/domain/line"

type Incident = { id: string; title: string; status: Status; frozen: boolean; openedBy: string; openedAt: string }
type Failure = { task: string; scheduled_time: string; error_message: string | null }

async function get<T>(url: string): Promise<T> {
  const r = await fetch(url)
  const b = await r.json()
  if (!r.ok) throw new Error(b.error ?? `Request failed (${r.status})`)
  return b as T
}

export default function HomePage() {
  const router = useRouter()
  const qc = useQueryClient()
  const [error, setError] = useState<string | null>(null)
  const incidents = useQuery({ queryKey: ["incidents"], queryFn: () => get<{ incidents: Incident[] }>("/api/incidents") })
  const failures = useQuery({ queryKey: ["failures"], queryFn: () => get<{ failures: Failure[]; latest: string | null }>("/api/failures"), refetchInterval: 5_000 })

  const open = useMutation({
    mutationFn: async (f: Failure) => {
      const r = await fetch("/api/incidents", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ title: `${f.task.split(".").pop()} failed`, objectFqn: f.task, errorMessage: f.error_message }),
      })
      const b = await r.json()
      if (!r.ok) throw new Error(b.error ?? "Couldn't open the room")
      return b.id as string
    },
    onSuccess: (id) => {
      qc.invalidateQueries({ queryKey: ["incidents"] })
      router.push(`/room/${id}`)
    },
    onError: (e) => setError((e as Error).message),
  })

  const live = (incidents.data?.incidents ?? []).filter((i) => i.status !== "CLOSED")
  const done = (incidents.data?.incidents ?? []).filter((i) => i.status === "CLOSED")
  // Only offer a room while the line is actually down; older failures are history.
  const latestFailure = failures.data?.latest === "FAILED" ? failures.data.failures[0] : undefined

  return (
    <main className="mx-auto w-full max-w-7xl space-y-6 px-4 py-6 sm:px-6 sm:py-10 lg:px-8">
      <PageHeader
        eyebrow="Multi-player incident room"
        title="Your whole team and three agents, in one live room."
        description="Everyone on the incident sees every message, tool call and vote as it happens. @triage finds what broke, @fixer proves a fix on a zero-copy clone, @scribe keeps the record. Agents do the legwork; people decide: two must approve before anything ships, and anyone can pull the andon to stop the line."
      />

      {error && <p className="text-sm text-destructive">{error}</p>}

      {!failures.isPending && !failures.isError && (
        <LineStrip state={lineFromLatestRun(failures.data?.latest ?? undefined)} />
      )}

      <Card className={latestFailure ? "border-destructive/40" : undefined}>
        <CardHeader className="flex flex-row items-center gap-2 space-y-0">
          <Flame className="size-4 text-destructive" aria-hidden />
          <CardTitle>Failing on the line</CardTitle>
          <Badge variant="outline" className="ml-auto gap-1 font-normal">
            <Radio className="size-3" aria-hidden /> checks every 5s
          </Badge>
        </CardHeader>
        <CardContent className="space-y-3">
          {failures.isPending ? (
            <Skeleton className="h-16 w-full" />
          ) : failures.isError ? (
            <p className="text-sm text-destructive">{(failures.error as Error).message}</p>
          ) : !latestFailure ? (
            <p className="text-sm text-muted-foreground">
              The line is running. Run <code className="font-mono">demo/break.sql</code> to break the demo pipeline.
            </p>
          ) : (
            <div className="flex flex-col gap-3 sm:flex-row sm:items-center">
              <div className="min-w-0 flex-1">
                <p className="font-mono text-sm">{latestFailure.task}</p>
                <p className="truncate text-sm text-muted-foreground" title={latestFailure.error_message ?? ""}>
                  {formatDateTime(latestFailure.scheduled_time)} · {latestFailure.error_message}
                </p>
              </div>
              <Button onClick={() => open.mutate(latestFailure)} disabled={open.isPending}>
                <AlertTriangle className="size-4" aria-hidden />
                {open.isPending ? "Opening…" : "Open a room"}
              </Button>
            </div>
          )}
        </CardContent>
      </Card>

      <section className="space-y-3">
        <h2 className="eyebrow text-[11px]">Open incidents</h2>
        {incidents.isPending ? (
          <Skeleton className="h-24 w-full" />
        ) : incidents.isError ? (
          <EmptyState variant="error" title="Couldn't load incidents" description={(incidents.error as Error).message} />
        ) : !live.length ? (
          <EmptyState title="No open incidents" description="Open a room from a failure above." />
        ) : (
          <div className="grid gap-3 sm:grid-cols-2">
            {live.map((i) => (
              <IncidentCard key={i.id} i={i} />
            ))}
          </div>
        )}
      </section>

      {!!done.length && (
        <section className="space-y-3">
          <h2 className="eyebrow text-[11px] opacity-70">Closed</h2>
          <div className="grid gap-3 sm:grid-cols-2">
            {done.slice(0, 6).map((i) => (
              <IncidentCard key={i.id} i={i} />
            ))}
          </div>
        </section>
      )}
    </main>
  )
}

function IncidentCard({ i }: { i: Incident }) {
  return (
    <Link href={`/room/${i.id}`} className="block rounded-xl focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring">
      <Card className="transition-colors hover:border-primary/50">
        <CardContent className="space-y-2 p-4">
          <div className="flex items-center gap-2">
            <span className="font-mono text-xs text-muted-foreground">{i.id}</span>
            <StatusBadge status={i.status} frozen={i.frozen} className="ml-auto" />
          </div>
          <p className="font-medium">{i.title}</p>
          <p className="text-xs text-muted-foreground">
            Opened by {i.openedBy} · {formatDateTime(i.openedAt)}
          </p>
        </CardContent>
      </Card>
    </Link>
  )
}
