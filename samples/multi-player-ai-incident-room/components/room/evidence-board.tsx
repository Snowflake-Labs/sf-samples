"use client"

import { CheckCircle2, GitBranch, ShieldCheck, TriangleAlert, Workflow, XCircle } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import type { Evidence } from "@/lib/domain/evidence"
import { formatDateTime } from "@/lib/format"

const usd = (n: number) => n.toLocaleString(undefined, { style: "currency", currency: "USD", maximumFractionDigits: 0 })
const short = (fqn: string) => fqn.split(".").pop() ?? fqn

export function EvidenceBoard({ board }: { board: Evidence[] }) {
  if (!board.length) {
    return (
      <Card>
        <CardContent className="p-4 text-sm text-muted-foreground">Evidence the agents find gets pinned here.</CardContent>
      </Card>
    )
  }
  const order = { failure: 0, schema: 1, blast: 2, patch: 3 } as const
  return (
    <div className="space-y-3">
      {[...board].sort((a, b) => order[a.kind] - order[b.kind]).map((e) => (
        <EvidenceCard key={e.kind === "patch" ? e.patchId : e.kind} e={e} />
      ))}
    </div>
  )
}

function EvidenceCard({ e }: { e: Evidence }) {
  switch (e.kind) {
    case "failure":
      return (
        <Card className="border-destructive/40">
          <CardHeader className="flex flex-row items-center gap-2 space-y-0 pb-2">
            <TriangleAlert className="size-4 text-destructive" aria-hidden />
            <CardTitle className="text-sm">What failed</CardTitle>
          </CardHeader>
          <CardContent className="space-y-1 text-sm">
            <p className="font-mono">{short(e.task)}</p>
            <p className="text-xs text-muted-foreground">{formatDateTime(e.at)}</p>
            <p className="rounded-md bg-destructive/10 p-2 font-mono text-xs text-destructive">{e.error}</p>
          </CardContent>
        </Card>
      )
    case "schema":
      return (
        <Card>
          <CardHeader className="flex flex-row items-center gap-2 space-y-0 pb-2">
            <Workflow className="size-4 text-muted-foreground" aria-hidden />
            <CardTitle className="text-sm">The pipeline right now</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2 text-xs">
            <pre className="overflow-x-auto whitespace-pre-wrap rounded-md bg-muted p-2 font-mono">{e.taskSql}</pre>
            {Object.entries(e.columns)
              .filter(([t]) => !t.endsWith("_V"))
              .map(([t, cols]) => (
                <p key={t}>
                  <span className="font-mono font-medium">{t}</span>
                  <span className="text-muted-foreground"> · {cols.map((c) => c.split(" ")[0]).join(", ")}</span>
                </p>
              ))}
          </CardContent>
        </Card>
      )
    case "blast":
      return (
        <Card>
          <CardHeader className="flex flex-row items-center gap-2 space-y-0 pb-2">
            <GitBranch className="size-4 text-warning" aria-hidden />
            <CardTitle className="text-sm">Blast radius</CardTitle>
            <Badge variant="outline" className="ml-auto font-normal">
              from lineage
            </Badge>
          </CardHeader>
          <CardContent className="space-y-1 text-sm">
            {e.objects.length ? (
              e.objects.map((o) => (
                <p key={o.object} className="flex items-center gap-2">
                  <span className="font-mono">{short(o.object)}</span>
                  <span className="text-xs text-muted-foreground">
                    {o.hops} hop{o.hops === 1 ? "" : "s"} downstream
                  </span>
                </p>
              ))
            ) : (
              <p className="text-muted-foreground">Nothing downstream.</p>
            )}
          </CardContent>
        </Card>
      )
    case "patch":
      return (
        <Card className={e.ok ? "border-success/50" : "border-destructive/40"}>
          <CardHeader className="flex flex-row items-center gap-2 space-y-0 pb-2">
            {e.ok ? <ShieldCheck className="size-4 text-success" aria-hidden /> : <XCircle className="size-4 text-destructive" aria-hidden />}
            <CardTitle className="text-sm">Clone test · {e.patchId}</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2 text-sm">
            <p className="font-mono text-xs text-muted-foreground">{e.cloneSchema}</p>
            {e.error ? (
              <p className="text-destructive">{e.error}</p>
            ) : (
              <>
                {e.clone && e.prod && (
                  <div className="grid grid-cols-3 gap-1 text-xs">
                    <span />
                    <span className="font-medium">Clone</span>
                    <span className="font-medium">Prod last good</span>
                    <span className="text-muted-foreground">Rows</span>
                    <span className="tabular-nums">{e.clone.rows}</span>
                    <span className="tabular-nums">{e.prod.rows}</span>
                    <span className="text-muted-foreground">Revenue</span>
                    <span className="tabular-nums">{usd(e.clone.revenue)}</span>
                    <span className="tabular-nums">{usd(e.prod.revenue)}</span>
                  </div>
                )}
                <p className={`flex items-center gap-1.5 ${e.ok ? "text-success" : "text-destructive"}`}>
                  {e.ok ? <CheckCircle2 className="size-4" aria-hidden /> : <XCircle className="size-4" aria-hidden />}
                  {e.verdict}
                </p>
              </>
            )}
          </CardContent>
        </Card>
      )
  }
}
