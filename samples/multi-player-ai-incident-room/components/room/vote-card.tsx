"use client"

import { Rocket, ThumbsDown, ThumbsUp, Vote } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"

export interface PatchView {
  id: string
  taskSql: string
  cloneSchema: string
  shippedAt: string | null
  shippedBy: string | null
}

export function VoteCard(props: {
  patch: PatchView
  you: string
  required: number
  tally: { approvals: string[]; rejections: string[] }
  canVote: boolean
  shipCheck: { ok: boolean; reason?: string }
  frozen: boolean
  busy: boolean
  onVote: (v: "approve" | "reject") => void
  onShip: () => void
  /** Someone else just changed this card, e.g. "CLAIRE rejected". */
  flash?: string
}) {
  const { patch, you, required, tally } = props
  const mine = tally.approvals.includes(you) ? "approve" : tally.rejections.includes(you) ? "reject" : null
  const shipped = !!patch.shippedAt
  const approvedByYou = tally.approvals.includes(you)
  return (
    <Card className={`relative ${shipped ? "glow-emerald" : props.shipCheck.ok ? "glow-cyan" : tally.rejections.length ? "glow-red" : ""} ${props.flash ? "ring-4 ring-primary/60" : ""}`}>
      {props.flash && (
        <span className="absolute -top-2.5 right-3 z-10 rounded-full bg-primary px-2 py-0.5 text-[11px] font-semibold text-primary-foreground shadow">{props.flash}</span>
      )}
      <CardHeader className="flex flex-row items-center gap-2 space-y-0 pb-2">
        <Vote className="size-4 text-primary" aria-hidden />
        <CardTitle className="text-sm">{shipped ? "Shipped" : "Ship this fix to prod?"}</CardTitle>
        <span className="ml-auto font-mono text-xs text-muted-foreground">{patch.id}</span>
      </CardHeader>
      <CardContent className="space-y-3">
        <p className="text-xs text-muted-foreground">This exact SQL replaces the task body. It already passed on {patch.cloneSchema}.</p>
        <pre className="max-h-48 overflow-auto whitespace-pre-wrap rounded-md bg-muted p-2 font-mono text-xs">{patch.taskSql}</pre>

        <div className="space-y-1">
          <div className="flex items-center justify-between text-sm">
            <span>Approvals</span>
            <span className="tabular-nums">
              {tally.approvals.length} of {required}
            </span>
          </div>
          <div className="flex gap-1" aria-hidden>
            {Array.from({ length: required }, (_, i) => (
              <span key={i} className={`h-1.5 flex-1 rounded-full ${i < tally.approvals.length ? "bg-success" : "bg-muted"}`} />
            ))}
          </div>
          {!!tally.approvals.length && <p className="text-xs text-muted-foreground">Approved by {tally.approvals.join(", ")}</p>}
          {!!tally.rejections.length && <p className="text-xs text-destructive">Rejected by {tally.rejections.join(", ")}</p>}
        </div>

        {shipped ? (
          <p className="text-sm text-success">
            Shipped by {patch.shippedBy}. The task was updated and a run started.
          </p>
        ) : (
          <>
            <div className="grid grid-cols-2 gap-2">
              <Button variant={mine === "approve" ? "default" : "outline"} disabled={!props.canVote || props.busy} onClick={() => props.onVote("approve")}>
                <ThumbsUp className="size-4" aria-hidden /> Approve
              </Button>
              <Button variant={mine === "reject" ? "destructive" : "outline"} disabled={!props.canVote || props.busy} onClick={() => props.onVote("reject")}>
                <ThumbsDown className="size-4" aria-hidden /> Reject
              </Button>
            </div>
            <Button className="w-full" disabled={!props.shipCheck.ok || !approvedByYou || props.busy} onClick={props.onShip}>
              <Rocket className="size-4" aria-hidden /> Ship to prod
            </Button>
            <p className="text-center text-xs text-muted-foreground">
              {props.frozen
                ? "Line stopped. Clear the andon to vote or ship."
                : !props.shipCheck.ok
                  ? props.shipCheck.reason
                  : approvedByYou
                    ? "Quorum reached. You can ship."
                    : "Quorum reached. Someone who approved can ship."}
            </p>
            <p className="text-center text-[11px] text-muted-foreground">Agents can't vote. Prod changes run only through SHIP_FIX, which re-checks these votes.</p>
          </>
        )}
      </CardContent>
    </Card>
  )
}
