"use client"

import { copyText } from "@/lib/clipboard"
import { Ban, BookMarked, Check, UserPlus, Loader2, OctagonX, Play, Send, Square, Wrench, X } from "lucide-react"
import Link from "next/link"
import { useCallback, useEffect, useMemo, useReducer, useRef, useState } from "react"
import { EmptyState } from "@/components/empty-state"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { EXAMPLE_PROMPTS } from "@/lib/constants"
import { AGENTS, type AgentName, type Status } from "@/lib/domain/incident"
import { flashFor, lineFor, typingNow, type Flash } from "@/lib/domain/line"
import { initialView, reduce, type Item, type View } from "@/lib/domain/room-view"
import type { RoomEvent } from "@/lib/room/bus"
import { AGENT_META } from "./agents"
import { EvidenceBoard } from "./evidence-board"
import { LineStrip } from "./line-strip"
import { Markdown } from "./markdown"
import { StatusBadge } from "./status-badge"
import { VoteCard, type PatchView } from "./vote-card"

interface Snapshot {
  incident: { id: string; title: string; status: Status; frozen: boolean; frozenBy?: string; frozenReason?: string; requiredApprovals: number }
  events: RoomEvent[]
  patch: (PatchView & { testedOk: boolean }) | null
  tally: { approvals: string[]; rejections: string[] }
  shipCheck: { ok: boolean; reason?: string }
  running: AgentName[]
  postmortem: string | null
  you: string
}

type Action = { type: "reset"; view: View } | { type: "event"; e: RoomEvent }
const viewReducer = (v: View, a: Action) => (a.type === "reset" ? a.view : reduce(v, a.e))

export function RoomClient({ incidentId, dev }: { incidentId: string; dev: boolean }) {
  const [snap, setSnap] = useState<Snapshot | null>(null)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [view, dispatch] = useReducer(viewReducer, undefined, initialView)
  const [notice, setNotice] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [flash, setFlash] = useState<Flash | null>(null)

  const refresh = useCallback(async () => {
    const r = await fetch(`/api/incidents/${incidentId}`)
    const b = await r.json()
    if (!r.ok) throw new Error(b.error ?? `Couldn't load the room (${r.status})`)
    return b as Snapshot
  }, [incidentId])

  // Load history, then follow live events. Re-fetch the snapshot (patch, votes) when they change.
  useEffect(() => {
    let alive = true
    // Connect first and buffer, so nothing published while the snapshot loads is lost.
    // The reducer skips events it has already seen, so overlap with history is harmless.
    let ready = false
    let you = ""
    const buffered: RoomEvent[] = []
    const es = new EventSource(`/api/incidents/${incidentId}/stream`)
    const apply = (e: RoomEvent) => {
      dispatch({ type: "event", e })
      const f = flashFor(e, you)
      if (f) setFlash(f)
      if (["status", "vote", "ship", "andon", "andon.clear", "closed", "evidence", "agent.say"].includes(e.kind)) {
        refresh().then((s2) => alive && setSnap(s2)).catch(() => {})
      }
    }
    es.onmessage = (m) => {
      const e = JSON.parse(m.data) as RoomEvent
      if (ready) apply(e)
      else buffered.push(e)
    }
    refresh()
      .then((s) => {
        if (!alive) return
        you = s.you
        setSnap(s)
        const v = s.events.reduce(reduce, {
          ...initialView(),
          status: s.incident.status,
          frozen: s.incident.frozen ? { by: s.incident.frozenBy ?? "", reason: s.incident.frozenReason } : null,
          tally: s.tally,
        })
        dispatch({ type: "reset", view: { ...v, working: s.running } })
        // Replay what arrived during the load, without flashing it as news.
        for (const e of buffered.splice(0)) dispatch({ type: "event", e })
        ready = true
      })
      .catch((e) => alive && setLoadError((e as Error).message))
    return () => {
      alive = false
      es.close()
    }
  }, [incidentId, refresh])

  const act = useCallback(
    async (body: Record<string, unknown>) => {
      setBusy(true)
      setNotice(null)
      try {
        const r = await fetch(`/api/incidents/${incidentId}/action`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(body),
        })
        const b = await r.json().catch(() => ({}))
        if (!r.ok) setNotice(b.error ?? `That didn't work (${r.status})`)
        return r.ok
      } finally {
        setBusy(false)
      }
    },
    [incidentId],
  )

  // Typing pings skip `act`: they're fire-and-forget and must not flash the busy state.
  const lastPing = useRef(0)
  const ping = useCallback(() => {
    const now = Date.now()
    if (now - lastPing.current < 1500) return
    lastPing.current = now
    void fetch(`/api/incidents/${incidentId}/action`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ type: "typing", on: true }),
    }).catch(() => {})
  }, [incidentId])

  // A flash points at what someone else just changed; it fades after a moment.
  useEffect(() => {
    if (!flash) return
    const t = setTimeout(() => setFlash(null), 2500)
    return () => clearTimeout(t)
  }, [flash])

  // Typing signals expire on the clock, so re-render once a second while anyone is typing.
  const [now, setNow] = useState(() => Date.now())
  const anyTyping = Object.keys(view.typing).length > 0
  useEffect(() => {
    if (!anyTyping) return
    const t = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(t)
  }, [anyTyping])

  if (loadError) return <EmptyState variant="error" title="Couldn't open this room" description={loadError} className="m-8" />
  if (!snap) return <Skeleton className="m-8 h-[60vh]" />

  const status = view.status ?? snap.incident.status
  const frozen = view.frozen
  const terminal = status === "SHIPPED" || status === "CLOSED"
  const canVote = !frozen && status === "VOTING" && !!snap.patch?.testedOk

  return (
    <div className="mx-auto flex w-full max-w-7xl flex-col gap-4 px-4 py-4 sm:px-6 lg:px-8">
      <RoomHeader
        id={snap.incident.id}
        title={snap.incident.title}
        status={status}
        frozen={frozen}
        working={view.working}
        present={view.present}
        you={snap.you}
        dev={dev}
        busy={busy}
        terminal={status === "CLOSED"}
        shipped={status === "SHIPPED"}
        onAndon={() => {
          const reason = window.prompt("Pull the andon cord: stop every agent and block shipping. Why? (optional)") ?? undefined
          if (reason !== undefined) void act({ type: "andon", reason })
        }}
        onClear={() => act({ type: "andon.clear" })}
        onClose={() => window.confirm("Close the incident and drop its clone?") && act({ type: "close" })}
      />

      <LineStrip state={lineFor(status, !!frozen)} flash={flash?.target === "line" ? flash.text : undefined} />

      {notice && (
        <div role="alert" className="flex items-center gap-2 rounded-md border border-destructive/40 bg-destructive/10 px-3 py-2 text-sm text-destructive">
          <Ban className="size-4" aria-hidden /> {notice}
          <button className="ml-auto" onClick={() => setNotice(null)} aria-label="Dismiss">
            <X className="size-4" />
          </button>
        </div>
      )}

      <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_380px]">
        <section className="flex min-h-[70vh] flex-col rounded-xl border bg-card">
          <Timeline items={view.items} you={snap.you} />
          <TypingRow names={typingNow(view.typing, now, snap.you)} />
          <Composer
            onTyping={ping}
            disabled={status === "CLOSED"}
            frozen={!!frozen}
            onSend={(text) => {
              // `/file <decision>` records a decision instead of chatting.
              const m = text.match(/^\/file\b\s*([\s\S]*)$/i)
              return m ? act({ type: "file", text: m[1] }) : act({ type: "message", text })
            }}
          />
        </section>

        <aside className="space-y-3">
          {snap.patch && (
            <VoteCard
              patch={snap.patch}
              you={snap.you}
              required={snap.incident.requiredApprovals}
              tally={view.tally.patchId === snap.patch.id ? view.tally : snap.tally}
              canVote={canVote}
              shipCheck={snap.shipCheck}
              frozen={!!frozen}
              busy={busy}
              onVote={(vote) => act({ type: "vote", patchId: snap.patch!.id, vote })}
              onShip={() => act({ type: "ship", patchId: snap.patch!.id })}
              flash={flash?.target === "vote" ? flash.text : undefined}
            />
          )}
          <EvidenceBoard board={view.board} />
          {snap.postmortem && (
            <div className="rounded-xl border bg-card p-4">
              <p className="mb-2 text-sm font-semibold">Postmortem</p>
              <Markdown>{snap.postmortem}</Markdown>
            </div>
          )}
          {terminal && !snap.postmortem && <p className="text-xs text-muted-foreground">@scribe files the postmortem after a ship.</p>}
        </aside>
      </div>
    </div>
  )
}

function RoomHeader(p: {
  id: string
  title: string
  status: Status
  frozen: { by: string; reason?: string } | null
  working: AgentName[]
  present: string[]
  you: string
  dev: boolean
  busy: boolean
  terminal: boolean
  shipped: boolean
  onAndon: () => void
  onClear: () => void
  onClose: () => void
}) {
  return (
    <header className={`glass rounded-2xl p-5 ${p.frozen ? "glow-red" : ""}`}>
      <div className="flex flex-wrap items-center gap-3">
        <Link href="/" className="font-mono text-xs text-muted-foreground hover:underline">
          {p.id}
        </Link>
        <StatusBadge status={p.status} frozen={!!p.frozen} />
        <h1 className="min-w-0 flex-1 truncate text-2xl font-extrabold tracking-tight text-white">{p.title}</h1>
        {!p.terminal &&
          !p.shipped &&
          (p.frozen ? (
            <Button variant="outline" onClick={p.onClear} disabled={p.busy}>
              <Play className="size-4" aria-hidden /> Clear andon
            </Button>
          ) : (
            <Button variant="destructive" onClick={p.onAndon} disabled={p.busy} title="Stop every agent and block shipping">
              <OctagonX className="size-4" aria-hidden /> Andon
            </Button>
          ))}
        <InviteButton />
        {!p.terminal && (
          <Button variant="ghost" size="sm" onClick={p.onClose} disabled={p.busy}>
            Close
          </Button>
        )}
      </div>
      {p.frozen && (
        <p className="mt-2 text-sm text-destructive">
          Line stopped by {p.frozen.by}
          {p.frozen.reason ? `: "${p.frozen.reason}"` : ""}. Agents won't run and nothing can ship until someone clears it.
        </p>
      )}
      <div className="mt-3 flex flex-wrap items-center gap-2 text-xs">
        <span className="text-muted-foreground">In the room:</span>
        {p.present.map((u) => (
          <Badge key={u} variant={u === p.you ? "default" : "secondary"} className="gap-1.5 font-normal">
            <span className="live-dot size-1.5 rounded-full bg-success" aria-hidden />
            {u}
            {u === p.you ? " (you)" : ""}
          </Badge>
        ))}
        {AGENTS.map((a) => {
          const on = p.working.includes(a)
          return (
            <Badge key={a} variant="outline" className={`gap-1.5 font-normal ${on ? `ring-2 ${AGENT_META[a].ring}` : ""}`} title={AGENT_META[a].role}>
              <span className={`size-2 rounded-full ${AGENT_META[a].dot} ${on ? "animate-pulse" : "opacity-60"}`} aria-hidden />
              {AGENT_META[a].label}
              {on && <Loader2 className="size-3 animate-spin" aria-label="working" />}
            </Badge>
          )
        })}
        {p.dev && <DevIdentity you={p.you} />}
      </div>
    </header>
  )
}

/** Copy the room link so a teammate lands in the same room with their own login. */
function InviteButton() {
  const [copied, setCopied] = useState<boolean | null>(null)
  const copy = async () => {
    setCopied(await copyText(window.location.href))
    setTimeout(() => setCopied(null), 2000)
  }
  return (
    <Button variant="outline" size="sm" onClick={copy} title="Copy this room's link for a teammate">
      {copied ? <Check className="size-4" aria-hidden /> : <UserPlus className="size-4" aria-hidden />}
      {copied === null ? "Invite" : copied ? "Link copied" : "Copy failed"}
    </Button>
  )
}

function DevIdentity({ you }: { you: string }) {
  const back = typeof window === "undefined" ? "/" : window.location.pathname
  return (
    <span className="ml-auto flex items-center gap-1 text-muted-foreground">
      dev: be
      {["DASH", "CLAIRE", "EVA"].map((n) => (
        <a key={n} href={`/api/dev/as?name=${n}&back=${encodeURIComponent(back)}`} className={`underline ${n === you ? "font-semibold text-foreground" : ""}`}>
          {n}
        </a>
      ))}
    </span>
  )
}

function Timeline({ items, you }: { items: Item[]; you: string }) {
  const end = useRef<HTMLDivElement>(null)
  const last = items.at(-1)
  const lastLen = last?.kind === "agent" ? last.text.length : 0
  useEffect(() => {
    end.current?.scrollIntoView({ block: "end" })
  }, [items.length, lastLen])
  return (
    <div className="flex-1 space-y-3 overflow-y-auto p-4" style={{ maxHeight: "70vh" }} aria-live="polite">
      {!items.length && <p className="text-sm text-muted-foreground">The room is quiet. @triage is on its way.</p>}
      {items.map((it) =>
        it.kind === "human" ? (
          <div key={it.id} className={`flex ${it.by === you ? "justify-end" : ""}`}>
            <div className={`chip-in max-w-[85%] rounded-2xl px-3 py-2 text-sm ${it.by === you ? "bg-primary text-primary-foreground" : "glass"}`}>
              <p className="mb-0.5 text-[11px] font-medium opacity-80">{it.by}</p>
              <p className="whitespace-pre-wrap">{it.text}</p>
            </div>
          </div>
        ) : it.kind === "agent" ? (
          <AgentBubble key={it.id} it={it} />
        ) : it.kind === "decision" ? (
          <DecisionCard key={it.id} it={it} />
        ) : (
          <p
            key={it.id}
            className={`text-center text-xs ${it.tone === "danger" ? "font-medium text-destructive" : it.tone === "success" ? "text-success" : "text-muted-foreground"}`}
          >
            {it.text}
          </p>
        ),
      )}
      <div ref={end} />
    </div>
  )
}

function AgentBubble({ it }: { it: Extract<Item, { kind: "agent" }> }) {
  const m = AGENT_META[it.agent]
  return (
    <div className={`chip-in glass min-w-0 max-w-[92%] rounded-2xl px-4 py-3 ${it.done ? "" : "glow-cyan"}`}>
      <p className="mb-1 flex items-center gap-1.5 text-[11px] font-medium">
        <span className={`size-2 rounded-full ${m.dot}`} aria-hidden />
        {m.label}
        <span className="font-normal text-muted-foreground">· {m.role}</span>
        {!it.done && <Loader2 className="size-3 animate-spin text-muted-foreground" aria-label="working" />}
        {it.stopped && (
          <Badge variant="destructive" className="ml-1 gap-1 px-1.5 py-0 text-[10px]">
            <Square className="size-2.5" aria-hidden /> stopped by andon
          </Badge>
        )}
      </p>
      {!!it.tools.length && (
        <div className="mb-2 flex flex-wrap gap-1">
          {it.tools.map((t, i) => (
            <Badge key={`${t.id ?? t.name}-${i}`} variant={t.status === "error" ? "destructive" : "outline"} className="gap-1 font-mono text-[10px] font-normal">
              {t.status === "running" ? <Loader2 className="size-2.5 animate-spin" aria-hidden /> : <Wrench className="size-2.5" aria-hidden />}
              {t.name}
            </Badge>
          ))}
        </div>
      )}
      {it.text ? <Markdown>{it.text}</Markdown> : !it.done && <p className="text-sm text-muted-foreground">Working…</p>}
      {it.error && <p className="mt-1 text-xs text-destructive">{it.error}</p>}
    </div>
  )
}

function DecisionCard({ it }: { it: Extract<Item, { kind: "decision" }> }) {
  return (
    <div className="chip-in glass glow-emerald mx-auto max-w-[85%] rounded-2xl px-4 py-3 text-sm">
      <p className="mb-0.5 flex items-center gap-1.5 text-[11px] font-medium text-muted-foreground">
        <BookMarked className="size-3" aria-hidden />
        {it.requestedBy && it.requestedBy !== it.by ? `Decision drafted by @${it.by.toLowerCase()} for ${it.requestedBy}` : `Decision filed by ${it.by}`}
        {it.participants.length > 1 && (
          <span className="font-normal">· in the room: {it.participants.filter((p) => p !== it.by && p !== it.requestedBy).join(", ") || "just them"}</span>
        )}
      </p>
      <p className="whitespace-pre-wrap">{it.text}</p>
    </div>
  )
}

function TypingRow({ names }: { names: string[] }) {
  return (
    <p className="flex h-6 items-center gap-1.5 px-4 text-xs text-muted-foreground" aria-live="polite">
      {!!names.length && (
        <>
          <span className="flex gap-0.5" aria-hidden>
            {[0, 1, 2].map((i) => (
              <span key={i} className="size-1.5 animate-bounce rounded-full bg-muted-foreground/70" style={{ animationDelay: `${i * 150}ms` }} />
            ))}
          </span>
          {names.join(" and ")} {names.length > 1 ? "are" : "is"} typing…
        </>
      )}
    </p>
  )
}

function Composer({
  disabled,
  frozen,
  onSend,
  onTyping,
}: {
  disabled: boolean
  frozen: boolean
  onSend: (t: string) => Promise<boolean>
  onTyping: () => void
}) {
  const [text, setText] = useState("")
  const [sending, setSending] = useState(false)
  const send = async (t = text) => {
    if (!t.trim() || sending) return
    setSending(true)
    const ok = await onSend(t)
    setSending(false)
    if (ok) setText("")
  }
  const chips = useMemo(() => AGENTS.map((a) => AGENT_META[a].label), [])
  return (
    <div className="border-t p-3">
      <div className="mb-2 flex flex-wrap gap-1.5">
        {chips.map((c) => (
          <button key={c} type="button" className="rounded-full border px-2 py-0.5 text-xs hover:bg-muted" onClick={() => setText((t) => (t.includes(c) ? t : `${c} ${t}`))}>
            {c}
          </button>
        ))}
        <button type="button" className="rounded-full border px-2 py-0.5 text-xs hover:bg-muted" onClick={() => setText((t) => `@all ${t}`)}>
          @all
        </button>
        <button type="button" className="rounded-full border px-2 py-0.5 text-xs hover:bg-muted" title="Record a decision. Send /file on its own and @scribe drafts it." onClick={() => setText((t) => (t.startsWith("/file") ? t : `/file ${t}`))}>
          /file
        </button>
        <span className="mx-1 self-center text-xs text-muted-foreground">or try:</span>
        {EXAMPLE_PROMPTS.map((p) => (
          <button key={p} type="button" disabled={disabled} className="rounded-full bg-muted px-2 py-0.5 text-xs hover:bg-muted/70" onClick={() => send(p)}>
            {p}
          </button>
        ))}
      </div>
      <form
        className="flex gap-2"
        onSubmit={(e) => {
          e.preventDefault()
          void send()
        }}
      >
        <textarea
          value={text}
          onChange={(e) => {
            setText(e.target.value)
            if (e.target.value.trim()) onTyping()
          }}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) {
              e.preventDefault()
              void send()
            }
          }}
          rows={2}
          disabled={disabled}
          placeholder={frozen ? "Line stopped: talk it through. Agents won't run until the andon is cleared." : "Talk to the team, @mention an agent, or /file a decision…"}
          className="min-h-[44px] flex-1 resize-none rounded-md border bg-background px-3 py-2 text-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
          aria-label="Message"
        />
        <Button type="submit" disabled={disabled || sending || !text.trim()} aria-label="Send">
          {sending ? <Loader2 className="size-4 animate-spin" /> : <Send className="size-4" />}
        </Button>
      </form>
    </div>
  )
}
