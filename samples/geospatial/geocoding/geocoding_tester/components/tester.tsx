"use client"

import dynamic from "next/dynamic"
import { useState } from "react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { EmptyState } from "@/components/empty-state"
import { copyText } from "@/lib/clipboard"
import { LibpostalControl, libpostalState, type LibpostalStatus } from "@/components/libpostal-control"
import type { MapPoint } from "@/components/result-map"

// deck.gl and MapLibre need the browser.
const ResultMap = dynamic(() => import("@/components/result-map").then((m) => m.ResultMap), { ssr: false })

type Tab = "forward" | "reverse" | "normalize"
type Row = Record<string, unknown>
type Resp = { rows?: Row[]; result?: Row; sql?: string[]; status?: string; ms?: number; error?: string }
type Exchange = { path: string; body: unknown; response: unknown }

// Forward geocoding coverage: US plus the 14 countries of the EU index.
// Reverse geocoding works anywhere ("" = any country).
const COUNTRIES: [string, string][] = [
  ["US", "United States"], ["AT", "Austria"], ["BE", "Belgium"], ["CH", "Switzerland"], ["CZ", "Czechia"],
  ["DE", "Germany"], ["DK", "Denmark"], ["ES", "Spain"], ["FI", "Finland"], ["FR", "France"],
  ["IT", "Italy"], ["NL", "Netherlands"], ["NO", "Norway"], ["PL", "Poland"], ["PT", "Portugal"],
]

// Real Overture addresses. Forward lines may start with "CC |" to set a
// per-row country, so one batch can mix countries.
const SAMPLES = {
  forward: [
    { country: "US", text: "6310 Milford Rd, Fayetteville NC 28303\n11130 Seola Beach Dr SW, Seattle WA 98146" },
    { country: "FR", text: "10 Rue de Rivoli, 75001 Paris\n48 Rue de Soissons, 33000 Bordeaux" },
    { country: "DE", text: "Unter den Linden 77, 10117 Berlin\nRosenstrasse 2, 66111 Saarbruecken" },
    {
      country: "US",
      label: "Batch: 20 rows, 6 countries",
      text: [
        "6310 Milford Rd, Fayetteville NC 28303",
        "11130 Seola Beach Dr SW, Seattle WA 98146",
        "3804 Rodenberg Ave, Evansville IN 47720",
        "214 N Lincoln St, Falfurrias TX 78355",
        "300 Rio Tinto Dr, El Paso TX 79912",
        "2937 Eva Cranfill Rd, Yadkinville NC 27055",
        "DE | Unter den Linden 77, 10117 Berlin",
        "DE | Rosenstrasse 2, 66111 Saarbruecken",
        "DE | Meisenweg 29, 24327",
        "DE | Stueberg 16, 22844",
        "FR | 10 Rue de Rivoli, 75001 Paris",
        "FR | 48 Rue de Soissons, 33000 Bordeaux",
        "FR | 12 Allee des Marguerites, 97100",
        "NL | Schiekade 568, 3032AZ Rotterdam",
        "NL | De Ruijterstraat 10, 2518AS Den Haag",
        "NL | Dr. H. Colijnstraat 737, 1067CE Amsterdam",
        "ES | Calle Moratin 6, 28014 Madrid",
        "ES | Calle Roquerol 15, 08818",
        "AT | Alxingergasse 15, 1100 Wien",
        "AT | Wimmerstrasse 42, 4030 Linz",
      ].join("\n"),
    },
  ],
  reverse: [
    { country: "US", text: "40.7484, -73.9857" },
    { country: "", text: "48.8606, 2.3376" },
    { country: "", text: "52.5163, 13.3777" },
    {
      // A country filter keeps batches fast; "Any country" searches every
      // country's addresses (see README, Benchmarks).
      country: "US",
      label: "Batch: 20 US points",
      text: [
        "40.7484, -73.9857", "40.7580, -73.9855", "40.7061, -74.0087", "40.7794, -73.9632", "40.7527, -73.9772",
        "40.7306, -73.9866", "40.7128, -74.0060", "40.7614, -73.9776", "40.7411, -73.9897", "40.7681, -73.9819",
        "47.6205, -122.3493", "47.6097, -122.3331", "41.8789, -87.6359", "41.8827, -87.6233", "37.9933, -87.6217",
        "34.0522, -118.2437", "29.7604, -95.3698", "33.4484, -112.0740", "39.7392, -104.9903", "25.7617, -80.1918",
      ].join("\n"),
    },
  ],
  normalize: [
    { country: "US", text: "123 north main street apt 4b, springfield il 62701" },
    { country: "DE", text: "Hauptstr. 5, 10115 Berlin" },
    { country: "FR", text: "12 bd St Germain, 75005 Paris" },
  ],
}

// Runs a request through /api/job and polls, so batches that take longer than
// the App Runtime ingress timeout (about a minute) still return.
async function post(path: string, body: unknown): Promise<Resp> {
  const kind = path.replace("/api/", "")
  const start = await fetch("/api/job", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ kind, body }),
  })
  const { id, error } = await start.json()
  if (!id) return { error: error ?? "could not start job" }
  for (;;) {
    await new Promise((r) => setTimeout(r, 1500))
    const r = await fetch(`/api/job?id=${id}`)
    const job = await r.json()
    if (job.status === "done") return job.result as Resp
    if (job.error) return { error: job.error }
  }
}

const num = (v: unknown) => (v == null ? NaN : Number(v))

function toPoints(rows: Row[] = []): MapPoint[] {
  return rows
    .filter((r) => Number.isFinite(num(r.LAT)) && Number.isFinite(num(r.LON)))
    .map((r) => ({
      lat: num(r.LAT),
      lon: num(r.LON),
      id: num(r.INPUT_ID),
      kind: "result" as const,
      level: (r.MATCH_LEVEL as string) ?? "ADDRESS",
      label: [r.RAW_ADDRESS ?? r.FULL_ADDRESS, r.MATCH_LEVEL, r.MATCH_SCORE != null && `score ${r.MATCH_SCORE}`, r.DISTANCE_M != null && `${Math.round(num(r.DISTANCE_M))} m`]
        .filter(Boolean)
        .join(" | "),
    }))
}

function parseForward(text: string) {
  return text
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean)
    .map((l) => {
      const m = l.match(/^([A-Za-z]{2})\s*\|\s*(.+)$/)
      return m ? { address: m[2], country: m[1].toUpperCase() } : { address: l }
    })
}

function parsePoints(text: string) {
  return text
    .split("\n")
    .map((l) => l.split(/[,\s]+/).filter(Boolean).map(Number))
    .filter((p) => p.length >= 2 && p.every(Number.isFinite))
    .map(([lat, lon]) => ({ lat, lon }))
}

export function Tester() {
  const [tab, setTab] = useState<Tab>("forward")
  const [country, setCountry] = useState("US")
  const [text, setText] = useState(SAMPLES.forward[0].text)
  const [radius, setRadius] = useState("200")
  const [busy, setBusy] = useState(false)
  const [resp, setResp] = useState<Resp | null>(null)
  const [exchanges, setExchanges] = useState<Exchange[]>([])
  const [points, setPoints] = useState<MapPoint[]>([])
  const [sample, setSample] = useState(0)
  const [lp, setLp] = useState<LibpostalStatus | null>(null)

  function reset() {
    setResp(null)
    setPoints([])
    setExchanges([])
  }

  function switchTab(t: Tab) {
    setTab(t)
    reset()
    loadSample(t, 0)
  }

  function loadSample(t: Tab, i: number) {
    const list = SAMPLES[t]
    const s = list[i % list.length]
    setSample(i % list.length)
    setText(s.text)
    setCountry(s.country)
  }

  async function call(path: string, body: unknown) {
    const r = await post(path, body)
    setExchanges((x) => [...x, { path, body, response: r }])
    return r
  }

  async function run() {
    setBusy(true)
    reset()
    try {
      if (tab === "forward") {
        const r = await call("/api/forward", { items: parseForward(text), country })
        setResp(r)
        setPoints(toPoints(r.rows))
      } else if (tab === "reverse") {
        const pts = parsePoints(text)
        const r = await call("/api/reverse", { points: pts, radius: Number(radius), country: country || null })
        setResp(r)
        setPoints([
          ...pts.map((p, i) => ({ ...p, id: i + 1, kind: "input" as const, label: `input ${i + 1}: ${p.lat}, ${p.lon}` })),
          ...toPoints(r.rows),
        ])
      } else {
        const [n, f] = await Promise.all([
          call("/api/normalize", { address: text, country }),
          call("/api/forward", { items: [{ address: text }], country }),
        ])
        setResp({ ...n, rows: f.rows, sql: [...(n.sql ?? []), ...(f.sql ?? [])], error: n.error ?? f.error })
        setPoints(toPoints(f.rows))
      }
    } catch (e) {
      setResp({ error: String(e) })
    } finally {
      setBusy(false)
    }
  }

  const rows = resp?.rows ?? []
  const cols = rows.length ? Object.keys(rows[0]) : []
  const inputCount = tab === "reverse" ? parsePoints(text).length : tab === "forward" ? parseForward(text).length : 1
  // libpostal is used for normalization and for every non-US forward row.
  const needsLibpostal =
    tab === "normalize" || (tab === "forward" && parseForward(text).some((i) => (i.country ?? country) !== "US"))
  const lpState = libpostalState(lp)

  return (
    <div className="space-y-4">
      <LibpostalControl onChange={setLp} />
      <div className="flex flex-wrap gap-2">
        {(["forward", "reverse", "normalize"] as Tab[]).map((t) => (
          <Button key={t} variant={tab === t ? "default" : "outline"} onClick={() => switchTab(t)}>
            {t === "forward" ? "Forward geocode" : t === "reverse" ? "Reverse geocode" : "Normalize address"}
          </Button>
        ))}
      </div>

      <Card>
        <CardContent className="space-y-3 pt-6">
          <textarea
            className="h-32 w-full rounded-md border bg-background p-2 font-mono text-sm"
            value={text}
            onChange={(e) => setText(e.target.value)}
            placeholder={
              tab === "forward"
                ? "One address per line. Prefix a line with a country code to override it, e.g. DE | Unter den Linden 77, 10117 Berlin"
                : tab === "reverse"
                  ? "One point per line: lat, lon"
                  : "Address"
            }
          />
          <div className="flex flex-wrap items-end gap-3 text-sm">
            <label className="flex flex-col gap-1">
              <span className="text-muted-foreground">{tab === "forward" ? "Default country" : "Country"}</span>
              <select
                className="w-56 rounded-md border bg-background px-2 py-1.5"
                value={country}
                onChange={(e) => setCountry(e.target.value)}
              >
                {tab === "reverse" && <option value="">Any country</option>}
                {COUNTRIES.map(([code, name]) => (
                  <option key={code} value={code}>{code} - {name}</option>
                ))}
              </select>
            </label>
            {tab === "reverse" && (
              <label className="flex flex-col gap-1">
                <span className="text-muted-foreground">Radius (m)</span>
                <input className="w-28 rounded-md border bg-background px-2 py-1.5" value={radius} onChange={(e) => setRadius(e.target.value)} />
              </label>
            )}
            <span className="pb-1.5 text-muted-foreground">
              {inputCount} {tab === "reverse" ? (inputCount === 1 ? "point" : "points") : inputCount === 1 ? "address" : "addresses"}
              {tab === "reverse" && " - click the map to add a point"}
            </span>
          </div>
          <div className="flex flex-wrap gap-2">
            <Button onClick={run} disabled={busy}>{busy ? "Running..." : "Run"}</Button>
            <Button variant="outline" onClick={() => loadSample(tab, sample + 1)}>
              Next sample{"label" in SAMPLES[tab][(sample + 1) % SAMPLES[tab].length] ? " (batch)" : ""}
            </Button>
            {resp?.ms != null && <span className="self-center text-sm text-muted-foreground">{(resp.ms / 1000).toFixed(1)} s</span>}
          </div>
          {needsLibpostal && (lpState === "suspended" || lpState === "starting") && (
            <p className="text-sm text-warning">
              libpostal is {lpState}. The first call starts it: about 16 s, or about 100 s if its compute pool is also suspended.
            </p>
          )}
          {needsLibpostal && lpState === "absent" && (
            <p className="text-sm text-muted-foreground">
              libpostal is not installed (--no-intl), so only US addresses and the US parser work.
            </p>
          )}
        </CardContent>
      </Card>

      <ResultMap
        points={points}
        radius={tab === "reverse" ? Number(radius) : undefined}
        onClick={tab === "reverse" ? (la, lo) => setText((t) => `${t.trim() ? t.trim() + "\n" : ""}${la.toFixed(6)}, ${lo.toFixed(6)}`) : undefined}
      />

      {resp?.error && <EmptyState variant="error" title="Query failed" description={resp.error} />}

      {tab === "normalize" && resp?.result && (
        <Card>
          <CardHeader><CardTitle>Normalization functions</CardTitle></CardHeader>
          <CardContent className="grid gap-3 md:grid-cols-2">
            {Object.entries(resp.result).map(([k, v]) => (
              <div key={k} className="min-w-0">
                <div className="text-xs font-medium text-muted-foreground">{k}</div>
                <pre className="overflow-x-auto rounded bg-muted p-2 text-xs">{JSON.stringify(v, null, 2)}</pre>
              </div>
            ))}
          </CardContent>
        </Card>
      )}

      {rows.length > 0 && (
        <Card>
          <CardHeader><CardTitle>Results ({rows.length} of {inputCount})</CardTitle></CardHeader>
          <CardContent className="p-0">
            <Table>
              <TableHeader>
                <TableRow>{cols.map((c) => <TableHead key={c}>{c}</TableHead>)}</TableRow>
              </TableHeader>
              <TableBody>
                {rows.map((r, i) => (
                  <TableRow key={i}>{cols.map((c) => <TableCell key={c}>{r[c] == null ? "" : String(r[c])}</TableCell>)}</TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      )}
      {resp && !resp.error && tab !== "normalize" && rows.length === 0 && (
        <EmptyState title="No match" description={resp.status ?? "Nothing found. Try a larger radius or another country."} />
      )}

      {exchanges.map((x, i) => (
        <div key={i} className="grid gap-3 md:grid-cols-2">
          <CodePanel title={`Request: POST ${x.path}`} text={JSON.stringify(x.body, null, 2)} />
          <CodePanel title={`Response: ${x.path}`} text={JSON.stringify(trimRows(x.response), null, 2)} />
        </div>
      ))}
      {resp?.sql && <CodePanel title="SQL that ran" text={resp.sql.join(";\n\n") + ";"} />}
    </div>
  )
}

// Keep the response panel readable for 200-row batches.
function trimRows(r: unknown) {
  const o = r as Resp
  return o?.rows && o.rows.length > 20 ? { ...o, rows: [...o.rows.slice(0, 20), `... ${o.rows.length - 20} more rows`] } : r
}

function CodePanel({ title, text }: { title: string; text: string }) {
  const [copied, setCopied] = useState(false)
  return (
    <details className="min-w-0 rounded-lg border text-sm" open={title.startsWith("Request")}>
      <summary className="flex cursor-pointer items-center justify-between px-3 py-2 text-muted-foreground">
        <span>{title}</span>
        <Button
          size="sm"
          variant="ghost"
          onClick={async (e) => {
            e.preventDefault()
            if (await copyText(text)) {
              setCopied(true)
              setTimeout(() => setCopied(false), 1500)
            }
          }}
        >
          {copied ? "Copied" : "Copy"}
        </Button>
      </summary>
      <pre className="max-h-96 overflow-auto border-t bg-muted p-3 text-xs">{text}</pre>
    </details>
  )
}
