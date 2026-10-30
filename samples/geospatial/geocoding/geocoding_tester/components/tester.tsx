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
type Resp = { rows?: Row[]; result?: Row; results?: Row[]; sql?: string[]; status?: string; ms?: number; error?: string }
type Exchange = { path: string; body: unknown; response: unknown }

// Forward geocoding coverage: US plus the 14 countries of the EU index.
// Reverse geocoding works anywhere ("" = any country).
const COUNTRIES: [string, string][] = [
  ["US", "United States"], ["AT", "Austria"], ["BE", "Belgium"], ["CH", "Switzerland"], ["CZ", "Czechia"],
  ["DE", "Germany"], ["DK", "Denmark"], ["ES", "Spain"], ["FI", "Finland"], ["FR", "France"],
  ["IT", "Italy"], ["NL", "Netherlands"], ["NO", "Norway"], ["PL", "Poland"], ["PT", "Portugal"],
]

type Sample = { label: string; country: string; text: string; batch?: boolean }

const US10 = [
  "396 Akron Rd, Wadsworth OH 44281", "2517 30th St SW, Akron OH 44314", "4329 Wisconsin Ave, Stickney IL 60402",
  "3844 Parrot Dr, Ellicott City MD 21042", "227 Thomas Ave, Cumberland City TN 37050", "1711 Bearhollow Rd, Greensboro NC 27410",
  "107 Canal St, Port Washington OH 43837", "235 Broadbay Dr, Kill Devil Hills NC 27948", "1631 Turnberry Dr, Pickerington OH 43147",
  "3706 Bryant Cir, Kansas City KS 66102",
]
const DE10 = [
  "Gartenstrasse 7, 08541", "Blumenstrasse 32, 16348", "Bahnhofstrasse 3, 03051", "Weidenstieg 6, 24568", "Dorfstrasse 39, 24241",
  "Pestalozzistrasse 13, 07952", "Richterweg 2b, 09125", "Stephanstrasse 8b, 15345", "Schlackenweg 5, 01665", "Schulstrasse 32, 25876",
]
const FR10 = [
  "10 Impasse de l'Epi, 84000", "663 Rue de la Roche, 45200", "81 Rue du General de Gaulle, 59139", "124 Route de Nevers, 58180",
  "3 Route du Pont Cauvin, 50160", "110 Rue des Narcisses, 16220", "27 Grande Rue, 70000", "115 Route de Camasso, 30120",
  "1 Rue Traversiere, 28290", "48 Rue de Soissons, 33000 Bordeaux",
]
const NL8 = ["Lenna 7, 8471MT", "Buitenvaart 1601, 7905SL", "Glipper Dreef 189, 2104WE", "Obrechtlaan 4, 5691GV", "Tsjalk 7, 8721EN", "Bachlaan 310, 5011BG", "Singel 13, 3961CE", "Westerspoor 12, 1687AZ"]
const AT8 = ["Baerengasse 2, 4616", "Heinreichs 16, 3902", "Pfaffetschlag 73, 4163", "Ziegelgraben 18, 4631", "Siebachweg 1, 7423", "Mitterschlag 12, 3921", "Grillparzerstrasse 9, 4962", "Eingarten 12, 5204"]
const ES8 = ["Calle Iglesia 73, 24882", "Calle Sancho Panza 28, 45216", "Calle Vereda del Mercado 19, 04131", "Plaza Marsans 11, 08940", "Calle Bayoneta 5, 24343", "Calle Ciruelo 52, 35015", "Calle Alicante 41, 03185", "Calle Alhamilla 12, 04230"]
const tag = (cc: string, rows: string[]) => rows.map((r) => `${cc} | ${r}`)
const lines = (...rows: string[][]) => rows.flat().join("\n")

// Real Overture addresses. Forward and normalize lines may start with "CC |"
// to set a per-row country, so one batch can mix countries.
const SAMPLES: Record<"forward" | "reverse" | "normalize", Sample[]> = {
  forward: [
    { label: "US: 2 addresses", country: "US", text: "6310 Milford Rd, Fayetteville NC 28303\n11130 Seola Beach Dr SW, Seattle WA 98146" },
    { label: "FR: 2 addresses", country: "FR", text: "10 Rue de Rivoli, 75001 Paris\n48 Rue de Soissons, 33000 Bordeaux" },
    { label: "DE: 2 addresses", country: "DE", text: "Unter den Linden 77, 10117 Berlin\nRosenstrasse 2, 66111 Saarbruecken" },
    { label: "Batch: 10 US addresses", country: "US", batch: true, text: lines(US10) },
    { label: "Batch: 10 DE addresses", country: "DE", batch: true, text: lines(DE10) },
    { label: "Batch: 10 FR addresses", country: "FR", batch: true, text: lines(FR10) },
    {
      label: "Batch: 50 addresses, 6 countries",
      country: "US",
      batch: true,
      text: lines(US10, tag("DE", DE10.slice(0, 8)), tag("FR", FR10.slice(0, 8)), tag("NL", NL8), tag("AT", AT8), tag("ES", ES8)),
    },
    {
      country: "US",
      batch: true,
      label: "Batch: 20 addresses, 6 countries",
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
    { label: "New York: 1 point", country: "US", text: "40.7484, -73.9857" },
    { label: "Paris: 1 point, any country", country: "", text: "48.8606, 2.3376" },
    { label: "Berlin: 1 point, any country", country: "", text: "52.5163, 13.3777" },
    {
      label: "Batch: 10 Berlin points",
      country: "DE",
      batch: true,
      text: lines([
        "52.5163, 13.3777", "52.5200, 13.4050", "52.5096, 13.3759", "52.5251, 13.3694", "52.4986, 13.4197",
        "52.5397, 13.4247", "52.5058, 13.3221", "52.4875, 13.3524", "52.5308, 13.3846", "52.5145, 13.4501",
      ]),
    },
    {
      label: "Batch: 10 Paris points",
      country: "FR",
      batch: true,
      text: lines([
        "48.8606, 2.3376", "48.8566, 2.3522", "48.8738, 2.2950", "48.8530, 2.3499", "48.8867, 2.3431",
        "48.8462, 2.3464", "48.8656, 2.3212", "48.8417, 2.3219", "48.8710, 2.3590", "48.8495, 2.3810",
      ]),
    },
    {
      // "Any country" searches every country's addresses, so it is slower
      // (see README, Benchmarks).
      label: "Batch: 5 points, any country",
      country: "",
      batch: true,
      text: lines(["40.7484, -73.9857", "48.8606, 2.3376", "52.5163, 13.3777", "52.3702, 4.8952", "40.4168, -3.7038"]),
    },
    {
      // A country filter keeps batches fast.
      country: "US",
      batch: true,
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
    { label: "US: apartment, lowercase", country: "US", text: "123 north main street apt 4b, springfield il 62701" },
    { label: "DE: Str. abbreviation", country: "DE", text: "Hauptstr. 5, 10115 Berlin" },
    { label: "FR: bd abbreviation", country: "FR", text: "12 bd St Germain, 75005 Paris" },
    {
      label: "Batch: 12 addresses, 6 countries",
      country: "US",
      batch: true,
      text: lines([
        "123 north main street apt 4b, springfield il 62701",
        "6310 Milford Rd, Fayetteville NC 28303",
        "DE | Hauptstr. 5, 10115 Berlin",
        "DE | Gartenstr. 7, 08541",
        "FR | 12 bd St Germain, 75005 Paris",
        "FR | 81 r du Gen de Gaulle, 59139",
        "ES | C/ Iglesia 73, 24882",
        "ES | Pza Marsans 11, 08940",
        "NL | Glipper Dreef 189, 2104WE",
        "NL | Bachln 310, 5011BG",
        "IT | V. Novara 117, Milano",
        "AT | Grillparzerstr. 9, 4962",
      ]),
    },
    {
      label: "Batch: 10 US unit and suffix variants",
      country: "US",
      batch: true,
      text: lines([
        "396 Akron Road Suite 200, Wadsworth OH 44281",
        "2517 30th St SW Unit 3, Akron OH 44314",
        "4329 W Wisconsin Avenue, Stickney IL 60402",
        "3844 Parrot Drive #12, Ellicott City MD 21042",
        "227 Thomas Av, Cumberland City TN 37050",
        "1711 Bearhollow Road Apt B, Greensboro NC 27410",
        "107 Canal Street Fl 2, Port Washington OH 43837",
        "235 Broadbay Dr., Kill Devil Hills NC 27948",
        "1631 Turnberry Drv, Pickerington OH 43147",
        "3706 Bryant Circle, Kansas City KS 66102",
      ]),
    },
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
  const [picked, setPicked] = useState<number | null>(null)
  const [lp, setLp] = useState<LibpostalStatus | null>(null)

  function reset() {
    setResp(null)
    setPoints([])
    setExchanges([])
    setPicked(null)
  }

  function switchTab(t: Tab) {
    setTab(t)
    reset()
    loadSample(t, 0)
  }

  function loadSample(t: Tab, i: number) {
    const s = SAMPLES[t][i]
    setSample(i)
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
        // Each line is one record; "CC |" sets its country, as on Forward.
        const items = parseForward(text)
        const one = items.length === 1
        const [n, f] = await Promise.all([
          call("/api/normalize", one ? { address: items[0].address, country: items[0].country ?? country } : { items, country }),
          call("/api/forward", { items, country }),
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
  const inputCount = tab === "reverse" ? parsePoints(text).length : parseForward(text).length
  const isCustom = text !== SAMPLES[tab][sample]?.text
  // libpostal is used for normalization and for every non-US forward row.
  const needsLibpostal =
    tab === "normalize" || (tab === "forward" && parseForward(text).some((i) => (i.country ?? country) !== "US"))
  const lpState = libpostalState(lp)
  const normRows = resp?.results ?? []
  const pickedResult = picked != null ? normRows.find((r) => Number(r.ID) === picked) : null

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
              tab === "reverse"
                ? "One point per line: lat, lon"
                : "One address per line. Prefix a line with a country code to override it, e.g. DE | Unter den Linden 77, 10117 Berlin"
            }
          />
          <div className="flex flex-wrap items-end gap-3 text-sm">
            <label className="flex flex-col gap-1">
              <span className="text-muted-foreground">Sample</span>
              <select
                className="w-72 rounded-md border bg-background px-2 py-1.5"
                value={isCustom ? "custom" : String(sample)}
                onChange={(e) => {
                  if (e.target.value === "custom") return
                  reset()
                  loadSample(tab, Number(e.target.value))
                }}
              >
                {isCustom && <option value="custom">Custom</option>}
                {(["Single record", "Multiple records"] as const).map((g) => (
                  <optgroup key={g} label={g}>
                    {SAMPLES[tab].map((s, i) =>
                      !!s.batch === (g === "Multiple records") ? (
                        <option key={i} value={i}>{s.label}</option>
                      ) : null,
                    )}
                  </optgroup>
                ))}
              </select>
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-muted-foreground">{tab === "reverse" ? "Country" : "Default country"}</span>
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
            <Button onClick={run} disabled={busy || inputCount === 0}>
              {busy ? "Running..." : inputCount > 1 ? `Run ${inputCount} records` : "Run"}
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

      {tab === "normalize" && normRows.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Normalization ({normRows.length} records) - click a row for the full output</CardTitle>
          </CardHeader>
          <CardContent className="p-0">
            <Table>
              <TableHeader>
                <TableRow>
                  {["ID", "Input", "CC", "US parser", "STANDARDIZE_STREET", "libpostal", "NORMALIZE_STREET_EU"].map((h) => (
                    <TableHead key={h}>{h}</TableHead>
                  ))}
                </TableRow>
              </TableHeader>
              <TableBody>
                {normRows.map((r) => (
                  <TableRow
                    key={String(r.ID)}
                    className={`cursor-pointer ${picked === Number(r.ID) ? "bg-muted" : ""}`}
                    onClick={() => setPicked(picked === Number(r.ID) ? null : Number(r.ID))}
                  >
                    <TableCell>{String(r.ID)}</TableCell>
                    <TableCell className="max-w-64 truncate">{String(r.ADDRESS)}</TableCell>
                    <TableCell>{String(r.COUNTRY)}</TableCell>
                    <TableCell>{fields(r.PARSE_ADDRESS, ["number", "street", "city", "zip"])}</TableCell>
                    <TableCell>{short(r.STANDARDIZE_STREET)}</TableCell>
                    <TableCell>{fields(r.PARSE_ADDRESS_INTL, ["house_number", "road", "postcode", "city"])}</TableCell>
                    <TableCell>{short(r.NORMALIZE_STREET_EU)}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      )}

      {tab === "normalize" && (resp?.result || pickedResult) && (
        <Card>
          <CardHeader>
            <CardTitle>Normalization functions{pickedResult ? `: record ${pickedResult.ID}, ${pickedResult.ADDRESS}` : ""}</CardTitle>
          </CardHeader>
          <CardContent className="grid gap-3 md:grid-cols-2">
            {Object.entries(pickedResult ?? resp!.result!)
              .filter(([k]) => !["ID", "ADDRESS", "COUNTRY"].includes(k))
              .map(([k, v]) => (
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
  const cut = (a?: Row[]) => (a && a.length > 20 ? [...a.slice(0, 20), `... ${a.length - 20} more rows`] : a)
  if (o?.rows && o.rows.length > 20) return { ...o, rows: cut(o.rows) }
  if (o?.results && o.results.length > 20) return { ...o, results: cut(o.results) }
  return r
}

// Compact cell text for the normalize table.
function fields(v: unknown, keys: string[]) {
  if (!v || typeof v !== "object") return short(v)
  const o = v as Record<string, unknown>
  if (o.error) return "error"
  return keys.map((k) => o[k]).filter((x) => x != null && x !== "").join(" | ")
}

function short(v: unknown) {
  if (v == null) return ""
  if (typeof v === "object") return (v as Record<string, unknown>).error ? "error" : JSON.stringify(v)
  return String(v)
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
