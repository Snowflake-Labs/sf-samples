"use client"

import { useEffect, useMemo, useRef, useState } from "react"
import DeckGL from "@deck.gl/react"
import { WebMercatorViewport, type MapViewState, type PickingInfo } from "@deck.gl/core"
import { LineLayer, ScatterplotLayer } from "@deck.gl/layers"
import { Map } from "react-map-gl/maplibre"
import "maplibre-gl/dist/maplibre-gl.css"

export type MapPoint = {
  lon: number
  lat: number
  kind: "result" | "input"
  label: string
  level?: string
  /** Links an input point to its result (reverse batches). */
  id?: number
}

const BASEMAP = "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"

// Route every CARTO request through the app's own /api/basemap proxy (CSP).
const proxied = (url: string) =>
  url.includes("cartocdn.com")
    ? { url: `${window.location.origin}/api/basemap?u=${encodeURIComponent(url)}` }
    : { url }

const LEVEL_COLOR: Record<string, [number, number, number]> = {
  ADDRESS: [22, 163, 74],
  STREET: [37, 99, 235],
  POSTCODE: [234, 179, 8],
  LOCALITY: [249, 115, 22],
}

export function ResultMap({
  points,
  radius,
  onClick,
}: {
  points: MapPoint[]
  radius?: number
  onClick?: (lat: number, lon: number) => void
}) {
  const [view, setView] = useState<MapViewState>({ longitude: -40, latitude: 42, zoom: 2 })
  const box = useRef<HTMLDivElement>(null)

  // Fit to results whenever they change.
  useEffect(() => {
    if (!points.length) return
    const lons = points.map((p) => p.lon)
    const lats = points.map((p) => p.lat)
    const [w, e, s, n] = [Math.min(...lons), Math.max(...lons), Math.min(...lats), Math.max(...lats)]
    if (w === e && s === n) {
      setView((v) => ({ ...v, longitude: w, latitude: s, zoom: 16 }))
      return
    }
    const el = box.current
    const vp = new WebMercatorViewport({ width: el?.clientWidth || 900, height: el?.clientHeight || 460 })
    const { longitude, latitude, zoom } = vp.fitBounds([[w, s], [e, n]], { padding: 80 })
    setView((v) => ({ ...v, longitude, latitude, zoom: Math.min(zoom, 16) }))
  }, [points])

  const layers = useMemo(() => {
    const inputs = points.filter((p) => p.kind === "input")
    const results = points.filter((p) => p.kind === "result")
    const byId = new globalThis.Map(inputs.map((p) => [p.id, p]))
    const links = results.filter((r) => byId.has(r.id)).map((r) => ({ from: byId.get(r.id)!, to: r }))
    return [
      inputs.length && inputs.length <= 25 && radius
        ? new ScatterplotLayer({
            id: "radius",
            data: inputs,
            getPosition: (p: MapPoint) => [p.lon, p.lat],
            getRadius: radius,
            radiusUnits: "meters",
            filled: true,
            getFillColor: [37, 99, 235, 25],
            stroked: true,
            getLineColor: [37, 99, 235, 160],
            lineWidthMinPixels: 1,
          })
        : null,
      links.length
        ? new LineLayer({
            id: "links",
            data: links,
            getSourcePosition: (l: { from: MapPoint }) => [l.from.lon, l.from.lat],
            getTargetPosition: (l: { to: MapPoint }) => [l.to.lon, l.to.lat],
            getColor: [100, 116, 139],
            getWidth: 2,
          })
        : null,
      new ScatterplotLayer({
        id: "points",
        data: points,
        pickable: true,
        getPosition: (p: MapPoint) => [p.lon, p.lat],
        getRadius: 7,
        radiusUnits: "pixels",
        stroked: true,
        getLineColor: [255, 255, 255],
        lineWidthMinPixels: 2,
        getFillColor: (p: MapPoint) =>
          p.kind === "input" ? [220, 38, 38] : LEVEL_COLOR[p.level ?? "ADDRESS"] ?? [22, 163, 74],
      }),
    ]
  }, [points, radius])

  return (
    <div ref={box} className="relative h-[460px] w-full overflow-hidden rounded-lg border">
      <DeckGL
        viewState={view}
        onViewStateChange={({ viewState }) => setView(viewState as MapViewState)}
        controller
        layers={layers}
        getCursor={() => (onClick ? "crosshair" : "grab")}
        onClick={(info: PickingInfo) => {
          if (onClick && info.coordinate) onClick(info.coordinate[1], info.coordinate[0])
        }}
        getTooltip={({ object }: PickingInfo<MapPoint>) => (object ? object.label : null)}
      >
        <Map mapStyle={BASEMAP} transformRequest={proxied} />
      </DeckGL>
      <div className="absolute bottom-2 left-2 flex gap-3 rounded bg-background/90 px-2 py-1 text-xs">
        {Object.entries(LEVEL_COLOR).map(([k, c]) => (
          <span key={k} className="flex items-center gap-1">
            <span className="inline-block size-2.5 rounded-full" style={{ background: `rgb(${c.join(",")})` }} />
            {k}
          </span>
        ))}
        <span className="flex items-center gap-1">
          <span className="inline-block size-2.5 rounded-full bg-red-600" />
          input
        </span>
      </div>
    </div>
  )
}
