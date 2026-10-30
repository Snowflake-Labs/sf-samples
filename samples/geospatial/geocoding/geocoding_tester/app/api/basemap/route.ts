export const dynamic = "force-dynamic"

// Same-origin proxy for the CARTO basemap. Snowflake App Runtime serves pages
// with CSP connect-src 'self', so MapLibre cannot fetch the style, tiles,
// glyphs or sprites from basemaps.cartocdn.com directly. Only CARTO hosts are
// allowed, so this cannot be used as an open proxy.
const ALLOWED = /^([a-d]\.|tiles(-[a-d])?\.)?basemaps\.cartocdn\.com$/

export async function GET(req: Request) {
  const raw = new URL(req.url).searchParams.get("u")
  let target: URL
  try {
    target = new URL(raw ?? "")
  } catch {
    return new Response("bad url", { status: 400 })
  }
  if (target.protocol !== "https:" || !ALLOWED.test(target.hostname)) {
    return new Response("host not allowed", { status: 403 })
  }
  const upstream = await fetch(target, { headers: { accept: req.headers.get("accept") ?? "*/*" } })
  const headers = new Headers()
  // Node's fetch already decompressed the body, so content-encoding is not copied.
  for (const h of ["content-type", "cache-control", "etag"]) {
    const v = upstream.headers.get(h)
    if (v) headers.set(h, v)
  }
  if (!headers.has("cache-control")) headers.set("cache-control", "public, max-age=86400")
  return new Response(upstream.body, { status: upstream.status, headers })
}
