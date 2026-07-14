# Viewer: the Martin Web UI

The demo viewer is **Martin's built-in Web UI**, enabled with
`--webui enable-for-all` in the service spec. No separate app is deployed. Once
the service is up, the public ingress serves:

- `/`            - the Web UI (tile-source catalog + interactive inspector map)
- `/catalog`     - JSON list of published sources (expect `features_mvt` + `features_pmt`)
- `/features_mvt/{z}/{x}/{y}` - arm 1, dynamic `ST_AsMVT` (live from PostGIS)
- `/features_pmt/{z}/{x}/{y}` - arm 2, precomputed PMTiles (from the mounted stage)

Open the ingress URL the installer prints (auth-gated by the SPCS proxy) and use
the **Inspect Tile Source** page to render each source on a basemap.

## Basemap CSP fix (required, or basemaps are blank)

Behind the SPCS public ingress the proxy injects a baseline CSP on every response:

```
default-src 'self' 'unsafe-inline' 'unsafe-eval' blob: data:; object-src 'none'; connect-src 'self'; frame-ancestors 'self';
```

The `connect-src 'self'` clause blocks the browser from fetching **external**
basemap raster tiles (openstreetmap.org, cartocdn.com), so the inspector map shows
your vector layers over a blank background with a "Refused to connect ... violates
the Content Security Policy" console error. Same-origin `features_*` tiles are
unaffected.

**Fix (documented SPCS behavior):** the proxy widens the page CSP `connect-src` to
include the hosts of any **EAI attached to the service**. The installer therefore
attaches `TILESERVER_BASEMAP_EAI` (network rule `BASEMAP_NETWORK_RULE`, see
`infra.sql`) listing the OSM + Carto hosts. Keep the PG EAI attached too, or Martin
loses PostGIS and crash-loops:

```sql
ALTER SERVICE TILESERVER.CORE.MARTIN
  SET EXTERNAL_ACCESS_INTEGRATIONS = (TILESERVER_PG_EAI, TILESERVER_BASEMAP_EAI);
```

`ALTER SERVICE ... SET EXTERNAL_ACCESS_INTEGRATIONS` triggers a service restart.

Notes: the baseline CSP already allows `blob:`/`unsafe-inline`/`unsafe-eval`, so the
MapLibre worker + inline-style warnings are report-only and harmless. Network rules
require full hostnames (no wildcards) - include the Carto `a./b./c./d.` subdomains.
