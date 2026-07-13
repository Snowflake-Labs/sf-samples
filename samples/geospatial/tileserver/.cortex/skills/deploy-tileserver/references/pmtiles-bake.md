# PMTiles bake pipeline (arm 2)

Arm 2 serves a **precomputed** PMTiles archive from the same dataset, alongside
the dynamic arm, from the one Martin service. The bake runs by default on every
install via `scripts/bake_pmtiles.sh`.

## Pipeline

```
export_geojson.py            build tippecanoe image        tippecanoe
(Snowflake -> GeoJSONL)  ->  (felt/tippecanoe from src) ->  (GeoJSONL -> .mbtiles)
        |                                                          |
        v                                                          v
   mbtiles2pmtiles.py  <---------------------------------  validate magic
   (.mbtiles -> .pmtiles, TMS->XYZ)                         first bytes = "PMTiles\x03"
        |
        v
   snow stage copy -> @TILESERVER.CORE.TILES/features_pmt.pmtiles
        |
        v
   ALTER SERVICE ... SUSPEND; RESUME   (Martin re-reads the mounted archive)
```

## Why tippecanoe (not martin-cp / per-tile)

- The small Postgres instance is flaky under the many short connections a per-tile
  bake makes. Exporting one GeoJSONL and baking locally avoids per-tile DB load.
- `bake_pmtiles.py` (the direct per-tile baker) is kept for small/dev bakes only.

## Gotchas (baked into the scripts)

- **felt/tippecanoe is not pullable** from Docker Hub / GHCR - build it from source
  with `tippecanoe/Dockerfile` (multi-stage, debian bookworm).
- This tippecanoe build **emits MBTiles (SQLite) even with a `.pmtiles` name.**
  `mbtiles2pmtiles.py` converts it: it flips the TMS `tile_row` to XYZ and writes a
  real PMTiles v3 archive with `tile_compression = GZIP` (tippecanoe tiles are
  gzipped MVT).
- **Validate the magic number.** A real archive starts with `PMTiles\x03`; a
  mis-named MBTiles starts with `SQLite format 3`. Martin treats a bad PMTiles
  source as **FATAL** and aborts at startup (default `on_invalid: abort`), so the
  bake script asserts the magic before upload.
- **`snow stage copy` does not gzip** the raw file here (good - the archive is
  already internally gzipped). Do not double-compress.
- Per-subtype `tippecanoe.minzoom` layering (set in `export_geojson.py`) keeps tiles
  light: countries at z0, regions z3, county z5, ... microhood z10.

## Scope / performance

- `COUNTRY=US` (or a bbox) keeps the export small and fast. A full global export via
  the Snowflake connector is slow (single-threaded fetch); for global/large, prefer
  reading Overture GeoParquet directly with DuckDB (parallel, predicate pushdown).
