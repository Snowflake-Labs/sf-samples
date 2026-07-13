#!/usr/bin/env bash
# deploy-tileserver / bake_pmtiles.sh
#
# Bake the PMTiles archive (arm 2) and upload it to the TILES stage. Pipeline:
#   export_geojson.py -> build tippecanoe image -> tippecanoe -> mbtiles2pmtiles.py
#   -> validate magic -> snow stage copy -> @TILESERVER.CORE.TILES
#
# Usage:
#   bake_pmtiles.sh --connection <conn>
# Env:
#   SOURCE_TABLE   Snowflake source (default Overture divisions share)
#   COUNTRY        optional country filter (e.g. US) to keep the bake small/fast
#   MAXZOOM        tippecanoe max zoom (default 12)
#   TILES_STAGE    stage FQN (default TILESERVER.CORE.TILES)
#   CONTAINER_CMD  docker|podman (default: docker if present else podman)
#   WORKDIR        scratch dir (default: a mktemp dir)
set -euo pipefail

CONNECTION=""
while [ $# -gt 0 ]; do
  case "$1" in
    --connection) CONNECTION="${2:-}"; shift 2;;
    *) echo "Unknown arg: $1"; exit 2;;
  esac
done
[ -n "$CONNECTION" ] || { echo "ERROR: --connection required"; exit 2; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SKILL_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
MAXZOOM="${MAXZOOM:-12}"
TILES_STAGE="${TILES_STAGE:-TILESERVER.CORE.TILES}"
CONTAINER_CMD="${CONTAINER_CMD:-$(command -v docker >/dev/null 2>&1 && echo docker || echo podman)}"
WORKDIR="${WORKDIR:-$(mktemp -d)}"
GEOJSONL="$WORKDIR/features.geojsonl"
MBTILES="$WORKDIR/features.mbtiles"
PMTILES="$WORKDIR/features_pmt.pmtiles"

echo "[bake] workdir=$WORKDIR maxzoom=$MAXZOOM country=${COUNTRY:-<all>}"

# 1. Export GeoJSONL from Snowflake (connection passed to the exporter via env).
echo "[bake] exporting GeoJSONL from Snowflake ..."
SNOWFLAKE_CONNECTION="$CONNECTION" OUT="$GEOJSONL" python3 "$SCRIPT_DIR/export_geojson.py"

# 2. Build the felt/tippecanoe image (not pullable from registries) from source.
echo "[bake] building tippecanoe image ..."
"$CONTAINER_CMD" build --platform linux/amd64 -t tileserver-tippecanoe:latest "$SKILL_DIR/tippecanoe"

# 3. tippecanoe -> mbtiles (per-subtype minzoom already stamped in each feature).
echo "[bake] running tippecanoe ..."
"$CONTAINER_CMD" run --rm -v "$WORKDIR:/data" tileserver-tippecanoe:latest \
  -o /data/features.mbtiles -z"$MAXZOOM" \
  --drop-densest-as-needed --detect-shared-borders --force \
  /data/features.geojsonl

# 4. Convert MBTiles (SQLite) -> real PMTiles v3 (TMS->XYZ, GZIP).
echo "[bake] converting mbtiles -> pmtiles ..."
SRC="$MBTILES" OUT="$PMTILES" python3 "$SCRIPT_DIR/mbtiles2pmtiles.py"

# 5. Validate the PMTiles magic number (Martin aborts on a bad source).
magic="$(head -c 7 "$PMTILES" | tr -d '\0')"
if [ "$magic" != "PMTiles" ]; then
  echo "ERROR: $PMTILES is not a valid PMTiles archive (magic='$magic'). Aborting upload." >&2
  exit 1
fi
echo "[bake] magic OK (PMTiles)"

# 6. Upload to the TILES stage (raw, not gzipped - the archive is already gzipped internally).
echo "[bake] uploading to @$TILES_STAGE ..."
snow stage copy "$PMTILES" "@$TILES_STAGE" -c "$CONNECTION" --overwrite

echo "[bake] done: features_pmt.pmtiles -> @$TILES_STAGE"
