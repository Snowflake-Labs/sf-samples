#!/usr/bin/env bash
# Dev helper: run Martin locally in Docker against a Snowflake Postgres instance.
#
# Portable: the DATABASE_URL is taken from the environment if already set (e.g.
# exported by provision_pg.py). Otherwise it is assembled from PGHOST + the
# password looked up in ~/.pgpass for that host - no host is hardcoded here.
#
# Usage:
#   PGHOST=<instance-host> PGUSER=snowflake_admin bash run_martin_local.sh
#   # or, if you already have a full URL:
#   DATABASE_URL='postgresql://...' bash run_martin_local.sh
set -euo pipefail

if [ -z "${DATABASE_URL:-}" ]; then
  HOST="${PGHOST:-}"
  USER="${PGUSER:-snowflake_admin}"
  DB="${PGDATABASE:-postgres}"
  [ -n "$HOST" ] || { echo "ERROR: set DATABASE_URL, or PGHOST for a ~/.pgpass lookup" >&2; exit 1; }
  PW="$(awk -F: -v h="$HOST" '$1==h {print $5}' ~/.pgpass)"
  [ -n "${PW:-}" ] || { echo "ERROR: no password in ~/.pgpass for $HOST" >&2; exit 1; }
  export DATABASE_URL="postgresql://${USER}:${PW}@${HOST}:5432/${DB}?sslmode=require"
fi

docker rm -f martin_tiles >/dev/null 2>&1 || true
exec docker run --rm --name martin_tiles -p 3000:3000 \
  -e DATABASE_URL \
  ghcr.io/maplibre/martin:latest \
  --pool-size 4 --listen-addresses 0.0.0.0:3000 --webui enable-for-all
