#!/usr/bin/env bash
# Run Martin locally in Docker against the Snowflake Postgres instance.
# Builds DATABASE_URL at runtime from ~/.pgpass so the password is never
# printed or stored in a file. Small pool size to respect the instance's
# limited connection budget.
set -euo pipefail

HOST="lnvzh5kytbhlbiwf45dhai7lhe.pm-fleet-test.us-west-2.aws.postgres.snowflake.app"
PW="$(awk -F: -v h="$HOST" '$1==h {print $5}' ~/.pgpass)"
if [ -z "${PW:-}" ]; then
  echo "ERROR: no password found in ~/.pgpass for $HOST" >&2
  exit 1
fi
export DATABASE_URL="postgresql://snowflake_admin:${PW}@${HOST}:5432/postgres?sslmode=require"

docker rm -f martin_tiles >/dev/null 2>&1 || true
exec docker run --rm --name martin_tiles -p 3000:3000 \
  -e DATABASE_URL \
  ghcr.io/maplibre/martin:latest \
  --pool-size 4 --listen-addresses 0.0.0.0:3000
