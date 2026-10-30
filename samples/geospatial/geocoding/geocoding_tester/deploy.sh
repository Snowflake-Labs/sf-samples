#!/usr/bin/env bash
# Deploy GEOCODING_TESTER (Snowflake App Runtime). Called by install_geocoding.sh;
# can also be run on its own:  bash geocoding_tester/deploy.sh --connection <conn>
set -euo pipefail

CONN=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --connection|-c) CONN="${2:-}"; shift 2 ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done
[[ -n "$CONN" ]] || { echo "ERROR: --connection is required" >&2; exit 1; }

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TRACKING_TAG='{"origin":"sf_sit-is-geocoding","name":"oss-geocoding","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"app"}}'

if ! snow app deploy --help >/dev/null 2>&1; then
  echo "ERROR: this Snowflake CLI has no 'snow app' commands. Upgrade it (brew upgrade snowflake-cli" >&2
  echo "       or pip install -U snowflake-cli), or re-run the installer with --no-app." >&2
  exit 1
fi

# Scratch schema for the per-request input/output tables the app creates.
snow sql -c "$CONN" --silent -q "CREATE SCHEMA IF NOT EXISTS GEOCODING.TESTER COMMENT='${TRACKING_TAG}';" >/dev/null

# The browser may only connect to the app's own origin (CSP), so the app
# proxies the CARTO basemap server-side; that needs egress to CARTO.
snow sql -c "$CONN" --silent -q "
  CREATE NETWORK RULE IF NOT EXISTS GEOCODING.PUBLIC.GEOCODING_TESTER_CARTO_RULE
    MODE = EGRESS TYPE = HOST_PORT
    VALUE_LIST = ('basemaps.cartocdn.com', 'a.basemaps.cartocdn.com', 'b.basemaps.cartocdn.com',
                  'c.basemaps.cartocdn.com', 'd.basemaps.cartocdn.com', 'tiles.basemaps.cartocdn.com',
                  'tiles-a.basemaps.cartocdn.com', 'tiles-b.basemaps.cartocdn.com',
                  'tiles-c.basemaps.cartocdn.com', 'tiles-d.basemaps.cartocdn.com')
    COMMENT = '${TRACKING_TAG}';
  CREATE EXTERNAL ACCESS INTEGRATION IF NOT EXISTS GEOCODING_TESTER_CARTO_EAI
    ALLOWED_NETWORK_RULES = (GEOCODING.PUBLIC.GEOCODING_TESTER_CARTO_RULE)
    ENABLED = TRUE COMMENT = '${TRACKING_TAG}';" >/dev/null

cd "$APP_DIR"
snow app deploy -c "$CONN"
snow app open -c "$CONN" --print-only 2>/dev/null | sed 's/^/    app URL: /' || true
