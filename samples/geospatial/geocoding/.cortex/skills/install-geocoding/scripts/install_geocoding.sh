#!/usr/bin/env bash
# ============================================================================
# install_geocoding.sh — one-command installer for Snowflake-native geocoding
# ============================================================================
# Idempotent orchestrator that deploys the GEOCODING.PUBLIC objects (Overture
# share view, UDFs, procedures) and, optionally, the libpostal SPCS service for
# international geocoding. Re-runnable: every module uses CREATE OR REPLACE /
# IF NOT EXISTS, so a second run detects-and-reuses.
#
# Every object created is tagged with COMMENT = 'oss-geocoding' and the session
# sets QUERY_TAG = 'oss-geocoding', so a future cleanup can discover them.
#
# Usage:
#   bash install_geocoding.sh --connection <conn> [--warehouse <wh>] [--intl]
#
#   --connection   (required) Snowflake CLI connection name (`snow connection list`)
#   --warehouse    (optional) warehouse to USE for the session (MEDIUM recommended)
#   --intl         (optional) also build+push the libpostal image to THIS account's
#                  own image repository and deploy the SPCS intl service. Heavy:
#                  requires Docker or Podman and an amd64 image build (slow on ARM).
# ============================================================================
set -euo pipefail

CONN=""
WAREHOUSE=""
INTL=0

usage() { grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --connection|-c) CONN="${2:-}"; shift 2 ;;
    --warehouse|-w)  WAREHOUSE="${2:-}"; shift 2 ;;
    --intl)          INTL=1; shift ;;
    -h|--help)       usage 0 ;;
    *) echo "Unknown argument: $1" >&2; usage 1 ;;
  esac
done

if [[ -z "$CONN" ]]; then
  echo "ERROR: --connection is required. See: snow connection list" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GEO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"   # samples/geospatial/geocoding
SQL_DIR="$GEO_ROOT/sql"
LIBPOSTAL_DIR="$GEO_ROOT/libpostal_service"

export SNOWFLAKE_CLI_NO_UPDATE_CHECK="${SNOWFLAKE_CLI_NO_UPDATE_CHECK:-true}"

echo "==> Preflight"
command -v snow >/dev/null 2>&1 || { echo "ERROR: Snowflake CLI (snow) not found." >&2; exit 1; }
snow sql -c "$CONN" -q "SELECT CURRENT_ACCOUNT() AS account, CURRENT_ROLE() AS role;" \
  || { echo "ERROR: connection '$CONN' is not usable." >&2; exit 1; }

# ----------------------------------------------------------------------------
# Core install — single session so QUERY_TAG applies across all modules.
# ----------------------------------------------------------------------------
MODULES=(
  "$SQL_DIR/00_setup.sql"
  "$SQL_DIR/udfs/parse_address.sql"
  "$SQL_DIR/udfs/standardize_street.sql"
  "$SQL_DIR/procedures/forward_geocode_table.sql"
  "$SQL_DIR/procedures/reverse_geocode_table.sql"
)

for m in "${MODULES[@]}"; do
  [[ -f "$m" ]] || { echo "ERROR: missing SQL module: $m" >&2; exit 1; }
done

TMP_SQL="$(mktemp -t install_geocoding.XXXXXX.sql)"
cleanup() { rm -f "$TMP_SQL"; }
trap cleanup EXIT

{
  echo "ALTER SESSION SET QUERY_TAG = 'oss-geocoding';"
  [[ -n "$WAREHOUSE" ]] && echo "USE WAREHOUSE ${WAREHOUSE};"
  for m in "${MODULES[@]}"; do
    echo "-- ==== module: ${m#$GEO_ROOT/} ===="
    cat "$m"
    echo
  done
} > "$TMP_SQL"

echo "==> Deploying core geocoding objects (GEOCODING.PUBLIC)"
snow sql -c "$CONN" -f "$TMP_SQL"

# ----------------------------------------------------------------------------
# Optional international path — libpostal on SPCS.
# ----------------------------------------------------------------------------
if [[ "$INTL" -eq 1 ]]; then
  echo "==> International (libpostal SPCS) path"
  CONTAINER_CMD=""
  if command -v docker >/dev/null 2>&1; then CONTAINER_CMD="docker"
  elif command -v podman >/dev/null 2>&1; then CONTAINER_CMD="podman"
  else
    echo "ERROR: --intl needs Docker or Podman to build the libpostal image." >&2
    exit 1
  fi
  echo "    using container runtime: $CONTAINER_CMD"

  echo "    ensuring image repository exists"
  snow sql -c "$CONN" -q "USE DATABASE GEOCODING; USE SCHEMA PUBLIC;
    CREATE IMAGE REPOSITORY IF NOT EXISTS GEOCODING.PUBLIC.IMAGE_REPOSITORY COMMENT='oss-geocoding';"

  REPO_URL="$(snow spcs image-repository url GEOCODING.PUBLIC.IMAGE_REPOSITORY -c "$CONN" 2>/dev/null | tr -d '[:space:]')"
  if [[ -z "$REPO_URL" ]]; then
    echo "ERROR: could not resolve image repository URL. Check privileges / that SPCS is enabled." >&2
    exit 1
  fi
  echo "    repository: $REPO_URL"

  echo "    building linux/amd64 image (this can take a while on ARM)"
  "$CONTAINER_CMD" build --platform linux/amd64 -t "${REPO_URL}/libpostal:latest" "$LIBPOSTAL_DIR"

  echo "    logging in to the SPCS image registry"
  snow spcs image-registry login -c "$CONN"

  echo "    pushing image"
  "$CONTAINER_CMD" push "${REPO_URL}/libpostal:latest"

  echo "    deploying SPCS service + intl functions"
  snow sql -c "$CONN" -q "ALTER SESSION SET QUERY_TAG='oss-geocoding';"
  snow sql -c "$CONN" -f "$LIBPOSTAL_DIR/deploy.sql"

  echo "    NOTE: poll SYSTEM\$GET_SERVICE_STATUS('GEOCODING.PUBLIC.LIBPOSTAL_SVC') until READY."
fi

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
echo
echo "==> Done. Deployed objects (COMMENT tag: oss-geocoding):"
snow sql -c "$CONN" -q "
  SHOW OBJECTS LIKE '%' IN SCHEMA GEOCODING.PUBLIC;
  SELECT \"name\", \"kind\" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) ORDER BY \"kind\", \"name\";" || true

cat <<EOF

Geocoding is installed. Try it:
  CALL GEOCODING.PUBLIC.FORWARD_GEOCODE_TABLE('MY_DB.PUBLIC.PERMITS','ADDRESS_TEXT','PERMIT_ID','MY_DB.PUBLIC.PERMITS_GEOCODED');
  CALL GEOCODING.PUBLIC.REVERSE_GEOCODE_TABLE('MY_DB.PUBLIC.PINGS','LAT','LON','DEVICE_ID',200,'MY_DB.PUBLIC.PINGS_ADDRESSED','US');

Smoke tests: $SQL_DIR/examples/
EOF
