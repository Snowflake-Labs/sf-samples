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
#   bash install_geocoding.sh --connection <conn> [--warehouse <wh>] [--intl | --eu]
#
#   --connection   (required) Snowflake CLI connection name (`snow connection list`)
#   --warehouse    (optional) warehouse to USE for the session (MEDIUM recommended)
#   --intl         (optional) also build+push the libpostal image to THIS account's
#                  own image repository and deploy the SPCS intl service. Heavy:
#                  requires Docker or Podman and an amd64 image build (slow on ARM).
#   --eu           (optional) everything --intl does, plus European forward
#                  geocoding: builds the EU candidate tables (BUILD_EU_INDEX,
#                  ~19 GB, one-time; use an X-Large warehouse via --eu-warehouse).
#   --eu-warehouse (optional) warehouse for the one-time EU index build
#                  (defaults to --warehouse / session default).
# ============================================================================
set -euo pipefail

CONN=""
WAREHOUSE=""
INTL=0
EU=0
EU_WAREHOUSE=""

usage() { grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --connection|-c) CONN="${2:-}"; shift 2 ;;
    --warehouse|-w)  WAREHOUSE="${2:-}"; shift 2 ;;
    --intl)          INTL=1; shift ;;
    --eu)            INTL=1; EU=1; shift ;;
    --eu-warehouse)  EU_WAREHOUSE="${2:-}"; shift 2 ;;
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
snow sql -c "$CONN" --silent --format csv -q "SELECT 'connected: ' || CURRENT_ACCOUNT() || ' as ' || CURRENT_ROLE() AS c;" \
  | tail -n +2 | sed 's/^/    /' \
  || { echo "ERROR: connection '$CONN' is not usable." >&2; exit 1; }

# ----------------------------------------------------------------------------
# Core install — single session so QUERY_TAG applies across all modules.
# ----------------------------------------------------------------------------
MODULES=(
  "$SQL_DIR/00_setup.sql"
  "$SQL_DIR/udfs/parse_address.sql"
  "$SQL_DIR/udfs/standardize_street.sql"
  "$SQL_DIR/udfs/normalize_street_eu.sql"
  "$SQL_DIR/eu/eu_index.sql"
  "$SQL_DIR/procedures/forward_geocode_table.sql"
  "$SQL_DIR/procedures/reverse_geocode_table.sql"
  "$SQL_DIR/procedures/evaluate_geocode.sql"
)

for m in "${MODULES[@]}"; do
  [[ -f "$m" ]] || { echo "ERROR: missing SQL module: $m" >&2; exit 1; }
done

TMP_SQL="$(mktemp -t install_geocoding.XXXXXX.sql)"
cleanup() { rm -f "$TMP_SQL" "${PUSH_LOG:-}"; }
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

echo "==> Deploying core geocoding objects (GEOCODING.PUBLIC, ${#MODULES[@]} modules)"
# --silent drops the statement echo (about 1,500 lines of SQL source). The
# output is shown in full only when something fails.
if ! CORE_OUT="$(snow sql -c "$CONN" --silent --format csv -f "$TMP_SQL" 2>&1)"; then
  echo "$CORE_OUT" >&2
  echo "ERROR: core install failed (output above)." >&2
  exit 1
fi
# Report only the setup messages (listing, share resolution, row count).
echo "$CORE_OUT" | grep -E '^"?(Overture |Could not|OVERTURE_ADDRESS view|ERROR)' \
  | tr -d '"' | sed 's/^/    /'

# ----------------------------------------------------------------------------
# Optional international path — libpostal on SPCS.
# ----------------------------------------------------------------------------
if [[ "$INTL" -eq 1 ]]; then
  echo "==> International (libpostal SPCS) path"
  CONTAINER_CMD=""
  # Use the first runtime whose daemon / VM actually answers, not just the
  # first binary on PATH.
  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then CONTAINER_CMD="docker"
  elif command -v podman >/dev/null 2>&1 && podman info >/dev/null 2>&1; then CONTAINER_CMD="podman"
  else
    echo "ERROR: --intl needs a running Docker daemon or Podman machine to build the libpostal image." >&2
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

  # The image is about 3.5 GB (libpostal models). Uploads of the large layers
  # often end with "timeout awaiting response headers"; a retry skips layers
  # the registry already has, so a few attempts converge. Print a heartbeat,
  # since a non-TTY push is silent for many minutes.
  IMAGE="${REPO_URL}/libpostal:latest"
  PUSH_LOG="$(mktemp -t install_geocoding_push.XXXXXX)"
  PUSHED=0
  for attempt in 1 2 3 4 5; do
    echo "    pushing image, attempt ${attempt}/5 (the first push can take 10-45 min)"
    "$CONTAINER_CMD" push "$IMAGE" > "$PUSH_LOG" 2>&1 &
    PUSH_PID=$!
    T0=$SECONDS
    while kill -0 "$PUSH_PID" 2>/dev/null; do
      sleep 60
      kill -0 "$PUSH_PID" 2>/dev/null && echo "      still pushing ($(( (SECONDS - T0) / 60 )) min)"
    done
    if wait "$PUSH_PID"; then PUSHED=1; break; fi
    echo "      attempt ${attempt} failed: $(grep -oE 'timeout[^"]*|denied[^"]*|unauthorized[^"]*' "$PUSH_LOG" | tail -1)"
  done
  if [[ "$PUSHED" -ne 1 ]]; then
    cat "$PUSH_LOG" >&2; rm -f "$PUSH_LOG"
    echo "ERROR: image push failed 5 times (log above). Re-run the installer to resume." >&2
    exit 1
  fi
  rm -f "$PUSH_LOG"
  echo "    image pushed"

  echo "    deploying SPCS service + intl functions"
  snow sql -c "$CONN" -q "ALTER SESSION SET QUERY_TAG='oss-geocoding';"
  snow sql -c "$CONN" -f "$LIBPOSTAL_DIR/deploy.sql"

  echo "    NOTE: poll SYSTEM\$GET_SERVICE_STATUS('GEOCODING.PUBLIC.LIBPOSTAL_SVC') until READY."
fi

# ----------------------------------------------------------------------------
# Optional European forward geocoding — one-time candidate index build.
# Idempotent: BUILD_EU_INDEX(FALSE) only builds tables that do not exist yet.
# ----------------------------------------------------------------------------
if [[ "$EU" -eq 1 ]]; then
  echo "==> European forward geocoding index (BUILD_EU_INDEX)"
  EU_WH="${EU_WAREHOUSE:-$WAREHOUSE}"
  snow sql -c "$CONN" -q "ALTER SESSION SET QUERY_TAG='oss-geocoding';
    $( [[ -n "$EU_WH" ]] && echo "USE WAREHOUSE ${EU_WH};" )
    ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 14400;
    CALL GEOCODING.PUBLIC.BUILD_EU_INDEX(FALSE);"
fi

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
echo
echo "==> Done. Deployed objects (COMMENT tag: oss-geocoding):"
snow sql -c "$CONN" --silent --format csv -q "
  SHOW OBJECTS IN SCHEMA GEOCODING.PUBLIC
  ->> SELECT \"kind\" || ': ' || LISTAGG(\"name\", ', ') WITHIN GROUP (ORDER BY \"name\") AS objects
      FROM \$1 GROUP BY \"kind\" ORDER BY \"kind\";
  SHOW USER FUNCTIONS IN SCHEMA GEOCODING.PUBLIC
  ->> SELECT 'FUNCTION: ' || LISTAGG(DISTINCT \"name\", ', ') WITHIN GROUP (ORDER BY \"name\") AS objects FROM \$1;
  SHOW USER PROCEDURES IN SCHEMA GEOCODING.PUBLIC
  ->> SELECT 'PROCEDURE: ' || LISTAGG(DISTINCT \"name\", ', ') WITHIN GROUP (ORDER BY \"name\") AS objects FROM \$1;" \
  | grep -v -e '^OBJECTS$' -e '^[[:space:]]*$' | tr -d '"' | sed 's/^/    /' || true

cat <<EOF

Geocoding is installed. Try it:
  CALL GEOCODING.PUBLIC.FORWARD_GEOCODE_TABLE('MY_DB.PUBLIC.PERMITS','ADDRESS_TEXT','PERMIT_ID','MY_DB.PUBLIC.PERMITS_GEOCODED');
  CALL GEOCODING.PUBLIC.REVERSE_GEOCODE_TABLE('MY_DB.PUBLIC.PINGS','LAT','LON','DEVICE_ID',200,'MY_DB.PUBLIC.PINGS_ADDRESSED','US');

Smoke tests: $SQL_DIR/examples/
EOF
