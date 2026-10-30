#!/usr/bin/env bash
# ============================================================================
# install_geocoding.sh — one-command installer for Snowflake-native geocoding
# ============================================================================
# Idempotent orchestrator that deploys the GEOCODING.PUBLIC objects (Overture
# share view, UDFs, procedures), the libpostal SPCS service for international
# geocoding, the European candidate index and the geocoding tester app. The
# last three can be skipped with --no-intl / --no-eu / --no-app. Re-runnable: every module uses CREATE OR REPLACE /
# IF NOT EXISTS, so a second run detects-and-reuses.
#
# Every object created carries a JSON tracking COMMENT and the session sets a
# matching JSON QUERY_TAG (origin "sf_sit-is-geocoding", see "Tracking" in
# README.md). To pause or remove them, see "Pause or remove" in README.md.
#
# Usage:
#   bash install_geocoding.sh --connection <conn> [--warehouse <wh>] [--eu-warehouse <wh>]
#                             [--no-intl | --no-eu] [--no-app]
#
#   --connection   (required) Snowflake CLI connection name (`snow connection list`)
#   --warehouse    (optional) warehouse to USE for the session (MEDIUM recommended)
#   --eu-warehouse (optional) warehouse for the one-time EU index build
#                  (defaults to --warehouse / session default; X-Large recommended).
#   --no-intl      skip the libpostal SPCS service (needs Docker or Podman and an
#                  amd64 image build) and, with it, the EU index.
#   --no-eu        keep libpostal but skip the European index build (~20 GB).
#   --no-app       skip deploying the GEOCODING_TESTER app.
#   --intl, --eu   accepted for backward compatibility; now the default.
# ============================================================================
set -euo pipefail

CONN=""
WAREHOUSE=""
INTL=1
EU=1
APP=1
EU_WAREHOUSE=""

usage() { grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --connection|-c) CONN="${2:-}"; shift 2 ;;
    --warehouse|-w)  WAREHOUSE="${2:-}"; shift 2 ;;
    --intl|--eu)     shift ;;
    --no-intl)       INTL=0; EU=0; shift ;;
    --no-eu)         EU=0; shift ;;
    --no-app)        APP=0; shift ;;
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
APP_DIR="$GEO_ROOT/geocoding_tester"

# Tracking tag shared by the session QUERY_TAG and object COMMENTs. Contains no
# single quotes, so it can be embedded in a '...' SQL literal as is.
TRACKING_TAG='{"origin":"sf_sit-is-geocoding","name":"oss-geocoding","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}'

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
  echo "ALTER SESSION SET QUERY_TAG = '${TRACKING_TAG}';"
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
# International path (default; --no-intl skips) — libpostal on SPCS.
# ----------------------------------------------------------------------------
if [[ "$INTL" -eq 1 ]]; then
  echo "==> International (libpostal SPCS) path"
  CONTAINER_CMD=""
  # Use the first runtime whose daemon / VM actually answers, not just the
  # first binary on PATH.
  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then CONTAINER_CMD="docker"
  elif command -v podman >/dev/null 2>&1 && podman info >/dev/null 2>&1; then CONTAINER_CMD="podman"
  else
    echo "ERROR: the libpostal service needs a running Docker daemon or Podman machine to build its image." >&2
    echo "       Start one and re-run, or pass --no-intl for a US-only install." >&2
    exit 1
  fi
  echo "    using container runtime: $CONTAINER_CMD"

  echo "    ensuring image repository exists"
  snow sql -c "$CONN" --silent -q "CREATE IMAGE REPOSITORY IF NOT EXISTS GEOCODING.PUBLIC.IMAGE_REPOSITORY COMMENT='${TRACKING_TAG}';" >/dev/null

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

  # The image is about 3.5 GB (libpostal models). A push uploads several layers
  # in parallel, and on a slow uplink a large upload often ends with "timeout
  # awaiting response headers". The Dockerfile therefore cuts the models into
  # 200 MB parts, one build stage each; pushing the stages in order uploads a
  # single new layer per push. A retry skips layers the registry already has.
  # A non-TTY push prints nothing for minutes, so print a heartbeat.
  PUSH_LOG="$(mktemp -t install_geocoding_push.XXXXXX)"
  push_with_retry() {
    local image="$1" attempt
    for attempt in 1 2 3 4 5 6 7 8; do
      "$CONTAINER_CMD" push "$image" > "$PUSH_LOG" 2>&1 &
      local pid=$! t0=$SECONDS
      while kill -0 "$pid" 2>/dev/null; do
        sleep 60
        kill -0 "$pid" 2>/dev/null && echo "        still pushing ${image##*:} ($(( (SECONDS - t0) / 60 )) min)"
      done
      wait "$pid" && return 0
      echo "        attempt ${attempt}/8 failed: $(grep -oE 'timeout[^"]*|denied[^"]*|unauthorized[^"]*' "$PUSH_LOG" | tail -1)"
    done
    cat "$PUSH_LOG" >&2
    return 1
  }
  STAGES=(part-base part-0 part-1 part-2 part-3 part-4 part-5 part-6 part-7)
  N=$(( ${#STAGES[@]} + 1 ))
  i=0
  for stage in "${STAGES[@]}" latest; do
    i=$(( i + 1 ))
    if [[ "$stage" != latest ]]; then
      "$CONTAINER_CMD" build -q --platform linux/amd64 --target "$stage" \
        -t "${REPO_URL}/libpostal:${stage}" "$LIBPOSTAL_DIR" >/dev/null
    fi
    echo "    pushing image, step ${i}/${N} (${stage})"
    push_with_retry "${REPO_URL}/libpostal:${stage}" || {
      rm -f "$PUSH_LOG"
      echo "ERROR: image push failed (log above). Re-run the installer to resume; uploaded layers are kept." >&2
      exit 1
    }
  done
  rm -f "$PUSH_LOG"
  echo "    image pushed"

  echo "    deploying SPCS service + intl functions"
  # A re-run may find the pool / service suspended (e.g. after cost control).
  # RESUME errors when already running; that is fine.
  snow sql -c "$CONN" --silent -q "ALTER COMPUTE POOL IF EXISTS GEOCODING_LIBPOSTAL_POOL RESUME;" >/dev/null 2>&1 || true
  snow sql -c "$CONN" --silent -q "ALTER SERVICE IF EXISTS GEOCODING.PUBLIC.LIBPOSTAL_SVC RESUME;" >/dev/null 2>&1 || true
  # One session, so the QUERY_TAG applies to the deploy statements.
  { echo "ALTER SESSION SET QUERY_TAG = '${TRACKING_TAG}';"; cat "$LIBPOSTAL_DIR/deploy.sql"; } > "$TMP_SQL"
  if ! DEPLOY_OUT="$(snow sql -c "$CONN" --silent --format csv -f "$TMP_SQL" 2>&1)"; then
    echo "$DEPLOY_OUT" >&2
    echo "ERROR: SPCS deploy failed (output above)." >&2
    exit 1
  fi

  echo "    waiting for LIBPOSTAL_SVC to be READY (usually 2-8 min)"
  T0=$SECONDS
  SVC_STATUS=""
  while (( SECONDS - T0 < 1200 )); do
    SVC_STATUS="$(snow sql -c "$CONN" --silent --format csv -q \
      "SELECT PARSE_JSON(SYSTEM\$GET_SERVICE_STATUS('GEOCODING.PUBLIC.LIBPOSTAL_SVC'))[0]:status::STRING AS s;" \
      2>/dev/null | tail -1 | tr -d '"[:space:]')"
    [[ "$SVC_STATUS" == "READY" ]] && break
    echo "      ${SVC_STATUS:-PENDING} ($(( (SECONDS - T0) / 60 )) min)"
    sleep 30
  done
  if [[ "$SVC_STATUS" != "READY" ]]; then
    echo "ERROR: LIBPOSTAL_SVC not READY after 20 min (status: ${SVC_STATUS:-unknown}). Logs:" >&2
    snow sql -c "$CONN" -q "SELECT SYSTEM\$GET_SERVICE_LOGS('GEOCODING.PUBLIC.LIBPOSTAL_SVC', 0, 'libpostal', 50);" >&2 || true
    exit 1
  fi
  echo "    LIBPOSTAL_SVC is READY"
  snow sql -c "$CONN" --silent --format csv -q \
    "SELECT 'smoke test: ' || GEOCODING.PUBLIC.PARSE_ADDRESS_INTL('Rue de Rivoli 10, 75001 Paris'):road::STRING AS t;" \
    | tail -1 | sed 's/^/    /'
fi

# ----------------------------------------------------------------------------
# European forward geocoding (default; --no-eu skips) — one-time candidate index build.
# Idempotent: BUILD_EU_INDEX(FALSE) only builds tables that do not exist yet.
# ----------------------------------------------------------------------------
if [[ "$EU" -eq 1 ]]; then
  echo "==> European forward geocoding index (BUILD_EU_INDEX)"
  # The locality polygons come from the free Overture Maps "Divisions" listing
  # (Carto). Acquire it the same way 00_setup.sql acquires Addresses.
  echo "    ensuring the Overture Maps Divisions listing (GZT0Z4CM1E9M9)"
  snow sql -c "$CONN" --silent --format csv -q "
    CALL SYSTEM\$ACCEPT_LEGAL_TERMS('DATA_EXCHANGE_LISTING', 'GZT0Z4CM1E9M9');
    CREATE DATABASE IF NOT EXISTS OVERTURE_MAPS__DIVISIONS FROM LISTING GZT0Z4CM1E9M9;" >/dev/null \
    || { echo "ERROR: could not acquire the Overture Maps - Divisions listing. Get it from the Marketplace as OVERTURE_MAPS__DIVISIONS and re-run." >&2; exit 1; }
  EU_WH="${EU_WAREHOUSE:-$WAREHOUSE}"
  [[ -z "$EU_WAREHOUSE" ]] && echo "    note: no --eu-warehouse given; the first build can take hours below X-Large"
  echo "    building the index if missing (first build about 2-5 min on X-Large)"
  EU_OUT="$(snow sql -c "$CONN" --silent --format csv -q "ALTER SESSION SET QUERY_TAG = '${TRACKING_TAG}';
    $( [[ -n "$EU_WH" ]] && echo "USE WAREHOUSE ${EU_WH};" )
    ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 14400;
    CALL GEOCODING.PUBLIC.BUILD_EU_INDEX(FALSE);" | grep -E '^"?(EU index|ERROR)' | tr -d '"')"
  echo "    $EU_OUT"
  if [[ "$EU_OUT" == ERROR* ]]; then exit 1; fi
fi

# ----------------------------------------------------------------------------
# Tester app (default; --no-app skips) — Snowflake App Runtime.
# ----------------------------------------------------------------------------
if [[ "$APP" -eq 1 ]]; then
  echo "==> GEOCODING_TESTER app (Snowflake App Runtime)"
  bash "$APP_DIR/deploy.sh" --connection "$CONN"
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

APP_URL=""
if [[ "$APP" -eq 1 ]]; then
  APP_URL="$(cd "$APP_DIR" && snow app open -c "$CONN" --print-only 2>/dev/null | grep -oE 'https://[^[:space:]]+' | head -1 || true)"
fi

cat <<EOF

Geocoding is installed.
${APP_URL:+
  Function tester: ${APP_URL}
}
  Documentation:   $GEO_ROOT/README.md
                   (sections: Function tester, Batch processing, Benchmarks)

Try it:
  CALL GEOCODING.PUBLIC.FORWARD_GEOCODE_TABLE('MY_DB.PUBLIC.PERMITS','ADDRESS_TEXT','PERMIT_ID','MY_DB.PUBLIC.PERMITS_GEOCODED');
  CALL GEOCODING.PUBLIC.REVERSE_GEOCODE_TABLE('MY_DB.PUBLIC.PINGS','LAT','LON','DEVICE_ID',200,'MY_DB.PUBLIC.PINGS_ADDRESSED','US');

Smoke tests: $SQL_DIR/examples/
EOF
