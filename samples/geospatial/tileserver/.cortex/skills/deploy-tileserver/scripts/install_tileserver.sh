#!/usr/bin/env bash
#
# deploy-tileserver / install_tileserver.sh
#
# One-command, idempotent installer for the Snowflake vector-tile demo:
#   arm 1  dynamic ST_AsMVT  (PostGIS function source, live via Martin on SPCS)
#   arm 2  precomputed PMTiles (baked from the same data, mounted into Martin)
#   arm 3  H3 aggregation    (native core Snowflake, client-side deck.gl - export helper)
# The viewer is Martin's built-in Web UI on the public ingress.
#
# Layers (detect-and-reuse-else-create throughout):
#   0 preflight -> 1 postgres+postgis -> 2 spcs infra (+PG secret/EAI)
#   -> 3 data sync -> 4 MVT function -> 5 martin image -> 6 pmtiles bake
#   -> 7 service (+EAIs, web UI) -> 8 verify -> friction log
#
# Usage:
#   bash .cortex/skills/deploy-tileserver/scripts/install_tileserver.sh --connection <conn>
#
# Flags:
#   --connection <name>   REQUIRED. Snow CLI connection.
#   --no-pmtiles          Skip the arm-2 PMTiles bake (arms 1 + 3 still install).
#   --pg-instance <name>  Reuse a specific Postgres instance (else detect/create).
#   --no-create-pg        Never CREATE a Postgres instance (billable); require reuse.
#   --source-table <fqn>  Snowflake source table (default Overture divisions share).
#   --country <cc>        Narrow the dataset (e.g. US) for a small/fast bake.
# Env re-run shortcuts:
#   SKIP_PG SKIP_INFRA SKIP_DATA SKIP_MVT SKIP_IMAGE SKIP_BAKE SKIP_SERVICE (=1)
set -euo pipefail

# ── arg parse ────────────────────────────────────────────────────
CONNECTION=""
WITH_PMTILES=1
ALLOW_CREATE_PG=1
PG_INSTANCE="${PG_INSTANCE:-}"
# Empty by default => auto-detect the source dataset (build_source_snapshot.py
# probes known free polygon datasets: Overture divisions, then CARTO states).
# Pass --source-table to pin one explicitly.
SOURCE_TABLE_ARG="${SOURCE_TABLE:-}"
# Default to a single country (US) rather than the full worldwide dataset: a
# global load + PMTiles bake is impractically slow (hours). Override with
# --country <cc> (or COUNTRY=<cc>); pass --country ALL for the whole world.
COUNTRY_ARG="${COUNTRY:-US}"
while [ $# -gt 0 ]; do
  case "$1" in
    --connection) CONNECTION="${2:-}"; shift 2;;
    --connection=*) CONNECTION="${1#*=}"; shift;;
    --no-pmtiles) WITH_PMTILES=0; shift;;
    --pg-instance) PG_INSTANCE="${2:-}"; shift 2;;
    --no-create-pg) ALLOW_CREATE_PG=0; shift;;
    --source-table) SOURCE_TABLE_ARG="${2:-}"; shift 2;;
    --country) COUNTRY_ARG="${2:-}"; shift 2;;
    *) echo "Unknown arg: $1"; exit 2;;
  esac
done
[ -n "$CONNECTION" ] || { echo "ERROR: --connection <name> is required"; exit 2; }

SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SKILL_DIR="$(cd "$SCRIPTS/.." && pwd)"
REF="$SKILL_DIR/references"
LOG_DIR="$SKILL_DIR/logs"
mkdir -p "$LOG_DIR"
FRICTION_LOG="$LOG_DIR/friction-log_$(date +%Y-%m-%d_%H-%M).md"
START_TS=$(date +%s)
declare -a STEP_STATUS

export SNOWFLAKE_CONNECTION="$CONNECTION"
# Only pin SOURCE_TABLE when the operator passed one; otherwise leave it unset so
# build_source_snapshot.py auto-detects an accessible source.
[ -n "$SOURCE_TABLE_ARG" ] && export SOURCE_TABLE="$SOURCE_TABLE_ARG"
# --country ALL means the full worldwide dataset (no filter); anything else is a
# single-country scope passed through to the exporters/sync as COUNTRY.
if [ -n "$COUNTRY_ARG" ] && [ "$(printf '%s' "$COUNTRY_ARG" | tr '[:lower:]' '[:upper:]')" != "ALL" ]; then
  export COUNTRY="$COUNTRY_ARG"
fi

QTAG='{"origin":"sf_sit-is","name":"oss-deploy-tileserver","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}'

note() { echo "[deploy-tileserver] $*"; }
step() { STEP_STATUS+=("$1|$2"); }
sf()   { snow sql -c "$CONNECTION" -q "ALTER SESSION SET query_tag='$QTAG'; $1"; }
obj_exists() { snow sql -c "$CONNECTION" --format=CSV -q "$1" 2>/dev/null | grep -qiE "$2"; }

# ── 0. preflight ─────────────────────────────────────────────────
note "[0] preflight ..."
for t in snow python3; do
  command -v "$t" >/dev/null 2>&1 || { echo "ERROR: '$t' not found"; exit 1; }
done
CONTAINER_CMD="${CONTAINER_CMD:-$(command -v docker >/dev/null 2>&1 && echo docker || echo podman)}"
command -v "$CONTAINER_CMD" >/dev/null 2>&1 || { echo "ERROR: no container runtime (docker/podman)"; exit 1; }
export CONTAINER_CMD
# Fail fast if the container DAEMON is not actually running. The binary can be on
# PATH while the daemon is stopped (e.g. Docker Desktop not started); without this
# the install would create the billable Postgres instance (step 1) and only crash
# much later at the image build/push (step 5). Check liveness here, before any
# billable infra, with an actionable message.
if ! "$CONTAINER_CMD" info >/dev/null 2>&1; then
  echo "ERROR: container runtime '$CONTAINER_CMD' is installed but its daemon is not running."
  case "$(basename "$CONTAINER_CMD")" in
    *podman*) echo "       Start the Podman machine (e.g. 'podman machine start') and re-run.";;
    *)        echo "       Start Docker (e.g. 'open -a Docker' on macOS, or 'sudo systemctl start docker' on Linux) and re-run.";;
  esac
  exit 1
fi
# Python deps: the installer hard-imports psycopg2, pmtiles, and
# snowflake-connector-python. Verify them and auto-install any that are missing so
# a from-scratch run is frictionless. Set SKIP_DEP_INSTALL=1 to only verify (fail
# fast) instead of installing.
note "[0] verifying Python deps (psycopg2, pmtiles, snowflake-connector) ..."
MISSING="$(python3 - <<'PY'
import importlib.util as u
mods = {"psycopg2": "psycopg2-binary", "pmtiles": "pmtiles",
        "snowflake.connector": "snowflake-connector-python"}
print(" ".join(pkg for mod, pkg in mods.items() if u.find_spec(mod) is None))
PY
)"
if [ -n "$MISSING" ]; then
  if [ "${SKIP_DEP_INSTALL:-0}" = "1" ]; then
    echo "ERROR: missing Python deps: $MISSING (run: pip install $MISSING)"; exit 1
  fi
  note "[0] installing missing Python deps: $MISSING"
  python3 -m pip install --quiet --disable-pip-version-check $MISSING \
    || { echo "ERROR: pip install failed for: $MISSING"; exit 1; }
fi
snow sql -c "$CONNECTION" -q "SELECT CURRENT_ACCOUNT();" >/dev/null 2>&1 \
  || { echo "ERROR: connection '$CONNECTION' does not work"; exit 1; }
# Verify a source dataset is accessible BEFORE creating any billable infra (a fresh
# PG instance costs money). Fails fast with clear guidance if none is reachable.
if [ "${SKIP_DATA:-0}" != "1" ]; then
  note "[0] probing source dataset accessibility ..."
  SRC_PROBE="$(python3 "$SCRIPTS/build_source_snapshot.py" --probe-only 2>&1)" \
    || { echo "$SRC_PROBE"; echo "ERROR: no usable source dataset (see message above)"; exit 1; }
  echo "$SRC_PROBE" | sed -n 's/^SOURCE_/  source /p'
fi
step "0 preflight" OK

# ── 1. postgres + postgis (detect-reuse-else-create) ─────────────
PG_URL=""; PGHOST=""
if [ "${SKIP_PG:-0}" = "1" ]; then
  note "[1] SKIP_PG=1 - expecting PG_URL in the environment"
  PG_URL="${PG_URL:-}"; PGHOST="${PGHOST:-}"
  step "1 postgres" SKIPPED
else
  note "[1] provisioning Postgres + PostGIS ..."
  PROV_ARGS=(--connection "$CONNECTION")
  [ -n "$PG_INSTANCE" ] && PROV_ARGS+=(--pg-instance "$PG_INSTANCE")
  [ "$ALLOW_CREATE_PG" = "1" ] && PROV_ARGS+=(--allow-create)
  PROV_OUT="$(python3 "$SCRIPTS/provision_pg.py" "${PROV_ARGS[@]}")" || { step "1 postgres" FAILED; echo "$PROV_OUT"; exit 1; }
  echo "$PROV_OUT"
  PGHOST="$(printf '%s\n' "$PROV_OUT" | sed -n 's/^PGHOST=//p' | tail -1)"
  PG_URL="$(printf '%s\n' "$PROV_OUT" | sed -n 's/^PG_URL=//p' | tail -1)"
  [ -n "$PG_URL" ] || { echo "ERROR: provision_pg.py did not emit PG_URL"; step "1 postgres" FAILED; exit 1; }
  export PG_URL PGHOST
  step "1 postgres" OK
fi

# ── 2. spcs infra (+ PG secret + PG egress EAI) ──────────────────
if [ "${SKIP_INFRA:-0}" = "1" ]; then
  note "[2] SKIP_INFRA=1"; step "2 infra" SKIPPED
else
  note "[2] provisioning SPCS infra ..."
  snow sql -c "$CONNECTION" -f "$REF/infra.sql" >/tmp/ts_infra.log 2>&1 \
    || { echo "ERROR: infra.sql failed"; tail -30 /tmp/ts_infra.log; step "2 infra" FAILED; exit 1; }
  if [ -n "$PG_URL" ]; then
    sf "CREATE SECRET IF NOT EXISTS TILESERVER.CORE.PG_URL TYPE=GENERIC_STRING SECRET_STRING='$PG_URL' COMMENT='$QTAG';" >/dev/null
    sf "CREATE OR REPLACE NETWORK RULE TILESERVER.CORE.PG_EGRESS MODE=EGRESS TYPE=HOST_PORT VALUE_LIST=('$PGHOST:5432') COMMENT='$QTAG';
        CREATE EXTERNAL ACCESS INTEGRATION IF NOT EXISTS TILESERVER_PG_EAI ALLOWED_NETWORK_RULES=(TILESERVER.CORE.PG_EGRESS) ENABLED=TRUE COMMENT='$QTAG';" >/dev/null
  fi
  step "2 infra" OK
fi

# ── 3. data sync (Snowflake -> PostGIS public.features) ──────────
if [ "${SKIP_DATA:-0}" = "1" ]; then
  note "[3] SKIP_DATA=1"; step "3 data" SKIPPED
else
  note "[3] syncing data to PostGIS ..."
  # Build the owned snapshot first (TILESERVER.CORE.SOURCE_FEATURES) so both the
  # PG load and the later PMTiles bake read a stable, owned copy - immune to a
  # mid-run Marketplace-share lapse. Requires the TILESERVER.CORE schema (step 2).
  python3 "$SCRIPTS/build_source_snapshot.py" || { step "3 data" FAILED; exit 1; }
  python3 "$SCRIPTS/sync_to_pg.py" || { step "3 data" FAILED; exit 1; }
  step "3 data" OK
fi

# ── 4. MVT function source ───────────────────────────────────────
if [ "${SKIP_MVT:-0}" = "1" ]; then
  note "[4] SKIP_MVT=1"; step "4 mvt" SKIPPED
else
  note "[4] creating public.features_mvt ..."
  python3 "$SCRIPTS/pgexec.py" "@$SKILL_DIR/sql/features_mvt.sql" || { step "4 mvt" FAILED; exit 1; }
  step "4 mvt" OK
fi

# ── 5. martin image ──────────────────────────────────────────────
if [ "${SKIP_IMAGE:-0}" = "1" ]; then
  note "[5] SKIP_IMAGE=1"; step "5 image" SKIPPED
else
  note "[5] publishing Martin image ..."
  REPO_URL="$(snow spcs image-repository url TILESERVER.CORE.IMAGES -c "$CONNECTION" 2>/dev/null | tail -1)"
  [ -n "$REPO_URL" ] || { echo "ERROR: could not resolve image repository URL"; step "5 image" FAILED; exit 1; }
  bash "$SCRIPTS/build_push_martin.sh" --connection "$CONNECTION" --repo-url "$REPO_URL" \
    || { step "5 image" FAILED; exit 1; }
  step "5 image" OK
fi

# ── 6. pmtiles bake (default on) ─────────────────────────────────
if [ "$WITH_PMTILES" = "0" ] || [ "${SKIP_BAKE:-0}" = "1" ]; then
  note "[6] PMTiles bake skipped"; step "6 bake" SKIPPED
else
  note "[6] baking PMTiles (arm 2) ..."
  bash "$SCRIPTS/bake_pmtiles.sh" --connection "$CONNECTION" || { step "6 bake" FAILED; exit 1; }
  step "6 bake" OK
fi

# ── 7. martin service (+ EAIs, web UI) ───────────────────────────
INGRESS=""
if [ "${SKIP_SERVICE:-0}" = "1" ]; then
  note "[7] SKIP_SERVICE=1"; step "7 service" SKIPPED
else
  note "[7] deploying Martin service ..."
  SPEC="/tmp/martin_service.yaml"
  sed -e 's#${MARTIN_IMAGE}#/tileserver/core/images/martin:latest#' \
      -e 's#${PG_URL_SECRET}#TILESERVER.CORE.PG_URL#' \
      -e 's#${TILES_STAGE}#TILESERVER.CORE.TILES#' \
      "$SKILL_DIR/spcs/martin_service.yaml.tmpl" > "$SPEC"
  snow stage copy "$SPEC" "@TILESERVER.CORE.SPECS" -c "$CONNECTION" --overwrite >/dev/null

  EAIS="(TILESERVER_PG_EAI, TILESERVER_BASEMAP_EAI)"
  if obj_exists "SHOW SERVICES LIKE 'MARTIN' IN SCHEMA TILESERVER.CORE;" 'MARTIN'; then
    sf "ALTER SERVICE TILESERVER.CORE.MARTIN FROM @TILESERVER.CORE.SPECS SPECIFICATION_FILE='martin_service.yaml';
        ALTER SERVICE TILESERVER.CORE.MARTIN SET EXTERNAL_ACCESS_INTEGRATIONS=$EAIS;" >/dev/null || true
  else
    sf "CREATE SERVICE TILESERVER.CORE.MARTIN
          IN COMPUTE POOL TILESERVER_POOL
          FROM @TILESERVER.CORE.SPECS SPECIFICATION_FILE='martin_service.yaml'
          EXTERNAL_ACCESS_INTEGRATIONS=$EAIS
          COMMENT='$QTAG';" >/dev/null || { step "7 service" FAILED; exit 1; }
  fi
  step "7 service" OK
fi

# ── 8. verify ────────────────────────────────────────────────────
note "[8] verifying ..."
VERIFY_OK=1
# arm 1: dynamic MVT world tile (0,0,0) must be non-empty bytes. The function keeps
# the coarsest admin level at low zoom, so z0 is non-empty for any dataset (whether
# it has countries, regions, or states) that has features.
if [ -n "$PG_URL" ]; then
  MVT0="$(python3 "$SCRIPTS/pgexec.py" "SELECT COALESCE(length(public.features_mvt(0,0,0)),0) AS n" 2>/dev/null | tail -1 || true)"
  case "$MVT0" in ''|*[!0-9]*) MVT0=0;; esac
  note "  arm1 features_mvt(0,0,0) length = ${MVT0}"
  [ "${MVT0:-0}" -gt 0 ] || VERIFY_OK=0
fi
# service: poll until READY/RUNNING (fresh services take a minute to pull + boot).
SVC_STATUS=""
for _i in $(seq 1 30); do
  SVC_STATUS="$(snow sql -c "$CONNECTION" --format=JSON -q "SHOW SERVICES LIKE 'MARTIN' IN SCHEMA TILESERVER.CORE;" 2>/dev/null \
    | python3 -c "import sys,json; rows=json.load(sys.stdin) or []; print(rows[0].get('status','') if rows else '')" 2>/dev/null || true)"
  case "$SVC_STATUS" in READY|RUNNING) break;; esac
  sleep 10
done
note "  Martin service status = ${SVC_STATUS:-<none>}"
case "$SVC_STATUS" in READY|RUNNING) : ;; *) VERIFY_OK=0;; esac
# both EAIs must be attached to the service
EAI_CNT="$(snow sql -c "$CONNECTION" --format=JSON -q "DESCRIBE SERVICE TILESERVER.CORE.MARTIN;" 2>/dev/null \
  | python3 -c "import sys,json;
rows=json.load(sys.stdin) or []
row=rows[0] if rows else {}
val=str(row.get('external_access_integrations','')).upper()
print(sum(x in val for x in ('TILESERVER_PG_EAI','TILESERVER_BASEMAP_EAI')))" 2>/dev/null || echo 0)"
note "  EAIs attached to service = ${EAI_CNT}/2"
[ "${EAI_CNT:-0}" = "2" ] || VERIFY_OK=0
# arm 2: valid PMTiles archive on the TILES stage (only if we baked)
if [ "$WITH_PMTILES" = "1" ] && [ "${SKIP_BAKE:-0}" != "1" ]; then
  PMT_PRESENT="$(snow sql -c "$CONNECTION" --format=JSON -q "LIST @TILESERVER.CORE.TILES PATTERN='.*features_pmt[.]pmtiles';" 2>/dev/null \
    | python3 -c "import sys,json; rows=json.load(sys.stdin) or []; print(len(rows))" 2>/dev/null || echo 0)"
  note "  arm2 features_pmt.pmtiles on TILES stage = ${PMT_PRESENT} file(s)"
  [ "${PMT_PRESENT:-0}" -ge 1 ] || VERIFY_OK=0
fi
INGRESS=""
# The public endpoint URL is provisioned a few minutes AFTER the service reaches
# RUNNING (SHOW ENDPOINTS first returns a "provisioning in progress" placeholder),
# so poll until a real *.snowflakecomputing.app host appears (up to ~10 min).
for _i in $(seq 1 40); do
  INGRESS="$(snow sql -c "$CONNECTION" --format=CSV -q "SHOW ENDPOINTS IN SERVICE TILESERVER.CORE.MARTIN;" 2>/dev/null \
    | grep -oiE '[a-z0-9-]+\.snowflakecomputing\.app' | head -1 || true)"
  [ -n "$INGRESS" ] && break
  sleep 15
done
[ -n "$INGRESS" ] && INGRESS="https://$INGRESS"
note "  public ingress = ${INGRESS:-<not resolved>}"
[ -n "$INGRESS" ] || VERIFY_OK=0
if [ "$VERIFY_OK" = "1" ]; then step "8 verify" OK; else step "8 verify" FAILED; fi

# ── friction log + summary ───────────────────────────────────────
ELAPSED=$(( $(date +%s) - START_TS ))
{
  echo "# deploy-tileserver friction log"
  echo ""
  echo "- date: $(date)"
  echo "- connection: $CONNECTION"
  echo "- elapsed: ${ELAPSED}s"
  echo "- pmtiles: $([ "$WITH_PMTILES" = 1 ] && echo baked || echo skipped)"
  echo "- pg host: ${PGHOST:-<reused/env>}"
  echo "- ingress: ${INGRESS:-<not resolved>}"
  echo ""
  echo "## steps"
  for s in "${STEP_STATUS[@]}"; do echo "- ${s%|*}: ${s#*|}"; done
} > "$FRICTION_LOG"

note "done in ${ELAPSED}s. friction log: $FRICTION_LOG"
[ -n "$INGRESS" ] && note "Martin Web UI (viewer) + tiles: $INGRESS"
note "sources to expect at the ingress: /catalog, /features_mvt/{z}/{x}/{y}, /features_pmt/{z}/{x}/{y}"
