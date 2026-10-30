#!/usr/bin/env bash
# ============================================================================
# uninstall_geocoding.sh — remove what install_geocoding.sh created
# ============================================================================
# DRY RUN BY DEFAULT: lists what exists and the statements it would run.
# Pass --yes to execute. Drops in dependency-safe order (app, its integration,
# service, compute pool, database), then optional shared objects.
#
# Usage:
#   bash uninstall_geocoding.sh --connection <conn> [--yes]
#        [--keep-eu-index] [--drop-shares] [--keep-warehouses]
#
#   --keep-eu-index    keep the GEOCODING database and its ~19.7 GB EU index
#                      tables; drop only the app, service and compute pool
#   --drop-shares      also drop the OVERTURE_MAPS__ADDRESSES / __DIVISIONS
#                      Marketplace databases (kept by default: other projects
#                      may use them; --keep-shares is accepted as a no-op)
#   --keep-warehouses  keep GEOCODING_BUILD_WH / GEOCODING_XS_WH (default: they
#                      are dropped only if they carry the geocoding tracking tag)
# ============================================================================
set -uo pipefail

CONN=""
YES=0
KEEP_EU=0
DROP_SHARES=0
KEEP_WH=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --connection|-c)   CONN="${2:-}"; shift 2 ;;
    --yes|-y)          YES=1; shift ;;
    --keep-eu-index)   KEEP_EU=1; shift ;;
    --keep-shares)     DROP_SHARES=0; shift ;;
    --drop-shares)     DROP_SHARES=1; shift ;;
    --keep-warehouses) KEEP_WH=1; shift ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done
[[ -n "$CONN" ]] || { echo "ERROR: --connection is required" >&2; exit 1; }
export SNOWFLAKE_CLI_NO_UPDATE_CHECK="${SNOWFLAKE_CLI_NO_UPDATE_CHECK:-true}"
TAG='sf_sit-is-geocoding'

q() { snow sql -c "$CONN" --silent --format csv -q "$1" 2>/dev/null | tail -n +2 | tr -d '"'; }
exists() { [[ -n "$(q "$1")" ]]; }

PLAN=()
add() { PLAN+=("$1|$2"); }   # statement | description

echo "==> Geocoding uninstall ($( ((YES)) && echo 'EXECUTE' || echo 'dry run; pass --yes to execute' ))"

HAS_DB=0; exists "SHOW DATABASES LIKE 'GEOCODING'" && HAS_DB=1

if (( HAS_DB )); then
  exists "SHOW APPLICATION SERVICES LIKE 'GEOCODING_TESTER' IN SCHEMA GEOCODING.PUBLIC" \
    && add "DROP APPLICATION SERVICE IF EXISTS GEOCODING.PUBLIC.GEOCODING_TESTER" "tester app"
  exists "SHOW SERVICES LIKE 'LIBPOSTAL_SVC' IN SCHEMA GEOCODING.PUBLIC" \
    && add "DROP SERVICE IF EXISTS GEOCODING.PUBLIC.LIBPOSTAL_SVC" "libpostal service"
fi
exists "SHOW INTEGRATIONS LIKE 'GEOCODING_TESTER_CARTO_EAI'" \
  && add "DROP EXTERNAL ACCESS INTEGRATION IF EXISTS GEOCODING_TESTER_CARTO_EAI" "tester basemap integration"
exists "SHOW COMPUTE POOLS LIKE 'GEOCODING_LIBPOSTAL_POOL'" \
  && add "DROP COMPUTE POOL IF EXISTS GEOCODING_LIBPOSTAL_POOL" "libpostal compute pool (after the service)"
# Opt-in guardrails from references/cost-guardrails.sql, if they were applied.
exists "SHOW INTEGRATIONS LIKE 'GEOCODING_BUDGET_EMAIL'" \
  && add "DROP NOTIFICATION INTEGRATION IF EXISTS GEOCODING_BUDGET_EMAIL" "budget email integration"
exists "SHOW RESOURCE MONITORS LIKE 'GEOCODING_WH_MONITOR'" \
  && add "DROP RESOURCE MONITOR IF EXISTS GEOCODING_WH_MONITOR" "warehouse resource monitor"

if (( HAS_DB )); then
  if (( KEEP_EU )); then
    add "DROP NETWORK RULE IF EXISTS GEOCODING.PUBLIC.GEOCODING_TESTER_CARTO_RULE" "tester basemap network rule"
    add "DROP SCHEMA IF EXISTS GEOCODING.TESTER" "tester scratch schema (EU index kept)"
  else
    add "DROP DATABASE IF EXISTS GEOCODING" "GEOCODING database: functions, procedures, EU index (~19.7 GB), image repository"
  fi
fi

if (( DROP_SHARES )); then
  for db in OVERTURE_MAPS__ADDRESSES DS_OVERTURE_MAPS__ADDRESSES OVERTURE_MAPS__DIVISIONS; do
    exists "SHOW DATABASES LIKE '$db'" && add "DROP DATABASE IF EXISTS $db" "Marketplace database $db (other projects may use it)"
  done
fi

if (( ! KEEP_WH )); then
  for wh in GEOCODING_BUILD_WH GEOCODING_XS_WH; do
    # Only warehouses this project created (tracking tag in the COMMENT).
    if [[ -n "$(q "SHOW WAREHOUSES LIKE '$wh' ->> SELECT \"name\" FROM \$1 WHERE \"comment\" LIKE '%$TAG%'")" ]]; then
      add "DROP WAREHOUSE IF EXISTS $wh" "warehouse $wh"
    fi
  done
fi

if (( ${#PLAN[@]} == 0 )); then
  echo "    nothing to remove"
  exit 0
fi

echo "    in this order:"
i=0
for p in "${PLAN[@]}"; do
  i=$((i + 1))
  printf '    %d. %-55s -- %s\n' "$i" "${p%%|*};" "${p#*|}"
done

if (( ! YES )); then
  echo
  echo "Dry run only. Re-run with --yes to execute."
  exit 0
fi

echo
FAILED=0
for p in "${PLAN[@]}"; do
  stmt="${p%%|*}"
  if out="$(snow sql -c "$CONN" --silent -q "$stmt;" 2>&1)"; then
    echo "    done: $stmt"
  else
    echo "    FAILED: $stmt" >&2
    echo "$out" | sed 's/^/      /' >&2
    FAILED=$((FAILED + 1))
  fi
done
echo
(( FAILED == 0 )) && echo "Uninstall complete." || { echo "Uninstall finished with $FAILED failure(s)." >&2; exit 1; }
