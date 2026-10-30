#!/usr/bin/env bash
# ============================================================================
# preflight.sh — read-only readiness check before install_geocoding.sh
# ============================================================================
# Creates nothing and changes nothing. Prints one line per check:
#   OK        ready
#   DEGRADED  install still works, but a feature will be skipped or slow
#   BLOCKING  the install will fail; fix this first
#
# Usage:
#   bash preflight.sh --connection <conn> [--no-intl] [--no-eu] [--no-app]
#
# Exit codes: 0 = ready, 1 = blocking problem, 2 = ready with degraded features.
# ============================================================================
set -uo pipefail

CONN=""
INTL=1
EU=1
APP=1
while [[ $# -gt 0 ]]; do
  case "$1" in
    --connection|-c) CONN="${2:-}"; shift 2 ;;
    --no-intl) INTL=0; EU=0; shift ;;
    --no-eu)   EU=0; shift ;;
    --no-app)  APP=0; shift ;;
    --intl|--eu) shift ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done
[[ -n "$CONN" ]] || { echo "ERROR: --connection is required. See: snow connection list" >&2; exit 1; }
export SNOWFLAKE_CLI_NO_UPDATE_CHECK="${SNOWFLAKE_CLI_NO_UPDATE_CHECK:-true}"

BLOCK=0
DEGR=0
ok()    { printf '  OK        %s\n' "$1"; }
degr()  { printf '  DEGRADED  %s\n            -> %s\n' "$1" "$2"; DEGR=$((DEGR + 1)); }
block() { printf '  BLOCKING  %s\n            -> %s\n' "$1" "$2"; BLOCK=$((BLOCK + 1)); }
q()     { snow sql -c "$CONN" --silent --format csv -q "$1" 2>/dev/null | tail -n +2; }

echo "==> Geocoding preflight (read-only)"

# --- Tooling -----------------------------------------------------------------
if command -v snow >/dev/null 2>&1; then
  ok "Snowflake CLI: $(snow --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)"
else
  block "Snowflake CLI (snow) not found" "install it: https://docs.snowflake.com/en/developer-guide/snowflake-cli/installation/installation"
  echo; echo "Result: BLOCKING"; exit 1
fi

if [[ "$APP" -eq 1 ]]; then
  if snow app setup --help >/dev/null 2>&1; then
    ok "snow app commands available (tester app)"
  else
    degr "this snow CLI has no 'snow app' commands, so the tester app cannot deploy" \
      "upgrade the CLI to 3.26 or later, or pass --no-app"
  fi
fi

if [[ "$INTL" -eq 1 ]]; then
  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    ok "container runtime: docker"
  elif command -v podman >/dev/null 2>&1 && podman info >/dev/null 2>&1; then
    ok "container runtime: podman"
  else
    block "no running Docker daemon or Podman machine (needed to build the libpostal image)" \
      "start Docker/Podman, or pass --no-intl for US forward + worldwide reverse only"
  fi
fi

# --- Connection ----------------------------------------------------------------
CTX="$(q "SELECT CURRENT_ACCOUNT() || ',' || CURRENT_ROLE() || ',' || CURRENT_REGION() || ',' || COALESCE(CURRENT_WAREHOUSE(), '')")"
if [[ -z "$CTX" ]]; then
  block "connection '$CONN' is not usable" "check it with: snow connection test -c $CONN"
  echo; echo "Result: BLOCKING"; exit 1
fi
CTX="$(echo "$CTX" | tr -d '"')"
IFS=, read -r ACCOUNT ROLE REGION WH <<<"$CTX"
ok "connected: account $ACCOUNT, role $ROLE, region $REGION"
[[ -n "$WH" ]] && ok "session warehouse: $WH" || degr "no default warehouse on this connection" "pass --warehouse to the installer"

# --- Privileges ------------------------------------------------------------------
# Account-level grants of the current role (directly granted; inherited grants
# are not expanded, so ACCOUNTADMIN / SYSADMIN-style roles are checked by name).
GRANTS="$(q "SHOW GRANTS TO ROLE IDENTIFIER('\"$ROLE\"') ->> SELECT \"privilege\" FROM \$1 WHERE \"granted_on\" = 'ACCOUNT'" | tr -d '"')"
need_priv() {
  local priv="$1" why="$2"
  if [[ "$ROLE" == "ACCOUNTADMIN" ]] || grep -qx "$priv" <<<"$GRANTS"; then
    ok "privilege $priv"
  else
    degr "role $ROLE has no direct $priv grant ($why)" \
      "use ACCOUNTADMIN or GRANT $priv ON ACCOUNT TO ROLE $ROLE (an inherited grant also works)"
  fi
}
need_priv "CREATE DATABASE" "GEOCODING database and Marketplace listings"
need_priv "IMPORT SHARE" "Overture Maps listings"
if [[ "$INTL" -eq 1 ]]; then
  need_priv "CREATE COMPUTE POOL" "libpostal compute pool"
  need_priv "BIND SERVICE ENDPOINT" "libpostal service"
fi
[[ "$APP" -eq 1 ]] && need_priv "CREATE INTEGRATION" "tester app's CARTO basemap integration"

# --- SPCS --------------------------------------------------------------------------
if [[ "$INTL" -eq 1 || "$APP" -eq 1 ]]; then
  if snow sql -c "$CONN" --silent -q "SHOW COMPUTE POOLS LIMIT 1" >/dev/null 2>&1; then
    ok "Snowpark Container Services available"
  else
    block "SHOW COMPUTE POOLS failed: SPCS is not available to this role/account" \
      "SPCS is not offered on trial or some regions; pass --no-intl --no-app"
  fi
fi

# --- Marketplace listings ------------------------------------------------------------
check_listing() {
  local id="$1" name="$2" db="$3"
  if [[ -n "$(q "SHOW DATABASES LIKE '$db'")" || -n "$(q "SHOW DATABASES LIKE 'DS_$db'")" ]]; then
    ok "$name already installed ($db)"
  elif snow sql -c "$CONN" --silent -q "DESCRIBE AVAILABLE LISTING $id" >/dev/null 2>&1; then
    ok "$name listing $id is available; the installer will acquire it"
  else
    block "$name listing $id is not visible from this account/region" \
      "get \"$name\" (Carto) from the Marketplace as $db, then re-run"
  fi
}
check_listing GZT0Z4CM1E9NQ "Overture Maps - Addresses" OVERTURE_MAPS__ADDRESSES
[[ "$EU" -eq 1 ]] && check_listing GZT0Z4CM1E9M9 "Overture Maps - Divisions" OVERTURE_MAPS__DIVISIONS

# --- Existing install (reuse) ---------------------------------------------------------
if [[ -n "$(q "SHOW DATABASES LIKE 'GEOCODING'")" ]]; then
  EXISTING="$(q "SHOW TABLES LIKE 'OVERTURE_ADDRESS_EU' IN SCHEMA GEOCODING.PUBLIC")"
  ok "existing GEOCODING database found: re-run reuses it$( [[ -n "$EXISTING" ]] && echo ', EU index already built (no rebuild)')"
fi

# --- EU index warehouse ----------------------------------------------------------------
if [[ "$EU" -eq 1 && -z "$(q "SHOW TABLES LIKE 'OVERTURE_ADDRESS_EU' IN SCHEMA GEOCODING.PUBLIC")" ]]; then
  BIG="$(q "SHOW WAREHOUSES ->> SELECT \"name\" FROM \$1 WHERE \"size\" IN ('X-Large','2X-Large','3X-Large','4X-Large','5X-Large','6X-Large') LIMIT 1" | tr -d '"')"
  if [[ -n "$BIG" ]]; then
    ok "X-Large or larger warehouse for the one-time EU build: $BIG (pass --eu-warehouse $BIG)"
  else
    degr "no X-Large warehouse visible; the one-time EU index build can take hours on smaller ones" \
      "create GEOCODING_BUILD_WH (see SKILL.md Step 2) and pass --eu-warehouse GEOCODING_BUILD_WH, or --no-eu"
  fi
fi

echo
if (( BLOCK > 0 )); then
  echo "Result: BLOCKING ($BLOCK blocking, $DEGR degraded)"; exit 1
elif (( DEGR > 0 )); then
  echo "Result: READY WITH DEGRADED FEATURES ($DEGR)"; exit 2
fi
echo "Result: READY"
exit 0
