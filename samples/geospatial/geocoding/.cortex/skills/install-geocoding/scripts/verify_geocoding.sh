#!/usr/bin/env bash
# ============================================================================
# verify_geocoding.sh — read-only post-install check
# ============================================================================
# Confirms the installed objects exist and actually geocode, using real
# Overture rows so the checks do not depend on hand-picked addresses. Writes
# only to GEOCODING.TESTER scratch tables, which it drops afterwards.
#
# Usage:
#   bash verify_geocoding.sh --connection <conn> [--no-intl] [--no-eu] [--no-app]
#
# Exit codes: 0 = all checks passed, 1 = at least one failed.
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
[[ -n "$CONN" ]] || { echo "ERROR: --connection is required" >&2; exit 1; }
export SNOWFLAKE_CLI_NO_UPDATE_CHECK="${SNOWFLAKE_CLI_NO_UPDATE_CHECK:-true}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(cd "$SCRIPT_DIR/../../../.." && pwd)/geocoding_tester"
FAIL=0
pass() { printf '  PASS  %s\n' "$1"; }
fail() { printf '  FAIL  %s\n        -> %s\n' "$1" "$2"; FAIL=$((FAIL + 1)); }
# Last CSV value of a multi-statement run (the final SELECT).
q() { snow sql -c "$CONN" --silent --format csv -q "ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 900; $1" 2>&1 | tr -d '"' | grep -v '^[[:space:]]*$' | tail -1; }

echo "==> Verifying geocoding install"

# --- Objects -------------------------------------------------------------------
expect_fn() {
  local kind="$1" name="$2"
  if [[ -n "$(snow sql -c "$CONN" --silent --format csv -q "SHOW USER $kind LIKE '$name' IN SCHEMA GEOCODING.PUBLIC" 2>/dev/null | tail -n +2)" ]]; then
    pass "$kind $name"
  else
    fail "$kind $name missing" "re-run the installer"
  fi
}
for f in PARSE_ADDRESS STANDARDIZE_STREET NORMALIZE_STREET_EU; do expect_fn FUNCTIONS "$f"; done
for p in FORWARD_GEOCODE_TABLE REVERSE_GEOCODE_TABLE EVALUATE_GEOCODE BUILD_EU_INDEX; do expect_fn PROCEDURES "$p"; done

# --- US forward on 3 real Overture addresses -------------------------------------
OUT="$(q "CREATE SCHEMA IF NOT EXISTS GEOCODING.TESTER;
  CREATE OR REPLACE TRANSIENT TABLE GEOCODING.TESTER.VERIFY_US AS
    SELECT ROW_NUMBER() OVER (ORDER BY ID) AS ID,
           NUMBER || ' ' || STREET || ', ' || POSTAL_CITY || ' ' || POSTCODE AS ADDRESS,
           ST_Y(GEOMETRY) AS LAT, ST_X(GEOMETRY) AS LON
    FROM (SELECT * FROM GEOCODING.PUBLIC.OVERTURE_ADDRESS
          WHERE COUNTRY = 'US' AND NUMBER IS NOT NULL AND STREET IS NOT NULL
            AND POSTCODE IS NOT NULL AND POSTAL_CITY IS NOT NULL
          ORDER BY ID LIMIT 3);
  CALL GEOCODING.PUBLIC.FORWARD_GEOCODE_TABLE('GEOCODING.TESTER.VERIFY_US', 'ADDRESS', 'ID', 'GEOCODING.TESTER.VERIFY_US_OUT');
  SELECT COUNT(*) FROM GEOCODING.TESTER.VERIFY_US_OUT;")"
[[ "$OUT" == "3" ]] && pass "US forward geocoding: 3/3 matched" || fail "US forward geocoding matched '$OUT' of 3" "see the README Troubleshooting section"

# --- Reverse on 1 point --------------------------------------------------------------
OUT="$(q "CALL GEOCODING.PUBLIC.REVERSE_GEOCODE_TABLE('GEOCODING.TESTER.VERIFY_US', 'LAT', 'LON', 'ID', 200, 'GEOCODING.TESTER.VERIFY_REV_OUT', 'US');
  SELECT COUNT(*) FROM GEOCODING.TESTER.VERIFY_REV_OUT;")"
[[ "$OUT" == "3" ]] && pass "reverse geocoding: 3/3 points matched" || fail "reverse geocoding matched '$OUT' of 3" "check GEOCODING.PUBLIC.OVERTURE_ADDRESS"

# --- libpostal + EU ---------------------------------------------------------------------
if [[ "$INTL" -eq 1 ]]; then
  OUT="$(q "SELECT GEOCODING.PUBLIC.PARSE_ADDRESS_INTL('Rue de Rivoli 10, 75001 Paris'):road::STRING;")"
  [[ "$OUT" == "rue de rivoli" ]] && pass "libpostal service (PARSE_ADDRESS_INTL)" \
    || fail "PARSE_ADDRESS_INTL returned '$OUT'" "SELECT SYSTEM\$GET_SERVICE_STATUS('GEOCODING.PUBLIC.LIBPOSTAL_SVC');"
fi
if [[ "$EU" -eq 1 ]]; then
  OUT="$(q "CREATE OR REPLACE TRANSIENT TABLE GEOCODING.TESTER.VERIFY_EU AS
    SELECT ROW_NUMBER() OVER (ORDER BY COUNTRY) AS ID, ADDRESS, COUNTRY FROM (
      SELECT * FROM (SELECT STREET || ' ' || NUMBER || ', ' || POSTCODE AS ADDRESS, COUNTRY
                     FROM GEOCODING.PUBLIC.OVERTURE_ADDRESS_EU
                     WHERE COUNTRY = 'DE' AND NUMBER IS NOT NULL AND POSTCODE IS NOT NULL ORDER BY ID LIMIT 1)
      UNION ALL
      SELECT * FROM (SELECT NUMBER || ' ' || STREET || ', ' || POSTCODE, COUNTRY
                     FROM GEOCODING.PUBLIC.OVERTURE_ADDRESS_EU
                     WHERE COUNTRY = 'FR' AND NUMBER IS NOT NULL AND POSTCODE IS NOT NULL ORDER BY ID LIMIT 1));
    CALL GEOCODING.PUBLIC.FORWARD_GEOCODE_TABLE('GEOCODING.TESTER.VERIFY_EU', 'ADDRESS', 'ID', 'GEOCODING.TESTER.VERIFY_EU_OUT', 'US', 'COUNTRY');
    SELECT COUNT_IF(MATCH_LEVEL = 'ADDRESS') FROM GEOCODING.TESTER.VERIFY_EU_OUT;")"
  [[ "$OUT" == "2" ]] && pass "European forward geocoding: DE and FR at ADDRESS level" \
    || fail "European forward geocoding: '$OUT' of 2 at ADDRESS level" "check the EU index: CALL GEOCODING.PUBLIC.BUILD_EU_INDEX(FALSE)"
fi

# --- Tester app ----------------------------------------------------------------------------
if [[ "$APP" -eq 1 ]]; then
  URL="$(cd "$APP_DIR" && snow app open -c "$CONN" --print-only 2>/dev/null | grep -oE 'https://[^[:space:]]+' | head -1)"
  [[ -n "$URL" ]] && pass "function tester deployed: $URL" || fail "function tester URL not found" "bash geocoding_tester/deploy.sh --connection $CONN"
fi

snow sql -c "$CONN" --silent -q "DROP TABLE IF EXISTS GEOCODING.TESTER.VERIFY_US; DROP TABLE IF EXISTS GEOCODING.TESTER.VERIFY_US_OUT;
  DROP TABLE IF EXISTS GEOCODING.TESTER.VERIFY_REV_OUT; DROP TABLE IF EXISTS GEOCODING.TESTER.VERIFY_EU;
  DROP TABLE IF EXISTS GEOCODING.TESTER.VERIFY_EU_OUT;" >/dev/null 2>&1 || true

echo
if (( FAIL > 0 )); then echo "Verify: $FAIL check(s) failed"; exit 1; fi
echo "Verify: all checks passed"
exit 0
