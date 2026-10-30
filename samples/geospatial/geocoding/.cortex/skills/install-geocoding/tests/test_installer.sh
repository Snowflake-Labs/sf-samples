#!/usr/bin/env bash
# ============================================================================
# test_installer.sh — offline self-test for the installer scripts
# ============================================================================
# Needs no Snowflake connection. Checks:
#   - every script parses (bash -n)
#   - every SQL module the installer lists exists
#   - every CREATE of a persistent object carries the tracking COMMENT
#   - libpostal_service/VERSION matches the image tag in deploy.sql and
#     service-spec.yaml
#   - --help works and unknown flags are rejected
#
# Usage: bash tests/test_installer.sh        Exit 0 = all passed.
# ============================================================================
set -uo pipefail

SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GEO_ROOT="$(cd "$SKILL_DIR/../../.." && pwd)"
SCRIPTS="$SKILL_DIR/scripts"
FAIL=0
pass() { printf '  PASS  %s\n' "$1"; }
fail() { printf '  FAIL  %s\n' "$1"; FAIL=$((FAIL + 1)); }

echo "==> Syntax"
for f in "$SCRIPTS"/*.sh "$GEO_ROOT/geocoding_tester/deploy.sh"; do
  bash -n "$f" 2>/dev/null && pass "bash -n ${f#$GEO_ROOT/}" || fail "bash -n ${f#$GEO_ROOT/}"
done

echo "==> SQL modules listed by the installer"
while read -r m; do
  [[ -f "$GEO_ROOT/sql/$m" ]] && pass "sql/$m" || fail "missing sql/$m"
done < <(grep -oE '\$SQL_DIR/[^"]+' "$SCRIPTS/install_geocoding.sh" | sed 's#\$SQL_DIR/##' | grep '\.sql$' | sort -u)

echo "==> Tracking COMMENT on persistent CREATE statements"
# Each CREATE of a database / schema / table / view / function / procedure /
# warehouse / pool / service / repository / integration must have the origin
# tag before the statement ends. Temporary and transient scratch objects and
# commented-out lines are exempt.
while IFS= read -r f; do
  missing="$(awk '
    /^[[:space:]]*--/ { next }
    toupper($0) ~ /CREATE (OR REPLACE )?(SECURE )?(DATABASE|SCHEMA|TABLE|VIEW|FUNCTION|PROCEDURE|WAREHOUSE|COMPUTE POOL|SERVICE|IMAGE REPOSITORY|NETWORK RULE|EXTERNAL ACCESS INTEGRATION|NOTIFICATION INTEGRATION)/ \
      && toupper($0) !~ /TEMPORARY|TRANSIENT|EXECUTE IMMEDIATE|^[[:space:]]*\x27|\|\|/ {
        open = 1; start = NR; buf = ""
    }
    open { buf = buf $0 "\n"
      if (buf ~ /sf_sit-is-geocoding/) { open = 0 }
      else if ($0 ~ /;[[:space:]]*$/ || $0 ~ /^\$\$;/) { print start; open = 0 }
    }' "$f")"
  if [[ -z "$missing" ]]; then pass "${f#$GEO_ROOT/}"
  else fail "${f#$GEO_ROOT/}: CREATE without tracking COMMENT at line(s) $(echo $missing)"; fi
done < <(find "$GEO_ROOT/sql/00_setup.sql" "$GEO_ROOT/sql/udfs" "$GEO_ROOT/sql/procedures" "$GEO_ROOT/sql/eu" \
           "$GEO_ROOT/libpostal_service/deploy.sql" "$SKILL_DIR/references/cost-guardrails.sql" -name '*.sql' | sort)

echo "==> libpostal image version"
V="$(tr -d '[:space:]' < "$GEO_ROOT/libpostal_service/VERSION")"
n="$(grep -c "libpostal:${V}\$" "$GEO_ROOT/libpostal_service/deploy.sql")"
[[ "$n" == "2" ]] && pass "deploy.sql uses libpostal:$V in both spec blocks" || fail "deploy.sql has $n of 2 references to libpostal:$V"
grep -q "libpostal:${V}\$" "$GEO_ROOT/libpostal_service/service-spec.yaml" && pass "service-spec.yaml uses libpostal:$V" || fail "service-spec.yaml does not use libpostal:$V"
grep -q 'libpostal:latest' "$GEO_ROOT/libpostal_service/deploy.sql" && fail "deploy.sql still references :latest" || pass "no :latest in deploy.sql"

echo "==> CLI flags"
for s in install_geocoding.sh preflight.sh verify_geocoding.sh uninstall_geocoding.sh; do
  bash "$SCRIPTS/$s" --help >/dev/null 2>&1 && pass "$s --help" || fail "$s --help"
  bash "$SCRIPTS/$s" --bogus-flag >/dev/null 2>&1 && fail "$s accepted --bogus-flag" || pass "$s rejects unknown flags"
done
bash "$SCRIPTS/install_geocoding.sh" --connection x --grant-role 'bad;role' >/dev/null 2>&1 \
  && fail "install accepted --grant-role 'bad;role'" || pass "install rejects unsafe --grant-role"

echo
if (( FAIL > 0 )); then echo "Self-test: $FAIL failure(s)"; exit 1; fi
echo "Self-test: all passed"
