#!/usr/bin/env bash
# deploy-tileserver / teardown_tileserver.sh
#
# Idempotent, dependency-ordered teardown of the tileserver stack. Encodes the
# drop ordering that is otherwise only prose in SKILL.md / troubleshooting.md, so
# a clean-then-reinstall loop needs zero hand SQL.
#
# Ordering matters (see references/troubleshooting.md):
#   1. DROP SERVICE MARTIN            (frees the compute pool + ingress)
#   2. DROP the Postgres instance     (only with --drop-pg; billable to recreate)
#      - detaches the network policy that references TILESERVER.CORE.PG_INGRESS
#   3. empty + DROP the network policy (else DROP DATABASE fails: rule still bound)
#   4. DROP both EAIs                  (PG + basemap)
#   5. DROP COMPUTE POOL
#   6. DROP DATABASE TILESERVER        (image repo, stages, secret, network rules)
#
# Usage:
#   teardown_tileserver.sh --connection <conn> [--drop-pg]
#
#   --drop-pg   Also DROP the Postgres instance (a full teardown). The instance is
#               billable and slow to recreate; omit to keep it for fast re-installs.
set -euo pipefail

CONNECTION=""
DROP_PG=0
while [ $# -gt 0 ]; do
  case "$1" in
    --connection) CONNECTION="${2:-}"; shift 2;;
    --connection=*) CONNECTION="${1#*=}"; shift;;
    --drop-pg) DROP_PG=1; shift;;
    *) echo "Unknown arg: $1"; exit 2;;
  esac
done
[ -n "$CONNECTION" ] || { echo "ERROR: --connection <name> is required"; exit 2; }

QTAG='{"origin":"sf_sit-is","name":"oss-deploy-tileserver","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}'
note() { echo "[teardown] $*"; }
# Run one SQL statement; never abort the whole teardown on a single failure.
sf() { snow sql -c "$CONNECTION" -q "ALTER SESSION SET query_tag='$QTAG'; $1" >/dev/null 2>&1 || true; }

note "1/6 drop Martin service ..."
sf "DROP SERVICE IF EXISTS TILESERVER.CORE.MARTIN;"

if [ "$DROP_PG" = "1" ]; then
  note "2/6 drop Postgres instance (full teardown) ..."
  # Detect the tileserver PG instance by name (default TILESERVER_PG) and drop it
  # with a correctly-quoted identifier. Names can be case-sensitive quoted
  # lowercase identifiers, which an unquoted DROP would silently miss.
  INST="$(snow sql -c "$CONNECTION" --format=JSON -q "SHOW POSTGRES INSTANCES;" 2>/dev/null \
    | python3 -c "import sys,json
rows=json.load(sys.stdin) or []
cands=[r.get('name') for r in rows if 'oss-deploy-tileserver' in (r.get('comment') or '') or (r.get('name') or '').upper()=='TILESERVER_PG']
print(cands[0] if cands else '')" 2>/dev/null || true)"
  if [ -n "$INST" ]; then
    note "    dropping instance '$INST' ..."
    # Try quoted (exact) first, then unquoted, then uppercase - one will match.
    sf "DROP POSTGRES INSTANCE IF EXISTS \"$INST\";"
    sf "DROP POSTGRES INSTANCE IF EXISTS $INST;"
  else
    note "    no tileserver Postgres instance found (already gone)"
  fi
else
  note "2/6 keeping Postgres instance (no --drop-pg)"
fi

note "3/6 empty + drop network policy ..."
sf "ALTER NETWORK POLICY TILESERVER_PG_POLICY SET ALLOWED_NETWORK_RULE_LIST=();"
sf "DROP NETWORK POLICY IF EXISTS TILESERVER_PG_POLICY;"

note "4/6 drop external access integrations ..."
sf "DROP EXTERNAL ACCESS INTEGRATION IF EXISTS TILESERVER_PG_EAI;"
sf "DROP EXTERNAL ACCESS INTEGRATION IF EXISTS TILESERVER_BASEMAP_EAI;"

note "5/6 drop compute pool ..."
sf "DROP COMPUTE POOL IF EXISTS TILESERVER_POOL;"

note "6/6 drop database (image repo, stages, secret, network rules) ..."
sf "DROP DATABASE IF EXISTS TILESERVER;"

note "done. Verifying remnants ..."
snow sql -c "$CONNECTION" --format=JSON -q "SHOW DATABASES LIKE 'TILESERVER';" 2>/dev/null \
  | python3 -c "import sys,json; rows=json.load(sys.stdin); print('  DB TILESERVER still present!' if rows else '  DB TILESERVER gone')" 2>/dev/null || true
if [ "$DROP_PG" = "1" ]; then
  snow sql -c "$CONNECTION" --format=JSON -q "SHOW POSTGRES INSTANCES;" 2>/dev/null \
    | python3 -c "import sys,json; rows=json.load(sys.stdin); left=[r.get('name') for r in rows if 'oss-deploy-tileserver' in (r.get('comment') or '') or (r.get('name') or '').upper()=='TILESERVER_PG']; print('  PG instance still present: '+str(left) if left else '  PG instance gone')" 2>/dev/null || true
fi
