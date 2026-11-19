#!/usr/bin/env bash
# Set up everything the Multi-player AI Incident Room needs, from scratch.
# Usage: ./setup.sh <snowflake-cli connection name>   (the connection must be able to use ACCOUNTADMIN)
# Safe to re-run at any point: it ends with a healthy pipeline, one seeded past incident, and no open rooms.
set -euo pipefail

CONN="${1:?usage: ./setup.sh <connection>}"
cd "$(dirname "$0")"

run() {
  echo "==> $1"
  snow sql --connection "$CONN" --filename "$1" > /dev/null
}

run setup.sql             # roles, warehouse, databases, demo pipeline, room tables
run sql/tools.sql         # agent tools (owner's rights)
run sql/ship.sql          # SHIP_FIX, the only path to prod
run sql/agents.sql        # @triage, @fixer, @scribe
run demo/reset_memory.sql # one past incident so @triage has something to remember
run demo/reset.sql        # original task body, no open rooms, one healthy run of the load

echo "Done. Next: snow app deploy --connection $CONN"
echo "  or run locally: npm install && SNOWFLAKE_DEFAULT_CONNECTION_NAME=$CONN npm run dev"
