#!/usr/bin/env bash
# ============================================================================
# Martech: deploy_all.sh
# Provisions DB, deploys engine + use-case SQL, optionally seeds data.
#
# Usage:
#   ./deploy_all.sh                              # default DB=MARTECH
#   ./deploy_all.sh --db MARTECH_DEV             # custom DB
#   ./deploy_all.sh --db MARTECH_DEV --connection aws-east1
#   ./deploy_all.sh --db MARTECH_DEV --seed      # also generate synthetic data
# ============================================================================
set -euo pipefail

DB="${MARTECH_DB:-MARTECH}"
CONN="${SNOWFLAKE_CONNECTION_NAME:-aws-east1}"
SEED=0

while [[ $# -gt 0 ]]; do
    case $1 in
        --db)         DB="$2"; shift 2;;
        --connection) CONN="$2"; shift 2;;
        --seed)       SEED=1; shift;;
        *) echo "Unknown arg: $1"; exit 1;;
    esac
done

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
ENGINE_DIR="$ROOT/engine/sql"
USECASE_DIR="$ROOT/use-cases/martech"

# Minimum Snowflake CLI version this deploy is tested against. Bump as needed.
MIN_SNOW_VERSION="3.6.0"

# Preflight: ensure the Snowflake CLI is present, runnable, and recent enough.
# Note: `snow --version` itself fails (non-zero, no output) when the bundled
# snowflake-connector-python is too old for the CLI (e.g. the workload_identity
# ModuleNotFoundError), so this also catches a broken/clobbered install.
check_snow_cli() {
    if ! command -v snow >/dev/null 2>&1; then
        echo "ERROR: Snowflake CLI ('snow') not found on PATH." >&2
        echo "  Install it with:  pip install snowflake-cli" >&2
        exit 1
    fi
    local ver
    ver="$(snow --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)"
    if [[ -z "$ver" ]]; then
        echo "ERROR: 'snow --version' failed to run — your Snowflake CLI install looks broken." >&2
        echo "  This is usually a connector version mismatch. Fix with:" >&2
        echo "      pip install --upgrade snowflake-cli" >&2
        exit 1
    fi
    if [[ "$(printf '%s\n%s\n' "$MIN_SNOW_VERSION" "$ver" | sort -V | head -1)" != "$MIN_SNOW_VERSION" ]]; then
        echo "ERROR: Snowflake CLI $ver is older than the required minimum $MIN_SNOW_VERSION." >&2
        echo "  Upgrade with:  pip install --upgrade snowflake-cli" >&2
        exit 1
    fi
    echo "✔ Snowflake CLI $ver (>= $MIN_SNOW_VERSION)"
}

echo "═══════════════════════════════════════════════════════════════"
echo "Martech IDR Deploy"
echo "  Database:   $DB"
echo "  Connection: $CONN"
echo "  Seed data:  $SEED"
echo "═══════════════════════════════════════════════════════════════"

check_snow_cli

# Run a martech SQL file. Performs &{deployment_db} -> $DB substitution
# in shell, then deploys with --enable-templating NONE so snow CLI does NOT
# interpret '&' as a variable prefix. This is critical for JS-bodied procs:
# without --enable-templating NONE, the LEGACY templater silently mangles
# '&&' (logical AND) into '&' (bitwise AND), which silently breaks fuzzy
# matching rule dispatchers (root-caused while debugging engine R15).
# Execute a rewritten SQL file via snow, capturing the REAL exit code so a SQL
# failure aborts the deploy instead of being silently swallowed. Benign
# templating warnings ("Warning: &{...") are filtered from display only.
_run_snow_file() {
    local rewritten="$1" src="$2"
    local out rc=0
    out=$(snow sql -f "$rewritten" --enable-templating NONE --connection "$CONN" 2>&1) || rc=$?
    printf '%s\n' "$out" | grep -v "^Warning: &{" || true
    if [[ $rc -ne 0 ]]; then
        echo "" >&2
        echo "ERROR: deploy aborted — SQL failed (snow exit $rc): $src" >&2
        exit 1
    fi
}

run_sql() {
    local file="$1"
    echo "▶ $file"
    local rewritten; rewritten=$(mktemp)
    sed -E "s/&\\{deployment_db\\}/${DB}/g" "$file" > "$rewritten"
    _run_snow_file "$rewritten" "$file"
    rm -f "$rewritten"
}

# Run an engine SQL file with deploy-time rewrites (engine source files stay
# untouched; NFR-3):
#   - Engine procedures (IDR.PROCEDURES.*) stay in the shared IDR database -- NOT rewritten.
#   - The Jaro-Winkler UDF deploys to the use-case SILVER schema (procs call it as DB.SILVER.*).
#   - IDR_DEMO.* data/table refs    -> target use-case DB (e.g. MARTECH.*)
#   - IDR.CONFIG.* + "USE DATABASE IDR" -> target DB CONFIG (config is per use-case)
#   - IDR_DEMO_WH                    -> MARTECH_WH
run_engine_sql() {
    local file="$1"
    echo "engine: $(basename "$file") (rewriting IDR_DEMO -> $DB)"
    # IMPORTANT: --enable-templating NONE prevents snow CLI from treating '&'
    # as a variable-substitution prefix, which would otherwise mangle JS
    # operators like '&&' (logical AND) into '&' (bitwise AND) inside engine
    # procedures. This was the root cause of fuzzy-rule predicates being
    # silently dropped from generated matching SQL.
    local rewritten; rewritten=$(mktemp)
    sed -E "s/USE ROLE SYSADMIN/USE ROLE ACCOUNTADMIN/g; s/USE DATABASE IDR_DEMO/USE DATABASE ${DB}/g; s/USE DATABASE IDR\\b/USE DATABASE ${DB}/g; s/ALTER DATABASE IDR_DEMO/ALTER DATABASE ${DB}/g; s/IDR_DEMO\\./${DB}./g; s/IDR_DEMO_WH/MARTECH_WH/g; s/IDR\\.CONFIG\\./${DB}.CONFIG./g; s/&\\{deployment_db\\}/${DB}/g" \
        "$file" > "$rewritten"
    _run_snow_file "$rewritten" "$file"
    rm -f "$rewritten"
}

# 1. Infrastructure
run_sql "$USECASE_DIR/deploy/01_infrastructure.sql"

# 2. Engine: tags, tables, UDFs, procedures (seeds run AFTER tables exist)
run_engine_sql "$ENGINE_DIR/tags/idr_tags.sql"
for f in "$ENGINE_DIR/tables/"*.sql; do run_engine_sql "$f"; done
run_engine_sql "$ENGINE_DIR/udfs/01_jaro_winkler_udf.sql"
for f in "$ENGINE_DIR/procedures/"*.sql; do run_engine_sql "$f"; done

# 3. Martech tables (bronze + idr_core gaps + silver + gold)
echo "── Martech tables ──"
for f in "$USECASE_DIR/ddl/"*.sql; do run_sql "$f"; done
for f in "$USECASE_DIR/sql/idr_core/"*.sql; do run_sql "$f"; done
for f in "$USECASE_DIR/sql/silver/"*.sql; do run_sql "$f"; done
for f in "$USECASE_DIR/sql/gold/"*.sql; do run_sql "$f"; done

# 4. Engine seeds (after tables exist)
for f in "$ENGINE_DIR/seeds/"*.sql; do run_engine_sql "$f"; done

# 5. Martech config seeds
echo "── Martech config seeds ──"
# Refresh metadata cache FIRST: discovers IDR-tagged BRONZE tables/columns and
# populates IDR_CORE_TABLE/COLUMN_METADATA_CACHE, which SP_RUN_IDR_PIPELINE,
# SP_STANDARDIZE_DATA, and SP_EXTRACT_IDENTIFIERS read to discover tables.
# Must run BEFORE shopify_column_metadata_seed (which MERGEs extra columns the
# refresh would otherwise truncate).
echo "▶ refreshing IDR metadata cache"
snow sql -q "USE WAREHOUSE MARTECH_WH; CALL IDR.PROCEDURES.SP_REFRESH_METADATA_CACHE('${DB}');" --connection "$CONN"
run_sql "$USECASE_DIR/sql/config/standardization_rules_seed.sql"
run_sql "$USECASE_DIR/sql/config/matching_rules_seed.sql"
run_sql "$USECASE_DIR/sql/config/source_priority_seed.sql"
run_sql "$USECASE_DIR/sql/config/llm_review_config_seed.sql"
# v1: ML blocking strategies + stoplist (must be loaded before SP_CUSTOM_STANDARDIZE runs)
run_sql "$USECASE_DIR/sql/config/ml_individual_blocking_strategies_seed.sql"
run_sql "$USECASE_DIR/sql/config/ml_individual_blocking_stoplist_seed.sql"
# Shopify column metadata: BRONZE.SHOPIFY_ORDER_RAW has only RAW_PAYLOAD VARIANT
# (no taggable flat columns), so SP_REFRESH_METADATA_CACHE can't auto-discover.
# This seed registers the flat columns produced on STD_SHOPIFY_ORDER_RAW so
# SP_EXTRACT_IDENTIFIERS sees shopify identifiers. Idempotent (MERGE) — safe to
# run after any future SP_REFRESH_METADATA_CACHE invocation.
run_sql "$USECASE_DIR/sql/config/shopify_column_metadata_seed.sql"

# 6. Martech procedures
echo "── Martech procedures ──"
for f in "$USECASE_DIR/sql/procedures/"*.sql; do run_sql "$f"; done

# 7. Streams + task (suspended on deploy)
echo "── Streams + task ──"
run_sql "$USECASE_DIR/sql/streams_and_task.sql"

# 7b. Clustering keys (must run AFTER all tables exist)
echo "── Clustering keys ──"
run_sql "$USECASE_DIR/sql/clustering_keys.sql"

# 8. Optional: seed synthetic data
if [[ "$SEED" -eq 1 ]]; then
    echo "▶ generating synthetic data"
    cd "$USECASE_DIR/backend"
    pip install -q -r requirements.txt
    MARTECH_DB="$DB" python martech_data_generator.py --connection-name "$CONN" --customers 10000 --events 50000
    echo "▶ running IDR pipeline"
    snow sql -q "CALL IDR.PROCEDURES.SP_RUN_IDR_PIPELINE('${DB}');" --connection "$CONN"
fi

# 9. Enable the incremental task (runs whether or not --seed was opted).
echo "── Resuming incremental task ──"
echo "▶ ALTER TASK ${DB}.APP.IDR_INCREMENTAL_TASK RESUME"
snow sql -q "ALTER TASK ${DB}.APP.IDR_INCREMENTAL_TASK RESUME;" --connection "$CONN"

echo "═══════════════════════════════════════════════════════════════"
echo "✔ Deploy complete. Incremental task ${DB}.APP.IDR_INCREMENTAL_TASK is RESUMED."
echo "  To suspend it:  snow sql -q \"ALTER TASK ${DB}.APP.IDR_INCREMENTAL_TASK SUSPEND;\" --connection $CONN"
echo "═══════════════════════════════════════════════════════════════"
