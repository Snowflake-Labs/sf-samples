#!/bin/bash
# ============================================================================
# Martech: SPCS Deployment Script
# Builds Docker image, pushes to Snowflake registry, creates/updates service.
#
# Usage: bash deploy/deploy_spcs.sh [--connection NAME] [--db DB] [--warehouse WH] [--compute-pool POOL] [--registry HOST]
# Example: bash deploy/deploy_spcs.sh --connection aws-east1
#          bash deploy/deploy_spcs.sh --connection aws-east1 --db MARTECH_PROD --compute-pool MARTECH_POOL
# Flags mirror deploy_all.sh. --registry is optional (auto-discovered if omitted).
# ============================================================================
set -e

# Defaults (mirror deploy_all.sh conventions)
CONNECTION="${SNOWFLAKE_CONNECTION_NAME:-aws-east1}"
DATABASE_NAME="MARTECH"
WAREHOUSE_NAME="MARTECH_WH"
COMPUTE_POOL="MARTECH_POOL"
REGISTRY_OVERRIDE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --connection)   CONNECTION="$2"; shift 2;;
        --db)           DATABASE_NAME="$2"; shift 2;;
        --warehouse)    WAREHOUSE_NAME="$2"; shift 2;;
        --compute-pool) COMPUTE_POOL="$2"; shift 2;;
        --registry)     REGISTRY_OVERRIDE="$2"; shift 2;;
        -h|--help)
            echo "Usage: bash deploy/deploy_spcs.sh [--connection NAME] [--db DB] [--warehouse WH] [--compute-pool POOL] [--registry HOST]"
            exit 0;;
        *) echo "Unknown arg: $1" >&2; exit 1;;
    esac
done

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SERVICE_DB="${DATABASE_NAME}"
SERVICE_SCHEMA="APP"
SERVICE_NAME="MARTECH_SERVICE"
IMAGE_NAME="martech"

echo "═══════════════════════════════════════════════════════════"
echo " Martech — SPCS Deployment"
echo " Connection:    $CONNECTION"
echo " Service:       $SERVICE_DB.$SERVICE_SCHEMA.$SERVICE_NAME"
echo " Compute Pool:  $COMPUTE_POOL"
echo "═══════════════════════════════════════════════════════════"

# ─── Get registry URL ─────────────────────────────────────────────────────────
echo ""
echo "▶ Resolving image registry URL..."
if [ -n "$REGISTRY_OVERRIDE" ]; then
    REGISTRY_URL="$REGISTRY_OVERRIDE"
else
    REGISTRY_URL=$(snow spcs image-registry url --connection "$CONNECTION" 2>/dev/null | tr -d '[:space:]')
fi
if [ -z "$REGISTRY_URL" ]; then
    echo "ERROR: Could not get registry URL. Check your --connection (got: '$CONNECTION')."
    exit 1
fi
IMAGE_PATH="${REGISTRY_URL}/$(echo "${SERVICE_DB}" | tr '[:upper:]' '[:lower:]')/$(echo "${SERVICE_SCHEMA}" | tr '[:upper:]' '[:lower:]')/images/${IMAGE_NAME}"
echo "  Registry: $REGISTRY_URL"
echo "  Image:    $IMAGE_PATH:latest"
echo "  Spec ref: /${SERVICE_DB}/${SERVICE_SCHEMA}/images/${IMAGE_NAME}:latest"

# ─── Docker login ─────────────────────────────────────────────────────────────
echo ""
echo "▶ Authenticating Docker to Snowflake registry..."
snow spcs image-registry login --connection "$CONNECTION"

# ─── Build ────────────────────────────────────────────────────────────────────
echo ""
echo "▶ Building Docker image (linux/amd64)..."
cd "$REPO_ROOT"
docker build --platform linux/amd64 -t "${IMAGE_NAME}:latest" .

# ─── Tag & Push ───────────────────────────────────────────────────────────────
echo ""
echo "▶ Tagging and pushing image..."
docker tag "${IMAGE_NAME}:latest" "${IMAGE_PATH}:latest"
docker push "${IMAGE_PATH}:latest"

# ─── Create or Update Service ─────────────────────────────────────────────────
echo ""
echo "▶ Creating/updating SPCS service..."
SERVICE_FQN="${SERVICE_DB}.${SERVICE_SCHEMA}.${SERVICE_NAME}"

snow sql -q "
ALTER SERVICE ${SERVICE_FQN}
FROM SPECIFICATION \$\$
spec:
  containers:
  - name: \"${IMAGE_NAME}\"
    image: \"/${SERVICE_DB}/${SERVICE_SCHEMA}/images/${IMAGE_NAME}:latest\"
    env:
      SNOWFLAKE_WAREHOUSE: \"${WAREHOUSE_NAME}\"
      DEPLOYMENT_DATABASE: \"${DATABASE_NAME}\"
      MARTECH_DB: \"${DATABASE_NAME}\"
      MARTECH_WAREHOUSE: \"${WAREHOUSE_NAME}\"
      MARTECH_ROLE: \"ACCOUNTADMIN\"
    resources:
      requests:
        memory: \"2Gi\"
        cpu: \"1\"
      limits:
        memory: \"4Gi\"
        cpu: \"1\"
  endpoints:
  - name: \"ui\"
    port: 8080
    public: true
  networkPolicyConfig:
    allowInternetEgress: true
\$\$;
" --connection "$CONNECTION" 2>/dev/null || \
snow sql -q "
CREATE SERVICE ${SERVICE_FQN}
  IN COMPUTE POOL ${COMPUTE_POOL}
  FROM SPECIFICATION \$\$
spec:
  containers:
  - name: \"${IMAGE_NAME}\"
    image: \"/${SERVICE_DB}/${SERVICE_SCHEMA}/images/${IMAGE_NAME}:latest\"
    env:
      SNOWFLAKE_WAREHOUSE: \"${WAREHOUSE_NAME}\"
      DEPLOYMENT_DATABASE: \"${DATABASE_NAME}\"
      MARTECH_DB: \"${DATABASE_NAME}\"
      MARTECH_WAREHOUSE: \"${WAREHOUSE_NAME}\"
      MARTECH_ROLE: \"ACCOUNTADMIN\"
    resources:
      requests:
        memory: \"2Gi\"
        cpu: \"1\"
      limits:
        memory: \"4Gi\"
        cpu: \"1\"
  endpoints:
  - name: \"ui\"
    port: 8080
    public: true
  networkPolicyConfig:
    allowInternetEgress: true
\$\$
MIN_INSTANCES = 1
MAX_INSTANCES = 1;
" --connection "$CONNECTION"

# ─── Wait for READY ──────────────────────────────────────────────────────────
echo ""
echo "▶ Waiting for service to become READY..."
for i in $(seq 1 12); do
    STATUS=$(snow sql -q "SELECT v.value:status::STRING FROM (SELECT SYSTEM\$GET_SERVICE_STATUS('${SERVICE_FQN}') AS s), LATERAL FLATTEN(input => PARSE_JSON(s)) v" --connection "$CONNECTION" 2>/dev/null | grep -oE 'READY|PENDING|FAILED')
    if [ "$STATUS" = "READY" ]; then
        echo "  ✓ Service is READY"
        break
    elif [ "$STATUS" = "FAILED" ]; then
        echo "  ✗ Service FAILED. Check logs:"
        echo "    snow sql -q \"CALL SYSTEM\$GET_SERVICE_LOGS('${SERVICE_FQN}', 0, '${IMAGE_NAME}', 50)\" --connection $CONNECTION"
        exit 1
    fi
    echo "  Status: ${STATUS:-checking...} (attempt $i/12)"
    sleep 10
done

# ─── Get endpoint URL ─────────────────────────────────────────────────────────
echo ""
echo "▶ Fetching endpoint URL..."
snow sql -q "SHOW ENDPOINTS IN SERVICE ${SERVICE_FQN}" --connection "$CONNECTION" 2>/dev/null | grep -oE 'https://[^ ]*\.snowflakecomputing\.app' || echo "  (URL may take a moment to appear — run SHOW ENDPOINTS manually)"

echo ""
echo "═══════════════════════════════════════════════════════════"
echo " ✓ SPCS deployment complete!"
echo "═══════════════════════════════════════════════════════════"
