#!/usr/bin/env bash
# deploy-tileserver / build_push_martin.sh
#
# Publish the Martin image into the SPCS image repository. Martin itself needs no
# custom Dockerfile - we pull the upstream image, retag it for the repo, and push.
#
# Usage:
#   build_push_martin.sh --connection <conn> --repo-url <registry-host>/tileserver/core/images
# Env:
#   MARTIN_SRC   upstream image (default ghcr.io/maplibre/martin:latest)
#   MARTIN_TAG   tag to push    (default martin:latest)
#   CONTAINER_CMD  docker|podman (default: docker if present else podman)
set -euo pipefail

CONNECTION=""
REPO_URL=""
while [ $# -gt 0 ]; do
  case "$1" in
    --connection) CONNECTION="${2:-}"; shift 2;;
    --repo-url)   REPO_URL="${2:-}"; shift 2;;
    *) echo "Unknown arg: $1"; exit 2;;
  esac
done
[ -n "$CONNECTION" ] || { echo "ERROR: --connection required"; exit 2; }
[ -n "$REPO_URL" ]   || { echo "ERROR: --repo-url required"; exit 2; }

MARTIN_SRC="${MARTIN_SRC:-ghcr.io/maplibre/martin:latest}"
MARTIN_TAG="${MARTIN_TAG:-martin:latest}"
CONTAINER_CMD="${CONTAINER_CMD:-$(command -v docker >/dev/null 2>&1 && echo docker || echo podman)}"

echo "[build_push_martin] using $CONTAINER_CMD; source=$MARTIN_SRC target=$REPO_URL/$MARTIN_TAG"

# Log in to the SPCS registry for this connection (idempotent).
snow spcs image-registry login -c "$CONNECTION"

# Pull upstream (amd64 - SPCS runs linux/amd64), retag, push.
"$CONTAINER_CMD" pull --platform linux/amd64 "$MARTIN_SRC"
"$CONTAINER_CMD" tag "$MARTIN_SRC" "$REPO_URL/$MARTIN_TAG"
"$CONTAINER_CMD" push "$REPO_URL/$MARTIN_TAG"

echo "[build_push_martin] pushed $REPO_URL/$MARTIN_TAG"
