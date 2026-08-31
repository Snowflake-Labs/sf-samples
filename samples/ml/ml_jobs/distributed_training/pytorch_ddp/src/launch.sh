#!/usr/bin/env bash
# Run an ordinary torchrun workload directly on every ML Job instance.
#
# ML Jobs owns instance allocation and exports the static topology. This file
# is the only Snowflake-aware part of the training payload: train.py consumes
# the standard environment variables populated by torchrun.
set -euo pipefail

: "${SNOWFLAKE_JOB_INDEX:?SNOWFLAKE_JOB_INDEX is required}"
: "${SNOWFLAKE_JOBS_COUNT:?SNOWFLAKE_JOBS_COUNT is required}"
: "${MLRS_HEAD_IP:?MLRS_HEAD_IP is required}"
: "${MLRS_RDZV_PORT:?MLRS_RDZV_PORT is required}"

if [[ -z "${NPROC_PER_NODE:-}" ]]; then
    NPROC_PER_NODE="$(python -c 'import torch; print(max(torch.cuda.device_count(), 1))')"
fi

echo "[launch] instance=${SNOWFLAKE_JOB_INDEX}/${SNOWFLAKE_JOBS_COUNT}" \
     "master=${MLRS_HEAD_IP}:${MLRS_RDZV_PORT}" \
     "nproc_per_node=${NPROC_PER_NODE}"

# Static rendezvous with the head IP: ML Jobs instances can't resolve peer hostnames (c10d dynamic rendezvous would fail).
exec torchrun \
    --nnodes="${SNOWFLAKE_JOBS_COUNT}" \
    --nproc-per-node="${NPROC_PER_NODE}" \
    --node-rank="${SNOWFLAKE_JOB_INDEX}" \
    --master-addr="${MLRS_HEAD_IP}" \
    --master-port="${MLRS_RDZV_PORT}" \
    train.py "$@"
