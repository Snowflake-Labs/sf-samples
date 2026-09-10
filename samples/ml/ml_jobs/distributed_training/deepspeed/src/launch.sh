#!/usr/bin/env bash
# Translate the ML Job topology into DeepSpeed's no-SSH hostfile launcher.
set -euo pipefail

: "${SNOWFLAKE_JOB_INDEX:?SNOWFLAKE_JOB_INDEX is required}"
: "${SNOWFLAKE_JOBS_COUNT:?SNOWFLAKE_JOBS_COUNT is required}"
: "${MLRS_HEAD_IP:?MLRS_HEAD_IP is required}"
: "${MLRS_RDZV_PORT:?MLRS_RDZV_PORT is required}"
: "${MLRS_NODE_IPS:?MLRS_NODE_IPS is required}"

if [[ -z "${NPROC_PER_NODE:-}" ]]; then
    NPROC_PER_NODE="$(python -c 'import torch; print(max(torch.cuda.device_count(), 1))')"
fi

IFS=',' read -r -a NODE_IPS <<< "${MLRS_NODE_IPS}"
if [[ "${#NODE_IPS[@]}" -ne "${SNOWFLAKE_JOBS_COUNT}" ]]; then
    echo "MLRS_NODE_IPS contains ${#NODE_IPS[@]} nodes; expected ${SNOWFLAKE_JOBS_COUNT}." >&2
    exit 2
fi

HOSTFILE="${TMPDIR:-/tmp}/deepspeed-hostfile"
: > "${HOSTFILE}"
for node_ip in "${NODE_IPS[@]}"; do
    printf '%s slots=%s\n' "${node_ip}" "${NPROC_PER_NODE}" >> "${HOSTFILE}"
done

echo "[launch] instance=${SNOWFLAKE_JOB_INDEX}/${SNOWFLAKE_JOBS_COUNT}" \
     "master=${MLRS_HEAD_IP}:${MLRS_RDZV_PORT}" \
     "slots_per_node=${NPROC_PER_NODE}"
echo "[launch] hostfile:"
sed 's/^/[launch]   /' "${HOSTFILE}"

exec deepspeed \
    --hostfile="${HOSTFILE}" \
    --no_ssh \
    --node_rank="${SNOWFLAKE_JOB_INDEX}" \
    --master_addr="${MLRS_HEAD_IP}" \
    --master_port="${MLRS_RDZV_PORT}" \
    train.py \
    "$@"
