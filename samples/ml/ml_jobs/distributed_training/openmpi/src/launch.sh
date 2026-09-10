#!/usr/bin/env bash
# Bootstrap job-scoped SSH agents, then launch MPI-enabled LightGBM from instance 0.
set -euo pipefail

: "${SNOWFLAKE_JOB_INDEX:?SNOWFLAKE_JOB_INDEX is required}"
: "${SNOWFLAKE_JOBS_COUNT:?SNOWFLAKE_JOBS_COUNT is required}"
: "${MLRS_HEAD_IP:?MLRS_HEAD_IP is required}"
: "${MLRS_NODE_IPS:?MLRS_NODE_IPS is required}"
: "${MLRS_EPHEMERAL_PORT_MIN:?MLRS_EPHEMERAL_PORT_MIN is required}"
: "${MLRS_EPHEMERAL_PORT_MAX:?MLRS_EPHEMERAL_PORT_MAX is required}"

MAX_ROWS=100000
NUM_ITERATIONS=20
NUM_THREADS=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --max-rows) MAX_ROWS="$2"; shift 2 ;;
        --num-iterations) NUM_ITERATIONS="$2"; shift 2 ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

SSH_PORT="${MLRS_EPHEMERAL_PORT_MIN}"
CONTROL_PORT="$((MLRS_EPHEMERAL_PORT_MIN + 1))"
# ORTE OOB control ports and Open MPI TCP BTL data ports, as two small disjoint sub-ranges.
OOB_PORT_MIN="$((MLRS_EPHEMERAL_PORT_MIN + 2))"
OOB_PORT_MAX="$((OOB_PORT_MIN + 19))"
BTL_PORT_MIN="$((OOB_PORT_MAX + 1))"
BTL_PORT_RANGE=20
if (( BTL_PORT_MIN + BTL_PORT_RANGE - 1 > MLRS_EPHEMERAL_PORT_MAX )); then
    echo "Open MPI requires at least 42 ports in the exported ephemeral range." >&2
    exit 2
fi
# ORTE and BTL TCP are pinned to the instance subnet inferred from the head IP.
MPI_TCP_SUBNET="$(echo "${MLRS_HEAD_IP}" | cut -d. -f1-2).0.0/16"

IFS=',' read -r -a NODE_IPS <<< "${MLRS_NODE_IPS}"
if [[ "${#NODE_IPS[@]}" -ne "${SNOWFLAKE_JOBS_COUNT}" ]]; then
    echo "MLRS_NODE_IPS contains ${#NODE_IPS[@]} nodes; expected ${SNOWFLAKE_JOBS_COUNT}." >&2
    exit 2
fi

PAYLOAD_DIR="$(pwd)"
STAGED_PRIVATE_KEY="${PAYLOAD_DIR}/keys/id_ed25519"
STAGED_PUBLIC_KEY="${STAGED_PRIVATE_KEY}.pub"
if [[ ! -s "${STAGED_PRIVATE_KEY}" || ! -s "${STAGED_PUBLIC_KEY}" ]]; then
    echo "The job-scoped SSH key was not packaged by submit_job.py." >&2
    exit 2
fi

RUNTIME_DIR="${TMPDIR:-/tmp}/snowflake-openmpi"
DATA_FILE="${RUNTIME_DIR}/higgs.partition.csv"
LIGHTGBM_BIN="${RUNTIME_DIR}/lightgbm"
OUTPUT_DIR="${PAYLOAD_DIR}/output/openmpi"
DIAGNOSTIC_DIR="${OUTPUT_DIR}/instance-${SNOWFLAKE_JOB_INDEX}"
mkdir -p "${RUNTIME_DIR}" "${DIAGNOSTIC_DIR}"

# Stage mounts do not necessarily preserve POSIX ownership and mode semantics
# closely enough for OpenSSH's private-key checks. Copy the job-scoped key to
# node-local storage before use so ssh can safely require mode 0600.
PRIVATE_KEY="${RUNTIME_DIR}/id_ed25519"
PUBLIC_KEY="${PRIVATE_KEY}.pub"
cp "${STAGED_PRIVATE_KEY}" "${PRIVATE_KEY}"
cp "${STAGED_PUBLIC_KEY}" "${PUBLIC_KEY}"
chmod 600 "${PRIVATE_KEY}"
chmod 644 "${PUBLIC_KEY}"

SSHD_PID=""
SSHD_LOG=""
CONTROL_PID=""
WORKERS_NOTIFIED=0
cleanup() {
    local status=$?
    trap - EXIT INT TERM
    if [[ "${SNOWFLAKE_JOB_INDEX}" -eq 0 && "${status}" -ne 0 && "${WORKERS_NOTIFIED}" -eq 0 ]]; then
        for worker_ip in "${NODE_IPS[@]:1}"; do
            python control.py send --host "${worker_ip}" --port "${CONTROL_PORT}" --status "${status}" || true
        done
    fi
    if [[ -n "${SSHD_PID}" ]]; then
        kill "${SSHD_PID}" 2>/dev/null || true
        wait "${SSHD_PID}" 2>/dev/null || true
    fi
    if [[ -n "${CONTROL_PID}" ]]; then
        kill "${CONTROL_PID}" 2>/dev/null || true
        wait "${CONTROL_PID}" 2>/dev/null || true
    fi
    if [[ -n "${SSHD_LOG}" && -f "${SSHD_LOG}" ]]; then
        cp "${SSHD_LOG}" "${DIAGNOSTIC_DIR}/sshd.log" 2>/dev/null || true
        if [[ "${status}" -ne 0 ]]; then
            echo "[diagnostic] instance ${SNOWFLAKE_JOB_INDEX} sshd log:" >&2
            tail -n 200 "${SSHD_LOG}" >&2 || true
        fi
    fi
    exit "${status}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ "${SNOWFLAKE_JOB_INDEX}" -ne 0 ]]; then
    python control.py wait --port "${CONTROL_PORT}" --timeout 7200 &
    CONTROL_PID=$!
fi

python build_lightgbm.py --output "${LIGHTGBM_BIN}"
python prepare_data.py \
    --max-rows "${MAX_ROWS}" \
    --rank "${SNOWFLAKE_JOB_INDEX}" \
    --world-size "${SNOWFLAKE_JOBS_COUNT}" \
    --output "${DATA_FILE}"
cp "${PAYLOAD_DIR}/train.conf" "${RUNTIME_DIR}/train.conf"

cp "${PUBLIC_KEY}" "${RUNTIME_DIR}/authorized_keys"
ssh-keygen -q -t ed25519 -N '' -f "${RUNTIME_DIR}/ssh_host_ed25519_key"
mkdir -p /run/sshd
chmod 755 /run/sshd
cat > "${RUNTIME_DIR}/sshd_config" <<EOF
Port ${SSH_PORT}
ListenAddress 0.0.0.0
HostKey ${RUNTIME_DIR}/ssh_host_ed25519_key
AuthorizedKeysFile ${RUNTIME_DIR}/authorized_keys
PidFile ${RUNTIME_DIR}/sshd.pid
StrictModes no
PasswordAuthentication no
KbdInteractiveAuthentication no
ChallengeResponseAuthentication no
UsePAM no
PermitRootLogin prohibit-password
LogLevel VERBOSE
EOF

SSHD_LOG="${RUNTIME_DIR}/sshd.log"
if ! /usr/sbin/sshd -t -f "${RUNTIME_DIR}/sshd_config" 2> "${SSHD_LOG}"; then
    echo "The job-scoped SSH server configuration is invalid:" >&2
    cat "${SSHD_LOG}" >&2
    exit 1
fi
/usr/sbin/sshd -D -e -f "${RUNTIME_DIR}/sshd_config" > "${SSHD_LOG}" 2>&1 &
SSHD_PID=$!

MPI_USER="$(id -un)"
SSH_OPTIONS=(
    -4
    -i "${PRIVATE_KEY}"
    -p "${SSH_PORT}"
    -o BatchMode=yes
    -o ConnectTimeout=5
    -o ConnectionAttempts=1
    -o StrictHostKeyChecking=no
    -o UserKnownHostsFile=/dev/null
    -o LogLevel=ERROR
)

LOCAL_SSH_READY=0
for _ in $(seq 1 30); do
    if ssh "${SSH_OPTIONS[@]}" "${MPI_USER}@127.0.0.1" true 2>/dev/null; then
        LOCAL_SSH_READY=1
        break
    fi
    if ! kill -0 "${SSHD_PID}" 2>/dev/null; then
        break
    fi
    sleep 1
done
if [[ "${LOCAL_SSH_READY}" -ne 1 ]]; then
    echo "The job-scoped SSH agent failed its local readiness check:" >&2
    cat "${SSHD_LOG}" >&2
    local_client_log="${RUNTIME_DIR}/ssh-local.log"
    set +e
    ssh "${SSH_OPTIONS[@]}" -vvv "${MPI_USER}@127.0.0.1" true \
        > "${local_client_log}" 2>&1
    set -e
    cp "${local_client_log}" "${DIAGNOSTIC_DIR}/ssh-local.log" 2>/dev/null || true
    echo "[diagnostic] verbose local SSH client log:" >&2
    tail -n 200 "${local_client_log}" >&2 || true
    exit 1
fi

echo "[launch] instance=${SNOWFLAKE_JOB_INDEX}/${SNOWFLAKE_JOBS_COUNT}" \
     "nodes=${MLRS_NODE_IPS}" \
     "ssh_port=${SSH_PORT}" \
     "control_port=${CONTROL_PORT}" \
     "oob_ports=${OOB_PORT_MIN}-${OOB_PORT_MAX}" \
     "btl_ports=${BTL_PORT_MIN}-$((BTL_PORT_MIN + BTL_PORT_RANGE - 1))" \
     "tcp_subnet=${MPI_TCP_SUBNET}"

if [[ "${SNOWFLAKE_JOB_INDEX}" -ne 0 ]]; then
    set +e
    wait "${CONTROL_PID}"
    STATUS=$?
    set -e
    CONTROL_PID=""
    exit "${STATUS}"
fi

for node_ip in "${NODE_IPS[@]}"; do
    connected=0
    deadline=$((SECONDS + 120))
    while (( SECONDS < deadline )); do
        if ssh "${SSH_OPTIONS[@]}" "${MPI_USER}@${node_ip}" true 2>/dev/null; then
            connected=1
            break
        fi
        sleep 1
    done
    if [[ "${connected}" -ne 1 ]]; then
        echo "SSH agent on ${node_ip}:${SSH_PORT} did not become ready." >&2
        echo "[diagnostic] testing raw TCP reachability before one verbose SSH attempt" >&2
        if ! python control.py probe \
            --host "${node_ip}" \
            --port "${SSH_PORT}" \
            --timeout 5; then
            echo "[diagnostic] TCP connection to ${node_ip}:${SSH_PORT} failed." >&2
        else
            echo "[diagnostic] TCP is reachable; the failure is in the SSH handshake or authentication." >&2
        fi
        safe_node_ip="${node_ip//./_}"
        client_log="${RUNTIME_DIR}/ssh-to-${safe_node_ip}.log"
        set +e
        ssh "${SSH_OPTIONS[@]}" -vvv "${MPI_USER}@${node_ip}" true \
            > "${client_log}" 2>&1
        set -e
        cp "${client_log}" "${DIAGNOSTIC_DIR}/ssh-to-${safe_node_ip}.log" 2>/dev/null || true
        echo "[diagnostic] verbose SSH client log:" >&2
        tail -n 200 "${client_log}" >&2 || true
        exit 1
    fi
done

HOSTFILE="${RUNTIME_DIR}/hostfile"
: > "${HOSTFILE}"
for node_ip in "${NODE_IPS[@]}"; do
    printf '%s slots=1\n' "${node_ip}" >> "${HOSTFILE}"
done

WORKLOAD=(
    "${LIGHTGBM_BIN}"
    "config=${RUNTIME_DIR}/train.conf"
    "data=${DATA_FILE}"
    "output_model=${OUTPUT_DIR}/lightgbm_model.txt"
    "num_machines=${SNOWFLAKE_JOBS_COUNT}"
    "num_iterations=${NUM_ITERATIONS}"
    "num_threads=${NUM_THREADS}"
)
mkdir -p "${OUTPUT_DIR}"
MPIRUN="$(command -v mpirun)"
MPI_BIN_DIR="$(cd "$(dirname "${MPIRUN}")" && pwd)"
MPI_PREFIX="$(dirname "${MPI_BIN_DIR}")"
OMPI_INFO="${MPI_BIN_DIR}/ompi_info"

# The PyPI wheel is installed under the active Python prefix. Point Open MPI
# at that real prefix so its bundled ORTE runtime finds its data files,
# components, executables, and libraries after wheel relocation.
export OPAL_PREFIX="${MPI_PREFIX}"

# Point the local launcher, remote ORTE daemon, and MPI ranks at the wheel's own
# Open MPI libraries ahead of anything the image exposes globally.
MPI_WHEEL_LIBRARY_PATH="${MPI_PREFIX}/lib:${MPI_PREFIX}/lib/openmpi"
export LD_LIBRARY_PATH="${MPI_WHEEL_LIBRARY_PATH}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"

MPI_SSH_ARGS="-4 -i ${PRIVATE_KEY} -p ${SSH_PORT} -o BatchMode=yes -o ConnectTimeout=5 -o ConnectionAttempts=1 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
MPI_ARGS=(
    --allow-run-as-root
    --prefix "${MPI_PREFIX}"
    --hostfile "${HOSTFILE}"
    --np "${SNOWFLAKE_JOBS_COUNT}"
    --map-by node
    --bind-to none
    --wd "${RUNTIME_DIR}"
    --mca plm rsh
    --mca plm_rsh_agent ssh
    --mca plm_rsh_args "${MPI_SSH_ARGS}"
    --mca plm_rsh_no_tree_spawn 1
    --mca plm_rsh_pass_libpath "${MPI_WHEEL_LIBRARY_PATH}"
    --mca oob_tcp_dynamic_ipv4_ports "${OOB_PORT_MIN}-${OOB_PORT_MAX}"
    --mca oob_tcp_if_include "${MPI_TCP_SUBNET}"
    --mca pml ob1
    --mca btl self,tcp
    --mca btl_tcp_port_min_v4 "${BTL_PORT_MIN}"
    --mca btl_tcp_port_range_v4 "${BTL_PORT_RANGE}"
    --mca btl_tcp_if_include "${MPI_TCP_SUBNET}"
    -x LD_LIBRARY_PATH
)

echo "[mpi] launcher versions"
echo "[mpi] installation prefix=${MPI_PREFIX}"
"${MPIRUN}" --version
"${OMPI_INFO}" --version

# Verify the Open MPI runtime independently of hostfile parsing and remote
# launch. The isolated PLM never invokes SSH and supports this one-rank check.
echo "[mpi] local isolated launcher smoke test"
LOCAL_SMOKE_CMD=(
    "${MPIRUN}"
    --allow-run-as-root
    --np 1
    --bind-to none
    --mca plm isolated
    /bin/hostname
)
set +e
"${LOCAL_SMOKE_CMD[@]}"
LOCAL_SMOKE_STATUS=$?
set -e
if [[ "${LOCAL_SMOKE_STATUS}" -ne 0 ]]; then
    echo "The local isolated Open MPI smoke test failed with status ${LOCAL_SMOKE_STATUS}." >&2
    exit "${LOCAL_SMOKE_STATUS}"
fi

# Exercise ORTE's complete remote-launch path before starting the workload.
# ML Jobs already assigns CPU resources to each instance, and LightGBM manages
# its own worker threads, so ORTE must not bind the rank to host topology that
# may be only partially visible through the container's cgroup.
echo "[mpi] local isolated launcher smoke test passed"
echo "[mpi] distributed SSH launcher smoke test"
DISTRIBUTED_SMOKE_CMD=(
    "${MPIRUN}"
    "${MPI_ARGS[@]}"
    --mca plm_base_verbose 10
    /bin/hostname
)
set +e
"${DISTRIBUTED_SMOKE_CMD[@]}"
SMOKE_STATUS=$?
set -e
if [[ "${SMOKE_STATUS}" -ne 0 ]]; then
    echo "The distributed ORTE launcher smoke test failed with status ${SMOKE_STATUS}." >&2
    echo "[diagnostic] ORTE rsh launcher parameters:" >&2
    "${OMPI_INFO}" --param plm rsh --level 9 >&2 || true
    exit "${SMOKE_STATUS}"
fi

echo "[mpi] distributed launcher smoke test passed"
STARTED_AT="$(date +%s)"
set +e
"${MPIRUN}" "${MPI_ARGS[@]}" "${WORKLOAD[@]}"
STATUS=$?
set -e

FINISHED_AT="$(date +%s)"
cat > "${OUTPUT_DIR}/run_metadata.json" <<EOF
{
  "dataset": "UCI HIGGS",
  "elapsed_seconds": $((FINISHED_AT - STARTED_AT)),
  "global_row_limit": ${MAX_ROWS},
  "mpi_processes": ${SNOWFLAKE_JOBS_COUNT},
  "num_iterations": ${NUM_ITERATIONS},
  "status": ${STATUS}
}
EOF

for worker_ip in "${NODE_IPS[@]:1}"; do
    python control.py send \
        --host "${worker_ip}" \
        --port "${CONTROL_PORT}" \
        --status "${STATUS}" || STATUS=1
done
WORKERS_NOTIFIED=1
exit "${STATUS}"
