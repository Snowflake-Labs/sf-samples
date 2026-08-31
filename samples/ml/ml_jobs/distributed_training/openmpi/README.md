# Open MPI with Job-Scoped Launch Agents

## Overview

This sample trains an MPI-enabled LightGBM binary classifier on a bounded partition of the UCI HIGGS dataset. It demonstrates how a repository whose standard entrypoint is `mpirun` can run on direct per-instance ML Jobs even though ML Jobs does not configure SSH or MPI launch agents between instances.

LightGBM's data-parallel tree learner is a representative MPI workload: every MPI rank reads a distinct pre-partitioned slice, participates in distributed histogram reduction, and contributes to one trained model.

- every instance streams its own deterministic partition of HIGGS;
- `launch.sh` starts an SSH daemon scoped to the job's private key and exported port range;
- instance 0 builds a conventional hostfile and starts `mpirun`;
- MPI-enabled LightGBM contains no Snowflake-specific code;
- completion signaling propagates the head process result to every per-instance entrypoint.

The default configuration reads the first 100,000 HIGGS examples and trains 20 boosting iterations. It validates remote process launch, MPI communication, distributed data loading, and model output without downloading the entire 11-million-row dataset.

## Files

| Path | Purpose |
| --- | --- |
| `requirements.txt` | Local dependency for submitting the job |
| `scripts/submit_job.py` | Creates a temporary SSH key, submits the job, and reports its distributed result |
| `src/requirements.txt` | Installs the Open MPI distribution and LightGBM build tools |
| `src/build_lightgbm.py` | Downloads, verifies, and builds the pinned MPI-enabled LightGBM CLI |
| `src/launch.sh` | Starts launch agents and invokes `mpirun` from instance 0 |
| `src/control.py` | Propagates completion status from the head to worker entrypoints |
| `src/prepare_data.py` | Streams and deterministically partitions HIGGS |
| `src/train.conf` | LightGBM data-parallel training configuration |

The `keys` directory is absent from the repository. It is generated in a temporary copy of `src` immediately before submission.

## How It Works

This is an agent-based launcher, so the topology adapter has more responsibilities than the no-SSH DeepSpeed sample:

1. `submit_job.py` creates a new Ed25519 key pair in a temporary payload for each submission. Each instance copies the private key from the stage mount to node-local storage and applies mode `0600` before invoking OpenSSH.
2. The managed runtime installs the official Open MPI wheel, CMake, and Ninja from `src/requirements.txt`.
3. Every entrypoint verifies and builds the pinned LightGBM source distribution with `USE_MPI=ON`, prepares its HIGGS partition, and starts and locally verifies `sshd` on `MLRS_EPHEMERAL_PORT_MIN`.
4. Instance 0 waits until every launch agent is reachable, writes the ordered `MLRS_NODE_IPS` roster to an Open MPI hostfile, and runs local-isolated and two-node SSH `hostname` smoke tests through `mpirun` before starting LightGBM.
5. The launcher sets Open MPI's prefix to the wheel's actual installation location and explicitly selects ORTE's `rsh` launcher. It gives ORTE's OOB control plane and Open MPI's TCP data plane two small disjoint sub-ranges of the exported ephemeral ports, and pins both to the instance subnet inferred from `MLRS_HEAD_IP`.
6. When `mpirun` exits, instance 0 sends the same status to the waiting entrypoint on every worker.

SSH authenticates only with the job-scoped key and listens only for the lifetime of the payload. Host-key checking is disabled because each node also creates an ephemeral host key. This is user-provided launcher infrastructure; ML Jobs allocates and connects the nodes but does not provide SSH.

The private key is never committed to the repository, but the generated copy is uploaded with the job payload and remains in the payload stage until that staged payload is removed. Apply the same stage access controls and retention policy used for other job-scoped credentials.

## Prerequisites

Complete the shared [Snowflake account and local setup](../README.md#prerequisites). This sample needs:

- a CPU compute pool with at least two available nodes;
- a Snowflake Container Runtime with Python package installation, a C++ compiler, and OpenSSH server support;
- an EAI that allows PyPI startup dependencies and the UCI Machine Learning Repository;
- a local OpenSSH client with `ssh-keygen`.

For example, create a CPU compute pool with SQL similar to:

```sql
CREATE COMPUTE POOL IF NOT EXISTS DISTRIBUTED_CPU_POOL
  MIN_NODES = 2
  MAX_NODES = 2
  INSTANCE_FAMILY = CPU_X64_M;

GRANT USAGE ON COMPUTE POOL DISTRIBUTED_CPU_POOL TO ROLE <role_name>;
```

Configure dataset access separately:

```sql
CREATE OR REPLACE NETWORK RULE UCI_HIGGS_NETWORK_RULE
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('archive.ics.uci.edu:443');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION PYPI_UCI_HIGGS_EAI
  ALLOWED_NETWORK_RULES = (
    SNOWFLAKE.EXTERNAL_ACCESS.PYPI_RULE,
    UCI_HIGGS_NETWORK_RULE
  )
  ENABLED = TRUE;

GRANT USAGE ON INTEGRATION PYPI_UCI_HIGGS_EAI TO ROLE <role_name>;
```

## How to Run

### Install the Local Dependency

From this directory:

```bash
python -m pip install -r requirements.txt
```

### Submit the Job

```bash
python scripts/submit_job.py \
  --compute-pool DISTRIBUTED_CPU_POOL \
  --stage-name DISTRIBUTED_TRAINING_STAGE \
  --target-instances 2 \
  --external-access-integration PYPI_UCI_HIGGS_EAI
```

The script performs a wiring preflight, generates the one-use SSH key, submits the temporary payload, waits for completion, and prints the distributed result. The temporary local payload is deleted after its upload is complete.

To exercise more of the dataset or change the bounded training run:

```bash
python scripts/submit_job.py \
  --compute-pool DISTRIBUTED_CPU_POOL \
  --external-access-integration PYPI_UCI_HIGGS_EAI \
  --max-rows 1000000 \
  --num-iterations 100
```

Use `--no-wait` to return immediately after submission.

## View Results

After the job finishes, call `distributed_result()` for the structured outcome:

```python
from snowflake.ml.jobs import get_job

job = get_job("<job_id>")
result = job.distributed_result()
print(result)
```

A successful run reports one zero exit code per instance:

```text
Distributed result: {
  'success': True,
  'exit_codes': {0: 0, 1: 0},   # each instance's exit code
  'failed_instance': None,
  'return_value': None          # None for a list-form entrypoint
}
```

### Output Files

Instance 0 writes the trained model and run metadata to `output/openmpi/` under the `app/` directory of this job's folder on the stage you passed as `--stage-name`:

```text
app/output/openmpi/
├── instance-0/sshd.log        # per-instance SSH server/client diagnostics
├── instance-1/sshd.log
├── lightgbm_model.txt         # trained LightGBM model (text format)
└── run_metadata.json          # run summary: MPI processes, iterations, row limit, status, elapsed
```

The instance 0 log contains LightGBM's per-iteration binary log loss and AUC. Each instance log also reports its local HIGGS row count, launch-agent address, and final propagated status.

ML Jobs assigns the per-job folder name, so locate the files by pattern (or browse the stage in Snowsight):

```bash
snow sql --query "LS @<stage_name> PATTERN='.*openmpi.*';"
```

### View Logs

```python
from snowflake.ml.jobs import get_job

job = get_job("<job_id>")
for instance_id in range(2):
    print(f"Instance {instance_id}")
    print(job.get_logs(instance_id=instance_id))
```

## Adapt the Launcher to Your Repository

Use this recipe only when the repository genuinely depends on agent-based remote process creation and cannot use direct, no-SSH, or per-instance startup.

1. Put the repository's MPI distribution and build or runtime dependencies in `src/requirements.txt`, or select a compatible Snowflake Container Runtime that already contains them.
2. Keep the job-scoped key generation and launch-agent readiness checks.
3. Replace the `lightgbm` command and hostfile slot count with the repository's normal `mpirun` command.
4. Preserve the port layout: SSH, completion control, and two small disjoint sub-ranges for ORTE OOB and the TCP BTL, all within the exported ephemeral range, plus the subnet pinning for ORTE and the BTL.
5. Preserve head-to-worker completion signaling so every ML Job entrypoint returns a consistent status.
6. Submit the directory with `parallel=True`, `min_instances` equal to `target_instances`, and the framework-agnostic wiring preflight.

Libraries and launchers must be ABI-compatible across all nodes. If the repository supports a no-SSH launcher or can start one process directly on every instance, that simpler pattern generally has fewer moving parts.

## Configuration and Reproducibility

| Asset | Version or source | License |
| --- | --- | --- |
| Open MPI | PyPI wheel `4.1.8` | Open MPI license and bundled third-party terms |
| LightGBM | PyPI source distribution `4.7.0`, SHA-256 `f8e20f682c9aabd000bcf4a7ed8aa6f473c1adfecccae34ec24e823d156f4af0` | MIT |
| HIGGS | UCI dataset 280 | CC BY 4.0 |

See the [LightGBM repository](https://github.com/lightgbm-org/LightGBM) and [HIGGS dataset page](https://archive.ics.uci.edu/dataset/280/higgs) for source, attribution, and full terms.

HIGGS is ordered deterministically before partitioning: row `i` is assigned to MPI rank `i % world_size`. `--max-rows` bounds the global prefix rather than the size of each partition. Run `python scripts/submit_job.py --help` for the complete set of submission options.

The source checksum and pinned Open MPI wheel make every node build against the same interfaces. The PyPI source archive contains a Python package directory named `lightgbm`, so `build_lightgbm.py` redirects the same-named CLI executable into the CMake build directory before linking. For repeated production jobs, prebuild this MPI-enabled LightGBM binary in a compatible Snowflake Container Runtime to avoid paying the per-job compile cost.

## Troubleshooting

### A Worker Waits for Completion

Inspect instance 0 first. A head failure before `mpirun` starts usually means the LightGBM build failed, a launch agent was unreachable, or the exported port range was too small. Worker control waits are bounded at two hours so they eventually return a failure instead of waiting forever.

### SSH Authentication Fails

Submit with `scripts/submit_job.py`; do not upload `src` directly. The submission script is responsible for packaging the matching private key and `authorized_keys` entry. Confirm that the selected Snowflake Container Runtime contains `/usr/sbin/sshd` and can start it with sufficient privileges.

A startup failure prints both the job-scoped `sshd.log` and a verbose SSH client attempt in that instance's job log, and copies per-instance SSH diagnostics under `app/output/openmpi/instance-<index>/` on the stage. When remote readiness fails, instance 0 separately tests raw TCP connectivity so a network failure can be distinguished from an SSH handshake or authentication failure.

### HIGGS Download Fails

Confirm that `PYPI_UCI_HIGGS_EAI` is granted to the submitting role and allows `archive.ics.uci.edu:443`. Every node streams the same compressed prefix and retains only its own rows, so increasing `--max-rows` increases network use on every instance.

### LightGBM Reports That It Is Not Built with MPI

Inspect the `build_lightgbm.py` output and confirm CMake found the `mpicc` and `mpicxx` installed by the Open MPI wheel. The ordinary PyPI LightGBM binary wheel is not a substitute for the MPI-enabled CLI built by this sample.

## Cost and Cleanup

The default run uses two CPU nodes, compiles LightGBM once on each node, and streams the compressed HIGGS prefix independently on each node. Compute and network costs depend on the selected instance family and account configuration. Suspend or drop dedicated compute resources when you finish, and remove models or payloads from the stage according to your retention policy.
