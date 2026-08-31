# PrimeRL Reverse Text with a Two-Node Multi-Role Topology

## Overview

This sample runs PrimeRL's official reverse-text Quick Run on two `GPU_NV_S` ML Job instances. Its purpose is to demonstrate that native per-instance execution with `parallel=True` can run a real multi-role PrimeRL workflow without converting the roles into head-created ML Job tasks.

PrimeRL is a natural multi-role example: policy inference generates rollouts, an environment server scores them, the orchestrator batches the rollouts and coordinates weight versions, and the trainer updates the policy.

The sample assigns roles by instance index:

| Instance | Roles | GPU use |
| --- | --- | --- |
| 0 | PrimeRL inference server | One inference GPU |
| 1 | Trainer, orchestrator, and reverse-text environment server | One trainer GPU |

NCCL transfers updated weights from the trainer to inference, ZMQ sends rollouts from the orchestrator to the trainer, and HTTP connects the orchestrator to inference. The payload dispatcher provides role placement, addresses, lifecycle monitoring, status propagation, and artifact collection; the PrimeRL components retain their native interfaces.

The defaults match the upstream Quick Run: the `PrimeIntellect/Qwen3-0.6B-Reverse-Text-SFT` model trains for 20 GRPO steps with batch size 128, group size 16, a 2K training context, and at most 128 generated tokens per rollout.

This is a distributed multi-role RL example, not a multi-GPU trainer example. Scaling the trainer with FSDP or adding inference replicas is intentionally left to applications that need it.

## Files

| Path | Purpose |
| --- | --- |
| `requirements.txt` | Local dependency for submitting the job |
| `scripts/submit_job.py` | Submits, waits for, and reports the two-instance job |
| `src/requirements.txt` | Installs `uv` in the managed job runtime |
| `src/rl.toml` | Official reverse-text Quick Run adapted to external role placement |
| `src/dispatch.py` | Bootstraps PrimeRL in a child process and assigns roles, ports, lifecycle, logs, and distributed status |

## How It Works

Both instances receive the same ordered node roster and exported port range. `dispatch.py` deterministically assigns six ports for inference HTTP, NCCL weight broadcast, ZMQ rollout transport, the environment server, the trainer rendezvous, and cross-role completion signaling.

Before starting a role, each instance:

1. clones PrimeRL at the pinned commit and installs its locked GPU and `reverse-text` workspace dependencies;
2. downloads the pinned SFT-warmed model from Hugging Face;
3. writes an effective PrimeRL config that points to the local model snapshot;
4. runs PrimeRL's `rl --dry-run` path to validate the config and produce official resolved component JSON files.

The dispatcher selects the NCCL and GLOO interface that owns the instance's exported node IP. The training instance waits for both the inference `/health` and `/v1/models` endpoints before starting the remaining roles.

Instance 0 starts one vLLM inference worker. Instance 1 starts `env-server`, `orchestrator`, and a one-process `torchrun` trainer. The trainer and orchestrator are terminal roles: when both finish, instance 1 signals instance 0 to stop inference and return the same status. A failure in any managed role terminates its peers and makes the distributed result fail.

This replaces PrimeRL's local or SLURM role placement, not its RL implementation. The model, GRPO algorithm, vLLM server, reverse-text verifier, weight broadcast, and rollout transport come from the pinned upstream repository.

## Prerequisites

Complete the shared [Snowflake account and local setup](../README.md#prerequisites). This sample requires:

- a `GPU_NV_S` compute pool with at least two available nodes;
- one visible NVIDIA GPU on each node;
- a Python 3.12 Snowflake Container Runtime with GPU support;
- an external access integration that allows PyPI, GitHub, the PyTorch package index, and Hugging Face.

For a dedicated two-node pool:

```sql
CREATE COMPUTE POOL IF NOT EXISTS PRIME_RL_GPU_S_POOL
  MIN_NODES = 2
  MAX_NODES = 2
  INSTANCE_FAMILY = GPU_NV_S;

GRANT USAGE ON COMPUTE POOL PRIME_RL_GPU_S_POOL TO ROLE <role_name>;
```

The dispatcher checks for at least one visible GPU before resolving or starting the PrimeRL components.

The following SQL configures the sample-specific outbound hosts. Replace names and grants to match your environment:

```sql
CREATE OR REPLACE NETWORK RULE PRIME_RL_NETWORK_RULE
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = (
    'github.com:443',
    'release-assets.githubusercontent.com:443',
    'download.pytorch.org:443',
    'download-r2.pytorch.org:443',
    'huggingface.co:443',
    '*.aws.cdn.hf.co:443'
  );

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION PRIME_RL_EAI
  ALLOWED_NETWORK_RULES = (
    SNOWFLAKE.EXTERNAL_ACCESS.PYPI_RULE,
    PRIME_RL_NETWORK_RULE
  )
  ENABLED = TRUE;

GRANT USAGE ON INTEGRATION PRIME_RL_EAI TO ROLE <role_name>;
```

The dispatcher checks the Python version before cloning PrimeRL. `--runtime-environment` must identify a Snowflake Container Runtime compatible with ML Jobs; an arbitrary CUDA or Python image does not provide the ML Jobs runtime contract.

## How to Run

### Install the Local Dependency

From this directory:

```bash
python -m pip install -r requirements.txt
```

### Submit the Job

If the account's default GPU runtime uses Python 3.12:

```bash
python scripts/submit_job.py \
  --compute-pool PRIME_RL_GPU_S_POOL \
  --stage-name DISTRIBUTED_TRAINING_STAGE \
  --external-access-integration PRIME_RL_EAI
```

Otherwise, select an available Python 3.12 Snowflake Container Runtime:

```bash
python scripts/submit_job.py \
  --compute-pool PRIME_RL_GPU_S_POOL \
  --stage-name DISTRIBUTED_TRAINING_STAGE \
  --runtime-environment <python_3_12_runtime> \
  --external-access-integration PRIME_RL_EAI
```

The topology is intentionally fixed at two single-GPU instances. The script requests both instances as the minimum, performs a wiring preflight, waits for both entrypoints, and prints the distributed result.

The default command runs all 20 upstream Quick Run steps. For a shorter end-to-end smoke test:

```bash
python scripts/submit_job.py \
  --compute-pool PRIME_RL_GPU_S_POOL \
  --external-access-integration PRIME_RL_EAI \
  --max-steps 1
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

A successful run reports one zero exit code for each role instance:

```text
Distributed result: {
  'success': True,
  'exit_codes': {0: 0, 1: 0},   # each role instance's exit code
  'failed_instance': None,
  'return_value': None          # None; dispatch.py reports status via exit code
}
```

### Output Files

Each role copies its resolved configs, component logs, and local PrimeRL output under a relative `output/prime_rl/`. The payload runs from the `app/` directory of this job's folder on the stage you passed as `--stage-name`, so the files land there:

```text
app/output/prime_rl/
├── inference/                                    # instance 0: vLLM inference
│   ├── component_logs/inference.log
│   └── run/snowflake-prime-rl/configs/resolved/
└── trainer_orchestrator/                         # instance 1: trainer, orchestrator, env-server
    ├── component_logs/
    │   ├── env-server-0.log
    │   ├── orchestrator.log
    │   └── trainer.log
    └── run/snowflake-prime-rl/
        ├── checkpoints/step_20/
        ├── configs/resolved/
        └── metrics.jsonl
```

The checkpoint step matches `--max-steps`. Component logs show rollout generation, reverse-text rewards, trainer loss, weight versions, and the final completion signal.

ML Jobs assigns the per-job folder name, so locate the files by pattern (or browse the stage in Snowsight):

```bash
snow sql --query "LS @<stage_name> PATTERN='.*prime_rl.*';"
```

### View Logs

```python
from snowflake.ml.jobs import get_job

job = get_job("<job_id>")
print("Inference role")
print(job.get_logs(instance_id=0))
print("Trainer and orchestrator roles")
print(job.get_logs(instance_id=1))
```

## Adapt the Dispatcher to Your Repository

An application-specific multi-role repository normally needs a small dispatcher rather than a training-code rewrite:

1. Define a deterministic mapping from `SNOWFLAKE_JOB_INDEX` to repository roles.
2. Allocate every inter-node listener consistently from `MLRS_EPHEMERAL_PORT_MIN` through `MLRS_EPHEMERAL_PORT_MAX`.
3. Build service URLs and transport endpoints from the ordered `MLRS_NODE_IPS` roster.
4. Start each role with the repository's native entrypoint and configuration.
5. Distinguish terminal roles from support roles, monitor early failures, and define a clean shutdown protocol.
6. Copy role-specific logs and artifacts to non-conflicting stage paths.
7. Submit with `parallel=True`, a fixed `min_instances`, and the wiring preflight.

The wiring preflight is a c10d connectivity and collective check that runs before the workload starts. It does not validate PrimeRL's HTTP, ZMQ, or full multi-role protocol.

The two-node mapping is a compact reference, not a general PrimeRL scheduler. Applications can extend the role map and use PrimeRL's multi-node inference and trainer settings while preserving the same address and lifecycle principles.

## Configuration and Reproducibility

The sample pins its repository and remote model:

| Asset | Revision | License |
| --- | --- | --- |
| PrimeRL | `9e3be00b39df5c4f917a95d8296e6239a6179a35` | Apache 2.0 |
| `PrimeIntellect/Qwen3-0.6B-Reverse-Text-SFT` | `c97a910849ec6aa962add3dc253a0817d61c0210` | Apache 2.0 |

See the [PrimeRL repository](https://github.com/PrimeIntellect-ai/prime-rl) and [model card](https://huggingface.co/PrimeIntellect/Qwen3-0.6B-Reverse-Text-SFT) for attribution and full terms.

`dispatch.py` clones the exact PrimeRL commit, installs from its frozen `uv.lock`, downloads the exact model revision, and substitutes the local snapshot path before PrimeRL resolves the component configs. The trainer-orchestrator instance also installs PrimeRL's separately locked `flash-attn` extra and verifies that its CUDA extension imports before starting either role.

The first run on each instance downloads and installs a large GPU dependency set. The upstream claim that the first trainer step appears within a minute assumes that dependencies and model assets are already available; ML Job provisioning and bootstrap time are additional. For repeated production runs, bake the pinned PrimeRL environment into a compatible Snowflake Container Runtime.

## Troubleshooting

### SPCS Reports `DONE` but `distributed_result()` Reports Missing Records

A run can finish every PrimeRL component successfully and still raise an error similar to:

```text
Instance 0 wrote no result record (likely killed or OOM before it could report)
Distributed job failed: 2/2 instances did not exit 0
```

The sample submits `entrypoint="dispatch.py"` directly. Its bootstrap runs the dispatcher from PrimeRL's locked virtual environment as a child process and waits for its exit status. This preserves the parent ML Job launcher so it can write the per-instance result records that `distributed_result()` expects.

If you see this missing-record error, treat it as a real failure signal and inspect the SPCS status and both instance logs for a platform kill, OOM, or failure to write to the output stage.

### PrimeRL Bootstrap Fails

Confirm that the runtime uses Python 3.12 and that `PRIME_RL_EAI` allows GitHub, release assets, the PyTorch index, and PyPI. The log identifies the pinned commit before `uv sync --frozen`; a lock or dependency error generally means the selected runtime platform is incompatible with that upstream lock.

### One Role Waits Indefinitely

Inspect both instance logs. PrimeRL intentionally has long-lived support roles, so the first useful signal is usually an earlier inference, environment, orchestrator, or trainer error on the other instance. Keep the wiring preflight enabled and verify that the compute pool allocated both nodes.

### A Listener Cannot Bind or Connect

Confirm that at least six ports are available in the exported ephemeral range and that no repository override reuses one. `dispatch.py` prints the selected role addresses at startup.

### Hugging Face Download Fails

Confirm that the EAI is granted to the submitting role and allows all Hugging Face hosts listed above. The sample disables the native Xet client, so Hugging Face can redirect large-file downloads through a regional CDN host such as `us.aws.cdn.hf.co`.

### CUDA Out of Memory

Confirm that each role reports one visible GPU and that the trainer starts one rank. The bundled 0.6B model is selected specifically for a single `GPU_NV_S` trainer. The model, sequence length, and completion length are set in `src/dispatch.py` and `src/rl.toml`; if a larger model or context does not fit, reduce those values there or select a larger instance family.

## Cost and Cleanup

The default run holds two `GPU_NV_S` nodes and downloads the model independently on both nodes. Compute and network costs depend on the selected instance family and account configuration. Suspend or drop dedicated compute resources when finished, and remove run artifacts from the stage according to your retention policy.
