# Run Existing Distributed Training Codebases on Snowflake ML Jobs

These samples demonstrate how to run an existing distributed training codebase on [Snowflake ML Jobs](https://docs.snowflake.com/developer-guide/snowflake-ml/ml-jobs/overview) while keeping its native launcher, training strategy, and entrypoint.

Here, a distributed training codebase means training code that is already designed to launch and coordinate work across multiple nodes through tools such as `torchrun`, `accelerate launch`, `deepspeed`, or `mpirun`.

These samples use direct per-instance execution. ML Jobs invokes the same entrypoint on every allocated instance and provides the static topology needed by the codebase's launcher. This lets an existing codebase keep its launcher instead of restructuring its training logic around a head process. ML Jobs provides the multi-node execution environment; the codebase and its distributed framework still implement behavior such as DDP, FSDP, or ZeRO.

For detailed guidance about this execution mode, see [Direct per-instance execution for multi-node ML Jobs](https://docs.snowflake.com/en/developer-guide/snowflake-ml/ml-jobs/direct-per-instance-execution). For a comparison with the default head/worker mode, see [Multi-Node Capabilities](../README.md#multi-node-capabilities).

## Overview

### What is Direct Per-Instance Execution?

Setting `parallel=True` when submitting an ML Job starts the payload entrypoint once on every allocated instance:

```python
jobs.submit_directory(
    payload_dir,
    entrypoint=["bash", "launch.sh"],
    compute_pool=compute_pool,
    stage_name=stage_name,
    target_instances=2,
    parallel=True,
    # ...
)
```

The entrypoint can be a Python file or a list containing a command and its arguments. This sample uses the list form to launch a Bash adapter.

Each entrypoint receives its instance index, the number of instances, and the same ordered node roster. This gives a distributed training codebase enough information to construct its own process topology without a Snowflake-specific head-to-worker task layer.

### Why Bring Your Own Distributed Training Codebase?

Many distributed training codebases already define how processes are launched and coordinated. Direct per-instance execution lets the codebase retain:

- its normal training entrypoint and command-line arguments;
- its launcher or role dispatcher;
- its data-, model-, and pipeline-parallel strategies;
- its framework and model dependencies;
- its checkpointing and artifact behavior.

Keep the ML Job integration in a small launcher or dispatcher adapter. The adapter translates the ML Job topology into the interface expected by the codebase, while the training code remains framework-native and does not need Snowflake-specific imports.

## Key Features

### Static Topology Contract

Every entrypoint receives the same topology with its own instance index:


| Environment variable      | Meaning                                              |
| ------------------------- | ---------------------------------------------------- |
| `SNOWFLAKE_JOB_INDEX`     | Zero-based index of the current instance             |
| `SNOWFLAKE_JOBS_COUNT`    | Number of instances in the job                       |
| `MLRS_HEAD_IP`            | IP address of instance 0                             |
| `MLRS_NODE_IPS`           | Comma-separated IP addresses in instance-index order |
| `MLRS_RDZV_PORT`          | Port available for the workload rendezvous           |
| `MLRS_EPHEMERAL_PORT_MIN` | Lower bound of the available workload port range     |
| `MLRS_EPHEMERAL_PORT_MAX` | Upper bound of the available workload port range     |


Keep these variables in a small launcher or dispatcher. That adapter can map them to standard framework inputs such as `--nnodes`, `--node-rank`, `--master-addr`, and `--master-port`, while the training code continues to use the framework's standard distributed interface.

Use `MLRS_RDZV_PORT` when the workload needs a single rendezvous endpoint. Workloads that need multiple listeners, such as multi-role applications or agent-based launchers, can allocate ports from `MLRS_EPHEMERAL_PORT_MIN` through `MLRS_EPHEMERAL_PORT_MAX`. The launcher or dispatcher is responsible for assigning ports consistently and avoiding collisions within the job.

### Preflight Validation

Use a preflight to validate the distributed environment before the workload starts:

```python
job = jobs.submit_directory(
    payload_dir,
    entrypoint=["bash", "launch.sh"],
    parallel=True,
    preflight="wiring",  # Or "reference".
    # ...
)
```

The two preflight levels validate c10d-based infrastructure before the workload starts:

- `wiring` uses a PyTorch c10d `TCPStore` rendezvous and then runs a small Gloo or NCCL `all_reduce`.
- `reference` includes the `wiring` checks and, on GPU runtimes, times one synthetic DDP training step. CPU-only pools skip that extra step.

A successful preflight confirms the corresponding c10d connectivity and collective path. It does not validate the user launcher, training code, HTTP or ZMQ services, or other application-specific protocols.

### Distributed Results

Use `distributed_result()` to wait for the job and retrieve a structured result across the target instances:

```python
result = job.distributed_result()
print(result.success)
print(result.exit_codes)
print(result.failed_instance)
```

The distributed result contains:


| Field | Meaning |
| --- | --- |
| `success` | Whether every target instance exited successfully |
| `exit_codes` | Mapping from instance index to that instance's exit code |
| `failed_instance` | Earliest-failing instance, or `None` on success |
| `return_value` | Return value recorded by instance 0 on success; list-form commands normally return `None` |


If any instance doesn't exit successfully, ML Jobs raises `DistributedJobError` and makes the same structured result available through `error.result`.

## Execution Patterns

The four patterns below demonstrate how different distributed training codebases can use direct per-instance execution while retaining their existing launch and coordination logic.

Each directory pairs one of these patterns with a representative workload. The bounded defaults are intended to validate a real execution path and produce real artifacts, not to benchmark model quality or distributed scaling.

| Pattern | How it works | Reference workload |
| --- | --- | --- |
| Static rendezvous | Each instance starts `torchrun` with a shared rendezvous endpoint and its own node rank | [PyTorch DDP](./pytorch_ddp): Qwen3-0.6B continued pretraining on WikiText-103 |
| No-SSH hostfile launch | Each instance starts DeepSpeed with the same hostfile and its own node rank | [DeepSpeed](./deepspeed): full-parameter instruction tuning of Qwen3-1.7B on Dolly-15k with ZeRO-3 |
| SSH-based `mpirun` launch | Instance 0 uses a generated hostfile and job-scoped SSH to launch MPI processes on every instance | [Open MPI](./openmpi): distributed LightGBM gradient boosting on the HIGGS dataset |
| Multi-role topology | A dispatcher assigns different application roles to fixed instances and coordinates their lifecycle | [PrimeRL](./prime_rl): reverse-text Quick Run with Qwen3-0.6B, vLLM inference, a GRPO trainer, and a reverse-text environment server |

Start with PyTorch DDP for a standard `torchrun` pattern or DeepSpeed for no-SSH hostfile launch. Open MPI and PrimeRL demonstrate more advanced integration patterns.

A meta-launcher maps onto one of these patterns rather than adding a new one. For example, `accelerate launch` builds a static `torchrun` rendezvous, so an Accelerate codebase follows the same topology mapping as [`pytorch_ddp`](./pytorch_ddp) — see that sample's "Adapt the Launcher to Your Codebase" section.

## Prerequisites

### Snowflake Account Setup

Work with your account administrator to provision a compute pool and stage. The following example creates a two-node GPU compute pool and a shared stage; adjust the node count and instance family as documented by the selected sample:

```sql
CREATE COMPUTE POOL IF NOT EXISTS DISTRIBUTED_GPU_POOL
  MIN_NODES = 2
  MAX_NODES = 2
  INSTANCE_FAMILY = GPU_NV_S;

CREATE STAGE IF NOT EXISTS DISTRIBUTED_TRAINING_STAGE;

GRANT USAGE ON COMPUTE POOL DISTRIBUTED_GPU_POOL TO ROLE <role_name>;
GRANT READ, WRITE ON STAGE DISTRIBUTED_TRAINING_STAGE TO ROLE <role_name>;
```

The role also needs `USAGE` on the database and schema containing the stage. Individual samples document any additional external access integrations they require.

### External Access

Inter-instance rendezvous and collective communication do not require an external access integration. An integration is needed only when the payload accesses an external service such as PyPI, Hugging Face, GitHub, or an experiment tracker. Each sample documents its own outbound access requirements.

### Local Setup

Configure a Snowflake connection for Snowpark Python, then install Snowflake ML Python 1.54.0 or later:

```bash
pip install "snowflake-ml-python>=1.54.0"
```

You still need to select a Snowflake Container Runtime that supports direct
per-instance execution and the requested preflight mode. Upgrading the local
SDK does not upgrade the runtime image used by the job.

The individual sample READMEs provide their additional local dependencies and execution commands.

## How to Run

### Step 1: Choose a Sample

Choose a sample from the [execution patterns](#execution-patterns) above and change to its directory. Each sample README documents its workload, external access, runtime, and resource requirements.

### Step 2: Install the Local Dependency

From the selected sample directory:

```bash
python -m pip install -r requirements.txt
```

### Step 3: Submit the Job

```bash
python scripts/submit_job.py \
  --compute-pool DISTRIBUTED_GPU_POOL \
  --stage-name DISTRIBUTED_TRAINING_STAGE
```

Add any sample-specific options documented in its README, such as an external access integration, runtime environment, or target instance count. Each submission script packages its payload, enables `parallel=True`, runs the appropriate preflight, waits for the distributed job, and reports the result from every instance.

These samples use a fixed world size. Their submission scripts set `min_instances` equal to `target_instances` so the entrypoints start only after the complete requested topology is available.

## Monitor Your Job

Use the following options to monitor a direct per-instance job:

- **Job status:** Use `job.status`, `job.wait()`, or the ML Job page in Snowsight to check the overall job state.
- **Per-instance logs:** Use `job.get_logs(instance_id=i)` to inspect launcher and workload output. Add `verbose=True` for runtime and bootstrap details.
- **Distributed results:** Use `distributed_result()` for the per-instance outcome described in [Distributed Results](#distributed-results). If it raises `DistributedJobError`, inspect `error.result` and the failed instance's logs.
- **Ray Dashboard:** Use it for infrastructure-level visibility while the job is running. Use per-instance logs to monitor launcher and workload progress, and distributed results to inspect per-instance outcomes after the job completes.

## Limitations

- `parallel=True` exposes a fixed topology for the lifetime of the job. Elastic membership and dynamic replacement of a failed instance are not supported.
- Multi-node `torchrun` must use a static rendezvous. Pass the instance 0 IP and port with `--master-addr` and `--master-port`, or use `--rdzv-endpoint` with torchrun's default static backend, as `pytorch_ddp` does. Dynamic rendezvous isn't supported.

## Troubleshooting

### Only One Entrypoint Starts

Confirm that the compute pool has enough available nodes for `target_instances`. Then confirm that the submission requests more than one instance, sets `parallel=True`, and has `min_instances` equal to `target_instances` for a fixed world size.

### A c10d-Based Launcher Waits Indefinitely

Run the wiring preflight before the workload. Verify that every instance maps the same head address and rendezvous port, and that each instance uses its own `SNOWFLAKE_JOB_INDEX` as the node rank.

### Dependency or Model Downloads Fail

Confirm that the job is configured with an external access integration and that its network rules allow every external host used by the codebase.
