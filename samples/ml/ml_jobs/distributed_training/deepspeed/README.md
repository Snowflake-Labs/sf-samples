# DeepSpeed ZeRO-3 with a No-SSH Hostfile

## Overview

This sample performs full-parameter instruction tuning of a pinned Qwen3-1.7B model on a pinned Dolly-15k dataset subset with DeepSpeed ZeRO-3. It demonstrates how an existing DeepSpeed repository can keep its hostfile-based launcher without configuring SSH between ML Job instances.

ZeRO-3 is a natural fit for this workload because it partitions parameters, gradients, and optimizer state across the distributed world instead of replicating the complete training state on every GPU.

- `train.py` uses the standard DeepSpeed engine and contains no Snowflake-specific imports;
- `launch.sh` converts the ML Job node roster into a DeepSpeed hostfile;
- every instance invokes the same `deepspeed --no_ssh` command with its own node rank;
- `submit_job.py` enables direct per-instance execution with `parallel=True`.

The default configuration reads at most 2,048 examples and runs only 20 optimizer steps. It validates real ZeRO-3 initialization, training, communication, and checkpoint consolidation; it is not a model-quality or scaling benchmark.

## Files

| Path | Purpose |
| --- | --- |
| `requirements.txt` | Local dependency for submitting the job |
| `scripts/submit_job.py` | Submits, waits for, and reports the distributed job |
| `src/launch.sh` | Builds the hostfile and starts DeepSpeed in no-SSH mode |
| `src/train.py` | Framework-standard instruction-tuning workload |
| `src/zero3.json` | ZeRO-3 optimizer and communication configuration |
| `src/requirements.txt` | Dependencies installed in the job runtime |

## How It Works

The launcher writes one hostfile row for each address in `MLRS_NODE_IPS`:

```text
10.0.0.4 slots=1
10.0.0.5 slots=1
```

It then maps the ML Job topology to DeepSpeed's no-SSH launcher:

| ML Job value | DeepSpeed input |
| --- | --- |
| `MLRS_NODE_IPS` | Ordered hostfile entries |
| `SNOWFLAKE_JOB_INDEX` | `--node_rank` |
| `MLRS_HEAD_IP` | `--master_addr` |
| `MLRS_RDZV_PORT` | `--master_port` |

Unlike the default DeepSpeed SSH launcher, no process remotely creates a worker. Direct per-instance execution has already started the launcher once on every node, and `--no_ssh` joins those launchers into one fixed world.

## Prerequisites

Complete the shared [Snowflake account and local setup](../README.md#prerequisites). This sample requires a GPU compute pool with at least two available nodes and an external access integration that allows PyPI and Hugging Face.

The following SQL configures the sample-specific external access. Replace names and grants to match your environment:

```sql
CREATE OR REPLACE NETWORK RULE HUGGINGFACE_NETWORK_RULE
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = (
    'huggingface.co:443',
    'us.aws.cdn.hf.co:443',
    'cdn-lfs.huggingface.co:443',
    'cdn-lfs-us-1.huggingface.co:443'
  );

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION PYPI_HF_EAI
  ALLOWED_NETWORK_RULES = (
    SNOWFLAKE.EXTERNAL_ACCESS.PYPI_RULE,
    HUGGINGFACE_NETWORK_RULE
  )
  ENABLED = TRUE;

GRANT USAGE ON INTEGRATION PYPI_HF_EAI TO ROLE <role_name>;
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
  --compute-pool DISTRIBUTED_GPU_POOL \
  --stage-name DISTRIBUTED_TRAINING_STAGE \
  --target-instances 2 \
  --external-access-integration PYPI_HF_EAI
```

The script performs a wiring preflight, submits `src` as the payload, waits for completion, and prints the distributed result.

By default, `launch.sh` assigns one DeepSpeed process to every visible GPU. Use one GPU per instance for the smallest validation run:

```bash
python scripts/submit_job.py \
  --compute-pool DISTRIBUTED_GPU_POOL \
  --stage-name DISTRIBUTED_TRAINING_STAGE \
  --external-access-integration PYPI_HF_EAI \
  --nproc-per-node 1
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

Rank 0 gathers the ZeRO-3 parameter shards and writes a metrics file and a full Hugging Face checkpoint to a relative `output/deepspeed/`. The payload runs from the `app/` directory of this job's folder on the stage you passed as `--stage-name`, so the files land there:

```text
app/output/deepspeed/
├── metrics.json               # final loss, perplexity, token count, tokens/s, world size, and ZeRO stage
└── model/                     # standard HF checkpoint: model.save_pretrained() + tokenizer.save_pretrained()
    ├── config.json            # model architecture and hyperparameters
    ├── model.safetensors      # consolidated ZeRO-3 weights (~4 GB for Qwen3-1.7B)
    ├── generation_config.json # default text-generation settings
    ├── tokenizer.json         # fast tokenizer: vocabulary and merges
    ├── tokenizer_config.json  # tokenizer options and special-token map
    └── chat_template.jinja    # Qwen3 chat prompt template
```

ML Jobs assigns the per-job folder name, so locate the files by pattern (or browse the stage in Snowsight):

```bash
snow sql --query "LS @<stage_name> PATTERN='.*deepspeed.*';"
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

An existing DeepSpeed repository that supports no-SSH launch normally needs only the topology adapter:

1. Copy the hostfile construction in `launch.sh` next to the repository entrypoint.
2. Replace `train.py` and its arguments with the repository's normal DeepSpeed command.
3. Keep `--no_ssh`, `--node_rank`, `--master_addr`, and `--master_port` so every per-instance launcher joins the same world.
4. Set the hostfile slot count to the number of processes the repository should start per node.
5. Put the repository's dependencies in `src/requirements.txt` or use a runtime image that already contains them.
6. Submit the directory with `parallel=True` and `min_instances` equal to `target_instances`.

The launcher pattern is independent of ZeRO stage. A repository can use another DeepSpeed training strategy while keeping the same hostfile and no-SSH topology mapping.

## Configuration and Reproducibility

The sample pins the remote assets used by the tested configuration:

| Asset | Revision | License |
| --- | --- | --- |
| `Qwen/Qwen3-1.7B` | `70d244cc86ccca08cf5af4e1e306ecf908b1ad5e` | Apache 2.0 |
| `databricks/databricks-dolly-15k` | `bdd27f4d94b9c1f951818a7da7fd7aeea5dbff1a` | CC BY-SA 3.0 |

See the [Qwen3-1.7B model card](https://huggingface.co/Qwen/Qwen3-1.7B) and [Dolly-15k dataset card](https://huggingface.co/datasets/databricks/databricks-dolly-15k) for their full terms and attribution information.

`--nproc-per-node` controls how many DeepSpeed processes each instance starts, and `zero3.json` is intentionally separate so a repository can replace it with its existing DeepSpeed configuration.

## Troubleshooting

### A Launcher Waits During Distributed Initialization

Keep the default wiring preflight enabled. Confirm that all requested compute pool nodes are available, every hostfile has the same ordered entries, and each launcher receives a unique `SNOWFLAKE_JOB_INDEX`.

### Hugging Face Download Fails

Confirm that the EAI is granted to the submitting role and that its network rule allows the Hugging Face hosts above. The sample disables the Xet download path, so no Xet-specific hosts are required.

### CUDA Out of Memory

Reduce `--nproc-per-node`, lower the sequence length or batch size in `src/train.py`, or choose a GPU instance family with more memory. Gradient checkpointing and ZeRO-3 are enabled by default, but model construction and consolidated checkpoint saving still require CPU memory. For larger models, use DeepSpeed's partition-aware model initialization and sharded checkpoint APIs instead of this compact reference implementation.

### Checkpoint Saving Is Slow

The sample deliberately gathers a standard checkpoint on rank 0 so the output is easy to inspect and reuse. Production jobs can save native partitioned DeepSpeed checkpoints to avoid the rank-0 gathering cost.

## Cost and Cleanup

The default run uses two GPU nodes and downloads the model independently on each node. Compute and network costs depend on the selected instance family and account configuration. Suspend or drop dedicated compute resources when you finish, and remove payloads or checkpoints from the stage if they are no longer needed.
