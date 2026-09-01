# PyTorch DDP with a Static torchrun Rendezvous

## Overview

This sample continues training a pinned Qwen3-0.6B causal language model on a pinned WikiText-103 dataset subset with PyTorch DistributedDataParallel (DDP). It demonstrates how an existing `torchrun` workload can run directly on every ML Job instance.

The default configuration selects and tokenizes up to 4,096 non-empty WikiText examples, then runs only 20 optimizer steps. It is intended to validate the execution path and produce a real checkpoint, not to benchmark model quality or distributed scaling.

## Files


| Path                    | Purpose                                             |
| ----------------------- | --------------------------------------------------- |
| `requirements.txt`      | Local dependency for submitting the job             |
| `scripts/submit_job.py` | Submits, waits for, and reports the distributed job |
| `src/launch.sh`         | Maps the ML Job topology to `torchrun` flags        |
| `src/train.py`          | Framework-standard DDP training code                |
| `src/requirements.txt`  | Dependencies installed in the job runtime           |




## How It Works

The launcher translates the ML Job topology into a static `torchrun` rendezvous:


| ML Job value           | `torchrun` argument |
| ---------------------- | ------------------- |
| `SNOWFLAKE_JOBS_COUNT` | `--nnodes`          |
| `SNOWFLAKE_JOB_INDEX`  | `--node-rank`       |
| `MLRS_HEAD_IP`         | `--master-addr`     |
| `MLRS_RDZV_PORT`       | `--master-port`     |


`torchrun` then supplies `RANK`, `WORLD_SIZE`, `LOCAL_RANK`, `MASTER_ADDR`, and `MASTER_PORT` to `train.py`.

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

Run `python scripts/submit_job.py --help` for the full argument list. In short, the script:

- submits `src` as the payload with a `wiring` preflight, waits for the job, and prints the distributed result;
- on each instance, runs `torchrun` through `launch.sh`, starting one training process per visible GPU.

Common options:

- `--preflight reference` — also times one synthetic DDP step on GPU before the workload; `--preflight none` skips the preflight.
- `--no-wait` — returns the job id immediately instead of waiting for completion.

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

Rank 0 writes a metrics file and a full Hugging Face checkpoint to a relative `output/pytorch_ddp/`. The payload runs from the `app/` directory of this job's folder on the stage you passed as `--stage-name`, so the files land there:

```text
app/output/pytorch_ddp/
├── metrics.json               # final loss, perplexity, token count, tokens/s, backend, world size, and pinned model/dataset revisions
└── model/                     # standard HF checkpoint: model.save_pretrained() + tokenizer.save_pretrained()
    ├── config.json            # model architecture and hyperparameters
    ├── model.safetensors      # trained weights (~1.2 GB for Qwen3-0.6B)
    ├── generation_config.json # default text-generation settings
    ├── tokenizer.json         # fast tokenizer: vocabulary and merges
    ├── tokenizer_config.json  # tokenizer options and special-token map
    └── chat_template.jinja    # Qwen3 chat prompt template
```

ML Jobs assigns the per-job folder name, so locate the files by pattern (or browse the stage in Snowsight):

```bash
snow sql --query "LS @<stage_name> PATTERN='.*pytorch_ddp.*';"
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

An existing `torchrun` repository normally needs only the launcher adapter:

1. Copy `launch.sh` next to the repository entrypoint.
2. Replace `train.py` and its arguments with the repository's normal command.
3. Keep the static `nnodes`, `node-rank`, `master-address`, and `master-port` mapping.
4. Put the repository's dependencies in `src/requirements.txt` or use a runtime image that already contains them.
5. Submit the directory with `parallel=True` and `min_instances` equal to `target_instances`.

The training strategy can change without changing the topology mapping. For example, `train.py` can wrap the model with FSDP instead of DDP while `launch.sh` continues to use the same `torchrun` command.

The launcher can change too. A repository that runs `accelerate launch` instead of `torchrun` maps the same topology onto accelerate's `--num_machines`, `--num_processes`, `--machine_rank`, `--main_process_ip`, and `--main_process_port`. Pass the head IP and port (`MLRS_HEAD_IP`/`MLRS_RDZV_PORT`) directly, as `launch.sh` does, so accelerate uses a static rendezvous.

## Configuration and Reproducibility

The sample pins the remote assets used by the tested configuration:


| Asset                 | Revision                                   | License               |
| --------------------- | ------------------------------------------ | --------------------- |
| `Qwen/Qwen3-0.6B`     | `c1899de289a04d12100db370d81485cdf75e47ca` | Apache 2.0            |
| `Salesforce/wikitext` | `b08601e04326c79dfdd32d625aee71d232d685c3` | CC BY-SA 3.0 and GFDL |


See the [Qwen3-0.6B model card](https://huggingface.co/Qwen/Qwen3-0.6B) and [WikiText dataset card](https://huggingface.co/datasets/Salesforce/wikitext) for their full terms and attribution information.

## Troubleshooting



### A Rank Waits Indefinitely During Initialization

Keep the default wiring preflight enabled and confirm that all requested compute pool nodes are available.

### Hugging Face Download Fails

Confirm that the EAI is granted to the submitting role and that its network rule allows the Hugging Face hosts above. The sample disables the Xet download path, so no Xet-specific hosts are required.

### CUDA Out of Memory

The default 0.6B configuration fits comfortably on a single GPU, so OOM points to a scaled-up workload or an undersized GPU. Reduce `--sequence-length` or per-process `--batch-size` in `src/train.py`, or choose a GPU instance family with more memory. Gradient checkpointing is on by default; a model too large for one GPU would need FSDP or DeepSpeed.

## Cost and Cleanup

The default run uses two GPU nodes and downloads the model independently on each node. Compute and network costs depend on the selected instance family and account configuration. Suspend or drop dedicated compute resources when you finish, and remove payloads or checkpoints from the stage if they are no longer needed.
