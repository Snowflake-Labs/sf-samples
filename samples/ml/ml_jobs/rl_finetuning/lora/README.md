# LoRA RL Training with SGLang

This recipe runs LoRA GRPO training using [Snowflake ML Jobs](https://docs.snowflake.com/developer-guide/snowflake-ml/ml-jobs/overview) and the AReaL framework with SGLang rollout and local Qwen3-8B judge models.

## Overview

LoRA (Low-Rank Adaptation) trains only ~1% of parameters by injecting low-rank matrices (rank 16) into all linear layers. This recipe uses SGLang for rollout inference and disk-based weight synchronization.

**Why SGLang instead of vLLM?** vLLM has a bug in AReaL v1.0.1 where LoRA adapter names are not re-registered after weight updates, causing rollout failures. SGLang correctly handles LoRA hot-reload from disk.

**Why dp=1 for training?** FSDP2 combined with LoRA can produce deadlocks when using multiple data-parallel replicas. Using dp=1 avoids this issue.

The job runs on 8 A100 GPUs with the following layout:

| GPUs | Role | Details |
|------|------|---------|
| 0-3 | SGLang rollout | 4 replicas with LoRA hot-reload |
| 4 | Training | 1 replica (dp=1), LoRA rank 16 |
| 5-7 | vLLM judges | 3 Qwen3-8B instances scoring SOAP sections |

Weight updates use disk sync: LoRA adapters are saved to disk by the trainer, then SGLang reloads them. This works well because LoRA adapters are small (~50MB vs full weights).

> **Note:** Evaluation is disabled during training (`valid_dataset=None`) because the AReaL eval rollout hangs when used with LoRA adapters.

## Prerequisites

### Snowflake Account Setup

> NOTE: The steps below use role name `ENGINEER`. Replace this with the role name you will be using.

1. Create a compute pool with 8 A100 GPUs:

```sql
CREATE COMPUTE POOL IF NOT EXISTS RL_LOCAL_JUDGE_POOL
    MIN_NODES = 1
    MAX_NODES = 1
    INSTANCE_FAMILY = GPU_NV_L;
GRANT USAGE ON COMPUTE POOL RL_LOCAL_JUDGE_POOL TO ROLE ENGINEER;
```

2. Create database and schema:

```sql
CREATE DATABASE IF NOT EXISTS RL_TRAINING_DB;
CREATE SCHEMA IF NOT EXISTS RL_TRAINING_DB.RL_SCHEMA;

GRANT USAGE ON DATABASE RL_TRAINING_DB TO ROLE ENGINEER;
GRANT USAGE ON SCHEMA RL_TRAINING_DB.RL_SCHEMA TO ROLE ENGINEER;
GRANT CREATE TABLE ON SCHEMA RL_TRAINING_DB.RL_SCHEMA TO ROLE ENGINEER;
GRANT CREATE STAGE ON SCHEMA RL_TRAINING_DB.RL_SCHEMA TO ROLE ENGINEER;
```

3. Create an external access integration:

```sql
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION RL_TRAINING_EAI
  ALLOWED_NETWORK_RULES = (SNOWFLAKE.EXTERNAL_ACCESS.PYPI_RULE)
  ENABLED = true;
GRANT USAGE ON INTEGRATION RL_TRAINING_EAI TO ROLE ENGINEER;
```

4. Store your W&B API key as a Snowflake secret:

```sql
CREATE SECRET IF NOT EXISTS RL_TRAINING_DB.RL_SCHEMA.WANDB_API_KEY_SECRET
  TYPE = GENERIC_STRING
  SECRET_STRING = '<your-wandb-api-key>';
GRANT READ ON SECRET RL_TRAINING_DB.RL_SCHEMA.WANDB_API_KEY_SECRET TO ROLE ENGINEER;
```

### Local Setup

1. Set your working directory:

```bash
cd samples/ml/ml_jobs/rl_finetuning/lora
```

2. Configure your default Snowflake connection following the [connection configuration guide](https://docs.snowflake.com/developer-guide/snowflake-cli/connecting/configure-connections#define-connections).

3. Install dependencies:

```bash
pip install snowflake-ml-python snowflake-snowpark-python pandas
```

## How to Run

### Step 1: Prepare Data

Generate synthetic data using the [data synthesis pipeline](../data_synthesis/) first, then upload to Snowflake:

```bash
python scripts/prepare_data.py
```

This creates `MEDICAL_SOAP_TRAIN` and `MEDICAL_SOAP_TEST` tables in `RL_TRAINING_DB.RL_SCHEMA`.

### Step 2: Submit Training

```bash
python scripts/submit_train.py --compute-pool RL_LOCAL_JUDGE_POOL
```

The script uploads the `src/` directory to a Snowflake stage and submits the job. It will print a job ID for monitoring.

### Step 3: Monitor Training

**W&B Dashboard:** Training logs metrics to W&B project `spcs-medical-soap-rl`, including per-section reward scores (format, S, O, A, P).

**Service Logs:**

```python
from snowflake.ml.jobs import get_job

job = get_job("<JOB_ID>")
print(job.get_logs())
```

## File Descriptions

| File | Description |
|------|-------------|
| `src/config.yaml` | AReaL config with SGLang rollout, LoRA settings (rank 16, alpha 32) |
| `src/run_medical_soap.py` | Job entrypoint: installs SGLang + AReaL, starts judges, launches training |
| `src/reward.py` | Reward functions for SOAP note validation |
| `src/prompt_utils.py` | System prompts for SOAP generation and judge evaluation |
| `src/patches.py` | Runtime patches for AReaL LoRA + SGLang compatibility |
| `src/requirements.txt` | Python dependencies installed at job startup |
| `scripts/prepare_data.py` | Uploads synthetic data to Snowflake tables |
| `scripts/submit_train.py` | Submits the training job to SPCS |

### What `patches.py` Fixes

The `patches.py` file applies runtime fixes to AReaL v1.0.1 for LoRA compatibility:

- **Version-0 guard**: Skips LoRA path on initial rollout when no adapter exists yet
- **Disk save ordering**: Saves weights to disk before signaling SGLang to reload (fixes race condition)
- **LoRA-safe serialization**: Uses `safetensors` directly instead of `save_pretrained` to avoid FSDP2 storage pointer crashes
- **SGLang config extension**: Adds `lora_target_modules` field required for LoRA memory pool initialization
- **Async reward timeout**: Extends reward API timeout from 15s to 300s

## Troubleshooting

### LoRA Adapter Not Loading

Check that `weight_update_mode: disk` is set in `config.yaml`. SGLang needs to read adapters from disk; XCCL mode does not work with LoRA.

### SGLang Startup Failures

SGLang requires specific versions (`sglang==0.5.7`, `sgl-kernel==0.3.20`). These are installed at job startup. Check logs for installation errors.

### Out of Memory (OOM) Errors

LoRA uses less memory than full-weight training, but the 4 SGLang rollout replicas + 3 judges still require 8x A100-40GB. Reduce `max_concurrent` in `config.yaml` if needed.

### W&B Connection Issues

Ensure your external access integration allows outbound HTTPS. The W&B base URL is `https://snowflake.wandb.io`. Verify the secret is accessible to your role.
