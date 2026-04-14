# Full-Weight RL Training

This recipe runs full-weight GRPO training using [Snowflake ML Jobs](https://docs.snowflake.com/developer-guide/snowflake-ml/ml-jobs/overview) and the AReaL framework with vLLM rollout and local Qwen3-8B judge models.

## Overview

Full-weight training updates all parameters of a Qwen3-1.7B model using GRPO. The job runs on 8 A100 GPUs with the following layout:

| GPUs | Role | Details |
|------|------|---------|
| 0-1 | vLLM rollout | 2 replicas generating candidate responses |
| 2-5 | FSDP training | 4 data-parallel replicas updating all weights |
| 6-7 | vLLM judges | 2 Qwen3-8B instances scoring SOAP sections |

Weight updates use XCCL (in-memory transfer) between rollout and training workers. The reward function scores each response out of 5.0: 1.0 for valid JSON format + up to 1.0 per SOAP section (S, O, A, P) via LLM judge.

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
cd samples/ml/ml_jobs/rl_finetuning/fullweight
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
| `src/config.yaml` | AReaL training config (GPU allocation, hyperparameters, data paths) |
| `src/run_medical_soap.py` | Job entrypoint: installs packages, starts judges, launches training |
| `src/reward.py` | Reward functions for SOAP note validation |
| `src/prompt_utils.py` | System prompts for SOAP generation and judge evaluation |
| `src/patches.py` | Runtime patches for AReaL (async rewards, extended timeouts) |
| `src/requirements.txt` | Python dependencies installed at job startup |
| `scripts/prepare_data.py` | Uploads synthetic data to Snowflake tables |
| `scripts/submit_train.py` | Submits the training job to SPCS |

## Troubleshooting

### Out of Memory (OOM) Errors

Reduce `batch_size` or `max_new_tokens` in `src/config.yaml`. Full-weight training on Qwen3-1.7B requires 8x A100-40GB (`GPU_NV_L`).

### W&B Connection Issues

Ensure your external access integration allows outbound HTTPS. The W&B base URL is `https://snowflake.wandb.io`. Verify the secret is accessible to your role.

### Checkpoint Access

Model checkpoints are saved to `/mnt/job_stage/output/` on the job's stage mount. Access them via the stage after training completes.
