# LLM Fine-Tuning with Snowflake ML Jobs

This example demonstrates how to fine-tune large language models (LLMs) using [Snowflake ML Jobs](https://docs.snowflake.com/developer-guide/snowflake-ml/ml-jobs/overview) and [ArcticTraining](https://www.snowflake.com/en/engineering-blog/arctictraining-llm-post-training-framework/), Snowflake's open-source training framework. The sample trains a Qwen3-1.7B model to generate structured SOAP (Subjective, Objective, Assessment, Plan) notes from medical dialogues.

## Overview

### What is Fine-Tuning?

Fine-tuning adapts a pre-trained LLM to perform well on a specific task by continuing training on domain-specific data. While pre-trained models have broad general knowledge, fine-tuning enables them to:

- Follow specific output formats (like structured JSON)
- Understand domain-specific terminology (medical, legal, financial)
- Produce more accurate and consistent outputs for specialized tasks

### What is LoRA?

[LoRA (Low-Rank Adaptation)](https://arxiv.org/abs/2106.09685) is a parameter-efficient fine-tuning technique that dramatically reduces memory requirements and training time. Instead of updating all model weights, LoRA:

- Freezes the pre-trained model weights
- Injects trainable low-rank matrices into transformer layers
- Typically trains only 0.1-1% of the original parameters

This makes it possible to fine-tune large models on consumer GPUs while achieving performance close to full fine-tuning.

### What is ArcticTraining?

[ArcticTraining](https://www.snowflake.com/en/engineering-blog/arctictraining-llm-post-training-framework/) is Snowflake's open-source framework for LLM post-training. It provides:

- **Declarative YAML configs** for reproducible training runs
- **Built-in Snowflake integration** for reading data directly from tables
- **DeepSpeed support** for efficient memory management and distributed training
- **Checkpoint management** with automatic saving to Snowflake stages

The sample demonstrates a key ML Jobs capability: **custom entrypoints via list arguments**. Instead of pointing to a Python script, you can pass a command with arguments:

```python
jobs.submit_directory(
    payload_dir,
    entrypoint=["arctic_training", "Qwen3-1.7B-LoRA-config.yaml"],  # CLI command + args
    compute_pool=compute_pool,
    ...
)
```

This enables running any CLI tool (like `arctic_training`) as the job entrypoint rather than just Python scripts.

## Dataset

The sample uses the [omi-health/medical-dialogue-to-soap-summary](https://huggingface.co/datasets/omi-health/medical-dialogue-to-soap-summary) dataset from Hugging Face. It contains doctor-patient transcripts paired with clinical SOAP note summaries. The goal is to train models to accurately extract and structure clinical information from conversational medical dialogues into the standardized SOAP format.

## Prerequisites

### Snowflake Account Setup

Work with your account administrator to provision the required resources in your Snowflake account.

> NOTE: The steps below use role name `ENGINEER`. Replace this with the role name you will be using.

1. Create a GPU compute pool if you don't already have one:

```sql
CREATE COMPUTE POOL IF NOT EXISTS GPU_POOL
    MIN_NODES = 1
    MAX_NODES = 1
    INSTANCE_FAMILY = GPU_NV_M;
GRANT USAGE ON COMPUTE POOL GPU_POOL TO ROLE ENGINEER;
```

2. Create a virtual warehouse if you don't already have one:

```sql
CREATE WAREHOUSE IF NOT EXISTS DEMO_WH;
GRANT USAGE ON WAREHOUSE DEMO_WH TO ROLE ENGINEER;
```

3. Configure database privileges. Subsequent steps will create resources inside this database:

```sql
-- OPTIONAL: Create a separate database for easy cleanup
CREATE DATABASE IF NOT EXISTS LLM_DEMO;

GRANT USAGE ON DATABASE LLM_DEMO TO ROLE ENGINEER;
GRANT CREATE SCHEMA ON DATABASE LLM_DEMO TO ROLE ENGINEER;
GRANT CREATE STAGE ON SCHEMA LLM_DEMO.PUBLIC TO ROLE ENGINEER;
GRANT CREATE TABLE ON SCHEMA LLM_DEMO.PUBLIC TO ROLE ENGINEER;
```

4. Create an external access integration for PyPI and Hugging Face access if you don't already have one:

```sql
-- Requires ACCOUNTADMIN privileges
CREATE OR REPLACE NETWORK RULE LLM_DEMO.PUBLIC.HUGGINGFACE_RULE
  MODE = 'EGRESS'
  TYPE = 'HOST_PORT'
  VALUE_LIST = (
    'huggingface.co:443',
    'www.huggingface.co:443',
    'cdn-lfs.huggingface.co:443',
    'cdn-lfs-us-1.huggingface.co:443'
  );

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION PYPI_HF_EAI
  ALLOWED_NETWORK_RULES = (
    SNOWFLAKE.EXTERNAL_ACCESS.PYPI_RULE,  -- Predefined rule from Snowflake
    LLM_DEMO.PUBLIC.HUGGINGFACE_RULE,     -- Custom rule defined above
  )
  ENABLED = true;
GRANT USAGE ON INTEGRATION PYPI_HF_EAI TO ROLE ENGINEER;
```

### Local Setup

1. All steps assume your working directory is the `llm_finetune/` folder:

```bash
cd samples/ml/ml_jobs/llm_finetune
```

2. Configure your default Snowflake connection following the [connection configuration guide](https://docs.snowflake.com/developer-guide/snowflake-cli/connecting/configure-connections#define-connections)

3. Install the required dependencies:

```bash
pip install -r requirements.txt
```

## How to Run

### Step 1: Prepare the Dataset

Download and upload the dataset to Snowflake:

```bash
python scripts/prepare_data.py --database LLM_DEMO --schema PUBLIC
```

This script:
- Downloads the medical dialogue dataset from Hugging Face
- Parses SOAP sections from the raw text
- Uploads train and test splits to Snowflake tables (`SOAP_DATA_TRAIN`, `SOAP_DATA_TEST`)

### Step 2: Run Training

Submit a fine-tuning job using the training script:

**LoRA Fine-Tuning (Recommended):**
```bash
python scripts/run_train.py \
    --type lora \
    --compute-pool GPU_POOL \
    --external-access-integrations PYPI_HF_EAI \
    --database LLM_DEMO \
    --schema PUBLIC
```

**Full Fine-Tuning:**
```bash
python scripts/run_train.py \
    --type full \
    --compute-pool GPU_POOL \
    --external-access-integrations PYPI_HF_EAI \
    --database LLM_DEMO \
    --schema PUBLIC
```

> **Note:** LoRA training is ~50% faster and requires significantly less GPU memory than full fine-tuning while still achieving strong performance.

The script will output a job ID that you can use to monitor progress. You can also monitor the job via the [Job UI in Snowsight](../README.md#job-ui-in-snowsight).

### Step 3: Run Evaluation

After training completes, evaluate the fine-tuned model:

```bash
python scripts/run_eval.py \
    <TRAIN_JOB_ID> \
    --compute-pool GPU_POOL \
    --external-access-integrations PYPI_HF_EAI \
    --database LLM_DEMO \
    --schema PUBLIC
```

Replace `<TRAIN_JOB_ID>` with the job ID from the training step (e.g., `LLM_DEMO.PUBLIC.ARCTIC_TRAINING_XXXXXXX`).

The evaluation script:
- Retrieves the trained model checkpoint from the training job's stage
- Generates SOAP notes for each test example using the fine-tuned model
- Uses an LLM-as-judge approach (Qwen3-8B) to compare predictions against ground truth
- Reports pass/fail accuracy for each SOAP section

## Results

> Note: Exact numbers may vary between runs due to factors like data shuffling and numerical stability.

| Section | Qwen3-1.7B (Baseline) | Full Fine-Tune | LoRA Adapter |
|---------|-----------------------|-----------------|---------------|
| S       | 31.6%                 | 59.2%          | 58.0%        |
| O       | 41.2%                 | 56.0%          | 50.8%        |
| A       | 40.8%                 | 52.8%          | 48.4%        |
| P       | 46.4%                 | 60.4%          | 58.0%        |

Both fine-tuning approaches show improvements over the baseline, with LoRA achieving comparable results to full fine-tuning at a fraction of the training time and memory cost.

### Improving Performance

This sample uses conservative settings optimized for quick iteration on smaller GPU instances. For production use cases, consider the following improvements:

- **Use a larger base model**: Larger models like Qwen3-4B or Qwen3-8B typically achieve higher accuracy. Update `model.name_or_path` in the config file and provision a compute pool with more GPU memory (e.g., `GPU_NV_L`).

- **Increase `max_length`**: The `max_length` setting filters out training examples longer than the configured maximum length. Increasing this value retains more data but requires additional GPU memory.

- **Train for more epochs**: Increasing `epochs` in the config may improve convergence, though watch for overfitting on small datasets.

- **Adjust LoRA rank**: Higher `r` values (e.g., 16 or 32) in the LoRA config capture more fine-grained adaptations at the cost of increased memory and training time.

- **Use multi-node training**: For larger models or datasets, configure `target_instances > 1` to distribute training across multiple GPU nodes.

## Key Features

### ArcticTraining in Container Runtime

This sample demonstrates how to run [ArcticTraining](https://github.com/snowflakedb/ArcticTraining) jobs inside Snowflake's [Container Runtime for ML](https://docs.snowflake.com/en/developer-guide/snowflake-ml/container-runtime-ml). If you're already familiar with ArcticTraining or DeepSpeed workflows, this integration provides several advantages:

**Custom CLI Entrypoints**: ML Jobs support list-based entrypoints, enabling you to run CLI tools like `arctic_training` directly:

```python
from snowflake.ml import jobs

job = jobs.submit_directory(
    "src/",
    entrypoint=["arctic_training", "Qwen3-1.7B-LoRA-config.yaml"],
    compute_pool="GPU_POOL",
    stage_name="payload_stage",
    external_access_integrations=["PYPI_HF_EAI"],
)
```

This same pattern works for other training launchers like `torchrun`, `accelerate`, or `deepspeed`.

**Stage-Mounted Checkpoints**: The Container Runtime mounts the job's stage at `/mnt/job_stage/`, enabling direct checkpoint writes to Snowflake storage:

```yaml
checkpoint:
  - type: huggingface
    save_end_of_training: true
    output_dir: /mnt/job_stage/output/model/
```

Checkpoints saved here persist after job completion and can be accessed by downstream evaluation jobs or for model deployment.

**Snowflake Data Sources**: ArcticTraining's `SnowflakeDataSource` reads training data directly from Snowflake, eliminating the need to export data.

```yaml
data:
  sources:
    - type: snowflake       # Custom data source (see train.py)
      table_name: my_table  # Reads from Snowflake table
```

See [here](https://github.com/snowflakedb/ArcticTraining/tree/main/projects/causal_snowflake) for more information about Snowflake data integration in ArcticTraining.

The custom `SOAPDataSource` in [train.py](src/train.py) extends this to format examples with system/user/assistant messages for instruction tuning.

### LLM-as-Judge Evaluation

The evaluation script uses a larger model (Qwen3-8B) to judge the quality of generated SOAP notes:

1. The fine-tuned model generates SOAP notes for each test example
2. For each S, O, A, P section, the judge model compares the prediction against ground truth
3. The judge returns pass/fail verdicts based on factual accuracy and completeness
4. Final scores are aggregated across the test set

This approach provides more nuanced evaluation than simple text matching, accounting for valid paraphrasing and synonym usage.

## Troubleshooting

### Out of Memory (OOM) Errors

If you encounter OOM errors during training:

1. Reduce `data.max_length` in the config file (e.g., from `6Ki` to `4Ki`)
2. Use LoRA instead of full fine-tuning
3. Request a larger GPU instance family (e.g., `GPU_NV_L` instead of `GPU_NV_M`)

### Network Access Errors

Ensure you have configured external access integration(s) to allow the required network access rules.

### Viewing Job Logs

Monitor training progress by fetching job logs:

```python
from snowflake.ml.jobs import get_job

job = get_job("ARCTIC_TRAINING_XXXXXXXXX")
print(job.get_logs())
```

Or use the verbose flag when running the training script:

```bash
python scripts/run_train.py --type lora --compute-pool GPU_POOL \
    --external-access-integrations PYPI_HF_EAI --verbose
```
