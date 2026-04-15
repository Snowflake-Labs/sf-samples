# Data Synthesis Pipeline

This pipeline generates synthetic doctor-patient dialogues with SOAP (Subjective, Objective, Assessment, Plan) notes for RL training. It supports two backends: Snowflake Cortex and local vLLM.

## Overview

The pipeline generates training data in three stages:

1. **Scenario generation**: Randomized patient demographics, medical histories, and conditions across 30 specialties (~400 conditions total)
2. **Dialogue generation**: Synthetic doctor-patient conversations based on each scenario
3. **SOAP note generation**: Structured JSON notes (S, O, A, P sections) summarizing each dialogue

Output is saved as JSON files with one record per sample containing the dialogue and four SOAP section fields.

## Prerequisites

### For Snowflake Cortex Backend

- A Snowflake account with access to Cortex LLM functions
- A configured Snowflake connection (see [connection guide](https://docs.snowflake.com/developer-guide/snowflake-cli/connecting/configure-connections#define-connections))

### For Local vLLM Backend

- A GPU with sufficient VRAM (the default model `Qwen/Qwen3-235B-A22B-Instruct-2507` requires multi-GPU with `--tensor-parallel-size 4`)
- Python dependencies:

```bash
pip install vllm transformers
```

## How to Run

### Snowflake Cortex Backend

Uses `snowflake.cortex.complete()` with Claude Sonnet by default. Runs multi-threaded (10 workers):

```bash
python synthesize_data.py \
    --backend cortex \
    --num-samples 1000 \
    --output-dir ./output
```

### Local vLLM Backend

Uses a local vLLM model for inference. Runs in batches (vLLM handles GPU parallelism):

```bash
python synthesize_data.py \
    --backend local \
    --model Qwen/Qwen3-235B-A22B-Instruct-2507 \
    --tensor-parallel-size 4 \
    --num-samples 1000 \
    --output-dir ./output
```

The final output is written to `synthetic_train_data.json` in the output directory.

## File Descriptions

| File | Description |
|------|-------------|
| `synthesize_data.py` | Main orchestrator: generates scenarios, dialogues, and SOAP notes |
| `medical_scenarios.py` | Defines 30 specialties, ~400 conditions, patient demographics |
| `prompt_utils.py` | System prompts for SOAP generation and LLM-as-judge evaluation |
| `local_inference.py` | vLLM wrapper for local GPU inference (batch + single-prompt) |

## Checkpointing and Resume

The pipeline saves per-batch JSON checkpoints to the output directory. If a run is interrupted, re-running with the same `--output-dir` skips completed batches automatically. Use `--merge-only` to combine existing checkpoints into the final output without generating new data.
