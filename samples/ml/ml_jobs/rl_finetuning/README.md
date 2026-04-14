# RL Training Recipes for Medical SOAP Notes

This directory contains recipes for reinforcement learning (RL) training using [Snowflake ML Jobs](https://docs.snowflake.com/developer-guide/snowflake-ml/ml-jobs/overview) and the [AReaL](https://github.com/inclusiveai/AReaL) framework on Snowpark Container Services (SPCS).

## Overview

### What is RL / GRPO?

Reinforcement Learning (RL) for LLMs improves model outputs by optimizing against a reward signal rather than imitating reference answers. Unlike supervised fine-tuning (SFT), which trains the model to replicate ground-truth outputs, RL lets the model explore different responses and learn which ones score highest.

[GRPO (Group Relative Policy Optimization)](https://arxiv.org/abs/2402.03300) is an RL algorithm that:

- Samples multiple responses per prompt, then ranks them by reward
- Updates the policy to increase the probability of higher-reward responses relative to lower-reward ones
- Avoids the need for a separate critic/value model, reducing memory overhead

### Medical SOAP Use Case

These recipes train a Qwen3-1.7B model to generate structured SOAP (Subjective, Objective, Assessment, Plan) notes from doctor-patient dialogues. The reward function uses **decomposed LLM-judge scoring**: separate Qwen3-8B judge models running on dedicated GPUs evaluate each SOAP section independently (S, O, A, P), providing fine-grained reward signals. A format reward checks for valid JSON structure, giving a maximum score of 5.0 per response.

### AReaL Framework

[AReaL](https://github.com/inclusiveai/AReaL) is an RL post-training framework that orchestrates multi-GPU workloads via Ray, coordinating rollout inference (vLLM or SGLang), FSDP training, and reward evaluation across a GPU cluster.

## Directory Structure

| Directory | Description |
|-----------|-------------|
| [`data_synthesis/`](data_synthesis/) | Generate synthetic doctor-patient dialogues with SOAP notes |
| [`fullweight/`](fullweight/) | Full-weight GRPO training with vLLM rollout |
| [`lora/`](lora/) | LoRA GRPO training with SGLang rollout |

Start with `data_synthesis/` to prepare training data, then choose either `fullweight/` or `lora/` for training. LoRA trains ~1% of parameters and is faster to iterate; full-weight updates all parameters for maximum capacity.
