# Distributed Partition Function (DPF) - Example Walkthrough

## Introduction

The **Distributed Partition Function (DPF)** lets you process data in parallel across one or more nodes in a compute pool. DPF partitions your data by a specified column (or by staged files) and executes your Python function on each partition concurrently. It handles distributed orchestration, errors, observability, and artifact persistence automatically.

This example uses a **supply chain allocation** scenario: given factories with limited capacity and warehouses with specific demand, find the optimal shipping plan per region using `scipy.optimize.linprog`. Each region is solved as an independent DPF partition.

## Execution Modes

DPF supports two execution modes, both demonstrated in this notebook:

| Mode | Method | Description |
|------|--------|-------------|
| **DataFrame mode** | `run()` | Partition a Snowpark DataFrame by column values and execute your function on each partition concurrently. |
| **Stage mode** | `run_from_stage()` | Process files from a Snowflake stage where each file becomes a partition. Ideal for large-scale file processing. |

## What This Notebook Covers

1. **Setup** - Session, stage, scale compute, and synthetic data generation
2. **DataFrame mode** - Define a processing function, run DPF, monitor progress, retrieve results, inspect logs, restore completed runs
3. **Stage mode** - Copy data to parquet files on stage, run DPF from stage
4. **ML Jobs deployment** - Deploy DPF workloads via the `@remote` decorator

## Prerequisites

- A [compute pool](https://docs.snowflake.com/en/sql-reference/sql/create-compute-pool) with at least 3 max nodes (e.g., `CPU_X64_S`), or use the system-provided `SYSTEM_COMPUTE_POOL_CPU`
- A Snowflake Notebook running on the compute pool (Container Runtime)
- Stage access permissions for storing results and artifacts

## Getting Started

This notebook is intended to be run in a **Snowflake Notebook**. If running locally, use the **ML Jobs deployment** section at the bottom of the notebook to submit DPF workloads via the `@remote` decorator.

Open the [DPF Example Notebook](./dpf_example.ipynb) for a full end-to-end walkthrough.

## References

- [DPF Documentation](https://docs.snowflake.com/en/developer-guide/snowflake-ml/process-data-across-partitions)
- [DPF API Reference](https://docs.snowflake.com/en/developer-guide/snowpark-ml/reference/latest/container-runtime/distributors.distributed_partition_function)
- [ML Jobs Documentation](https://docs.snowflake.com/developer-guide/snowflake-ml/ml-jobs/overview)
- [Many Model Training (MMT) Example](../many_model_training/mmt_example.ipynb)
