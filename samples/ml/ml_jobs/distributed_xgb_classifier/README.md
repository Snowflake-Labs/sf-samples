# Distributed XGBoost Training with Multi-Node ML Jobs

This example demonstrates how to train an XGBoost model using distributed computing across multiple nodes with Snowflake ML Jobs.

> NOTE: Prefer notebooks? This tutorial is also available as a [Jupyter Notebook](../distributed_xgb_classifier_nb/multi_node_xgb.ipynb)!

## Overview

The sample trains an XGBoost model on a large dataset using Ray's distributed capabilities. It demonstrates:

- Configuring a multi-node ML job with `target_instances=3`
- Creating and using a synthetic dataset with SQL
- Leveraging Ray for distributed data processing and model training
- Testing different scaling configurations with the [XGBoost distributor](https://docs.snowflake.com/en/developer-guide/snowflake-ml/container-runtime-ml#xgboost)

## Prerequisites

- `snowflake-ml-python>=1.9.0`
- A Snowflake compute pool with sufficient resources

## How to Run

1. Create a compute pool if you don't already have one:

```sql
CREATE COMPUTE POOL IF NOT EXISTS E2E_CPU_POOL
    MIN_NODES = 1
    MAX_NODES = 3
    INSTANCE_FAMILY = CPU_X64_S;
```

Note: `MAX_NODES` should be at least equal to `num_instances` (3 in this example).
If `MAX_NODES` is less than `num_instances`, only `MAX_NODES` nodes will start initially, 
and remaining nodes will only start when slots become available (when existing
nodes shut down). This can cause new worker nodes to fail connecting to the 
head node, potentially causing job failure.

2. Ensure the `ENABLE_BATCH_JOB_SERVICES` parameter is enabled on your Snowflake account.
Contact your Snowflake account admin to enable the feature if needed.

3. Run the example:

```python
from snowflake.ml.jobs import submit_file, submit_directory

# Option 1: Run just the main file
job = submit_file(
    "src/train.py",
    "E2E_CPU_POOL",
    stage_name="multi_node_payload_stage",
    target_instances=3  # Specify multiple instances
)

# Option 2: Run the entire directory
job = submit_directory(
    "src",
    "E2E_CPU_POOL",
    entrypoint="src/train.py",
    stage_name="multi_node_payload_stage",
    target_instances=3  # Specify multiple instances
)

# Monitor the job
print(job.id)
print(job.status)

# Get head node logs (without specifying an instance_id)
# This returns logs from whichever instance was selected as the head node
logs_head = job.get_logs()

# To get logs from a specific instance by ID
logs_instance0 = job.get_logs(instance_id=0)
logs_instance1 = job.get_logs(instance_id=1)


# Wait for completion
job.wait()
```

## Viewing Logs

For multi-node jobs, you can access logs from individual instances:

```python
# Get logs from the head node (default)
logs_head = job.get_logs()

# Get logs from specific worker nodes by ID
logs_worker1 = job.get_logs(instance_id=1)
logs_worker2 = job.get_logs(instance_id=2)

# Display logs in the notebook/console
job.show_logs()  # Head node
job.show_logs(instance_id=1)  # Node 1
```

## Using GPU for Training

The sample code uses a CPU compute pool for jobs. For workloads that would benefit from GPU acceleration, follow these steps:

### 1. Create a GPU Compute Pool (if not already available)

Create a compute pool if you don't already have one:

```sql
CREATE COMPUTE POOL IF NOT EXISTS E2E_GPU_POOL
    MIN_NODES = 1
    MAX_NODES = 3
    INSTANCE_FAMILY = GPU_NV_S;
```

### 2. Configure Your Training Job to Use GPU

When submitting your training job, modify the code scaling config in `train.py` and change the argument `use_gpu` to `true`:

```python
# Configure Ray scaling for XGBoost
scaling_config = XGBScalingConfig(
    num_workers=num_workers, 
    num_cpu_per_worker=num_cpu_per_worker,
    use_gpu=true
)
```

Also change the compute pool from the old cpu one to use the gpu one in `train.py`:

```python
# compute_pool = "E2E_CPU_POOL"
compute_pool = "E2E_GPU_POOL"
```