# Distributed XGBoost Training with Multi-Node ML Jobs

This example demonstrates how to train an XGBoost model using distributed computing across multiple nodes with Snowflake ML Jobs.

## Overview

The sample trains an XGBoost model on a large dataset using Ray's distributed capabilities. It demonstrates:

- Configuring a multi-node ML job with `num_instances=3`
- Creating and using a synthetic dataset with SQL
- Leveraging Ray for distributed data processing and model training
- Testing different scaling configurations with the [XGBoost distributor](https://docs.snowflake.com/en/developer-guide/snowflake-ml/container-runtime-ml#xgboost)

## Prerequisites

- `snowflake-ml-python>=1.8.2`
- A Snowflake compute pool with sufficient resources

## How to Run

1. Create a compute pool if you don't already have one:

```sql
CREATE COMPUTE POOL IF NOT EXISTS E2E_CPU_POOL
    MIN_NODES = 1
    MAX_NODES = 3  -- For multi-node, ensure MAX_NODES is at least no less than num_instances
    INSTANCE_FAMILY = CPU_X64_S;
```

2. Run the example:

```python
from snowflake.ml.jobs import submit_file, submit_directory

# Option 1: Run just the main file
job = submit_file(
    "path/to/main.py",
    "E2E_CPU_POOL",
    stage_name="multi_node_payload_stage",
    num_instances=3  # Specify multiple instances
)

# Option 2: Run the entire directory
job = submit_directory(
    "path/to/xgb-distributed",
    "E2E_CPU_POOL",
    entrypoint="src/main.py",
    stage_name="multi_node_payload_stage",
    num_instances=3  # Specify multiple instances
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

# Note: The head node can be any of the instances (0, 1, or 2)
# and is determined by the Ray cluster at runtime.
# To find the head node index, use _get_head_instance_id() method.
# 
# It's an internal API for current use only. In future releases
# we will ensure the head node is always node 0 and deprecate this internal API.
from snowflake.ml.jobs.job import _get_head_instance_id
head_node_id = _get_head_instance_id(job._session, job.id)
print(f"Head Node index: {head_node_id}")

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

When submitting your training job, modify the code scaling config in `main.py` and change the arguemnt `use_gpu` to `true`:

```python
# Configure Ray scaling for XGBoost
scaling_config = XGBScalingConfig(
    num_workers=num_workers, 
    num_cpu_per_worker=num_cpu_per_worker,
    use_gpu=true
)
```

Also change the compute pool from the old cpu one to use the gpu one in `main.py`:

```python
# compute_pool = "E2E_CPU_POOL"
compute_pool = "E2E_GPU_POOL"
```