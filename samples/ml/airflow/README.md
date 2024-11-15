# Productionizing an XGBoost Training Script

## Setup

Generate synthetic data using [generate_data.sql](./scripts/generate_data.sql).
Adjust the `rowcount` value as desired to test performance with different data
sizes.

### Connecting to Snowflake in Python

The scripts included in this example use the `SnowflakeLoginOptions` utility API
from `snowflake-ml-python` to retrieve Snowflake connection settings from config
files must be authored before use. See [Configure Connections](https://docs.snowflake.com/developer-guide/snowflake-cli/connecting/configure-connections#define-connections) for information on how to define default
Snowflake connection(s) in a config.toml file

```python
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions

# Requires valid ~/.snowflake/config.toml file
session = Session.builder.configs(SnowflakeLoginOptions()).create()
```

## Training Script

The [src directory](./src/) contains the model training code which will comprise
the job payload. Note that the script only uses Snowpark APIs for data ingestion
and model registration; otherwise the script uses vanilla XGBoost and SKLearn for
model training and evaluation. This is the recommended approach for single-node
training in container runtimes.

A `requirements.txt` file is not necessary since the script only depends on
common ML libraries like xgboost, sklearn, and snowflake-ml-python which are
installed by default on Container Runtime images.

### Script Parameters

- `--source_table` (OPTIONAL) Training data location. Defaults to `loan_applications`
  which is created in the [setup step](#setup)
- `--save_mode` (OPTIONAL) Controls whether to save model to a local path or into Model Registry. Defaults to local
- `--output_dir` (OPTIONAL) Local save path. Only used if `save_mode=local`

## Launch Job

Set up compute pool
[compute_pool.sql](./scripts/compute_pool.sql) contains helpful SQL commands for
setting up an SPCS Compute Pool which can be used for this example. The main step
is simply:

```sql
CREATE OR REPLACE COMPUTE POOL ML_DEMO_POOL
    MIN_NODES = 1
    MAX_NODES = 10
    INSTANCE_FAMILY = HIGHMEM_X64_S;
```

This will create a basic compute pool using `HIGHMEM_X64_S` instances as the node type.
`HIGHMEM_X64_S` gives us nodes with 58 GiB of memory which can be helpful when operating
on large datasets such as `loan_applications_1b`. You may consider using a smaller
INSTANCE_FAMILY such as `CPU_X64_S` during small-scale experimentation to minimize costs.
Note that we recommend using nodes with at least 2 CPUs to avoid potential deadlocks.
See [CREATE COMPUTE POOL](https://docs.snowflake.com/en/sql-reference/sql/create-compute-pool)
documentation for more information on different instance families and their respective
resources.

### Manual Job Execution

In this example, we will upload the payload to a stage and mount that stage into
the job container. This approach removes the need to build and upload your own
container into the Snowflake Image Registry just to run your training script in
SPCS. You can upload your payload into a stage using the Snowsight UI or by running
the script below:

```python
import os
from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions

session = Session.builder.configs(SnowflakeLoginOptions()).create()

# Create stage
stage_name = "@ML_DEMO_STAGE"
session.sql(f"create stage if not exists {stage_name.lstrip('@')}").collect()

# Upload payload to stage
payload_path = "path/to/headless/scheduled-xgb/src/*"  # NOTE: Update to reflect your local path
session.file.put(
    payload_path,
    stage_name,
    overwrite=True,
    auto_compress=False,
)
```

Once the payload has been uploaded to a stage, we can then launch the SPCS JOB
SERVICE. This requires authoring a [service specification](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/specification-reference)
which we then pass to an [EXECUTE JOB SERVICE](https://docs.snowflake.com/en/sql-reference/sql/execute-job-service)
query. [submit_job.sql](./scripts/submit_job.sql) contains the SQL query for running
our job including the YAML service specification.

#### Helper Script

The included [submit_job.py](./scripts/job_utils/submit_job.py) script can automatically
generate the service specification and run the SQL query on your behalf. The
script can be invoked with default arguments which have been configured to work
with this example:

```bash
> python headless/scheduled-xgb/scripts/submit_job.py
Generated service name: JOB_36E39504_6210_495D_A7A2_07816924D31E
Submitted job id: 01b828e7-0002-9808-0000-da071b345272
```

You may also inspect the available arguments if you need to customize its behavior

```bash
> python headless/scheduled-xgb/scripts/submit_job.py -h
```

### Scheduled Job Execution

Jobs are commonly used as part of CI/CD pipelines. Pipelines and jobs  may be
launched:

- Explicitly by a manual or external action
- On a scheduled basis
- Based on event triggers

Explicit triggers can be used to bridge job/pipeline frameworks, for instance
allowing [Airflow](#apache-airflow) DAGs to execute SPCS jobs.
Meanwhile, [Snowflake Tasks](https://docs.snowflake.com/en/user-guide/tasks-intro)
enable configuring scheduled and triggered DAGs natively in Snowflake.

#### Airflow Integration

In this example we will explore using [Apache Airflow](https://airflow.apache.org/)
to build a CD pipeline that runs our training script on a weekly basis.
We will define a task which executes the following steps in an SPCS container:

1. Pull the latest code from a private GitHub repository
2. Run the training script on the latest production data
3. Save the trained model to a stage for downstream consumption

First we will need to generate a GitHub PAT for our job agent to authenticate
with GitHub. This step is only necessary if we are pulling the code from a
private repository. See [GitHub's documentation](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens)
for more information about PATs.

Once we have our PAT, we can register it as a [Snowflake Secret](https://docs.snowflake.com/en/sql-reference/sql/create-secret).
We will also need to configure an [External Access Integration (EAI)](https://docs.snowflake.com/en/developer-guide/external-network-access/creating-using-external-network-access)
to enable external network access in our SPCS jobs.
[github_integration.sql](./scripts/github_integration.sql) contains the SQL
commands necessary to create both the secret and the EAI.
Be sure to replace fields indicated with `<angle braces>` appropriate values.

We are finally ready to define our Airflow DAG.
Configure Airflow's [Snowflake connection](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html#json-format-example)
using the `AIRFLOW_CONN_SNOWFLAKE_DEFAULT` environment variable.
Point Airflow's `dag_folders` setting to find [train_dag.py](./airflow/train_dag.py)
which contains the DAG definition and job submission logic. The DAG also
references our prepared service specification at [job_spec.yaml](./airflow/job_spec.yaml).
Make sure to modify the GitHub repository URL in the YAML file to point to your
own GitHub repository.

```bash
# Pull GitHub repo using secret
# ACTION REQUIRED: Change the repository URL below to your repo!
git clone https://${GIT_TOKEN}@github.com/sfc-gh-dhung/test-repo.git $LOCAL_REPO_PATH
```

The current example DAG only contains a single task, but this can easily be
chained together with additional tasks such as upstream data preprocessing and
downstream model registration or inference/evaluation.

## Observability

### Job Monitoring

SPCS JOB SERVICE executions are tied to query execution and can be inspected in
Snowsight under query history. Active and recent jobs may also be inspected using
[SHOW JOB SERVICES](https://docs.snowflake.com/en/sql-reference/sql/show-services),
[DESCIBE SERVICE](https://docs.snowflake.com/en/sql-reference/sql/desc-service),
and [SHOW SERVICE CONTAINERS IN SERVICE](https://docs.snowflake.com/en/sql-reference/sql/show-service-containers-in-service).


Success/failure notifications can be enabled through
[Snowflake Tasks](https://docs.snowflake.com/en/user-guide/tasks-errors).
Externally triggered jobs will need to manually configure alerting based on
job execution result.

### Logging

[SPCS Documentation](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/monitoring-services#accessing-container-logs)
gives a good overview of logging options for SPCS jobs and services. In short:
- [SYSTEM$GET_SERVICE_LOGS](https://docs.snowflake.com/en/sql-reference/functions/system_get_service_logs)
  retrieves container logs of an existing SERVICE or JOB SERVICE.
  - ```sql
    -- Get last 100 logs from container named 'main' in job service 'JOB_36E39504'
    SELECT SYSTEM$GET_SERVICE_LOGS('JOB_36E39504', '0', 'main', 100)
    ```
  - Previous runs of restarted containers and dropped services cannot be
    inspected in this way. This includes JOB SERVICE entities which have
    been automatically cleaned up after completion.
- Container console logs in SPCS are automatically captured to the account's active
  [Event Table](https://docs.snowflake.com/en/developer-guide/logging-tracing/event-table-setting-up).
  Log level may optionally be customized using the [spec.logExporters](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/specification-reference#label-snowpark-containers-spec-reference-spec-logexporters).
  service specification field. If not set, all logs will be captured by default.
  - Recommendation is for ACCOUNTADMIN to create (filtered) VIEWS on top of
    Event Table(s) and configure RBAC at the VIEW level
    ```sql
    USE ROLE ACCOUNTADMIN;

    -- Create database and schema to hold our views
    CREATE DATABASE IF NOT EXISTS TELEMETRY_DB;
    CREATE SCHEMA IF NOT EXISTS TELEMETRY_DB.SPCS_LOGS;
    GRANT USAGE ON SCHEMA TELEMETRY_DB.SPCS_LOGS TO ROLE <role_name>;

    -- Create and grant VIEW to expose relevant logs to <role_name>
    CREATE VIEW TELEMETRY_DB.SPCS_LOGS.ML_DEMO_JOBS_V as (
      -- Default event table. Replace with your active event table if applicable.
      SELECT * FROM SNOWFLAKE.TELEMETRY.EVENTS_VIEW
      where 1=1
        and resource_attributes:"snow.database.name" = 'ML_DEMO_DB'
        and resource_attributes:"snow.schema.name" = 'ML_DEMO_SCHEMA'
        and resource_attributes:"snow.compute_pool.name" = 'ML_DEMO_POOL'
        and resource_attributes:"snow.service.type" = 'Job'
    );
    GRANT SELECT ON TELEMETRY_DB.SPCS_LOGS.ML_DEMO_JOBS_V TO ROLE <role_name>;
    ```

DataDog supports integration with Event Tables. See
[this blog post](https://www.datadoghq.com/blog/snowflake-snowpark-monitoring-datadog/)
by DataDog for more information.

### Compute Metrics

Service level metrics such as CPU/GPU utilization, network activity, and disk
activity can be logged using the `spec.platformMonitor` service specification
field.
See [Accessing Event Table Service Metrics](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/monitoring-services#accessing-event-table-service-metrics).

Compute pool level metrics can be monitored and exported to visualizers like
DataDog and Grafana by setting up a monitor service in SPCS. See
[Tutorial: Grafana Visualization Service for Compute Pool Metrics](https://github.com/Snowflake-Labs/spcs-templates/blob/main/user-metrics/tutorial%20-%20visualize_metrics_using_grafana/Introduction.md).

### Model Metrics

[Snowflake Model Registry](https://docs.snowflake.com/en/developer-guide/snowflake-ml/model-registry/overview)
natively supports saving evaluation metrics when logging models
as shown in the [example training script](#training-script).
Such metrics are displayed in the Model Registry UI and are included in the
`SHOW VERSIONS IN MODEL <model_name>` output.

We can also integrate with frameworks like
[MLflow](https://mlflow.org/) and [Weights and Biases (W&B)](https://wandb.ai/)
for live training progress monitoring and experiment tracking.
- We can run an MLflow tracking server on a separate SPCS service and securely
connect to it using [service-to-service](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/working-with-services#service-to-service-communications)
communication.
  - We recommend persisting tracking server state using a 
    a [block storage volume](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/block-storage-volume)
    for runs metadata (default `./mlruns`)
    and a [stage volume](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/snowflake-stage-volume)
    for artifacts (default `./mlartifacts`)
    See [Configure Server](https://mlflow.org/docs/latest/tracking/server.html#configure-server)
    for how to configure your MLflow tracking server.
  <!-- TODO: Add example of MLflow as an SPCS service -->
- We can also connect to externally hosted MLflow tracking servers or W&B with
  External Access Integrations for external network access.
  See the [Single Node PyTorch Example](../single-node/README.md#weights-and-biases-integration)
  for a full example with W&B integration.
