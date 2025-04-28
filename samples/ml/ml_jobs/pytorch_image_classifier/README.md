# PyTorch Classifier Example

## Setup

Install Python requirements using `pip install -r requirements.txt` from the sample directory.

## Training Script

The model training code is located in the [src directory](./src/) and contains multiple files:
- A pip `requirements.txt` file
- `model.py` contains the model definition
- `train.py` contains basic training logic
- `train_wandb.py` is a variant of `train.py` with added [Weights and Biases integration](#weights-and-biases-integration)

### Upload data to stage

Manually download the CIFAR-10 datasets and upload them to a stage.

Python:
```python
import torchvision
import torchvision.transforms as transforms

transform = transforms.Compose([
    transforms.ToTensor(),
    transforms.Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))
])

trainset = torchvision.datasets.CIFAR10(root="./data/cifar10", train=True, download=True, transform=transform)
testset = torchvision.datasets.CIFAR10(root="./data/cifar10", train=False, download=True, transform=transform)
```

Shell:
```bash
snow stage create cifar10_data
snow stage copy ./data/cifar10 @cifar10_data --recursive
```

## Launch Job

Scripts needed to launch model training as a job are included in the
[scripts/](./scripts) directory. It includes:
- `submit_job.py` facilitates deploying the training scripts as a job
- `check_job.py` can be used to inspect running and recently completed jobs.
- `job_spec.yaml` includes additional service specification values to demonstrate
  using the `spec_override` parameter of the ML Job API.

```bash
python scripts/submit_job.py -h  # View available options
python scripts/submit_job.py -p DEMO_POOL_CPU -e pypi_eai  # Basic run
```

The API uploads the payload from `./src/` into a Snowflake stage and generates a
[service specification](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/specification-reference)
configured to mount the stage and run the entrypoint `train.py`.
In this example, we additionally provide a `job_spec.yml` used to mount the `@cifar10_data`
stage created in the [Upload Data to Stage](#upload-data-to-stage) step.
Finally, the job is executed as an SPCS [JOB SERVICE](https://docs.snowflake.com/en/sql-reference/sql/execute-job-service) on `DEMO_POOL_CPU`.

> NOTE: There is currently no automatic cleanup of the uploaded payloads. Be sure to
  manually clean up the payload stage(s) to prevent unintended storage costs.

The above command(s) will print a message like `Started job with ID: MLJOB_(uuid)`.
You can check on the progress of the submitted job using [scripts/check_job.py](./scripts/check_job.py)

```bash
python scripts/check_job.py MLJOB_2CECA414_F52E_4F45_840A_9623A52DA5C4
python scripts/check_job.py MLJOB_2CECA414_F52E_4F45_840A_9623A52DA5C4 --show-logs  # Print any job execution logs
python scripts/check_job.py MLJOB_2CECA414_F52E_4F45_840A_9623A52DA5C4 --block  # Block until job completion
```

## Weights and Biases Integration

Integrating with Weights and Biases (W&B) requires adding just a
[few lines of code](https://docs.wandb.ai/quickstart/#putting-it-all-together) to the training script.
[train_wandb.py](./src/train_wandb.py) is an updated version of the training script with W&B integration included.

Configure an External Access Integration to allow your SPCS service to connect to the W&B servers.

SQL:

```sql
CREATE OR REPLACE NETWORK RULE WANDB_RULE
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('api.wandb.ai:443');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION WANDB_EAI
  ALLOWED_NETWORK_RULES = (WANDB_RULE)
  ENABLED = true;
GRANT USAGE ON INTEGRATION WANDB_EAI TO ROLE <role_name>;
```

Configure W&B authentication by securely injecting your W&B API key as a [Snowflake Secrets](https://docs.snowflake.com/en/user-guide/api-authentication#managing-secrets).

SQL:

```sql
CREATE OR REPLACE SECRET WANDB_SECRET
    TYPE = GENERIC_STRING
    SECRET_STRING = '<api_key>';
```

Include the newly configured External Access Integration and Secret when running
[submit_job.py](./scripts/submit_job.py):

```bash
python scripts/submit_job.py \
  -p DEMO_POOL_CPU \
  -e pypi_eai wandb_eai \
  --wandb-secret-name wandb_secret
```
