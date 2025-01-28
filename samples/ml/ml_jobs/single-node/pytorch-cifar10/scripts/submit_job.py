import os
import yaml
import snowflake.ml.jobs as jobs
import logging
from snowflake.snowpark import Session

def load_spec(filepath: str) -> dict:
    with open(filepath, "r") as f:
        return yaml.safe_load(f)


def run_job(session: Session, compute_pool: str, external_access_integrations: list, payload_stage: str = "payload_stage", wandb_secret_name: str = None, args_list: list = None, block: bool = False) -> None:
    spec = load_spec(os.path.join(os.path.dirname(__file__), "job_spec.yaml"))
    payload_source = os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "src"))
    entrypoint = "train.py"
    args = [
        "--data-dir",
        "/data",
        "--model-dir",
        "/opt/app",
        "--tensorboard-dir",
        "/logs",
    ]
    if args_list:
        args = args + args_list

    if wandb_secret_name:
        spec["spec"]["containers"][0]["secrets"] = [
            {
                "snowflakeSecret": wandb_secret_name,
                "secretKeyRef": "secret_string",
                "envVarName": "WANDB_API_KEY",
            },
        ]
        entrypoint = "train_wandb.py"

    logging.info("Payload will be uploaded to stage: %s", payload_stage)
    job = jobs.submit_directory(
        payload_source,
        entrypoint=entrypoint,
        args=args,
        compute_pool=compute_pool,
        stage_name=payload_stage,
        spec_overrides=spec,
        external_access_integrations=external_access_integrations,
        session=session,
    )
    print("Started job with ID:", job.id)
    if block:
        job.wait()
        job.show_logs()


if __name__ == '__main__':
    import argparse
    from snowflake.ml.utils.connection_params import SnowflakeLoginOptions

    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--compute-pool", type=str, required=True)
    parser.add_argument("-e", "--external-access-integrations", type=str, nargs="*", required=True, help="PyPI EAI and any additional EAIs (e.g. W&B)")
    parser.add_argument("--wandb-secret-name", type=str, required=False, help="Name of the secret containing the W&B API key")
    parser.add_argument("-c", "--snowflake-config", type=str, required=False)
    parser.add_argument("-s", "--payload_stage", type=str, required=False, default="payload_stage")
    parser.add_argument("--block", action="store_true", help="Block until job completes")
    args, unparsed_args = parser.parse_known_args()

    session = Session.builder.configs(SnowflakeLoginOptions(args.snowflake_config)).create()

    run_job(
        session=session,
        compute_pool=args.compute_pool,
        external_access_integrations=args.external_access_integrations,
        wandb_secret_name=args.wandb_secret_name,
        args_list=unparsed_args,
    )