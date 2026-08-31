#!/usr/bin/env python3
"""Submit the PyTorch DDP sample as a native per-instance ML Job."""

from __future__ import annotations

import argparse
import dataclasses
from pathlib import Path

from snowflake.ml import jobs
from snowflake.snowpark import Session


SAMPLE_DIR = Path(__file__).resolve().parents[1]
PAYLOAD_DIR = SAMPLE_DIR / "src"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--compute-pool",
        required=True,
        help="Compute pool to run the job on.",
    )
    parser.add_argument(
        "--stage-name",
        default="DISTRIBUTED_TRAINING_STAGE",
        help="Stage that holds the payload, artifacts, and result records (default: %(default)s).",
    )
    parser.add_argument(
        "--target-instances",
        type=int,
        default=2,
        help="Number of instances to start; also used as min_instances for a fixed world size (default: %(default)s).",
    )
    parser.add_argument(
        "--external-access-integration",
        action="append",
        default=[],
        help="EAI used for PyPI and Hugging Face access; repeat for multiple EAIs.",
    )
    parser.add_argument(
        "--connection-name",
        help="Named Snowpark connection to use; defaults to your Snowpark configuration.",
    )
    parser.add_argument(
        "--database",
        help="Database for the session; overrides the connection default.",
    )
    parser.add_argument(
        "--schema",
        help="Schema for the session; overrides the connection default.",
    )
    parser.add_argument(
        "--runtime-environment",
        help="Snowflake Container Runtime to run the job in; defaults to the account's runtime.",
    )
    parser.add_argument(
        "--preflight",
        choices=("none", "wiring", "reference"),
        default="wiring",
        help=(
            "Validation to run before the workload: 'wiring' checks c10d connectivity "
            "and a collective, 'reference' also times one synthetic DDP step on GPU, "
            "'none' skips it (default: %(default)s)."
        ),
    )
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="Submit and print the job id immediately without waiting for completion.",
    )
    return parser.parse_args()


def create_session(args: argparse.Namespace) -> Session:
    builder = Session.builder
    if args.connection_name:
        builder = builder.config("connection_name", args.connection_name)
    if args.database:
        builder = builder.config("database", args.database)
    if args.schema:
        builder = builder.config("schema", args.schema)
    return builder.create()


def submit(args: argparse.Namespace):
    if args.target_instances < 2:
        raise ValueError("--target-instances must be at least 2 for this sample.")

    session = create_session(args)
    env_vars = {
        "HF_HOME": "/tmp/huggingface",
        "HF_HUB_CACHE": "/tmp/huggingface/hub",
        "HF_DATASETS_CACHE": "/tmp/huggingface/datasets",
        "HF_HUB_DISABLE_XET": "1",
        "PYTHONUNBUFFERED": "1",
    }

    submit_kwargs = {
        "entrypoint": ["bash", "launch.sh"],
        "compute_pool": args.compute_pool,
        "stage_name": args.stage_name,
        "target_instances": args.target_instances,
        "min_instances": args.target_instances,
        "parallel": True,
        "env_vars": env_vars,
        "external_access_integrations": args.external_access_integration,
        "session": session,
    }
    if args.preflight != "none":
        submit_kwargs["preflight"] = args.preflight
    if args.runtime_environment:
        submit_kwargs["runtime_environment"] = args.runtime_environment

    job = jobs.submit_directory(str(PAYLOAD_DIR), **submit_kwargs)
    print(f"Submitted job: {job.id}")

    if args.no_wait:
        return job

    try:
        result = job.distributed_result()
    except jobs.DistributedJobError as error:
        if error.result is not None:
            print(f"Distributed result: {dataclasses.asdict(error.result)}")
            failed_instance = error.result.failed_instance
        else:
            failed_instance = None
        if failed_instance is not None:
            print(
                f"Instance {failed_instance} failed. View its logs with: "
                f'get_job("{job.id}").get_logs(instance_id={failed_instance}, verbose=True)'
            )
        raise

    print(f"Distributed result: {dataclasses.asdict(result)}")
    return job


if __name__ == "__main__":
    submit(parse_args())
