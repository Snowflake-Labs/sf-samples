import argparse
import json
import os
import re
import sys

from snowflake.snowpark import Session
from snowflake.ml import jobs


def submit_job(
    session: Session,
    payload_dir: str,
    recipe: str,
    compute_pool: str,
    external_access_integrations: list,
    database: str = None,
    schema: str = None,
    stage_name: str = 'payload_stage',
) -> jobs.MLJob:
    if not os.path.isfile(os.path.join(payload_dir, recipe)):
        raise FileNotFoundError(f"Recipe file not found: {os.path.join(payload_dir, recipe)}")

    return jobs.submit_directory(
        payload_dir,
        entrypoint=["arctic_training", recipe],
        compute_pool=compute_pool,
        stage_name=stage_name,
        external_access_integrations=external_access_integrations,
        database=database,
        schema=schema,
        session=session,
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create finetuning job for LoRA finetuning')
    parser.add_argument('-t', '--type', choices=['lora', 'full'], default='lora', help='Type of finetuning to perform')
    parser.add_argument('-p', '--compute-pool', required=True, help='Name of the GPU compute pool to use')
    parser.add_argument('--database', help='Snowflake database (defaults to session default database)')
    parser.add_argument('--schema', help='Snowflake schema (defaults to session default schema)')
    parser.add_argument('--stage-name', default='payload_stage', help='Stage name for job artifacts')
    parser.add_argument('-e', '--external-access-integrations', nargs="+", required=True, help='External access integrations are required for PyPI and HuggingFace access')
    args = parser.parse_args()

    session = Session.builder.getOrCreate()
    payload_dir = os.path.join(os.path.dirname(__file__), "..", "src")
    if args.type == 'lora':
        recipe = 'Qwen3-1.7B-LoRA-config.yaml'
    else:
        recipe = 'Qwen3-1.7B-config.yaml'

    job = submit_job(
        session=session,
        payload_dir=payload_dir,
        recipe=recipe,
        compute_pool=args.compute_pool,
        external_access_integrations=args.external_access_integrations,
        database=args.database,
        schema=args.schema,
        stage_name=args.stage_name
    )

    print(f"Job submitted with ID: {job.id}")
    print(f"Job finished with status: {job.wait()}")
    print(f"Job logs:\n{job.get_logs()}")