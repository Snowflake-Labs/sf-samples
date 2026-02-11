import argparse
import json
import os
import re
import sys

from snowflake.snowpark import Session
from snowflake.ml import jobs


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Log model from arctic training job')
    parser.add_argument('train_job_id', type=str, help='Fully qualified ID of the training job to evaluate')
    parser.add_argument('-p', '--compute-pool', required=True, help='Name of the GPU compute pool to use')
    parser.add_argument('--database', help='Snowflake database (defaults to session default database)')
    parser.add_argument('--schema', help='Snowflake schema (defaults to session default schema)')
    parser.add_argument('--stage-name', default='payload_stage', help='Stage name for job artifacts')
    parser.add_argument('-m', '--model-name', required=True, help='Name of the model to log')
    parser.add_argument('-s', '--service-name', required=True, help='Name of the service to use for hosting model')
    parser.add_argument('-e', '--external-access-integrations', nargs="+", required=True, help='External access integrations are required for PyPI and HuggingFace access')
    args = parser.parse_args()

    session = Session.builder.getOrCreate()
    payload_dir = os.path.join(os.path.dirname(__file__), "..", "src")

    # Retrieve the train job and check its status
    train_job = jobs.get_job(args.train_job_id)
    if train_job.status == "FAILED":
        raise ValueError(f"Train job {args.train_job_id} failed")
    if train_job.status != "DONE":
        raise ValueError(f"Train job {args.train_job_id} is not done. Current status: {train_job.status}")

    # Get the model stage path from the train job
    train_job_stage = train_job._stage_path
    model_stage_path = f"{train_job_stage}/output/model/"

    # Prepare the evaluation arguments based on the corresponding training recipe
    recipe_name = next(arg for arg in train_job._container_spec["args"] if arg.endswith(".yaml"))
    if "lora" in recipe_name.lower():
        log_model_args = ["Qwen/Qwen3-1.7B", "--lora_path", model_stage_path]
    else:
        log_model_args = [model_stage_path]
    log_model_args.extend(["-m", args.model_name, "-c", args.compute_pool, "-s", args.service_name])
    
    job = jobs.submit_directory(
        payload_dir,
        entrypoint="log_n_deploy_model.py",
        args=log_model_args,
        compute_pool=args.compute_pool,
        stage_name=args.stage_name,
        database=args.database,
        schema=args.schema,
        external_access_integrations=args.external_access_integrations,
        session=session,
        pip_requirements=["transformers", "snowflake-ml-python>=1.26", "peft"],
    )

    print(f"Job submitted with ID: {job.id}")
    print(f"Job finished with status: {job.wait()}")
    print(f"Job logs:\n{job.get_logs()}")