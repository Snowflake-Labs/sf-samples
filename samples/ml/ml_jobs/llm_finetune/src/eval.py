import argparse
import json
import os
import time
import tempfile
from itertools import cycle
from pathlib import Path
from typing import Optional
from snowflake.snowpark import Session
from snowflake.ml.fileset.sfcfs import SFFileSystem
from snowflake.ml.data import DataConnector

os.environ["VLLM_LOGGING_LEVEL"] = "ERROR" # Must be set before importing vLLM

import torch
from vllm import LLM, SamplingParams
from vllm.lora.request import LoRARequest

from prompt_utils import (
    create_judge_prompt,
    create_user_prompt,
    extract_SOAP_response,
    JUDGE_SYSTEM_PROMPT,
    SYSTEM_PROMPT,
)


def resolve_path_global_step(path: Optional[str]) -> Optional[str]:
    if not path or not (p := Path(path)).is_dir():
        return path
    
    dirs = [d for d in p.iterdir() if d.is_dir() and d.name.startswith('global_step_')]
    if dirs:
        return str(max(dirs, key=lambda d: int(d.name.split('_')[-1])))
    return path


def load_and_run_vllm(model: str, convos: list, lora: Optional[str] = None) -> list:
    llm = LLM(model=model, enforce_eager=True, enable_lora=True, tensor_parallel_size=torch.cuda.device_count())
    chat_kwargs = dict(
        sampling_params=SamplingParams(temperature=0.0, max_tokens=8192),
        chat_template_kwargs=dict(enable_thinking=False),
        lora_request=None if lora is None else LoRARequest("lora_adapter", 1, lora),
    )

    outputs = llm.chat(convos, **chat_kwargs)
    time.sleep(5)  # ensure all async ops complete
    llm.llm_engine.engine_core.shutdown()

    return outputs


def resolve_stage_path(session: Session, path: str) -> str:
    if not isinstance(path, str) or not path.startswith("@"):
        return path

    # Download @DB.SCHEMA.STAGE/path to /tmp/DB.SCHEMA.STAGE/path
    dest_path = f"/tmp/{path[1:].rstrip('/')}/"
    fs = SFFileSystem(snowpark_session=session)
    fs.get(path, dest_path, recursive=True)

    return dest_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("model_name_or_path", help="Generator model path/name")
    parser.add_argument("--lora_path", default=None, help="LoRA adapter path if any")
    parser.add_argument("--judge_model_name_or_path", default="Qwen/Qwen3-8B", help="Judge model path/name.")
    parser.add_argument("-d", "--data_table", default="soap_data_test", help="Data table name")
    args = parser.parse_args()

    # Get the Snowflake session
    session = Session.builder.getOrCreate()

    # Resolve checkpoint paths to latest global_step if applicable
    for name, value in vars(args).items():
        resolved_value = resolve_path_global_step(resolve_stage_path(session, value))
        setattr(args, name, resolved_value)

    # Load dataset test split
    ds = DataConnector.from_sql(
        f"SELECT * FROM {args.data_table}",
        session=session,
    ).to_huggingface_dataset()

    # --------- 1) Generator ----------
    gen_convos = [
        [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": create_user_prompt(d)},
        ]
        for d in ds["dialogue"]
    ]

    gen_outputs = load_and_run_vllm(
        args.model_name_or_path,
        gen_convos,
        args.lora_path if args.lora_path else None,
    )
    preds = []
    for out in gen_outputs:
        try:
            preds.append(json.loads(out.outputs[0].text))
        except:
            preds.append({k: "Could not parse JSON" for k in "SOAP"})

    # --------- 2) LLM-as-judge ----------
    judge_convos = []
    for pred, sample in zip(preds, ds):
        ground_truth = {k: sample[k] for k in "SOAP"}
        for k in "SOAP":
            judge_convos.append(
                [
                    {"role": "system", "content": JUDGE_SYSTEM_PROMPT},
                    {"role": "user", "content": create_judge_prompt(sample["dialogue"], k, ground_truth[k], pred[k])},
                ]
            )

    judge_outputs = load_and_run_vllm(
        args.judge_model_name_or_path,
        judge_convos,
    )
    scores = []
    for out in judge_outputs:
        try:
            scores.append(json.loads(out.outputs[0].text))
        except:
            scores.append({"verdict": "fail", "reason": "Could not parse JSON"})

    # --------- 3) Aggregate results ----------
    pass_counts = {k: 0 for k in "SOAP"}
    for k, s in zip(cycle("SOAP"), scores):
        if str(s.get("verdict", "")).lower().strip() == "pass":
            pass_counts[k] += 1

    print("\nEvaluation Results:")
    for k in "SOAP":
        print(f"  {k}: {pass_counts[k]}/{len(ds)} correct ({100.0 * pass_counts[k] / len(ds):.2f}%)")


if __name__ == "__main__":
    main()
