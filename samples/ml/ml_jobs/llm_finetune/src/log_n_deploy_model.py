import argparse
import logging
from pathlib import Path
from peft import PeftModel
from transformers import AutoModelForCausalLM, AutoTokenizer, pipeline
from typing import Optional
import pandas as pd

from snowflake.snowpark import Session

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

from snowflake.ml.fileset.sfcfs import SFFileSystem
from snowflake.ml.registry.registry import Registry
from snowflake.ml.model import openai_signatures
from snowflake.ml.model.inference_engine import InferenceEngine 

from prompt_utils import (
    create_user_prompt,
    SYSTEM_PROMPT,
)

def resolve_path_global_step(path: Optional[str]) -> Optional[str]:
    if not path or not (p := Path(path)).is_dir():
        return path
    
    dirs = [d for d in p.iterdir() if d.is_dir() and d.name.startswith('global_step_')]
    if dirs:
        return str(max(dirs, key=lambda d: int(d.name.split('_')[-1])))
    return path


def resolve_stage_path(session: Session, path: str) -> str:
    if not isinstance(path, str) or not path.startswith("@"):
        return path

    # Download @DB.SCHEMA.STAGE/path to /tmp/DB.SCHEMA.STAGE/path
    dest_path = f"/tmp/{path[1:].rstrip('/')}/"
    fs = SFFileSystem(snowpark_session=session)
    fs.get(path, dest_path, recursive=True)

    return dest_path

SYSTEM_PROMPT = """\
You are an expert medical professor specializing in clinical documentation.
Your task is to generate medically accurate SOAP note summaries from provided
doctor–patient transcripts.

You must strictly follow these rules:

1. Base the SOAP note ONLY on the information explicitly stated in the transcript.
   Do not infer or add medical facts not present in the dialogue.

2. Maintain patient confidentiality and use professional, sensitive medical
   language suitable for clinician-to-clinician communication.

3. Use concise medical terminology and standard abbreviations where appropriate.

4. Structure the output as a single valid JSON object with exactly four top-level
   keys: "S", "O", "A", and "P".

5. Each key must map to a string value containing the corresponding section text.
   Do not nest objects or arrays inside these fields.

6. Do NOT include markdown, bullet points, numbering, special characters, or
   formatting outside of plain text within each field.

7. Do NOT include any explanatory text, headers, or commentary outside the JSON
   object.

SOAP section guidelines:

- "S" (Subjective):
  Summarize the patient- or caregiver-reported symptoms, concerns, chief complaint,
  and relevant history. Use the patient’s statements as the primary source.

- "O" (Objective):
  Include measurable or observed findings such as exam findings, imaging results,
  genetic testing, and other diagnostics. Specify laterality, descriptors, and
  clinically relevant details. Include normal ranges only if stated.

- "A" (Assessment):
  Provide a concise clinical assessment synthesizing subjective and objective data.
  State the primary diagnosis and relevant differential diagnoses when applicable.
  Mention known or potential complications and overall clinical impression.

- "P" (Plan):
  Outline the management and follow-up plan, including therapies, referrals,
  monitoring, and patient or caregiver education. Address adherence or compliance
  considerations if mentioned.

The final output must be a single valid JSON object and nothing else.
"""

USER_PROMPT_PREFIX = "Create a Medical SOAP note summary from the following dialogue:"

def create_user_prompt(dialogue: str) -> str:
    return f"{USER_PROMPT_PREFIX}\n\n{dialogue}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("model_name_or_path", help="Generator model path/name")
    parser.add_argument("--lora_path", default=None, help="LoRA adapter path if any")
    parser.add_argument("-m", "--model-name", required=True, help="Name of the model to log")
    parser.add_argument("-c", "--compute-pool", required=True, help="Name of the GPU compute pool to use for hosting service")
    parser.add_argument("-s", "--service-name", help="Name of the service to use for hosting model")
    parser.add_argument("-d", "--data_table", default="soap_data_test", help="Data table name")

    args = parser.parse_args()

    # Get the Snowflake session
    session = Session.builder.getOrCreate()


    # --------- 1) Log model ----------
    version_name = "lora" if args.lora_path else "base"
    model_registry = Registry(session=session)
    try:
        mv = model_registry.get_model(args.model_name).version(version_name)
    except Exception as e:
        # Resolve checkpoint paths to latest global_step if applicable
        if args.lora_path:
            # 1. Load the base model
            logger.info(f"Loading base model from {args.model_name_or_path}")
            base_model = AutoModelForCausalLM.from_pretrained(
                args.model_name_or_path,
                torch_dtype="auto",
                device_map="auto"
            )
            # 2. Load the LoRA adapter on top of it
            lora_path = resolve_path_global_step(resolve_stage_path(session, args.lora_path))
            logger.info(f"Loading LoRA adapter from {lora_path}")
            model = PeftModel.from_pretrained(base_model, lora_path)
            # 3. Merge LoRA weights into base model and unload adapter
            fused_model = model.merge_and_unload()
            logger.info(f"Merged LoRA weights into base model")

            # Create pipeline with merged model
            tokenizer = AutoTokenizer.from_pretrained(lora_path)
            logger.info(f"Creating transformer pipeline with merged model")
            pipe = pipeline("text-generation", model=fused_model, tokenizer=tokenizer)
            logger.info(f"Created transformer pipeline with merged model")
        else:
            model_path = resolve_path_global_step(resolve_stage_path(session, args.model_name_or_path))
            logger.info(f"Model {args.model_name} not found, logging to model registry")
            logger.info(f"Hydrating model from {model_path}")
            pipe = pipeline("text-generation", model=model_path)
            logger.info(f"Created transformer pipeline with model")

        logger.info(f"Logging hydrated model {args.model_name} version {version_name} to model registry")
        mv = model_registry.log_model(
            pipe,
            model_name=args.model_name,
            version_name=version_name,
            signatures=openai_signatures.OPENAI_CHAT_SIGNATURE)
        logger.info(f"Model {args.model_name} version {mv.version_name} logged successfully")

    # --------- 2) Deploy model ----------
    svcs= mv.list_services()
    service_name = args.service_name
    if len(svcs) > 0:
        service_name = svcs["name"].iloc[0]
        logger.info(f"Model {args.model_name} version {mv.version_name} already deployed as service {service_name}")
    else:
        logger.info(f"Deploying model version {mv.version_name} to compute pool {args.compute_pool} as service {args.service_name}")
        mv.create_service(
            service_name=args.service_name,
            service_compute_pool=args.compute_pool,
            gpu_requests="4", # for vLLM, we need to specify this arg, value doesn't matter; fixed in next release
            ingress_enabled=True,
            inference_engine_options={  # Specify vLLM explicitly for now; fixed in next release
                "engine": InferenceEngine.VLLM,
                "engine_args_override": [
                    "--max-model-len=8192",
                    "--gpu-memory-utilization=0.9"
                ]
            }
        )
        logger.info(f"Model {args.model_name} version {mv.version_name} deployed to compute pool {args.compute_pool} as service {service_name}")
    
    # --------- 3) Call model ----------
    if not args.data_table:
        logger.info("No data table provided, skipping model call")
        return
    
    # Load dataset test split
    ds = session.sql(
        f"SELECT * FROM {args.data_table}"
    ).to_pandas()
    logger.info(f"Loaded {len(ds)} rows of data from {args.data_table}")

    # --------- 1) Generator ----------
    gen_convos = [
        [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": create_user_prompt(d)},
        ]
        for d in ds["DIALOGUE"]
    ]

    df = pd.DataFrame.from_records({"messages": gen_convo} for gen_convo in gen_convos)
    df["max_completion_tokens"] = 250
    df["temperature"] = 0.01
    df["stop"] = None
    df["n"] = 1
    df["stream"] = False
    df["top_p"] = 1.0
    df["frequency_penalty"] = 0.1
    df["presence_penalty"] = 0.1
    logger.info(f"Input dataframe - first 20 rows:\n{df.head(20).to_string()}")

    output_df = mv.run(
        df.head(20),
        function_name="__call__",
        service_name=service_name,
    )
    logger.info(f"Output dataframe:\n{output_df.to_string()}")

if __name__ == "__main__":
    main()
