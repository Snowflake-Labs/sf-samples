#!/usr/bin/env python3
"""Evaluate multiple models on the medical SOAP note generation task.

Compares RL-trained checkpoints, base HuggingFace models, and frontier models
via Snowflake Cortex COMPLETE on a holdout test set.

Supported model formats:
    cortex:<model_name>     — Snowflake Cortex COMPLETE API (no local GPU needed)
    hf:<model_id>           — HuggingFace model (requires local GPU / submit_eval.py)
    checkpoint:<stage_path> — RL checkpoint from Snowflake stage (requires GPU)

Usage:
    # Quick eval with 100 samples on Cortex models
    SNOWFLAKE_DEFAULT_CONNECTION_NAME=preprod8 python scripts/eval_models.py \
        --models "cortex:claude-sonnet-4-6,cortex:llama4-maverick" \
        --num-samples 100

    # Full eval on all 4028 test samples
    SNOWFLAKE_DEFAULT_CONNECTION_NAME=preprod8 python scripts/eval_models.py \
        --models "cortex:claude-sonnet-4-6,cortex:llama4-maverick" \
        --num-samples 4028

    # Specify judge model
    SNOWFLAKE_DEFAULT_CONNECTION_NAME=preprod8 python scripts/eval_models.py \
        --models "cortex:llama4-maverick" \
        --judge-model "llama3.3-70b" \
        --num-samples 100
"""

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field

from snowflake.snowpark import Session

# ---------------------------------------------------------------------------
# Add project src/ to path so we can import prompt_utils and reward
# ---------------------------------------------------------------------------
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_SRC_DIR = os.path.join(_SCRIPT_DIR, "..", "src")
sys.path.insert(0, _SRC_DIR)

from prompt_utils import (
    JUDGE_SECTION_SYSTEM_PROMPT,
    SYSTEM_PROMPT,
    USER_PROMPT_PREFIX,
    create_section_judge_prompt,
)
from reward import SOAP_KEYS, extract_json_from_response, validate_soap_json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("eval_models")


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
@dataclass
class SampleResult:
    """Result for a single test sample."""

    valid_json: bool = False
    has_soap_keys: bool = False
    sections_non_empty: dict = field(default_factory=dict)
    section_pass: dict = field(default_factory=dict)
    raw_response: str = ""
    error: str = ""


@dataclass
class ModelResults:
    """Aggregated results for one model across all samples."""

    model_name: str
    samples: list = field(default_factory=list)
    total: int = 0

    @property
    def valid_json_count(self) -> int:
        return sum(1 for s in self.samples if s.valid_json)

    def section_pass_count(self, key: str) -> int:
        return sum(1 for s in self.samples if s.section_pass.get(key, False))


# ---------------------------------------------------------------------------
# Snowflake session helpers
# ---------------------------------------------------------------------------
def create_session(args) -> Session:
    """Create a Snowflake session from CLI args and environment."""
    connection_name = os.getenv("SNOWFLAKE_DEFAULT_CONNECTION_NAME")
    builder = Session.builder
    if connection_name:
        builder = builder.config("connection_name", connection_name)
    if args.database:
        builder = builder.config("database", args.database)
    if args.schema:
        builder = builder.config("schema", args.schema)
    builder = builder.config("role", "SYSADMIN")
    session = builder.create()

    if args.database:
        session.sql(f"USE DATABASE {args.database}").collect()
    if args.schema:
        session.sql(f"USE SCHEMA {args.schema}").collect()

    return session


def load_test_data(session: Session, table: str, num_samples: int) -> list[dict]:
    """Load test samples from the MEDICAL_SOAP_TEST table."""
    logger.info(f"Loading {num_samples} samples from {table}...")
    rows = session.sql(
        f"SELECT * FROM {table} LIMIT {num_samples}"
    ).collect()

    samples = []
    for row in rows:
        samples.append({
            "dialogue": row["DIALOGUE"],
            "S": row["PRED_S"],
            "O": row["PRED_O"],
            "A": row["PRED_A"],
            "P": row["PRED_P"],
        })

    logger.info(f"Loaded {len(samples)} test samples.")
    return samples


# ---------------------------------------------------------------------------
# Cortex COMPLETE inference
# ---------------------------------------------------------------------------
def _escape_sql_string(s: str) -> str:
    """Escape single quotes for SQL string literals."""
    return s.replace("\\", "\\\\").replace("'", "\\'")


def call_cortex_complete(
    session: Session, model_name: str, system_prompt: str, user_prompt: str,
    temperature: float | None = None,
) -> str | None:
    """Call Snowflake Cortex COMPLETE and return the response text.

    Returns None on error.
    """
    escaped_system = _escape_sql_string(system_prompt)
    escaped_user = _escape_sql_string(user_prompt)

    options = {}
    if temperature is not None:
        options["temperature"] = temperature

    # Build options string for SQL
    if options:
        opts_parts = []
        for k, v in options.items():
            if isinstance(v, (int, float)):
                opts_parts.append(f"'{k}': {v}")
            else:
                opts_parts.append(f"'{k}': '{v}'")
        options_str = ", ".join(opts_parts)
    else:
        options_str = ""

    sql = (
        f"SELECT SNOWFLAKE.CORTEX.COMPLETE(\n"
        f"    '{model_name}',\n"
        f"    [\n"
        f"        {{'role': 'system', 'content': '{escaped_system}'}},\n"
        f"        {{'role': 'user', 'content': '{escaped_user}'}}\n"
        f"    ],\n"
        f"    {{{options_str}}})"
    )

    try:
        result = session.sql(sql).collect()
        if result and len(result) > 0:
            raw = result[0][0]
            # Cortex COMPLETE returns a JSON string with "choices"
            try:
                resp_json = json.loads(raw)
                if "choices" in resp_json:
                    return resp_json["choices"][0].get("messages", resp_json["choices"][0].get("message", ""))
                # Some models return the text directly
                return raw
            except (json.JSONDecodeError, KeyError, IndexError):
                return raw
        return None
    except Exception as e:
        logger.warning(f"Cortex COMPLETE error for {model_name}: {e}")
        return None


# ---------------------------------------------------------------------------
# Judge evaluation via Cortex
# ---------------------------------------------------------------------------
def judge_section(
    session: Session,
    judge_model: str,
    dialogue: str,
    key: str,
    ground_truth: str,
    prediction: str,
) -> bool:
    """Use LLM judge via Cortex COMPLETE to evaluate a single SOAP section.

    Returns True if the section passes, False otherwise.
    """
    judge_user_prompt = create_section_judge_prompt(dialogue, key, ground_truth, prediction)

    response = call_cortex_complete(
        session, judge_model, JUDGE_SECTION_SYSTEM_PROMPT, judge_user_prompt,
        temperature=0,
    )

    if response is None:
        return False

    parsed = extract_json_from_response(response)
    if parsed and isinstance(parsed, dict):
        verdict = parsed.get("verdict", "").lower().strip()
        return verdict == "pass"

    return False


# ---------------------------------------------------------------------------
# Model evaluation
# ---------------------------------------------------------------------------
def evaluate_cortex_model(
    session: Session,
    model_name: str,
    test_data: list[dict],
    judge_model: str,
) -> ModelResults:
    """Evaluate a Cortex model on the test dataset."""
    results = ModelResults(model_name=f"cortex:{model_name}", total=len(test_data))
    logger.info(f"Evaluating cortex:{model_name} on {len(test_data)} samples...")

    for i, sample in enumerate(test_data):
        if (i + 1) % 100 == 0 or i == 0:
            logger.info(
                f"  [{model_name}] Processing sample {i + 1}/{len(test_data)}..."
            )

        user_prompt = f"{USER_PROMPT_PREFIX}\n\n{sample['dialogue']}"
        sr = SampleResult()

        # Generate SOAP note
        response = call_cortex_complete(session, model_name, SYSTEM_PROMPT, user_prompt)
        if response is None:
            sr.error = "No response from Cortex COMPLETE"
            results.samples.append(sr)
            continue

        sr.raw_response = response

        # Parse and validate JSON
        parsed = extract_json_from_response(response)
        if parsed is None:
            sr.error = "Failed to parse JSON"
            results.samples.append(sr)
            continue

        sr.valid_json = True

        if not validate_soap_json(parsed):
            sr.error = f"Invalid SOAP keys: {list(parsed.keys()) if isinstance(parsed, dict) else 'not a dict'}"
            results.samples.append(sr)
            continue

        sr.has_soap_keys = True

        # Check non-empty sections
        for key in ["S", "O", "A", "P"]:
            sr.sections_non_empty[key] = bool(
                parsed.get(key) and len(parsed[key].strip()) > 0
            )

        # Judge each section
        for key in ["S", "O", "A", "P"]:
            pred_text = parsed.get(key, "")
            gt_text = sample[key]

            try:
                passed = judge_section(
                    session, judge_model, sample["dialogue"], key, gt_text, pred_text,
                )
                sr.section_pass[key] = passed
            except Exception as e:
                logger.warning(
                    f"  Judge error on sample {i + 1}, section {key}: {e}"
                )
                sr.section_pass[key] = False

        results.samples.append(sr)

    return results


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------
def print_comparison_table(all_results: list[ModelResults], num_samples: int):
    """Print a formatted comparison table of all model results."""
    if not all_results:
        print("No results to display.")
        return

    # Determine column widths
    model_names = [r.model_name for r in all_results]
    col_width = max(max(len(n) for n in model_names), 20)
    metric_width = 20

    def fmt_count(count: int, total: int) -> str:
        pct = (count / total * 100) if total > 0 else 0
        return f"{count} / {total} ({pct:.2f}%)"

    # Header
    header = f"| {'Metric':<{metric_width}} |"
    separator = f"|{'-' * (metric_width + 2)}|"
    for name in model_names:
        header += f" {name:^{col_width}} |"
        separator += f"{'-' * (col_width + 2)}|"

    print()
    print("=" * len(separator))
    print("EVALUATION RESULTS")
    print("=" * len(separator))
    print(header)
    print(separator)

    # Section pass rates
    for key, label in [
        ("S", "S (Subjective)"),
        ("O", "O (Objective)"),
        ("A", "A (Assessment)"),
        ("P", "P (Plan)"),
    ]:
        row = f"| {label:<{metric_width}} |"
        for r in all_results:
            count = r.section_pass_count(key)
            row += f" {fmt_count(count, r.total):^{col_width}} |"
        print(row)

    # Valid JSON
    row = f"| {'Valid JSON':<{metric_width}} |"
    for r in all_results:
        row += f" {fmt_count(r.valid_json_count, r.total):^{col_width}} |"
    print(row)

    print(separator)
    print()


def write_results_json(all_results: list[ModelResults], output_path: str):
    """Write detailed results to a JSON file."""
    output = {}
    for r in all_results:
        model_data = {
            "total_samples": r.total,
            "valid_json": r.valid_json_count,
            "section_pass": {
                key: r.section_pass_count(key) for key in ["S", "O", "A", "P"]
            },
            "section_pass_pct": {
                key: round(r.section_pass_count(key) / r.total * 100, 2)
                if r.total > 0
                else 0
                for key in ["S", "O", "A", "P"]
            },
        }
        output[r.model_name] = model_data

    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    logger.info(f"Results written to {output_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Evaluate models on medical SOAP note generation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--models",
        required=True,
        help=(
            "Comma-separated list of models to evaluate. Formats:\n"
            "  cortex:<name>       — Snowflake Cortex COMPLETE\n"
            "  hf:<model_id>      — HuggingFace model (local GPU)\n"
            "  checkpoint:<stage>  — RL checkpoint from Snowflake stage"
        ),
    )
    parser.add_argument(
        "--num-samples",
        type=int,
        default=100,
        help="Number of test samples to evaluate (default: 100, full: 4028)",
    )
    parser.add_argument(
        "--judge-model",
        default="llama3.3-70b",
        help="Cortex model for LLM-as-judge evaluation (default: llama3.3-70b)",
    )
    parser.add_argument("--database", default="RL_TRAINING_DB")
    parser.add_argument("--schema", default="RL_SCHEMA")
    parser.add_argument(
        "--test-table", default="MEDICAL_SOAP_TEST",
        help="Snowflake table with test data (default: MEDICAL_SOAP_TEST)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output JSON file for detailed results (optional)",
    )
    args = parser.parse_args()

    # Parse model list
    model_specs = [m.strip() for m in args.models.split(",") if m.strip()]
    if not model_specs:
        parser.error("No models specified. Use --models 'cortex:claude-sonnet-4-6,...'")

    cortex_models = []
    local_models = []
    for spec in model_specs:
        if spec.startswith("cortex:"):
            cortex_models.append(spec.split(":", 1)[1])
        elif spec.startswith("hf:") or spec.startswith("checkpoint:"):
            local_models.append(spec)
        else:
            parser.error(
                f"Unknown model format: {spec}. "
                "Use cortex:<name>, hf:<model_id>, or checkpoint:<stage_path>"
            )

    # Warn about local models
    if local_models:
        print()
        print("NOTE: The following models require local GPU inference:")
        for m in local_models:
            print(f"  - {m}")
        print("Use scripts/submit_eval.py to run these on SPCS.")
        print("Only Cortex models will be evaluated in this run.")
        print()

    if not cortex_models:
        print("No Cortex models to evaluate. Exiting.")
        sys.exit(0)

    # Create session and load data
    session = create_session(args)
    test_data = load_test_data(session, args.test_table, args.num_samples)

    if not test_data:
        logger.error("No test data loaded. Check table name and connection.")
        sys.exit(1)

    # Evaluate each Cortex model
    all_results: list[ModelResults] = []
    total_start = time.time()

    for model_name in cortex_models:
        start = time.time()
        results = evaluate_cortex_model(
            session, model_name, test_data, args.judge_model,
        )
        elapsed = time.time() - start
        logger.info(
            f"Finished {model_name}: {elapsed:.1f}s "
            f"({elapsed / len(test_data):.2f}s/sample)"
        )
        all_results.append(results)

    total_elapsed = time.time() - total_start
    logger.info(f"Total evaluation time: {total_elapsed:.1f}s")

    # Print comparison table
    print_comparison_table(all_results, len(test_data))

    # Write JSON output if requested
    if args.output:
        write_results_json(all_results, args.output)


if __name__ == "__main__":
    main()
