#!/usr/bin/env python3
"""
Medical training data synthesis using Claude Sonnet 4.5 via Snowflake Cortex
or local models via vLLM.
Generates synthetic doctor-patient dialogues with SOAP notes using parallel processing.
"""

import argparse
import json
import logging
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from medical_scenarios import (
    generate_scenarios_batch,
    scenario_to_prompt,
    MedicalScenario,
)
from prompt_utils import SYSTEM_PROMPT, create_user_prompt

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("synthesis.log"),
    ],
)
logger = logging.getLogger(__name__)


# Constants
DEFAULT_NUM_SAMPLES = 92500
DEFAULT_BATCH_SIZE = 100
DEFAULT_NUM_WORKERS = 10
DEFAULT_OUTPUT_DIR = "./output"
DEFAULT_CHECKPOINT_DIR = "./checkpoints"
DEFAULT_CORTEX_MODEL = "claude-4-sonnet"
DEFAULT_LOCAL_MODEL = "Qwen/Qwen3-235B-A22B-Instruct-2507"
DEFAULT_TENSOR_PARALLEL = 4
DEFAULT_VLLM_BATCH_SIZE = 32
DEFAULT_MAX_MODEL_LEN = 8192  # Limit context to save KV cache memory

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY_BASE = 2  # seconds


# Global configuration
class InferenceConfig:
    """Global configuration for inference backend."""
    
    use_local_model: bool = False
    local_model_name: str = DEFAULT_LOCAL_MODEL
    tensor_parallel_size: int = DEFAULT_TENSOR_PARALLEL
    cortex_model_name: str = DEFAULT_CORTEX_MODEL
    vllm_batch_size: int = DEFAULT_VLLM_BATCH_SIZE
    gpu_memory_utilization: float = 0.90
    max_model_len: int = DEFAULT_MAX_MODEL_LEN


config = InferenceConfig()


class SnowflakeSessionPool:
    """Thread-local Snowflake session pool."""
    
    def __init__(self):
        self._sessions = {}
    
    def get_session(self):
        """Get or create a session for the current thread."""
        import threading
        thread_id = threading.current_thread().ident
        
        if thread_id not in self._sessions:
            from snowflake.snowpark import Session
            self._sessions[thread_id] = Session.builder.getOrCreate()
            logger.debug(f"Created new Snowflake session for thread {thread_id}")
        
        return self._sessions[thread_id]
    
    def close_all(self):
        """Close all sessions."""
        for session in self._sessions.values():
            try:
                session.close()
            except Exception as e:
                logger.warning(f"Error closing session: {e}")
        self._sessions.clear()


# Global session pool (for Cortex backend)
session_pool = SnowflakeSessionPool()


def call_llm_cortex(
    prompt: str,
    system_prompt: Optional[str] = None,
    temperature: float = 0.7,
    max_tokens: int = 4000,
) -> str:
    """Call the LLM via Snowflake Cortex with retry logic."""
    from snowflake.cortex import complete
    
    session = session_pool.get_session()
    
    for attempt in range(MAX_RETRIES):
        try:
            # Build the full prompt with system prompt if provided
            if system_prompt:
                full_prompt = f"{system_prompt}\n\n{prompt}"
            else:
                full_prompt = prompt
            
            response = complete(
                model=config.cortex_model_name,
                prompt=full_prompt,
                session=session,
                options={
                    "max_tokens": max_tokens,
                    "temperature": temperature,
                },
            )
            
            return response.strip() if response else ""
            
        except Exception as e:
            delay = RETRY_DELAY_BASE * (2 ** attempt)
            logger.warning(
                f"Cortex call failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}. "
                f"Retrying in {delay}s..."
            )
            if attempt < MAX_RETRIES - 1:
                time.sleep(delay)
            else:
                raise


def call_llm_local(
    prompt: str,
    system_prompt: Optional[str] = None,
    temperature: float = 0.7,
    max_tokens: int = 4000,
) -> str:
    """Call the LLM via local vLLM with retry logic."""
    from local_inference import complete_local
    
    for attempt in range(MAX_RETRIES):
        try:
            # Pass system_prompt separately for proper chat template formatting
            response = complete_local(
                prompt=prompt,
                system_prompt=system_prompt,
                max_tokens=max_tokens,
                temperature=temperature,
            )
            
            return response.strip() if response else ""
            
        except Exception as e:
            delay = RETRY_DELAY_BASE * (2 ** attempt)
            logger.warning(
                f"Local LLM call failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}. "
                f"Retrying in {delay}s..."
            )
            if attempt < MAX_RETRIES - 1:
                time.sleep(delay)
            else:
                raise


def call_llm_with_retry(
    prompt: str,
    system_prompt: Optional[str] = None,
    temperature: float = 0.7,
    max_tokens: int = 4000,
) -> str:
    """Call the LLM with retry logic - routes to appropriate backend."""
    
    if config.use_local_model:
        return call_llm_local(prompt, system_prompt, temperature, max_tokens)
    else:
        return call_llm_cortex(prompt, system_prompt, temperature, max_tokens)


def call_llm_batch_local(
    prompts: List[str],
    system_prompt: Optional[str] = None,
    temperature: float = 0.7,
    max_tokens: int = 4000,
) -> List[str]:
    """Call local LLM for a batch of prompts (more efficient)."""
    from local_inference import complete_batch
    
    for attempt in range(MAX_RETRIES):
        try:
            # Pass system_prompt separately for proper chat template formatting
            responses = complete_batch(
                prompts=prompts,
                system_prompt=system_prompt,
                max_tokens=max_tokens,
                temperature=temperature,
            )
            return [r.strip() if r else "" for r in responses]
            
        except Exception as e:
            delay = RETRY_DELAY_BASE * (2 ** attempt)
            logger.warning(
                f"Batch LLM call failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}. "
                f"Retrying in {delay}s..."
            )
            if attempt < MAX_RETRIES - 1:
                time.sleep(delay)
            else:
                raise


def generate_dialogue(scenario: MedicalScenario) -> str:
    """Generate a doctor-patient dialogue for the given scenario."""
    
    prompt = scenario_to_prompt(scenario)
    
    dialogue = call_llm_with_retry(
        prompt=prompt,
        system_prompt=(
            "You are an expert medical dialogue generator. Generate realistic, "
            "detailed doctor-patient conversations that would occur in actual "
            "clinical settings. Include appropriate medical terminology, test "
            "results, and treatment discussions."
        ),
        temperature=0.7,
        max_tokens=4000,
    )
    
    return dialogue


def generate_soap_notes(dialogue: str) -> Dict[str, str]:
    """Generate SOAP notes for the given dialogue."""
    
    user_prompt = create_user_prompt(dialogue)
    
    response = call_llm_with_retry(
        prompt=user_prompt,
        system_prompt=SYSTEM_PROMPT,
        temperature=0.1,  # Lower temperature for more consistent SOAP notes
        max_tokens=2000,
    )
    
    return parse_soap_response(response)


def parse_soap_response(response: str) -> Dict[str, str]:
    """Parse SOAP notes from LLM response."""
    original_response = response
    
    try:
        # Try to extract JSON from the response
        response = response.strip()
        
        # Handle potential markdown code blocks
        if response.startswith("```"):
            lines = response.split("\n")
            # Remove first and last lines if they're code fences
            if lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            response = "\n".join(lines)
        
        # Try to find JSON object in the response
        # Sometimes models output text before/after the JSON
        json_start = response.find("{")
        json_end = response.rfind("}") + 1
        if json_start != -1 and json_end > json_start:
            response = response[json_start:json_end]
        
        soap_data = json.loads(response)
        
        # Validate required keys
        required_keys = ["S", "O", "A", "P"]
        for key in required_keys:
            if key not in soap_data:
                raise ValueError(f"Missing required key: {key}")
        
        return {
            "pred_S": soap_data["S"],
            "pred_O": soap_data["O"],
            "pred_A": soap_data["A"],
            "pred_P": soap_data["P"],
        }
        
    except (json.JSONDecodeError, ValueError) as e:
        # Log more details for debugging
        logger.warning(f"Failed to parse SOAP response: {e}")
        logger.warning(f"Response length: {len(original_response)} chars")
        logger.warning(f"Response preview (first 500 chars): {original_response[:500]}")
        if len(original_response) > 500:
            logger.warning(f"Response preview (last 200 chars): ...{original_response[-200:]}")
        
        # Return empty SOAP notes on parse failure
        return {
            "pred_S": "",
            "pred_O": "",
            "pred_A": "",
            "pred_P": "",
        }


def generate_single_sample(
    scenario: MedicalScenario,
    sample_id: int,
) -> Optional[Dict]:
    """Generate a single training sample (dialogue + SOAP notes)."""
    
    try:
        # Step 1: Generate dialogue
        dialogue = generate_dialogue(scenario)
        
        if not dialogue or len(dialogue) < 500:
            logger.warning(f"Sample {sample_id}: Generated dialogue too short, skipping")
            return None
        
        # Step 2: Generate SOAP notes
        soap_notes = generate_soap_notes(dialogue)
        
        # Validate SOAP notes
        if not any(soap_notes.values()):
            logger.warning(f"Sample {sample_id}: Empty SOAP notes, skipping")
            return None
        
        return {
            "dialogue": dialogue,
            "pred_S": soap_notes["pred_S"],
            "pred_O": soap_notes["pred_O"],
            "pred_A": soap_notes["pred_A"],
            "pred_P": soap_notes["pred_P"],
            "_metadata": {
                "scenario": {
                    "specialty": scenario.specialty,
                    "condition": scenario.condition,
                    "complexity": scenario.complexity,
                    "presentation_type": scenario.presentation_type,
                },
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }
        }
        
    except Exception as e:
        logger.error(f"Sample {sample_id}: Generation failed: {e}")
        return None


def generate_batch_local(
    scenarios: List[MedicalScenario],
    start_sample_id: int,
) -> List[Optional[Dict]]:
    """
    Generate a batch of samples using local vLLM (batched inference).
    This is more efficient than generating one at a time.
    """
    
    # Step 1: Generate all dialogues in batch
    dialogue_prompts = [scenario_to_prompt(s) for s in scenarios]
    dialogue_system_prompt = (
        "You are an expert medical dialogue generator. Generate realistic, "
        "detailed doctor-patient conversations that would occur in actual "
        "clinical settings. Include appropriate medical terminology, test "
        "results, and treatment discussions."
    )
    
    logger.info(f"Generating {len(scenarios)} dialogues in batch...")
    dialogues = call_llm_batch_local(
        prompts=dialogue_prompts,
        system_prompt=dialogue_system_prompt,
        temperature=0.7,
        max_tokens=4000,
    )
    
    # Step 2: Generate SOAP notes for valid dialogues
    valid_dialogues = []
    valid_indices = []
    for i, dialogue in enumerate(dialogues):
        if dialogue and len(dialogue) >= 500:
            valid_dialogues.append(dialogue)
            valid_indices.append(i)
        else:
            logger.warning(f"Sample {start_sample_id + i}: Dialogue too short, skipping")
    
    if valid_dialogues:
        logger.info(f"Generating SOAP notes for {len(valid_dialogues)} dialogues in batch...")
        soap_prompts = [create_user_prompt(d) for d in valid_dialogues]
        soap_responses = call_llm_batch_local(
            prompts=soap_prompts,
            system_prompt=SYSTEM_PROMPT,
            temperature=0.1,
            max_tokens=2000,
        )
    else:
        soap_responses = []
    
    # Step 3: Assemble results
    results = [None] * len(scenarios)
    
    for i, (idx, dialogue, soap_response) in enumerate(zip(valid_indices, valid_dialogues, soap_responses)):
        soap_notes = parse_soap_response(soap_response)
        
        if not any(soap_notes.values()):
            logger.warning(f"Sample {start_sample_id + idx}: Empty SOAP notes, skipping")
            continue
        
        scenario = scenarios[idx]
        results[idx] = {
            "dialogue": dialogue,
            "pred_S": soap_notes["pred_S"],
            "pred_O": soap_notes["pred_O"],
            "pred_A": soap_notes["pred_A"],
            "pred_P": soap_notes["pred_P"],
            "_metadata": {
                "scenario": {
                    "specialty": scenario.specialty,
                    "condition": scenario.condition,
                    "complexity": scenario.complexity,
                    "presentation_type": scenario.presentation_type,
                },
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }
        }
    
    return results


def process_batch(
    batch_id: int,
    scenarios: List[MedicalScenario],
    start_sample_id: int,
    checkpoint_dir: Path,
) -> Tuple[int, List[Dict]]:
    """Process a batch of scenarios and save checkpoint."""
    
    results = []
    
    if config.use_local_model:
        # Use batched inference for local model
        batch_results = generate_batch_local(scenarios, start_sample_id)
        for sample in batch_results:
            if sample:
                if "_metadata" in sample:
                    del sample["_metadata"]
                results.append(sample)
    else:
        # Sequential processing for Cortex
        for i, scenario in enumerate(scenarios):
            sample_id = start_sample_id + i
            
            sample = generate_single_sample(scenario, sample_id)
            
            if sample:
                if "_metadata" in sample:
                    del sample["_metadata"]
                results.append(sample)
            
            # Log progress every 10 samples
            if (i + 1) % 10 == 0:
                logger.info(f"Batch {batch_id}: Processed {i + 1}/{len(scenarios)} samples")
    
    # Save checkpoint
    checkpoint_file = checkpoint_dir / f"batch_{batch_id:05d}.json"
    with open(checkpoint_file, "w") as f:
        json.dump(results, f)
    
    logger.info(
        f"Batch {batch_id}: Completed. Generated {len(results)}/{len(scenarios)} samples. "
        f"Saved to {checkpoint_file}"
    )
    
    return batch_id, results


def get_completed_batches(checkpoint_dir: Path) -> set:
    """Get set of completed batch IDs from checkpoint directory."""
    
    completed = set()
    
    if not checkpoint_dir.exists():
        return completed
    
    for f in checkpoint_dir.glob("batch_*.json"):
        try:
            batch_id = int(f.stem.split("_")[1])
            completed.add(batch_id)
        except (ValueError, IndexError):
            continue
    
    return completed


def merge_checkpoints(
    checkpoint_dir: Path,
    output_file: Path,
) -> int:
    """Merge all checkpoint files into a single output file."""
    
    all_samples = []
    
    checkpoint_files = sorted(checkpoint_dir.glob("batch_*.json"))
    
    for f in checkpoint_files:
        with open(f) as fp:
            samples = json.load(fp)
            all_samples.extend(samples)
    
    with open(output_file, "w") as f:
        json.dump(all_samples, f, indent=2)
    
    logger.info(f"Merged {len(checkpoint_files)} checkpoints into {output_file}")
    logger.info(f"Total samples: {len(all_samples)}")
    
    return len(all_samples)


def run_synthesis(
    num_samples: int = DEFAULT_NUM_SAMPLES,
    batch_size: int = DEFAULT_BATCH_SIZE,
    num_workers: int = DEFAULT_NUM_WORKERS,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    checkpoint_dir: str = DEFAULT_CHECKPOINT_DIR,
    resume: bool = True,
    seed: int = 42,
):
    """Run the full synthesis pipeline."""
    
    # Set random seed for reproducibility
    random.seed(seed)
    
    # Setup directories
    output_dir = Path(output_dir)
    checkpoint_dir = Path(checkpoint_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    
    # Calculate batches
    num_batches = (num_samples + batch_size - 1) // batch_size
    
    logger.info(f"Starting synthesis pipeline:")
    logger.info(f"  Backend: {'Local vLLM' if config.use_local_model else 'Snowflake Cortex'}")
    if config.use_local_model:
        logger.info(f"  Model: {config.local_model_name}")
        logger.info(f"  Tensor parallel size: {config.tensor_parallel_size}")
    else:
        logger.info(f"  Model: {config.cortex_model_name}")
    logger.info(f"  Target samples: {num_samples}")
    logger.info(f"  Batch size: {batch_size}")
    logger.info(f"  Number of batches: {num_batches}")
    logger.info(f"  Number of workers: {num_workers}")
    logger.info(f"  Output directory: {output_dir}")
    logger.info(f"  Checkpoint directory: {checkpoint_dir}")
    
    # Initialize local model if needed
    if config.use_local_model:
        logger.info("Initializing local vLLM model...")
        from local_inference import initialize_model
        initialize_model(
            model_name=config.local_model_name,
            tensor_parallel_size=config.tensor_parallel_size,
            gpu_memory_utilization=config.gpu_memory_utilization,
            max_model_len=config.max_model_len,
        )
    
    # Check for completed batches if resuming
    completed_batches = set()
    if resume:
        completed_batches = get_completed_batches(checkpoint_dir)
        if completed_batches:
            logger.info(f"Resuming: Found {len(completed_batches)} completed batches")
    
    # Generate all scenarios upfront
    logger.info("Generating medical scenarios...")
    all_scenarios = generate_scenarios_batch(num_samples)
    logger.info(f"Generated {len(all_scenarios)} scenarios")
    
    # Prepare batch tasks
    batch_tasks = []
    for batch_id in range(num_batches):
        if batch_id in completed_batches:
            continue
        
        start_idx = batch_id * batch_size
        end_idx = min(start_idx + batch_size, num_samples)
        batch_scenarios = all_scenarios[start_idx:end_idx]
        
        batch_tasks.append((batch_id, batch_scenarios, start_idx))
    
    logger.info(f"Batches to process: {len(batch_tasks)}")
    
    if not batch_tasks:
        logger.info("All batches already completed!")
    else:
        start_time = time.time()
        completed_count = 0
        
        if config.use_local_model:
            # For local model, process batches sequentially (vLLM handles parallelism internally)
            for batch_id, scenarios, start_id in batch_tasks:
                try:
                    _, results = process_batch(batch_id, scenarios, start_id, checkpoint_dir)
                    completed_count += 1
                    
                    elapsed = time.time() - start_time
                    rate = completed_count / elapsed if elapsed > 0 else 0
                    remaining = len(batch_tasks) - completed_count
                    eta_seconds = remaining / rate if rate > 0 else 0
                    
                    logger.info(
                        f"Progress: {completed_count}/{len(batch_tasks)} batches "
                        f"({rate * 60:.2f} batches/min, ETA: {eta_seconds/60:.1f} min)"
                    )
                    
                except Exception as e:
                    logger.error(f"Batch {batch_id} failed: {e}")
        else:
            # For Cortex, use thread pool for parallel processing
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                futures = {
                    executor.submit(
                        process_batch, 
                        batch_id, 
                        scenarios, 
                        start_id, 
                        checkpoint_dir
                    ): batch_id
                    for batch_id, scenarios, start_id in batch_tasks
                }
                
                for future in as_completed(futures):
                    batch_id = futures[future]
                    try:
                        _, results = future.result()
                        completed_count += 1
                        
                        elapsed = time.time() - start_time
                        rate = completed_count / elapsed if elapsed > 0 else 0
                        remaining = len(batch_tasks) - completed_count
                        eta_seconds = remaining / rate if rate > 0 else 0
                        
                        logger.info(
                            f"Progress: {completed_count}/{len(batch_tasks)} batches "
                            f"({rate * 60:.2f} batches/min, ETA: {eta_seconds/60:.1f} min)"
                        )
                        
                    except Exception as e:
                        logger.error(f"Batch {batch_id} failed: {e}")
    
    # Merge all checkpoints
    output_file = output_dir / "synthetic_train_data.json"
    total_samples = merge_checkpoints(checkpoint_dir, output_file)
    
    logger.info(f"Synthesis complete! Generated {total_samples} samples")
    logger.info(f"Output file: {output_file}")
    
    return output_file


def main():
    parser = argparse.ArgumentParser(
        description="Synthesize medical training data using Claude Sonnet 4.5 or local models"
    )
    
    # Basic options
    parser.add_argument(
        "--num-samples",
        type=int,
        default=DEFAULT_NUM_SAMPLES,
        help=f"Number of samples to generate (default: {DEFAULT_NUM_SAMPLES})",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Samples per batch (default: {DEFAULT_BATCH_SIZE})",
    )
    parser.add_argument(
        "--num-workers",
        type=int,
        default=DEFAULT_NUM_WORKERS,
        help=f"Number of parallel workers for Cortex (default: {DEFAULT_NUM_WORKERS})",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--checkpoint-dir",
        type=str,
        default=DEFAULT_CHECKPOINT_DIR,
        help=f"Checkpoint directory (default: {DEFAULT_CHECKPOINT_DIR})",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Start fresh instead of resuming from checkpoints",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)",
    )
    parser.add_argument(
        "--merge-only",
        action="store_true",
        help="Only merge existing checkpoints without generating new data",
    )
    
    # Local model options
    parser.add_argument(
        "--use-local-model",
        action="store_true",
        help="Use local vLLM model instead of Snowflake Cortex",
    )
    parser.add_argument(
        "--local-model-name",
        type=str,
        default=DEFAULT_LOCAL_MODEL,
        help=f"HuggingFace model name for local inference (default: {DEFAULT_LOCAL_MODEL})",
    )
    parser.add_argument(
        "--tensor-parallel-size",
        type=int,
        default=DEFAULT_TENSOR_PARALLEL,
        help=f"Number of GPUs for tensor parallelism (default: {DEFAULT_TENSOR_PARALLEL})",
    )
    parser.add_argument(
        "--gpu-memory-utilization",
        type=float,
        default=0.90,
        help="Fraction of GPU memory to use (default: 0.90)",
    )
    parser.add_argument(
        "--max-model-len",
        type=int,
        default=DEFAULT_MAX_MODEL_LEN,
        help=f"Maximum model context length (default: {DEFAULT_MAX_MODEL_LEN})",
    )
    
    # Cortex options
    parser.add_argument(
        "--cortex-model",
        type=str,
        default=DEFAULT_CORTEX_MODEL,
        help=f"Cortex model name (default: {DEFAULT_CORTEX_MODEL})",
    )
    
    args = parser.parse_args()
    
    # Update global config
    config.use_local_model = args.use_local_model
    config.local_model_name = args.local_model_name
    config.tensor_parallel_size = args.tensor_parallel_size
    config.cortex_model_name = args.cortex_model
    config.gpu_memory_utilization = args.gpu_memory_utilization
    config.max_model_len = args.max_model_len
    
    if args.merge_only:
        checkpoint_dir = Path(args.checkpoint_dir)
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "synthetic_train_data.json"
        merge_checkpoints(checkpoint_dir, output_file)
    else:
        try:
            run_synthesis(
                num_samples=args.num_samples,
                batch_size=args.batch_size,
                num_workers=args.num_workers,
                output_dir=args.output_dir,
                checkpoint_dir=args.checkpoint_dir,
                resume=not args.no_resume,
                seed=args.seed,
            )
        finally:
            if config.use_local_model:
                from local_inference import cleanup
                cleanup()
            else:
                session_pool.close_all()


if __name__ == "__main__":
    main()
