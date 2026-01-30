"""
Synthetic SOAP Data Generation Pipeline

Generates synthetic clinical visit dialogues and SOAP summaries using Snowflake Cortex LLM.
Uses a two-step pipeline with a predefined diversity grid to ensure sample uniqueness.
"""

import argparse
import itertools
import json
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple

import pandas as pd
from snowflake.cortex import complete
from snowflake.snowpark import Session


MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds


# =============================================================================
# Diversity Grid Categories
# =============================================================================

SPECIALTIES = [
    "Cardiology", "Pediatrics", "Orthopedics", "Neurology", "Oncology",
    "Dermatology", "Gastroenterology", "Pulmonology", "Endocrinology",
    "Rheumatology", "Nephrology", "Psychiatry", "Ophthalmology",
    "Otolaryngology", "Urology", "Obstetrics", "Gynecology",
    "Infectious Disease", "Hematology", "Allergy and Immunology",
    "Family Medicine", "Internal Medicine", "Emergency Medicine",
    "Geriatrics", "Sports Medicine"
]

CONDITION_TYPES = [
    "Acute illness",
    "Chronic disease management", 
    "Preventive care",
    "Follow-up visit",
    "Emergency presentation"
]

AGE_GROUPS = [
    "Pediatric (0-5 years)",
    "Child (6-12 years)",
    "Adolescent (13-17 years)",
    "Young adult (18-35 years)",
    "Middle-aged adult (36-55 years)",
    "Older adult (56-70 years)",
    "Elderly (71+ years)"
]

VISIT_CONTEXTS = [
    "New patient intake",
    "Follow-up appointment",
    "Specialist referral",
    "Routine checkup",
    "Urgent care visit"
]


# =============================================================================
# Prompt Templates
# =============================================================================

SUMMARY_GENERATION_PROMPT = """\
Generate {count} unique clinical visit summaries. Each summary should be exactly 2 sentences.

For each summary, include:
- Doctor's full name (with title, e.g., Dr. Sarah Chen)
- Patient's full name
- Guardian name (only if patient is a minor, otherwise omit)
- Chief complaint / reported symptoms
- Preliminary or confirmed diagnosis

Constraints for this batch:
{constraints}

Output as a JSON array of objects with keys: "doctor", "patient", "guardian" (null if not applicable), "symptoms", "diagnosis", "summary"

Return ONLY the JSON array, no other text."""

DETAIL_GENERATION_PROMPT = """\
Based on the following clinical visit summary, generate a realistic doctor-patient dialogue and a comprehensive SOAP note.

Summary:
- Doctor: {doctor}
- Patient: {patient}
- Guardian: {guardian}
- Symptoms: {symptoms}
- Diagnosis: {diagnosis}
- Context: {summary}

Specialty: {specialty}
Condition Type: {condition_type}
Age Group: {age_group}
Visit Context: {visit_context}

Generate output as a JSON object with exactly two keys:
1. "dialogue": A realistic, detailed conversation between doctor and patient (and guardian if applicable). Include greetings, symptom discussion, examination findings, diagnosis explanation, and treatment plan discussion.
2. "soap": A properly formatted SOAP note with sections labeled as "S:", "O:", "A:", "P:" (each on its own line, followed by the content).

The SOAP note should follow this format exactly:
S: [Subjective findings - patient's complaints, symptoms, history]
O: [Objective findings - vital signs, physical exam, lab results]
A: [Assessment - diagnosis and clinical reasoning]
P: [Plan - treatment, medications, follow-up]

Return ONLY the JSON object, no other text."""


# =============================================================================
# Helper Functions
# =============================================================================

def clean_json_string(s: str) -> str:
    """Clean a string for JSON parsing by removing/escaping invalid control characters."""
    # Remove control characters except for \n, \r, \t which are valid in JSON strings
    # Control characters are 0x00-0x1F except 0x09 (tab), 0x0A (newline), 0x0D (carriage return)
    cleaned = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', s)
    return cleaned


def parse_json_safely(s: str) -> dict:
    """
    Attempt to parse JSON with multiple fallback strategies for malformed LLM output.
    """
    # Strategy 1: Direct parse after cleaning
    cleaned = clean_json_string(s)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass
    
    # Strategy 2: Try with strict=False (allows control characters in strings)
    try:
        return json.loads(cleaned, strict=False)
    except json.JSONDecodeError:
        pass
    
    # Strategy 3: Replace problematic escape sequences
    # Sometimes LLMs produce invalid escapes like \' instead of '
    try:
        fixed = re.sub(r"\\(?![\"\\\/bfnrtu])", r"\\\\", cleaned)
        return json.loads(fixed, strict=False)
    except json.JSONDecodeError:
        pass
    
    # Strategy 4: Extract fields manually with regex for dialogue/soap output
    result = {}
    dialogue_match = re.search(r'"dialogue"\s*:\s*"((?:[^"\\]|\\.)*)"\s*[,}]', cleaned, re.DOTALL)
    soap_match = re.search(r'"soap"\s*:\s*"((?:[^"\\]|\\.)*)"\s*[,}]', cleaned, re.DOTALL)
    
    if dialogue_match:
        result["dialogue"] = dialogue_match.group(1).replace('\\"', '"').replace('\\n', '\n')
    if soap_match:
        result["soap"] = soap_match.group(1).replace('\\"', '"').replace('\\n', '\n')
    
    if result:
        return result
    
    # Give up
    raise json.JSONDecodeError("Could not parse JSON with any strategy", s, 0)


# =============================================================================
# Core Functions
# =============================================================================

def parse_splits(splits_str: str) -> Tuple[float, float, float]:
    """Parse splits string like '80/10/10' into ratios."""
    parts = splits_str.split("/")
    if len(parts) != 3:
        raise ValueError(f"Splits must be in format 'train/dev/test' (e.g., '80/10/10'), got: {splits_str}")
    
    train, dev, test = map(int, parts)
    total = train + dev + test
    if total != 100:
        raise ValueError(f"Splits must sum to 100, got {total}")
    
    return train / 100, dev / 100, test / 100


def build_diversity_grid(num_samples: int) -> List[Dict[str, str]]:
    """
    Build diversity grid by creating all combinations of categories
    and cycling through them to assign to samples.
    """
    # Create all possible combinations
    all_combinations = list(itertools.product(
        SPECIALTIES, CONDITION_TYPES, AGE_GROUPS, VISIT_CONTEXTS
    ))
    
    # Shuffle for randomness
    random.shuffle(all_combinations)
    
    # Cycle through combinations to cover num_samples
    grid = []
    for i in range(num_samples):
        combo = all_combinations[i % len(all_combinations)]
        grid.append({
            "specialty": combo[0],
            "condition_type": combo[1],
            "age_group": combo[2],
            "visit_context": combo[3]
        })
    
    return grid


def generate_summaries_batch(
    session: Session,
    model: str,
    diversity_batch: List[Dict[str, str]],
    batch_idx: int
) -> List[Dict]:
    """
    Generate clinical visit summaries for a batch of diversity combinations.
    """
    # Build constraints description for this batch
    constraints_lines = []
    for i, combo in enumerate(diversity_batch):
        constraints_lines.append(
            f"{i+1}. Specialty: {combo['specialty']}, "
            f"Condition: {combo['condition_type']}, "
            f"Age: {combo['age_group']}, "
            f"Context: {combo['visit_context']}"
        )
    
    constraints = "\n".join(constraints_lines)
    prompt = SUMMARY_GENERATION_PROMPT.format(
        count=len(diversity_batch),
        constraints=constraints
    )
    
    try:
        response = complete(model=model, prompt=prompt, session=session)
        
        # Parse JSON response
        # Try to find JSON array in response
        response_text = response.strip()
        if response_text.startswith("```"):
            # Remove markdown code blocks if present
            response_text = re.sub(r"```(?:json)?\n?", "", response_text)
            response_text = response_text.strip()
        
        # Find JSON array in response (handle potential extra text)
        start_idx = response_text.find("[")
        end_idx = response_text.rfind("]") + 1
        if start_idx != -1 and end_idx > start_idx:
            response_text = response_text[start_idx:end_idx]
        
        # Parse with fallback strategies
        summaries = parse_json_safely(response_text)
        
        # Attach diversity metadata to each summary
        for i, summary in enumerate(summaries):
            if i < len(diversity_batch):
                summary.update(diversity_batch[i])
        
        return summaries
        
    except json.JSONDecodeError as e:
        print(f"  Warning: Batch {batch_idx} JSON parse failed: {e}")
        print(f"    Response preview: {response_text[:300] if 'response_text' in dir() else 'N/A'}...")
        return []
    except Exception as e:
        # Try to get more details from the exception
        error_msg = str(e)
        if hasattr(e, 'response') and hasattr(e.response, 'text'):
            error_msg += f" - Response: {e.response.text[:500]}"
        print(f"  Warning: Batch {batch_idx} failed: {error_msg}")
        return []


def generate_detail(
    session: Session,
    model: str,
    summary: Dict,
    idx: int
) -> Dict:
    """
    Generate detailed dialogue and SOAP note for a single summary.
    """
    prompt = DETAIL_GENERATION_PROMPT.format(
        doctor=summary.get("doctor", "Dr. Smith"),
        patient=summary.get("patient", "John Doe"),
        guardian=summary.get("guardian") or "N/A",
        symptoms=summary.get("symptoms", ""),
        diagnosis=summary.get("diagnosis", ""),
        summary=summary.get("summary", ""),
        specialty=summary.get("specialty", ""),
        condition_type=summary.get("condition_type", ""),
        age_group=summary.get("age_group", ""),
        visit_context=summary.get("visit_context", "")
    )
    
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            response = complete(model=model, prompt=prompt, session=session)
            
            # Parse JSON response
            response_text = response.strip()
            if response_text.startswith("```"):
                response_text = re.sub(r"```(?:json)?\n?", "", response_text)
                response_text = response_text.strip()
            
            # Find JSON object in response
            start_idx = response_text.find("{")
            end_idx = response_text.rfind("}") + 1
            if start_idx != -1 and end_idx > start_idx:
                response_text = response_text[start_idx:end_idx]
            
            # Parse with fallback strategies
            result = parse_json_safely(response_text)
            
            return {
                "dialogue": result.get("dialogue", ""),
                "soap": result.get("soap", ""),
                "idx": idx
            }
            
        except json.JSONDecodeError as e:
            # Don't retry JSON parse errors - the response won't change
            print(f"  Warning: Detail {idx} JSON parse failed: {e}")
            return None
        except Exception as e:
            last_error = e
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))  # Exponential backoff
                continue
            error_msg = str(e)
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                error_msg += f" - Response: {e.response.text[:200]}"
            print(f"  Warning: Detail generation {idx} failed after {MAX_RETRIES} retries: {error_msg}")
            return None
    
    return None


def split_data(
    records: List[Dict],
    train_ratio: float,
    dev_ratio: float,
    test_ratio: float
) -> Dict[str, List[Dict]]:
    """Split records into train/dev/test sets."""
    random.shuffle(records)
    
    n = len(records)
    train_end = int(n * train_ratio)
    dev_end = train_end + int(n * dev_ratio)
    
    return {
        "train": records[:train_end],
        "validation": records[train_end:dev_end],
        "test": records[dev_end:]
    }


def upload_data(
    session: Session,
    splits: Dict[str, List[Dict]],
    prefix: str = "synthetic_soap_data_",
    database: str = None,
    schema: str = None
) -> None:
    """Upload data splits to Snowflake tables."""
    for split_name, records in splits.items():
        if not records:
            print(f"Skipping empty {split_name} split")
            continue
            
        table_name = (prefix + split_name).upper()
        print(f"Uploading {len(records)} rows of {split_name} data to Snowflake table {table_name}...")
        
        df = pd.DataFrame(records)
        
        session.write_pandas(
            df,
            table_name,
            auto_create_table=True,
            overwrite=True,
            database=database,
            schema=schema,
        )


def main():
    parser = argparse.ArgumentParser(
        description='Generate synthetic SOAP data using Snowflake Cortex LLM'
    )
    parser.add_argument(
        '--database',
        help='Snowflake database (defaults to session default database)'
    )
    parser.add_argument(
        '--schema',
        help='Snowflake schema (defaults to session default schema)'
    )
    parser.add_argument(
        '--num_samples',
        type=int,
        default=10000,
        help='Number of samples to generate (default: 10000)'
    )
    parser.add_argument(
        '--splits',
        default='80/10/10',
        help='Train/dev/test split ratios, must sum to 100 (default: 80/10/10)'
    )
    parser.add_argument(
        '--model',
        default='llama3.1-70b',
        help='Cortex model for generation (default: llama3.1-70b)'
    )
    parser.add_argument(
        '--batch_size',
        type=int,
        default=100,
        help='Summaries per batch for Step 1 (default: 100)'
    )
    parser.add_argument(
        '--max_workers',
        type=int,
        default=10,
        help='Max parallel workers for Step 2 (default: 10)'
    )
    parser.add_argument(
        '--seed',
        type=int,
        default=42,
        help='Random seed for reproducibility (default: 42)'
    )
    
    args = parser.parse_args()
    
    # Set random seed
    random.seed(args.seed)
    
    # Parse splits
    train_ratio, dev_ratio, test_ratio = parse_splits(args.splits)
    print(f"Splits: train={train_ratio:.0%}, dev={dev_ratio:.0%}, test={test_ratio:.0%}")
    
    # Create Snowflake session
    print("Connecting to Snowflake...")
    session = Session.builder.getOrCreate()
    
    # Step 0: Build diversity grid
    print(f"\nBuilding diversity grid for {args.num_samples} samples...")
    diversity_grid = build_diversity_grid(args.num_samples)
    print(f"  Created {len(diversity_grid)} unique diversity combinations")
    
    # Step 1: Generate summaries in batches
    batch_size = min(args.batch_size, args.num_samples)
    print(f"\nStep 1: Generating summaries in batches of {batch_size}...")
    all_summaries = []
    num_batches = (args.num_samples + batch_size - 1) // batch_size
    
    for batch_idx in range(num_batches):
        start_idx = batch_idx * batch_size
        end_idx = min(start_idx + batch_size, args.num_samples)
        batch = diversity_grid[start_idx:end_idx]
        
        print(f"  Batch {batch_idx + 1}/{num_batches} ({len(batch)} samples)...")
        summaries = generate_summaries_batch(session, args.model, batch, batch_idx)
        all_summaries.extend(summaries)
        print(f"    Generated {len(summaries)} summaries (total: {len(all_summaries)})")
    
    print(f"\nStep 1 complete: {len(all_summaries)} summaries generated")
    
    if not all_summaries:
        print("Error: No summaries generated. Exiting.")
        return
    
    # Step 2: Generate detailed dialogues and SOAP notes
    print(f"\nStep 2: Generating detailed dialogues and SOAP notes...")
    all_records = []
    failed_count = 0
    
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {
            executor.submit(generate_detail, session, args.model, summary, idx): idx
            for idx, summary in enumerate(all_summaries)
        }
        
        for i, future in enumerate(as_completed(futures)):
            result = future.result()
            if result:
                all_records.append(result)
            else:
                failed_count += 1
            
            if (i + 1) % 100 == 0 or (i + 1) == len(futures):
                print(f"  Progress: {i + 1}/{len(futures)} ({len(all_records)} successful, {failed_count} failed)")
    
    print(f"\nStep 2 complete: {len(all_records)} records generated ({failed_count} failed)")
    
    if not all_records:
        print("Error: No records generated. Exiting.")
        return
    
    # Remove the idx field used for tracking
    for record in all_records:
        record.pop("idx", None)
    
    # Step 3: Split and upload data
    print(f"\nStep 3: Splitting data...")
    splits = split_data(all_records, train_ratio, dev_ratio, test_ratio)
    for split_name, records in splits.items():
        print(f"  {split_name}: {len(records)} records")
    
    print(f"\nStep 4: Uploading to Snowflake...")
    upload_data(
        session,
        splits,
        database=args.database,
        schema=args.schema
    )
    
    print("\nDone!")


if __name__ == "__main__":
    main()
