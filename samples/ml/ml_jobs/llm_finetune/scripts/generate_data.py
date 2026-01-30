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
from functools import reduce
from typing import Dict, List, Optional, Tuple

from snowflake.cortex import complete
from snowflake.snowpark import DataFrame, Session
from snowflake.snowpark.functions import col, lit, concat, coalesce, sql_expr


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
Generate {count} unique clinical visit summaries. Each summary should be exactly one sentence.

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

    # Give up
    raise json.JSONDecodeError("Could not parse JSON with any strategy", s, 0)


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


# =============================================================================
# Core Functions
# =============================================================================

def _generate_summaries_batch(
    session: Session,
    model: str,
    diversity_batch: List[Dict[str, str]],
    batch_idx: int
) -> List[Dict]:
    """
    Generate clinical visit summaries for a batch of diversity combinations.
    Returns a list of dicts with summary fields.
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


def generate_summaries(
    num_samples: int,
    model: str,
    batch_size: Optional[int] = None,
    session: Optional[Session] = None
) -> DataFrame:
    """
    Generate clinical visit summaries as a Snowpark DataFrame.

    Args:
        num_samples: Number of summaries to generate
        model: Cortex model to use for generation
        batch_size: Number of summaries per batch (default: 100)
        session: Snowpark session (default: get or create)

    Returns:
        Snowpark DataFrame with summary columns
    """
    if session is None:
        session = Session.builder.getOrCreate()

    if batch_size is None:
        batch_size = 100

    batch_size = min(batch_size, num_samples)

    # Build diversity grid
    print(f"Building diversity grid for {num_samples} samples...")
    diversity_grid = build_diversity_grid(num_samples)

    # Generate summaries in batches
    print(f"Generating summaries in batches of {batch_size}...")
    num_batches = (num_samples + batch_size - 1) // batch_size
    batch_dfs = []

    for batch_idx in range(num_batches):
        start_idx = batch_idx * batch_size
        end_idx = min(start_idx + batch_size, num_samples)
        batch = diversity_grid[start_idx:end_idx]

        print(f"  Batch {batch_idx + 1}/{num_batches} ({len(batch)} samples)...")
        summaries = _generate_summaries_batch(session, model, batch, batch_idx)

        if summaries:
            # Create DataFrame for this batch
            batch_df = session.create_dataframe(summaries)
            batch_dfs.append(batch_df)
            print(f"    Generated {len(summaries)} summaries")
        else:
            print(f"    Warning: No summaries generated for batch {batch_idx}")

    if not batch_dfs:
        raise RuntimeError("No summaries were generated")

    # Union all batch DataFrames
    print(f"Combining {len(batch_dfs)} batches...")
    result_df = reduce(lambda a, b: a.union_all(b), batch_dfs)

    print(f"Summary generation complete")
    return result_df


def generate_full_data(summaries: DataFrame, model: str) -> DataFrame:
    """
    Generate detailed dialogues and SOAP notes from summaries using SQL AI_COMPLETE.

    Args:
        summaries: Snowpark DataFrame with summary columns
        model: Cortex model to use for generation

    Returns:
        Snowpark DataFrame with dialogue and soap columns
    """
    # Derive session from DataFrame
    session = summaries.session

    print("Generating detailed dialogues and SOAP notes using AI_COMPLETE...")

    # Build the detail generation prompt for each row
    # Column names are uppercased by Snowflake
    prompt_template = concat(
        lit("""Based on the following clinical visit summary, generate a realistic doctor-patient dialogue and a comprehensive SOAP note.

Summary:
- Doctor: """),
        coalesce(col("DOCTOR"), lit("Dr. Smith")),
        lit("\n- Patient: "),
        coalesce(col("PATIENT"), lit("John Doe")),
        lit("\n- Guardian: "),
        coalesce(col("GUARDIAN"), lit("N/A")),
        lit("\n- Symptoms: "),
        coalesce(col("SYMPTOMS"), lit("")),
        lit("\n- Diagnosis: "),
        coalesce(col("DIAGNOSIS"), lit("")),
        lit("\n- Context: "),
        coalesce(col("SUMMARY"), lit("")),
        lit("\n\nSpecialty: "),
        coalesce(col("SPECIALTY"), lit("")),
        lit("\nCondition Type: "),
        coalesce(col("CONDITION_TYPE"), lit("")),
        lit("\nAge Group: "),
        coalesce(col("AGE_GROUP"), lit("")),
        lit("\nVisit Context: "),
        coalesce(col("VISIT_CONTEXT"), lit("")),
        lit("""

Generate output as a JSON object with exactly two keys:
1. "dialogue": A realistic, detailed conversation between doctor and patient (and guardian if applicable). Include greetings, symptom discussion, examination findings, diagnosis explanation, and treatment plan discussion.
2. "soap": A properly formatted SOAP note with sections labeled as "S:", "O:", "A:", "P:" (each on its own line, followed by the content).

The SOAP note should follow this format exactly:
S: [Subjective findings - patient's complaints, symptoms, history]
O: [Objective findings - vital signs, physical exam, lab results]
A: [Assessment - diagnosis and clinical reasoning]
P: [Plan - treatment, medications, follow-up]

Return ONLY the JSON object, no other text.""")
    )

    print("  Adding prompt column...")
    df_with_prompt = summaries.with_column("PROMPT", prompt_template)

    # Call AI_COMPLETE using SQL expression and store the response
    print(f"  Calling AI_COMPLETE with model '{model}'...")
    df_with_response = df_with_prompt.with_column(
        "RESPONSE",
        sql_expr(f"REGEXP_REPLACE(AI_COMPLETE('{model}', PROMPT), '^```[a-zA-Z]*|```$|[[:cntrl:]]', ' ', 1, 0, 'm')")
    )

    # Parse JSON response and extract dialogue and soap fields
    # Filter out rows where parsing failed (NULL values)
    print("  Parsing responses and extracting fields...")
    result_df = df_with_response.select_expr(
        "TRY_PARSE_JSON(RESPONSE):dialogue::STRING as DIALOGUE",
        "TRY_PARSE_JSON(RESPONSE):soap::STRING as SOAP"
    ).filter(
        col("DIALOGUE").is_not_null() & col("SOAP").is_not_null()
    )

    print(f"Detail generation complete. Generated {result_df.count()} rows.")
    if result_df.count() != summaries.count():
        print(f"  Warning: Only {result_df.count()} valid samples were generated, expected {summaries.count()}")
    return result_df


def save_as_table(
    data: DataFrame,
    table_name: str,
    split_weights: List[float]
) -> None:
    """
    Split data and save to Snowflake tables.

    Args:
        data: Snowpark DataFrame to save
        table_name: Base table name (used as prefix)
        split_weights: List of 3 weights for train/validation/test splits
    """
    if len(split_weights) != 3:
        raise ValueError(f"split_weights must have exactly 3 values, got {len(split_weights)}")

    split_names = ["TRAIN", "VALIDATION", "TEST"]

    print(f"Splitting data with weights {split_weights}...")
    split_dfs = data.random_split(split_weights)

    for split_name, split_df in zip(split_names, split_dfs):
        full_table_name = f"{table_name}_{split_name}"
        print(f"  Saving {split_name} split to {full_table_name}...")
        split_df.write.save_as_table(
            full_table_name,
            mode="overwrite"
        )

    print("Data saved successfully")


def main():
    parser = argparse.ArgumentParser(
        description='Generate synthetic SOAP data using Snowflake Cortex LLM'
    )
    parser.add_argument(
        '--table_name',
        default='SYNTHETIC_SOAP_DATA',
        help='Base table name for output (default: SYNTHETIC_SOAP_DATA)'
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
        help='Summaries per batch for summary generation (default: 100)'
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

    # Parse splits into weights
    train_ratio, dev_ratio, test_ratio = parse_splits(args.splits)
    split_weights = [train_ratio, dev_ratio, test_ratio]
    print(f"Splits: train={train_ratio:.0%}, validation={dev_ratio:.0%}, test={test_ratio:.0%}")

    # Create Snowflake session
    print("Connecting to Snowflake...")
    session = Session.builder.getOrCreate()

    # Step 1: Generate summaries
    print(f"\n=== Step 1: Generating {args.num_samples} summaries ===")
    summaries_df = generate_summaries(
        num_samples=args.num_samples,
        model=args.model,
        batch_size=args.batch_size,
        session=session
    )

    # Step 2: Generate detailed dialogues and SOAP notes
    print(f"\n=== Step 2: Generating dialogues and SOAP notes ===")
    full_data_df = generate_full_data(summaries_df, args.model)

    # Step 3: Split and save to tables
    print(f"\n=== Step 3: Splitting and saving to tables ===")
    save_as_table(full_data_df, args.table_name, split_weights)

    print("\nDone!")


if __name__ == "__main__":
    main()
