"""
Synthetic SOAP Data Generation Pipeline

Generates synthetic clinical visit dialogues and SOAP summaries using Snowflake Cortex LLM.
Uses a two-step pipeline with a predefined diversity grid to ensure sample uniqueness.
"""

import argparse
import itertools
import random
import time
from typing import Dict, List, Optional, Tuple

from snowflake.snowpark import DataFrame, Session
from snowflake.snowpark.functions import col, lit, concat, coalesce, sql_expr, listagg, count as sf_count


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
# Helper Functions
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


# =============================================================================
# Core Functions
# =============================================================================

def generate_scenarios(
    num_samples: int,
    model: str,
    batch_size: Optional[int] = None,
    session: Optional[Session] = None,
    debug: bool = False
) -> DataFrame:
    """
    Generate clinical visit scenarios as a Snowpark DataFrame using SQL-based AI_COMPLETE.

    Uses SPLIT_TO_TABLE to expand batch LLM responses into individual prose summaries,
    then joins back to the diversity grid to attach metadata.

    Args:
        num_samples: Number of scenarios to generate
        model: Cortex model to use for generation
        batch_size: Number of scenarios per batch (default: 100)
        session: Snowpark session (default: get or create)
        debug: If True, run validation counts and print warnings

    Returns:
        Snowpark DataFrame with summary columns
    """
    if session is None:
        session = Session.builder.getOrCreate()

    if batch_size is None:
        batch_size = 100

    batch_size = min(batch_size, num_samples)

    # Build diversity grid
    diversity_grid = build_diversity_grid(num_samples)

    # Add batch indexing to each entry
    for i, entry in enumerate(diversity_grid):
        entry["batch_id"] = i // batch_size
        entry["idx_in_batch"] = (i % batch_size) + 1

    # Create DataFrame with diversity grid
    diversity_df = session.create_dataframe(diversity_grid)

    # Build constraint string for each row
    constraint_col = concat(
        col("IDX_IN_BATCH").cast("STRING"),
        lit(". Specialty: "), col("SPECIALTY"),
        lit(", Condition: "), col("CONDITION_TYPE"),
        lit(", Age: "), col("AGE_GROUP"),
        lit(", Context: "), col("VISIT_CONTEXT")
    )
    df_with_constraints = diversity_df.with_column("CONSTRAINT_STR", constraint_col)

    # Aggregate constraints by batch using LISTAGG
    batch_df = df_with_constraints.group_by("BATCH_ID").agg(
        listagg("CONSTRAINT_STR", lit("\n")).within_group("IDX_IN_BATCH").alias("CONSTRAINTS"),
        sf_count("*").alias("BATCH_COUNT")
    )

    # Build prompt for each batch - LLM echoes diversity metadata in each summary
    prompt_col = concat(
        lit("Generate "), col("BATCH_COUNT").cast("STRING"),
        lit(""" unique clinical visit summaries. Each summary should be exactly one sentence.

For each summary, include:
- Doctor's full name (with title, e.g., Dr. Sarah Chen)
- Patient's full name
- Guardian name (only if patient is a minor, otherwise null)
- Chief complaint / reported symptoms
- Preliminary or confirmed diagnosis
- Echo back the exact specialty, condition_type, age_group, and visit_context from the constraints

Constraints for this batch:
"""),
        col("CONSTRAINTS"),
        lit("""

Output each summary as a separate JSON object. Separate each JSON object with the delimiter "|||NEXT|||".
Each JSON object must have these keys: doctor, patient, guardian, symptoms, diagnosis, summary, specialty, condition_type, age_group, visit_context

Example format:
{"doctor": "Dr. Smith", "patient": "John Doe", "guardian": null, "symptoms": "chest pain", "diagnosis": "angina", "summary": "Dr. Smith evaluated John Doe for chest pain and diagnosed angina.", "specialty": "Cardiology", "condition_type": "Acute illness", "age_group": "Young adult (18-35 years)", "visit_context": "New patient intake"}|||NEXT|||{"doctor": "Dr. Jones", ...}

Return ONLY the JSON objects separated by |||NEXT|||, no markdown code blocks, no other text.""")
    )

    df_with_prompt = batch_df.with_column("PROMPT", prompt_col)

    # Call AI_COMPLETE with REGEXP_REPLACE to clean markdown artifacts
    df_with_response = df_with_prompt.with_column(
        "RESPONSE",
        sql_expr(f"REGEXP_REPLACE(AI_COMPLETE('{model}', PROMPT), '^```[a-zA-Z]*|```$|[[:cntrl:]]', ' ', 1, 0, 'm')")
    )

    # Use SPLIT_TO_TABLE with LATERAL to expand batch responses into individual samples
    expanded_df = df_with_response.join_table_function(
        "split_to_table",
        col("RESPONSE"),
        lit("|||NEXT|||")
    )

    # Clean control characters and parse JSON once per sample
    parsed_df = expanded_df.with_column(
        "PARSED_JSON",
        sql_expr("TRY_PARSE_JSON(VALUE)")
    )

    # Extract all fields from the single parsed JSON object
    result_df = parsed_df.select(
        sql_expr("PARSED_JSON:doctor::STRING").alias("DOCTOR"),
        sql_expr("PARSED_JSON:patient::STRING").alias("PATIENT"),
        sql_expr("PARSED_JSON:guardian::STRING").alias("GUARDIAN"),
        sql_expr("PARSED_JSON:symptoms::STRING").alias("SYMPTOMS"),
        sql_expr("PARSED_JSON:diagnosis::STRING").alias("DIAGNOSIS"),
        sql_expr("PARSED_JSON:summary::STRING").alias("SUMMARY"),
        sql_expr("PARSED_JSON:specialty::STRING").alias("SPECIALTY"),
        sql_expr("PARSED_JSON:condition_type::STRING").alias("CONDITION_TYPE"),
        sql_expr("PARSED_JSON:age_group::STRING").alias("AGE_GROUP"),
        sql_expr("PARSED_JSON:visit_context::STRING").alias("VISIT_CONTEXT")
    ).filter(
        col("DOCTOR").is_not_null()
    )

    if debug:
        result_count = result_df.count()
        if result_count < num_samples:
            print(f"  Warning: Only {result_count} valid scenarios generated, expected {num_samples}")

    return result_df


def generate_full_data(scenarios: DataFrame, model: str, debug: bool = False) -> DataFrame:
    """
    Generate detailed dialogues and SOAP notes from scenario summaries using SQL AI_COMPLETE.

    Args:
        scenarios: Snowpark DataFrame with SUMMARY and diversity metadata columns
        model: Cortex model to use for generation
        debug: If True, run validation counts and print warnings

    Returns:
        Snowpark DataFrame with dialogue and SOAP section columns
    """
    # Build the detail generation prompt for each row
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

Generate output as a JSON object with exactly five keys:
1. "dialogue": A realistic, detailed conversation between doctor and patient (and guardian if applicable). Include greetings, symptom discussion, examination findings, diagnosis explanation, and treatment plan discussion.
2. "S": Subjective findings - patient's complaints, symptoms, history as reported by the patient
3. "O": Objective findings - vital signs, physical exam findings, lab results, observations
4. "A": Assessment - diagnosis and clinical reasoning
5. "P": Plan - treatment, medications, follow-up instructions

Return ONLY the JSON object, no other text.""")
    )

    df_with_prompt = scenarios.with_column("PROMPT", prompt_template)

    # Call AI_COMPLETE using SQL expression and store the response
    df_with_response = df_with_prompt.with_column(
        "RESPONSE",
        sql_expr(f"REGEXP_REPLACE(AI_COMPLETE('{model}', PROMPT), '^```[a-zA-Z]*|```$|[[:cntrl:]]', ' ', 1, 0, 'm')")
    )

    # Parse JSON response once, then extract dialogue and SOAP section fields
    # Filter out rows where parsing failed (NULL values)
    parsed_df = df_with_response.with_column(
        "PARSED_JSON",
        sql_expr("TRY_PARSE_JSON(RESPONSE)")
    )
    result_df = parsed_df.select(
        sql_expr("PARSED_JSON:dialogue::STRING").alias("DIALOGUE"),
        sql_expr("PARSED_JSON:S::STRING").alias("S"),
        sql_expr("PARSED_JSON:O::STRING").alias("O"),
        sql_expr("PARSED_JSON:A::STRING").alias("A"),
        sql_expr("PARSED_JSON:P::STRING").alias("P")
    ).filter(
        col("DIALOGUE").is_not_null() &
        col("S").is_not_null() &
        col("O").is_not_null() &
        col("A").is_not_null() &
        col("P").is_not_null()
    )

    if debug:
        result_count = result_df.count()
        expected_count = scenarios.count()
        if result_count != expected_count:
            print(f"  Warning: Only {result_count} valid samples generated, expected {expected_count}")

    return result_df


def save_as_table(
    data: DataFrame,
    table_name: str,
    split_weights: List[float],
    session: Session
) -> int:
    """
    Split data and save to Snowflake tables.

    Args:
        data: Snowpark DataFrame to save
        table_name: Base table name (used as prefix)
        split_weights: List of 3 weights for train/validation/test splits
        session: Snowpark session for querying table counts

    Returns:
        Total number of rows saved across all splits
    """
    if len(split_weights) != 3:
        raise ValueError(f"split_weights must have exactly 3 values, got {len(split_weights)}")

    split_names = ["TRAIN", "VALIDATION", "TEST"]
    split_dfs = data.random_split(split_weights)

    total_rows = 0
    for split_name, split_df in zip(split_names, split_dfs):
        full_table_name = f"{table_name}_{split_name}"
        split_df.write.save_as_table(full_table_name, mode="overwrite")
        # Query the saved table to get actual row count
        count = session.table(full_table_name).count()
        total_rows += count
        print(f"  {split_name}: {count} rows")

    return total_rows


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
        help='Scenarios per batch for scenario generation (default: 100)'
    )
    parser.add_argument(
        '--seed',
        type=int,
        default=42,
        help='Random seed for reproducibility (default: 42)'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug mode with intermediate count checks'
    )

    args = parser.parse_args()

    # Set random seed
    random.seed(args.seed)

    # Parse splits into weights
    train_ratio, dev_ratio, test_ratio = parse_splits(args.splits)
    split_weights = [train_ratio, dev_ratio, test_ratio]

    session = Session.builder.getOrCreate()

    # Step 1: Generate scenarios
    print(f"Generating {args.num_samples} scenarios...")
    t0 = time.time()
    scenarios_df = generate_scenarios(
        num_samples=args.num_samples,
        model=args.model,
        batch_size=args.batch_size,
        session=session,
        debug=args.debug
    )
    print(f"  Done in {time.time() - t0:.1f}s")

    # Step 2: Generate detailed dialogues and SOAP notes
    print("Generating dialogues and SOAP notes...")
    t0 = time.time()
    full_data_df = generate_full_data(scenarios_df, args.model, debug=args.debug)
    print(f"  Done in {time.time() - t0:.1f}s")

    # Step 3: Split and save to tables
    print(f"Saving to {args.table_name}_{{TRAIN,VALIDATION,TEST}}...")
    t0 = time.time()
    total_rows = save_as_table(full_data_df, args.table_name, split_weights, session)
    print(f"  Total: {total_rows} rows in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
