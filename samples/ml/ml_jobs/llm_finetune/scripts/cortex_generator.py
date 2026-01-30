"""
LLM-Based SOAP Data Generation Utilities

Generates synthetic clinical visit dialogues and SOAP summaries using Snowflake Cortex LLM.
Uses a two-step pipeline with a predefined diversity grid to ensure sample uniqueness.

This module provides the core LLM generation logic and can be imported by CLI scripts.
"""

import itertools
import random
from typing import Dict, List, Optional

from snowflake.snowpark import DataFrame, Session
from snowflake.snowpark.functions import col, lit, concat, sql_expr, listagg, count as sf_count


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
# Core Generation Functions
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
        Snowpark DataFrame with SUMMARY and diversity metadata columns
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

    # Create DataFrame with diversity grid (used for join later)
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

    # Build prompt for prose summaries (no JSON required)
    prompt_col = concat(
        lit("Generate "), col("BATCH_COUNT").cast("STRING"),
        lit(""" unique clinical visit scenario summaries, one for each numbered constraint below.

Each summary should be a short paragraph (2-3 sentences) describing a realistic clinical encounter, including:
- The doctor's full name with title (e.g., Dr. Sarah Chen)
- The patient's full name (include guardian name if patient is a minor)
- The presenting symptoms or chief complaint
- The preliminary or confirmed diagnosis
- Context appropriate to the specialty and visit type

Constraints:
"""),
        col("CONSTRAINTS"),
        lit("""

IMPORTANT: Output each summary as plain text separated by "|||". Output them in order matching the constraint numbers.
Do NOT use JSON, numbering, or any other formatting. Just the summary paragraphs separated by |||.

Example output format:
Dr. Sarah Chen evaluated 45-year-old Michael Torres at his cardiology follow-up appointment for ongoing management of atrial fibrillation. Mr. Torres reported occasional palpitations and Dr. Chen adjusted his anticoagulation therapy.|||Dr. James Wilson, a pediatric specialist, saw 3-year-old Emma Garcia accompanied by her mother Maria for acute otitis media. Emma presented with ear pain and fever, and Dr. Wilson prescribed a course of antibiotics.""")
    )

    df_with_prompt = batch_df.with_column("PROMPT", prompt_col)

    # Call AI_COMPLETE with REGEXP_REPLACE to clean control characters
    df_with_response = df_with_prompt.with_column(
        "RESPONSE",
        sql_expr(f"REGEXP_REPLACE(AI_COMPLETE('{model}', PROMPT), '[[:cntrl:]]', ' ')")
    )

    # Use SPLIT_TO_TABLE to expand batch responses into individual samples
    # SPLIT_TO_TABLE returns INDEX (1-based position) which maps to IDX_IN_BATCH
    expanded_df = df_with_response.join_table_function(
        "split_to_table",
        col("RESPONSE"),
        lit("|||")
    )

    # Trim whitespace from each summary and keep BATCH_ID + INDEX for joining
    summaries_df = expanded_df.select(
        col("BATCH_ID"),
        col("INDEX"),
        sql_expr("TRIM(VALUE)").alias("SUMMARY")
    ).filter(
        col("SUMMARY") != lit("")
    )

    # Join back to diversity_df to attach metadata using BATCH_ID and INDEX
    result_df = summaries_df.join(
        diversity_df,
        (summaries_df["BATCH_ID"] == diversity_df["BATCH_ID"]) &
        (summaries_df["INDEX"] == diversity_df["IDX_IN_BATCH"])
    ).select(
        summaries_df["SUMMARY"],
        diversity_df["SPECIALTY"],
        diversity_df["CONDITION_TYPE"],
        diversity_df["AGE_GROUP"],
        diversity_df["VISIT_CONTEXT"]
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
        lit("""Based on the following clinical visit scenario, generate a realistic doctor-patient dialogue and SOAP note components.

Scenario:
"""),
        col("SUMMARY"),
        lit("""

Additional context:
- Specialty: """),
        col("SPECIALTY"),
        lit("""
- Condition Type: """),
        col("CONDITION_TYPE"),
        lit("""
- Age Group: """),
        col("AGE_GROUP"),
        lit("""
- Visit Context: """),
        col("VISIT_CONTEXT"),
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

