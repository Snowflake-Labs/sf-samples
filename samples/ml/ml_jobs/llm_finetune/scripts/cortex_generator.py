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
from snowflake.snowpark.functions import col, lit, concat, sql_expr


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
    session: Optional[Session] = None,
    debug: bool = False
) -> DataFrame:
    """
    Generate clinical visit scenarios as a Snowpark DataFrame using SQL-based AI_COMPLETE.

    Uses per-row generation for reliability, avoiding batch splitting issues.

    Args:
        num_samples: Number of scenarios to generate
        model: Cortex model to use for generation
        session: Snowpark session (default: get or create)
        debug: If True, run validation counts and print warnings

    Returns:
        Snowpark DataFrame with SUMMARY and diversity metadata columns
    """
    if session is None:
        session = Session.builder.getOrCreate()

    # Build diversity grid
    diversity_grid = build_diversity_grid(num_samples)

    # Create DataFrame with diversity grid
    diversity_df = session.create_dataframe(diversity_grid)

    # Build per-row prompt for scenario generation
    prompt_col = concat(
        lit("""Generate a brief clinical visit scenario (2-3 sentences) with these constraints:
- Specialty: """), col("SPECIALTY"),
        lit("""
- Condition Type: """), col("CONDITION_TYPE"),
        lit("""
- Patient Age: """), col("AGE_GROUP"),
        lit("""
- Visit Context: """), col("VISIT_CONTEXT"),
        lit("""

Include:
1. Doctor's full name with title (e.g., Dr. Sarah Chen)
2. Patient's full name (include guardian if pediatric)
3. Chief complaint and symptoms
4. Preliminary diagnosis

Return ONLY the scenario text, no JSON or formatting.""")
    )

    df_with_prompt = diversity_df.with_column("PROMPT", prompt_col)

    # Call AI_COMPLETE per row and clean response
    result_df = df_with_prompt.with_column(
        "SUMMARY",
        sql_expr(f"TRIM(REGEXP_REPLACE(AI_COMPLETE('{model}', PROMPT), '[[:cntrl:]]', ' '))")
    ).select(
        col("SUMMARY"),
        col("SPECIALTY"),
        col("CONDITION_TYPE"),
        col("AGE_GROUP"),
        col("VISIT_CONTEXT")
    ).filter(
        col("SUMMARY").is_not_null() &
        (sql_expr("LENGTH(SUMMARY)") > lit(50))  # Filter out empty/too-short responses
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
        lit("""Based on this clinical scenario, generate a doctor-patient dialogue and SOAP note.

SCENARIO: """),
        col("SUMMARY"),
        lit("""

CONTEXT:
- Specialty: """), col("SPECIALTY"),
        lit("""
- Condition: """), col("CONDITION_TYPE"),
        lit("""
- Age Group: """), col("AGE_GROUP"),
        lit("""
- Visit Type: """), col("VISIT_CONTEXT"),
        lit("""

OUTPUT FORMAT: Return a JSON object with exactly these 5 keys:
{
  "dialogue": "Full conversation between doctor and patient...",
  "S": "Subjective - patient's reported symptoms and history...",
  "O": "Objective - vital signs, exam findings, observations...",
  "A": "Assessment - diagnosis and clinical reasoning...",
  "P": "Plan - treatment, medications, follow-up..."
}

IMPORTANT RULES:
1. Return ONLY the JSON object - no markdown, no code blocks, no extra text
2. Use double quotes for all keys and string values
3. Escape any special characters in strings (especially quotes and newlines)
4. The dialogue should be detailed (at least 10 exchanges)
5. Each SOAP section should be 2-4 sentences

JSON OUTPUT:""")
    )

    df_with_prompt = scenarios.with_column("PROMPT", prompt_template)

    # Call AI_COMPLETE and aggressively clean the response for JSON parsing
    df_with_response = df_with_prompt.with_column(
        "RAW_RESPONSE",
        sql_expr(f"AI_COMPLETE('{model}', PROMPT)")
    ).with_column(
        "RESPONSE",
        # Multi-step cleanup for reliable JSON extraction:
        # 1. Remove markdown code fences (```json ... ```)
        # 2. Remove control characters
        # 3. Trim whitespace
        sql_expr("""
            TRIM(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(RAW_RESPONSE, '^[\\s]*```[a-zA-Z]*[\\s]*', ''),
                        '[\\s]*```[\\s]*$', ''
                    ),
                    '[[:cntrl:]]', ' '
                )
            )
        """)
    )

    # Parse JSON response and extract fields
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
