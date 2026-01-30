"""
Synthetic SOAP Data Generation CLI

Unified command-line interface for generating synthetic clinical visit dialogues
and SOAP summaries. Supports two generation modes:

  - heuristic: Fast template-based generation (less diverse, but instant)
  - cortex: Snowflake Cortex LLM generation (high quality, but slow and expensive)

Usage:
    python generate_data.py --mode heuristic --num_samples 10000
    python generate_data.py --mode cortex --num_samples 1000 --model llama3.1-70b
"""

import argparse
import random
import time
from typing import List, Tuple

from snowflake.snowpark import DataFrame, Session


# =============================================================================
# Shared Utilities
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


# =============================================================================
# Heuristic Generation Mode
# =============================================================================

def run_heuristic_generation(args, session: Session, split_weights: List[float]) -> None:
    """Run heuristic-based (template) generation pipeline."""
    from heuristic_generator import generate_heuristic_data

    # Generate data using heuristics
    print(f"Generating {args.num_samples} samples using heuristics (template-based)...")
    t0 = time.time()
    data_df = generate_heuristic_data(
        num_samples=args.num_samples,
        session=session,
    )
    print(f"  Done in {time.time() - t0:.1f}s")

    # Split and save to tables
    print(f"Saving to {args.table_name}_{{TRAIN,VALIDATION,TEST}}...")
    t0 = time.time()
    total_rows = save_as_table(data_df, args.table_name, split_weights, session)
    print(f"  Total: {total_rows} rows in {time.time() - t0:.1f}s")


# =============================================================================
# Cortex LLM Generation Mode
# =============================================================================

def run_cortex_generation(args, session: Session, split_weights: List[float]) -> None:
    """Run LLM-based generation pipeline using Snowflake Cortex."""
    from cortex_generator import generate_scenarios, generate_full_data

    # Step 1: Generate scenarios
    print(f"Generating {args.num_samples} scenarios using Cortex ({args.model})...")
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


# =============================================================================
# CLI Entry Point
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Generate synthetic SOAP data using LLM or heuristics',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fast heuristic mode (template-based, ~20,000 samples/second)
  python generate_data.py --mode heuristic --num_samples 10000

  # LLM mode with Cortex (high quality, slower)
  python generate_data.py --mode cortex --num_samples 1000 --model llama3.1-70b

  # Specify database and schema
  python generate_data.py --mode heuristic --database MY_DB --schema MY_SCHEMA
"""
    )

    # Mode selection
    parser.add_argument(
        '--mode',
        choices=['heuristic', 'cortex'],
        default='heuristic',
        help='Generation mode: "heuristic" for templates (fast, less diverse) or "cortex" for Cortex LLM (slow, high quality). Default: heuristic'
    )

    # Output configuration
    parser.add_argument(
        '--table_name',
        default='SOAP_DATA',
        help='Base table name for output (default: SOAP_DATA)'
    )
    parser.add_argument(
        '--num_samples',
        type=int,
        default=1000,
        help='Number of samples to generate (default: 1000)'
    )
    parser.add_argument(
        '--splits',
        default='80/10/10',
        help='Train/dev/test split ratios, must sum to 100 (default: 80/10/10)'
    )

    # Cortex LLM-specific options
    parser.add_argument(
        '--model',
        default='llama3.1-70b',
        help='Cortex model for generation, only used with --mode=cortex (default: llama3.1-70b)'
    )
    parser.add_argument(
        '--batch_size',
        type=int,
        default=100,
        help='Scenarios per batch for scenario generation, only used with --mode=cortex (default: 100)'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug mode with intermediate count checks (only used with --mode=cortex)'
    )

    # Common options
    parser.add_argument(
        '--seed',
        type=int,
        default=42,
        help='Random seed for reproducibility (default: 42)'
    )
    parser.add_argument(
        '--database',
        help='Snowflake database (defaults to session default database)'
    )
    parser.add_argument(
        '--schema',
        help='Snowflake schema (defaults to session default schema)'
    )

    args = parser.parse_args()

    # Set random seed
    random.seed(args.seed)

    # Parse splits into weights
    train_ratio, dev_ratio, test_ratio = parse_splits(args.splits)
    split_weights = [train_ratio, dev_ratio, test_ratio]

    # Create Snowpark session
    session = Session.builder.getOrCreate()

    # Set database and schema if provided
    if args.database:
        session.use_database(args.database)
    if args.schema:
        session.use_schema(args.schema)

    # Run appropriate generation mode
    if args.mode == 'cortex':
        run_cortex_generation(args, session, split_weights)
    else:
        run_heuristic_generation(args, session, split_weights)


if __name__ == "__main__":
    main()
