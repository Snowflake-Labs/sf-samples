import argparse
import re
from datetime import timedelta


def validate_schedule(value):
    """
    Validate and convert a schedule string to a timedelta object.

    This function validates schedule format and converts it to a timedelta object
    for use with argparse. It supports formats like "1d" (1 day), "12h" (12 hours),
    "30m" (30 minutes), or just numbers (interpreted as days).

    Args:
        value: Schedule string to validate and convert

    Returns:
        timedelta or None: Converted timedelta object, or None if value is None

    Raises:
        argparse.ArgumentTypeError: If the format is invalid or conversion fails
    """
    if value is None:
        return None
    if not re.match(r"^(\d+[dhm]|\d+)$", value):
        raise argparse.ArgumentTypeError(
            f"Invalid schedule format: {value}. Must be a number followed by 'd', 'h', 'm', or just a number for days."
        )

    # Convert to timedelta
    try:
        if value.endswith("d"):
            return timedelta(days=int(value[:-1]))
        elif value.endswith("h"):
            return timedelta(hours=int(value[:-1]))
        elif value.endswith("m"):
            return timedelta(minutes=int(value[:-1]))
        else:
            return timedelta(days=int(value))
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Could not convert {value} to a valid timedelta."
        )
