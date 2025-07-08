import argparse
import re
from datetime import timedelta


def validate_schedule(value):
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
