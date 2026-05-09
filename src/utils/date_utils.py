"""Date/timestamp utility helpers."""
from datetime import datetime, timezone


def utc_now_str(fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Return current UTC time as a formatted string."""
    return datetime.now(timezone.utc).strftime(fmt)


def parse_timestamp(value: str, fmt: str = "%Y-%m-%d %H:%M:%S"):
    """Parse a timestamp string to a datetime object."""
    return datetime.strptime(value, fmt)


def to_str(dt: datetime, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    return dt.strftime(fmt)
