import os
import json
from datetime import datetime
from src.core.logger import get_logger

logger = get_logger(__name__)


def format_duration(elapsed: float) -> str:
    """
    Format elapsed seconds into a human-readable duration string.

    Parameters
    ----------
    elapsed : elapsed time in seconds

    Returns
    -------
    str : e.g. '1h 2m 3.45s' | '2m 3.45s' | '3.45s'
    """
    mins, secs = divmod(elapsed, 60)
    hrs, mins  = divmod(mins, 60)

    if hrs:
        return f"{int(hrs)}h {int(mins)}m {secs:.2f}s"
    if mins:
        return f"{int(mins)}m {secs:.2f}s"
    return f"{secs:.2f}s"


def write_audit(audit: dict, audit_config: dict) -> None:
    """
    Append an audit record to the daily audit log for the table.
    Each line is a JSON object (newline-delimited JSON / .jsonl).

    Parameters
    ----------
    audit        : audit record dict
    audit_config : audit section from app config
    """
    try:
        if not audit_config.get("enabled", False):
            return

        audit_dir  = audit_config["path"]
        table_name = audit["table_name"]
        audit_path = os.path.join(audit_dir, table_name)

        os.makedirs(audit_path, exist_ok=True)

        audit_file = os.path.join(
            audit_path,
            f"audit_{datetime.now():%Y%m%d}.jsonl"
        )

        fields       = audit_config.get("fields", list(audit.keys()))
        audit_record = {k: audit.get(k) for k in fields}

        with open(audit_file, "a") as f:
            f.write(json.dumps(audit_record, default=str) + "\n")

        logger.debug(f"Audit record written to '{audit_file}'.")

    except Exception as e:
        logger.error(f"Failed to write audit record: {e}")
        # Audit failure should never stop the pipeline
        # so we log and swallow the exception


def ensure_dir(path: str) -> str:
    """
    Create directory if it does not exist.

    Parameters
    ----------
    path : directory path

    Returns
    -------
    str : same path (for chaining)
    """
    try:
        os.makedirs(path, exist_ok=True)
        return path
    except Exception as e:
        logger.error(f"Failed to create directory '{path}': {e}")
        raise


def sanitize_filename(name: str) -> str:
    """
    Remove or replace characters unsafe for filenames.

    Parameters
    ----------
    name : raw string

    Returns
    -------
    str : sanitized filename
    """
    return "".join(c if c.isalnum() or c in ("_", "-", ".") else "_" for c in name)