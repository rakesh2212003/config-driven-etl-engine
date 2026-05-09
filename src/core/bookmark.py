"""
Bookmark manager for SCD incremental loads.

The bookmark file path is read from:
    config/app.yaml  →  mappings.scd_bookmark

File format:
{
  "bookmark": [
    { "table_name": "scd_customer", "bookmark_timestamp": "2026-04-25 11:00:00" }
  ]
}
"""

import json
from pathlib import Path
from src.core.config import ConfigLoader
from src.core.logger import get_logger

logger = get_logger(__name__)


def _bookmark_path() -> Path:
    """Resolve the bookmark file path from app.yaml."""
    config = ConfigLoader.load_yaml("config/app.yaml")
    rel_path = config.get("mappings", {}).get("scd_bookmark", "config/mappings/bookmark/scd_bookmark.json")
    return ConfigLoader.BASE_PATH / rel_path


def load_bookmark(table: str) -> dict:
    """
    Return the bookmark entry for *table*, e.g.:
        {"table_name": "scd_customer", "bookmark_timestamp": "2026-04-25 11:00:00"}
    Returns {} when no entry exists yet.
    """
    try:
        path = _bookmark_path()

        if not path.exists() or path.stat().st_size == 0:
            logger.info(f"No bookmark file found for {table}, starting fresh")
            return {}

        with open(path, "r") as f:
            data = json.load(f)

        entries = data.get("bookmark", [])
        for entry in entries:
            if entry.get("table_name") == table:
                logger.info(f"Bookmark loaded for {table}: {entry}")
                return entry

        logger.info(f"No bookmark entry found for {table}, starting fresh")
        return {}

    except json.JSONDecodeError:
        logger.warning("Bookmark file corrupted — treating as empty")
        return {}
    except Exception as e:
        logger.error(f"Failed to load bookmark: {str(e)}", exc_info=True)
        raise


def update_bookmark(table: str, timestamp) -> None:
    """
    Upsert the bookmark_timestamp for *table* in the bookmark JSON file.
    """
    try:
        path = _bookmark_path()
        path.parent.mkdir(parents=True, exist_ok=True)

        data = {"bookmark": []}
        if path.exists() and path.stat().st_size > 0:
            with open(path, "r") as f:
                data = json.load(f)

        entries = data.get("bookmark", [])
        updated = False
        for entry in entries:
            if entry.get("table_name") == table:
                entry["bookmark_timestamp"] = str(timestamp)
                updated = True
                break

        if not updated:
            entries.append({"table_name": table, "bookmark_timestamp": str(timestamp)})

        data["bookmark"] = entries

        with open(path, "w") as f:
            json.dump(data, f, indent=4)

        logger.info(f"Bookmark updated → {table}: {timestamp}")

    except Exception as e:
        logger.error(f"Failed to update bookmark: {str(e)}", exc_info=True)
        raise
