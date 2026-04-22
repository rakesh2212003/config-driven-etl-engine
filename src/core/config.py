import json
import os
import shutil
from datetime import datetime

import yaml

from src.core.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BOOKMARK_DIR         = "data/bookmarks"
BOOKMARK_CURRENT_DIR = os.path.join(BOOKMARK_DIR, "current")
BOOKMARK_HISTORY_DIR = os.path.join(BOOKMARK_DIR, "history")


# ---------------------------------------------------------------------------
# App config
# ---------------------------------------------------------------------------

def load_config(path: str = "config/app.yaml") -> dict:
    """
    Load and return the app config from a YAML file.
    """
    try:
        if not os.path.exists(path):
            raise FileNotFoundError(f"App config not found at '{path}'.")

        with open(path, "r") as f:
            config = yaml.safe_load(f)

        logger.info(f"App config loaded from '{path}'.")
        return config

    except FileNotFoundError as e:
        logger.error(str(e))
        raise
    except yaml.YAMLError as e:
        logger.error(f"Failed to parse app config '{path}': {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error loading app config '{path}': {e}")
        raise


# ---------------------------------------------------------------------------
# Mapping config
# ---------------------------------------------------------------------------

def load_mapping(
    table_name: str,
    mapping_dir: str = "config/mappings/data-mapping-config",
) -> dict:
    """
    Load and return the data mapping config for a given table.
    """
    try:
        path = os.path.join(mapping_dir, f"{table_name}.json")

        if not os.path.exists(path):
            raise FileNotFoundError(
                f"Mapping config for table '{table_name}' not found at '{path}'."
            )

        with open(path, "r") as f:
            mapping = json.load(f)

        logger.info(f"Mapping config for '{table_name}' loaded from '{path}'.")
        return mapping

    except FileNotFoundError as e:
        logger.error(str(e))
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse mapping config for '{table_name}': {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error loading mapping config for '{table_name}': {e}")
        raise


# ---------------------------------------------------------------------------
# Bookmark — separate file per table + history
#
# Structure:
#   data/bookmarks/
#     current/
#       customers.json        ← current state for customers
#       orders.json           ← current state for orders
#     history/
#       customers/
#         customers_20240101_120000.json   ← snapshot per update
#         customers_20240102_093000.json
#       orders/
#         orders_20240101_120000.json
# ---------------------------------------------------------------------------

def _bookmark_path(table_name: str) -> str:
    return os.path.join(BOOKMARK_CURRENT_DIR, f"{table_name}.json")


def _bookmark_history_path(table_name: str) -> str:
    return os.path.join(BOOKMARK_HISTORY_DIR, table_name)


def load_bookmark(table_name: str) -> dict:
    """
    Load the current bookmark for a table.
    Returns empty dict if no bookmark exists yet (fresh start).

    Parameters
    ----------
    table_name : table name (e.g. 'customers')

    Returns
    -------
    dict with keys: last_processed_file, last_processed_at, status, history[]
    """
    try:
        path = _bookmark_path(table_name)

        if not os.path.exists(path):
            logger.warning(
                f"No bookmark found for '{table_name}' at '{path}'. Starting fresh."
            )
            return {
                "table":               table_name,
                "last_processed_file": None,
                "last_processed_at":   None,
                "status":              None,
            }

        with open(path, "r") as f:
            bookmark = json.load(f)

        logger.info(
            f"Bookmark loaded for '{table_name}' — "
            f"last_file='{bookmark.get('last_processed_file')}' "
            f"status='{bookmark.get('status')}'"
        )
        return bookmark

    except json.JSONDecodeError as e:
        logger.error(f"Bookmark file for '{table_name}' is corrupted: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error loading bookmark for '{table_name}': {e}")
        raise


def save_bookmark(table_name: str, file_name: str, status: str) -> None:
    """
    Save the current bookmark for a table and archive a history snapshot.

    Parameters
    ----------
    table_name : table name (e.g. 'customers')
    file_name  : CDC filename just processed (e.g. 'cdc_20240101_120000.csv')
    status     : 'SUCCESS' | 'FAILED'
    """
    try:
        now       = datetime.now()
        now_str   = now.strftime("%Y-%m-%d %H:%M:%S")
        now_stamp = now.strftime("%Y%m%d_%H%M%S")

        bookmark = {
            "table":               table_name,
            "last_processed_file": file_name,
            "last_processed_at":   now_str,
            "status":              status,
        }

        # --- Save current ---
        current_path = _bookmark_path(table_name)
        os.makedirs(BOOKMARK_CURRENT_DIR, exist_ok=True)

        with open(current_path, "w") as f:
            json.dump(bookmark, f, indent=4, default=str)

        logger.info(f"Bookmark saved for '{table_name}' [{status}] → '{current_path}'")

        # --- Archive to history ---
        history_dir = _bookmark_history_path(table_name)
        os.makedirs(history_dir, exist_ok=True)

        history_file = os.path.join(
            history_dir, f"{table_name}_{now_stamp}.json"
        )

        shutil.copy2(current_path, history_file)
        logger.debug(f"Bookmark history snapshot saved → '{history_file}'")

    except Exception as e:
        logger.error(f"Failed to save bookmark for '{table_name}': {e}")
        raise