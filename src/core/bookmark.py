import json
from pathlib import Path
from src.core.logger import get_logger


logger = get_logger(__name__)

BOOKMARK_FILE = Path("config/bookmarks.json")


# -------------------------
# Load bookmark
# -------------------------
def load_bookmark(table: str) -> dict:
    try:
        if not BOOKMARK_FILE.exists():
            logger.info("Bookmark file not found, starting fresh")
            return {}

        # 🔹 Handle empty file
        if BOOKMARK_FILE.stat().st_size == 0:
            logger.warning("Bookmark file is empty, initializing")
            return {}

        with open(BOOKMARK_FILE, "r") as f:
            data = json.load(f)

        return data.get(table, {})

    except json.JSONDecodeError:
        logger.warning("Bookmark file corrupted, resetting")
        return {}

    except Exception as e:
        logger.error(f"Failed to load bookmark: {str(e)}", exc_info=True)
        raise

# -------------------------
# Update bookmark
# -------------------------
def update_bookmark(table: str, key: str, value):
    try:
        data = {}

        if BOOKMARK_FILE.exists():
            with open(BOOKMARK_FILE, "r") as f:
                data = json.load(f)

        if table not in data:
            data[table] = {}

        data[table][key] = value

        with open(BOOKMARK_FILE, "w") as f:
            json.dump(data, f, indent=2)

        logger.info(f"Bookmark updated for {table}: {key} = {value}")

    except Exception as e:
        logger.error(f"Failed to update bookmark: {str(e)}", exc_info=True)
        raise