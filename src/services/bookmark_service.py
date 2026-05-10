import json
from pathlib import Path
from datetime import datetime

from src.core.logger import get_logger


logger = get_logger(__name__)


def load_bookmark(bookmark_path:str) -> dict:
    path = Path(bookmark_path)
    try:
        if not path.exists():
            logger.warning(
                f"Bookmark file not found: "
                f"{bookmark_path}"
            )
            return {}
        with open(path, "r") as file:
            bookmark = json.load(file)
        logger.info(
            f"Bookmark loaded: {bookmark_path}"
        )
        return bookmark
    except Exception as error:
        logger.exception(
            f"Failed to load bookmark: "
            f"{bookmark_path}"
        )
        raise error


def update_bookmark(bookmark_path:str, bookmark_column:str, bookmark_value:str) -> None:
    path = Path(bookmark_path)
    try:
        updated_bookmark = {
            "bookmark_column": bookmark_column,
            "last_processed_value": bookmark_value,
            "last_run_finished_at": (
                datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            ),
            "last_run_status": "SUCCESS"
        }

        with open(path, "w") as file:
            json.dump(
                updated_bookmark,
                file,
                indent=4
            )

        logger.info(
            f"Bookmark updated successfully: "
            f"{bookmark_path}"
        )

    except Exception as error:
        logger.exception(
            f"Failed to update bookmark: "
            f"{bookmark_path}"
        )
        raise error
    