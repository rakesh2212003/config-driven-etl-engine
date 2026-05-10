import json
import yaml
from pathlib import Path
from src.core.logger import get_logger

logger = get_logger(__name__)
BASE_PATH = Path(__file__).resolve().parents[2]

def resolve_path(relative_path:str) -> Path:
    path = BASE_PATH / relative_path
    if not path.exists():
        logger.error(f"Config file not found: {path}")
        raise FileNotFoundError(f"Config file not found: {path}")
    return path


def load_json(relative_path:str) -> dict:
    path = resolve_path(relative_path)
    try:
        logger.info(f"Loading JSON config: {relative_path}")
        with open(path, "r", encoding="utf-8") as file:
            return json.load(file)
    except Exception:
        logger.exception(f"Failed to load JSON config: {relative_path}")
        raise


def load_yaml(relative_path: str) -> dict:
    path = resolve_path(relative_path)
    try:
        logger.info(f"Loading YAML config: {relative_path}")
        with open(path, "r", encoding="utf-8") as file:
            return yaml.safe_load(file)
    except Exception:
        logger.exception(f"Failed to load YAML config: {relative_path}")
        raise
    