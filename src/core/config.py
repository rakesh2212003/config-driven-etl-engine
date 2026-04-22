import json
import yaml
from pathlib import Path
from src.core.logger import get_logger

logger = get_logger("ConfigLoader")


class ConfigLoader:
    BASE_PATH = Path(__file__).resolve().parents[2]

    @classmethod
    def get_path(cls, relative_path: str) -> Path:
        try:
            path = cls.BASE_PATH / relative_path

            if not path.exists():
                raise FileNotFoundError(f"Config file not found: {path}")

            return path

        except Exception as e:
            logger.error(f"Path resolution failed: {str(e)}", exc_info=True)
            raise

    @classmethod
    def load_json(cls, relative_path: str) -> dict:
        try:
            path = cls.get_path(relative_path)

            with open(path, "r") as f:
                data = json.load(f)

            logger.info(f"Loaded JSON config: {relative_path}")
            return data

        except Exception as e:
            logger.error(f"Failed to load JSON: {relative_path}", exc_info=True)
            raise

    @classmethod
    def load_yaml(cls, relative_path: str) -> dict:
        try:
            path = cls.get_path(relative_path)

            with open(path, "r") as f:
                data = yaml.safe_load(f)

            logger.info(f"Loaded YAML config: {relative_path}")
            return data

        except Exception as e:
            logger.error(f"Failed to load YAML: {relative_path}", exc_info=True)
            raise