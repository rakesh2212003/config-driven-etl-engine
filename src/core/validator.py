from src.core.config import ConfigLoader
from src.core.logger import get_logger
from src.pipelines.registry import _REGISTRY

logger = get_logger(__name__)

def validate(job:str, table_name:str) -> None:
    errors = []

    valid_jobs = sorted(_REGISTRY.keys())
    if job not in _REGISTRY:
        errors.append(
            f"Unknown job '{job}'. Available jobs: {valid_jobs}"
        )

    # 2. Validate table (only if job is valid — no point checking further otherwise)
    if not errors:
        app_config = ConfigLoader.load_yaml("config/app.yaml")
        valid_tables = sorted(app_config.get("tables", {}).keys())
        if table_name not in app_config.get("tables", {}):
            errors.append(
                f"Unknown table '{table_name}'. Available tables: {valid_tables}"
            )

    if errors:
        for err in errors:
            logger.error(f"Validation failed: {err}")
        raise ValueError("\n".join(errors))

    logger.info(f"Validation passed — job='{job}' table='{table_name}'")
