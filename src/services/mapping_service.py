from src.transforms.common import apply_transformations
from src.core.logger import get_logger

logger = get_logger("MappingService")


class MappingService:

    @staticmethod
    def apply(df, mapping_config: dict):
        try:
            logger.info("Applying mapping configuration")

            columns = mapping_config.get("columns", [])

            if not columns:
                raise ValueError("No columns defined in mapping config")

            # 🔹 Apply transformations
            df = apply_transformations(df, columns)

            # 🔹 Select only target columns
            target_columns = [col["target_field"] for col in columns]
            df = df.select(*target_columns)

            logger.info(f"Mapping applied successfully. Columns: {target_columns}")
            return df

        except Exception as e:
            logger.error(f"Mapping failed: {str(e)}", exc_info=True)
            raise