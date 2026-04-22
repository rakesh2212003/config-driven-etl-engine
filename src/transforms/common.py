from src.transforms.registry import TransformRegistry
from src.core.logger import get_logger

logger = get_logger("Transform")


def apply_transformations(df, column_mappings: list):
    try:
        for col_map in column_mappings:
            source = col_map["source_field"]
            target = col_map["target_field"]
            transforms = col_map.get("transformations", [])

            # Start with source column
            col_expr = df[source]

            # Apply transformations in sequence
            for t in transforms:
                transform_fn = TransformRegistry.get(t)
                col_expr = transform_fn(col_expr)

            # Assign to target column
            df = df.withColumn(target, col_expr)

        logger.info("All transformations applied successfully")
        return df

    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}", exc_info=True)
        raise