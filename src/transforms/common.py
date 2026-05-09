"""
Apply column mappings + transformations + type casting from the JSON config.

JSON column spec:
    {
        "seq":          1,
        "source_field": "customer_identifier",
        "target_field": "customer_id",
        "data_type":    "int",
        "transform":    ["trim"]          ← key is "transform" (not "transformations")
    }

Processing order for each column:
    1.  Start with source_field column
    2.  Apply each named transform in order
    3.  Cast to data_type (if specified and not already handled by a cast_ transform)
    4.  Alias to target_field
"""
from pyspark.sql import functions as F
from src.transforms.registry import TransformRegistry
from src.core.logger import get_logger

logger = get_logger("Transform")

# Maps JSON data_type strings to Spark type strings
_TYPE_MAP = {
    "string":    "string",
    "int":       "int",
    "integer":   "int",
    "long":      "long",
    "double":    "double",
    "float":     "float",
    "boolean":   "boolean",
    "timestamp": "timestamp",
    "date":      "date",
}


def apply_transformations(df, column_mappings: list):
    """
    Rename, transform, and cast columns according to *column_mappings*.

    Returns a DataFrame that still contains ALL original columns plus
    the new target columns; caller must `.select()` to keep only what's needed.
    """
    try:
        # Sort by seq so mappings are applied in declared order
        for col_map in sorted(column_mappings, key=lambda x: x.get("seq", 0)):
            source    = col_map["source_field"]
            target    = col_map["target_field"]
            data_type = col_map.get("data_type", "string")
            # Support both "transform" and "transformations" keys for flexibility
            transforms = col_map.get("transform", col_map.get("transformations", []))

            col_expr = F.col(source)

            # 1. Apply named transforms in order
            for t in transforms:
                fn = TransformRegistry.get(t)
                col_expr = fn(col_expr)

            # 2. Cast to declared data_type (skip if already handled by a cast_ transform)
            spark_type = _TYPE_MAP.get(data_type.lower())
            if spark_type and not any(t.startswith("cast_") for t in transforms):
                col_expr = col_expr.cast(spark_type)

            # 3. Assign target column
            df = df.withColumn(target, col_expr)

        logger.info("All transformations applied successfully")
        return df

    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}", exc_info=True)
        raise
