"""
Transform registry.

Each entry maps a transform name (used in the JSON mapping config)
to a function that takes a Spark Column and returns a Column.
"""
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, DoubleType, TimestampType, DateType


class TransformRegistry:
    _registry = {
        # String
        "trim":       lambda c: F.trim(c),
        "upper":      lambda c: F.upper(c),
        "lower":      lambda c: F.lower(c),
        "capitalize": lambda c: F.initcap(F.trim(c)),   # "john doe" → "John Doe"

        # Cast
        "cast_int":       lambda c: c.cast(IntegerType()),
        "cast_long":      lambda c: c.cast(LongType()),
        "cast_double":    lambda c: c.cast(DoubleType()),
        "cast_string":    lambda c: c.cast("string"),
        "cast_timestamp": lambda c: c.cast(TimestampType()),
        "cast_date":      lambda c: c.cast(DateType()),

        # Null handling
        "null_if_empty":  lambda c: F.when(F.trim(c) == "", None).otherwise(c),
    }

    @classmethod
    def get(cls, name: str):
        if name not in cls._registry:
            raise ValueError(
                f"Unknown transformation: '{name}'. "
                f"Available: {sorted(cls._registry.keys())}"
            )
        return cls._registry[name]

    @classmethod
    def register(cls, name: str, fn):
        """Allow dynamic registration of custom transforms."""
        cls._registry[name] = fn
