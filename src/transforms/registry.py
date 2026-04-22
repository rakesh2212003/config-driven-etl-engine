from pyspark.sql import functions as F


class TransformRegistry:
    _registry = {
        "trim": lambda c: F.trim(c),
        "upper": lambda c: F.upper(c),
        "lower": lambda c: F.lower(c),
        "cast_int": lambda c: c.cast("int"),
        "cast_string": lambda c: c.cast("string"),
    }

    @classmethod
    def get(cls, name: str):
        if name not in cls._registry:
            raise ValueError(f"Unknown transformation: {name}")

        return cls._registry[name]