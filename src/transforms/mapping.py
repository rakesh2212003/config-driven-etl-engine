from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    TimestampType,
)

from src.core.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Type registry
# ---------------------------------------------------------------------------

SPARK_TYPE_MAP = {
    "STRING":    StringType(),
    "INT":       IntegerType(),
    "INTEGER":   IntegerType(),
    "LONG":      LongType(),
    "BIGINT":    LongType(),
    "SHORT":     ShortType(),
    "DOUBLE":    DoubleType(),
    "FLOAT":     FloatType(),
    "BOOLEAN":   BooleanType(),
    "BOOL":      BooleanType(),
    "DATE":      DateType(),
    "TIMESTAMP": TimestampType(),
}


def _resolve_type(type_str: str):
    spark_type = SPARK_TYPE_MAP.get(type_str.upper())
    if spark_type is None:
        raise ValueError(f"Unsupported type '{type_str}' in mapping config.")
    return spark_type


# ---------------------------------------------------------------------------
# Rule handlers
# ---------------------------------------------------------------------------

def _apply_trim(col: Any, _rule: dict) -> Any:
    return F.trim(col)

def _apply_upper(col: Any, _rule: dict) -> Any:
    return F.upper(col)

def _apply_lower(col: Any, _rule: dict) -> Any:
    return F.lower(col)

def _apply_coalesce(col: Any, rule: dict) -> Any:
    return F.coalesce(col, F.lit(rule.get("default")))

def _apply_regex_replace(col: Any, rule: dict) -> Any:
    return F.regexp_replace(col, rule["pattern"], rule.get("replacement", ""))

def _apply_truncate(col: Any, rule: dict) -> Any:
    return F.substring(col, 1, rule["max_length"])

def _apply_cast(col: Any, rule: dict, target_type: str) -> Any:
    spark_type = _resolve_type(target_type)
    fmt        = rule.get("format")
    on_error   = rule.get("on_error", "NULLIFY").upper()

    if target_type.upper() == "DATE" and fmt:
        casted = F.to_date(col, fmt)
    elif target_type.upper() == "TIMESTAMP" and fmt:
        casted = F.to_timestamp(col, fmt)
    else:
        casted = col.cast(spark_type)

    if on_error == "NULLIFY":
        return F.when(col.isNull(), F.lit(None).cast(spark_type)).otherwise(casted)
    return casted

def _apply_derived(_col: Any, rule: dict) -> Any:
    return F.expr(rule["expr"])


# ---------------------------------------------------------------------------
# Rule dispatcher
# ---------------------------------------------------------------------------

_RULE_HANDLERS = {
    "TRIM":          _apply_trim,
    "UPPER":         _apply_upper,
    "LOWER":         _apply_lower,
    "COALESCE":      _apply_coalesce,
    "REGEX_REPLACE": _apply_regex_replace,
    "TRUNCATE":      _apply_truncate,
}


def _apply_rules(col: Any, rules: list[dict], target_type: str) -> Any:
    try:
        for rule in rules:
            op = rule["op"].upper()
            if op == "CAST":
                col = _apply_cast(col, rule, target_type)
            elif op == "DERIVED":
                col = _apply_derived(col, rule)
            elif op in _RULE_HANDLERS:
                col = _RULE_HANDLERS[op](col, rule)
            else:
                raise ValueError(f"Unknown rule op '{op}'.")
        return col
    except Exception as e:
        logger.error(f"Failed applying rules for type '{target_type}': {e}")
        raise


# ---------------------------------------------------------------------------
# Nullable enforcement
# ---------------------------------------------------------------------------

def _enforce_nullable(col: Any, col_def: dict) -> Any:
    if not col_def.get("nullable", True):
        target = col_def["target"]
        return F.when(
            col.isNull(),
            F.raise_error(F.lit(f"NOT NULL violation on column '{target}'."))
        ).otherwise(col)
    return col


# ---------------------------------------------------------------------------
# Core transform
# ---------------------------------------------------------------------------

def apply_mapping(df: DataFrame, mapping_config: dict) -> DataFrame:
    """
    Apply mapping config rules to a PySpark DataFrame.

    Parameters
    ----------
    df             : source DataFrame
    mapping_config : loaded mapping config dict

    Returns
    -------
    Transformed DataFrame with target columns in seq order.
    """
    try:
        columns_cfg  = sorted(mapping_config["columns"], key=lambda c: c["seq"])
        select_exprs = []

        for col_def in columns_cfg:
            source      = col_def.get("source")
            target      = col_def["target"]
            target_type = col_def["type"]
            rules       = col_def.get("rules", [])

            try:
                if source is None:
                    derived_rule = next(
                        (r for r in rules if r["op"].upper() == "DERIVED"), None
                    )
                    if derived_rule is None:
                        raise ValueError(
                            f"Column '{target}' has no source and no DERIVED rule."
                        )
                    col = _apply_derived(None, derived_rule)
                else:
                    if source not in df.columns:
                        raise ValueError(
                            f"Source column '{source}' not found in DataFrame. "
                            f"Available: {df.columns}"
                        )
                    col = F.col(source)
                    col = _apply_rules(col, rules, target_type)

                col = col.cast(_resolve_type(target_type))
                col = _enforce_nullable(col, col_def)
                select_exprs.append(col.alias(target))

                logger.debug(f"Mapped '{source}' -> '{target}' [{target_type}]")

            except Exception as e:
                logger.error(
                    f"Failed mapping column seq={col_def.get('seq')} "
                    f"source='{source}' target='{target}': {e}"
                )
                raise

        logger.info(f"Mapping applied — {len(select_exprs)} column(s) mapped.")
        return df.select(*select_exprs)

    except Exception as e:
        logger.error(f"apply_mapping failed: {e}")
        raise