import os
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.core.logger import get_logger
from src.common.helpers import format_duration

logger = get_logger(__name__)


def _build_hudi_options(
    table_name: str,
    dis_path: str,
    mapping_config: dict,
) -> tuple[dict, str]:
    """
    Build full Hudi write options from mapping config.

    Returns
    -------
    tuple of (options dict, target path)
    """
    try:
        mapping_cfg      = mapping_config["mapping"]
        hudi_cfg         = mapping_config["hudi"]
        primary_keys     = ",".join(mapping_cfg["primary_key"])
        precombine_field = mapping_cfg["precombine_field"]
        table_type       = hudi_cfg["table_type"]
        target_path      = os.path.join(dis_path, table_name)

        options = {
            "hoodie.table.name":                        table_name,
            "hoodie.datasource.write.table.type":       table_type,
            "hoodie.datasource.write.recordkey.field":  primary_keys,
            "hoodie.datasource.write.precombine.field": precombine_field,
            "hoodie.datasource.write.operation":        mapping_cfg["write_mode"],
            "hoodie.datasource.write.table.name":       table_name,
            "hoodie.datasource.hive_sync.enable":       "false",
        }

        options.update(hudi_cfg.get("options", {}))

        # Compaction — only for MERGE_ON_READ
        compaction = hudi_cfg.get("compaction", {})
        if table_type == "MERGE_ON_READ" and compaction.get("enabled", False):
            options["hoodie.compact.inline"]                    = "true"
            options["hoodie.compact.inline.trigger.strategy"]  = compaction.get("trigger_strategy", "NUM_COMMITS")
            options["hoodie.compact.inline.max.delta.commits"] = str(compaction.get("trigger_max_commits", 5))

        return options, target_path

    except KeyError as e:
        logger.error(f"Missing key while building Hudi options: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to build Hudi options for '{table_name}': {e}")
        raise


def _close_existing_records(
    spark: SparkSession,
    incoming_df: DataFrame,
    target_path: str,
    mapping_config: dict,
) -> DataFrame | None:
    """
    SCD Type 2 — close existing current records that appear in the incoming batch.
    Sets effective_to = incoming.effective_from, is_current = false.

    Returns
    -------
    DataFrame of closed records, or None if table doesn't exist yet
    """
    try:
        scd_cfg          = mapping_config["scd"]
        primary_keys     = mapping_config["mapping"]["primary_key"]
        effective_from   = scd_cfg["effective_from_field"]
        effective_to     = scd_cfg["effective_to_field"]
        is_current       = scd_cfg["is_current_field"]
        precombine_field = mapping_config["mapping"]["precombine_field"]

        if not os.path.exists(target_path):
            logger.info(f"No existing Hudi table at '{target_path}'. Skipping SCD2 close.")
            return None

        try:
            existing_df = spark.read.format("hudi").load(target_path)
        except Exception as e:
            logger.warning(f"Could not read existing Hudi table at '{target_path}': {e}")
            return None

        join_condition = [existing_df[pk] == incoming_df[pk] for pk in primary_keys]

        closed_df = (
            existing_df.alias("existing")
            .join(incoming_df.alias("incoming"), join_condition, "inner")
            .filter(F.col(f"existing.{is_current}") == True)
            .select(
                *[F.col(f"existing.{c}") for c in existing_df.columns
                  if c not in [effective_to, is_current, precombine_field]],
                F.col(f"incoming.{effective_from}").alias(effective_to),
                F.lit(False).alias(is_current),
                F.col(f"incoming.{effective_from}").alias(precombine_field),
            )
        )

        count = closed_df.count()
        logger.info(f"SCD2: {count} existing record(s) will be closed.")
        return closed_df

    except Exception as e:
        logger.error(f"Failed during SCD2 close step: {e}")
        raise


def write_hudi(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    app_config: dict,
    mapping_config: dict,
) -> None:
    """
    Write a transformed DataFrame to Hudi with SCD Type 2 support.

    Parameters
    ----------
    spark          : active SparkSession
    df             : transformed DataFrame
    table_name     : target Hudi table name
    app_config     : loaded app config dict
    mapping_config : loaded mapping config dict
    """
    try:
        dis_path            = app_config["dis_layer"]["path"]
        options, target_path = _build_hudi_options(table_name, dis_path, mapping_config)
        scd_type            = mapping_config.get("scd", {}).get("type")

        start = time.time()
        logger.info(f"Writing to Hudi table '{table_name}' at '{target_path}'")

        if scd_type == 2:
            # Step 1 — close existing current records
            closed_df = _close_existing_records(spark, df, target_path, mapping_config)
            if closed_df is not None:
                logger.info("Writing closed SCD2 records.")
                (
                    closed_df.write
                    .format("hudi")
                    .options(**options)
                    .mode("append")
                    .save(target_path)
                )

        # Step 2 — write incoming records
        (
            df.write
            .format("hudi")
            .options(**options)
            .mode("append")
            .save(target_path)
        )

        logger.info(f"Write complete in {format_duration(time.time() - start)}")

    except Exception as e:
        logger.error(f"Failed to write to Hudi table '{table_name}': {e}")
        raise