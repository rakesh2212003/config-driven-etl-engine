import os
import uuid
from datetime import datetime

from src.core.config import load_config, load_mapping, load_bookmark, save_bookmark
from src.core.spark import get_spark
from src.core.logger import get_logger
from src.io.reader import read_raw, get_pending_cdc_files
from src.io.writter import write_hudi
from src.transforms.mapping import apply_mapping
from src.common.helpers import format_duration, write_audit

logger = get_logger(__name__)


def run(table_name: str) -> None:
    """
    Main RAW -> DIS job for a given table.

    Flow
    ----
    1. Load configs and bookmark
    2. Get pending CDC files since last run
    3. For each file:
        a. Read raw CDC file
        b. Apply mapping transformations
        c. Write to Hudi (SCD2)
        d. Save bookmark (SUCCESS)
        e. On failure — save bookmark (FAILED), stop pipeline
    """
    run_id     = str(uuid.uuid4())
    started_at = datetime.now()

    logger.info("=" * 60)
    logger.info(f"Job     : RAW2DIS")
    logger.info(f"Table   : {table_name}")
    logger.info(f"Run ID  : {run_id}")
    logger.info(f"Started : {started_at:%Y-%m-%d %H:%M:%S}")
    logger.info("=" * 60)

    # ------------------------------------------------------------------
    # 1. Load configs
    # ------------------------------------------------------------------
    try:
        app_config     = load_config()
        mapping_config = load_mapping(table_name)
        bookmark       = load_bookmark(table_name)
    except Exception as e:
        logger.error(f"Failed to load configs for '{table_name}': {e}")
        raise

    # ------------------------------------------------------------------
    # 2. Init Spark
    # ------------------------------------------------------------------
    try:
        spark = get_spark(app_config)
    except Exception as e:
        logger.error(f"Failed to initialize Spark: {e}")
        raise

    # ------------------------------------------------------------------
    # 3. Get pending CDC files
    # ------------------------------------------------------------------
    try:
        pending_files = get_pending_cdc_files(
            table_name, app_config, mapping_config, bookmark
        )
    except Exception as e:
        logger.error(f"Failed to resolve pending CDC files: {e}")
        raise

    if not pending_files:
        logger.info(f"No new CDC files to process for '{table_name}'. Exiting.")
        return

    logger.info(f"Processing {len(pending_files)} file(s) for '{table_name}'.")
    fmt_options = app_config["raw_layer"]["options"]

    # ------------------------------------------------------------------
    # 4. Process each CDC file
    # ------------------------------------------------------------------
    for file_path in pending_files:
        file_name  = os.path.basename(file_path)
        file_start = datetime.now()

        logger.info(f"--- Processing: '{file_name}' ---")

        audit = {
            "run_id":        run_id,
            "table_name":    table_name,
            "cdc_file":      file_name,
            "rows_read":     0,
            "rows_inserted": 0,
            "rows_updated":  0,
            "rows_deleted":  0,
            "rows_rejected": 0,
            "started_at":    file_start.strftime("%Y-%m-%d %H:%M:%S"),
            "finished_at":   None,
            "status":        "FAILED",
        }

        try:
            # a. Read
            raw_df = read_raw(spark, file_path, fmt_options)
            audit["rows_read"] = raw_df.count()

            # b. Count by op for audit
            try:
                op_counts = raw_df.groupBy("op").count().collect()
                for row in op_counts:
                    if row["op"] == "I":
                        audit["rows_inserted"] = row["count"]
                    elif row["op"] == "U":
                        audit["rows_updated"] = row["count"]
                    elif row["op"] == "D":
                        audit["rows_deleted"] = row["count"]
            except Exception as e:
                logger.warning(f"Could not compute op counts for audit: {e}")

            # c. Apply mapping
            transformed_df = apply_mapping(raw_df, mapping_config)

            # d. Write to Hudi
            write_hudi(spark, transformed_df, table_name, app_config, mapping_config)

            # e. Save bookmark — SUCCESS
            save_bookmark(table_name, file_name, "SUCCESS")

            audit["status"]      = "SUCCESS"
            audit["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            write_audit(audit, app_config["audit"])

            logger.info(f"File '{file_name}' processed successfully.")

        except Exception as e:
            logger.error(f"Pipeline failed on file '{file_name}': {e}")

            # Save bookmark — FAILED (resume from this file on next run)
            try:
                save_bookmark(table_name, file_name, "FAILED")
            except Exception as be:
                logger.error(f"Additionally failed to save bookmark: {be}")

            audit["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            write_audit(audit, app_config["audit"])

            raise RuntimeError(
                f"Pipeline stopped on file '{file_name}' for table '{table_name}'. "
                f"Bookmark saved as FAILED. Fix the issue and rerun to resume."
            ) from e

    finished_at = datetime.now()
    logger.info("=" * 60)
    logger.info(f"Job complete for '{table_name}'.")
    logger.info(f"Finished : {finished_at:%Y-%m-%d %H:%M:%S}")
    logger.info(f"Duration : {format_duration((finished_at - started_at).total_seconds())}")
    logger.info("=" * 60)