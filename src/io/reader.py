import os
import glob
import time

from pyspark.sql import DataFrame, SparkSession

from src.core.logger import get_logger
from src.common.helpers import format_duration

logger = get_logger(__name__)


def _detect_format(file_path: str) -> str:
    """
    Detect file format from extension.

    Parameters
    ----------
    file_path : path to the file

    Returns
    -------
    str : 'csv' | 'parquet' | 'json'
    """
    ext = os.path.splitext(file_path)[-1].lower()
    fmt_map = {
        ".csv":     "csv",
        ".parquet": "parquet",
        ".json":    "json",
    }
    fmt = fmt_map.get(ext)
    if fmt is None:
        raise ValueError(f"Unsupported file format '{ext}' for file '{file_path}'.")
    return fmt


def _get_pending_files(
    raw_path: str,
    file_pattern: str,
    last_processed_file: str,
) -> list[str]:
    """
    Scan raw directory for CDC files newer than the last processed file.
    Sorted oldest to newest by filename (timestamp embedded in name).

    Parameters
    ----------
    raw_path            : path to raw table directory
    file_pattern        : glob pattern (e.g. cdc_*.csv)
    last_processed_file : filename of last successfully processed file, or None

    Returns
    -------
    list of full file paths sorted oldest -> newest
    """
    try:
        all_files = sorted(glob.glob(os.path.join(raw_path, file_pattern)))

        if not all_files:
            logger.warning(f"No files matching '{file_pattern}' found in '{raw_path}'.")
            return []

        if last_processed_file is None:
            logger.info(f"No bookmark found. Processing all {len(all_files)} file(s).")
            return all_files

        last_file_path = os.path.join(raw_path, last_processed_file)

        if last_file_path not in all_files:
            logger.warning(
                f"Last processed file '{last_processed_file}' not found in directory. "
                f"Processing all files."
            )
            return all_files

        pending = [f for f in all_files if f > last_file_path]
        logger.info(f"Found {len(pending)} pending file(s) after '{last_processed_file}'.")
        return pending

    except Exception as e:
        logger.error(f"Failed to scan pending files in '{raw_path}': {e}")
        raise


def read_raw(
    spark: SparkSession,
    file_path: str,
    fmt_options: dict = None,
) -> DataFrame:
    """
    Read a single raw CDC file into a DataFrame.
    Format is auto-detected from the file extension.

    Parameters
    ----------
    spark       : active SparkSession
    file_path   : full path to the CDC file
    fmt_options : format options dict from app config (raw_layer.options)

    Returns
    -------
    DataFrame
    """
    try:
        fmt     = _detect_format(file_path)
        options = (fmt_options or {}).get(fmt, {})

        logger.info(f"Reading '{os.path.basename(file_path)}' [format={fmt}]")
        start = time.time()

        reader = spark.read.format(fmt)
        for key, value in options.items():
            reader = reader.option(key, value)

        df = reader.load(file_path)
        row_count = df.count()

        logger.info(
            f"Read complete — {row_count} rows in {format_duration(time.time() - start)}"
        )
        return df

    except ValueError as e:
        logger.error(str(e))
        raise
    except Exception as e:
        logger.error(f"Failed to read '{file_path}': {e}")
        raise


def get_pending_cdc_files(
    table_name: str,
    app_config: dict,
    mapping_config: dict,
    bookmark: dict,
) -> list[str]:
    """
    Return list of unprocessed CDC files for a table, sorted oldest to newest.

    Parameters
    ----------
    table_name     : table name (e.g. 'customers')
    app_config     : loaded app config dict
    mapping_config : loaded mapping config dict
    bookmark       : loaded bookmark dict for this table

    Returns
    -------
    list of full file paths
    """
    try:
        raw_base     = app_config["raw_layer"]["path"]
        raw_path     = os.path.join(raw_base, table_name)
        file_pattern = mapping_config["bookmark"]["file_pattern"]

        last_processed_file = bookmark.get("last_processed_file")

        return _get_pending_files(raw_path, file_pattern, last_processed_file)

    except KeyError as e:
        logger.error(f"Missing config key while resolving CDC files: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to resolve pending CDC files for '{table_name}': {e}")
        raise