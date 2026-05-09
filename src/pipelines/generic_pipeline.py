"""
GenericPipeline — config-driven SCD-2 ETL pipeline.

Reads table config from config/app.yaml → tables.<table_name>
Reads column mappings from the JSON file declared in mapping_path
Applies SCD Type-2 logic when scd.enabled = true
Tracks incremental loads via the bookmark file
"""

import os
from pyspark.sql import functions as F

from src.pipelines.base_pipeline import BasePipeline
from src.io.reader import read_data
from src.io.writer import write_data
from src.services.mapping_service import MappingService
from src.services.scd_service import SCDService
from src.core.config import ConfigLoader
from src.core.logger import get_logger
from src.core.bookmark import load_bookmark, update_bookmark

logger = get_logger(__name__)


class GenericPipeline(BasePipeline):

    def __init__(self, spark, table_name: str):
        super().__init__()
        self.spark = spark
        self.table_name = table_name

        app_config = ConfigLoader.load_yaml("config/app.yaml")

        tables = app_config.get("tables", {})
        if table_name not in tables:
            raise ValueError(
                f"Table '{table_name}' not found in config/app.yaml. "
                f"Available: {list(tables.keys())}"
            )

        table_cfg = tables[table_name]

        self.input_path    = table_cfg["input_path"]
        self.output_path   = table_cfg["output_path"]
        self.mapping_path  = table_cfg["mapping_path"]

        fmt_block          = table_cfg.get("format", {})
        self.input_format  = fmt_block.get("input",  "csv")
        self.output_format = fmt_block.get("output", "parquet")

        # CDC (incremental bookmark) settings
        cdc_cfg            = table_cfg.get("cdc", {})
        self.cdc_type      = cdc_cfg.get("type", "full")          # full | incremental
        self.cdc_column    = cdc_cfg.get("column")                 # e.g. "updated_timestamp"

        # SCD settings
        self.scd_config    = table_cfg.get("scd", {})
        self.scd_enabled   = self.scd_config.get("enabled", False)

        # Raw-layer read options from app.yaml
        raw_opts           = app_config.get("raw_layer", {}).get("options", {})
        self.read_options  = raw_opts.get(self.input_format, {})

    # ──────────────────────────────────────────────────────────────────────────
    # Extract
    # ──────────────────────────────────────────────────────────────────────────
    def extract(self):
        df = read_data(
            self.spark,
            self.input_path,
            fmt=self.input_format,
            options=self.read_options,
        )

        # Apply incremental CDC filter using bookmark timestamp
        if self.cdc_type == "incremental" and self.cdc_column:
            bookmark = load_bookmark(self.table_name)
            last_ts  = bookmark.get("bookmark_timestamp")

            if last_ts:
                logger.info(f"CDC filter: {self.cdc_column} > '{last_ts}'")
                df = df.filter(
                    F.col(self.cdc_column).cast("timestamp") > F.lit(last_ts).cast("timestamp")
                )
            else:
                logger.info("No prior bookmark found — full load on first run")

        logger.info(f"Extracted {df.count()} rows from {self.input_path}")
        return df

    # ──────────────────────────────────────────────────────────────────────────
    # Transform  (rename + cast + SCD columns)
    # ──────────────────────────────────────────────────────────────────────────
    def transform(self, df):
        mapping_config = ConfigLoader.load_json(self.mapping_path)
        df = MappingService.apply(df, mapping_config)
        logger.info(f"Post-mapping columns: {df.columns}")
        return df

    # ──────────────────────────────────────────────────────────────────────────
    # Load  (SCD merge → write → update bookmark)
    # ──────────────────────────────────────────────────────────────────────────
    def load(self, df):
        # Determine operation column name (target_field of the "op" mapping)
        mapping_config = ConfigLoader.load_json(self.mapping_path)
        op_col = self._resolve_op_col(mapping_config)

        # Load existing snapshot (if any) for SCD merge
        existing_df = self._load_existing_snapshot()

        if self.scd_enabled:
            logger.info("Running SCD Type-2 merge")
            final_df = SCDService.apply(
                incoming_df  = df,
                existing_df  = existing_df,
                scd_config   = self.scd_config,
                op_col       = op_col,
            )
        else:
            # No SCD — plain overwrite/append
            logger.info("SCD disabled — writing directly")
            final_df = df

        write_data(final_df, self.output_path, fmt=self.output_format, mode="overwrite")

        # Update bookmark to the max effective_from seen in incoming batch
        if self.cdc_type == "incremental" and self.cdc_column:
            eff_from_col = self.scd_config.get("effective_from_col", "effective_from")
            if eff_from_col in df.columns:
                max_ts = df.agg(F.max(eff_from_col)).collect()[0][0]
                if max_ts is not None:
                    update_bookmark(self.table_name, str(max_ts))

        return True

    # ──────────────────────────────────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────────────────────────────────
    def _load_existing_snapshot(self):
        """
        Return existing snapshot fully materialised into memory, or None on first run.

        WHY checkpoint/cache here:
        Spark DataFrames are lazy — the query plan holds live references to the
        parquet files on disk. When we later write the merged result back to the
        SAME output directory with mode=overwrite, Spark deletes those files
        BEFORE it finishes reading them, causing:
            FAILED_READ_FILE.FILE_NOT_EXIST

        Calling .cache() + an eager .count() forces Spark to read all the data
        into the in-memory block store NOW, so the query plan no longer depends
        on the physical files. The subsequent overwrite is then safe.
        """
        try:
            path = ConfigLoader.BASE_PATH / self.output_path

            # Check for actual parquet data files (ignore _SUCCESS / metadata)
            has_data = path.exists() and any(
                f for f in path.iterdir()
                if f.suffix in (".parquet",) or f.name.endswith(".snappy.parquet")
            )

            if not has_data:
                logger.info("No existing snapshot — first run detected")
                return None

            df = self.spark.read.format(self.output_format).load(str(path))

            # ⚠️  Materialise into memory to avoid read-write conflict on overwrite
            df = df.cache()
            row_count = df.count()   # triggers the actual read and caching
            logger.info(f"Loaded and cached {row_count} rows from existing snapshot")
            return df

        except Exception as e:
            logger.warning(f"Could not load existing snapshot ({e}) — treating as first run")
            return None

    @staticmethod
    def _resolve_op_col(mapping_config: dict) -> str:
        """Find the target_field for the operation/op column."""
        for col_def in mapping_config.get("columns", []):
            src = col_def.get("source_field", "").lower()
            tgt = col_def.get("target_field", "").lower()
            if src in ("operation", "op") or tgt in ("operation", "op"):
                return col_def["target_field"]
        return "op"   # fallback
