"""
SCD Type-2 merge service.

Given:
  - incoming_df  : new/changed/deleted records (already mapped to target schema)
  - existing_df  : current snapshot in the distilled layer (or None on first run)
  - scd_config   : the 'scd' block from app.yaml for this table
  - op_col       : name of the operation column in incoming_df  (I / U / D)

Produces a full SCD-2 snapshot where:
  - current_row  = 1  for the active version
  - current_row  = 0  for all historical versions
  - effective_to = high_date for active rows, actual close date for history
  - is_deleted   = True/False

Operation codes accepted (case-insensitive):
  I  →  Insert
  U  →  Update   (closes existing active row, inserts new)
  D  →  Delete   (closes existing active row, marks is_deleted=True)
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.core.logger import get_logger

logger = get_logger("SCDService")


class SCDService:

    @staticmethod
    def apply(
        incoming_df: DataFrame,
        existing_df,          # DataFrame | None
        scd_config: dict,
        op_col: str = "op",
    ) -> DataFrame:
        """
        Returns the full updated SCD-2 snapshot.
        """
        try:
            pk_cols        = scd_config["primary_key"]
            eff_from_col   = scd_config["effective_from_col"]
            eff_to_col     = scd_config["effective_to_col"]
            current_col    = scd_config["current_row_col"]
            deleted_col    = scd_config["deleted_flag_col"]
            high_date      = scd_config.get("high_date", "9999-12-31 23:59:59")

            # ── Normalise op codes to uppercase, filter out rows with null PK ──
            incoming_df = (
                incoming_df
                .withColumn(op_col, F.upper(F.trim(F.col(op_col))))
                .filter(
                    F.col(pk_cols[0]).isNotNull() &
                    (F.col(eff_from_col).isNotNull())
                )
            )

            # ── Deduplicate incoming: keep latest version per PK per op batch ──
            # If two rows share the same PK in the same file (e.g. I then U for
            # same customer_id), keep only the last one by effective_from.
            w_incoming = Window.partitionBy(*pk_cols).orderBy(F.col(eff_from_col).desc())
            incoming_dedup = (
                incoming_df
                .withColumn("_rn", F.row_number().over(w_incoming))
                .filter(F.col("_rn") == 1)
                .drop("_rn")
            )

            # ── First-run path: no existing snapshot ──────────────────────────
            if existing_df is None:
                logger.info("No existing snapshot — bootstrapping SCD from incoming data")
                return SCDService._bootstrap(
                    incoming_dedup, pk_cols, eff_from_col,
                    eff_to_col, current_col, deleted_col, high_date, op_col
                )

            # ── Incremental path ──────────────────────────────────────────────
            logger.info("Merging incoming records into existing SCD snapshot")

            # Keys that appear in the new batch
            changed_keys = incoming_dedup.select(*pk_cols)

            # Split existing into rows that ARE affected and those that aren't
            join_cond = [existing_df[k] == changed_keys[k] for k in pk_cols]
            affected_existing = existing_df.join(changed_keys, pk_cols, "inner")
            unaffected_existing = existing_df.join(changed_keys, pk_cols, "left_anti")

            # Close the currently-active affected rows
            # (set effective_to = incoming effective_from, current_row = 0)
            closed_rows = SCDService._close_active_rows(
                affected_existing, incoming_dedup,
                pk_cols, eff_from_col, eff_to_col, current_col
            )

            # Build new active rows for I and U operations
            new_active_rows = (
                incoming_dedup
                .filter(F.col(op_col).isin("I", "U"))
                .withColumn(eff_to_col, F.to_timestamp(F.lit(high_date)))
                .withColumn(current_col,  F.lit(1))
                .withColumn(deleted_col,  F.lit(False))
                .drop(op_col)
            )

            # For D operations: add a closed/deleted row (current_row=0, is_deleted=True)
            deleted_rows = (
                incoming_dedup
                .filter(F.col(op_col) == "D")
                .withColumn(eff_to_col,  F.col(eff_from_col))  # closes immediately
                .withColumn(current_col,  F.lit(0))
                .withColumn(deleted_col,  F.lit(True))
                .drop(op_col)
            )

            # Union everything
            result = (
                unaffected_existing
                .unionByName(closed_rows)
                .unionByName(new_active_rows)
                .unionByName(deleted_rows)
            )

            logger.info("SCD merge completed successfully")
            return result

        except Exception as e:
            logger.error(f"SCD apply failed: {str(e)}", exc_info=True)
            raise

    # ─────────────────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _bootstrap(df, pk_cols, eff_from_col, eff_to_col,
                   current_col, deleted_col, high_date, op_col):
        """Convert a first-load batch into an SCD-2 snapshot."""

        inserts_updates = (
            df.filter(F.col(op_col).isin("I", "U"))
            .withColumn(eff_to_col,  F.to_timestamp(F.lit(high_date)))
            .withColumn(current_col,  F.lit(1))
            .withColumn(deleted_col,  F.lit(False))
            .drop(op_col)
        )

        deletes = (
            df.filter(F.col(op_col) == "D")
            .withColumn(eff_to_col,  F.col(eff_from_col))
            .withColumn(current_col,  F.lit(0))
            .withColumn(deleted_col,  F.lit(True))
            .drop(op_col)
        )

        return inserts_updates.unionByName(deletes)

    @staticmethod
    def _close_active_rows(affected_existing, incoming_dedup,
                           pk_cols, eff_from_col, eff_to_col, current_col):
        """
        For each affected existing row that is currently active (current_row=1),
        set effective_to = incoming effective_from, current_row = 0.
        Historical rows (current_row=0) are passed through unchanged.
        """
        # Alias to avoid column name collisions in join
        inc = incoming_dedup.select(
            *pk_cols,
            F.col(eff_from_col).alias("_new_eff_from")
        )

        joined = affected_existing.join(inc, pk_cols, "left")

        closed = (
            joined
            .withColumn(
                eff_to_col,
                F.when(
                    F.col(current_col) == 1,
                    F.col("_new_eff_from")
                ).otherwise(F.col(eff_to_col))
            )
            .withColumn(
                current_col,
                F.lit(0)
            )
            .drop("_new_eff_from")
        )

        return closed
