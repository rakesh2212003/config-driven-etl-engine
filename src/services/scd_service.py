from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    lit,
    col
)

from src.core.logger import get_logger


logger = get_logger(__name__)


def apply_scd2(df:DataFrame) -> DataFrame:

    try:
        logger.info(
            "Applying SCD Type 2 logic"
        )

        df = (
            df
            .withColumn(
                "effective_to",
                lit(None).cast("timestamp")
            )
            .withColumn(
                "current_row",
                lit(1)
            )
        )

        logger.info(
            "SCD Type 2 columns added successfully"
        )

        return df

    except Exception as error:
        logger.exception(
            "Failed to apply SCD Type 2 logic"
        )
        raise error