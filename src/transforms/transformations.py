from pyspark.sql import DataFrame
from pyspark.sql.functions import (col, trim, upper, lower, initcap)
from src.core.logger import get_logger


logger = get_logger(__name__)


def apply_transformations(df:DataFrame, col_name:str, transformations: list[str]) -> DataFrame:
    try:
        for transformation in transformations:

            if transformation == "trim":
                df = df.withColumn(
                    col_name,
                    trim(col(col_name))
                )

            elif transformation == "upper":
                df = df.withColumn(
                    col_name,
                    upper(col(col_name))
                )

            elif transformation == "lower":
                df = df.withColumn(
                    col_name,
                    lower(col(col_name))
                )

            elif transformation == "capitalize":
                df = df.withColumn(
                    col_name,
                    initcap(col(col_name))
                )

            else:
                logger.warning(
                    f"Unsupported transformation: "
                    f"{transformation}"
                )

        logger.info(
            f"Transformations applied on: {col_name}"
        )

        return df

    except Exception as error:
        logger.exception(
            f"Failed transformations on: {col_name}"
        )
        raise error
    