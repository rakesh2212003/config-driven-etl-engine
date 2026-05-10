from pyspark.sql import DataFrame, SparkSession
from src.core.logger import get_logger

logger = get_logger(__name__)

def read_dataframe(spark:SparkSession, path:str, fmt:str = "csv", options:dict|None = None) -> DataFrame:
    try:
        logger.info(
            f"Reading data from: {path} | format={fmt}"
        )
        reader = spark.read.format(fmt)
        if options:
            reader = reader.options(**options)
        dataframe = reader.load(path)
        logger.info(
            f"Read successful: {path}"
        )
        return dataframe

    except Exception:
        logger.exception(
            f"Failed to read data: {path}"
        )
        raise