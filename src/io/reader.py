import time
from pyspark.sql import DataFrame, SparkSession

from src.core.logger import get_logger
from src.common.helpers import format_duration

logger = get_logger(__name__)

def read_raw(spark:SparkSession, path:str, fmt:str, fmt_options:dict=None, **options) -> DataFrame:
    try:
        logger.info(f"Reading RAW data from {path} [format={fmt}]")
        start = time.time()
        reader = spark.read.format(fmt)

        for key,value in (fmt_options or {}).items():
            reader = reader.option(key, value)
        
        for key,value in (options or {}).items():
            reader = reader.option(key, value)

        df = reader.load(path)
        logger.info(f"Read complete in {format_duration(time.time()-start)}")

        return df
    
    except Exception as e:
        logger.error(f"Failed to read data from {path}: {e}")
        raise