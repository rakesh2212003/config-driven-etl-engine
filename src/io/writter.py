from pyspark.sql import DataFrame
from src.core.logger import get_logger

logger = get_logger(__name__)

def write_dataframe(df:DataFrame, path:str, fmt:str = "parquet", mode:str = "overwrite", options:dict|None = None) -> None:

    try:
        logger.info(
            f"Writing data to: {path} "
            f"| format={fmt} "
            f"| mode={mode}"
        )

        writer = (
            df.write
            .format(fmt)
            .mode(mode)
        )

        if options:
            writer = writer.options(**options)

        writer.save(path)

        logger.info(
            f"Write successful: {path}"
        )

    except Exception:
        logger.exception(
            f"Failed to write data: {path}"
        )
        raise
