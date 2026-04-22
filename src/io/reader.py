from src.core.logger import get_logger

logger = get_logger("Reader")


def read_data(spark, path: str, fmt: str = "csv", options: dict = None):
    try:
        logger.info(f"Reading data from: {path} | format: {fmt}")

        df = (
            spark.read
            .format(fmt)
            .options(**(options or {}))
            .load(path)
        )

        logger.info("Read successful")
        return df

    except Exception as e:
        logger.error(f"Failed to read data from {path}: {str(e)}", exc_info=True)
        raise