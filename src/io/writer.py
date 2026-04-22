from src.core.logger import get_logger

logger = get_logger("Writer")


def write_data(df, path: str, fmt: str = "parquet", mode: str = "overwrite"):
    try:
        logger.info(f"Writing data to: {path} | format: {fmt} | mode: {mode}")

        (
            df.write
            .format(fmt)
            .mode(mode)
            .save(path)
        )

        logger.info("Write successful")
        return True

    except Exception as e:
        logger.error(f"Failed to write data to {path}: {str(e)}", exc_info=True)
        raise