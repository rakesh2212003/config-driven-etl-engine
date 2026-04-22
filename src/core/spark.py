from pyspark.sql import SparkSession
from src.core.logger import get_logger

logger = get_logger(__name__)


def get_spark(config: dict) -> SparkSession:
    """
    Build and return a SparkSession from app config.

    Parameters
    ----------
    config : loaded app config dict

    Returns
    -------
    SparkSession
    """
    try:
        spark_cfg = config.get("spark", {})
        app_name  = spark_cfg.get("app_name", "RAW2DIS")
        master    = spark_cfg.get("master", "local[*]")
        log_level = spark_cfg.get("log_level", "WARN")

        logger.info(f"Initializing SparkSession [app={app_name}, master={master}]")

        builder = SparkSession.builder \
            .appName(app_name) \
            .master(master)

        for key, value in spark_cfg.get("config", {}).items():
            builder = builder.config(key, str(value))

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(log_level)

        logger.info("SparkSession initialized successfully.")
        return spark

    except Exception as e:
        logger.error(f"Failed to initialize SparkSession: {e}")
        raise