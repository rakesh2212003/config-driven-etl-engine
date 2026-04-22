from pyspark.sql import SparkSession
from src.core.config import ConfigLoader
from src.core.logger import get_logger

logger = get_logger("SparkSession")


def get_spark():
    try:
        config = ConfigLoader.load_yaml("config/app_config.yaml")

        app_name = config.get("app_name", "ETL Engine")
        spark_configs = config.get("spark", {})

        logger.info(f"Initializing Spark Session: {app_name}")

        builder = SparkSession.builder.appName(app_name)

        for key, value in spark_configs.items():
            builder = builder.config(key, value)

        spark = builder.getOrCreate()

        logger.info("Spark Session created successfully")
        return spark

    except Exception as e:
        logger.error(f"Failed to initialize Spark Session: {str(e)}", exc_info=True)
        raise