from pyspark.sql import SparkSession
from src.core.config import ConfigLoader
from src.core.logger import get_logger

logger = get_logger("SparkSession")


def get_spark() -> SparkSession:
    """
    Build a SparkSession from config/app.yaml.

    YAML structure expected:
        spark:
          app_name: "..."
          master:   "local[*]"
          config:
            spark.driver.memory: "4g"
            ...
    """
    try:
        config = ConfigLoader.load_yaml("config/app.yaml")

        spark_block  = config.get("spark", {})
        app_name     = spark_block.get("app_name", "ETL Engine")
        master       = spark_block.get("master", "local[*]")
        spark_configs = spark_block.get("config", {})

        logger.info(f"Initializing Spark Session: {app_name}  master={master}")

        builder = (
            SparkSession.builder
            .appName(app_name)
            .master(master)
        )

        for key, value in spark_configs.items():
            builder = builder.config(key, str(value))

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        logger.info("Spark Session created successfully")
        return spark

    except Exception as e:
        logger.error(f"Failed to initialize Spark Session: {str(e)}", exc_info=True)
        raise
