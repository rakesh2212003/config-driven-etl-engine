from pyspark.sql import SparkSession
from src.core.config_loader import load_yaml
from src.core.logger import get_logger

logger = get_logger(__name__)
CONFIG = load_yaml("config/app.yaml")

def get_spark_session() -> SparkSession:
    try:
        spark_config = CONFIG["spark"]

        app_name = spark_config("app_name", "ETL")
        master = spark_config.get("master", "local[*]")
        extra_configs = spark_config.get("config", {})

        logger.info(
            f"Initializing Spark Session: "
            f"app_name={app_name}, master={master}"
        )

        builder = (
            SparkSession.builder
            .appName(app_name)
            .master(master)
        )

        for key, value in extra_configs.items():
            builder = builder.config(key, str(value))

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark Session created successfully")

        return spark

    except Exception:
        logger.exception("Failed to initialize Spark Session")
        raise
