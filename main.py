import argparse

from src.core.session import get_spark
from src.core.logger import setup_logger, get_logger
from src.pipelines.generic_pipeline import GenericPipeline


# 🔹 Setup logging once
setup_logger()
logger = get_logger(__name__)


def main():
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("--table", required=True, help="Table name to process")

        args = parser.parse_args()
        table_name = args.table

        logger.info(f"Starting job for table: {table_name}")

        # 🔹 Create Spark session
        spark = get_spark()

        # 🔹 Run generic pipeline
        pipeline = GenericPipeline(spark, table_name)
        success = pipeline.run()

        if not success:
            raise RuntimeError("Pipeline execution failed")

        logger.info(f"Job completed successfully for table: {table_name}")

    except Exception as e:
        logger.error(f"Job failed: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()