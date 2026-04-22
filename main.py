import argparse

from src.core.session import get_spark
from src.core.logger import setup_logger, get_logger
from src.pipelines.customer_pipeline import CustomerPipeline


# 🔹 Initialize logging once
setup_logger()
logger = get_logger("Main")


# 🔹 Pipeline registry
PIPELINE_REGISTRY = {
    "customer": CustomerPipeline,
}


def main():
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("--table", required=True, help="Pipeline to run")

        args = parser.parse_args()
        job_name = args.table

        logger.info(f"Starting job: {job_name}")

        if job_name not in PIPELINE_REGISTRY:
            raise ValueError(f"Unknown job: {job_name}")

        spark = get_spark()

        pipeline_class = PIPELINE_REGISTRY[job_name]
        pipeline = pipeline_class(spark)

        success = pipeline.run()

        if not success:
            raise RuntimeError("Pipeline execution failed")

        logger.info("Job completed successfully")

    except Exception as e:
        logger.error(f"Job failed: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()