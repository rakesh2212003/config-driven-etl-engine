from src.core.logger import setup_logger, get_logger
from src.core.validator import validate
from src.core.session import get_spark
from src.pipelines.registry import get_pipeline_class


def run(job: str, table_name: str) -> None:
    setup_logger()
    logger = get_logger(__name__)

    # ── Fail fast before touching any heavy resource ──
    validate(job, table_name)

    logger.info(f"Starting job='{job}' table='{table_name}'")

    spark = get_spark()

    pipeline_cls = get_pipeline_class(job)
    pipeline = pipeline_cls(spark, table_name)
    success = pipeline.run()

    if not success:
        raise RuntimeError(f"Job '{job}' failed for table: {table_name}")

    logger.info(f"Job='{job}' table='{table_name}' completed successfully")
