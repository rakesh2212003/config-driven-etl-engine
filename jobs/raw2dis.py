import argparse
from src.core.logger import setup_logger, get_logger
from src.pipelines.raw2dis_pipeline import GenericPipeline


setup_logger()
logger = get_logger("jobs.raw2dis")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Raw to Distilled Pipeline Job")
    parser.add_argument("--table", required=True, help="Table name to process")
    return parser.parse_args()

def main() -> None:
    args = parse_arguments()
    table_name = args.table
    try:
        logger.info(f"---------- Starting raw2dis job for table: '{table_name}' ----------")
        pipeline = GenericPipeline()
        pipeline.run(table_name)
        logger.info(f"---------- raw2dis job completed successfully for table: '{table_name}' ----------")
    except Exception:
        logger.exception(f"---------- raw2dis job failed for table: '{table_name}' ----------")
        raise

if __name__ == "__main__":
    main()
