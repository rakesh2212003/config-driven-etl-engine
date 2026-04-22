import argparse
from src.core.logger import get_logger
from src.jobs import get_job

logger = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="RAW2DIS PySpark ETL Pipeline",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--job",
        required=True,
        help="Job name to execute (e.g. raw2dis)",
    )
    parser.add_argument(
        "--table",
        required=True,
        help="Table name to process (e.g. customers)",
    )

    args = parser.parse_args()
    logger.info(f"Launching job='{args.job}' table='{args.table}'")

    try:
        job = get_job(args.job)
        job(args.table)
    except Exception as e:
        logger.error(f"Job '{args.job}' failed for table '{args.table}': {e}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()