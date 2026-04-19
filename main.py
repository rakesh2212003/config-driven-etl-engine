import argparse
from src.jobs import get_job

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pyspark ETL Pipeline")
    parser.add_argument("--job", required=True, help="Job name to execute")
    parser.add_argument("--table", required=True, help="Table name to process")
    args = parser.parse_args()

    job = get_job(args.job)
    job(args.table)