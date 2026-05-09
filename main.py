import argparse
from src.job import run

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--job",   required=True, help="Job type to run (e.g. scd)")
    parser.add_argument("--table", required=True, help="Table name to process")
    args = parser.parse_args()
    run(args.job, args.table)
