import time
import logging
import pandas as pd
import os
import glob
import sys

# Add parent directory to path so we can import src modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.generators.settlement_generator import generate_settlement_file

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def run_benchmark():
    num_records = 1000000
    output_dir = "benchmarks/data"

    logger.info(f"Generating {num_records} settlement records for Pandas benchmark...")
    start_gen = time.time()
    generate_settlement_file(num_records, output_dir=output_dir)
    end_gen = time.time()
    logger.info(
        f"Generated {num_records} records in {end_gen - start_gen:.2f} seconds."
    )

    # Find the file we just generated
    files = glob.glob(f"{output_dir}/settlement_*.csv")
    latest_file = max(files, key=os.path.getctime)

    logger.info(f"Starting Pandas Validation Benchmark on {latest_file}...")

    start_time = time.time()

    # EXACT LOGIC FROM src/validate_settlement.py
    df = pd.read_csv(latest_file)
    errors = []

    if not df["transaction_id"].is_unique:
        errors.append("transaction_id is not unique (contains duplicates).")

    invalid_amounts = df[~df["settled_amount_paise"].between(1, 9999999999)]
    if not invalid_amounts.empty:
        errors.append(
            f"settled_amount_paise contains {len(invalid_amounts)} invalid values."
        )

    null_refs = df[df["bank_ref_id"].isnull()]
    if not null_refs.empty:
        errors.append(f"bank_ref_id contains {len(null_refs)} null values.")

    end_time = time.time()
    duration = end_time - start_time

    if not errors:
        logger.info(f"BENCHMARK COMPLETE")
        logger.info(
            f"Time taken to load and validate {num_records} rows in memory with Pandas: {duration:.2f} seconds"
        )
    else:
        logger.error("Validation failed during benchmark:")
        for err in errors:
            logger.error(f" - {err}")


if __name__ == "__main__":
    run_benchmark()
