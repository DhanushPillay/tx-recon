import glob
import logging
import os
import sys
import time

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.generators.settlement_generator import generate_settlement_file

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def run_benchmark():
    num_records = 1000000
    output_dir = "benchmarks/data"

    logger.info(f"Generating {num_records} settlement records...")
    start_gen = time.time()
    generate_settlement_file(num_records, output_dir=output_dir)
    end_gen = time.time()
    logger.info(f"Generated in {end_gen - start_gen:.2f}s")

    files = glob.glob(f"{output_dir}/settlement_*.csv")
    latest_file = max(files, key=os.path.getctime)

    # Warmup: read once to warm OS page cache
    pd.read_csv(latest_file)

    # Measure
    start_time = time.time()
    df = pd.read_csv(latest_file)
    read_time = time.time() - start_time

    start_time = time.time()
    errors = []
    if not df["transaction_id"].is_unique:
        errors.append("transaction_id is not unique")
    invalid_amounts = df[~df["settled_amount_paise"].between(1, 9999999999)]
    if not invalid_amounts.empty:
        errors.append(f"{len(invalid_amounts)} invalid amounts")
    null_refs = df[df["bank_ref_id"].isnull()]
    if not null_refs.empty:
        errors.append(f"{len(null_refs)} null refs")
    validate_time = time.time() - start_time

    logger.info(
        f"CSV read:     {read_time:.3f}s ({num_records/read_time:,.0f} rows/sec)"
    )
    logger.info(f"Validation:   {validate_time:.3f}s")
    logger.info(f"Total:        {read_time + validate_time:.3f}s")
    if errors:
        for e in errors:
            logger.error(f"  {e}")


if __name__ == "__main__":
    run_benchmark()
