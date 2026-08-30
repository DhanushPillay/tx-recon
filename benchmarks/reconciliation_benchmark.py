import time
import logging
import sys
import os
import glob

# Add parent directory to path so we can import src modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import get_spark_session
from src.generators.settlement_generator import generate_settlement_file

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def run_benchmark():
    num_records = 1000000
    output_dir = "benchmarks/data"

    # Check if a benchmark settlement file already exists from the pandas test
    files = glob.glob(f"{output_dir}/settlement_*.csv")
    if not files:
        logger.info(f"No benchmark CSV found. Generating {num_records} records...")
        generate_settlement_file(num_records, output_dir=output_dir)
    else:
        logger.info(f"Reusing existing benchmark CSVs in {output_dir}/")

    logger.info("Initializing Spark Session for Reconciliation Benchmark")
    spark = get_spark_session("ReconciliationBenchmark")

    logger.info(f"Reading bank settlement CSVs from {output_dir}/*.csv")
    bank_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{output_dir}/*.csv")
    )

    bank_df.createOrReplaceTempView("bank_settlements")

    merge_sql = """
    MERGE INTO nessie.db.webhooks t
    USING bank_settlements s
    ON t.transaction_id = s.transaction_id
    WHEN MATCHED AND (t.amount_paise * 0.985) = s.settled_amount_paise THEN
        UPDATE SET 
            t.reconciliation_status = 'MATCHED',
            t.bank_ref_id = s.bank_ref_id
    WHEN MATCHED AND (t.amount_paise * 0.985) != s.settled_amount_paise THEN
        UPDATE SET 
            t.reconciliation_status = 'EXCEPTION_FEE_MISMATCH',
            t.bank_ref_id = s.bank_ref_id
    """

    logger.info("Starting Iceberg MERGE INTO operation (Reconciliation)...")

    start_time = time.time()

    # Execute the MERGE
    spark.sql(merge_sql)

    end_time = time.time()
    duration = end_time - start_time

    # Count how many were matched
    matched_df = spark.sql(
        "SELECT COUNT(*) as cnt FROM nessie.db.webhooks WHERE reconciliation_status = 'MATCHED'"
    )
    matched_count = matched_df.collect()[0]["cnt"]

    logger.info(f"BENCHMARK COMPLETE")
    logger.info(f"Time taken to execute MERGE INTO on Iceberg: {duration:.2f} seconds")
    logger.info(f"Total rows successfully MATCHED: {matched_count}")


if __name__ == "__main__":
    run_benchmark()
