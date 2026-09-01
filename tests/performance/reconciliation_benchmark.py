import argparse
import json
import logging
import os
import random
import sys
import time
import uuid

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from hardware import get_hardware_info  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.common.config import get_spark_session  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

MERGE_SQL = """
MERGE INTO {table} t
USING bank_settlements s
ON t.transaction_id = s.transaction_id
WHEN MATCHED AND (t.amount_paise - ((t.amount_paise * 15) DIV 1000)) = s.settled_amount_paise THEN
    UPDATE SET
        t.reconciliation_status = 'MATCHED',
        t.bank_ref_id = s.bank_ref_id
WHEN MATCHED AND (t.amount_paise - ((t.amount_paise * 15) DIV 1000)) != s.settled_amount_paise THEN
    UPDATE SET
        t.reconciliation_status = 'EXCEPTION_FEE_MISMATCH',
        t.bank_ref_id = s.bank_ref_id
"""

SCALE_OPTIONS = [100_000, 500_000, 1_000_000, 2_000_000]


def create_table(spark, table_name, num_rows):
    logger.info(f"Creating {table_name} with {num_rows:,} rows...")

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(
        f"""
        CREATE TABLE {table_name} (
            transaction_id string,
            amount_paise int,
            gateway_status string,
            timestamp_utc string,
            merchant_id string,
            reconciliation_status string,
            bank_ref_id string,
            ingested_at timestamp
        ) USING iceberg
        TBLPROPERTIES (
            'write.target-file-size-bytes' = '268435456',
            'write.parquet.compression-codec' = 'zstd',
            'write.distribution-mode' = 'hash'
        )
    """
    )

    batch_size = 50000
    schema = StructType(
        [
            StructField("transaction_id", StringType()),
            StructField("amount_paise", IntegerType()),
            StructField("gateway_status", StringType()),
            StructField("timestamp_utc", StringType()),
            StructField("merchant_id", StringType()),
            StructField("reconciliation_status", StringType()),
            StructField("bank_ref_id", StringType()),
            StructField("ingested_at", TimestampType()),
        ]
    )
    for offset in range(0, num_rows, batch_size):
        current_batch = min(batch_size, num_rows - offset)
        data = [
            (
                f"tx_{uuid.uuid4().hex[:12]}",
                random.randint(1000, 1000000),
                "SUCCESS",
                "2024-01-15T10:00:00Z",
                "merch_12345",
                "PENDING_SETTLEMENT",
                None,
                None,
            )
            for _ in range(current_batch)
        ]
        df = spark.createDataFrame(data, schema)
        df.repartition(4).writeTo(table_name).append()

    count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
    logger.info(f"Table {table_name} created with {count:,} rows")
    return count


def create_settlement_data(spark, table_name, update_fraction):
    count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
    settlement_count = int(count * update_fraction)
    logger.info(
        f"Creating settlement data: {settlement_count:,} rows ({update_fraction*100:.0f}% of {count:,})"
    )

    ids_df = spark.sql(
        f"SELECT transaction_id, amount_paise FROM {table_name} LIMIT {settlement_count}"
    )

    from pyspark.sql.functions import col, lit

    settlement_df = (
        ids_df.withColumn(
            "settled_amount_paise",
            (col("amount_paise") - ((col("amount_paise") * lit(15)) / lit(1000))).cast("int"),
        )
        .withColumn("bank_ref_id", col("transaction_id"))
        .withColumn("settlement_date", lit("2024-01-16"))
        .select("bank_ref_id", "transaction_id", "settled_amount_paise", "settlement_date")
    )

    settlement_df.createOrReplaceTempView("bank_settlements")
    return settlement_count


def measure_merge(spark, table_name, update_fraction):
    create_settlement_data(spark, table_name, update_fraction)

    files_before = spark.sql(f"SELECT COUNT(*) FROM {table_name}.files").collect()[0][0]

    start = time.time()
    spark.sql(MERGE_SQL.format(table=table_name))
    write_time = time.time() - start

    files_after = spark.sql(f"SELECT COUNT(*) FROM {table_name}.files").collect()[0][0]

    matched = spark.sql(
        f"SELECT COUNT(*) FROM {table_name} WHERE reconciliation_status = 'MATCHED'"
    ).collect()[0][0]
    mismatched = spark.sql(
        f"SELECT COUNT(*) FROM {table_name} WHERE reconciliation_status = 'EXCEPTION_FEE_MISMATCH'"
    ).collect()[0][0]

    start = time.time()
    spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()
    read_time = time.time() - start

    return {
        "write_time_sec": round(write_time, 2),
        "read_time_sec": round(read_time, 2),
        "files_before": files_before,
        "files_after": files_after,
        "matched": matched,
        "mismatched": mismatched,
    }


def run_benchmark(scale=None):
    hw = get_hardware_info()
    logger.info(
        f"Hardware: {hw['platform']}, {hw['cpu_count']} cores, Python {hw['python_version']}"
    )

    spark = get_spark_session("ReconciliationBenchmark")

    # Adaptive query execution
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "400")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.db")

    row_counts = [scale] if scale else SCALE_OPTIONS
    results = {}

    for num_rows in row_counts:
        table_name = f"nessie.db.webhooks_bench_{num_rows // 1000}k"
        create_table(spark, table_name, num_rows)

        for update_pct in [10, 50]:
            update_frac = update_pct / 100
            label = f"{num_rows // 1000}k_rows_{update_pct}pct_update"
            logger.info(f"\n=== Benchmark: {label} ===")

            result = measure_merge(spark, table_name, update_frac)
            results[label] = result

            logger.info(
                f"  MERGE write: {result['write_time_sec']}s, "
                f"read: {result['read_time_sec']}s, "
                f"matched: {result['matched']:,}, "
                f"mismatched: {result['mismatched']:,}"
            )
            logger.info(f"  Files: {result['files_before']} -> {result['files_after']}")

            spark.sql(
                f"""UPDATE {table_name}
                    SET reconciliation_status = 'PENDING_SETTLEMENT',
                        bank_ref_id = NULL"""
            )

    for num_rows in row_counts:
        table_name = f"nessie.db.webhooks_bench_{num_rows // 1000}k"
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    spark.stop()

    output = {"hardware": hw, "benchmarks": results}

    out_path = os.path.join(os.path.dirname(__file__), "results_iceberg.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    logger.info(f"\nResults written to {out_path}")

    return output


def main():
    parser = argparse.ArgumentParser(description="Reconciliation Benchmark")
    parser.add_argument("--scale", type=int, default=None, choices=SCALE_OPTIONS)
    args = parser.parse_args()
    run_benchmark(args.scale)


if __name__ == "__main__":
    main()
